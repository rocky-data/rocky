import * as vscode from "vscode";
import { buildHead, makeNonce } from "./htmlUtil";

/**
 * Tabular payload rendered by the Query Results panel. `rows` cells are
 * coerced to display strings via {@link coerceCell} before they reach the
 * webview, so the webview never has to reason about JSON types.
 */
export interface GridData {
  columns: string[];
  rows: unknown[][];
  meta?: GridMeta;
}

/** Footer/toolbar metadata shown alongside the grid. */
export interface GridMeta {
  rowCount?: number;
  truncated?: boolean;
  executedSql?: string;
  adapterKind?: string;
  model?: string;
  cte?: string | null;
}

/** Message posted from host → webview. */
type HostMessage =
  | { type: "loading"; title: string }
  | { type: "error"; message: string }
  | {
      type: "data";
      columns: string[];
      rows: string[][];
      meta: GridMeta;
      title: string;
    };

/**
 * Coerce an arbitrary JSON cell value into a display string.
 *
 * `QueryResult` rows are `serde_json::Value`s, so a cell can be a string,
 * number, bool, null, or (rarely) a nested array/object. Null renders empty;
 * objects/arrays render as compact JSON; everything else stringifies.
 */
export function coerceCell(value: unknown): string {
  if (value === null || value === undefined) return "";
  if (typeof value === "string") return value;
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

/**
 * Serialize a grid (already coerced to display strings) as RFC-4180 CSV.
 * Cells containing a comma, quote, or newline are double-quoted with embedded
 * quotes doubled. Pure — unit-tested.
 */
export function toCsv(columns: string[], rows: string[][]): string {
  const esc = (v: string): string =>
    /[",\r\n]/.test(v) ? `"${v.replace(/"/g, '""')}"` : v;
  const header = columns.map(esc).join(",");
  const body = rows.map((r) => r.map(esc).join(",")).join("\r\n");
  return body ? `${header}\r\n${body}` : header;
}

/**
 * Panel-area "Query Results" view (docks next to Problems/Terminal, matching
 * dbt's preview panel). A {@link vscode.WebviewView} isn't instantiated until
 * the view is first revealed, so the provider buffers the latest payload and
 * flushes it on `resolveWebviewView` and on the webview's `ready` handshake —
 * the preview command can call {@link setLoading}/{@link setData} before the
 * panel has ever been opened.
 */
export class ResultGridViewProvider implements vscode.WebviewViewProvider {
  public static readonly viewType = "rocky.queryResults";

  private view: vscode.WebviewView | undefined;
  private ready = false;
  private pending: HostMessage | undefined;
  private lastMeta: GridMeta | undefined;
  private lastData: { columns: string[]; rows: string[][] } | undefined;

  constructor(private readonly extensionUri: vscode.Uri) {}

  resolveWebviewView(view: vscode.WebviewView): void {
    this.view = view;
    this.ready = false;
    view.webview.options = {
      enableScripts: true,
      localResourceRoots: [this.extensionUri],
    };
    view.webview.html = this.html(view.webview);

    view.webview.onDidReceiveMessage((msg: { type?: string }) => {
      if (msg.type === "ready") {
        this.ready = true;
        if (this.pending) {
          view.webview.postMessage(this.pending);
          this.pending = undefined;
        }
      } else if (msg.type === "copyTsv") {
        void this.copyTsv();
      } else if (msg.type === "saveCsv") {
        void this.saveCsv();
      } else if (msg.type === "showSql") {
        void this.showSql();
      }
    });

    view.onDidDispose(() => {
      this.view = undefined;
      this.ready = false;
    });
  }

  /** Reveal the panel and show a spinner-y placeholder for `title`. */
  setLoading(title: string): void {
    this.post({ type: "loading", title });
  }

  /** Render `data` in the grid (coercing cells to display strings). */
  setData(title: string, data: GridData): void {
    this.lastMeta = data.meta;
    const rows = data.rows.map((r) => r.map(coerceCell));
    this.lastData = { columns: data.columns, rows };
    this.post({
      type: "data",
      columns: data.columns,
      rows,
      meta: data.meta ?? {},
      title,
    });
  }

  /** Show an error message in the panel. */
  setError(message: string): void {
    this.post({ type: "error", message });
  }

  private post(msg: HostMessage): void {
    if (this.view && this.ready) {
      void this.view.webview.postMessage(msg);
    } else {
      // Buffer until the webview is resolved + ready (it flushes on `ready`).
      this.pending = msg;
    }
  }

  private async copyTsv(): Promise<void> {
    if (!this.lastData) return;
    const header = this.lastData.columns.join("\t");
    const body = this.lastData.rows.map((r) => r.join("\t")).join("\n");
    await vscode.env.clipboard.writeText(`${header}\n${body}`);
    void vscode.window.showInformationMessage("Query results copied (TSV).");
  }

  private async saveCsv(): Promise<void> {
    if (!this.lastData) return;
    const name = this.lastMeta?.cte ?? this.lastMeta?.model ?? "query-results";
    const uri = await vscode.window.showSaveDialog({
      filters: { CSV: ["csv"] },
      saveLabel: "Save query results",
      defaultUri: vscode.Uri.file(`${name}.csv`),
    });
    if (!uri) return;
    const csv = toCsv(this.lastData.columns, this.lastData.rows);
    await vscode.workspace.fs.writeFile(uri, Buffer.from(csv, "utf8"));
    void vscode.window.showInformationMessage(`Saved ${this.lastData.rows.length} rows to CSV.`);
  }

  private async showSql(): Promise<void> {
    const sql = this.lastMeta?.executedSql;
    if (!sql) return;
    const doc = await vscode.workspace.openTextDocument({
      content: sql,
      language: "sql",
    });
    await vscode.window.showTextDocument(doc, {
      viewColumn: vscode.ViewColumn.Beside,
      preview: true,
    });
  }

  private html(webview: vscode.Webview): string {
    const nonce = makeNonce();
    const extraStyles = `
      #toolbar { display:flex; align-items:center; gap:12px; margin-bottom:8px; flex-wrap:wrap; }
      #meta { color: var(--vscode-descriptionForeground); font-size:12px; }
      #grid-wrap { overflow:auto; max-height: calc(100vh - 80px); }
      table.grid { border-collapse: collapse; width: 100%; font-variant-numeric: tabular-nums; }
      table.grid th { cursor: pointer; user-select: none; position: sticky; top: 0; }
      table.grid th .arrow { color: var(--vscode-descriptionForeground); margin-left:4px; }
      table.grid td, table.grid th { white-space: pre; max-width: 480px; overflow: hidden; text-overflow: ellipsis; }
      table.grid td.num, table.grid th.num { text-align: right; font-variant-numeric: tabular-nums; }
      .empty { color: var(--vscode-descriptionForeground); padding: 12px 0; }
      .err { color: var(--vscode-errorForeground); padding: 12px 0; white-space: pre-wrap; }
    `;
    return /* html */ `<!DOCTYPE html>
<html lang="en">
${buildHead(webview, nonce, "Query Results", extraStyles)}
<body>
  <div id="toolbar">
    <span id="title"></span>
    <span id="meta"></span>
    <span style="flex:1"></span>
    <button id="copy" hidden>Copy TSV</button>
    <button id="csv" hidden>Save CSV</button>
    <button id="sql" hidden>Show SQL</button>
  </div>
  <div id="status" class="empty">Run <strong>Preview</strong> on a model to see rows here.</div>
  <div id="grid-wrap"></div>
  <script nonce="${nonce}">
    const vscode = acquireVsCodeApi();
    const $ = (id) => document.getElementById(id);
    let state = { columns: [], rows: [], sortCol: -1, sortDir: 1, numeric: [] };

    function cmp(a, b) {
      const na = Number(a), nb = Number(b);
      const numeric = a !== "" && b !== "" && !Number.isNaN(na) && !Number.isNaN(nb);
      if (numeric) return na - nb;
      return a < b ? -1 : a > b ? 1 : 0;
    }

    // A column is numeric when every non-empty cell parses as a number.
    function computeNumericCols(columns, rows) {
      return columns.map((_, i) => {
        let seen = false;
        for (const r of rows) {
          const v = r[i];
          if (v === "" || v == null) continue;
          seen = true;
          if (Number.isNaN(Number(v))) return false;
        }
        return seen;
      });
    }

    function render() {
      const wrap = $("grid-wrap");
      if (!state.columns.length) { wrap.innerHTML = ""; return; }
      let rows = state.rows;
      if (state.sortCol >= 0) {
        rows = rows.slice().sort((r1, r2) => state.sortDir * cmp(r1[state.sortCol], r2[state.sortCol]));
      }
      const cls = (i) => state.numeric[i] ? " class='num'" : "";
      const thead = "<thead><tr>" + state.columns.map((c, i) => {
        const arrow = i === state.sortCol ? (state.sortDir > 0 ? "▲" : "▼") : "";
        return "<th data-col='" + i + "'" + cls(i) + ">" + escapeHtml(c) + "<span class='arrow'>" + arrow + "</span></th>";
      }).join("") + "</tr></thead>";
      const tbody = "<tbody>" + rows.map((r) =>
        "<tr>" + r.map((cell, i) => "<td" + cls(i) + ">" + escapeHtml(cell) + "</td>").join("") + "</tr>"
      ).join("") + "</tbody>";
      wrap.innerHTML = "<table class='grid'>" + thead + tbody + "</table>";
      for (const th of wrap.querySelectorAll("th")) {
        th.addEventListener("click", () => {
          const col = Number(th.getAttribute("data-col"));
          if (state.sortCol === col) state.sortDir = -state.sortDir;
          else { state.sortCol = col; state.sortDir = 1; }
          render();
        });
      }
    }

    function escapeHtml(s) {
      return String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;")
        .replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#39;");
    }

    function applyData(m) {
      state = { columns: m.columns, rows: m.rows, sortCol: -1, sortDir: 1, numeric: computeNumericCols(m.columns, m.rows) };
      $("title").textContent = m.title;
      const bits = [];
      if (typeof m.meta.rowCount === "number") bits.push(m.meta.rowCount + (m.meta.truncated ? "+ rows (truncated)" : " rows"));
      if (m.meta.adapterKind) bits.push(m.meta.adapterKind);
      if (m.meta.cte) bits.push("CTE: " + m.meta.cte);
      $("meta").textContent = bits.join(" · ");
      const status = $("status");
      status.hidden = state.rows.length > 0;
      if (!state.rows.length) { status.className = "empty"; status.textContent = "(0 rows)"; }
      $("copy").hidden = false;
      $("csv").hidden = false;
      $("sql").hidden = !m.meta.executedSql;
      render();
    }

    window.addEventListener("message", (ev) => {
      const m = ev.data;
      const status = $("status"), copy = $("copy"), csv = $("csv"), sql = $("sql"), meta = $("meta"), title = $("title");
      if (m.type === "loading") {
        title.textContent = m.title; meta.textContent = "";
        status.className = "empty"; status.textContent = "Running…"; status.hidden = false;
        copy.hidden = true; csv.hidden = true; sql.hidden = true; $("grid-wrap").innerHTML = "";
      } else if (m.type === "error") {
        title.textContent = ""; meta.textContent = "";
        status.className = "err"; status.textContent = m.message; status.hidden = false;
        copy.hidden = true; csv.hidden = true; sql.hidden = true; $("grid-wrap").innerHTML = "";
      } else if (m.type === "data") {
        applyData(m);
        vscode.setState(m); // survive webview reloads (window reload, panel re-open)
      }
    });

    $("copy").addEventListener("click", () => vscode.postMessage({ type: "copyTsv" }));
    $("csv").addEventListener("click", () => vscode.postMessage({ type: "saveCsv" }));
    $("sql").addEventListener("click", () => vscode.postMessage({ type: "showSql" }));

    // Restore the last results from persisted state before announcing readiness.
    const saved = vscode.getState();
    if (saved && saved.type === "data") applyData(saved);

    vscode.postMessage({ type: "ready" });
  </script>
</body>
</html>`;
  }
}

/** Register the Query Results panel view; returns the provider for the host to push data into. */
export function registerQueryResultsView(
  context: vscode.ExtensionContext,
): ResultGridViewProvider {
  const provider = new ResultGridViewProvider(context.extensionUri);
  context.subscriptions.push(
    vscode.window.registerWebviewViewProvider(
      ResultGridViewProvider.viewType,
      provider,
      { webviewOptions: { retainContextWhenHidden: true } },
    ),
  );
  return provider;
}
