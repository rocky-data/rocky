import * as vscode from "vscode";
import type {
  DiffSummary,
  LineageColumnChange,
  LineageDiffOutput,
  LineageDiffResult,
  LineageQualifiedColumn,
} from "../types/generated/lineage_diff";
import { buildHead, escapeHtml, makeNonce } from "./htmlUtil";

/**
 * Verdict for a lineage diff. `clean` when nothing changed; `warn` when columns
 * were modified or removed (those have downstream blast radius); `ok` when the
 * only changes are additions.
 */
export function diffVerdict(s: DiffSummary): "clean" | "ok" | "warn" {
  if (s.added + s.modified + s.removed === 0) return "clean";
  return s.modified + s.removed > 0 ? "warn" : "ok";
}

/** Describe a column change, e.g. `email: STRING → INT` or `ssn`. */
export function formatColumnChange(c: LineageColumnChange): string {
  if (c.old_type != null || c.new_type != null) {
    return `${c.column_name}: ${c.old_type ?? "∅"} → ${c.new_type ?? "∅"}`;
  }
  return c.column_name;
}

/** Downstream blast radius as `model.column, model.column`, or `—` when none. */
export function formatBlastRadius(
  consumers: LineageQualifiedColumn[] | undefined,
): string {
  if (!consumers || consumers.length === 0) return "—";
  return consumers.map((q) => `${q.model}.${q.column}`).join(", ");
}

/** Open a webview rendering the column-level lineage diff vs a base ref. */
export function showLineageDiff(result: LineageDiffOutput): void {
  const panel = vscode.window.createWebviewPanel(
    "rockyLineageDiff",
    "Rocky lineage diff",
    vscode.ViewColumn.Beside,
    { enableScripts: false, retainContextWhenHidden: true },
  );
  panel.webview.html = render(panel.webview, result);
}

function render(webview: vscode.Webview, result: LineageDiffOutput): string {
  const nonce = makeNonce();
  const s = result.summary;
  const verdict = diffVerdict(s);

  const changed = result.results.filter(
    (r: LineageDiffResult) => r.column_changes.length > 0,
  );

  const sections = changed
    .map((r: LineageDiffResult) => {
      const rows = r.column_changes
        .map(
          (c: LineageColumnChange) => `
        <tr>
          <td><code>${escapeHtml(c.change_type)}</code></td>
          <td>${escapeHtml(formatColumnChange(c))}</td>
          <td>${escapeHtml(formatBlastRadius(c.downstream_consumers))}</td>
        </tr>`,
        )
        .join("");
      return /* html */ `
      <h2>${escapeHtml(r.model_name)} <span class="muted">· ${escapeHtml(r.status)}</span></h2>
      <table>
        <thead><tr><th>Change</th><th>Column</th><th>Downstream blast radius</th></tr></thead>
        <tbody>${rows}</tbody>
      </table>`;
    })
    .join("");

  return /* html */ `<!DOCTYPE html>
<html lang="en">
${buildHead(webview, nonce, "Rocky lineage diff")}
<body>
  <h1>Lineage diff <span class="badge ${verdict}">${verdict}</span></h1>
  <p class="muted">Column-level changes from <code>${escapeHtml(result.base_ref)}</code> → <code>${escapeHtml(result.head_ref)}</code>, with downstream impact.</p>

  <div class="stat-grid">
    ${stat("Added", s.added)}
    ${stat("Modified", s.modified)}
    ${stat("Removed", s.removed)}
    ${stat("Unchanged", s.unchanged)}
  </div>

  ${
    sections ||
    `<p class="badge ok">No column-level changes vs ${escapeHtml(result.base_ref)}.</p>`
  }
</body>
</html>`;
}

function stat(label: string, value: number): string {
  return /* html */ `
    <div class="stat">
      <div class="label">${escapeHtml(label)}</div>
      <div class="value">${value}</div>
    </div>`;
}
