import * as vscode from "vscode";
import type { RunResult } from "../types/rockyJson";
import { buildHead, escapeHtml, makeNonce } from "./htmlUtil";

/**
 * Open a webview rendering the structured output of `rocky run`. Replaces
 * the previous "dump JSON in editor" approach with a scannable summary.
 */
export function showRunSummary(result: RunResult): void {
  const filter = result.filter ?? "";
  const panel = vscode.window.createWebviewPanel(
    "rockyRunSummary",
    `Rocky run${filter ? `: ${filter}` : ""}`,
    vscode.ViewColumn.Beside,
    { enableScripts: false, retainContextWhenHidden: true },
  );
  panel.webview.html = renderRunSummary(panel.webview, result);
}

function renderRunSummary(
  webview: vscode.Webview,
  result: RunResult,
): string {
  const nonce = makeNonce();
  const copied = result.tables_copied ?? 0;
  const failed = result.tables_failed ?? 0;
  const skipped = result.tables_skipped ?? 0;
  const drifted = result.drift?.tables_drifted ?? 0;
  const duration = formatDuration(result.duration_ms);
  const verdict =
    failed > 0 ? "fail" : drifted > 0 || skipped > 0 ? "warn" : "ok";

  const errorRows = (result.errors ?? [])
    .map(
      (e) => `
      <tr>
        <td>${escapeHtml(formatAssetKey(e.asset_key))}</td>
        <td>${escapeHtml(e.error ?? "")}</td>
      </tr>`,
    )
    .join("");

  return /* html */ `<!DOCTYPE html>
<html lang="en">
${buildHead(webview, nonce, "Rocky run summary")}
<body>
  <h1>Rocky run <span class="badge ${verdict}">${verdict}</span></h1>
  <p class="muted">Filter: <code>${escapeHtml(result.filter ?? "(none)")}</code> · Duration: ${duration}${result.shadow ? " · shadow mode" : ""}</p>

  <div class="stat-grid">
    ${renderStat("Copied", copied)}
    ${renderStat("Failed", failed)}
    ${renderStat("Skipped", skipped)}
    ${renderStat("Drifted", drifted)}
  </div>

  ${errorRows ? `
    <h2>Errors (${result.errors?.length ?? 0})</h2>
    <table>
      <thead><tr><th>Asset</th><th>Message</th></tr></thead>
      <tbody>${errorRows}</tbody>
    </table>` : ""}

  ${result.drift && drifted > 0 ? `
    <h2>Drift</h2>
    <p>${drifted} of ${result.drift.tables_checked ?? "?"} tables drifted.</p>` : ""}
</body>
</html>`;
}

function renderStat(label: string, value: number): string {
  return /* html */ `
    <div class="stat">
      <div class="label">${escapeHtml(label)}</div>
      <div class="value">${value}</div>
    </div>`;
}

function formatDuration(ms: number | undefined): string {
  if (typeof ms !== "number") return "—";
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s`;
  return `${Math.floor(ms / 60_000)}m ${Math.floor((ms % 60_000) / 1000)}s`;
}

function formatAssetKey(key: unknown): string {
  if (Array.isArray(key)) return key.join(".");
  if (typeof key === "string") return key;
  return JSON.stringify(key);
}
