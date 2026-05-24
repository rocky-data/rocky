import * as vscode from "vscode";
import type {
  ColumnClassificationStatus,
  ComplianceException,
  ComplianceOutput,
  ComplianceSummary,
  EnvMaskingStatus,
} from "../types/generated/compliance";
import { buildHead, escapeHtml, makeNonce } from "./htmlUtil";

/**
 * Overall verdict for a compliance report. `fail` when any classified column
 * is unmasked-where-expected; `warn` when columns are unmasked but explicitly
 * allow-listed (the `total_classified − total_masked − total_exceptions` gap);
 * `ok` when every classified column resolves to a masking strategy.
 */
export function complianceVerdict(summary: ComplianceSummary): "ok" | "warn" | "fail" {
  if (summary.total_exceptions > 0) return "fail";
  const allowListed =
    summary.total_classified - summary.total_masked - summary.total_exceptions;
  return allowListed > 0 ? "warn" : "ok";
}

/**
 * Render a column's per-environment masking status as a compact string, e.g.
 * `default: hash · prod: redact · dev: unresolved`. Pure — unit-tested.
 */
export function formatEnvStatuses(envs: EnvMaskingStatus[]): string {
  return envs
    .map((e) => `${e.env}: ${e.masking_strategy}${e.enforced ? "" : " ⚠"}`)
    .join(" · ");
}

/** Open a webview rendering the structured output of `rocky compliance`. */
export function showCompliance(result: ComplianceOutput): void {
  const panel = vscode.window.createWebviewPanel(
    "rockyCompliance",
    "Rocky compliance",
    vscode.ViewColumn.Beside,
    { enableScripts: false, retainContextWhenHidden: true },
  );
  panel.webview.html = render(panel.webview, result);
}

function render(webview: vscode.Webview, result: ComplianceOutput): string {
  const nonce = makeNonce();
  const s = result.summary;
  const verdict = complianceVerdict(s);
  const allowListed = s.total_classified - s.total_masked - s.total_exceptions;

  const exceptionRows = result.exceptions
    .map(
      (e: ComplianceException) => `
      <tr>
        <td>${escapeHtml(e.model)}</td>
        <td>${escapeHtml(e.column)}</td>
        <td>${escapeHtml(e.env)}</td>
        <td>${escapeHtml(e.reason)}</td>
      </tr>`,
    )
    .join("");

  const perColumnRows = result.per_column
    .map(
      (c: ColumnClassificationStatus) => `
      <tr>
        <td>${escapeHtml(c.model)}</td>
        <td>${escapeHtml(c.column)}</td>
        <td><code>${escapeHtml(c.classification)}</code></td>
        <td>${escapeHtml(formatEnvStatuses(c.envs))}</td>
      </tr>`,
    )
    .join("");

  return /* html */ `<!DOCTYPE html>
<html lang="en">
${buildHead(webview, nonce, "Rocky compliance")}
<body>
  <h1>Rocky compliance <span class="badge ${verdict}">${verdict}</span></h1>
  <p class="muted">Static governance check — classification sidecars vs <code>[mask]</code> policy. No warehouse calls.</p>

  <div class="stat-grid">
    ${stat("Classified", s.total_classified)}
    ${stat("Masked", s.total_masked)}
    ${stat("Exceptions", s.total_exceptions)}
    ${stat("Allow-listed", allowListed)}
  </div>

  ${
    exceptionRows
      ? `<h2>Exceptions (${result.exceptions.length})</h2>
    <p class="muted">Classified columns with no resolved masking strategy, not on the allow list.</p>
    <table>
      <thead><tr><th>Model</th><th>Column</th><th>Env</th><th>Reason</th></tr></thead>
      <tbody>${exceptionRows}</tbody>
    </table>`
      : `<p class="badge ok">No exceptions — every classified column resolves to a masking strategy.</p>`
  }

  ${
    perColumnRows
      ? `<h2>Classified columns (${result.per_column.length})</h2>
    <table>
      <thead><tr><th>Model</th><th>Column</th><th>Tag</th><th>Masking by env</th></tr></thead>
      <tbody>${perColumnRows}</tbody>
    </table>`
      : `<p class="muted">No classified columns found. Tag columns via a <code>[classification]</code> block in a model sidecar.</p>`
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
