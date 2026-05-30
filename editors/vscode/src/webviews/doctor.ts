import * as vscode from "vscode";
import type { DoctorResult } from "../types/rockyJson";
import { buildHead, escapeHtml, makeNonce } from "./htmlUtil";

/**
 * Renders a Rocky doctor result as a webview panel with per-check status,
 * suggestions, and "Re-run" / "Open settings" actions. Replaces the multiline
 * toast which was unreadable for >2 checks.
 */
export function showDoctorResult(result: DoctorResult): void {
  const panel = vscode.window.createWebviewPanel(
    "rockyDoctor",
    "Rocky Doctor",
    vscode.ViewColumn.Beside,
    {
      enableScripts: true,
      retainContextWhenHidden: true,
      // Allow only the exact command: URIs this webview's HTML links to, so a
      // future templating bug can't smuggle in an arbitrary command. Every
      // command: anchor here resolves to workbench.action.openSettings (the
      // ?"…" query is the settings filter, not a separate command).
      enableCommandUris: ["workbench.action.openSettings"],
    },
  );

  panel.webview.html = renderDoctorHtml(panel.webview, result);

  const allowedCommands = new Set(["rerun", "openSettings"] as const);
  panel.webview.onDidReceiveMessage((raw: unknown) => {
    // Treat all webview messages as untrusted — the webview's script is
    // ours today, but a future CSP gap or injected iframe could forge one.
    if (typeof raw !== "object" || raw === null) return;
    const cmd = (raw as { command?: unknown }).command;
    if (typeof cmd !== "string" || !allowedCommands.has(cmd as never)) return;

    if (cmd === "rerun") {
      panel.dispose();
      void vscode.commands.executeCommand("rocky.doctor");
    } else if (cmd === "openSettings") {
      void vscode.commands.executeCommand(
        "workbench.action.openSettings",
        "rocky",
      );
    }
  });
}

function renderDoctorHtml(
  webview: vscode.Webview,
  result: DoctorResult,
): string {
  const nonce = makeNonce();
  const overall = result.overall ?? "unknown";
  const checks = result.checks ?? [];
  const suggestions = result.suggestions ?? [];

  const verdict =
    overall === "healthy"
      ? "ok"
      : overall === "warning"
        ? "warn"
        : "fail";

  const rows = checks
    .map((c) => {
      const isFailed =
        c.status === "critical" ||
        c.status === "warning";
      const actionLinks = isFailed ? buildCheckActionLinks(c.name ?? "") : "";
      return `
      <tr>
        <td><span class="badge ${badgeFor(c.status)}">${escapeHtml(c.status ?? "?")}</span></td>
        <td><strong>${escapeHtml(c.name ?? "")}</strong></td>
        <td>${escapeHtml(c.message ?? "")}${actionLinks}</td>
        <td class="muted">${typeof c.duration_ms === "number" ? c.duration_ms + "ms" : ""}</td>
      </tr>`;
    })
    .join("");

  const suggestionList = suggestions
    .map((s) => `<li>${escapeHtml(s)}${buildSuggestionActionLink(s)}</li>`)
    .join("");

  return /* html */ `<!DOCTYPE html>
<html lang="en">
${buildHead(webview, nonce, "Rocky Doctor")}
<body>
  <h1>Rocky Doctor <span class="badge ${verdict}">${escapeHtml(overall)}</span></h1>

  <div style="margin: 16px 0;">
    <button id="rerun">Re-run Doctor</button>
    <button id="settings">Open Settings</button>
  </div>

  <h2>Checks (${checks.length})</h2>
  <table>
    <thead>
      <tr><th>Status</th><th>Check</th><th>Message</th><th></th></tr>
    </thead>
    <tbody>${rows || `<tr><td colspan="4" class="muted">No checks reported.</td></tr>`}</tbody>
  </table>

  ${suggestionList ? `<h2>Suggestions</h2><ul>${suggestionList}</ul>` : ""}

  <script nonce="${nonce}">
    const vscode = acquireVsCodeApi();
    document.getElementById('rerun').addEventListener('click', () => {
      vscode.postMessage({ command: 'rerun' });
    });
    document.getElementById('settings').addEventListener('click', () => {
      vscode.postMessage({ command: 'openSettings' });
    });
  </script>
</body>
</html>`;
}

function badgeFor(status: string | undefined): string {
  switch (status) {
    case "healthy":
      return "ok";
    case "warning":
      return "warn";
    case "critical":
    case "error":
      return "fail";
    default:
      return "warn";
  }
}

/**
 * Returns inline `command:` anchor tags for a failed check.
 *
 * Uses VS Code `command:` URIs — these work because
 * `workbench.action.openSettings` is on the webview's `enableCommandUris`
 * allowlist. All links are hardcoded to that command; no user-supplied data is
 * interpolated into the href.
 */
function buildCheckActionLinks(checkName: string): string {
  // Always offer "Open settings" for any failed check.
  const settingsLink = ` <a href="command:workbench.action.openSettings?%22rocky%22" title="Open Rocky settings">Open settings</a>`;

  // For CLI-not-found checks, also offer a path configuration shortcut.
  const isCliCheck =
    /cli|binary|install|path|not found/i.test(checkName);
  if (isCliCheck) {
    const configLink = ` · <a href="command:workbench.action.openSettings?%22rocky.server.path%22" title="Configure CLI path">Configure path</a>`;
    return settingsLink + configLink;
  }

  return settingsLink;
}

/**
 * Returns an inline `command:` anchor when the suggestion text implies a
 * well-known one-click fix, otherwise returns an empty string.
 */
function buildSuggestionActionLink(suggestion: string): string {
  if (/settings?|configure|set/i.test(suggestion)) {
    return ` <a href="command:workbench.action.openSettings?%22rocky%22" title="Open Rocky settings">Open settings</a>`;
  }
  return "";
}
