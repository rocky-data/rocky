import * as vscode from "vscode";

/** HTML-escape user-controlled text for safe interpolation. */
export function escapeHtml(text: string): string {
  return text
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

/** Crypto-quality nonces aren't required for our use, but unpredictable is good. */
export function makeNonce(): string {
  let result = "";
  const chars =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  for (let i = 0; i < 32; i++) {
    result += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return result;
}

/** Standard CSP for Rocky webviews — no remote anything, scripts via nonce. */
export function buildCsp(webview: vscode.Webview, nonce: string): string {
  return [
    "default-src 'none'",
    `style-src ${webview.cspSource} 'unsafe-inline'`,
    `script-src 'nonce-${nonce}'`,
    `img-src ${webview.cspSource} data:`,
    "font-src " + webview.cspSource,
  ].join("; ");
}

/** Boilerplate <head> with CSP, title, and a shared stylesheet. */
export function buildHead(
  webview: vscode.Webview,
  nonce: string,
  title: string,
  extraStyles = "",
): string {
  return /* html */ `<head>
  <meta charset="utf-8" />
  <meta http-equiv="Content-Security-Policy" content="${buildCsp(webview, nonce)}" />
  <title>${escapeHtml(title)}</title>
  <style nonce="${nonce}">
    body {
      margin: 0; padding: 16px;
      font-family: var(--vscode-font-family);
      color: var(--vscode-foreground);
      background: var(--vscode-editor-background);
      font-size: 13px;
      line-height: 1.5;
    }
    h1 { font-size: 16px; margin: 0 0 8px 0; }
    h2 { font-size: 14px; margin: 24px 0 8px 0; }
    table { border-collapse: collapse; width: 100%; margin-bottom: 16px; }
    th, td {
      text-align: left; padding: 6px 12px;
      border-bottom: 1px solid var(--vscode-panel-border);
    }
    th {
      font-weight: 600;
      background: var(--vscode-editorWidget-background);
    }
    .badge {
      display: inline-block;
      padding: 2px 8px;
      border-radius: 10px;
      font-size: 11px;
      font-weight: 600;
      text-transform: uppercase;
    }
    .badge.ok { background: var(--vscode-testing-iconPassed); color: var(--vscode-editor-background); }
    .badge.warn { background: var(--vscode-testing-iconQueued); color: var(--vscode-editor-background); }
    .badge.fail { background: var(--vscode-testing-iconFailed); color: var(--vscode-editor-background); }
    .stat-grid {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(150px, 1fr));
      gap: 12px;
      margin-bottom: 16px;
    }
    .stat {
      border: 1px solid var(--vscode-panel-border);
      border-radius: 4px;
      padding: 12px;
    }
    .stat .label {
      font-size: 11px;
      text-transform: uppercase;
      color: var(--vscode-descriptionForeground);
      margin-bottom: 4px;
    }
    .stat .value { font-size: 20px; font-weight: 600; }
    .muted { color: var(--vscode-descriptionForeground); }
    pre { background: var(--vscode-editorWidget-background); padding: 8px; overflow: auto; }
    button {
      background: var(--vscode-button-background);
      color: var(--vscode-button-foreground);
      border: none;
      padding: 6px 12px;
      font-size: 12px;
      cursor: pointer;
      border-radius: 2px;
      margin-right: 8px;
    }
    button:hover { background: var(--vscode-button-hoverBackground); }
    ${extraStyles}
  </style>
</head>`;
}
