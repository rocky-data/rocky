import * as vscode from "vscode";
import { escapeHtml, makeNonce } from "../htmlUtil";

/** Options for {@link renderAppHtml}. */
export interface RenderAppOptions {
  /** Bundle name under `dist/webviews/` — `"devtools"` loads devtools.js + devtools.css. */
  entry: string;
  /** Document title. */
  title: string;
  /** Extension root URI, used to resolve `dist/webviews/`. */
  extensionUri: vscode.Uri;
}

/**
 * CSP for a bundle-based ESM webview app. Two deltas from the inline-script CSP
 * in {@link buildCsp}: `script-src` must list `webview.cspSource` (not just the
 * nonce) so the code-split chunks loaded by the ESM entry are allowed — the
 * nonce authorizes only the entry `<script>`; and `style-src` keeps
 * `'unsafe-inline'` because ReactFlow writes inline transform styles onto nodes.
 */
function buildAppCsp(webview: vscode.Webview, nonce: string): string {
  return [
    "default-src 'none'",
    `style-src ${webview.cspSource} 'unsafe-inline'`,
    `script-src ${webview.cspSource} 'nonce-${nonce}'`,
    `font-src ${webview.cspSource}`,
    `img-src ${webview.cspSource} data:`,
  ].join("; ");
}

/** Full HTML document hosting the code-split React app at `dist/webviews/<entry>`. */
export function renderAppHtml(
  webview: vscode.Webview,
  opts: RenderAppOptions,
): string {
  const nonce = makeNonce();
  const root = vscode.Uri.joinPath(opts.extensionUri, "dist", "webviews");
  // Trailing slash is required: the ESM entry's chunk imports ("./chunks/…")
  // resolve against <base href>; without it they 404 silently and the panel is blank.
  const baseHref = `${webview.asWebviewUri(root)}/`;
  const scriptUri = webview.asWebviewUri(
    vscode.Uri.joinPath(root, `${opts.entry}.js`),
  );
  const styleUri = webview.asWebviewUri(
    vscode.Uri.joinPath(root, `${opts.entry}.css`),
  );
  return /* html */ `<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta http-equiv="Content-Security-Policy" content="${buildAppCsp(webview, nonce)}" />
    <base href="${baseHref}" />
    <link rel="stylesheet" href="${styleUri}" />
    <title>${escapeHtml(opts.title)}</title>
  </head>
  <body>
    <div id="root"></div>
    <script type="module" nonce="${nonce}" src="${scriptUri}"></script>
  </body>
</html>`;
}
