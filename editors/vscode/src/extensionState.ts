import * as vscode from "vscode";

let _extensionUri: vscode.Uri | undefined;

/** Called once from activate() so other modules can resolve bundled assets. */
export function setExtensionUri(uri: vscode.Uri): void {
  _extensionUri = uri;
}

/**
 * Returns the extension's installation URI. Used by webview commands to
 * resolve bundled scripts and stylesheets via `webview.asWebviewUri`.
 */
export function getExtensionUri(): vscode.Uri {
  if (!_extensionUri) {
    throw new Error(
      "getExtensionUri() called before activate() initialized the URI",
    );
  }
  return _extensionUri;
}
