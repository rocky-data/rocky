import { getExtensionUri } from "../extensionState";
import { createWebviewPanelApp } from "../webviews/host/registerPanel";

/**
 * Open the Rocky webview devtools panel — a smoke test for the React/ReactFlow
 * webview foundation: Tailwind theming via `--vscode-*` variables, a code-split
 * bundle loading under the webview CSP, and a host RPC round-trip. It exists to
 * validate the webview toolchain end-to-end.
 */
export function devtools(): void {
  createWebviewPanelApp(getExtensionUri(), "rocky.devtools", {
    entry: "devtools",
    title: "Rocky Devtools",
    setup: (host) => {
      host.onRequest("echo", (params) => `host echoed: ${String(params)}`);
    },
  });
}
