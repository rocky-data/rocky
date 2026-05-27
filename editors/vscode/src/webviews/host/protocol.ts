/**
 * Wire protocol shared by the extension host and the React webview apps.
 *
 * Type-only: this module has no runtime. The host imports it (compiled to CJS
 * by `tsc -p ./`); each webview imports it type-only (erased by esbuild). A
 * panel declares its own method/push maps so its full contract lives in one
 * place and both sides type-check against it.
 */

/** A request from the webview to the host, answered by a correlated response. */
export interface RpcRequest<M extends string = string, P = unknown> {
  kind: "req";
  id: string;
  method: M;
  params: P;
}

/** The host's reply to an {@link RpcRequest}, correlated by `id`. */
export type RpcResponse<R = unknown> =
  | { kind: "res"; id: string; ok: true; result: R }
  | { kind: "res"; id: string; ok: false; error: string };

/** A fire-and-forget message pushed from the host to the webview. */
export interface PushMessage<T extends string = string, P = unknown> {
  kind: "push";
  type: T;
  payload: P;
}

/** Webview → host: the app has mounted and may now receive buffered pushes. */
export interface ReadyMessage {
  kind: "ready";
}

/**
 * Coarse active-theme info for the rare JS-side palette decision (e.g. a canvas
 * minimap). Ordinary styling tracks the theme automatically via `--vscode-*`
 * CSS variables, so most components never need this. Delivered as a push of
 * type `"theme"`.
 */
export interface ThemeTokens {
  kind: "light" | "dark" | "high-contrast" | "high-contrast-light";
}

/** Every message the webview may post to the host. */
export type WebviewToHost = RpcRequest | ReadyMessage;

/** Every message the host may post to the webview. */
export type HostToWebview = RpcResponse | PushMessage;
