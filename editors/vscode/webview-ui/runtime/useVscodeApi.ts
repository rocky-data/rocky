/**
 * The VS Code webview API handle, acquired exactly once. `acquireVsCodeApi()`
 * throws if called a second time, so it is memoized in this module and shared
 * by the RPC client and any state-persistence code.
 */
export interface VsCodeApi {
  postMessage(message: unknown): void;
  getState<T = unknown>(): T | undefined;
  setState<T>(state: T): void;
}

declare function acquireVsCodeApi(): VsCodeApi;

let api: VsCodeApi | undefined;

/** Acquire (once) and return the VS Code webview API handle. */
export function getVscodeApi(): VsCodeApi {
  if (!api) api = acquireVsCodeApi();
  return api;
}
