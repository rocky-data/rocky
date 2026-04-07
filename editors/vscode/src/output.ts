import * as vscode from "vscode";

let channel: vscode.OutputChannel | undefined;

/**
 * Returns the shared "Rocky" output channel, creating it on first access.
 * Used by both the LSP client lifecycle and the CLI subprocess wrapper so the
 * user has a single place to inspect what the extension is doing.
 */
export function getOutputChannel(): vscode.OutputChannel {
  if (!channel) {
    channel = vscode.window.createOutputChannel("Rocky");
  }
  return channel;
}

/** Disposes the singleton output channel. Called from `deactivate`. */
export function disposeOutputChannel(): void {
  channel?.dispose();
  channel = undefined;
}
