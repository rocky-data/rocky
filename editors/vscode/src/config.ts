import * as vscode from "vscode";

export interface RockyConfig {
  serverPath: string;
  extraArgs: string[];
  inlayHintsEnabled: boolean;
}

/** Reads the current Rocky settings. Always re-read; do not cache. */
export function getConfig(): RockyConfig {
  const cfg = vscode.workspace.getConfiguration("rocky");
  return {
    serverPath: cfg.get<string>("server.path", "rocky"),
    extraArgs: cfg.get<string[]>("server.extraArgs", []),
    inlayHintsEnabled: cfg.get<boolean>("inlayHints.enabled", true),
  };
}

/** Returns the first workspace folder's path, or undefined when none is open. */
export function getWorkspaceFolder(): string | undefined {
  return vscode.workspace.workspaceFolders?.[0]?.uri.fsPath;
}
