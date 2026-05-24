import * as fs from "fs";
import * as path from "path";
import * as vscode from "vscode";

export interface RockyConfig {
  serverPath: string;
  extraArgs: string[];
  inlayHintsEnabled: boolean;
  previewRowLimit: number;
  previewAllowWarehouse: boolean;
}

/** Reads the current Rocky settings. Always re-read; do not cache. */
export function getConfig(): RockyConfig {
  const cfg = vscode.workspace.getConfiguration("rocky");
  return {
    serverPath: cfg.get<string>("server.path", "rocky"),
    extraArgs: cfg.get<string[]>("server.extraArgs", []),
    inlayHintsEnabled: cfg.get<boolean>("inlayHints.enabled", true),
    previewRowLimit: cfg.get<number>("preview.rowLimit", 100),
    previewAllowWarehouse: cfg.get<boolean>("preview.allowWarehouse", false),
  };
}

/** Returns the first workspace folder's path, or undefined when none is open. */
export function getWorkspaceFolder(): string | undefined {
  return vscode.workspace.workspaceFolders?.[0]?.uri.fsPath;
}

/**
 * Walk up from `startDir` looking for the nearest directory containing a
 * `rocky.toml`. Returns that directory, or undefined if none found before
 * hitting the filesystem root.
 */
function findRockyTomlUp(startDir: string): string | undefined {
  let dir = startDir;
  for (let i = 0; i < 32; i++) {
    if (fs.existsSync(path.join(dir, "rocky.toml"))) return dir;
    const parent = path.dirname(dir);
    if (parent === dir) return undefined;
    dir = parent;
  }
  return undefined;
}

/**
 * Resolve the Rocky project root for a command. Strategy:
 *  1. If an active file is given, walk up from its directory to find `rocky.toml`.
 *  2. Otherwise, walk up from the active text editor's file (if any).
 *  3. Otherwise, walk up from each workspace folder.
 *  4. Fall back to the first workspace folder (let the CLI report the error).
 */
export function resolveProjectRoot(activeFile?: vscode.Uri | string): string | undefined {
  const candidates: string[] = [];
  if (activeFile) {
    const p = typeof activeFile === "string" ? activeFile : activeFile.fsPath;
    candidates.push(path.dirname(p));
  }
  const editor = vscode.window.activeTextEditor;
  if (editor && editor.document.uri.scheme === "file") {
    candidates.push(path.dirname(editor.document.uri.fsPath));
  }
  for (const folder of vscode.workspace.workspaceFolders ?? []) {
    candidates.push(folder.uri.fsPath);
  }
  for (const dir of candidates) {
    const found = findRockyTomlUp(dir);
    if (found) return found;
  }
  return getWorkspaceFolder();
}
