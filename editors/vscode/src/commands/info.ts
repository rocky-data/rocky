import * as vscode from "vscode";
import { getWorkspaceFolder } from "../config";
import { getOutputChannel } from "../output";
import { clearCliVersionCache } from "../rockyCli";

const URLS = {
  documentation: "https://github.com/rocky-data/rocky#readme",
  issues: "https://github.com/rocky-data/rocky/issues/new",
  marketplace:
    "https://marketplace.visualstudio.com/items?itemName=rocky-data.rocky",
} as const;

export function openOutputChannel(): void {
  getOutputChannel().show(true);
}

export function openDocumentation(): Thenable<boolean> {
  return vscode.env.openExternal(vscode.Uri.parse(URLS.documentation));
}

export function reportBug(): Thenable<boolean> {
  return vscode.env.openExternal(vscode.Uri.parse(URLS.issues));
}

export function viewMarketplace(): Thenable<boolean> {
  return vscode.env.openExternal(vscode.Uri.parse(URLS.marketplace));
}

/**
 * Open an integrated terminal in the workspace root and run a Rocky CLI
 * command. Used by the Get Started panel for `rocky init` and
 * `rocky playground`. The terminal is shown so the user can watch progress
 * and answer any prompts the CLI asks.
 *
 * The command is sent via {@link vscode.Terminal.sendText} (no shell quoting
 * required for these argv-free commands).
 */
function runInTerminal(name: string, command: string): void {
  const cwd = getWorkspaceFolder();
  const terminal = vscode.window.createTerminal({ name, cwd });
  terminal.show(true);
  terminal.sendText(command);
}

export function init(): void {
  runInTerminal("Rocky: Init", "rocky init");
}

export function playground(): void {
  runInTerminal("Rocky: Playground", "rocky playground");
}

/**
 * Force the next Extension Info refresh to re-shell `rocky --version`.
 * Wired up via the registered `rocky.refreshInfo` command in
 * {@link registerExtensionInfoView}; calling this clears the underlying
 * cache so the new fetch isn't served from the 60s TTL.
 */
export function invalidateInfoCaches(): void {
  clearCliVersionCache();
}
