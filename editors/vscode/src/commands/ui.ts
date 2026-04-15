import * as vscode from "vscode";
import { getWorkspaceFolder } from "../config";

/**
 * Open a string of text in a transient editor with the given language id.
 * Used for dumping CLI output (JSON, SQL, etc.) into a tab the user can scroll
 * and copy from. The document is untitled — closing it discards the content.
 */
export async function showTextInEditor(
  content: string,
  language: string,
  viewColumn: vscode.ViewColumn = vscode.ViewColumn.Beside,
): Promise<void> {
  const doc = await vscode.workspace.openTextDocument({ content, language });
  await vscode.window.showTextDocument(doc, viewColumn);
}

/** Convenience wrapper that pretty-prints JSON if the input parses cleanly. */
export async function showJsonInEditor(
  content: string,
  viewColumn?: vscode.ViewColumn,
): Promise<void> {
  let formatted = content;
  try {
    formatted = JSON.stringify(JSON.parse(content), null, 2);
  } catch {
    // Not valid JSON; show as-is.
  }
  await showTextInEditor(formatted, "json", viewColumn);
}

/** Convenience wrapper for SQL output. */
export async function showSqlInEditor(
  content: string,
  viewColumn?: vscode.ViewColumn,
): Promise<void> {
  await showTextInEditor(content, "sql", viewColumn);
}

/**
 * Show a warning modal with the given message and a custom confirmation
 * button. Returns true if the user confirmed.
 */
export async function confirmAction(
  message: string,
  confirmLabel: string,
  detail?: string,
): Promise<boolean> {
  const choice = await vscode.window.showWarningMessage(
    message,
    { modal: true, detail },
    confirmLabel,
  );
  return choice === confirmLabel;
}

/**
 * Bail out with an error toast when no workspace is open. Used by commands
 * that need a project root.
 */
export function ensureWorkspace(): boolean {
  if (!getWorkspaceFolder()) {
    vscode.window.showErrorMessage("No workspace folder open");
    return false;
  }
  return true;
}

/**
 * Extract a model name from the argument passed to a command handler.
 *
 * When a command fires from a tree-view context menu, VS Code passes the
 * TreeItem object — not a string. This normaliser handles:
 *  - string argument (returned as-is)
 *  - object with a `label` string property (e.g. ModelTreeItem)
 *  - undefined / null / anything else → undefined (so the caller can fall
 *    back to an input box or the active editor)
 */
export function resolveModelName(arg: unknown): string | undefined {
  if (typeof arg === "string" && arg.length > 0) return arg;
  if (
    arg != null &&
    typeof arg === "object" &&
    "label" in arg &&
    typeof (arg as Record<string, unknown>).label === "string"
  ) {
    return (arg as Record<string, unknown>).label as string;
  }
  return undefined;
}

/**
 * Prompt the user with an input box and validate emptiness.
 * Returns undefined when the user cancels or the input is blank.
 */
export async function promptForInput(
  prompt: string,
  options: { placeHolder?: string; value?: string; required?: boolean } = {},
): Promise<string | undefined> {
  const value = await vscode.window.showInputBox({
    prompt,
    placeHolder: options.placeHolder,
    value: options.value,
  });
  if (value === undefined) return undefined;
  const trimmed = value.trim();
  if (options.required && trimmed.length === 0) return undefined;
  return trimmed;
}
