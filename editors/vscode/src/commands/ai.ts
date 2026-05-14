import * as path from "path";
import * as vscode from "vscode";
import { runRockyWithProgress, showRockyError } from "../rockyCli";

// ---------------------------------------------------------------------------
// Pure helpers — invokable from commands AND the chat participant.
// ---------------------------------------------------------------------------

/** Run `rocky ai <intent>` and return the generated model source. */
export async function runAiGenerate(intent: string): Promise<string> {
  const { stdout } = await runRockyWithProgress("Generating model...", [
    "ai",
    intent,
    "--format",
    "rocky",
  ]);
  return stdout;
}

/** Run `rocky ai-sync` optionally scoped to a single model. Returns CLI output. */
export async function runAiSync(model?: string): Promise<string> {
  const args = ["ai-sync", "--models", "models"];
  if (model) args.push("--model", model);
  const { stdout } = await runRockyWithProgress("Syncing models...", args);
  return stdout;
}

/** Run `rocky ai-explain` for a named model or all models. Returns CLI output. */
export async function runAiExplain(modelName?: string): Promise<string> {
  const args = ["ai-explain", "--models", "models", "--save"];
  if (modelName) {
    args.push(modelName);
  } else {
    args.push("--all");
  }
  const { stdout } = await runRockyWithProgress("Generating intent...", args);
  return stdout;
}

/** Run `rocky ai-test` for a named model or all models. Returns CLI output. */
export async function runAiTest(modelName?: string): Promise<string> {
  const args = ["ai-test", "--models", "models", "--save"];
  if (modelName) {
    args.push(modelName);
  } else {
    args.push("--all");
  }
  const { stdout } = await runRockyWithProgress("Generating tests...", args);
  return stdout;
}

// ---------------------------------------------------------------------------
// VS Code command handlers — prompt the user, then call the helpers above.
// ---------------------------------------------------------------------------

export async function aiGenerate(): Promise<void> {
  const intent = await vscode.window.showInputBox({
    prompt: "Describe the model you want to create",
    placeHolder:
      "e.g., monthly revenue per customer from the orders table, filtered to 2024",
  });
  if (!intent) return;

  try {
    const source = await runAiGenerate(intent);
    const doc = await vscode.workspace.openTextDocument({
      content: source,
      language: "rocky",
    });
    await vscode.window.showTextDocument(doc);
  } catch (err) {
    showRockyError("Rocky AI failed", err);
  }
}

export async function aiSync(): Promise<void> {
  const model = await vscode.window.showInputBox({
    prompt: "Model name to sync (leave empty for all models with intent)",
    placeHolder: "e.g., fct_daily_revenue",
  });

  try {
    const output = await runAiSync(model ?? undefined);
    vscode.window.showInformationMessage(
      output.trim() || "No models need syncing.",
    );
  } catch (err) {
    showRockyError("Rocky AI Sync failed", err);
  }
}

export async function aiExplain(): Promise<void> {
  const modelName = activeModelName();

  try {
    const output = await runAiExplain(modelName);
    vscode.window.showInformationMessage(
      output.trim() || "Intent generated and saved.",
    );
  } catch (err) {
    showRockyError("Rocky AI Explain failed", err);
  }
}

export async function aiTest(): Promise<void> {
  const modelName = activeModelName();

  try {
    const output = await runAiTest(modelName);
    vscode.window.showInformationMessage(
      output.trim() || "Tests generated and saved.",
    );
  } catch (err) {
    showRockyError("Rocky AI Test failed", err);
  }
}

function activeModelName(): string | undefined {
  const editor = vscode.window.activeTextEditor;
  if (!editor) return undefined;
  return path
    .basename(editor.document.fileName)
    .replace(/\.(rocky|sql)$/, "");
}
