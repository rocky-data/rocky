import * as path from "path";
import * as vscode from "vscode";
import { runRockyWithProgress, showRockyError } from "../rockyCli";

export async function aiGenerate(): Promise<void> {
  const intent = await vscode.window.showInputBox({
    prompt: "Describe the model you want to create",
    placeHolder:
      "e.g., monthly revenue per customer from the orders table, filtered to 2024",
  });
  if (!intent) return;

  try {
    const { stdout } = await runRockyWithProgress("Generating model...", [
      "ai",
      intent,
      "--format",
      "rocky",
    ]);
    const doc = await vscode.workspace.openTextDocument({
      content: stdout,
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

  const args = ["ai-sync", "--models", "models"];
  if (model) args.push("--model", model);

  try {
    const { stdout } = await runRockyWithProgress("Syncing models...", args);
    vscode.window.showInformationMessage(
      stdout.trim() || "No models need syncing.",
    );
  } catch (err) {
    showRockyError("Rocky AI Sync failed", err);
  }
}

export async function aiExplain(): Promise<void> {
  const modelName = activeModelName();
  const args = ["ai-explain", "--models", "models", "--save"];
  if (modelName) {
    args.push(modelName);
  } else {
    args.push("--all");
  }

  try {
    const { stdout } = await runRockyWithProgress("Generating intent...", args);
    vscode.window.showInformationMessage(
      stdout.trim() || "Intent generated and saved.",
    );
  } catch (err) {
    showRockyError("Rocky AI Explain failed", err);
  }
}

export async function aiTest(): Promise<void> {
  const modelName = activeModelName();
  const args = ["ai-test", "--models", "models", "--save"];
  if (modelName) {
    args.push(modelName);
  } else {
    args.push("--all");
  }

  try {
    const { stdout } = await runRockyWithProgress("Generating tests...", args);
    vscode.window.showInformationMessage(
      stdout.trim() || "Tests generated and saved.",
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
