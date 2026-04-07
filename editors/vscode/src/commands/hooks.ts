import * as vscode from "vscode";
import {
  runRocky,
  runRockyJson,
  showRockyError,
} from "../rockyCli";
import type { HooksListResult } from "../types/rockyJson";
import { ensureWorkspace, showJsonInEditor } from "./ui";

const HOOK_EVENTS = [
  "pipeline_start",
  "discover_complete",
  "compile_complete",
  "pipeline_complete",
  "pipeline_error",
  "before_materialize",
  "after_materialize",
  "materialize_error",
  "before_model_run",
  "after_model_run",
  "model_error",
  "check_result",
  "drift_detected",
  "anomaly_detected",
  "state_synced",
];

export async function hooksTest(): Promise<void> {
  const event = await vscode.window.showQuickPick(HOOK_EVENTS, {
    placeHolder: "Select a hook event to test",
  });
  if (!event) return;

  try {
    const { stdout } = await runRocky(["hooks", "test", event]);
    vscode.window.showInformationMessage(
      stdout.trim() || `Hook '${event}' test completed.`,
    );
  } catch (err) {
    showRockyError("Hook test failed", err);
  }
}

export async function hooksList(): Promise<void> {
  if (!ensureWorkspace()) return;

  try {
    const result = await runRockyJson<HooksListResult>(["hooks", "list"]);
    const total = result.total ?? result.hooks?.length ?? 0;
    vscode.window.showInformationMessage(
      `Rocky hooks: ${total} configured.`,
    );
    await showJsonInEditor(JSON.stringify(result));
  } catch (err) {
    showRockyError("Hooks list failed", err);
  }
}
