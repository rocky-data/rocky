import * as vscode from "vscode";
import {
  runRockyJsonWithProgress,
  runRockyWithProgress,
  showRockyError,
} from "../rockyCli";
import type { DoctorResult } from "../types/rockyJson";
import { showDoctorResult } from "../webviews/doctor";
import { ensureWorkspace, resolveModelName, showJsonInEditor } from "./ui";

export async function doctor(): Promise<void> {
  if (!ensureWorkspace()) return;

  try {
    const result = await runRockyJsonWithProgress<DoctorResult>(
      "Running Rocky doctor...",
      ["doctor", "--output", "json"],
      { timeoutMs: 30000 },
    );
    showDoctorResult(result);
  } catch (err) {
    showRockyError("Doctor failed", err);
  }
}

export async function optimize(modelArg?: unknown): Promise<void> {
  if (!ensureWorkspace()) return;

  const model =
    resolveModelName(modelArg) ??
    (await vscode.window.showInputBox({
      prompt: "Model to analyze (leave empty for all)",
      placeHolder: "e.g., customer_orders",
    }));

  const args = ["optimize", "--output", "json"];
  if (model) args.push("--model", model);

  try {
    const { stdout } = await runRockyWithProgress(
      "Analyzing costs...",
      args,
      { timeoutMs: 60000 },
    );
    await showJsonInEditor(stdout);
  } catch (err) {
    showRockyError("Optimize failed", err);
  }
}

