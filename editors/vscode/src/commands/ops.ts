import * as vscode from "vscode";
import {
  RockyCliError,
  runRockyJsonWithProgress,
  runRockyWithProgress,
  showRockyError,
} from "../rockyCli";
import type { DoctorResult } from "../types/rockyJson";
import { showDoctorResult } from "../webviews/doctor";
import { ensureWorkspace, resolveModelName, showJsonInEditor } from "./ui";

export async function doctor(): Promise<void> {
  // Doctor checks binary install + config — it runs without a workspace
  // and reports `critical` for the missing rocky.toml. The Get Started
  // welcome links here precisely to verify installation, so don't gate it.
  try {
    const result = await runRockyJsonWithProgress<DoctorResult>(
      "Running Rocky doctor...",
      ["doctor", "--output", "json"],
      { timeoutMs: 30000 },
    );
    showDoctorResult(result);
  } catch (err) {
    // `rocky doctor` exits 2 when any check is `critical` (see
    // engine/crates/rocky-cli/src/commands/doctor.rs). The JSON payload on
    // stdout is still valid — surface it so users can see what's wrong
    // instead of a generic "Doctor failed" toast.
    if (err instanceof RockyCliError && err.exitCode === 2 && err.stdout) {
      try {
        const result = JSON.parse(err.stdout) as DoctorResult;
        showDoctorResult(result);
        return;
      } catch {
        // Fall through to the generic error path below.
      }
    }
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

