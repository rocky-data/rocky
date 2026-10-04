import * as vscode from "vscode";
import {
  RockyCliError,
  runRockyJsonWithProgress,
  runRockyWithProgress,
  showRockyError,
} from "../rockyCli";
import type { FreshnessOutput } from "../types/generated";
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

/**
 * `rocky.freshness` — check declared source and model freshness against the
 * warehouse. `rocky freshness` exits 1 when any check is `error` or
 * `runtime_error` but still prints the full JSON report, so that report is
 * shown either way.
 */
export async function freshness(): Promise<void> {
  if (!ensureWorkspace()) return;
  let stdout: string;
  try {
    ({ stdout } = await runRockyWithProgress(
      "Checking source freshness...",
      ["freshness", "--output", "json"],
      { timeoutMs: 120000 },
    ));
  } catch (err) {
    if (err instanceof RockyCliError && err.exitCode === 1 && err.stdout) {
      stdout = err.stdout;
    } else {
      showRockyError("Freshness check failed", err);
      return;
    }
  }
  let report: FreshnessOutput;
  try {
    report = JSON.parse(stdout) as FreshnessOutput;
  } catch {
    showRockyError("Freshness check failed", new Error("output was not JSON"));
    return;
  }
  await showJsonInEditor(stdout);
  const s = report.summary;
  const line = `Freshness: ${s.pass} pass, ${s.warn} warn, ${s.error} error, ${s.runtime_error} runtime_error`;
  if (s.error > 0 || s.runtime_error > 0) {
    void vscode.window.showErrorMessage(line);
  } else if (s.warn > 0) {
    void vscode.window.showWarningMessage(line);
  } else {
    void vscode.window.showInformationMessage(line);
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

