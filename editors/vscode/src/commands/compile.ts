import * as vscode from "vscode";
import {
  runRocky,
  runRockyJsonWithProgress,
  showRockyError,
} from "../rockyCli";
import type { CiResult, CompileResult } from "../types/rockyJson";
import { ensureWorkspace, showJsonInEditor } from "./ui";

export async function compile(): Promise<void> {
  if (!ensureWorkspace()) return;

  try {
    const result = await runRockyJsonWithProgress<CompileResult>(
      "Compiling Rocky models...",
      ["compile", "--output", "json"],
      { timeoutMs: 120000 },
    );

    const models = result.models ?? 0;
    const diags = result.diagnostics ?? [];
    const errors = diags.filter((d) => d.severity === "Error").length;
    const warnings = diags.filter((d) => d.severity === "Warning").length;

    if (errors > 0) {
      vscode.window.showErrorMessage(
        `Rocky compile: ${errors} error${errors === 1 ? "" : "s"}, ${warnings} warning${warnings === 1 ? "" : "s"} across ${models} model${models === 1 ? "" : "s"}.`,
      );
      await showJsonInEditor(JSON.stringify(result));
    } else if (warnings > 0) {
      vscode.window.showWarningMessage(
        `Rocky compile: ${warnings} warning${warnings === 1 ? "" : "s"} across ${models} model${models === 1 ? "" : "s"}.`,
      );
    } else {
      vscode.window.showInformationMessage(
        `Rocky compile: ${models} model${models === 1 ? "" : "s"} compiled cleanly.`,
      );
    }
  } catch (err) {
    showRockyError("Compile failed", err);
  }
}

export async function validate(): Promise<void> {
  if (!ensureWorkspace()) return;

  try {
    const { stdout } = await runRocky(["validate"], { timeoutMs: 30000 });
    vscode.window.showInformationMessage(
      stdout.trim() || "Rocky validate: ok.",
    );
  } catch (err) {
    showRockyError("Validate failed", err);
  }
}

export async function ci(): Promise<void> {
  if (!ensureWorkspace()) return;

  try {
    const result = await runRockyJsonWithProgress<CiResult>(
      "Running Rocky CI (compile + tests)...",
      ["ci", "--output", "json"],
      { timeoutMs: 300000 },
    );

    // Phase 2: CiOutput now has flat fields rather than nested compile/tests
    // sub-objects. Status is derived from compile_ok + tests_ok.
    const passed = result.compile_ok && result.tests_ok;
    const status = passed ? "pass" : "fail";
    const compileModels = result.models_compiled ?? 0;
    const testsPassed = result.tests_passed ?? 0;
    const testsTotal = (result.tests_passed ?? 0) + (result.tests_failed ?? 0);
    const summary = `Rocky CI: ${status} — ${compileModels} models compiled, ${testsPassed}/${testsTotal} tests passed`;

    if (!passed) {
      vscode.window.showErrorMessage(summary);
      await showJsonInEditor(JSON.stringify(result));
    } else {
      vscode.window.showInformationMessage(summary);
    }
  } catch (err) {
    showRockyError("CI failed", err);
  }
}
