import * as vscode from "vscode";
import { runRockyJsonWithProgress, showRockyError } from "../rockyCli";
import { getOutputChannel } from "../output";
import type { TestResult } from "../types/rockyJson";
import { ensureWorkspace, promptForInput } from "./ui";

/**
 * Phase 2: simple shell-out that surfaces a pass/fail toast and writes failure
 * detail to the output channel. Phase 3 will replace this with a real Test
 * Explorer integration backed by `vscode.tests.createTestController`.
 */
export async function test(modelArg?: string): Promise<void> {
  if (!ensureWorkspace()) return;

  const model =
    modelArg ??
    (await promptForInput(
      "Filter to a specific model (leave empty for all)",
      { placeHolder: "e.g., customer_orders" },
    ));

  const args = ["test", "--output", "json"];
  if (model) args.push("--model", model);

  try {
    const result = await runRockyJsonWithProgress<TestResult>(
      "Running Rocky tests...",
      args,
      { timeoutMs: 120000 },
    );

    const total = result.total ?? 0;
    const passed = result.passed ?? 0;
    const failed = result.failed ?? 0;
    const summary = `Rocky test: ${passed}/${total} passed`;

    if (failed > 0) {
      const channel = getOutputChannel();
      channel.appendLine("");
      channel.appendLine(`Rocky test failures (${failed}):`);
      for (const failure of result.failures ?? []) {
        channel.appendLine(`  ✗ ${failure.name}: ${failure.error}`);
      }
      vscode.window
        .showErrorMessage(`${summary} — ${failed} failed`, "Show Logs")
        .then((choice) => {
          if (choice === "Show Logs") channel.show();
        });
    } else {
      vscode.window.showInformationMessage(summary);
    }
  } catch (err) {
    showRockyError("Test failed", err);
  }
}
