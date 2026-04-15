import * as vscode from "vscode";
import { runRockyJsonWithProgress, showRockyError } from "../rockyCli";
import type { HistoryResult, MetricsResult } from "../types/rockyJson";
import {
  ensureWorkspace,
  promptForInput,
  resolveModelName,
  showJsonInEditor,
} from "./ui";

export async function history(modelArg?: unknown): Promise<void> {
  if (!ensureWorkspace()) return;

  const model =
    resolveModelName(modelArg) ??
    (await promptForInput(
      "Filter to a specific model (leave empty for all runs)",
      { placeHolder: "e.g., customer_orders" },
    ));

  const args = ["history", "--output", "json"];
  if (model) args.push("--model", model);

  try {
    const result = await runRockyJsonWithProgress<HistoryResult>(
      "Loading run history...",
      args,
      { timeoutMs: 30000 },
    );

    const count = result.count ?? result.runs?.length ?? 0;
    vscode.window.showInformationMessage(
      `Rocky history: ${count} run${count === 1 ? "" : "s"} found.`,
    );
    await showJsonInEditor(JSON.stringify(result));
  } catch (err) {
    showRockyError("History failed", err);
  }
}

export async function metrics(modelArg?: unknown): Promise<void> {
  if (!ensureWorkspace()) return;

  const model =
    resolveModelName(modelArg) ??
    (await promptForInput("Model to inspect", {
      placeHolder: "e.g., fct_daily_revenue",
      required: true,
    }));
  if (!model) return;

  try {
    const result = await runRockyJsonWithProgress<MetricsResult>(
      `Loading metrics for ${model}...`,
      ["metrics", model, "--trend", "--alerts", "--output", "json"],
      { timeoutMs: 30000 },
    );

    const snapshots = result.count ?? result.snapshots?.length ?? 0;
    const alerts = result.alerts?.length ?? 0;
    const tone =
      alerts > 0
        ? `${snapshots} snapshot${snapshots === 1 ? "" : "s"}, ${alerts} alert${alerts === 1 ? "" : "s"}`
        : `${snapshots} snapshot${snapshots === 1 ? "" : "s"}`;
    if (alerts > 0) {
      vscode.window.showWarningMessage(`Rocky metrics: ${tone}.`);
    } else {
      vscode.window.showInformationMessage(`Rocky metrics: ${tone}.`);
    }
    await showJsonInEditor(JSON.stringify(result));
  } catch (err) {
    showRockyError("Metrics failed", err);
  }
}
