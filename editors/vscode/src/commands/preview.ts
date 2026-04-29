import * as vscode from "vscode";
import { runRockyJsonWithProgress, showRockyError } from "../rockyCli";
import type {
  PreviewCostOutput,
  PreviewCreateOutput,
  PreviewDiffOutput,
} from "../types/generated";
import { ensureWorkspace, promptForInput, showJsonInEditor } from "./ui";

const BASE_PLACEHOLDER = "e.g., main";
const BRANCH_PLACEHOLDER = "e.g., preview-fix-price";

export async function previewCreate(): Promise<void> {
  if (!ensureWorkspace()) return;

  const base = await promptForInput("Base ref to diff against", {
    placeHolder: BASE_PLACEHOLDER,
    value: "main",
    required: true,
  });
  if (!base) return;

  const name = await promptForInput("Preview branch name (optional)", {
    placeHolder: BRANCH_PLACEHOLDER,
  });

  const args = ["preview", "create", "--base", base, "--output", "json"];
  if (name) args.push("--name", name);

  try {
    const result = await runRockyJsonWithProgress<PreviewCreateOutput>(
      `Creating preview branch (base=${base})...`,
      args,
      { timeoutMs: 0 },
    );

    const pruned = result.prune_set?.length ?? 0;
    const copied = result.copy_set?.length ?? 0;
    const skipped = result.skipped_set?.length ?? 0;
    const summary = `Rocky preview create: ${pruned} re-run, ${copied} copied, ${skipped} skipped (branch=${result.branch_name})`;

    if (result.run_status === "failed") {
      vscode.window.showErrorMessage(summary);
    } else {
      vscode.window.showInformationMessage(summary);
    }
    await showJsonInEditor(JSON.stringify(result));
  } catch (err) {
    showRockyError("Preview create failed", err);
  }
}

export async function previewDiff(): Promise<void> {
  if (!ensureWorkspace()) return;

  const name = await promptForInput("Preview branch name", {
    placeHolder: BRANCH_PLACEHOLDER,
    required: true,
  });
  if (!name) return;

  const base = await promptForInput("Base ref (optional)", {
    placeHolder: BASE_PLACEHOLDER,
    value: "main",
  });

  const args = ["preview", "diff", "--name", name, "--output", "json"];
  if (base) args.push("--base", base);

  try {
    const result = await runRockyJsonWithProgress<PreviewDiffOutput>(
      `Diffing preview branch '${name}'...`,
      args,
      { timeoutMs: 0 },
    );

    const summary = result.summary;
    const message = `Rocky preview diff: ${summary?.models_with_changes ?? 0} changed, ${summary?.models_unchanged ?? 0} unchanged, +${summary?.total_rows_added ?? 0}/-${summary?.total_rows_removed ?? 0}/~${summary?.total_rows_changed ?? 0} rows`;

    if (summary?.any_coverage_warning) {
      vscode.window.showWarningMessage(`${message} (coverage warning)`);
    } else {
      vscode.window.showInformationMessage(message);
    }
    await showJsonInEditor(JSON.stringify(result));
  } catch (err) {
    showRockyError("Preview diff failed", err);
  }
}

export async function previewCost(): Promise<void> {
  if (!ensureWorkspace()) return;

  const name = await promptForInput("Preview branch name", {
    placeHolder: BRANCH_PLACEHOLDER,
    required: true,
  });
  if (!name) return;

  const base = await promptForInput("Base ref (optional)", {
    placeHolder: BASE_PLACEHOLDER,
    value: "main",
  });

  const args = ["preview", "cost", "--name", name, "--output", "json"];
  if (base) args.push("--base", base);

  try {
    const result = await runRockyJsonWithProgress<PreviewCostOutput>(
      `Estimating cost delta for '${name}'...`,
      args,
      { timeoutMs: 60000 },
    );

    const summary = result.summary;
    const delta = summary?.delta_usd;
    const deltaStr =
      delta == null ? "n/a" : `${delta >= 0 ? "+" : ""}$${delta.toFixed(4)}`;
    const message = `Rocky preview cost: delta ${deltaStr}, ${summary?.models_skipped_via_copy ?? 0} models skipped via copy`;
    vscode.window.showInformationMessage(message);
    await showJsonInEditor(JSON.stringify(result));
  } catch (err) {
    showRockyError("Preview cost failed", err);
  }
}
