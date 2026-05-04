import * as vscode from "vscode";
import { runRockyJsonWithProgress, showRockyError } from "../rockyCli";
import type {
  ApproveOutput,
  BranchPromoteOutput,
} from "../types/generated";
import {
  confirmAction,
  ensureWorkspace,
  promptForInput,
  showJsonInEditor,
} from "./ui";

const BRANCH_NAME_PLACEHOLDER = "e.g., fix-price";
const FILTER_PLACEHOLDER = "e.g., client=acme";

export async function branchApprove(): Promise<void> {
  if (!ensureWorkspace()) return;

  const name = await promptForInput("Branch name to approve", {
    placeHolder: BRANCH_NAME_PLACEHOLDER,
    required: true,
  });
  if (!name) return;

  const message = await promptForInput("Approval message (optional)", {
    placeHolder: "free-form note recorded on the artifact",
  });

  const args = ["branch", "approve", name, "--output", "json"];
  if (message) args.push("--message", message);

  try {
    const result = await runRockyJsonWithProgress<ApproveOutput>(
      `Signing approval for branch '${name}'...`,
      args,
      { timeoutMs: 60000 },
    );

    vscode.window.showInformationMessage(
      `Approved '${result.artifact.branch}' (signer ${result.artifact.approver.email}) → ${result.artifact_path}`,
    );
    await showJsonInEditor(JSON.stringify(result));
  } catch (err) {
    showRockyError("Branch approve failed", err);
  }
}

export async function branchPromote(): Promise<void> {
  if (!ensureWorkspace()) return;

  const name = await promptForInput("Branch name to promote", {
    placeHolder: BRANCH_NAME_PLACEHOLDER,
    required: true,
  });
  if (!name) return;

  const filter = await promptForInput("Filter (optional, key=value)", {
    placeHolder: FILTER_PLACEHOLDER,
  });

  const confirmed = await confirmAction(
    `Promote branch '${name}' to production?`,
    "Promote",
    "This dispatches CREATE OR REPLACE TABLE per managed target.",
  );
  if (!confirmed) return;

  const args = ["branch", "promote", name, "--output", "json"];
  if (filter) args.push("--filter", filter);

  try {
    const result = await runRockyJsonWithProgress<BranchPromoteOutput>(
      `Promoting branch '${name}'...`,
      args,
      { timeoutMs: 0 },
    );

    const targetCount = result.targets?.length ?? 0;
    const summary = result.success
      ? `Promoted '${result.branch}' (${targetCount} target${targetCount === 1 ? "" : "s"})`
      : `Promote failed for '${result.branch}' after ${targetCount} target${targetCount === 1 ? "" : "s"}`;

    if (result.success) {
      vscode.window.showInformationMessage(summary);
    } else {
      vscode.window.showErrorMessage(summary);
    }
    await showJsonInEditor(JSON.stringify(result));
  } catch (err) {
    showRockyError("Branch promote failed", err);
  }
}
