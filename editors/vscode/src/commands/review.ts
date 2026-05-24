import { readdir } from "fs/promises";
import * as path from "path";
import * as vscode from "vscode";
import { resolveProjectRoot } from "../config";
import { runRockyJsonWithProgress, showRockyError } from "../rockyCli";
import type { ReviewOutput } from "../types/generated/review";
import { openReviewPanel } from "../webviews/reviewPanel";
import { ensureWorkspace } from "./ui";

/**
 * Plan ids from a `.rocky/plans/` directory listing: `<id>.json` files,
 * excluding the `<id>.reviewed.json` approval markers. Pure — unit-tested.
 */
export function planIdsFromFilenames(files: string[]): string[] {
  return files
    .filter((f) => f.endsWith(".json") && !f.endsWith(".reviewed.json"))
    .map((f) => f.replace(/\.json$/, ""));
}

async function listPlanIds(root: string): Promise<string[]> {
  try {
    const files = await readdir(path.join(root, ".rocky", "plans"));
    return planIdsFromFilenames(files);
  } catch {
    return [];
  }
}

/**
 * `rocky.reviewPlan` — open the review/apply gate for a persisted plan.
 * Discovers plan ids under `.rocky/plans/` (falls back to a prompt), runs
 * `rocky review <id>` (dry run), and shows the interactive review panel.
 */
export async function reviewPlan(): Promise<void> {
  if (!ensureWorkspace()) return;
  const root = resolveProjectRoot();
  if (!root) return;

  const ids = await listPlanIds(root);
  let planId: string | undefined;
  if (ids.length === 0) {
    planId = await vscode.window.showInputBox({
      prompt: "Plan id to review",
      placeHolder: "64-char plan id from `rocky plan` / `compact` / `archive`",
    });
  } else {
    const pick = await vscode.window.showQuickPick(
      ids.map((id) => ({ label: `${id.slice(0, 12)}…`, description: id })),
      { placeHolder: "Select a plan to review" },
    );
    planId = pick?.description;
  }
  if (!planId) return;

  try {
    const review = await runRockyJsonWithProgress<ReviewOutput>(
      `Reviewing plan ${planId.slice(0, 8)}…`,
      ["review", planId, "--output", "json"],
      { cwd: root },
    );
    openReviewPanel(planId, review);
  } catch (err) {
    showRockyError("Review failed", err);
  }
}
