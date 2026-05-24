import { resolveProjectRoot } from "../config";
import { runRockyJsonWithProgress, showRockyError } from "../rockyCli";
import type { LineageDiffOutput } from "../types/generated/lineage_diff";
import { showLineageDiff } from "../webviews/lineageDiffPanel";
import { ensureWorkspace } from "./ui";

/**
 * `rocky.lineageDiff` — column-level blast radius of the working tree vs the
 * base ref (`main`). Read-only (git + compile, no warehouse). The PR-review
 * companion to the branches view.
 */
export async function lineageDiff(): Promise<void> {
  if (!ensureWorkspace()) return;
  try {
    const result = await runRockyJsonWithProgress<LineageDiffOutput>(
      "Diffing column lineage vs main…",
      ["lineage-diff", "--output", "json"],
      { cwd: resolveProjectRoot() },
    );
    showLineageDiff(result);
  } catch (err) {
    showRockyError("Lineage diff failed", err);
  }
}
