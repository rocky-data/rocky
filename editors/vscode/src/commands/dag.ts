import { resolveProjectRoot } from "../config";
import { runRockyJsonWithProgress, showRockyError } from "../rockyCli";
import type { DagOutput } from "../types/generated/dag";
import { showExecutionPlan } from "../webviews/executionPlanPanel";
import { ensureWorkspace } from "./ui";

/**
 * `rocky.dag` — show the pipeline's execution plan (topological waves).
 *
 * Purely static (compile + topo sort, no warehouse calls). Distinct from the
 * lineage graph: this is the *run schedule* — which models execute in parallel
 * in which order.
 */
export async function showDag(): Promise<void> {
  if (!ensureWorkspace()) return;
  try {
    const result = await runRockyJsonWithProgress<DagOutput>(
      "Building execution plan…",
      ["dag", "--output", "json"],
      { cwd: resolveProjectRoot() },
    );
    showExecutionPlan(result);
  } catch (err) {
    showRockyError("Execution plan failed", err);
  }
}
