import { resolveProjectRoot } from "../config";
import { runRockyJsonWithProgress, showRockyError } from "../rockyCli";
import type { TraceOutput } from "../types/generated/trace";
import { showRunTrace } from "../webviews/runTracePanel";
import { ensureWorkspace } from "./ui";

/**
 * `rocky.trace` — show the execution timeline of the latest run.
 *
 * Traces `latest` (the most recent run in the state store) and renders a gantt
 * of per-model timing + parallel lanes. Read-only — reads run history, runs
 * nothing.
 */
export async function trace(): Promise<void> {
  if (!ensureWorkspace()) return;
  try {
    const result = await runRockyJsonWithProgress<TraceOutput>(
      "Tracing latest run…",
      ["trace", "latest", "--output", "json"],
      { cwd: resolveProjectRoot() },
    );
    showRunTrace(result);
  } catch (err) {
    showRockyError("Run trace failed", err);
  }
}
