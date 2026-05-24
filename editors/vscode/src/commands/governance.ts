import { resolveProjectRoot } from "../config";
import { runRockyJsonWithProgress, showRockyError } from "../rockyCli";
import type { ComplianceOutput } from "../types/generated/compliance";
import { showCompliance } from "../webviews/compliancePanel";
import { ensureWorkspace } from "./ui";

/**
 * `rocky.compliance` — run the static governance check and render the rollup.
 *
 * `rocky compliance` is a purely static resolver (classification sidecars vs
 * `[mask]` policy, no warehouse calls), so this is cheap to run on demand.
 */
export async function compliance(): Promise<void> {
  if (!ensureWorkspace()) return;
  try {
    const result = await runRockyJsonWithProgress<ComplianceOutput>(
      "Checking governance compliance…",
      ["compliance", "--output", "json"],
      { cwd: resolveProjectRoot() },
    );
    showCompliance(result);
  } catch (err) {
    showRockyError("Compliance check failed", err);
  }
}
