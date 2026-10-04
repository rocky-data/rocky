/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/ai_sync.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky ai-sync`.
 */
export interface AiSyncOutput {
  command: string;
  proposals: AiSyncProposal[];
  version: string;
  [k: string]: unknown;
}
export interface AiSyncProposal {
  diff: string;
  intent: string;
  model: string;
  proposed_source: string;
  /**
   * Whether a stored upstream-schema baseline existed for this model. `false` on the first sync of a model: the proposal follows declared intent only, and the current upstream schemas become the baseline. Optional on the wire: an engine older than this field never had one.
   */
  upstream_baseline_found?: boolean;
  /**
   * Upstream column changes since the baseline, one human-readable line each. Empty when there is no baseline or nothing changed.
   */
  upstream_changes?: string[];
  [k: string]: unknown;
}
