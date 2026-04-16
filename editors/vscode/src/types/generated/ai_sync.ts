/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/ai_sync.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky ai sync`.
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
  [k: string]: unknown;
}
