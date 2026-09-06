/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/snapshot.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * Output for `rocky snapshot --output json`.
 */
export interface SnapshotOutput {
  command: string;
  dry_run: boolean;
  duration_ms: number;
  pipeline: string;
  source: string;
  steps: SnapshotStepOutput[];
  steps_ok: number;
  steps_total: number;
  target: string;
  version: string;
  [k: string]: unknown;
}
/**
 * A single SQL step within a snapshot run.
 */
export interface SnapshotStepOutput {
  duration_ms: number;
  error?: string | null;
  sql: string;
  status: string;
  step: string;
  [k: string]: unknown;
}
