/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/replay.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky replay <run_id|latest>`.
 *
 * Inspection-only surface over the state store's [`RunRecord`]: shows every model that ran, with the SQL hash, row counts, bytes, and timings captured at the time. Re-execution (with pinned inputs + content-addressed writes) is deferred to a follow-up when the Arc-1 storage path arrives.
 */
export interface ReplayOutput {
  command: string;
  config_hash: string;
  finished_at: string;
  models: ReplayModelOutput[];
  run_id: string;
  started_at: string;
  status: string;
  trigger: string;
  version: string;
  [k: string]: unknown;
}
/**
 * A single model execution inside a replayed run.
 */
export interface ReplayModelOutput {
  bytes_scanned?: number | null;
  bytes_written?: number | null;
  duration_ms: number;
  finished_at: string;
  model_name: string;
  rows_affected?: number | null;
  sql_hash: string;
  started_at: string;
  status: string;
  [k: string]: unknown;
}
