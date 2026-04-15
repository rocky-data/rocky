/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/history.schema.json
 * Run just codegen from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky history` (all runs view).
 *
 * When invoked with `--model <name>`, the dispatch returns `ModelHistoryOutput` instead. The two shapes share the version/command header but differ in their primary collection field, which is why they are separate types rather than one enum.
 */
export interface HistoryOutput {
  command: string;
  count: number;
  runs: RunHistoryRecord[];
  version: string;
  [k: string]: unknown;
}
/**
 * One run from the state store, mirroring `rocky_core::state::RunRecord` with serializable field types (no enums or non-JSON-friendly Rust types).
 */
export interface RunHistoryRecord {
  /**
   * Duration in milliseconds (`finished_at - started_at`).
   */
  duration_ms: number;
  /**
   * Per-model execution details for this run.
   */
  models: RunModelRecord[];
  models_executed: number;
  run_id: string;
  started_at: string;
  status: string;
  trigger: string;
  [k: string]: unknown;
}
/**
 * Per-model execution record embedded in [`RunHistoryRecord`].
 */
export interface RunModelRecord {
  duration_ms: number;
  model_name: string;
  rows_affected?: number | null;
  status: string;
  [k: string]: unknown;
}
