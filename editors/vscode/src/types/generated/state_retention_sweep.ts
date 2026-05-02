/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/state_retention_sweep.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky state retention sweep`.
 *
 * Mirrors [`rocky_core::retention::SweepReport`] with the version/command envelope every CLI JSON output carries. `dry_run = true` reports counts that *would* result without touching the state store.
 */
export interface RetentionSweepOutput {
  /**
   * Quality snapshots (`quality_history`) deleted, or that would be deleted.
   */
  audit_deleted: number;
  /**
   * Quality snapshots remaining.
   */
  audit_kept: number;
  command: string;
  /**
   * Domains that were considered in this sweep, as the canonical lowercase strings (`"history"`, `"lineage"`, `"audit"`).
   */
  domains: string[];
  /**
   * `true` when `--dry-run` was set; the state store was left untouched.
   */
  dry_run: boolean;
  /**
   * Wall-clock duration of the sweep in milliseconds.
   */
  duration_ms: number;
  /**
   * DAG snapshots (`dag_snapshots`) deleted, or that would be deleted.
   */
  lineage_deleted: number;
  /**
   * DAG snapshots remaining.
   */
  lineage_kept: number;
  /**
   * Configured retention window in days at the time of the sweep.
   */
  max_age_days: number;
  /**
   * Configured per-domain floor at the time of the sweep.
   */
  min_runs_kept: number;
  /**
   * Run records (`run_history`) deleted, or that would be deleted in dry-run mode.
   */
  runs_deleted: number;
  /**
   * Run records remaining after the sweep.
   */
  runs_kept: number;
  version: string;
  [k: string]: unknown;
}
