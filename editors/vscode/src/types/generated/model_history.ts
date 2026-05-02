/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/model_history.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky history --model <name>`.
 */
export interface ModelHistoryOutput {
  command: string;
  count: number;
  executions: ModelExecutionRecord[];
  model: string;
  /**
   * Rolling statistics over the most recent N successful executions. Present only when `--rolling-stats` is passed.
   */
  rolling_stats?: RollingStats | null;
  version: string;
  [k: string]: unknown;
}
/**
 * One model execution from the state store, mirroring `rocky_core::state::ModelExecution`.
 */
export interface ModelExecutionRecord {
  duration_ms: number;
  rows_affected?: number | null;
  sql_hash: string;
  started_at: string;
  status: string;
  [k: string]: unknown;
}
/**
 * Rolling statistics computed over the most recent N successful executions of a model. Populated by `rocky history --model <name> --rolling-stats`.
 *
 * Statistics use population standard deviation (divided by N, not N-1), so `std_dev` is exactly 0 when all samples are equal.
 */
export interface RollingStats {
  /**
   * Rolling statistics for the `duration_ms` dimension.
   */
  duration_ms: RollingDimension;
  /**
   * Composite health score in `[0.0, 1.0]`.
   *
   * Computed as `1.0 - clamp((max(|z_rows|, |z_duration|) - 2.0) / 4.0, 0.0, 1.0)`. A score of `1.0` means both z-scores are within 2σ of the mean; `0.0` means at least one z-score is 6σ or more.
   */
  health_score: number;
  /**
   * Rolling statistics for the `rows_affected` dimension. Computed only over executions where `rows_affected` is not null.
   */
  rows_affected: RollingDimension;
  /**
   * Actual number of successful executions used (≤ window; may be smaller when model history is shorter than the requested window).
   */
  samples: number;
  /**
   * Maximum number of executions requested for the rolling window.
   */
  window: number;
  [k: string]: unknown;
}
/**
 * Per-dimension rolling statistics (mean, std dev, latest z-score).
 */
export interface RollingDimension {
  /**
   * Z-score of the most recent execution relative to the window.
   *
   * `None` when fewer than 2 samples are available or when `std_dev` is exactly 0 (all samples are equal — no meaningful deviation).
   */
  latest_z_score?: number | null;
  /**
   * Population mean over the sample window.
   */
  mean: number;
  /**
   * Population standard deviation (÷N) over the sample window.
   */
  std_dev: number;
  [k: string]: unknown;
}
