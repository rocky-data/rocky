/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/freshness.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * Outcome of one freshness check. Serialized `pass` / `warn` / `error` / `runtime_error`, matching dbt's `source freshness` statuses.
 *
 * - `pass`: within every threshold. - `warn`: older than `warn_after`, not older than `error_after`. - `error`: older than `error_after`. - `runtime_error`: the check could not be evaluated (invalid config, a failed query, or a value that does not read as a timestamp).
 */
export type FreshnessStatus = "pass" | "warn" | "error" | "runtime_error";

/**
 * JSON output for `rocky freshness`.
 */
export interface FreshnessOutput {
  /**
   * The instant every age was measured against.
   */
  checked_at: string;
  command: string;
  /**
   * One entry per model `[freshness]` block.
   */
  models: FreshnessCheckResult[];
  /**
   * One entry per declared source freshness block.
   */
  sources: FreshnessCheckResult[];
  summary: FreshnessSummary;
  version: string;
  [k: string]: unknown;
}
/**
 * One freshness check.
 */
export interface FreshnessCheckResult {
  /**
   * `checked_at - max_loaded_at`, in seconds. Negative when the newest load time is in the future (clock skew).
   */
  age_seconds?: number | null;
  error_after_seconds?: number | null;
  /**
   * The column read with `MAX(..)`. `None` when the measurement is the model's last successful build from the state store.
   */
  loaded_at_field?: string | null;
  /**
   * The newest load time seen. `None` when the table is empty, every value is NULL, the model was never built, or the check could not run.
   */
  max_loaded_at?: string | null;
  /**
   * Where `max_loaded_at` came from: `warehouse` or `state_store`.
   */
  measured_from: string;
  /**
   * Why the status is what it is, when that is not just the age.
   */
  message?: string | null;
  /**
   * Source `schema.table` (or `catalog.schema.table`), or the model name.
   */
  name: string;
  /**
   * The pipeline that declares the source or loads the model.
   */
  pipeline: string;
  /**
   * `pass`, `warn`, `error`, or `runtime_error`.
   */
  status: FreshnessStatus;
  /**
   * The table the measurement was taken from (`catalog.schema.table`).
   */
  table: string;
  warn_after_seconds?: number | null;
  [k: string]: unknown;
}
/**
 * Counts per status across `sources` and `models`.
 */
export interface FreshnessSummary {
  error: number;
  pass: number;
  runtime_error: number;
  warn: number;
  [k: string]: unknown;
}
