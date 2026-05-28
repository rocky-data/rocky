/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/profile.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky profile <model> [--column <col>]`.
 *
 * A per-column data profile (row / null / distinct counts, min / max, and the low-cardinality domain) for a model's target table, computed by a single aggregate query per column. DuckDB-only this release; a non-DuckDB target reports `unavailable` with an empty `columns` list rather than erroring.
 *
 * When the model's declared target isn't materialized (the agentic authoring loop pre-`rocky run`, or a replication-pipeline POC that doesn't run the transformation models), profile falls back to the model's first resolvable source table. The fallback is labelled via `profiled_table` (the table actually queried) and `fell_back_from` (the missing target), so consumers can surface "source preview, not model output" in the UI.
 */
export interface ProfileOutput {
  /**
   * One entry per profiled column.
   */
  columns: ProfileColumnStats[];
  command: string;
  /**
   * When set, profile fell back to a source because the model's declared target wasn't materialized; carries the declared-target FQN that was missing. `None` when the declared target was profiled directly.
   */
  fell_back_from?: string | null;
  model: string;
  /**
   * Fully-qualified table actually profiled (e.g. `staging.raw_orders` or `raw__orders.orders`). When `fell_back_from` is set, this differs from the model's declared target. Omitted when `unavailable` is set.
   */
  profiled_table?: string | null;
  /**
   * Set when profiling could not run (e.g. a non-DuckDB target this release).
   */
  unavailable?: string | null;
  version: string;
  [k: string]: unknown;
}
/**
 * Observed data profile for one column.
 */
export interface ProfileColumnStats {
  distinct: number;
  max?: string | null;
  min?: string | null;
  name: string;
  null_rate: number;
  nulls: number;
  /**
   * Observed low-cardinality domain (empty above the cardinality cap).
   */
  observed_values: string[];
  rows: number;
  /**
   * Inferred Rocky type name.
   */
  type: string;
  [k: string]: unknown;
}
