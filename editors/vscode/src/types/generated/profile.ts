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
 */
export interface ProfileOutput {
  /**
   * One entry per profiled column.
   */
  columns: ProfileColumnStats[];
  command: string;
  model: string;
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
