/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/compare.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky compare`.
 */
export interface CompareOutput {
  command: string;
  filter: string;
  overall_verdict: string;
  results: TableCompareResult[];
  tables_compared: number;
  tables_failed: number;
  tables_passed: number;
  tables_warned: number;
  version: string;
  [k: string]: unknown;
}
export interface TableCompareResult {
  /**
   * Null when the warehouse count could not be read.
   */
  production_count?: number | null;
  production_table: string;
  /**
   * Read errors for an `error` row, or threshold reasons for `warn`/`fail`. Empty for `pass`.
   */
  reasons: string[];
  /**
   * Null unless both counts were read.
   */
  row_count_diff_pct?: number | null;
  row_count_match: boolean;
  schema_diffs: string[];
  schema_match: boolean;
  /**
   * Null when the warehouse count could not be read.
   */
  shadow_count?: number | null;
  shadow_table: string;
  verdict: string;
  [k: string]: unknown;
}
