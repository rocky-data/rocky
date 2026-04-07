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
  production_count: number;
  production_table: string;
  row_count_diff_pct: number;
  row_count_match: boolean;
  schema_diffs: string[];
  schema_match: boolean;
  shadow_count: number;
  shadow_table: string;
  verdict: string;
  [k: string]: unknown;
}
