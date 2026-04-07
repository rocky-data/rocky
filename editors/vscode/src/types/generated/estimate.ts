/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/estimate.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky estimate`.
 */
export interface EstimateOutput {
  command: string;
  estimates: ModelEstimate[];
  total_models: number;
  version: string;
  [k: string]: unknown;
}
/**
 * Cost estimate for a single model.
 */
export interface ModelEstimate {
  /**
   * Estimated bytes that would be scanned (if available).
   */
  estimated_bytes_scanned?: number | null;
  /**
   * Estimated number of rows (if available).
   */
  estimated_rows?: number | null;
  model_name: string;
  /**
   * Raw EXPLAIN output from the warehouse.
   */
  raw_explain: string;
  [k: string]: unknown;
}
