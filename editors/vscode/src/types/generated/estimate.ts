/* eslint-disable */

/**
 * JSON output for `rocky estimate`.
 */
export interface EstimateOutput {
  command: string;
  estimates: ModelEstimate[];
  /**
   * Total estimated cost in USD across all models. `None` when no individual model produced a cost estimate.
   */
  total_estimated_cost_usd?: number | null;
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
   * Estimated compute cost in USD (derived from warehouse pricing model).
   */
  estimated_cost_usd?: number | null;
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
