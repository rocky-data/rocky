/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/estimate.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky estimate`.
 *
 * `estimates` is empty when the project declares no models; `message` is populated in that case to explain why, mirroring [`OptimizeOutput`]. An unknown `--model` is an error, not an empty result.
 */
export interface EstimateOutput {
  command: string;
  estimates: ModelEstimate[];
  /**
   * Why `estimates` is empty, when it is. Absent whenever `estimates` is non-empty.
   *
   * Populated for both empty cases: no model matched (`--model` is refused outright, so this means the project declares none), and models matched but none produced an estimate because SQL generation or `EXPLAIN` failed for every one of them.
   *
   * Without this, an empty `estimates` array was the command's only signal that it had nothing to report — and the text path said `No models found.` while the JSON path said nothing at all, so the human was told and the machine was not (#1428).
   */
  message?: string | null;
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
