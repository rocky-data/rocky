/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/optimize.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky optimize`.
 *
 * `recommendations` is empty when no run history exists; `message` is populated in that case to explain why.
 */
export interface OptimizeOutput {
  command: string;
  /**
   * Hint pointing users to `rocky compile --output json` for inferred incrementality recommendations on `full_refresh` models. Only populated when the optimize command detects that compile-time analysis could provide additional optimization opportunities.
   */
  incrementality_note?: string | null;
  message?: string | null;
  recommendations: OptimizeRecommendation[];
  total_models_analyzed: number;
  version: string;
  [k: string]: unknown;
}
/**
 * One materialization-strategy recommendation. Mirrors `rocky_core::optimize::MaterializationCost` but lives in the CLI crate so we don't have to derive JsonSchema across the workspace.
 */
export interface OptimizeRecommendation {
  /**
   * Projected per-run compute cost (USD). Populated from `rocky_core::optimize::MaterializationCost::compute_cost_per_run` so Dagster's `checks.py` can surface it as metadata without re-deriving from config.
   */
  compute_cost_per_run: number;
  current_strategy: string;
  /**
   * How many downstream models depend on this one. Drives whether the recommendation favours table materialisation (many consumers) vs a view.
   */
  downstream_references: number;
  estimated_monthly_savings: number;
  model_name: string;
  reasoning: string;
  recommended_strategy: string;
  /**
   * Projected monthly storage cost (USD).
   */
  storage_cost_per_month: number;
  [k: string]: unknown;
}
