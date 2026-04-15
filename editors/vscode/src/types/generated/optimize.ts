/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/optimize.schema.json
 * Run just codegen from the monorepo root to regenerate.
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
  current_strategy: string;
  estimated_monthly_savings: number;
  model_name: string;
  reasoning: string;
  recommended_strategy: string;
  [k: string]: unknown;
}
