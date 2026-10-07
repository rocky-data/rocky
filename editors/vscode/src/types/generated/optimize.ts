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
  message?: string | null;
  recommendations: OptimizeRecommendation[];
  /**
   * Which runs the recommendations were computed from. Shadow and branch runs are left out; runs with no recorded scope are counted, so history from before #2200 still informs the averages (#2201).
   */
  run_scope?: ProductionRunScope;
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
/**
 * Which runs a report about production counted (#2201).
 *
 * Shadow and branch runs are never counted. Runs recorded before runs carried a scope are counted or not per report, and `unrecorded_runs_counted` says which.
 */
export interface ProductionRunScope {
  /**
   * Shadow and branch runs the report left out.
   */
  excluded_runs: number;
  /**
   * Runs recorded as production that the report read.
   */
  production_runs: number;
  /**
   * Runs with no recorded scope that the report read.
   */
  unrecorded_runs: number;
  /**
   * `true` when runs with no recorded scope count as production in this report. Their write target is unknown.
   */
  unrecorded_runs_counted: boolean;
  [k: string]: unknown;
}
