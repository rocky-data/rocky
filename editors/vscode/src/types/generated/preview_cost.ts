/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/preview_cost.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky preview cost`.
 *
 * Diff layer over `rocky cost latest`'s machinery. Per-model deltas are computed by looking up the latest base-schema `RunRecord` and the branch run's `RunRecord` and subtracting field-by-field.
 *
 * `models_skipped_via_copy` is the prune-set complement: copied models did not run on the branch, so their delta is `None` and their savings accrue to `savings_from_copy_usd`.
 */
export interface PreviewCostOutput {
  /**
   * `RunRecord` id for the latest base-schema run that the branch is being compared against. `None` when no base run exists.
   */
  base_run_id?: string | null;
  branch_name: string;
  /**
   * `RunRecord` id for the branch execution. Empty when the branch run was a no-op (every changed model pruned to zero downstream).
   */
  branch_run_id: string;
  command: string;
  markdown: string;
  per_model: PreviewModelCostDelta[];
  /**
   * Budget breaches projected against the branch totals — populated only when the project declares a `[budget]` block. Lets a PR reviewer (and a CI gate) see "this PR would breach the run-level `max_usd` / `max_duration_ms` / `max_bytes_scanned` if merged" before the merge actually happens. Empty when no budget is configured or the projected totals stay within every limit. Mirrors the `RunOutput.budget_breaches` shape so the same downstream consumers (PR-comment templates, JSON listeners) can process both with one code path.
   */
  projected_budget_breaches?: BudgetBreachOutput[];
  summary: PreviewCostSummary;
  version: string;
  [k: string]: unknown;
}
/**
 * Per-model cost delta.
 */
export interface PreviewModelCostDelta {
  base_bytes_scanned?: number | null;
  base_cost_usd?: number | null;
  base_duration_ms: number;
  branch_bytes_scanned?: number | null;
  branch_cost_usd?: number | null;
  branch_duration_ms: number;
  delta_usd?: number | null;
  model_name: string;
  /**
   * `true` if this model was copied from base instead of re-run. When true, `branch_*` fields are `None` / `0` and the savings for this row roll into [`PreviewCostSummary::savings_from_copy_usd`].
   */
  skipped_via_copy: boolean;
  [k: string]: unknown;
}
/**
 * One budget breach surfaced on [`RunOutput::budget_breaches`].
 *
 * Kept as a CLI-side struct (rather than re-using [`rocky_core::config::BudgetBreach`]) so the JSON schema lives alongside the other `rocky run` output types. The fields mirror `BudgetBreach` one-to-one.
 */
export interface BudgetBreachOutput {
  actual: number;
  limit: number;
  /**
   * Which limit was breached: `"max_usd"`, `"max_duration_ms"`, or `"max_bytes_scanned"`.
   */
  limit_type: string;
  [k: string]: unknown;
}
/**
 * Aggregate cost rollup for [`PreviewCostOutput`].
 */
export interface PreviewCostSummary {
  /**
   * `total_branch_cost_usd - total_base_cost_usd`. Positive = the PR costs more to run than `main`.
   */
  delta_usd?: number | null;
  /**
   * Number of models that did not run on the branch because they were copied from base. Their savings show up below.
   */
  models_skipped_via_copy: number;
  /**
   * Sum of `base_cost_usd` for every copied model — the cost the PR avoided by copying instead of re-running. `None` when no base costs are available for the copied models.
   */
  savings_from_copy_usd?: number | null;
  /**
   * Sum of every per-model `base_cost_usd` (limited to models in the prune set — copied models contribute 0 here, accounted for in `savings_from_copy_usd`).
   */
  total_base_cost_usd?: number | null;
  /**
   * Sum of every per-model `branch_bytes_scanned` that produced a number. `None` when no branch model reported a byte count (mirrors `RunOutput.cost.total_bytes_scanned` semantics — the non-BigQuery adapters today still inherit the default stub on `WarehouseAdapter::execute_statement_with_stats`). Used to project the `[budget]` `max_bytes_scanned` limit at preview time.
   */
  total_branch_bytes_scanned?: number | null;
  /**
   * Sum of every per-model `branch_cost_usd` that produced a number.
   */
  total_branch_cost_usd?: number | null;
  /**
   * Sum of every per-model `branch_duration_ms`. Used to project the `[budget]` `max_duration_ms` limit at preview time.
   */
  total_branch_duration_ms: number;
  [k: string]: unknown;
}
