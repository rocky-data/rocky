/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/backfill.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * The composed, review-gated recovery plan emitted by `rocky backfill`.
 *
 * A backfill re-runs *existing* recipes over a scoped window — it never rewrites SQL to "fix" data. The command composes the set of models to rebuild (the affected models plus their downstream lineage closure), the order to rebuild them in, the partition window where models are partitioned, and an estimated cost. The plan is persisted and **always** requires a human sign-off (`rocky review <plan-id> --approve`) before `rocky apply` will execute it, regardless of any configured policy — backfills are where blast radius hides.
 */
export interface BackfillOutput {
  /**
   * The exact command that executes the plan once reviewed.
   */
  apply_command: string;
  /**
   * Always `"backfill"`.
   */
  command: string;
  /**
   * Best-effort cost projection for the rebuild. Always an *estimate*.
   */
  cost_estimate: BackfillCostEstimate;
  /**
   * Topological execution layers over the closure. Models in the same layer are independent; each layer runs after the previous completes.
   */
  execution_layers: string[][];
  /**
   * Human-readable one-line summary.
   */
  message: string;
  /**
   * The full set of models to rebuild — the seeds plus their downstream lineage closure — in topological (dependency-first) order.
   */
  models: string[];
  /**
   * Partition window applied to partitioned models in the closure, when a `--from`/`--to` range was supplied. Absent when the backfill is a full rebuild of each model.
   */
  partition_scope?: BackfillPartitionScope | null;
  /**
   * The persisted plan identifier (64-char blake3 hex). Feed it to `rocky review <plan-id> --approve` then `rocky apply <plan-id>`.
   */
  plan_id: string;
  /**
   * Always `true` — a backfill plan is unconditionally review-gated.
   */
  requires_review: boolean;
  /**
   * The exact command that clears the review gate.
   */
  review_command: string;
  /**
   * The models that triggered the backfill (the failure/gap seeds), before the downstream closure is added.
   */
  seed_models: string[];
  /**
   * What surfaced the affected models: `"manual"` (explicit `--model`) or `"last_run_failure"` (seeded from the previous run's failed models, i.e. the contained/quarantined window).
   */
  trigger: string;
  /**
   * Rocky version that composed the plan.
   */
  version: string;
  [k: string]: unknown;
}
/**
 * A best-effort, label-as-estimate cost projection for a backfill.
 *
 * The projection re-uses the same historical-observed cost formula as `rocky cost`: each closure model's most recent recorded execution is priced by the warehouse cost model. It is offline (no warehouse round-trip) and therefore approximate — `is_estimate` is always `true`.
 */
export interface BackfillCostEstimate {
  /**
   * How the figure was derived: `"historical_observed"` when at least one closure model had a prior execution to price, else `"unavailable"`.
   */
  basis: string;
  /**
   * Always `true` — this is a projection, not a measured cost.
   */
  is_estimate: boolean;
  /**
   * Per-model breakdown, in closure order.
   */
  per_model: BackfillModelCost[];
  /**
   * Summed estimated compute cost in USD, or `None` when no closure model has priced execution history (or the warehouse is unbilled, e.g. DuckDB).
   */
  total_cost_usd?: number | null;
  [k: string]: unknown;
}
/**
 * Per-model entry in a [`BackfillCostEstimate`].
 */
export interface BackfillModelCost {
  /**
   * Estimated compute cost in USD for one rebuild, or `None` when the model has no priced execution history.
   */
  cost_usd?: number | null;
  /**
   * Observed duration (ms) of the priced historical execution, when found.
   */
  duration_ms?: number | null;
  /**
   * The model name.
   */
  model_name: string;
  /**
   * The run the historical figure was read from, for provenance.
   */
  source_run_id?: string | null;
  [k: string]: unknown;
}
/**
 * The partition window a backfill scopes partitioned models to.
 */
export interface BackfillPartitionScope {
  /**
   * Range lower bound (`--from`).
   */
  from?: string | null;
  /**
   * The closure models that declare a partitioned (`time_interval`) materialization — the ones the window actually applies to. Empty when no model in the closure is partitioned (the window is then inert).
   */
  models: string[];
  /**
   * Range upper bound (`--to`).
   */
  to?: string | null;
  [k: string]: unknown;
}
