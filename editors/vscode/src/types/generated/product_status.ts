/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/product_status.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * How a table version's cost was split across the models that read it.
 */
export type CostSplitBasis = "bytes" | "estimated_bytes" | "rows_scanned_unweighted" | "owner_pays";
/**
 * A unit a monthly cost figure is counted in. Units are never added to each other, and the two storage bases are never added together.
 */
export type CostUnit = "compute_ms" | "storage_byte_hours_data_files" | "storage_byte_hours_allocated_blocks";
/**
 * Which `[product_budget.<name>]` limit a [`ProductBudgetCheck`] tests.
 */
export type ProductBudgetLimitType = "max_compute_hours" | "max_storage_gb_month" | "max_usd";
/**
 * The verdict of a monthly product budget, or of one of its limits.
 */
export type ProductBudgetVerdict = "in_budget" | "breached" | "not_evaluated";
/**
 * What to do when a [`ProductBudgetConfig`] limit is crossed. Only `warn` exists: the breach is reported and the `on_budget_breach` hook fires, and the run's exit code does not change.
 */
export type ProductBudgetBreachAction = "warn";
/**
 * What a [`MoneyPart`] prices.
 */
export type MoneyFigure = "compute" | "storage_data_files" | "storage_allocated_blocks" | "egress" | "ai_tokens";
/**
 * Whether a money figure in the monthly report could be computed.
 */
export type MoneyStatus = "priced" | "no_price" | "not_measured" | "partial";
/**
 * What a duration estimate timed.
 */
export type EstimateBasis = "model_delivery" | "pipeline_run";
/**
 * Whether a duration estimate has enough runs to judge from.
 */
export type EstimateState = "known" | "unknown";
/**
 * What an out-of-range row count does to the run.
 */
export type VolumeSeverity = "error" | "warning";
/**
 * The outcome of one volume check.
 */
export type VolumeVerdict = "in_range" | "below_min" | "above_max" | "not_evaluated";

/**
 * JSON output of `rocky product status`.
 */
export interface ProductStatusOutput {
  /**
   * The approval record, when one exists.
   */
  approval?: ProductApprovalOutput | null;
  /**
   * Byte-verification problems against the committed manifest (empty = every committed artifact matches its recorded hash).
   */
  artifact_problems?: string[];
  command: string;
  /**
   * The committed lowering phase, when a manifest exists.
   */
  committed_phase?: string | null;
  /**
   * The committed manifest's spec digest, when one exists.
   */
  committed_spec_digest?: string | null;
  /**
   * The product's cost for the current month (in the project's `[schedule].timezone`), with its `[product_budget.<name>]` verdict and breach events when it has a budget: its row from `rocky cost --month <month> --by product`. Present only from `rocky product status`, and only when the spec parses and the product owns a model.
   */
  cost_month?: ProductCostMonth | null;
  /**
   * Why `cost_month` could not be computed, when it could not.
   */
  cost_month_error?: string | null;
  /**
   * Delivery state, when the spec declares `deliver_by`: whether the latest production delivery was on time, how late, and whether the next delivery is at risk. The estimate times run start to the output model's finish.
   */
  delivery?: DeliveryStatus | null;
  /**
   * The persisted fulfillment state tag, when one exists.
   */
  fulfill_state?: string | null;
  /**
   * Number of fulfillment journal rows recorded for this product.
   */
  journal_rows: number;
  /**
   * The resolved output model, when the spec parses.
   */
  output_model?: string | null;
  /**
   * The product name the status was asked for.
   */
  product: string;
  /**
   * `product:<name>`, when the spec parses.
   */
  product_id?: string | null;
  /**
   * Whether the approval snapshot's bytes still digest to the recorded value. `null` without an approval; `false` is tamper.
   */
  snapshot_intact?: boolean | null;
  /**
   * Source range state, one row per `[[product.source.volume]]` the spec declares: the range and the count the newest production run that counted this product's sources took at its start. Empty (and omitted) when the spec declares no source range.
   */
  source_volumes?: ProductSourceVolumeStatus[];
  /**
   * The working spec's digest, when it parses.
   */
  spec_digest?: string | null;
  /**
   * Why the spec failed to parse, when it did not.
   */
  spec_error?: string | null;
  /**
   * Whether the working spec equals the approved revision. `null` when either side is missing.
   */
  spec_matches_approval?: boolean | null;
  /**
   * Whether `products/<name>.toml` exists and parses.
   */
  spec_present: boolean;
  /**
   * Whether an uncommitted staging journal is pending. Status never mutates; the next compile resolves it.
   */
  staging_journal_present: boolean;
  version: string;
  /**
   * Volume state, when the spec declares `[product.output.volume]`: the declared range and the latest production count judged against it.
   */
  volume?: ProductVolumeStatus | null;
  [k: string]: unknown;
}
/**
 * The approval record, as echoed by compile/status output.
 */
export interface ProductApprovalOutput {
  /**
   * RFC3339 approval instant.
   */
  approved_at?: string | null;
  /**
   * Best-effort approver identity.
   */
  approver: string;
  /**
   * Project-root-relative path of the immutable snapshot file.
   */
  snapshot_path: string;
  /**
   * The approved `sha256:<hex>` spec digest.
   */
  spec_digest: string;
  [k: string]: unknown;
}
/**
 * One product's current month, on `rocky product status`.
 */
export interface ProductCostMonth {
  /**
   * The instant the month was computed (RFC 3339).
   */
  as_of: string;
  /**
   * The product's row from `rocky cost --month <month> --by product`.
   */
  cost: ProductMonthCost;
  /**
   * Whether run history reaches back far enough to cover the month.
   */
  history_complete: boolean;
  /**
   * The month, `YYYY-MM`, in `timezone`.
   */
  month: string;
  /**
   * The project's `[schedule].timezone`.
   */
  timezone: string;
  [k: string]: unknown;
}
/**
 * One product's month inside [`ProductMonthCostOutput`].
 */
export interface ProductMonthCost {
  /**
   * What the product pays for tables it reads.
   */
  allocated_upstream: AllocatedUpstreamCost[];
  /**
   * Each time this month the product went over its budget, in run order. Found by checking the budget again after each production run of the month. A product that goes over, back under and over again has two events. An event is never withdrawn, even when a later run moves the product back under. Empty (and omitted) when there was none.
   */
  breach_events?: ProductBudgetBreachEvent[];
  /**
   * The product's `[product_budget.<name>]` checked against `charged`, as of this report. Absent when the product has no budget.
   */
  budget?: ProductBudgetStatus | null;
  /**
   * What the product's own executions cost, before any split. A memo for reading `kept` beside it: it is never summed and is not part of the invariant.
   */
  built: CostAmount[];
  /**
   * What the product is charged: `kept` plus `allocated_upstream`.
   */
  charged: CostAmount[];
  /**
   * The part of `built` the product still pays after other products pay for what they read.
   */
  kept: CostAmount[];
  /**
   * `charged` in USD, priced only from prices set in `[cost]`. Present only when `money_status` is `priced`. It covers compute and storage; egress and AI tokens are not measured and are not in it.
   */
  money?: number | null;
  /**
   * `charged` priced one figure at a time.
   */
  money_parts: MoneyPart[];
  /**
   * Why `money` is or is not present.
   */
  money_status: MoneyStatus;
  /**
   * The model the product owns.
   */
  output_model: string;
  /**
   * The part of `kept` that came back from readers outside every product.
   */
  owner_residual: CostAmount[];
  product: string;
  /**
   * Shares of the product's tables that readers did not pay.
   */
  shared_not_charged: SharedNotChargedCost[];
  [k: string]: unknown;
}
/**
 * What a product paid for one table it reads, in one unit.
 */
export interface AllocatedUpstreamCost {
  /**
   * What this product was charged.
   */
  amount: number;
  basis: CostSplitBasis;
  /**
   * The scan was so small that the proportions say little.
   */
  low_confidence: boolean;
  /**
   * The whole cost that was split, so `amount / of_total` is the share.
   */
  of_total: number;
  /**
   * The table has not been rebuilt since, in a month that is not over: a later reader can still change this share.
   */
  provisional: boolean;
  /**
   * The model whose table was read.
   */
  table: string;
  unit: CostUnit;
  [k: string]: unknown;
}
/**
 * One time a product went over its monthly budget.
 */
export interface ProductBudgetBreachEvent {
  /**
   * When that run finished (RFC 3339). The budget was checked as of then.
   */
  at: string;
  /**
   * The limits that were exceeded at that run.
   */
  breached: ProductBudgetCheck[];
  /**
   * The production run after which the product was first over.
   */
  run_id: string;
  [k: string]: unknown;
}
/**
 * One limit of a product budget, checked against the product's charge.
 */
export interface ProductBudgetCheck {
  /**
   * The month-to-date figure the limit tests. Absent when it is not known.
   */
  actual?: number | null;
  /**
   * The configured limit.
   */
  limit: number;
  limit_type: ProductBudgetLimitType;
  /**
   * Why the limit is `not_evaluated`.
   */
  reason?: string | null;
  /**
   * For `max_storage_gb_month`, the storage basis checked. Absent for the other limits.
   */
  unit?: CostUnit | null;
  /**
   * `breached` when `actual` is greater than `limit`. Equal is in budget.
   */
  verdict: ProductBudgetVerdict;
  [k: string]: unknown;
}
/**
 * A product's monthly budget, checked once.
 */
export interface ProductBudgetStatus {
  /**
   * One check per limit set (two for `max_storage_gb_month` when both storage bases were measured).
   */
  checks: ProductBudgetCheck[];
  /**
   * What a breach does. Only `warn`: the run's exit code does not change.
   */
  on_breach: ProductBudgetBreachAction;
  /**
   * Why the budget as a whole is `not_evaluated`, when it is.
   */
  reasons?: string[];
  /**
   * `breached` when any limit is breached. Otherwise `not_evaluated` when any limit could not be checked, or when run history does not cover the month (then always). Otherwise `in_budget`.
   */
  verdict: ProductBudgetVerdict;
  [k: string]: unknown;
}
/**
 * A whole-number amount in one [`CostUnit`].
 */
export interface CostAmount {
  /**
   * Whole units. Milliseconds, or byte-hours rounded down per interval.
   */
  amount: number;
  unit: CostUnit;
  [k: string]: unknown;
}
/**
 * One figure of a monthly money total.
 */
export interface MoneyPart {
  figure: MoneyFigure;
  status: MoneyStatus;
  /**
   * USD. Present only when `status` is `priced`.
   */
  usd?: number | null;
  [k: string]: unknown;
}
/**
 * A share of an owned table's cost that a reader did not pay.
 */
export interface SharedNotChargedCost {
  /**
   * The reader's share. It stays with the table's owner.
   */
  amount: number;
  basis: CostSplitBasis;
  /**
   * The model that read it.
   */
  reader: string;
  /**
   * The owned model whose table was read.
   */
  table: string;
  unit: CostUnit;
  [k: string]: unknown;
}
/**
 * Delivery state of one product deadline: was the latest delivery on time, and is the next one at risk.
 *
 * Reported by `rocky product status` and, per pipeline, by the scheduler status. `on_time` and `late_by_seconds` are facts about the latest delivery. `at_risk` rests on [`DurationEstimateStatus`], and is `null` whenever the estimate is `unknown`.
 */
export interface DeliveryStatus {
  /**
   * True when the p90 duration is longer than the time left before `next_due_at`. `null` when the estimate is `unknown`: no verdict.
   */
  at_risk?: boolean | null;
  /**
   * The product's `deliver_by` cron expression.
   */
  deliver_by: string;
  /**
   * The duration estimate `at_risk` rests on.
   */
  estimate: DurationEstimateStatus;
  /**
   * When the latest production delivery finished.
   */
  last_delivered_at?: string | null;
  /**
   * The deadline the latest delivery served, when its run recorded one.
   */
  last_due_at?: string | null;
  /**
   * How late the latest delivery was, in seconds; `0` when on time. `null` when no delivery recorded a deadline.
   */
  late_by_seconds?: number | null;
  /**
   * The deadline the next delivery must meet. A time in the past means the next delivery is already late.
   */
  next_due_at?: string | null;
  /**
   * Whether the latest delivery met its deadline. `null` when no delivery recorded a deadline.
   */
  on_time?: boolean | null;
  /**
   * The model the product delivers.
   */
  output_model: string;
  /**
   * The product whose deadline this is.
   */
  product: string;
  /**
   * The timezone `deliver_by` is read in.
   */
  timezone: string;
  [k: string]: unknown;
}
/**
 * A p50/p90 duration estimate and the sample behind it.
 */
export interface DurationEstimateStatus {
  /**
   * What was timed: `model_delivery` (run start to the model's finish) or `pipeline_run` (run start to run finish).
   */
  basis: EstimateBasis;
  /**
   * True when the newest 5 runs were all slower than the p90 of the older ones, so `p90_ms` is the slowest of those 5.
   */
  growth: boolean;
  /**
   * How many production runs the estimate read.
   */
  n: number;
  /**
   * Median duration in milliseconds, when `known`.
   */
  p50_ms?: number | null;
  /**
   * 90th-percentile duration in milliseconds, when `known`.
   */
  p90_ms?: number | null;
  /**
   * `known` with at least 5 runs; `unknown` below that, and then no verdict rests on the estimate.
   */
  state: EstimateState;
  /**
   * The most runs it reads (the newest 20).
   */
  window: number;
  [k: string]: unknown;
}
/**
 * One declared source range and the latest production count of its table.
 */
export interface ProductSourceVolumeStatus {
  /**
   * When that run started.
   */
  counted_at?: string | null;
  /**
   * The declared `max_rows` (inclusive), when set.
   */
  max_rows?: number | null;
  /**
   * The declared `min_rows` (inclusive), when set.
   */
  min_rows?: number | null;
  /**
   * The table's total rows when the newest production run that counted this product's sources started. Absent when that run took no count of this table, or none has run yet.
   */
  row_count?: number | null;
  /**
   * The run that took the count.
   */
  run_id?: string | null;
  /**
   * The declared severity: `warning` (the default) or `error`.
   */
  severity: VolumeSeverity;
  /**
   * The source table, as the spec lists it.
   */
  table: string;
  /**
   * That count judged against the range the spec declares now: `in_range`, `below_min`, `above_max`, or `not_evaluated` when no count exists.
   */
  verdict: VolumeVerdict;
  [k: string]: unknown;
}
/**
 * The declared volume range and the latest production count.
 */
export interface ProductVolumeStatus {
  /**
   * When that build finished.
   */
  counted_at?: string | null;
  /**
   * The declared `max_rows` (inclusive), when set.
   */
  max_rows?: number | null;
  /**
   * The declared `min_rows` (inclusive), when set.
   */
  min_rows?: number | null;
  /**
   * The newest production build that recorded a count. Absent when no build has been counted yet.
   */
  row_count?: number | null;
  /**
   * The run that took the count.
   */
  run_id?: string | null;
  /**
   * The declared severity: `warning` (the default) or `error`.
   */
  severity: VolumeSeverity;
  /**
   * That count judged against the range the spec declares now: `in_range`, `below_min`, `above_max`, or `not_evaluated` when no count exists.
   */
  verdict: VolumeVerdict;
  [k: string]: unknown;
}
