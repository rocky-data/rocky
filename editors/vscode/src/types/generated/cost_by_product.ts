/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/cost_by_product.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * A unit a monthly cost figure is counted in. Units are never added to each other, and the two storage bases are never added together.
 */
export type CostUnit = "compute_ms" | "storage_byte_hours_data_files" | "storage_byte_hours_allocated_blocks";
/**
 * What a [`MoneyPart`] prices.
 */
export type MoneyFigure = "compute" | "storage_data_files" | "storage_allocated_blocks" | "egress" | "ai_tokens";
/**
 * Whether a money figure in the monthly report could be computed.
 */
export type MoneyStatus = "priced" | "no_price" | "not_measured" | "partial";
/**
 * How a table version's cost was split across the models that read it.
 */
export type CostSplitBasis = "bytes" | "estimated_bytes" | "rows_scanned_unweighted" | "owner_pays";
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
 * `rocky cost --month YYYY-MM --by product`: one month of production cost, split across data products, in physical units, with money in USD where `[cost]` sets a price.
 *
 * For every unit, `sum(charged) + unallocated` equals `measured_total`, exactly. Shadow and branch spend (`shadow_branch`) and `not_measured` sit outside that equation. Money is priced from each figure on its own, so the money figures are floating point and are not claimed to add up to the cent.
 */
export interface ProductMonthCostOutput {
  /**
   * The instant the report was computed. Storage accrues to the earlier of this and `month_end`.
   */
  as_of: string;
  /**
   * `"cost_by_product"`.
   */
  command: string;
  /**
   * Whether run history reaches back far enough to cover the month. When false, state retention may have swept runs the month needs, and every figure is partial.
   */
  history_complete: boolean;
  /**
   * Everything measured in production this month.
   */
  measured_total: CostAmount[];
  /**
   * `measured_total` in USD. Present only when `measured_total_money_status` is `priced`.
   */
  measured_total_money?: number | null;
  measured_total_money_parts: MoneyPart[];
  measured_total_money_status: MoneyStatus;
  /**
   * The month, `YYYY-MM`, in `timezone`.
   */
  month: string;
  /**
   * First instant of the next month, UTC (RFC 3339).
   */
  month_end: string;
  /**
   * First instant of the month, UTC (RFC 3339).
   */
  month_start: string;
  not_measured: CostNotMeasured;
  /**
   * One entry per product that owns a model, sorted by name.
   */
  products: ProductMonthCost[];
  /**
   * Shadow and branch compute. Real spend, but not production.
   */
  shadow_branch: CostAmount[];
  /**
   * The IANA time zone the month is cut in: the project's `[schedule].timezone`.
   */
  timezone: string;
  /**
   * Cost of tables no product owns that no measured reader took.
   */
  unallocated: CostAmount[];
  /**
   * `unallocated` in USD. Present only when `unallocated_money_status` is `priced`.
   */
  unallocated_money?: number | null;
  unallocated_money_parts: MoneyPart[];
  unallocated_money_status: MoneyStatus;
  version: string;
  /**
   * Things that make the report less complete than it looks: a product spec that did not load, a model graph that could not be read.
   */
  warnings?: string[];
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
 * Executions the monthly report could not measure. Counts, never amounts.
 */
export interface CostNotMeasured {
  /**
   * Production executions that failed and recorded no time; their compute is lost. One that wrote its table is still a version.
   */
  failed_executions: number;
  /**
   * Tables whose storage before their first measurement this month is unknown, because run history does not reach back far enough.
   */
  storage_unknown_tables: number;
  /**
   * Distinct table names (lowercased, as the read named them, so `c.s.t` and `s.t` count twice). A name is counted when at least one read of it this month found no earlier recorded build: a source Rocky does not build, a table that matches two models, or a table built before the history Rocky keeps or before the reader began. The same name can also be charged for later reads. The cost of an unlinked read is not split.
   */
  unattributed_read_tables: number;
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
   * Why `breach_events` was not computed, when it was not: replaying the month would split more executions than the report allows (the reason gives the runs, the executions and the cap). `budget` is still checked. Absent otherwise.
   */
  breach_events_skipped?: string | null;
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
