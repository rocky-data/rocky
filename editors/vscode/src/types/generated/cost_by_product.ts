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
 * How a table version's cost was split across the models that read it.
 */
export type CostSplitBasis = "bytes" | "estimated_bytes" | "rows_scanned_unweighted" | "owner_pays";

/**
 * `rocky cost --month YYYY-MM --by product`: one month of production cost, split across data products, in physical units. No money.
 *
 * For every unit, `sum(charged) + unallocated` equals `measured_total`, exactly. Shadow and branch spend (`shadow_branch`) and `not_measured` sit outside that equation.
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
 * Executions the monthly report could not measure. Counts, never amounts.
 */
export interface CostNotMeasured {
  /**
   * Production executions that failed and recorded no time.
   */
  failed_executions: number;
  /**
   * Tables whose storage before their first measurement this month is unknown, because run history does not reach back far enough.
   */
  storage_unknown_tables: number;
  /**
   * Distinct tables read this month that name no model with a recorded build: a source Rocky does not build, or a table built before the history Rocky keeps. Their cost is not split.
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
