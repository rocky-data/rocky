/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/column_lineage.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * A construct whose lineage Rocky does not record, so a model that uses it has known-incomplete lineage (#2377).
 *
 * Impact analysis (`rocky lineage --downstream`, the blast radius) can then under-report: an edge the construct implies is missing. The list is what the extractor knows it skips. It is not a proof that everything else is traced.
 */
export type LineageGap =
  | "natural_join"
  | "group_by_all"
  | "set_operation"
  | "cte_body_row_selection"
  | "derived_table_row_selection"
  | "subquery_expression"
  | "nested_named_window"
  | "unmodelled_relation"
  | "unattributed_column"
  | "unresolved_projection"
  | "unresolved_star";

/**
 * JSON output for `rocky lineage <model> --column <col>`.
 */
export interface ColumnLineageOutput {
  column: string;
  command: string;
  /**
   * Direction of the trace walk: `"upstream"` (producers) or `"downstream"` (consumers). Defaults to upstream when `--column` is set without direction flags, matching the historical default.
   */
  direction: string;
  /**
   * Every downstream column that transitively consumes `(model, column)`, deduplicated and deterministically sorted. An author-time "what does changing this column affect" signal, always populated regardless of `direction` so the default (upstream) trace still carries the blast radius. Inspection only — this never feeds a build/skip/reuse decision. Empty when the column has no consumers.
   */
  downstream_consumers?: LineageQualifiedColumn[];
  /**
   * Models whose lineage is known to be incomplete, sorted by name (#2377): the focal model, every model upstream of it, and every model downstream of it (by model dependency, so a reader with no traced column edge is included). `trace`, `downstream_consumers` and `row_selection` may be under-reported through them. `[]` means none.
   *
   * Always written. A payload with no such field comes from an older build: read it as unknown, never as complete.
   */
  incomplete_lineage?: IncompleteLineageRecord[] | null;
  model: string;
  /**
   * Row-selection edges along the trace: columns that decide which rows or groups exist (join keys, filters, group keys, window keys) rather than feeding a value. `trace` stays value-derivation only.
   *
   * Upstream: the row-selection inputs of every model on the value trace, for the traced column. Downstream: the models whose rows the traced column (or a column derived from it) filters, joins, groups or partitions. Omitted when empty.
   */
  row_selection?: RowSelectionEdgeRecord[];
  trace: LineageEdgeRecord[];
  version: string;
  [k: string]: unknown;
}
export interface LineageQualifiedColumn {
  column: string;
  model: string;
  [k: string]: unknown;
}
/**
 * A model whose column lineage is known to be incomplete (#2377).
 */
export interface IncompleteLineageRecord {
  /**
   * The constructs in it that Rocky does not trace, such as `natural_join` or `set_operation`. `None` when they were never computed (unknown).
   */
  gaps?: LineageGap[] | null;
  /**
   * The model.
   */
  model: string;
  [k: string]: unknown;
}
/**
 * One row-selection lineage edge. See `ColumnLineageOutput::row_selection`.
 */
export interface RowSelectionEdgeRecord {
  /**
   * `join_key`, `filter`, `group_by`, `having`, `qualify`, `window_partition`, `window_order`, `distinct_on` or `order_limit`.
   */
  kind: string;
  /**
   * The column that influences row selection.
   */
  source: LineageQualifiedColumn;
  /**
   * The single output column affected (window keys). Omitted when the edge affects every output column of `target_model`.
   */
  target_column?: string | null;
  /**
   * The model whose rows it influences.
   */
  target_model: string;
  [k: string]: unknown;
}
export interface LineageEdgeRecord {
  source: LineageQualifiedColumn;
  target: LineageQualifiedColumn;
  /**
   * Transform kind: "direct", "cast", "expression", etc. Stringified from `rocky_sql::lineage::TransformKind` to avoid pulling schemars into rocky-sql.
   */
  transform: string;
  [k: string]: unknown;
}
