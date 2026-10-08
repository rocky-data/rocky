/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/column_lineage.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

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
