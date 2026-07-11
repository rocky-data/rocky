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
  trace: LineageEdgeRecord[];
  version: string;
  [k: string]: unknown;
}
export interface LineageQualifiedColumn {
  column: string;
  model: string;
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
