/* eslint-disable */

/**
 * JSON output for `rocky lineage <model> --column <col>`.
 */
export interface ColumnLineageOutput {
  column: string;
  command: string;
  model: string;
  trace: LineageEdgeRecord[];
  version: string;
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
export interface LineageQualifiedColumn {
  column: string;
  model: string;
  [k: string]: unknown;
}
