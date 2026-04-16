/* eslint-disable */

/**
 * JSON output for `rocky lineage <model>` (model lineage shape).
 *
 * When invoked with `--column <col>` (or `model.column` syntax), the dispatch returns `ColumnLineageOutput` instead — the two shapes share version/command/model headers but otherwise differ enough to keep separate.
 */
export interface LineageOutput {
  columns: LineageColumnDef[];
  command: string;
  downstream: string[];
  edges: LineageEdgeRecord[];
  model: string;
  upstream: string[];
  version: string;
  [k: string]: unknown;
}
export interface LineageColumnDef {
  name: string;
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
