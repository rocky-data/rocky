/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/lineage.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

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
  /**
   * Inferred column type, rendered as a Rocky type string (e.g. `STRING`, `INT64`, `DECIMAL(10,2)`, `TIMESTAMP`). Omitted when the type could not be inferred — typically a `SELECT *` against an upstream whose schema isn't cached, or a model that did not pass typecheck.
   */
  data_type?: string | null;
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
