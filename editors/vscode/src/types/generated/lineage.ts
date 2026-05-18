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
  /**
   * Per-node metadata for every model referenced by this lineage view (the focal model plus each endpoint of `edges`). Lets consumers (e.g. the VS Code subgraph drill-in) cluster nodes by their resolved target schema or source identity instead of parsing the qualified node name. Empty when no nodes were resolved; older JSON payloads cached locally may omit the field entirely.
   */
  nodes?: LineageNodeDef[];
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
/**
 * Per-node metadata for the lineage graph.
 *
 * One entry per distinct model referenced by `LineageOutput.edges` (plus the focal model). Carries cluster keys consumers can use to group nodes without having to parse the qualified node name. Either optional field may be absent — `target_schema` is `None` for nodes that aren't project models, and `source_id` is `None` for nodes that are project models (i.e. the two fields are mutually exclusive in practice).
 */
export interface LineageNodeDef {
  /**
   * Qualified node identifier as it appears in `edges[].source.model` and `edges[].target.model`. Stable across the rest of the payload.
   */
  model: string;
  /**
   * Source identifier for nodes that represent an external source (i.e. a referenced table outside the project model set). The value mirrors how the SQL referenced the source so consumers can key on it directly. Omitted for project models.
   */
  source_id?: string | null;
  /**
   * Resolved target schema for project models, from the model's declared target config. Omitted for external sources and any node Rocky couldn't resolve to a project model.
   */
  target_schema?: string | null;
  [k: string]: unknown;
}
