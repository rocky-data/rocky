/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/catalog.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * Asset kind discriminator for [`CatalogAsset`].
 */
export type AssetKind = "Source" | "Model" | "View" | "MaterializedView";
/**
 * Confidence grading for a [`CatalogEdge`].
 *
 * Coarse-grained on purpose: every consumer threshold-checks at "good enough", and a 3-bucket enum is simpler than a numeric score. A numeric `confidence_score` field can be added alongside this enum later without breaking callers.
 */
export type EdgeConfidence = "High" | "Medium" | "Low";

/**
 * JSON output for `rocky catalog`.
 *
 * A persisted, queryable snapshot of column-level lineage across the project. Emitted to `./.rocky/catalog/catalog.json` (default) so any non-Rocky consumer (BI tool, governance dashboard, PR bot) can read the artifact without invoking the engine. The CLI's own stdout is a one-screen summary; this struct is the structured payload backing `--output json`.
 */
export interface CatalogOutput {
  assets: CatalogAsset[];
  command: string;
  /**
   * Stable fingerprint of the `rocky.toml` bytes. Lets a downstream consumer detect whether the catalog was built against a config that has since changed.
   */
  config_hash: string;
  edges: CatalogEdge[];
  generated_at: string;
  /**
   * Identifier of the last successful run that produced these assets, when known. Absent until the catalog is enriched with run-history metadata.
   */
  last_run_id?: string | null;
  /**
   * Pipeline name used to build the catalog. When the project has multiple pipelines, this is the first one in declaration order.
   */
  project_name: string;
  stats: CatalogStats;
  version: string;
  [k: string]: unknown;
}
/**
 * A single asset (model or source) in the catalog.
 */
export interface CatalogAsset {
  columns: CatalogColumn[];
  downstream_models: string[];
  /**
   * Fully-qualified target identifier (`catalog.schema.table`) when resolvable, otherwise the model name.
   */
  fqn: string;
  /**
   * Free-form natural-language description of the asset's purpose, when supplied via the model's sidecar config.
   */
  intent?: string | null;
  /**
   * Whether the asset is a project-managed model or an external source.
   */
  kind: AssetKind;
  /**
   * Timestamp of the last successful materialization, when known.
   */
  last_materialized_at?: string | null;
  /**
   * Identifier of the last successful run that produced the asset, when known.
   */
  last_run_id?: string | null;
  /**
   * Model or source name as it appears in lineage edges.
   */
  model_name: string;
  upstream_models: string[];
  [k: string]: unknown;
}
/**
 * A column on a catalog asset.
 */
export interface CatalogColumn {
  /**
   * Declared or inferred type of the column, when known.
   */
  data_type?: string | null;
  name: string;
  /**
   * Whether the column accepts nulls, when known.
   */
  nullable?: boolean | null;
  [k: string]: unknown;
}
/**
 * A single column-level lineage edge.
 */
export interface CatalogEdge {
  /**
   * Confidence the producing edge is fully understood. `Medium` is emitted for star-expanded projections (where lineage is inferred rather than parsed from an explicit projection); `High` for edges sourced from explicit columns.
   */
  confidence: EdgeConfidence;
  source_column: string;
  source_model: string;
  target_column: string;
  target_model: string;
  /**
   * Transform kind: "direct", "cast", "expression", or "aggregation: <fn>". Stringified to match the existing lineage edge shape.
   */
  transform: string;
  [k: string]: unknown;
}
/**
 * Aggregate counts for the emitted catalog.
 */
export interface CatalogStats {
  asset_count: number;
  /**
   * Number of assets whose lineage was derived from `SELECT *` (and is therefore inferred rather than parsed). Surfaces partial-lineage coverage as a single number in the summary.
   */
  assets_with_star: number;
  column_count: number;
  duration_ms: number;
  edge_count: number;
  /**
   * Number of columns with no producing edge — typically the result of a star expansion that could not resolve an upstream column.
   */
  orphan_columns: number;
  [k: string]: unknown;
}
