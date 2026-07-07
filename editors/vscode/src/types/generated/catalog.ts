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
  /**
   * Recipe-identity triple from the asset's most recent successful materialization, when known. See [`RecipeIdentityView`]. `None` for source-kind assets (no execution) and for models built before the triple was captured.
   */
  recipe_identity?: RecipeIdentityView | null;
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
  /**
   * Human-readable description from the sidecar `[columns]` table, when set.
   */
  description?: string | null;
  name: string;
  /**
   * Whether the column accepts nulls, when known.
   */
  nullable?: boolean | null;
  [k: string]: unknown;
}
/**
 * The recipe-identity triple surfaced on a model record — the answer to "what exact program, over what inputs, in what environment produced this?".
 *
 * Read back from the persisted [`rocky_core::state::ModelExecution`]. Every field is optional: a record written before the triple was captured (state schema predating it) or a failed execution carries none of them, and the input side is absent on the default run path (which observes no inputs). The whole object is omitted from JSON when nothing was recorded — see [`Self::from_execution`] — so output for pre-triple records is unchanged.
 */
export interface RecipeIdentityView {
  /**
   * The **environment** key: blake3 (hex) over the engine version and the adapter / dialect identity. Excludes the hostname by construction.
   */
  env_hash?: string | null;
  /**
   * The hash-scheme tag (`"v1"`) in force when the triple was computed, so a future canonicalisation change is an explicit new scheme rather than a silent history fork.
   */
  hash_scheme?: string | null;
  /**
   * The **input** key: blake3 (hex) over the run's observed input identities. Present only when the run actually observed inputs (the `--skip-unchanged` gate's upstream freshness signatures, or the content-addressed reuse spine); absent on the default run path.
   */
  input_hash?: string | null;
  /**
   * Strength of [`Self::input_hash`]: `"strong"` (every observed upstream is a content hash — offline byte-verifiable) or `"heuristic"` (at least one is a freshness signature, attesting freshness rather than byte-identity). Carried so a weak input hash is never presented as a content claim. `None` whenever [`Self::input_hash`] is `None`.
   */
  input_proof_class?: string | null;
  /**
   * The program **identity** key: blake3 (hex) of the canonical `ModelIr` JSON. Stable across environments and engine versions for the same program text. The value `rocky history --recipe <hash>` filters on.
   */
  recipe_hash?: string | null;
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
