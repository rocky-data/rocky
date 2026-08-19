/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/rocky_product.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * Severity of the observation-phase staleness report.
 */
export type FreshnessSeverity = "error" | "warning";

/**
 * The whole document: an optional version pin plus `[product]`.
 */
export interface SpecFile {
  /**
   * The product declaration.
   */
  product: ProductSpec;
  /**
   * Optional schema-version pin.
   */
  spec_version?: number | null;
}
/**
 * The `[product]` table.
 */
export interface ProductSpec {
  /**
   * Plain-language statement of what the product is for.
   */
  intent: string;
  /**
   * Product name; names the state directory and the default model.
   */
  name: string;
  /**
   * What the product must produce.
   */
  output: OutputSpec;
  /**
   * Where the data comes from.
   */
  source: SourceSpec;
  /**
   * How much the agent is trusted.
   */
  trust: TrustSpec;
}
/**
 * The one output model and the guarantees around it.
 */
export interface OutputSpec {
  /**
   * Opaque SQL boolean expressions, checked by the engine.
   */
  checks?: string[];
  /**
   * Column classification tags.
   *
   * Held in an insertion-ordered map because the sidecar merge must be able to preserve an order it is given. The parser fills it in sorted key order: `toml::Value` hands back a sorted map, so document order is not recoverable here without span-based parsing. Sorted is deterministic, which is what byte-stable output needs; recovering true document order belongs with the merge that actually rewrites a human's file.
   */
  classifications?: {
    [k: string]: string;
  };
  /**
   * Every declared column.
   */
  columns: ColumnSpec[];
  /**
   * Optional freshness declaration.
   */
  freshness?: FreshnessSpec | null;
  /**
   * The columns that identify one row.
   */
  grain: string[];
  /**
   * Model name; defaults to the product name when omitted.
   */
  model?: string | null;
}
/**
 * One declared output column.
 */
export interface ColumnSpec {
  /**
   * Column name; must be a bare identifier.
   */
  name: string;
  /**
   * Whether the column may hold nulls.
   */
  nullable: boolean;
  /**
   * Rocky type name, optionally parameterized.
   */
  type: string;
}
/**
 * Declared freshness: the lag budget plus the column that witnesses it.
 *
 * `time_column` is required. Without it the observation-phase staleness check (`MAX(time_column)` against the budget) has nothing to read, and freshness would be a claim with no witness.
 */
export interface FreshnessSpec {
  /**
   * Lag budget in the duration grammar (`"24h"`).
   */
  max_lag: string;
  /**
   * Optional severity for the staleness report.
   */
  severity?: FreshnessSeverity | null;
  /**
   * The timestamp column the staleness check reads.
   */
  time_column: string;
}
/**
 * Grounding sources: exact `catalog.schema.table` triples only.
 *
 * v0 has no globs and no `include` selectors, because no engine counterpart exists. An `include` key is refused as an unknown key; a glob smuggled inside `tables` is refused here.
 */
export interface SourceSpec {
  /**
   * One or more exact table references.
   */
  tables: string[];
}
/**
 * The trust dial. `propose_only` is the only v0 value.
 */
export interface TrustSpec {
  /**
   * How much the agent may do unattended.
   */
  agent: string;
}
