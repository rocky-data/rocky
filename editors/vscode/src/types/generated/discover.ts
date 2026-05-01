/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/discover.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky discover`.
 */
export interface DiscoverOutput {
  /**
   * Pipeline-level data quality check configuration. Present when the pipeline declares a `[checks]` block in `rocky.toml`. Downstream orchestrators (e.g. Dagster) consume this to attach asset-level freshness policies and check expectations without re-reading `rocky.toml` themselves.
   */
  checks?: ChecksConfigOutput | null;
  command: string;
  /**
   * Tables filtered out of `sources` because they were reported by the discovery adapter but do not exist in the source warehouse. Same shape as `RunOutput.excluded_tables` so consumers can use one parser. Empty when nothing was filtered.
   */
  excluded_tables?: ExcludedTableOutput[];
  /**
   * Sources the discovery adapter attempted to fetch metadata for and failed (transient HTTP error, timeout, rate-limit budget exhausted, auth blip). Their absence from `sources` does NOT mean they were removed upstream — consumers diffing against a prior run must treat failed sources as "unknown state, do not delete." Empty when discovery completed cleanly. See FR-014.
   */
  failed_sources?: FailedSourceOutput[];
  /**
   * Number of schema-cache entries written by this invocation.
   *
   * Populated by `rocky discover --with-schemas` — the explicit warm-up path for the schema cache. Zero — and omitted from the wire format — when `--with-schemas` isn't set, so fixtures captured without the flag stay byte-stable.
   */
  schemas_cached?: number;
  sources: SourceOutput[];
  version: string;
  [k: string]: unknown;
}
/**
 * Pipeline-level checks configuration projected into the discover output.
 *
 * This is intentionally a thin projection of `rocky_core::config::ChecksConfig` — only the fields downstream orchestrators currently consume are exposed. Add fields as new integrations need them.
 */
export interface ChecksConfigOutput {
  freshness?: FreshnessConfigOutput | null;
  [k: string]: unknown;
}
/**
 * Freshness check configuration projected into the discover output.
 *
 * Per-schema `overrides` from `rocky_core::config::FreshnessConfig` are intentionally not exposed yet — the override-key semantics need to be nailed down before integrations can rely on them.
 */
export interface FreshnessConfigOutput {
  threshold_seconds: number;
  [k: string]: unknown;
}
/**
 * A table that the discovery adapter reported but that is missing from the source warehouse, so the run skipped it. Tracked separately from `errors` because it is not a runtime failure — the row never made it past the pre-flight existence check.
 */
export interface ExcludedTableOutput {
  /**
   * Dagster-style asset key path (`[source_type, ...components, table]`).
   */
  asset_key: string[];
  /**
   * Why the table was excluded. Currently always `"missing_from_source"` but kept as a free-form field so future causes (disabled, sync_paused, ...) can be added without a schema break.
   */
  reason: string;
  /**
   * Source schema the table was expected to live in.
   */
  source_schema: string;
  /**
   * Bare table name as reported by the discovery adapter.
   */
  table_name: string;
  [k: string]: unknown;
}
/**
 * A source the discovery adapter attempted to fetch metadata for and failed.
 *
 * Surfaced on `DiscoverOutput.failed_sources` so downstream consumers can distinguish a transient fetch failure from a deletion when diffing successive discover snapshots (FR-014).
 */
export interface FailedSourceOutput {
  /**
   * Coarse error class so consumers can branch without parsing the `message`. One of `"transient"` / `"timeout"` / `"rate_limit"` / `"auth"` / `"unknown"`.
   */
  error_class: string;
  /**
   * Adapter-side identifier for the source (e.g. Fivetran connector_id).
   */
  id: string;
  /**
   * Human-readable error from the adapter — for logs / debugging only. Don't pattern-match on this; use `error_class` for branching.
   */
  message: string;
  /**
   * Source schema name the adapter would have written into.
   */
  schema: string;
  /**
   * Adapter type (`"fivetran"`, `"airbyte"`, `"iceberg"`, ...).
   */
  source_type: string;
  [k: string]: unknown;
}
/**
 * A discovered source (connector, schema, integration — terminology varies by adapter).
 */
export interface SourceOutput {
  /**
   * Schema pattern components parsed from the source schema name. Keys are the component names from the schema pattern config. e.g., {"tenant": "acme", "regions": ["us_west"], "source": "shopify"}
   */
  components: {
    [k: string]: unknown;
  };
  id: string;
  last_sync_at?: string | null;
  /**
   * Adapter-namespaced metadata surfaced by the discovery adapter.
   *
   * Keys are conventionally prefixed with the adapter kind (e.g. `fivetran.service`, `fivetran.connector_id`, `fivetran.custom_reports`, `fivetran.custom_tables`, `fivetran.schema_prefix`) so entries from different adapters don't collide when an orchestrator folds them into an asset graph. Values are opaque `serde_json::Value` — Rocky relays service-specific payloads rather than modelling them.
   *
   * Empty for adapters that haven't opted in; the field is skipped from the wire format in that case so existing fixtures stay byte-stable. Populated per-adapter in the discover command — see [`rocky_core::source::DiscoveredConnector::metadata`].
   */
  metadata?: {
    [k: string]: unknown;
  };
  source_type: string;
  tables: TableOutput[];
  [k: string]: unknown;
}
export interface TableOutput {
  name: string;
  row_count?: number | null;
  [k: string]: unknown;
}
