/* eslint-disable */

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
  excluded_tables: ExcludedTableOutput[];
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
  source_type: string;
  tables: TableOutput[];
  [k: string]: unknown;
}
export interface TableOutput {
  name: string;
  row_count?: number | null;
  [k: string]: unknown;
}
