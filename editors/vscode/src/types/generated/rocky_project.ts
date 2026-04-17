/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/rocky_project.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * Schema-only helper that mirrors the deserializer's acceptance of both flat (`[adapter] type = "..."`) and named (`[adapter.foo] type = "..."`) adapter shapes.
 *
 * [`normalize_toml_shorthands`] rewrites the flat form into `adapter.default` before [`RockyConfig`] is deserialized, so the Rust type only sees the named form. The IDE schema, however, validates the raw TOML — so it must accept both shapes directly. 38 of 46 committed POC `rocky.toml` files use the flat form; the schema would reject all of them without this anyOf.
 */
export type AdaptersFieldSchema =
  | AdapterConfig
  | {
      [k: string]: AdapterConfig;
    };
export type RedactedString = string;
/**
 * Role an adapter block plays in the pipeline.
 *
 * Set via `kind = "data"` or `kind = "discovery"` in an `[adapter.*]` block. Required on discovery-only adapter types (`fivetran`, `airbyte`, `iceberg`, `manual`) so the adapter's role is self-evident in the raw config file — a reader shouldn't have to know the Rust trait surface of each adapter to tell whether it moves data.
 *
 * Optional on single-role warehouse types (defaults to `Data`) and on the dual-capable DuckDB adapter (absent means "register both roles").
 */
export type AdapterKind = "data" | "discovery";
/**
 * Supports both single-webhook and multi-webhook syntax per event.
 *
 * Single: `[hook.webhooks.on_pipeline_start]` Multiple: `[[hook.webhooks.on_pipeline_start]]`
 */
export type WebhookConfigOrList = WebhookConfig | WebhookConfig[];
export type FailureAction = "abort" | "warn" | "ignore";
/**
 * Schema-only helper mirroring [`AdaptersFieldSchema`] for `[pipeline.*]`.
 *
 * The flat-pipeline shorthand exists in [`normalize_toml_shorthands`] but is unused across all committed POCs; including it in the schema is defensive — a user typing `[pipeline] source = ...` shouldn't see a false IDE error. The pipeline payload still goes through the [`PipelineConfigSchemaPlaceholder`] until PR-b lands.
 */
export type PipelinesFieldSchema =
  | PipelineConfig
  | {
      [k: string]: PipelineConfig;
    };
/**
 * State storage backend variants.
 */
export type StateBackend = "local" | "s3" | "gcs" | "valkey" | "tiered";

/**
 * Top-level Rocky configuration (v2 format).
 *
 * Uses named adapters and named pipelines: ```toml [adapter.databricks_prod] type = "databricks" ...
 *
 * [pipeline.raw_replication] type = "replication" ... ```
 */
export interface RockyConfig {
  /**
   * Named adapter configurations (keyed by adapter name).
   */
  adapter?: AdaptersFieldSchema;
  /**
   * Cost estimation configuration.
   */
  cost?: CostSection;
  /**
   * Shell hooks configuration.
   */
  hook?: HooksConfig;
  /**
   * Named pipeline configurations (keyed by pipeline name).
   */
  pipeline?: PipelinesFieldSchema;
  /**
   * Schema evolution configuration (grace-period column drops).
   */
  schema_evolution?: SchemaEvolutionConfig;
  /**
   * Global state persistence configuration.
   */
  state?: StateConfig;
}
/**
 * Configuration for a named adapter instance.
 *
 * The `type` field determines which adapter crate handles this config. Adapter-specific fields are captured via `serde(flatten)`.
 *
 * Credential fields (`token`, `client_secret`, `api_key`, `api_secret`, `password`, `oauth_token`) are wrapped in [`RedactedString`] so that `Debug` output never leaks secrets.
 */
export interface AdapterConfig {
  /**
   * Snowflake account identifier (e.g., "xy12345.us-east-1").
   */
  account?: string | null;
  api_key?: RedactedString | null;
  api_secret?: RedactedString | null;
  client_id?: string | null;
  client_secret?: RedactedString | null;
  /**
   * Default database for the session.
   */
  database?: string | null;
  destination_id?: string | null;
  host?: string | null;
  http_path?: string | null;
  /**
   * Role this adapter block plays. See [`AdapterKind`] for the required-vs-optional rules per adapter type.
   */
  kind?: AdapterKind | null;
  /**
   * BigQuery processing location (e.g., "US", "EU", "us-central1").
   */
  location?: string | null;
  /**
   * OAuth access token (pre-obtained from an IdP).
   */
  oauth_token?: RedactedString | null;
  /**
   * Snowflake password (for password auth).
   */
  password?: RedactedString | null;
  /**
   * Optional file path for a persistent DuckDB database. When unset, the adapter uses an in-memory database. A persistent path is required when the same DuckDB adapter is also used as a discovery source — discovery and warehouse share the same database.
   */
  path?: string | null;
  /**
   * Path to RSA private key file (PEM) for key-pair auth.
   */
  private_key_path?: string | null;
  /**
   * Google Cloud project ID.
   */
  project_id?: string | null;
  /**
   * Retry policy for this adapter.
   */
  retry?: RetryConfig;
  /**
   * Snowflake role to use for the session.
   */
  role?: string | null;
  timeout_secs?: number | null;
  token?: RedactedString | null;
  /**
   * Adapter type: "databricks", "fivetran", "duckdb", etc.
   */
  type: string;
  /**
   * Snowflake username (for password or key-pair auth).
   */
  username?: string | null;
  /**
   * Snowflake warehouse to use for query execution.
   */
  warehouse?: string | null;
}
/**
 * Retry policy for transient warehouse errors (HTTP 429/503, rate limits, timeouts).
 *
 * Rocky retries transient errors with exponential backoff and optional jitter to prevent thundering herd across concurrent runs.
 */
export interface RetryConfig {
  /**
   * Backoff multiplier applied after each retry (e.g. 2.0 = double each time).
   */
  backoff_multiplier?: number;
  /**
   * Circuit breaker: trip after this many consecutive transient failures across statements. Once tripped, all subsequent statements fail immediately without attempting execution. Default: 5. Set to 0 to disable.
   */
  circuit_breaker_threshold?: number;
  /**
   * Initial backoff duration in milliseconds before the first retry.
   */
  initial_backoff_ms?: number;
  /**
   * Add random jitter to prevent concurrent runs from retrying in lockstep.
   */
  jitter?: boolean;
  /**
   * Maximum backoff duration in milliseconds (caps exponential growth).
   */
  max_backoff_ms?: number;
  /**
   * Maximum number of retry attempts. Set to 0 to disable retries (e.g. for CI).
   */
  max_retries?: number;
}
/**
 * Cost estimation configuration.
 *
 * Controls pricing assumptions used by [`crate::optimize::recommend_strategy`] when analyzing materialization costs and generating recommendations.
 */
export interface CostSection {
  /**
   * Cost per DBU (default: $0.40).
   */
  compute_cost_per_dbu?: number;
  /**
   * Minimum runs before making cost recommendations.
   */
  min_history_runs?: number;
  /**
   * Cost per GB of storage per month (default: $0.023).
   */
  storage_cost_per_gb_month?: number;
  /**
   * Warehouse size for cost estimation (e.g., "Small", "Medium", "Large").
   */
  warehouse_size?: string;
}
/**
 * The `[hook]` section from rocky.toml.
 *
 * Each key is an event name like `on_pipeline_start`. The value can be either a single hook config (TOML table) or an array of hook configs (TOML array of tables).
 *
 * Webhook entries live under `[hook.webhooks.on_<event>]` or `[[hook.webhooks.on_<event>]]`.
 */
export interface HooksConfig {
  /**
   * Webhook configurations keyed by event name.
   */
  webhooks?: {
    [k: string]: WebhookConfigOrList;
  };
  [k: string]: unknown;
}
/**
 * Configuration for a single webhook endpoint.
 */
export interface WebhookConfig {
  /**
   * If true, fire-and-forget (spawn task, don't await). Default: false.
   */
  async?: boolean;
  /**
   * Mustache-style body template. If None, the full HookContext is serialized as JSON.
   */
  body_template?: string | null;
  /**
   * Additional HTTP headers.
   */
  headers?: {
    [k: string]: string;
  };
  /**
   * HTTP method (default: POST).
   */
  method?: string;
  /**
   * What to do when the webhook fails.
   */
  on_failure?: FailureAction & string;
  /**
   * Optional preset name (e.g., "slack", "pagerduty"). When set, preset defaults are merged before execution.
   */
  preset?: string | null;
  /**
   * Number of retry attempts on failure (default: 0).
   */
  retry_count?: number;
  /**
   * Delay between retries in milliseconds (default: 1000).
   */
  retry_delay_ms?: number;
  /**
   * HMAC-SHA256 signing secret. When set, adds `X-Rocky-Signature: sha256=<hex>` header.
   */
  secret?: string | null;
  /**
   * Request timeout in milliseconds (default: 10000).
   */
  timeout_ms?: number;
  /**
   * Target URL for the webhook request.
   */
  url: string;
}
/**
 * Pipeline configuration. The full per-variant schema is generated by PR-b of the rocky-project-schema-autogen arc — until then any object shape is accepted by the IDE schema and the deserializer continues to validate at parse time.
 */
export interface PipelineConfig {
  [k: string]: unknown;
}
/**
 * Schema evolution configuration.
 *
 * Controls how Rocky handles columns that disappear from the source but still exist in the target table. Instead of immediately dropping them, Rocky can keep them for a grace period (filling with NULL) so downstream consumers have time to adapt.
 */
export interface SchemaEvolutionConfig {
  /**
   * Number of days to keep a dropped column before removing it from the target table. During this window the column is filled with NULL for new rows and a warning is emitted on every run. Default: 7.
   */
  grace_period_days?: number;
}
/**
 * State persistence configuration.
 *
 * Controls where Rocky stores watermarks and anomaly history between runs. On ephemeral environments (EKS pods), use S3, GCS, or Valkey for persistence.
 *
 * When both S3 and Valkey are configured (`backend = "tiered"`): - Download: Valkey first (fast), S3 fallback (durable) - Upload: write to both Valkey + S3
 */
export interface StateConfig {
  /**
   * Storage backend: local (default), s3, gcs, valkey, or tiered (valkey + s3 fallback)
   */
  backend?: StateBackend & string;
  /**
   * GCS bucket for state persistence
   */
  gcs_bucket?: string | null;
  /**
   * GCS key prefix (default: "rocky/state/")
   */
  gcs_prefix?: string | null;
  /**
   * S3 bucket for state persistence
   */
  s3_bucket?: string | null;
  /**
   * S3 key prefix (default: "rocky/state/")
   */
  s3_prefix?: string | null;
  /**
   * Valkey key prefix (default: "rocky:state:")
   */
  valkey_prefix?: string | null;
  /**
   * Valkey/Redis URL for state persistence
   */
  valkey_url?: string | null;
}
