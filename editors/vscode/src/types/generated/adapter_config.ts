/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/adapter_config.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

export type RedactedString = string;
/**
 * Cache backend selector for [`FivetranCacheConfig`]. See the parent struct for the TOML shape and per-backend semantics.
 */
export type FivetranCacheBackend = "none" | "file" | "object_store" | "valkey" | "tiered";
/**
 * Role an adapter block plays in the pipeline.
 *
 * Set via `kind = "data"` or `kind = "discovery"` in an `[adapter.*]` block. Required on discovery-only adapter types (`fivetran`, `airbyte`, `iceberg`, `manual`) so the adapter's role is self-evident in the raw config file — a reader shouldn't have to know the Rust trait surface of each adapter to tell whether it moves data.
 *
 * Optional on single-role warehouse types (defaults to `Data`) and on the dual-capable DuckDB adapter (absent means "register both roles").
 */
export type AdapterKind = "data" | "discovery";

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
  /**
   * Optional cache backend for the Fivetran state envelope.
   *
   * When set on a `type = "fivetran"` adapter, the resolved envelope is read from and written to the configured cache backend so concurrent `rocky` processes share one fetcher per org. Ignored on every other adapter type. When absent the adapter behaves as if `backend = "none"` — every fetch goes straight to the Fivetran API.
   */
  cache?: FivetranCacheConfig | null;
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
   * Programmatic Access Token (issued via Snowsight User Profile). Sent as a Bearer token with the `PROGRAMMATIC_ACCESS_TOKEN` token-type header — distinct from `oauth_token`.
   */
  pat?: RedactedString | null;
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
 * Persistent state cache configuration for the Fivetran adapter (FR-A).
 *
 * Cache backends shared across processes let several `rocky` invocations against one Fivetran org dedupe their discover fetches — the first process pays the API cost, every subsequent process within the TTL window reads the canonical envelope from the cache. See `engine/crates/rocky-fivetran/src/state_cache/` for the implementation details.
 *
 * ## TOML shape
 *
 * ```toml [adapter.fivetran.cache] backend = "tiered"                              # "none" | "file" | "object_store" | "valkey" | "tiered" file_root = ".rocky/fivetran-state/"            # required for backend = "file" object_store_url = "s3://my-bucket/rocky/fv/"   # required for backend = "object_store" / "tiered" valkey_url = "rediss://valkey:6379/"            # required for backend = "valkey" / "tiered" valkey_ttl_seconds = 600                        # default 600 ```
 *
 * ## Backend selection
 *
 * - `none` — disable the cache; default when the block is absent. - `file` — local-filesystem JSON files under `file_root`. - `object_store` — S3 / GCS / Azure / `file://`; URL parsed by `object_store::parse_url`. Credentials come from the SDK default chain (`AWS_*` env vars, IAM role, `GOOGLE_APPLICATION_CREDENTIALS`, etc.) — Rocky doesn't introduce its own credential surface. - `valkey` — Redis / Valkey; requires building rocky-fivetran with the `valkey` Cargo feature. URL accepts `redis://` and `rediss://` (TLS). - `tiered` — composes Valkey (primary, fast) + object-store (secondary, durable). Requires both URLs.
 */
export interface FivetranCacheConfig {
  /**
   * Which backend to instantiate. Defaults to [`FivetranCacheBackend::None`] so a stub `[adapter.fivetran.cache]` block with no `backend` key is well-defined.
   */
  backend?: FivetranCacheBackend & string;
  /**
   * Root directory for `backend = "file"`. Required for that backend; ignored otherwise. May contain `${VAR}` / `${VAR:-default}` env-var references; resolved at config-load time.
   */
  file_root?: string | null;
  /**
   * URL passed to `object_store::parse_url` for `backend = "object_store"` or `backend = "tiered"` (secondary layer). Examples: `s3://bucket/prefix/`, `gs://bucket/prefix/`, `az://container/prefix/`, `file:///path/`.
   */
  object_store_url?: string | null;
  /**
   * TTL applied to every `SET` against the Valkey backend. Defaults to 600s when unset. Ignored for non-Valkey backends.
   */
  valkey_ttl_seconds?: number | null;
  /**
   * Connection URL for `backend = "valkey"` or `backend = "tiered"` (primary layer). Standard `redis://` (plain) or `rediss://` (TLS).
   */
  valkey_url?: string | null;
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
   * Seconds the breaker will stay `Open` before a single trial request is allowed through (half-open state). On trial success the breaker closes and resumes normal traffic; on trial failure it re-opens immediately. `None` preserves the pre-Arc-3 "manual-reset-only" behaviour — a tripped breaker stays tripped for the rest of the run.
   */
  circuit_breaker_recovery_timeout_secs?: number | null;
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
  /**
   * Cross-statement retry budget for a single run (§P2.7). When set, adapters construct a [`crate::retry_budget::RetryBudget`] from this value and decrement it on every retry; once exhausted, remaining statements fail fast with adapter-specific `RetryBudgetExhausted` errors instead of burning the warehouse's rate-limit quota.
   *
   * `None` (default) keeps legacy behaviour — per-statement [`RetryConfig::max_retries`] is the only bound. `Some(0)` means no retries are allowed for the whole run.
   */
  max_retries_per_run?: number | null;
}
