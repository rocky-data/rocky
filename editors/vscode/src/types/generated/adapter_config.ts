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
 * Circuit-breaker backend selector for the Fivetran adapter (Layer 3). See [`FivetranCircuitBreakerConfig`] for the TOML shape.
 */
export type FivetranCircuitBreakerBackend = "none" | "valkey";
/**
 * Role an adapter block plays in the pipeline.
 *
 * Set via `kind = "data"` or `kind = "discovery"` in an `[adapter.*]` block. Required on discovery-only adapter types (`fivetran`, `airbyte`, `iceberg`, `manual`) so the adapter's role is self-evident in the raw config file — a reader shouldn't have to know the Rust trait surface of each adapter to tell whether it moves data.
 *
 * Optional on single-role warehouse types (defaults to `Data`) and on the dual-capable DuckDB adapter (absent means "register both roles").
 */
export type AdapterKind = "data" | "discovery";
/**
 * Cross-pod rate-limit budget backend selector for the Fivetran adapter (FR-B Phase 2). See [`FivetranRatelimitConfig`] for the TOML shape.
 */
export type FivetranRatelimitBackend = "file" | "valkey";
/**
 * Distributed cache-stampede lock backend selector for the Fivetran adapter (Layer 1). See [`FivetranStampedeConfig`] for the TOML shape.
 */
export type FivetranStampedeBackend = "none" | "valkey";

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
  /**
   * Optional per-account circuit breaker (Fivetran-only).
   *
   * Trips after `failure_threshold` consecutive remote failures and short-circuits subsequent HTTP attempts with a `CircuitOpen` error until a cooldown elapses. Coordinated across processes via the configured backend (Valkey). Ignored on non-fivetran adapters; absent block defaults to `AlwaysClosed` (no breaker).
   */
  circuit_breaker?: FivetranCircuitBreakerConfig | null;
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
   * Optional cross-pod rate-limit budget backend (Fivetran-only).
   *
   * Phase 1 ratelimit coordination is per-host (file in `${TMPDIR}/rocky-fivetran-ratelimit/`). Setting `backend = "valkey"` here lifts the budget into a shared store so several pods on different hosts observe the same `wake_at` window after one of them is throttled. Ignored on non-fivetran adapters. When absent the adapter falls back to the per-host file backend.
   */
  ratelimit?: FivetranRatelimitConfig | null;
  /**
   * Retry policy for this adapter.
   */
  retry?: RetryConfig;
  /**
   * Snowflake role to use for the session.
   */
  role?: string | null;
  /**
   * Optional distributed cache-stampede lock (Fivetran-only).
   *
   * On a cold-start herd, N processes simultaneously miss the cache, fan out N API calls, and write back N times. The stampede lock elects a single leader to issue the API call; followers poll the cache until the leader publishes the envelope. Ignored on non-fivetran adapters. When absent the adapter behaves as if every process is the leader (the pre-stampede behavior).
   */
  stampede?: FivetranStampedeConfig | null;
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
 * Circuit-breaker config for the Fivetran adapter (Layer 3).
 *
 * Trips after `failure_threshold` consecutive remote failures (5xx, network errors, exhausted retries) and short-circuits subsequent HTTP attempts with `FivetranError::CircuitOpen` until `cooldown_seconds` elapses. State transitions follow the standard `Closed → Open → HalfOpen → Closed` pattern, with exponentially extended cooldown on repeated half-open failures (capped at `cooldown_max_seconds`).
 *
 * State is shared across processes via Valkey so a Fivetran outage trips one breaker for the entire org rather than each process independently tripping its own.
 *
 * ## TOML shape
 *
 * ```toml [adapter.fivetran.circuit_breaker] backend = "valkey" valkey_url = "rediss://valkey:6379/" failure_threshold = 5 window_seconds = 60 cooldown_seconds = 300 cooldown_max_seconds = 3600 ```
 *
 * ## Fail-open
 *
 * When the state store is unreachable the client behaves as if the breaker is `Closed` — coordination failure must not refuse live traffic.
 */
export interface FivetranCircuitBreakerConfig {
  /**
   * Which backend to instantiate. Defaults to [`FivetranCircuitBreakerBackend::None`] so a stub block with no `backend` key is well-defined.
   */
  backend?: FivetranCircuitBreakerBackend & string;
  /**
   * Upper bound on the exponentially-extended cooldown after repeated half-open failures. Defaults to 3600s.
   */
  cooldown_max_seconds?: number | null;
  /**
   * Initial cooldown (seconds) before the breaker transitions `Open → HalfOpen` for a probe. Defaults to 300s.
   */
  cooldown_seconds?: number | null;
  /**
   * Consecutive failures required to transition `Closed → Open`. Defaults to 5 when unset.
   */
  failure_threshold?: number | null;
  /**
   * Connection URL for `backend = "valkey"`. Standard `redis://` (plain) or `rediss://` (TLS).
   */
  valkey_url?: string | null;
  /**
   * Failure-counting window in seconds. Failures older than the window are not counted toward `failure_threshold`. Defaults to 60s when unset.
   */
  window_seconds?: number | null;
}
/**
 * Cross-pod rate-limit budget config for the Fivetran adapter (FR-B Phase 2).
 *
 * Phase 1 (shipped in v1.37) writes `wake_at_epoch_ms` to a per-host file under `${TMPDIR}/rocky-fivetran-ratelimit/<account_hash>.json`. That works when several `rocky` processes share a host, but a Kubernetes deployment with one rocky-cli per pod sees N independent budgets even though every pod talks to the same Fivetran org. Lifting the budget into Valkey makes a 429 on one pod throttle every other pod for the same `Retry-After` window.
 *
 * ## TOML shape
 *
 * ```toml [adapter.fivetran.ratelimit] backend = "valkey" valkey_url = "rediss://valkey:6379/" ```
 *
 * ## Fail-open
 *
 * When the configured backend is unreachable the client falls back to the per-host file backend. The cluster regresses to Phase 1 behavior (each host enforces its own budget) rather than blocking traffic.
 */
export interface FivetranRatelimitConfig {
  /**
   * Which backend to instantiate. Defaults to [`FivetranRatelimitBackend::File`] so a stub block with no `backend` key is well-defined.
   */
  backend?: FivetranRatelimitBackend & string;
  /**
   * Cap (seconds) applied to the Valkey key's `EXPIRE` so an abandoned `wake_at` value never outlives its useful window. Defaults to 600s when unset; ignored for the file backend.
   */
  max_wake_seconds?: number | null;
  /**
   * Connection URL for `backend = "valkey"`. Standard `redis://` (plain) or `rediss://` (TLS).
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
   *
   * When the Fivetran shared circuit breaker is enabled (`[adapter.fivetran.circuit_breaker]` with a non-default backend), keep `max_retries` ≤ 4 so a single 429-storm bursts at most ~5 attempts (`max_retries + 1`) per envelope-fetch before voting `Remote` to the breaker. Higher values lengthen the storm without changing the outcome — the breaker still trips after `failure_threshold` envelope-fetches exhaust their retry budget, and the orchestrator only sees the result after that.
   */
  max_retries?: number;
  /**
   * Cross-statement retry budget for a single run (§P2.7). When set, adapters construct a [`crate::retry_budget::RetryBudget`] from this value and decrement it on every retry; once exhausted, remaining statements fail fast with adapter-specific `RetryBudgetExhausted` errors instead of burning the warehouse's rate-limit quota.
   *
   * `None` (default) keeps legacy behaviour — per-statement [`RetryConfig::max_retries`] is the only bound. `Some(0)` means no retries are allowed for the whole run.
   */
  max_retries_per_run?: number | null;
}
/**
 * Distributed cache-stampede protection config for the Fivetran adapter (Layer 1).
 *
 * On a cold-start burst, N processes can simultaneously observe the cache miss, fan out N API calls, and write back N copies of the same envelope. The stampede lock elects a single leader (via `SET <key>:lock <id> NX EX <ttl>` against Valkey) to do the API call; followers wait on the cache key with bounded polling until the leader's write becomes visible, then return.
 *
 * ## TOML shape
 *
 * ```toml [adapter.fivetran.stampede] backend = "valkey" valkey_url = "rediss://valkey:6379/" lock_ttl_seconds = 60 poll_timeout_seconds = 30 ```
 *
 * ## Fail-open
 *
 * When the lock backend is unreachable the client falls through to the direct HTTP path (no stampede protection, but no block).
 */
export interface FivetranStampedeConfig {
  /**
   * Which backend to instantiate. Defaults to [`FivetranStampedeBackend::None`] so a stub block with no `backend` key is well-defined.
   */
  backend?: FivetranStampedeBackend & string;
  /**
   * Lock TTL applied to the `SET NX EX <ttl>` call. The lock auto-expires after this many seconds so a crashed leader doesn't park every follower forever. Defaults to 60s.
   */
  lock_ttl_seconds?: number | null;
  /**
   * Hard cap on how long a follower will poll the cache before falling through to a direct HTTP fetch. Defaults to 30s.
   */
  poll_timeout_seconds?: number | null;
  /**
   * Connection URL for `backend = "valkey"`. Standard `redis://` (plain) or `rediss://` (TLS).
   */
  valkey_url?: string | null;
}
