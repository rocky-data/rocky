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
 * What to do when a [`BudgetConfig`] limit is exceeded by an actual run.
 *
 * `Warn` always fires the `budget_breach` event; `Error` additionally causes `rocky run` to exit with a non-zero status so orchestrators can gate downstream work on the breach.
 */
export type BudgetBreachAction = "warn" | "error";
/**
 * Supports both single-webhook and multi-webhook syntax per event.
 *
 * Single: `[hook.webhooks.on_pipeline_start]` Multiple: `[[hook.webhooks.on_pipeline_start]]`
 */
export type WebhookConfigOrList = WebhookConfig | WebhookConfig[];
export type FailureAction = "abort" | "warn" | "ignore";
/**
 * One entry in the top-level `[mask]` block. A scalar value (`pii = "hash"`) binds a classification tag to a default masking strategy; a nested table (`[mask.prod] pii = "none"`) overrides strategies for a specific environment.
 *
 * Serde deserializes the outer `[mask]` map as `BTreeMap<String, MaskEntry>`; scalars are tried first, then the nested table shape. Unknown strategy spellings (e.g., `"mask"`) hard-fail at config load time — Rocky never silently accepts something it can't emit SQL for.
 */
export type MaskEntry =
  | MaskStrategy
  | {
      [k: string]: MaskStrategy;
    };
/**
 * Column-masking strategy applied to a classified column at materialization time.
 *
 * Rocky translates a classification tag (e.g., `pii`) into one of these strategies via the `[mask]` / `[mask.<env>]` block in `rocky.toml`. The adapter renders each strategy as a warehouse-native function: Databricks uses `CREATE MASK ... RETURN <expr>` + `SET MASKING POLICY`; other adapters default-unsupported until demand.
 *
 * Serialized in lowercase to match the TOML spelling (`"hash"`, `"redact"`, `"partial"`, `"none"`).
 */
export type MaskStrategy = "hash" | "redact" | "partial" | "none";
/**
 * Schema-only helper mirroring [`AdaptersFieldSchema`] for `[pipeline.*]`.
 *
 * The flat-pipeline shorthand exists in [`normalize_toml_shorthands`] but is unused across all committed POCs; including it in the schema is defensive — a user typing `[pipeline] source = ...` shouldn't see a false IDE error. The pipeline payload references [`PipelineConfig`] directly, whose hand-written `JsonSchema` impl emits an `anyOf` of the five pipeline variants.
 */
export type PipelinesFieldSchema =
  | PipelineConfig
  | {
      [k: string]: PipelineConfig;
    };
/**
 * Pipeline configuration. The `type` field selects one of five variants — `replication` (default when omitted), `transformation`, `quality`, `snapshot`, or `load`. Each variant has its own field set; see the per-variant subschemas in `definitions`.
 */
export type PipelineConfig =
  | (ReplicationPipelineConfig & {
      type?: "replication";
      [k: string]: unknown;
    })
  | (TransformationPipelineConfig & {
      type: "transformation";
      [k: string]: unknown;
    })
  | (QualityPipelineConfig & {
      type: "quality";
      [k: string]: unknown;
    })
  | (SnapshotPipelineConfig & {
      type: "snapshot";
      [k: string]: unknown;
    })
  | (LoadPipelineConfig & {
      type: "load";
      [k: string]: unknown;
    });
/**
 * A single row-level assertion attached to a quality pipeline, scoped to one table in the pipeline's `tables` list.
 *
 * ```toml [[pipeline.nightly_dq.checks.assertions]] name     = "orders_customer_id_not_null"  # optional table    = "orders" type     = "not_null" column   = "customer_id" severity = "error" ```
 *
 * The `type`-specific fields (and `column`, `severity`) are flattened from `TestDecl` — the same surface used by declarative model tests.
 */
export type QualityAssertion = {
  /**
   * Column under test. Required for `not_null`, `unique`, `accepted_values`, `relationships`, `in_range`, `regex_match`. Ignored for `expression` and `row_count_range`.
   */
  column?: string | null;
  /**
   * Optional SQL boolean predicate that scopes the assertion to a subset of rows. When set, only rows where `(filter)` evaluates to `TRUE` are subject to the assertion — rows where the filter is `FALSE` or `NULL` pass unconditionally.
   *
   * Filter is user-supplied SQL; the caller is responsible for sandboxing execution (same contract as `expression`).
   *
   * Example: `filter = "created_at > current_date - interval 30 day"` restricts a `not_null` check to rows created in the last 30 days.
   */
  filter?: string | null;
  /**
   * Optional identifier used as the `CheckResult.name` in the JSON output. When unset, a synthesized `"{kind}:{column}"` name is used — which can collide if multiple assertions share the same table, kind, and column. Set `name` explicitly to disambiguate.
   */
  name?: string | null;
  /**
   * Severity of failure. Defaults to `error`.
   */
  severity?: TestSeverity & string;
  /**
   * Table name this assertion applies to. Must match a table discovered from one of the pipeline's `[[tables]]` entries (by unqualified table name).
   */
  table: string;
  [k: string]: unknown;
} & QualityAssertion1;
/**
 * Severity of a test failure.
 */
export type TestSeverity = "error" | "warning";
export type QualityAssertion1 =
  | {
      type: "not_null";
      [k: string]: unknown;
    }
  | {
      type: "unique";
      [k: string]: unknown;
    }
  | {
      type: "accepted_values";
      /**
       * The allowed values. Compared as string literals.
       */
      values: string[];
      [k: string]: unknown;
    }
  | {
      /**
       * Column in the target table to join against.
       */
      to_column: string;
      /**
       * Fully-qualified target table (`catalog.schema.table`).
       */
      to_table: string;
      type: "relationships";
      [k: string]: unknown;
    }
  | {
      /**
       * A SQL boolean expression. Rows where `NOT (expression)` are failures.
       */
      expression: string;
      type: "expression";
      [k: string]: unknown;
    }
  | {
      /**
       * Maximum row count (inclusive). `None` means no upper bound.
       */
      max?: number | null;
      /**
       * Minimum row count (inclusive). `None` means no lower bound.
       */
      min?: number | null;
      type: "row_count_range";
      [k: string]: unknown;
    }
  | {
      /**
       * Maximum value (inclusive). `None` means no upper bound.
       */
      max?: string | null;
      /**
       * Minimum value (inclusive). `None` means no lower bound.
       */
      min?: string | null;
      type: "in_range";
      [k: string]: unknown;
    }
  | {
      /**
       * The regex pattern. Dialect-specific syntax — stick to the portable subset (character classes, anchors, quantifiers).
       */
      pattern: string;
      type: "regex_match";
      [k: string]: unknown;
    }
  | {
      /**
       * Comparison between `op(column)` and `value`. The check passes when the comparison is true.
       */
      cmp: AggregateCmp;
      /**
       * Aggregate operator.
       */
      op: AggregateOp;
      type: "aggregate";
      /**
       * Threshold to compare against. Parsed as `f64`.
       */
      value: string;
      [k: string]: unknown;
    }
  | {
      /**
       * Columns that together form the key. Must have at least two entries (single-column uniqueness is covered by `Unique`).
       */
      columns: string[];
      /**
       * The kind of composite assertion. Currently `unique` only — kept as an enum to leave room for `not_null_any` / `not_null_all` in a later phase without another TestType.
       */
      kind: CompositeKind;
      type: "composite";
      [k: string]: unknown;
    }
  | {
      type: "not_in_future";
      [k: string]: unknown;
    }
  | {
      /**
       * N — days in the past. Must be > 0.
       */
      days: number;
      type: "older_than_n_days";
      [k: string]: unknown;
    };
/**
 * Comparison operator for `Aggregate` assertions. Each comparison has a long form (`lt`, `lte`, `gt`, `gte`, `eq`, `ne`) and an equivalent symbolic alias (`<`, `<=`, `>`, `>=`, `==`, `!=`).
 */
export type AggregateCmp = "lt" | "<" | "lte" | "<=" | "gt" | ">" | "gte" | ">=" | "eq" | "==" | "ne" | "!=";
/**
 * Aggregate operator used by [`TestType::Aggregate`].
 */
export type AggregateOp = "sum" | "count" | "avg" | "min" | "max";
/**
 * Kind of composite (multi-column) assertion.
 */
export type CompositeKind = "unique";
/**
 * Per-check-kind toggle that accepts either a plain boolean (legacy form) or a struct with `enabled` + `severity`.
 *
 * ```toml # Legacy — still supported row_count = true
 *
 * # New — per-check severity [pipeline.x.checks.row_count] enabled  = true severity = "warning" ```
 */
export type AggregateCheckToggle =
  | boolean
  | {
      enabled?: boolean;
      severity?: TestSeverity & string;
      [k: string]: unknown;
    };
/**
 * How to handle rows that fail error-severity row-level assertions.
 */
export type QuarantineMode = "split" | "tag" | "drop";
/**
 * Concurrency strategy: the literal `"adaptive"` for AIMD-based throttling, or a positive integer for fixed concurrency.
 */
export type ConcurrencyMode = "adaptive" | number;
/**
 * Workspace binding access level.
 */
export type BindingType = "READ_WRITE" | "READ_ONLY";
/**
 * File format for load pipelines, parsed from TOML.
 *
 * Mirrors `rocky_adapter_sdk::FileFormat` but lives in rocky-core to avoid a hard dependency from config parsing to the adapter SDK.
 */
export type LoadFileFormat = "csv" | "parquet" | "json_lines";
/**
 * Target dialect for transpilation.
 *
 * Serializes lowercase (`databricks`, `snowflake`, `bigquery`, `duckdb`) so the long-form names can sit in `rocky.toml` under the `[portability]` block without translation. The CLI's short-form flag values (`dbx`/`sf`/`bq`/`duckdb`) are kept as ergonomics in the `TargetDialect` clap enum and convert to this type at the boundary.
 */
export type Dialect = "databricks" | "snowflake" | "bigquery" | "duckdb";
/**
 * State storage backend variants.
 */
export type StateBackend = "local" | "s3" | "gcs" | "valkey" | "tiered";
/**
 * Policy controlling which terminal outcomes count for [`IdempotencyConfig::dedup_on`].
 */
export type DedupPolicy = "success" | "any";
/**
 * Policy applied when state upload fails after retries + circuit-breaker are exhausted. See [`StateConfig::on_upload_failure`].
 */
export type StateUploadFailureMode = "skip" | "fail";
/**
 * Domain identifier for the state-store retention sweep.
 *
 * The wire format is a lowercase string in `applies_to` — the [`Display`] impl produces the canonical spelling, and the `from_str` impl accepts only that spelling (no aliases).
 *
 * [`Display`]: std::fmt::Display
 */
export type StateRetentionDomain = "history" | "lineage" | "audit";

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
   * AI intent layer configuration. Currently scopes the per-request and cumulative-retry token budget for `rocky ai` / `ai-explain` / `ai-sync` / `ai-test`. See [`AiSection`].
   */
  ai?: AiSection;
  /**
   * Declarative run-level budget. See [`BudgetConfig`] for the semantics of each limit and the breach action.
   */
  budget?: BudgetConfig;
  /**
   * Project-level cache configuration. Today this is just `[cache.schemas]` (schema cache for `DESCRIBE TABLE` results); future cache surfaces live as sibling fields under [`CacheConfig`].
   */
  cache?: CacheConfig;
  /**
   * Advisory settings for column classification — currently just the `allow_unmasked` list that suppresses W004 warnings.
   */
  classifications?: ClassificationsConfig;
  /**
   * Cost estimation configuration.
   */
  cost?: CostSection;
  /**
   * Shell hooks configuration.
   */
  hook?: HooksConfig;
  /**
   * Workspace-default column-masking strategies plus optional per-env overrides. See [`MaskEntry`] for the TOML shape:
   *
   * ```toml [mask] pii = "hash"            # default strategy for "pii" classification confidential = "redact" # default strategy for "confidential"
   *
   * [mask.prod] pii = "none"            # prod override: do not mask pii confidential = "partial" ```
   *
   * Resolved per model at apply time via [`RockyConfig::resolve_mask_for_env`].
   */
  mask?: {
    [k: string]: MaskEntry;
  };
  /**
   * Named pipeline configurations (keyed by pipeline name).
   */
  pipeline?: PipelinesFieldSchema;
  /**
   * Dialect-portability lint configuration. Consumed by `rocky compile` to drive P001 (and, when wired, future) diagnostics. The CLI's `--target-dialect` flag, when set, takes precedence over [`PortabilityConfig::target_dialect`].
   */
  portability?: PortabilityConfig;
  /**
   * Run-level retry budget shared across every adapter for this run.
   *
   * When set, `rocky run` builds a single [`crate::retry_budget::RetryBudget`] from [`RunRetryConfig::max_retries_per_run`] and passes it to every connector via `with_retry_budget(...)`. One bad table that burns through retries on adapter A then has less budget available for adapter B's retries — the protection §P2.7 added within a single adapter now extends across the whole run.
   *
   * Unset (the default) preserves per-adapter semantics: each adapter still honours its own `retry.max_retries_per_run` independently. That's the backward-compatible path and stays the right choice when adapters have wildly different rate limits.
   */
  retry?: RunRetryConfig | null;
  /**
   * Hierarchical role declarations reconciled against the warehouse's native role/group system.
   *
   * See [`RoleConfig`] for the TOML shape and [`crate::role_graph::flatten_role_graph`] for the inheritance resolution semantics (DAG walk with cycle detection).
   */
  role?: {
    [k: string]: RoleConfig;
  };
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
/**
 * Configuration for the AI intent layer (`rocky ai`, `rocky ai-explain`, `rocky ai-sync`, `rocky ai-test`).
 *
 * `max_tokens` doubles as: 1. The per-request `max_tokens` cap on the Anthropic Messages API. 2. The cumulative output-token budget across the compile-verify retry loop — when the running total exceeds this value, the loop fail-stops instead of issuing another retry. This bounds the worst-case spend when the LLM produces runaway responses that fail validation.
 *
 * The default ([`DEFAULT_AI_MAX_TOKENS`]) preserves Rocky's pre-1.x hard-coded behaviour. Increase for projects that legitimately need longer generations (large model surfaces, verbose tests).
 *
 * ```toml [ai] max_tokens = 8192 ```
 */
export interface AiSection {
  /**
   * Per-request `max_tokens` and cumulative output-token budget across retries. Default [`DEFAULT_AI_MAX_TOKENS`].
   */
  max_tokens?: number;
}
/**
 * Declarative run-level budget for cost, duration, and data volume. All limits are optional; when unset the dimension is not enforced.
 *
 * A breach is detected at end of run by comparing [`BudgetConfig`] against the observed [`crate::cost::compute_observed_cost_usd`] total, the run wall clock, and the aggregate `bytes_scanned` summed across every materialization. Limits are independent and composed with all-OR — any single dimension breach trips the `budget_breach` event. Per-model budgets are deferred to a later wave; the first iteration enforces run-level totals only.
 */
export interface BudgetConfig {
  /**
   * Maximum allowed total bytes scanned across every materialization in the run. Useful for CI gates that want to fail when a regression bloats scan volume even if the dollar cost stays within `max_usd` (e.g. a BigQuery query that suddenly stops pruning partitions).
   *
   * Aggregated from per-model `bytes_scanned` figures the adapter reports — today that's BigQuery's `totalBytesBilled`; Databricks / Snowflake / DuckDB still inherit `None`, in which case the dimension is skipped rather than treated as zero (matching `max_usd`).
   */
  max_bytes_scanned?: number | null;
  /**
   * Maximum allowed run wall-clock duration in milliseconds.
   */
  max_duration_ms?: number | null;
  /**
   * Maximum allowed run cost in USD. When set and exceeded, emits `budget_breach` on the event bus; when paired with `on_breach = "error"`, also fails the run.
   */
  max_usd?: number | null;
  /**
   * What to do when a limit is breached. Defaults to `warn` — fire the event, keep the run successful. Set to `error` to fail the run.
   */
  on_breach?: BudgetBreachAction & string;
}
/**
 * Top-level `[cache]` configuration.
 *
 * Holds every cache surface that lives at the project level (i.e. has a `rocky.toml` knob). Today that's only the schema cache, but the shape is deliberately extensible: if a future `[cache.query]` or `[cache.plan]` surfaces, it lands as a new field on this struct with a `#[serde(default)]` attribute and its own `*Config` type — no breaking change to existing `rocky.toml` files.
 */
export interface CacheConfig {
  /**
   * Schema cache. Stores `DESCRIBE TABLE` results in `state.redb` so leaf models typecheck against real warehouse types without a live round-trip on every compile.
   */
  schemas?: SchemaCacheConfig;
}
/**
 * `[cache.schemas]` — schema cache configuration.
 *
 * Controls the DESCRIBE-result cache. Defaults are chosen so the feature is useful out of the box: the cache is on, entries live for 24 hours, and nothing replicates off-machine until the user opts in.
 */
export interface SchemaCacheConfig {
  /**
   * Enable schema cache reads + writes. Defaults to `true`. Set to `false` for strict CI where every typecheck should resolve against the current warehouse and never fall back to a cached entry.
   */
  enabled?: boolean;
  /**
   * Replicate the schema cache via `state_sync` to the remote backend. Defaults to `false`: a dev on a fresh clone should not inherit another machine's stale type stamps. Opt in to `true` for teams that want cross-machine cache warm-up via a shared state backend.
   */
  replicate?: boolean;
  /**
   * TTL for cache entries in seconds. Defaults to 86400 (24 hours). Lower it for high-DDL-churn teams; raise it for projects whose sources change on a weekly or slower cadence.
   */
  ttl_seconds?: number;
}
/**
 * Advisory settings for column classification.
 *
 * ```toml [classifications] allow_unmasked = ["internal"] ```
 *
 * Any classification tag listed in `allow_unmasked` suppresses the W004 "tag has no masking strategy" compiler warning. This is the escape hatch for teams that want to tag columns for discovery/lineage without requiring a matching `[mask]` strategy for every tag.
 */
export interface ClassificationsConfig {
  /**
   * Classification tags that are allowed to appear in a model's `[classification]` block without a corresponding `[mask]` strategy.
   */
  allow_unmasked?: string[];
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
 * Replication pipeline configuration.
 *
 * Copies tables from a source to a target using schema pattern discovery, with optional incremental strategy, metadata columns, governance, and data quality checks.
 */
export interface ReplicationPipelineConfig {
  /**
   * Data quality checks.
   */
  checks?: ChecksConfig;
  /**
   * Pipeline dependencies for chaining (Phase 4).
   */
  depends_on?: string[];
  /**
   * Execution settings (concurrency, retries, etc.).
   */
  execution?: ExecutionConfig;
  /**
   * Metadata columns added during replication.
   */
  metadata_columns?: MetadataColumnConfig[];
  /**
   * Source configuration.
   */
  source: PipelineSourceConfig;
  /**
   * Replication strategy: "incremental" or "full_refresh".
   */
  strategy?: string;
  /**
   * Target configuration.
   */
  target: PipelineTargetConfig;
  /**
   * Timestamp column for incremental strategy.
   */
  timestamp_column?: string;
  [k: string]: unknown;
}
/**
 * Data quality checks configuration (row count, column match, freshness, null rate, custom).
 */
export interface ChecksConfig {
  /**
   * Row count anomaly detection threshold (percentage deviation from baseline). Default: 50.0 (50% deviation triggers anomaly). Set to 0 to disable.
   */
  anomaly_threshold_pct?: number;
  /**
   * Row-level assertions (`not_null`, `unique`, `accepted_values`, `relationships`, `expression`, `row_count_range`) executed against specific tables in the quality pipeline. Each entry targets a single table by name and reuses the `TestDecl` surface from declarative model tests.
   */
  assertions?: QualityAssertion[];
  column_match?: AggregateCheckToggle & boolean;
  custom?: CustomCheckConfig[];
  enabled?: boolean;
  /**
   * When `true` (default), the quality run exits non-zero if any error-severity check fails. Set `false` to force the pipeline to always succeed and leave failure handling to downstream consumers of the JSON output.
   */
  fail_on_error?: boolean;
  freshness?: FreshnessConfig | null;
  null_rate?: NullRateConfig | null;
  /**
   * Row quarantine — split/tag/drop rows that violate error-severity row-level assertions. Disabled when unset. See [`QuarantineConfig`].
   */
  quarantine?: QuarantineConfig | null;
  row_count?: AggregateCheckToggle & boolean;
}
/**
 * A user-defined SQL check with a name, query template, and pass/fail threshold.
 */
export interface CustomCheckConfig {
  name: string;
  /**
   * Severity reported when this check fails.
   */
  severity?: TestSeverity & string;
  sql: string;
  threshold?: number;
}
/**
 * Freshness check configuration with optional per-schema overrides.
 */
export interface FreshnessConfig {
  /**
   * Per-schema freshness overrides. Key is a schema pattern (e.g., "raw__us_west__shopify"), value overrides threshold_seconds for matching schemas.
   */
  overrides?: {
    [k: string]: number;
  };
  /**
   * Severity reported when freshness lag exceeds the threshold.
   */
  severity?: TestSeverity & string;
  threshold_seconds: number;
}
/**
 * Null rate check configuration: columns to check, threshold, and sample size.
 */
export interface NullRateConfig {
  columns: string[];
  sample_percent?: number;
  /**
   * Severity reported when a column's null rate exceeds the threshold.
   */
  severity?: TestSeverity & string;
  threshold: number;
}
/**
 * Row quarantine configuration. When enabled, error-severity row-level assertions (`not_null`, `accepted_values`, `expression`) on a given table are compiled into a single boolean row predicate. Rows matching the predicate go to the `__valid` table; rows that don't go to the `__quarantine` table (or are dropped / tagged, per `mode`).
 *
 * Aggregate / set-based assertions (`unique`, `relationships`, `row_count_range`) are **not** lowered — they stay observational and produce `CheckResult` entries as before.
 *
 * ```toml [pipeline.nightly_dq.checks.quarantine] enabled = true mode    = "split"          # "split" (default) | "tag" | "drop" # suffix_valid       = "__valid" # suffix_quarantine  = "__quarantine" ```
 */
export interface QuarantineConfig {
  /**
   * Enable quarantine. Default: `false`.
   */
  enabled?: boolean;
  /**
   * How to split rows — see [`QuarantineMode`].
   */
  mode?: QuarantineMode & string;
  /**
   * Table-name suffix for the failing-rows table. Default `"__quarantine"`.
   */
  suffix_quarantine?: string;
  /**
   * Table-name suffix for the passing-rows table. Default `"__valid"`.
   */
  suffix_valid?: string;
}
/**
 * Controls parallelism and error handling for table processing.
 *
 * Rocky processes tables within a run concurrently using async tasks. Tune `concurrency` based on your warehouse capacity.
 */
export interface ExecutionConfig {
  /**
   * Concurrency strategy (default: `"adaptive"`).
   *
   * - `"adaptive"` — AIMD throttle that starts at 32 and adjusts based on rate-limit signals. Best for remote warehouses (Databricks, Snowflake). - An integer (e.g. `8`) — fixed concurrency, always this many in-flight tables. Use for local adapters (DuckDB) or when you know the limit. - `1` — serial execution.
   */
  concurrency?: ConcurrencyMode & string;
  /**
   * Abort remaining tables if error rate exceeds this percentage (0-100). Prevents wasting compute when the warehouse is unhealthy. Default: 50 (abort if more than half of completed tables failed). Set to 0 to disable.
   */
  error_rate_abort_pct?: number;
  /**
   * If true, abort all remaining tables on first error. If false, process all tables and report errors at the end (partial success).
   */
  fail_fast?: boolean;
  /**
   * Number of times to retry failed tables after the initial parallel phase. Default: 1. Set to 0 to disable auto-retry.
   */
  table_retries?: number;
}
/**
 * A metadata column added during replication (e.g., `_loaded_by`).
 */
export interface MetadataColumnConfig {
  name: string;
  type: string;
  value: string;
}
/**
 * Pipeline source configuration.
 */
export interface PipelineSourceConfig {
  /**
   * Name of the adapter to use (references a key in `[adapter.*]`). Defaults to `"default"` — resolved against the adapter map in [`normalize_rocky_config`].
   */
  adapter?: string;
  /**
   * Source catalog name.
   */
  catalog?: string | null;
  /**
   * Optional discovery configuration.
   */
  discovery?: DiscoveryConfig | null;
  /**
   * Schema pattern for parsing source schema names.
   */
  schema_pattern: SchemaPatternConfig;
}
/**
 * Discovery configuration within a pipeline source.
 */
export interface DiscoveryConfig {
  /**
   * Name of the adapter to use for discovery (references a key in `[adapter.*]`). Defaults to `"default"`.
   */
  adapter?: string;
}
/**
 * Schema pattern configuration from TOML, converted to [`SchemaPattern`] at runtime.
 */
export interface SchemaPatternConfig {
  components: string[];
  prefix: string;
  separator: string;
}
/**
 * Pipeline target configuration.
 */
export interface PipelineTargetConfig {
  /**
   * Name of the adapter to use (references a key in `[adapter.*]`). Defaults to `"default"`.
   */
  adapter?: string;
  /**
   * Template for the target catalog name.
   */
  catalog_template: string;
  /**
   * Governance settings for the target.
   */
  governance?: GovernanceConfig;
  /**
   * Template for the target schema name.
   */
  schema_template: string;
  /**
   * Separator for joining variadic components in target templates.
   *
   * When a source pattern uses `"__"` as its separator but the target templates use `"_"` between placeholders, set this to `"_"` so that multi-valued components (e.g., `{hierarchies}`) are joined correctly.
   *
   * Defaults to the source pattern separator when not set.
   */
  separator?: string | null;
}
/**
 * Governance settings: auto-creation of catalogs/schemas, tags, isolation, and grants.
 */
export interface GovernanceConfig {
  auto_create_catalogs?: boolean;
  auto_create_schemas?: boolean;
  /**
   * Permissions granted on every managed catalog.
   */
  grants?: GrantConfig[];
  /**
   * Workspace isolation settings.
   */
  isolation?: IsolationConfig | null;
  /**
   * Permissions granted on every managed schema.
   */
  schema_grants?: GrantConfig[];
  /**
   * Optional prefix prepended to auto-generated component tag keys (e.g., `"ge_"` turns `client` → `ge_client`). Does not affect keys in `[governance.tags]` — those are used verbatim.
   */
  tag_prefix?: string | null;
  /**
   * Tags applied to every managed catalog and schema.
   */
  tags?: {
    [k: string]: string;
  };
}
/**
 * A permission grant to apply to catalogs or schemas.
 */
export interface GrantConfig {
  permissions: string[];
  principal: string;
}
/**
 * Workspace isolation configuration (Databricks-specific).
 *
 * ```toml [governance.isolation] enabled = true
 *
 * [[governance.isolation.workspace_ids]] id = 7474656540609532 binding_type = "READ_WRITE"
 *
 * [[governance.isolation.workspace_ids]] id = 7474647537929812 binding_type = "READ_ONLY" ```
 */
export interface IsolationConfig {
  enabled?: boolean;
  workspace_ids?: WorkspaceBindingConfig[];
}
/**
 * A workspace binding with ID and access level.
 */
export interface WorkspaceBindingConfig {
  binding_type?: BindingType & string;
  id: number;
}
/**
 * Transformation pipeline configuration.
 *
 * Orchestrates `.sql` / `.rocky` model compilation and execution as a first-class pipeline, with its own execution, checks, and governance settings. Model-level strategy (incremental, merge, time_interval, etc.) is defined in each model's sidecar TOML, not at the pipeline level.
 *
 * ```toml [pipeline.silver] type = "transformation" models = "models/**"
 *
 * [pipeline.silver.target] adapter = "databricks_prod" [pipeline.silver.target.governance] auto_create_schemas = true
 *
 * [pipeline.silver.execution] concurrency = 8 ```
 */
export interface TransformationPipelineConfig {
  /**
   * Data quality checks run after model execution.
   */
  checks?: ChecksConfig;
  /**
   * Pipeline dependencies for chaining.
   */
  depends_on?: string[];
  /**
   * Execution settings (concurrency, retries, etc.).
   */
  execution?: ExecutionConfig;
  /**
   * Glob pattern for model files, relative to the config file directory. Default: `"models/**"`.
   */
  models?: string;
  /**
   * Target configuration (adapter + governance).
   */
  target: TransformationTargetConfig;
  [k: string]: unknown;
}
/**
 * Target configuration for transformation pipelines.
 *
 * Unlike replication targets (which use `catalog_template` / `schema_template` for dynamic routing), transformation targets only need an adapter reference and optional governance — the actual catalog/schema/table is defined per-model in sidecar TOML files.
 */
export interface TransformationTargetConfig {
  /**
   * Name of the adapter to use (references a key in `[adapter.*]`). Defaults to `"default"`.
   */
  adapter?: string;
  /**
   * Governance settings for the target.
   */
  governance?: GovernanceConfig;
}
/**
 * Quality pipeline configuration — standalone data quality checks.
 *
 * Runs checks against existing tables without any data movement.
 *
 * ```toml [pipeline.nightly_dq] type = "quality"
 *
 * [pipeline.nightly_dq.target] adapter = "databricks_prod"
 *
 * [[pipeline.nightly_dq.tables]] catalog = "acme_warehouse" schema = "raw__us_west__shopify"
 *
 * [pipeline.nightly_dq.checks] enabled = true freshness = { threshold_seconds = 86400 } ```
 */
export interface QualityPipelineConfig {
  /**
   * Data quality checks to run.
   */
  checks: ChecksConfig;
  /**
   * Pipeline dependencies for chaining.
   */
  depends_on?: string[];
  /**
   * Execution settings (concurrency, retries, etc.).
   */
  execution?: ExecutionConfig;
  /**
   * Tables to check. Each entry specifies catalog + schema, and optionally a specific table (omit for all tables in the schema).
   */
  tables?: TableRef[];
  /**
   * Target adapter for running check queries.
   */
  target: QualityTargetConfig;
  [k: string]: unknown;
}
/**
 * A reference to a specific catalog/schema/table for quality checks and snapshot pipelines.
 */
export interface TableRef {
  catalog: string;
  schema: string;
  /**
   * Specific table name. When `None`, all tables in the schema are checked.
   */
  table?: string | null;
}
/**
 * Target configuration for quality pipelines (adapter reference only).
 */
export interface QualityTargetConfig {
  /**
   * Name of the adapter to use (references a key in `[adapter.*]`).
   */
  adapter?: string;
}
/**
 * Snapshot pipeline configuration — SCD Type 2 slowly-changing dimension capture.
 *
 * Tracks historical changes to a source table by maintaining `valid_from` / `valid_to` columns in the target history table.
 *
 * ```toml [pipeline.customer_history] type = "snapshot" unique_key = ["customer_id"] updated_at = "updated_at"
 *
 * [pipeline.customer_history.source] adapter = "databricks_prod" catalog = "raw_catalog" schema = "raw__us_west__shopify" table = "customers"
 *
 * [pipeline.customer_history.target] adapter = "databricks_prod" catalog = "acme_warehouse" schema = "silver__scd" table = "customers_history" ```
 */
export interface SnapshotPipelineConfig {
  /**
   * Data quality checks run after snapshot.
   */
  checks?: ChecksConfig;
  /**
   * Pipeline dependencies for chaining.
   */
  depends_on?: string[];
  /**
   * Execution settings.
   */
  execution?: ExecutionConfig;
  /**
   * When true, rows deleted from the source get their `valid_to` set to the current timestamp in the target. Default: false.
   */
  invalidate_hard_deletes?: boolean;
  /**
   * Source table reference (single table, not pattern-based discovery).
   */
  source: SnapshotSourceConfig;
  /**
   * Target history table.
   */
  target: SnapshotTargetConfig;
  /**
   * Column(s) that uniquely identify a row in the source table.
   */
  unique_key: string[];
  /**
   * Column used to detect changes (compared between runs).
   */
  updated_at: string;
  [k: string]: unknown;
}
/**
 * Source table for a snapshot pipeline (explicit single-table reference).
 */
export interface SnapshotSourceConfig {
  /**
   * Name of the adapter to use (references a key in `[adapter.*]`).
   */
  adapter?: string;
  catalog: string;
  schema: string;
  table: string;
}
/**
 * Target table for a snapshot pipeline (explicit single-table reference + governance).
 */
export interface SnapshotTargetConfig {
  /**
   * Name of the adapter to use (references a key in `[adapter.*]`).
   */
  adapter?: string;
  catalog: string;
  governance?: GovernanceConfig;
  schema: string;
  table: string;
}
/**
 * Load pipeline configuration -- ingest files (CSV, Parquet, JSONL) into a warehouse.
 *
 * Loads data from a local directory into a target catalog/schema. The format can be auto-detected from file extensions or set explicitly.
 *
 * ```toml [pipeline.load_data] type = "load" source_dir = "data/" format = "csv"
 *
 * [pipeline.load_data.target] adapter = "prod" catalog = "warehouse" schema = "raw"
 *
 * [pipeline.load_data.options] batch_size = 5000 create_table = true truncate_first = false csv_delimiter = "," csv_has_header = true ```
 */
export interface LoadPipelineConfig {
  /**
   * Data quality checks run after loading.
   */
  checks?: ChecksConfig;
  /**
   * Pipeline dependencies for chaining.
   */
  depends_on?: string[];
  /**
   * Execution settings (concurrency, retries, etc.).
   */
  execution?: ExecutionConfig;
  /**
   * Explicit file format. When omitted, auto-detected from file extensions.
   */
  format?: LoadFileFormat | null;
  /**
   * Load options (batch size, create/truncate behavior, CSV settings).
   */
  options?: LoadOptionsConfig;
  /**
   * Directory or glob pattern for source files, relative to the config file.
   */
  source_dir: string;
  /**
   * Target table location.
   */
  target: LoadTargetConfig;
  [k: string]: unknown;
}
/**
 * Load-specific options parsed from TOML.
 */
export interface LoadOptionsConfig {
  /**
   * Number of rows per INSERT batch. Default: 10,000.
   */
  batch_size?: number;
  /**
   * Create the target table if it does not exist. Default: true.
   */
  create_table?: boolean;
  /**
   * CSV-specific: field delimiter character. Default: `,`.
   */
  csv_delimiter?: string;
  /**
   * CSV-specific: whether the first row is a header. Default: true.
   */
  csv_has_header?: boolean;
  /**
   * Truncate the target table before loading. Default: false.
   */
  truncate_first?: boolean;
}
/**
 * Target configuration for load pipelines.
 */
export interface LoadTargetConfig {
  /**
   * Name of the adapter to use (references a key in `[adapter.*]`).
   */
  adapter?: string;
  /**
   * Target catalog name.
   */
  catalog: string;
  /**
   * Governance settings for the target.
   */
  governance?: GovernanceConfig;
  /**
   * Target schema name.
   */
  schema: string;
  /**
   * Optional explicit table name. When omitted, derives from the file name (e.g., `orders.csv` -> table `orders`).
   */
  table?: string | null;
}
/**
 * Project-wide dialect portability configuration.
 *
 * Lives at the top level because a Rocky project targets one warehouse; per-pipeline overrides aren't supported yet (no demand signal). The `allow` list applies to every model — a per-model override is the `-- rocky-allow: <constructs>` pragma in the model SQL itself, parsed by [`rocky_sql::pragma`].
 */
export interface PortabilityConfig {
  /**
   * Project-wide allow-list of construct labels (case-insensitive, matched against `PortabilityIssue::construct`). Useful for blanket exemptions like `allow = ["QUALIFY"]` when a project standardizes on a specific extension. For per-model exemptions prefer the `-- rocky-allow: <construct>` pragma over expanding this list.
   */
  allow?: string[];
  /**
   * Target dialect for the portability lint. When unset, no lint runs (matches the wave-1 "flag opt-in" behavior). The CLI flag overrides this if both are present.
   */
  target_dialect?: Dialect | null;
}
/**
 * Top-level retry configuration applied across every adapter for this run. See [`RockyConfig::retry`] for the cross-adapter semantics this unlocks.
 */
export interface RunRetryConfig {
  /**
   * Total number of retries allowed across every adapter for this run. `None` means no cross-adapter cap (each adapter's own `retry.max_retries_per_run` still applies in isolation).
   */
  max_retries_per_run?: number | null;
}
/**
 * A single entry in the top-level `[role.*]` block, declaring a hierarchical role with optional inheritance and a list of permissions.
 *
 * ```toml [role.reader] permissions = ["SELECT", "USE CATALOG", "USE SCHEMA"]
 *
 * [role.analytics_engineer] inherits = ["reader"] permissions = ["MODIFY"]
 *
 * [role.admin] inherits = ["analytics_engineer"] permissions = ["MANAGE"] ```
 *
 * Resolution happens at reconcile time via [`crate::role_graph::flatten_role_graph`], which walks the `inherits` DAG and unions permissions from the role and every transitive ancestor. Cycles and unknown parents are caught as structured [`crate::role_graph::RoleGraphError`] values.
 *
 * Permission strings must match the canonical uppercase spellings of [`crate::ir::Permission`] (`"SELECT"`, `"USE CATALOG"`, ...).
 */
export interface RoleConfig {
  /**
   * Immediate parent role names. Rocky walks these transitively at reconcile time; cycles are rejected. Defaults to `[]` when omitted.
   */
  inherits?: string[];
  /**
   * Permissions this role grants. Rocky unions these with every ancestor's permissions before passing the flattened set to the governance adapter. Defaults to `[]` (permissionless grouping roles are legal — they exist only for inheritance).
   */
  permissions?: string[];
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
   * Per-run idempotency-key policy (`rocky run --idempotency-key`). Controls retention of stamped keys, what terminal statuses count as "deduplicated", and how long an `InFlight` entry survives before it's treated as a crashed-pod corpse and adopted by a fresh caller. See [`IdempotencyConfig`].
   */
  idempotency?: IdempotencyConfig;
  /**
   * What to do when state upload exhausts retries + circuit-breaker. Defaults to `skip` — rocky continues the run and the next run re-derives state from target-table metadata. See [`StateUploadFailureMode`].
   */
  on_upload_failure?: StateUploadFailureMode & string;
  /**
   * Retention policy applied to Rocky's own `state.redb` tables (run history, DAG snapshots, quality snapshots). Bounds the size of the control-plane store; operational tables (schema cache, watermarks, partition records) are never swept by this policy. See [`crate::retention::StateRetentionConfig`].
   */
  retention?: StateRetentionConfig;
  /**
   * Retry policy applied to transient state-transfer failures (network hiccups, hung endpoints that hit the per-request HTTP timeout, transient 5xx, etc.). Shares the same shape as the adapter retry config so operators can reason about both with a single mental model. Retries share the outer `transfer_timeout_seconds` budget, so the total wall-clock ceiling is unchanged.
   */
  retry?: RetryConfig;
  /**
   * S3 bucket for state persistence
   */
  s3_bucket?: string | null;
  /**
   * S3 key prefix (default: "rocky/state/")
   */
  s3_prefix?: string | null;
  /**
   * Wall-clock budget (seconds) for each state transfer operation (download or upload). Catches stuck SDK retry loops, DNS, TLS, and hung endpoints that the per-request HTTP timeout does not see. Defaults to 300s; raise for large state or slow networks.
   */
  transfer_timeout_seconds?: number;
  /**
   * Valkey key prefix (default: "rocky:state:")
   */
  valkey_prefix?: string | null;
  /**
   * Valkey/Redis URL for state persistence
   */
  valkey_url?: string | null;
}
/**
 * Config for `rocky run --idempotency-key` dedup.
 *
 * All fields are optional with sensible defaults. Block is present even when the user doesn't set `--idempotency-key`; it's a no-op in that case.
 */
export interface IdempotencyConfig {
  /**
   * Which terminal statuses count as "already processed" for dedup. See [`DedupPolicy`]. Default [`DedupPolicy::Success`].
   */
  dedup_on?: DedupPolicy & string;
  /**
   * Hours after which an `InFlight` entry is treated as a crashed-pod corpse and adopted by a fresh caller. Default 24. Applies only to backends whose in-flight lock does not carry a server-side TTL — Valkey providers set `EX` directly on `SET NX`, so this field is informational for them.
   */
  in_flight_ttl_hours?: number;
  /**
   * Number of days a `Succeeded` (or `Failed`-under-`any`) stamp is kept before GC. Default 30. GC runs during the state upload sweep.
   */
  retention_days?: number;
}
/**
 * State-store retention policy.
 *
 * Bounds the size of Rocky's `state.redb` by sweeping rows older than `max_age_days` from the run-history, DAG-snapshot, and quality-snapshot tables. The most recent `min_runs_kept` rows in each domain are always preserved, so a project that has not run in months still has its last good baseline available for `rocky history` and `rocky compare`.
 *
 * Operational state — schema cache, watermarks, partition records — is never swept by this policy: those tables hold live correctness data (without them, the next run cannot resume), not history.
 */
export interface StateRetentionConfig {
  /**
   * Domains to sweep. Defaults to `["history", "lineage", "audit"]`. Setting `applies_to = []` disables the sweep entirely without removing the config block — useful for staged rollouts.
   */
  applies_to?: StateRetentionDomain[];
  /**
   * Drop rows whose timestamp is older than this many days. Counted from the row's recorded `started_at` (history), `timestamp` (lineage / audit) — not the file mtime. Defaults to [`DEFAULT_STATE_RETENTION_MAX_AGE_DAYS`].
   */
  max_age_days?: number;
  /**
   * Always preserve at least this many rows in each domain, even if every row is older than `max_age_days`. Applied per domain (last N runs, last N DAG snapshots, last N quality snapshots). Defaults to [`DEFAULT_STATE_RETENTION_MIN_RUNS_KEPT`].
   */
  min_runs_kept?: number;
}
