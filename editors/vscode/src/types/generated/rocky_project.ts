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
 * What to do when a [`BudgetConfig`] limit is exceeded by an actual run.
 *
 * `Warn` always fires the `budget_breach` event; `Error` additionally causes `rocky run` to exit with a non-zero status so orchestrators can gate downstream work on the breach.
 */
export type BudgetBreachAction = "warn" | "error";
/**
 * Severity of a test failure.
 */
export type TestSeverity = "error" | "warning";
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
 * How a column is masked at apply time.
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
      /**
       * SQL scalar expression whose value must be unique across rows.
       */
      key_expr: string;
      type: "unique_expr";
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
 * Action when `rocky discover` detects a cross-source collision — the same external object id mapped to more than one target path.
 */
export type OnCollision = "off" | "warn" | "error";
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
 * The verdict a policy rule (or the default posture) yields.
 *
 * Ordered by restrictiveness for incomparable-rule tie-breaking: `Deny` is a hard override (handled separately), and among non-deny verdicts `RequireReview` is more restrictive than `Allow`.
 */
export type PolicyEffect = "allow" | "require_review" | "deny";
/**
 * The class of action a policy rule governs.
 *
 * `read` is always allowed (short-circuit). The mutating verbs (`propose` … `quarantine`) name coarse operations; `schema_change.additive`, `schema_change.breaking`, and `value_change` are *refinements* of the apply/promote verbs — a rule naming a bare verb (`apply`/`promote`) matches those refinements too, but a rule naming a refinement matches only that exact refinement.
 */
export type PolicyCapability =
  | "read"
  | "propose"
  | "apply"
  | "promote"
  | "backfill"
  | "gc"
  | "retry"
  | "quarantine"
  | "schema_change.additive"
  | "schema_change.breaking"
  | "value_change";
/**
 * Who is attempting an action.
 *
 * `agent` is a non-human caller (an AI harness authoring, applying, or remediating). `human` is a person. In v0 the principal is supplied explicitly (`rocky policy check --principal …`); auto-detection is a later phase.
 */
export type PolicyPrincipal = "human" | "agent";
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
 * Per-pipeline / per-client state-file namespacing policy.
 *
 * redb permits one writer per file, so fanning out one `rocky run` process per pipeline or client against the single global state file forces those independent runs to serialize on one advisory lock. Namespacing gives each namespace its own `<models>/.rocky-state/<namespace>.redb` file (its own lock, its own redb handle, its own remote object key), so runs on distinct namespaces proceed concurrently with zero shared corruption surface.
 *
 * Default is [`StateNamespacing::None`] — behavior is **byte-identical** to a project that never sets this key. Namespacing is purely opt-in.
 */
export type StateNamespacing = "none" | "pipeline";
/**
 * Policy applied when the engine opens a state store whose schema version is **newer** than the binary supports (a *forward*-incompatibility).
 *
 * This is the deploy-safety knob for rolling upgrades that cross a redb schema version. During a rolling bump, pods on the *old* binary can read state written by *already-upgraded* pods through a shared tiered backend. The default ([`Recreate`][Self::Recreate]) degrades that window instead of breaking it: the old pod does one full-refresh run and succeeds, rather than stranding the orchestrated run in an hour-long retry spiral.
 *
 * Only the *forward* case (on-disk newer than binary) is governed here. *Backward*-compatibility — a newer binary reading older state — always auto-migrates forward as before; there is no knob for it.
 */
export type SchemaMismatchPolicy = "recreate" | "fail";
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
   * Branch-level configuration. Currently scopes the optional approval gate consumed by `rocky branch promote`. See [`BranchSection`].
   */
  branch?: BranchSection;
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
   * Project-level freshness defaults inherited by per-model [`crate::models::ModelFreshnessConfig`] declarations that omit individual fields. See [`ProjectFreshnessConfig`] for the TOML shape:
   *
   * ```toml [freshness] expected_lag_seconds = 3600 time_column = "updated_at" severity = "warning" ```
   *
   * Inheritance is field-by-field: a per-model `[freshness]` table always wins for the fields it sets; absent fields fall through to the project-level default. Models with no per-model `[freshness]` at all inherit the project default when it carries an `expected_lag_seconds` value (the required field).
   */
  freshness?: ProjectFreshnessConfig;
  /**
   * Shell hooks configuration.
   */
  hook?: HooksConfig;
  /**
   * Imported producer-project snapshots, keyed by import name.
   *
   * Each `[imports.<name>]` block points at a vendored snapshot of a producer project's compiled IR. During `rocky compile`, the consumer's column references are checked against the producer's published schema: a column the producer dropped but the consumer still reads surfaces as an error (E030), and a recipe-hash mismatch against a configured `pin` surfaces as E033. Empty by default — a project with no imports incurs no extra work.
   */
  imports?: {
    [k: string]: ImportEntry;
  };
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
   * Agent-authority policy plane (explain-mode in v0). Declares, per `(principal, capability, scope)`, whether an action is allowed, requires human review, or is denied. Absent `[policy]` block ⇒ no rules and the default posture applies (agents on mutating actions fall to `default_agent_effect`, humans are never gated). See [`PolicyConfig`] and [`crate::policy`] for the evaluator.
   */
  policy?: PolicyConfig | null;
  /**
   * Dialect-portability lint configuration. Consumed by `rocky compile` to drive P001 (and, when wired, future) diagnostics. The CLI's `--target-dialect` flag, when set, takes precedence over [`PortabilityConfig::target_dialect`].
   */
  portability?: PortabilityConfig;
  /**
   * `[resilience]` — the run loop's classified-retry policy. Governs whether a model whose materialization fails *transiently* is re-run, with a conservative bounded budget and capped backoff. On by default (a transient failure is retried), but every knob is small and explicit; see [`ResilienceConfig`]. Permanent / Unknown failures never retry.
   */
  resilience?: ResilienceConfig;
  /**
   * Run-level retry budget shared across every adapter for this run.
   *
   * When set, `rocky run` builds a single [`crate::retry_budget::RetryBudget`] from [`RunRetryConfig::max_retries_per_run`] and passes it to every connector via `with_retry_budget(...)`. One bad table that burns through retries on adapter A then has less budget available for adapter B's retries — the protection §P2.7 added within a single adapter now extends across the whole run.
   *
   * Unset (the default) preserves per-adapter semantics: each adapter still honours its own `retry.max_retries_per_run` independently. That's the backward-compatible path and stays the right choice when adapters have wildly different rate limits.
   */
  retry?: RunRetryConfig | null;
  /**
   * Opt-in auditable reuse. Default-OFF: an absent `[reuse]` block keeps `rocky run` byte- and cost-identical (no per-model hashing, no extra state write). When enabled, an eligible content-addressed model whose inputs match a prior strong run may **point-to** that run's parquet (zero-copy) instead of re-executing its SQL — fail-closed to BUILD on any doubt. See [`ReuseConfig`].
   */
  reuse?: ReuseConfig;
  /**
   * Hierarchical role declarations reconciled against the warehouse's native role/group system.
   *
   * See [`RoleConfig`] for the TOML shape and [`crate::role_graph::flatten_role_graph`] for the inheritance resolution semantics (DAG walk with cycle detection).
   */
  role?: {
    [k: string]: RoleConfig;
  };
  /**
   * Opt-in run-execution tuning for the `--skip-unchanged` model-skip gate. Default-OFF: an absent `[run]` block (or one that leaves every field at its default) keeps `rocky run`'s behavior byte-identical to before the gate existed. See [`RunConfig`].
   */
  run?: RunConfig;
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
  /**
   * Escape hatch for adapter-specific keys this struct doesn't model.
   *
   * `AdapterConfig` is `#[serde(deny_unknown_fields)]` so typos at the top level (`tooken` for `token`) surface as parse errors rather than silent ignores. That guard, however, also blocks legitimate adapter-specific settings — an out-of-tree Trino adapter wanting a `default_schema` slot, a Postgres adapter wanting a non-standard `application_name`, etc. — from ever reaching the adapter.
   *
   * Authors of such adapters declare the keys under a nested `[extra]` table:
   *
   * ```toml [adapter.my_trino] type = "trino" host = "https://trino.example.com" token = "${TRINO_JWT}"
   *
   * [adapter.my_trino.extra] default_schema = "analytics" x_trino_user = "service-account" ```
   *
   * Top-level typos still error (`tooken = "..."` is still rejected); only keys nested under `[adapter.<name>.extra]` flow through to the adapter unchanged. Adapters read these via `.extra.get("...")` and validate them themselves — Rocky doesn't schema-check the contents.
   *
   * Values are `serde_json::Value` so the field survives `just codegen` (`toml::Value` doesn't derive `JsonSchema`); TOML scalars / tables / arrays still round-trip through serde because they all map to the JSON shape.
   */
  extra?: {
    [k: string]: unknown;
  };
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
 * Top-level `[branch]` configuration section.
 *
 * Currently scopes the optional approval gate consumed by `rocky branch promote`. Default-constructed (no `[branch]` block in `rocky.toml`) leaves the gate disabled — `branch promote` skips the approval loop and behaves like the unguarded baseline.
 */
export interface BranchSection {
  approval?: BranchApprovalConfig;
}
/**
 * `[branch.approval]` configuration block.
 *
 * Defaults are deliberately permissive — a project that doesn't add the section keeps the v0 `branch promote` behaviour. Flipping `required = true` opts in to the gate; the rest of the knobs tune the strictness once the gate is on.
 */
export interface BranchApprovalConfig {
  /**
   * When non-empty, only approvals from these signer emails count toward `min_approvers`. Empty (default) accepts any signer.
   */
  allowed_signers?: string[];
  /**
   * Approvals older than this many seconds are rejected even if their branch_state_hash still matches. Default 86400 (24h).
   */
  max_age_seconds?: number;
  /**
   * Minimum number of valid approvals required when `required = true`. "Valid" means: signature verifies, branch_state_hash matches the current state, signed within `max_age_seconds`, and (when `allowed_signers` is non-empty) the signer's email is in the list.
   */
  min_approvers?: number;
  /**
   * When true, `branch promote` refuses to run unless at least `min_approvers` valid approval artifacts are on disk for the branch. When false (default), the gate is bypassed silently.
   */
  required?: boolean;
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
 * Project-level freshness defaults.
 *
 * Top-level `[freshness]` block on `rocky.toml`. Provides defaults inherited by per-model [`crate::models::ModelFreshnessConfig`] declarations that omit one or more fields. Independent of the [`ChecksConfig::freshness`](FreshnessConfig) check (which lives under `[checks.freshness]` and feeds the data-quality test pipeline).
 *
 * All fields are optional. A project-level `[freshness]` with no `expected_lag_seconds` is treated as "no project default" for the W005 soft-warn — the suppression still requires a concrete TTL.
 */
export interface ProjectFreshnessConfig {
  /**
   * Default maximum lag in seconds before models are considered stale. When set, every model without its own `freshness` block inherits this value (plus the other fields). When `None`, no project-level default applies — per-model declarations are the only source of freshness metadata.
   */
  expected_lag_seconds?: number | null;
  /**
   * Default severity reported when the freshness check trips.
   */
  severity?: TestSeverity | null;
  /**
   * Default timestamp column used to evaluate freshness at runtime. Inherited by per-model freshness blocks that don't specify their own `time_column`.
   */
  time_column?: string | null;
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
   * HMAC-SHA256 signing secret. When set, adds `X-Rocky-Signature: sha256=<hex>` header. Wrapped in [`RedactedString`] so a stray `Debug` print of the hook config doesn't leak the secret into logs.
   */
  secret?: RedactedString | null;
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
 * A single imported producer-project snapshot.
 *
 * Declared as `[imports.<name>]` in `rocky.toml`. A producer project publishes a serialized snapshot of its compiled project (via `rocky publish-ir`); a consumer project vendors that snapshot file and references it here so `rocky compile` can verify that the columns the consumer reads still exist in the producer's output.
 *
 * ```toml [imports.orders] path = "vendor/orders"        # directory holding the vendored snapshots snapshot = "current.json"     # the producer's current published snapshot baseline = "baseline.json"    # optional prior snapshot used for diffing pin = "*"                     # optional recipe-hash pin ("*" = trust any) ```
 *
 * `pin` and `baseline` answer different questions and are complementary, not redundant. `pin` is a whole-project drift tripwire: a concrete recipe hash that, when set, makes `rocky compile` fail (E033) if the vendored snapshot differs at all. `baseline` is the column-level "before" image: the only input that lets the breaking-change diff emit the column codes (E030/E031/E032/W030/W031) for changes the consumer actually reads. Leave `pin` at `"*"` (or unset) to fail only on changes that touch your reads; set a concrete pin to fail on any drift. Run `rocky imports update` after reviewing a producer change to advance `baseline` to the current snapshot (the explicit accept) — nothing advances it automatically.
 */
export interface ImportEntry {
  /**
   * Optional filename of a prior/pinned snapshot used as the diff baseline, relative to `path`. When set, `rocky compile` diffs `baseline` against `snapshot` to detect columns the producer dropped.
   */
  baseline?: string | null;
  /**
   * Directory (relative to `rocky.toml`) holding the vendored snapshot files.
   */
  path: string;
  /**
   * Optional recipe-hash pin (hex). When set to a concrete hash, the snapshot's recipe hash must match or compilation fails. `"*"` (or absent) trusts whatever snapshot is vendored.
   */
  pin?: string | null;
  /**
   * Filename of the producer's current published snapshot, relative to `path`.
   */
  snapshot: string;
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
   * Pipeline dependencies for chaining.
   */
  depends_on?: string[];
  /**
   * Execution settings (concurrency, retries, etc.).
   */
  execution?: ExecutionConfig;
  /**
   * Unique-key columns for `strategy = "merge"`. The `MERGE` statement joins source rows onto the target on these columns; matched rows are updated, unmatched rows are inserted.
   *
   * Required when `strategy = "merge"` and `merge_keys_fallback` is absent. Ignored for other strategies.
   */
  merge_keys?: string[] | null;
  /**
   * Fallback unique-key columns used when `merge_keys` is not configured. Provides a single source of merge-key defaults for callers that derive keys from another source (e.g. the discovery adapter) but still want pipeline-level control.
   */
  merge_keys_fallback?: string[] | null;
  /**
   * Metadata columns added during replication.
   */
  metadata_columns?: MetadataColumnConfig[];
  /**
   * When `true`, the replication runner skips ("prunes") any table whose source is provably unchanged since the last successful copy — detected via the adapter's `source_change_marker` (a `DESCRIBE DETAIL`-derived marker on Databricks; adapters without a cheap change signal always copy). A pruned table runs no copy and no data checks: the runner emits no materialization and records it under `excluded_tables` with reason `"unchanged_since_last_copy"`.
   *
   * Downstream continuity — and preserving the table's prior check results — is then the orchestrator's job. The Dagster integration treats a pruned, unmaterialized key as unchanged via `satisfy_empty_outputs`; enabling pruning without an orchestrator that handles unmaterialized keys drops the table from the run.
   *
   * Defaults to `false` — opt in per pipeline, since silently skipping copies is a behavior change. The marker is compared against the target's recorded last-copied value (never wall-clock), so a failed prior run cannot cause a false skip. Pass `--no-prune` to `rocky run` to force a full pass (e.g. after a manual target-side mutation).
   */
  prune_unchanged?: boolean;
  /**
   * Source configuration.
   */
  source: PipelineSourceConfig;
  /**
   * Replication strategy. Accepted values:
   *
   * - `"incremental"` — append rows past the source-side watermark. - `"full_refresh"` — `CREATE OR REPLACE TABLE … AS SELECT …`. - `"merge"` — upserts the watermarked delta into the target via `MERGE INTO … USING (delta) ON merge_keys WHEN MATCHED UPDATE SET * WHEN NOT MATCHED INSERT *`. Requires `merge_keys` (or `merge_keys_fallback`) to be set. - `"view"` — emits a `CREATE OR REPLACE VIEW` over the source. Every read against the target re-runs the SELECT; no row movement. - `"materialized_view"` — warehouse-managed materialized view. Supported on Databricks, Snowflake, and BigQuery; DuckDB/Trino surface an unsupported-dialect error. - `"dynamic_table"` — Snowflake-only. Not configurable at the pipeline level today (the strategy needs a `target_lag` specifier that the pipeline block doesn't expose); declare it on a transformation model's sidecar TOML instead.
   */
  strategy?: string;
  /**
   * Per-`(connector, table)` overrides applied on top of the pipeline defaults. Each rule matches against a discovered connector + table pair and, when matched, replaces selected pipeline-level fields with override-supplied values.
   *
   * Resolution is **per-field most-specific-match-wins**: for each overrideable field, the most specific matching rule that explicitly sets that field wins; less-specific rules contribute values only for fields they actually set. Specificity ranking (highest to lowest):
   *
   * 1. Both `match.connector` AND `match.table` set, with `match.table` a literal (no glob). 2. Both set, with `match.table` containing a `*` or `?` glob. 3. Only `match.connector` set. 4. Only `match.table` set.
   *
   * Ties at the same specificity tier for a `(connector, table)` pair are rejected at parse time as ambiguous.
   *
   * Empty by default — projects that don't need per-table tweaking pay nothing in JSON output, schema surface area, or runtime cost.
   */
  table_overrides?: TableOverride[];
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
  /**
   * Cross-source overlap check — flags the same business key appearing in more than one sibling source feeding a shared consolidation target. Disabled when unset. See [`CrossSourceOverlapConfig`].
   */
  cross_source_overlap?: CrossSourceOverlapConfig | null;
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
 * Cross-source overlap check: flags the same business key appearing in more than one *sibling* source feeding a shared consolidation target (the "same account onboarded twice under two paths" case). Siblings are grouped by the runner; this config carries only the key + thresholds.
 *
 * `keys` (a column tuple) and `key_expr` (a derived SQL expression) are mutually exclusive — exactly one must be set, mirroring `unique` / `unique_expr`.
 */
export interface CrossSourceOverlapConfig {
  /**
   * Derived business-key expression (e.g. `md5(a || '-' || b)`), for sources without a single natural key. Mutually exclusive with `keys`. Passed through verbatim (trusted config, like `unique_expr`).
   */
  key_expr?: string | null;
  /**
   * Business-key columns whose shared value across sibling tables signals a duplicate. Mutually exclusive with `key_expr`.
   */
  keys?: string[];
  /**
   * Overlap-key count above which the check fails. Default 0 — any overlap fails.
   */
  max_overlap_rows?: number;
  /**
   * Maximum overlapping keys attached to the result for triage.
   */
  sample?: number;
  /**
   * Severity reported when the overlap-key count exceeds `max_overlap_rows`.
   */
  severity?: TestSeverity & string;
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
  /**
   * What to do when discover finds the same external object id mapped to more than one target path (likely the same object onboarded twice). `off` (default) skips detection entirely; `warn` reports `collision_candidates` + emits an event; `error` additionally fails the discover. Only adapters that supply `external_object_id` (e.g. Fivetran) participate; others are silently skipped.
   */
  on_collision?: OnCollision & string;
  /**
   * When `true`, `rocky discover` diffs the discovered source inventory against the prior persisted snapshot and reports first-seen sources in `new_sources`. Off by default — the diff and its state write only happen when opted in, so existing projects pay nothing.
   */
  report_new_sources?: boolean;
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
 * A per-`(connector, table)` override rule on a replication pipeline.
 *
 * Authored as one TOML array entry under `[[pipeline.<name>.table_overrides]]`. Each field except `match_` is optional — `None` means "inherit from the pipeline-level default." The resolver applies overrides with per-field most-specific-wins semantics; see [`ReplicationPipelineConfig::table_overrides`] for the ranking.
 *
 * # Example
 *
 * ```toml [[pipeline.raw.table_overrides]] match.connector = "stripe_main" match.table     = "pii_users" merge_keys      = ["user_id", "tenant_id"] ```
 */
export interface TableOverride {
  /**
   * When `Some(false)`, the matched `(connector, table)` pairs are dropped from the run and surfaced under `RunOutput.excluded_tables` with `reason = "table_override_disabled"`. Net-new field with no pipeline-level counterpart — disabling a whole pipeline is already covered by removing the pipeline block.
   */
  enabled?: boolean | null;
  /**
   * Match criteria — at least one of `connector` or `table` must be set. A rule with neither (or a `match` block omitted entirely) is rejected at parse time (use pipeline-level fields to change defaults for all tables). `default` lets serde deserialize an override missing the whole `match` block; the validator catches the resulting empty match.
   */
  match?: TableMatch;
  /**
   * Override for [`ReplicationPipelineConfig::merge_keys`]. `None` inherits the pipeline default.
   */
  merge_keys?: string[] | null;
  /**
   * Override for [`ReplicationPipelineConfig::merge_keys_fallback`]. `None` inherits the pipeline default.
   */
  merge_keys_fallback?: string[] | null;
  /**
   * Override for [`ReplicationPipelineConfig::strategy`]. `None` inherits the pipeline default.
   */
  strategy?: string | null;
  /**
   * Override for [`ReplicationPipelineConfig::timestamp_column`]. `None` inherits the pipeline default.
   */
  timestamp_column?: string | null;
}
/**
 * Match criteria for a [`TableOverride`].
 *
 * At least one of `connector` or `table` must be set (`None` for both is rejected at parse time as redundant — use pipeline-level fields to change defaults globally).
 *
 * `connector` matches against either [`crate::source::DiscoveredConnector::id`] (the stable adapter-side identifier — e.g. a Fivetran connector_id) **or** [`crate::source::DiscoveredConnector::schema`] (the human-meaningful source schema name — e.g. `src__acme__shopify`). String equality on either match wins.
 *
 * `table` matches against [`crate::source::DiscoveredTable::name`]. Supports `*` (zero or more characters) and `?` (exactly one character) glob wildcards; presence of either character switches the value from literal to glob.
 *
 * # Reserved
 *
 * `table` and `id` are reserved as schema-pattern component names — configs that use either as a component in [`SchemaPatternConfig::components`] are rejected at parse time to avoid colliding with `--filter table=` and `--filter id=`.
 */
export interface TableMatch {
  /**
   * Connector match — equals either `DiscoveredConnector.id` or `DiscoveredConnector.schema`. `None` matches every connector.
   */
  connector?: string | null;
  /**
   * Table-name match. Literal when no `*`/`?`; glob otherwise. `None` matches every table.
   */
  table?: string | null;
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
   * Optional data contract that gates the load. When set, each file is loaded into a staging table, validated against the contract, and promoted to the target only if validation passes. On failure the staging table is dropped and the target is left untouched.
   */
  contract?: ContractConfig | null;
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
 * Data contract configuration — enforced at copy/load time.
 */
export interface ContractConfig {
  /**
   * Type changes that are allowed (widening only).
   */
  allowed_type_changes?: AllowedTypeChange[];
  /**
   * Column names that must never be removed from the target.
   */
  protected_columns?: string[];
  /**
   * Columns that must exist with specific types.
   */
  required_columns?: RequiredColumn[];
  [k: string]: unknown;
}
/**
 * A permitted type widening (e.g., INT to BIGINT) that won't trigger a violation.
 */
export interface AllowedTypeChange {
  from: string;
  to: string;
  [k: string]: unknown;
}
/**
 * A column that must exist in the source with a specific type.
 */
export interface RequiredColumn {
  name: string;
  nullable?: boolean;
  /**
   * Expected type, written in warehouse vocabulary (e.g. `BIGINT`, `VARCHAR`, `NUMBER(38,0)`). It is normalized to a portable Rocky type before comparison, so the same contract ports across warehouses (DuckDB `VARCHAR` and Snowflake `STRING` both match). A type the normalizer doesn't recognize is treated as unknown and never fails the type check — presence and nullability still apply.
   */
  type: string;
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
 * The `[policy]` block: agent-authority policy for this project.
 */
export interface PolicyConfig {
  /**
   * Effect for an `agent` on a mutating capability when no rule matches. Defaults to `require_review` (the safe posture).
   */
  default_agent_effect?: PolicyEffect & string;
  /**
   * Ordered list of rules. Evaluated as a set (order only breaks final ties); see [`crate::policy::evaluate`].
   */
  rules?: PolicyRule[];
  /**
   * Schema version. Must be `1`.
   */
  version: number;
}
/**
 * One `[[policy.rules]]` entry: `(principal, capability, scope) → effect`.
 */
export interface PolicyRule {
  /**
   * Which capability this rule governs.
   */
  capability: PolicyCapability;
  /**
   * Optional v1 conditional refinements. **Parsed and ignored in v0** — captured as opaque JSON so a config authored for v1 still loads.
   */
  conditions?: {
    [k: string]: unknown;
  };
  /**
   * The verdict when this rule matches.
   */
  effect: PolicyEffect;
  /**
   * Who this rule applies to.
   */
  principal: PolicyPrincipal;
  /**
   * The models this rule covers. Defaults to the empty scope, which is *not* `any` — an all-default scope with no `any = true` matches nothing and is rejected at validation.
   */
  scope?: PolicyScope;
}
/**
 * Scope of a policy rule — the AND of every present predicate. A model matches the scope only when it satisfies *all* set keys.
 *
 * `any = true` is the empty scope (matches every model, zero constraints) and is mutually exclusive with every other key.
 */
export interface PolicyScope {
  /**
   * Match every model. Mutually exclusive with all other keys; carries zero constraints, so any rule with a real predicate outranks it.
   */
  any?: boolean;
  /**
   * Classification guard (positive). Satisfied when the model has at least one column classified with any listed value (e.g. `["pii"]`).
   */
  classifications?: string[];
  /**
   * Contract-boundary guard. Satisfied when the model's contracted status equals this value. (v0 reads contracted status best-effort from a sibling `.contract.toml`; see [`crate::policy`].)
   */
  contracted?: boolean | null;
  /**
   * Classification guard (negative). Satisfied when the model has *no* column classified with any listed value — e.g. `exclude_classifications = ["pii"]` matches only non-PII models.
   */
  exclude_classifications?: string[];
  /**
   * Medallion/semantic layer guard. Satisfied when the model's `layer` tag equals this value (v0 reads layer from the model's `layer` tag).
   */
  layer?: string | null;
  /**
   * Blast-radius guard: maximum downstream count. **Parse-only in v0** — accepted and validated but not yet evaluated by the matcher.
   */
  max_downstreams?: number | null;
  /**
   * Glob selectors over the model name (`*`/`?`). Satisfied when the model name matches at least one pattern.
   */
  models?: string[];
  /**
   * Required model tags (AND of `key = value` pairs). Satisfied when the model carries every listed tag with the exact value.
   */
  tags?: {
    [k: string]: string;
  };
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
 * `[resilience]` — the run loop's classified-retry policy.
 *
 * This is a **distinct layer** from the per-adapter `[adapter.*.retry]` (which retries individual statements inside a connector) and from the run-level `[retry]` budget ([`RunRetryConfig`], which caps connector retries). `[resilience]` governs whether the run loop re-runs a whole *model* whose materialization failed, classifying the failure via [`crate::failure_class::FailureClass`] and retrying only a *proven* transient one.
 *
 * # Not default-OFF, but conservative
 *
 * Unlike the skip / reuse gates, this layer is **on by default** — a model that fails transiently today *will* be retried once this ships. The lever that keeps that safe is [`Self::transient_max_retries`] (default `2`): a small, bounded budget with capped exponential backoff. Set `transient_max_retries = 0` (or `enabled = false`) to restore the prior single-attempt behaviour, e.g. in CI where a fast-fail is preferred.
 *
 * Permanent and Unknown failures are **never** retried regardless of these settings — that is a property of the classifier, not of this config.
 */
export interface ResilienceConfig {
  /**
   * Multiplier applied to the backoff after each retry.
   */
  backoff_multiplier?: number;
  /**
   * Trip the run-loop breaker after this many *consecutive* transient model failures; once tripped, no further model is retried for the rest of the run (they still get their one attempt). Default: `3`. `0` disables the breaker.
   */
  circuit_breaker_threshold?: number;
  /**
   * Continue disjoint subgraphs when a model fails, instead of aborting the whole run at the first failure. Default `false` (fail-fast — the run stops at the first failing model, exactly as before this knob existed).
   *
   * When `true`, a failed model and its downstream closure are *withheld* (never built on the failure's stale/missing output), while unrelated subtrees still materialize; the run reports `PartialFailure` with a containment manifest naming what failed and its blast radius. This changes no data semantics — everything withheld is already withheld by today's fail-fast run; it only *narrows* the withholding to the actual blast radius. The closure is conservative (computed from the resolved dependency graph, contain-more on any doubt).
   */
  contain_failures?: boolean;
  /**
   * Master switch for run-loop classified retry. Default `true`. When `false`, every model is attempted exactly once (no classification, no backoff) — the behaviour before this layer existed.
   */
  enabled?: boolean;
  /**
   * Initial backoff (ms) before the first retry.
   */
  initial_backoff_ms?: number;
  /**
   * Add ±25 % jitter so concurrent runs don't retry in lockstep.
   */
  jitter?: boolean;
  /**
   * Maximum backoff (ms) — caps the exponential growth.
   */
  max_backoff_ms?: number;
  /**
   * Optional ceiling on the *total* number of retries across all models in one run — a global budget separate from the per-adapter one. Default `Some(8)`: a conservative cap so one flaky layer can't spin the whole run. `None` removes the ceiling (per-model `transient_max_retries` is then the only bound); `Some(0)` forbids all retries.
   */
  max_retries_per_run?: number | null;
  /**
   * Maximum re-runs of a model that failed with a *transient* class. Conservative default: `2` (so at most three attempts total). `0` disables retry while leaving the classifier active for observability.
   */
  transient_max_retries?: number;
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
 * `[reuse]` — opt-in auditable reuse for content-addressed models.
 *
 * When `enabled = true`, a successful run records, per model, an input-match index entry and an offline-verifiable provenance record (the model's logic key, upstream input identities, output blake3(s), and proof class). That spine is the *input* side; on a later run, an eligible model whose recomputed `input_hash` hits the index for a prior **strong** run may **point-to** that run's already-written parquet — a zero-copy commit that skips the SQL — provided every clause of the runner's fail-closed reuse decision holds. Any doubt builds.
 *
 * **Default-OFF.** `enabled = false` (the default) keeps `rocky run` byte- *and* cost-identical to before the spine existed: no extra normalize+hash work, no extra state write, no reuse decision. The point-to path is strong (byte-identical) only on the content-addressed/UniForm write path; experimental, opt-in.
 */
export interface ReuseConfig {
  /**
   * Opt-in **column-level skip** for content-addressed models. When `true`, an unpartitioned content-addressed model whose logic, environment, and every provably-consumed upstream column are unchanged since its last successful build is **skipped** — its SQL does not run and no new commit is written; the prior output stays authoritative. Skipping on a value change to a consumed column is precisely the silent-staleness bug this gate is built to avoid, so the decision is fail-closed: any unproven input (a non-deterministic model, a changed recipe/env, an un-enumerable consumed set, a missing or moved column hash) forces a build.
   *
   * **Default-OFF.** `false` (the default) keeps `rocky run` byte- and cost-identical: no column-level comparison runs and no model is skipped on this basis. Independent of [`Self::enabled`] (byte-level point-to reuse) — the two are orthogonal opt-ins on the content-addressed path. Experimental; off the content-addressed path the feature does not apply.
   */
  column_level?: boolean;
  /**
   * Master switch for auditable reuse: populates the input-match spine and arms the point-to decision. `false` (default) ⇒ the spine is never written, no per-model hashing cost is paid, and no model reuses.
   */
  enabled?: boolean;
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
 * Permission strings must match the canonical uppercase spellings of [`rocky_ir::Permission`] (`"SELECT"`, `"USE CATALOG"`, ...).
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
 * `[run]` — opt-in tuning for the model-skip gate.
 *
 * The gate lets `rocky run` skip re-materializing a model whose logic and upstream data both *appear* unchanged since the last successful build. It is a best-effort optimization, **not** a guarantee of result- equivalence: non-deterministic SQL is excluded, and any ambiguity rebuilds. Every field defaults to the safe (no-skip) choice — the whole feature is off unless `skip_unchanged = true` (or the `--skip-unchanged` flag) is set.
 */
export interface RunConfig {
  /**
   * Treat an upstream `MAX(ts)` that moved by fewer than this many seconds as unchanged for the B3 freshness comparison — the late-arriving-but- irrelevant micro-update analog of a freshness SLA threshold. Default `0`: any movement at all forces a rebuild.
   */
  lag_tolerance_seconds?: number;
  /**
   * Allow a rowcount-only data-stability signal (`COUNT(*)`) when an upstream has no tracked timestamp column. Default `false`: without an explicit opt-in, a model whose upstreams are not watermarkable is not skip-eligible. Rowcount equality is weaker than a watermark: it can miss a same-size in-place `UPDATE` (or a matched insert+delete) that mutates values without changing the row count, so it stays behind this switch.
   */
  skip_rowcount_fallback?: boolean;
  /**
   * Master switch for the model-skip gate. `false` (default) ⇒ every selected model always builds, exactly as before. The `--skip-unchanged` CLI flag turns the gate on for a single invocation regardless of this value.
   */
  skip_unchanged?: boolean;
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
   * Per-pipeline / per-client state-file namespacing. Defaults to [`StateNamespacing::None`] (one global state file — byte-identical to a project that omits this key). Set `namespacing = "pipeline"` to give each pipeline its own `<models>/.rocky-state/<pipeline>.redb` so independent fan-out runs don't serialize on one advisory lock. The per-invocation `--state-namespace <key>` flag overrides this; an explicit `--state-path` disables namespacing for that run.
   */
  namespacing?: StateNamespacing & string;
  /**
   * What to do when the engine opens a state store whose schema version is **newer** than this binary supports (a forward-incompatibility, which happens during a rolling upgrade that crosses a redb schema version). Defaults to [`SchemaMismatchPolicy::Recreate`] — the old pod bootstraps fresh, does one full-refresh run, and never clobbers the newer shared state. Set to `fail` to keep the historical hard-abort behaviour. Only the run path honours this; inspection/branch commands still hard-fail on a forward-incompatible store. See [`SchemaMismatchPolicy`].
   */
  on_schema_mismatch?: SchemaMismatchPolicy & string;
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
   * Valkey/Redis URL for state persistence. May embed credentials, so the value is redacted in serialized config and logs.
   */
  valkey_url?: RedactedString | null;
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
  /**
   * Wall-clock budget (in milliseconds) the auto-sweep measures itself against at end-of-run. The sweep always runs to completion; exceeding the budget flips the per-run log line from `tracing::debug` to `tracing::warn` so operators can spot a state store that has grown large enough to warrant manual intervention. Defaults to [`DEFAULT_STATE_RETENTION_SWEEP_BUDGET_MS`].
   */
  sweep_budget_ms?: number;
  /**
   * Minimum number of seconds between two automatic end-of-run sweeps. The `rocky run` end-of-run hook reads `last_retention_sweep_at` from the state store's metadata table and skips the sweep when `now - last < sweep_interval_seconds`. The manual `rocky state retention sweep` subcommand is unaffected — it always runs. Defaults to [`DEFAULT_STATE_RETENTION_SWEEP_INTERVAL_SECONDS`].
   */
  sweep_interval_seconds?: number;
}
