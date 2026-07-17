use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use indexmap::IndexMap;
use regex::Regex;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::warn;

use crate::hooks::HooksConfig;
use crate::redacted::RedactedString;
use crate::schema::SchemaPattern;

// ===========================================================================
// Config key deprecation framework
// ===========================================================================

/// A mapping from an old (deprecated) config key to its replacement.
///
/// Used to provide a migration path for post-1.0 config key renames without
/// breaking existing user configs. When a deprecated key is found during
/// config loading, the value is remapped to the new key and a warning is
/// emitted.
#[derive(Debug, Clone)]
pub struct DeprecatedKey {
    /// Dot-delimited path to the old key (e.g., `"state.type"`).
    pub old_key: &'static str,
    /// Dot-delimited path to the new key (e.g., `"state.backend"`).
    pub new_key: &'static str,
    /// The version in which the key was deprecated (e.g., `"1.1.0"`).
    pub since_version: &'static str,
    /// Human-readable migration hint appended to the warning.
    pub message: &'static str,
}

/// A deprecation warning emitted when a deprecated config key is encountered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeprecationWarning {
    /// The old key path that was found.
    pub old_key: String,
    /// The new key path the value was remapped to.
    pub new_key: String,
    /// The version in which the key was deprecated.
    pub since_version: String,
    /// Human-readable migration hint.
    pub message: String,
}

/// Registry of all deprecated config keys.
///
/// Add entries here when renaming a config key post-1.0. The deprecation
/// framework will automatically warn and remap during config loading.
///
/// Each entry's `old_key` and `new_key` are dot-delimited paths relative
/// to the top-level TOML table (e.g., `"state.type"` → `"state.backend"`).
/// Only simple renames within the same parent table are supported.
const DEPRECATED_KEYS: &[DeprecatedKey] = &[
    // Example (commented out — uncomment when an actual rename happens):
    // DeprecatedKey {
    //     old_key: "state.type",
    //     new_key: "state.backend",
    //     since_version: "1.1.0",
    //     message: "Rename 'type' to 'backend' in the [state] section.",
    // },
];

/// Walks the TOML value tree and remaps deprecated keys to their new names.
///
/// Returns a list of [`DeprecationWarning`]s for every deprecated key that
/// was found and remapped. If the new key already exists in the table, the
/// old key is removed but the new key's value takes precedence.
fn apply_deprecations(value: &mut toml::Value) -> Vec<DeprecationWarning> {
    let mut warnings = Vec::new();

    let Some(root) = value.as_table_mut() else {
        return warnings;
    };

    for dep in DEPRECATED_KEYS {
        let old_segments: Vec<&str> = dep.old_key.split('.').collect();
        let new_segments: Vec<&str> = dep.new_key.split('.').collect();

        // Both old and new must have at least one segment, and the parent
        // path (all segments except the last) must be identical — we only
        // support renames within the same parent table.
        if old_segments.is_empty() || new_segments.is_empty() {
            continue;
        }

        let old_leaf = old_segments[old_segments.len() - 1];
        let new_leaf = new_segments[new_segments.len() - 1];
        let parent_path = &old_segments[..old_segments.len() - 1];

        // Navigate to the parent table.
        let Some(parent) = navigate_to_table_mut(root, parent_path) else {
            continue;
        };

        // Check if the old key exists.
        let Some(old_value) = parent.remove(old_leaf) else {
            continue;
        };

        let warning = DeprecationWarning {
            old_key: dep.old_key.to_string(),
            new_key: dep.new_key.to_string(),
            since_version: dep.since_version.to_string(),
            message: dep.message.to_string(),
        };

        warn!(
            old_key = dep.old_key,
            new_key = dep.new_key,
            since = dep.since_version,
            "Deprecated config key '{}': {}. Use '{}' instead (deprecated since v{}).",
            dep.old_key,
            dep.message,
            dep.new_key,
            dep.since_version,
        );

        // Only remap if the new key doesn't already exist — new key takes precedence.
        if !parent.contains_key(new_leaf) {
            parent.insert(new_leaf.to_string(), old_value);
        }

        warnings.push(warning);
    }

    warnings
}

/// Navigates into nested TOML tables following the given path segments.
///
/// Returns `None` if any segment along the path is missing or not a table.
fn navigate_to_table_mut<'a>(
    root: &'a mut toml::map::Map<String, toml::Value>,
    path: &[&str],
) -> Option<&'a mut toml::map::Map<String, toml::Value>> {
    let mut current = root;
    for &segment in path {
        current = current.get_mut(segment)?.as_table_mut()?;
    }
    Some(current)
}

/// Errors from loading and parsing pipeline configuration.
#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("No rocky.toml found at '{}'", .path.display())]
    FileNotFound { path: PathBuf },

    #[error("failed to read config file: {0}")]
    ReadFile(#[from] std::io::Error),

    #[error("failed to parse TOML: {0}")]
    ParseToml(#[from] toml::de::Error),

    /// TOML parse/deserialize error after env-var substitution, enriched with
    /// the list of substituted env vars so the operator can narrow the cause
    /// (e.g. a negative `timeout_secs` that came from `${TIMEOUT}`).
    #[error("failed to parse TOML: {source}{env_var_hint}")]
    ParseTomlWithEnvContext {
        #[source]
        source: toml::de::Error,
        env_var_hint: String,
    },

    #[error("environment variable '{name}' not set (referenced in config)")]
    MissingEnvVar {
        name: String,
        /// Byte range of the `${VAR}` placeholder in the raw config file.
        /// Used by the CLI error reporter to render source spans.
        span: Option<std::ops::Range<usize>>,
    },

    #[error("invalid schema pattern: {0}")]
    InvalidPattern(#[from] crate::schema::SchemaError),

    #[error(
        "adapter '{name}' (type '{adapter_type}') is discovery-only; \
         add `kind = \"discovery\"` to the [adapter.{name}] block"
    )]
    AdapterMissingDiscoveryKind { name: String, adapter_type: String },

    #[error(
        "adapter '{name}' has `kind = \"{declared}\"` but type '{adapter_type}' only supports {supported}"
    )]
    AdapterKindUnsupported {
        name: String,
        adapter_type: String,
        declared: String,
        supported: String,
    },

    #[error(
        "pipeline '{pipeline}' source.adapter = '{adapter}' points to an adapter whose `kind` excludes data movement"
    )]
    PipelineSourceAdapterNotData { pipeline: String, adapter: String },

    #[error(
        "pipeline '{pipeline}' source.discovery.adapter = '{adapter}' points to an adapter whose `kind` excludes discovery"
    )]
    PipelineDiscoveryAdapterNotDiscovery { pipeline: String, adapter: String },

    #[error(
        "pipeline '{pipeline}' uses strategy = \"merge\" but neither merge_keys nor merge_keys_fallback is configured. \
         Set [pipeline.{pipeline}.merge_keys = [\"col1\", \"col2\"]] in your rocky.toml."
    )]
    ReplicationMergeMissingKeys { pipeline: String },

    #[error(
        "pipeline '{pipeline}' table_overrides[{rule_index}] has an empty `match` block. \
         Set at least one of `match.connector` or `match.table`, or remove the rule and \
         change pipeline-level defaults instead."
    )]
    TableOverrideEmptyMatch { pipeline: String, rule_index: usize },

    #[error(
        "pipeline '{pipeline}' table_overrides have two duplicate fully-specific rules \
         (connector = {connector:?}, table = {table:?}) at indices {first_index} and \
         {second_index}. Combine them or narrow one further."
    )]
    TableOverrideDuplicate {
        pipeline: String,
        first_index: usize,
        second_index: usize,
        connector: String,
        table: String,
    },

    #[error(
        "pipeline '{pipeline}' table_overrides[{rule_index}] has an invalid glob pattern \
         in `match.table`: {reason}"
    )]
    TableOverrideInvalidGlob {
        pipeline: String,
        rule_index: usize,
        reason: String,
    },

    #[error(
        "pipeline '{pipeline}' table_overrides[{rule_index}] sets strategy = \"merge\" \
         but no merge_keys (or merge_keys_fallback) are reachable for the matched tables — \
         neither the override nor the pipeline default supplies any."
    )]
    TableOverrideMergeMissingKeys { pipeline: String, rule_index: usize },

    #[error(
        "pipeline '{pipeline}' source.schema_pattern.components contains the reserved \
         name {component:?} — rename it (e.g. `source_{component}`). \
         `table` and `id` are reserved for `--filter` and `[[table_overrides]]`."
    )]
    SchemaPatternReservedComponent { pipeline: String, component: String },

    #[error(
        "[adapter.{adapter}.cache] backend = \"{backend}\" requires `{field}` — set the field \
         under [adapter.{adapter}.cache] or change `backend`"
    )]
    FivetranCacheMissingField {
        adapter: String,
        backend: String,
        field: String,
    },

    #[error(
        "[adapter.{adapter}.ratelimit] backend = \"{backend}\" requires `{field}` — set the field \
         under [adapter.{adapter}.ratelimit] or change `backend`"
    )]
    FivetranRatelimitMissingField {
        adapter: String,
        backend: String,
        field: String,
    },

    #[error(
        "[adapter.{adapter}.stampede] backend = \"{backend}\" requires `{field}` — set the field \
         under [adapter.{adapter}.stampede] or change `backend`"
    )]
    FivetranStampedeMissingField {
        adapter: String,
        backend: String,
        field: String,
    },

    #[error(
        "[adapter.{adapter}.circuit_breaker] backend = \"{backend}\" requires `{field}` — set the \
         field under [adapter.{adapter}.circuit_breaker] or change `backend`"
    )]
    FivetranCircuitBreakerMissingField {
        adapter: String,
        backend: String,
        field: String,
    },

    #[error("[policy] version = {version} is unsupported; the only supported version is 1")]
    PolicyUnsupportedVersion { version: u32 },

    #[error(
        "[policy] rules[{rule_index}] sets scope.any = true alongside other scope keys — \
         `any` is mutually exclusive with `models`/`tags`/`classifications`/… \
         (remove `any`, or drop the other keys)"
    )]
    PolicyScopeAnyConflict { rule_index: usize },

    #[error(
        "[policy] rules[{rule_index}] has an empty scope — set `scope.any = true` to match \
         every model, or add at least one predicate (`models`, `tags`, `classifications`, \
         `contracted`, `layer`, …)"
    )]
    PolicyScopeEmpty { rule_index: usize },

    #[error(
        "[policy] rules[{rule_index}] autonomy_budget.window = {window:?} is not a valid \
         duration — use a `<N>d` / `<N>h` span (e.g. \"7d\", \"24h\")"
    )]
    PolicyBudgetInvalidWindow { rule_index: usize, window: String },

    #[error(
        "[policy] rules[{rule_index}] autonomy_budget.failures = 0 is invalid — a budget must \
         allow at least one failure before it degrades the rule (use `failures = 1` or higher)"
    )]
    PolicyBudgetZeroFailures { rule_index: usize },
}

/// Concurrency strategy for table processing.
///
/// Controls how many tables Rocky processes in parallel. Accepts either
/// `"adaptive"` (AIMD throttle that adjusts based on rate-limit signals)
/// or a fixed integer (e.g. `8`).
///
/// # TOML examples
///
/// ```toml
/// # Use adaptive concurrency (default) — starts at max, adjusts on 429s
/// concurrency = "adaptive"
///
/// # Fixed concurrency — always 8 in-flight tables
/// concurrency = 8
///
/// # Serial execution
/// concurrency = 1
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum ConcurrencyMode {
    /// AIMD-based adaptive concurrency. Starts at `max` (default 32) and
    /// dynamically adjusts based on warehouse rate-limit signals (HTTP 429,
    /// `UC_REQUEST_LIMIT_EXCEEDED`). On sustained success, concurrency
    /// slowly increases; on rate limits, it halves.
    #[default]
    Adaptive,
    /// Fixed concurrency — always exactly this many in-flight tables.
    /// Clamped to at least 1.
    Fixed(usize),
}

/// Default adaptive max when no explicit cap is given.
const ADAPTIVE_MAX_CONCURRENCY: usize = 32;

impl ConcurrencyMode {
    /// Whether this mode uses the AIMD adaptive throttle.
    pub fn is_adaptive(&self) -> bool {
        matches!(self, Self::Adaptive)
    }

    /// The maximum number of in-flight tables this mode allows.
    ///
    /// - `Adaptive` → [`ADAPTIVE_MAX_CONCURRENCY`] (32)
    /// - `Fixed(n)` → `n` (clamped to at least 1)
    pub fn max_concurrency(&self) -> usize {
        match self {
            Self::Adaptive => ADAPTIVE_MAX_CONCURRENCY,
            Self::Fixed(n) => (*n).max(1),
        }
    }
}

impl std::fmt::Display for ConcurrencyMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Adaptive => write!(f, "adaptive"),
            Self::Fixed(n) => write!(f, "{n}"),
        }
    }
}

impl Serialize for ConcurrencyMode {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Adaptive => serializer.serialize_str("adaptive"),
            Self::Fixed(n) => serializer.serialize_u64(*n as u64),
        }
    }
}

impl<'de> Deserialize<'de> for ConcurrencyMode {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw {
            Str(String),
            Num(usize),
        }
        match Raw::deserialize(deserializer)? {
            Raw::Str(s) if s.eq_ignore_ascii_case("adaptive") => Ok(ConcurrencyMode::Adaptive),
            Raw::Str(s) => Err(serde::de::Error::custom(format!(
                "invalid concurrency mode '{s}': expected \"adaptive\" or an integer"
            ))),
            Raw::Num(n) => Ok(ConcurrencyMode::Fixed(n.max(1))),
        }
    }
}

impl JsonSchema for ConcurrencyMode {
    fn schema_name() -> String {
        "ConcurrencyMode".to_owned()
    }

    fn json_schema(_: &mut schemars::r#gen::SchemaGenerator) -> schemars::schema::Schema {
        // Mirrors the custom Deserialize: either the literal "adaptive" or
        // a positive integer. Schemars can't derive this — `ConcurrencyMode`
        // doesn't have `Serialize`/`Deserialize` derive flags it can read.
        let adaptive = schemars::schema::Schema::Object(schemars::schema::SchemaObject {
            instance_type: Some(schemars::schema::InstanceType::String.into()),
            enum_values: Some(vec![serde_json::Value::String("adaptive".to_owned())]),
            ..Default::default()
        });
        let fixed = schemars::schema::Schema::Object(schemars::schema::SchemaObject {
            instance_type: Some(schemars::schema::InstanceType::Integer.into()),
            number: Some(Box::new(schemars::schema::NumberValidation {
                minimum: Some(1.0),
                ..Default::default()
            })),
            ..Default::default()
        });
        schemars::schema::Schema::Object(schemars::schema::SchemaObject {
            metadata: Some(Box::new(schemars::schema::Metadata {
                description: Some(
                    "Concurrency strategy: the literal `\"adaptive\"` for AIMD-based throttling, or a positive integer for fixed concurrency.".to_owned(),
                ),
                ..Default::default()
            })),
            subschemas: Some(Box::new(schemars::schema::SubschemaValidation {
                any_of: Some(vec![adaptive, fixed]),
                ..Default::default()
            })),
            ..Default::default()
        })
    }
}

/// Controls parallelism and error handling for table processing.
///
/// Rocky processes tables within a run concurrently using async tasks.
/// Tune `concurrency` based on your warehouse capacity.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ExecutionConfig {
    /// Concurrency strategy (default: `"adaptive"`).
    ///
    /// - `"adaptive"` — AIMD throttle that starts at 32 and adjusts based on
    ///   rate-limit signals. Best for remote warehouses (Databricks, Snowflake).
    /// - An integer (e.g. `8`) — fixed concurrency, always this many in-flight
    ///   tables. Use for local adapters (DuckDB) or when you know the limit.
    /// - `1` — serial execution.
    #[serde(default)]
    pub concurrency: ConcurrencyMode,
    /// If true, abort all remaining tables on first error.
    /// If false, process all tables and report errors at the end (partial success).
    #[serde(default)]
    pub fail_fast: bool,
    /// Abort remaining tables if error rate exceeds this percentage (0-100).
    /// Prevents wasting compute when the warehouse is unhealthy.
    /// Default: 50 (abort if more than half of completed tables failed). Set to 0 to disable.
    #[serde(default = "default_error_rate_abort_pct")]
    pub error_rate_abort_pct: u32,
    /// Number of times to retry failed tables after the initial parallel phase.
    /// Default: 1. Set to 0 to disable auto-retry.
    #[serde(default = "default_table_retries")]
    pub table_retries: u32,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            concurrency: ConcurrencyMode::default(),
            fail_fast: false,
            error_rate_abort_pct: default_error_rate_abort_pct(),
            table_retries: default_table_retries(),
        }
    }
}

fn default_error_rate_abort_pct() -> u32 {
    50
}

fn default_table_retries() -> u32 {
    1
}

/// State storage backend variants.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum StateBackend {
    /// State stored on local disk (default). No sync needed.
    #[default]
    Local,
    /// State synced to/from an S3 bucket via the `object_store` crate.
    S3,
    /// State synced to/from a Google Cloud Storage bucket.
    Gcs,
    /// State synced to/from a Valkey/Redis instance.
    Valkey,
    /// Tiered: Valkey (fast) with S3 fallback (durable).
    Tiered,
}

impl std::fmt::Display for StateBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StateBackend::Local => write!(f, "local"),
            StateBackend::S3 => write!(f, "s3"),
            StateBackend::Gcs => write!(f, "gcs"),
            StateBackend::Valkey => write!(f, "valkey"),
            StateBackend::Tiered => write!(f, "tiered"),
        }
    }
}

/// Policy applied when state upload fails after retries + circuit-breaker
/// are exhausted. See [`StateConfig::on_upload_failure`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StateUploadFailureMode {
    /// Log a warning and continue the run successfully. State becomes
    /// stale until the next healthy upload; the next run's `discover`
    /// re-derives watermarks from target-table metadata. Trades state
    /// durability for run liveness — matches the de-facto behaviour of
    /// the pre-retry callers in `rocky run`, which already `warn + continue`
    /// on a failed upload.
    #[default]
    Skip,
    /// Propagate the error to the caller. Appropriate for strict
    /// environments where state durability matters more than liveness
    /// (e.g. long-running backfills where re-deriving watermarks is
    /// prohibitively expensive).
    Fail,
}

/// Policy applied when the engine opens a state store whose schema version is
/// **newer** than the binary supports (a *forward*-incompatibility).
///
/// This is the deploy-safety knob for rolling upgrades that cross a redb
/// schema version. During a rolling bump, pods on the *old* binary can read
/// state written by *already-upgraded* pods through a shared tiered backend.
/// The default ([`Recreate`][Self::Recreate]) degrades that window instead of
/// breaking it: the old pod does one full-refresh run and succeeds, rather than
/// stranding the orchestrated run in an hour-long retry spiral.
///
/// Only the *forward* case (on-disk newer than binary) is governed here.
/// *Backward*-compatibility — a newer binary reading older state — always
/// auto-migrates forward as before; there is no knob for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum SchemaMismatchPolicy {
    /// Treat a forward-incompatible store as cold: log a single `WARN`,
    /// bootstrap a fresh local state, and **never write the downgraded state
    /// back** to the shared tier (so the newer state upgraded pods depend on is
    /// left intact). The run proceeds as a full refresh. Default — it turns a
    /// hard, run-stranding failure into a graceful one-time full refresh during
    /// the mixed-version window of a schema-changing upgrade.
    #[default]
    Recreate,
    /// Abort the open with a clear error (the historical behaviour).
    /// Appropriate when an operator would rather an incompatible pod fail
    /// loudly than do a full refresh against newer shared state.
    Fail,
}

impl std::fmt::Display for SchemaMismatchPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchemaMismatchPolicy::Recreate => write!(f, "recreate"),
            SchemaMismatchPolicy::Fail => write!(f, "fail"),
        }
    }
}

/// Per-pipeline / per-client state-file namespacing policy.
///
/// redb permits one writer per file, so fanning out one `rocky run` process
/// per pipeline or client against the single global state file forces those
/// independent runs to serialize on one advisory lock. Namespacing gives each
/// namespace its own `<models>/.rocky-state/<namespace>.redb` file (its own
/// lock, its own redb handle, its own remote object key), so runs on distinct
/// namespaces proceed concurrently with zero shared corruption surface.
///
/// Default is [`StateNamespacing::None`] — behavior is **byte-identical** to a
/// project that never sets this key. Namespacing is purely opt-in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum StateNamespacing {
    /// One global `<models>/.rocky-state.redb` for the whole project (default).
    /// Identical to today's behavior.
    #[default]
    None,
    /// One state file per pipeline, under `<models>/.rocky-state/<pipeline>.redb`.
    /// The per-invocation `--state-namespace <key>` flag takes precedence over
    /// this config and is the explicit way to fan out by client/tenant rather
    /// than by pipeline name. An explicit `--state-path` disables namespacing
    /// entirely for that invocation.
    Pipeline,
}

/// State persistence configuration.
///
/// Controls where Rocky stores watermarks and anomaly history between runs.
/// On ephemeral environments (EKS pods), use S3, GCS, or Valkey for persistence.
///
/// When both S3 and Valkey are configured (`backend = "tiered"`):
/// - Download: Valkey first (fast), S3 fallback (durable)
/// - Upload: write to both Valkey + S3
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StateConfig {
    /// Storage backend: local (default), s3, gcs, valkey, or tiered (valkey + s3 fallback)
    #[serde(default)]
    pub backend: StateBackend,
    /// S3 bucket for state persistence
    pub s3_bucket: Option<String>,
    /// S3 key prefix (default: "rocky/state/")
    pub s3_prefix: Option<String>,
    /// GCS bucket for state persistence
    pub gcs_bucket: Option<String>,
    /// GCS key prefix (default: "rocky/state/")
    pub gcs_prefix: Option<String>,
    // Wrapped in `RedactedString`: serializes to "***" (e.g. in the plan
    // config snapshot) and prints as `***` in Debug/logs, so the embedded
    // credential never lands in the on-disk plan or a trace. Call `.expose()`
    // at the point of use (state sync / idempotency) for the real URL.
    /// Valkey/Redis URL for state persistence. May embed credentials, so the
    /// value is redacted in serialized config and logs.
    pub valkey_url: Option<RedactedString>,
    /// Valkey key prefix (default: "rocky:state:")
    pub valkey_prefix: Option<String>,
    /// Wall-clock budget (seconds) for each state transfer operation
    /// (download or upload). Catches stuck SDK retry loops, DNS, TLS, and
    /// hung endpoints that the per-request HTTP timeout does not see.
    /// Defaults to 300s; raise for large state or slow networks.
    #[serde(default = "default_state_transfer_timeout_secs")]
    pub transfer_timeout_seconds: u64,
    /// Retry policy applied to transient state-transfer failures (network
    /// hiccups, hung endpoints that hit the per-request HTTP timeout,
    /// transient 5xx, etc.). Shares the same shape as the adapter retry
    /// config so operators can reason about both with a single mental
    /// model. Retries share the outer `transfer_timeout_seconds` budget,
    /// so the total wall-clock ceiling is unchanged.
    #[serde(default)]
    pub retry: RetryConfig,
    /// What to do when state upload exhausts retries + circuit-breaker.
    /// Defaults to `skip` — rocky continues the run and the next run
    /// re-derives state from target-table metadata. See
    /// [`StateUploadFailureMode`].
    #[serde(default)]
    pub on_upload_failure: StateUploadFailureMode,
    /// Per-run idempotency-key policy (`rocky run --idempotency-key`).
    /// Controls retention of stamped keys, what terminal statuses count as
    /// "deduplicated", and how long an `InFlight` entry survives before it's
    /// treated as a crashed-pod corpse and adopted by a fresh caller. See
    /// [`IdempotencyConfig`].
    #[serde(default)]
    pub idempotency: IdempotencyConfig,
    /// Retention policy applied to Rocky's own `state.redb` tables (run
    /// history, DAG snapshots, quality snapshots). Bounds the size of the
    /// control-plane store; operational tables (schema cache, watermarks,
    /// partition records) are never swept by this policy. See
    /// [`crate::retention::StateRetentionConfig`].
    #[serde(default)]
    pub retention: crate::retention::StateRetentionConfig,
    /// Per-pipeline / per-client state-file namespacing. Defaults to
    /// [`StateNamespacing::None`] (one global state file — byte-identical to a
    /// project that omits this key). Set `namespacing = "pipeline"` to give
    /// each pipeline its own `<models>/.rocky-state/<pipeline>.redb` so
    /// independent fan-out runs don't serialize on one advisory lock. The
    /// per-invocation `--state-namespace <key>` flag overrides this; an
    /// explicit `--state-path` disables namespacing for that run.
    #[serde(default)]
    pub namespacing: StateNamespacing,
    /// What to do when the engine opens a state store whose schema version is
    /// **newer** than this binary supports (a forward-incompatibility, which
    /// happens during a rolling upgrade that crosses a redb schema version).
    /// Defaults to [`SchemaMismatchPolicy::Recreate`] — the old pod bootstraps
    /// fresh, does one full-refresh run, and never clobbers the newer shared
    /// state. Set to `fail` to keep the historical hard-abort behaviour. Only
    /// the run path honours this; inspection/branch commands still hard-fail on
    /// a forward-incompatible store. See [`SchemaMismatchPolicy`].
    #[serde(default)]
    pub on_schema_mismatch: SchemaMismatchPolicy,
}

impl Default for StateConfig {
    fn default() -> Self {
        Self {
            backend: StateBackend::default(),
            s3_bucket: None,
            s3_prefix: None,
            gcs_bucket: None,
            gcs_prefix: None,
            valkey_url: None,
            valkey_prefix: None,
            transfer_timeout_seconds: default_state_transfer_timeout_secs(),
            retry: RetryConfig::default(),
            on_upload_failure: StateUploadFailureMode::default(),
            idempotency: IdempotencyConfig::default(),
            retention: crate::retention::StateRetentionConfig::default(),
            namespacing: StateNamespacing::default(),
            on_schema_mismatch: SchemaMismatchPolicy::default(),
        }
    }
}

fn default_state_transfer_timeout_secs() -> u64 {
    300
}

/// Policy controlling which terminal outcomes count for
/// [`IdempotencyConfig::dedup_on`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DedupPolicy {
    /// Only successful runs stamp a persistent dedup entry; failed runs
    /// leave the key claimable for a retry. Default — matches the common
    /// "dedup successful notifications, allow retries" use case.
    #[default]
    Success,
    /// Any terminal status stamps a persistent entry — subsequent calls
    /// with the same key always skip, regardless of whether the prior run
    /// succeeded. Use when replays are expensive even for failures.
    Any,
}

/// Config for `rocky run --idempotency-key` dedup.
///
/// All fields are optional with sensible defaults. Block is present even
/// when the user doesn't set `--idempotency-key`; it's a no-op in that case.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct IdempotencyConfig {
    /// Number of days a `Succeeded` (or `Failed`-under-`any`) stamp is kept
    /// before GC. Default 30. GC runs during the state upload sweep.
    #[serde(default = "default_idempotency_retention_days")]
    pub retention_days: u32,
    /// Which terminal statuses count as "already processed" for dedup. See
    /// [`DedupPolicy`]. Default [`DedupPolicy::Success`].
    #[serde(default)]
    pub dedup_on: DedupPolicy,
    /// Hours after which an `InFlight` entry is treated as a crashed-pod
    /// corpse and adopted by a fresh caller. Default 24. Applies only to
    /// backends whose in-flight lock does not carry a server-side TTL —
    /// Valkey providers set `EX` directly on `SET NX`, so this field is
    /// informational for them.
    #[serde(default = "default_idempotency_in_flight_ttl_hours")]
    pub in_flight_ttl_hours: u32,
}

impl Default for IdempotencyConfig {
    fn default() -> Self {
        Self {
            retention_days: default_idempotency_retention_days(),
            dedup_on: DedupPolicy::default(),
            in_flight_ttl_hours: default_idempotency_in_flight_ttl_hours(),
        }
    }
}

fn default_idempotency_retention_days() -> u32 {
    30
}

fn default_idempotency_in_flight_ttl_hours() -> u32 {
    24
}

/// Retry policy for transient warehouse errors (HTTP 429/503, rate limits, timeouts).
///
/// Rocky retries transient errors with exponential backoff and optional jitter
/// to prevent thundering herd across concurrent runs.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RetryConfig {
    /// Maximum number of retry attempts. Set to 0 to disable retries (e.g. for CI).
    ///
    /// When the Fivetran shared circuit breaker is enabled (`[adapter.fivetran.circuit_breaker]`
    /// with a non-default backend), keep `max_retries` ≤ 4 so a single 429-storm bursts
    /// at most ~5 attempts (`max_retries + 1`) per envelope-fetch before voting `Remote`
    /// to the breaker. Higher values lengthen the storm without changing the outcome —
    /// the breaker still trips after `failure_threshold` envelope-fetches exhaust their
    /// retry budget, and the orchestrator only sees the result after that.
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    /// Initial backoff duration in milliseconds before the first retry.
    #[serde(default = "default_initial_backoff_ms")]
    pub initial_backoff_ms: u64,
    /// Maximum backoff duration in milliseconds (caps exponential growth).
    #[serde(default = "default_max_backoff_ms")]
    pub max_backoff_ms: u64,
    /// Backoff multiplier applied after each retry (e.g. 2.0 = double each time).
    #[serde(default = "default_backoff_multiplier")]
    pub backoff_multiplier: f64,
    /// Add random jitter to prevent concurrent runs from retrying in lockstep.
    #[serde(default = "default_jitter")]
    pub jitter: bool,
    /// Circuit breaker: trip after this many consecutive transient failures across statements.
    /// Once tripped, all subsequent statements fail immediately without attempting execution.
    /// Default: 5. Set to 0 to disable.
    #[serde(default = "default_circuit_breaker_threshold")]
    pub circuit_breaker_threshold: u32,
    /// Seconds the breaker will stay `Open` before a single trial
    /// request is allowed through (half-open state). On trial success
    /// the breaker closes and resumes normal traffic; on trial failure
    /// it re-opens immediately. `None` preserves the pre-Arc-3
    /// "manual-reset-only" behaviour — a tripped breaker stays tripped
    /// for the rest of the run.
    #[serde(default)]
    pub circuit_breaker_recovery_timeout_secs: Option<u64>,
    /// Cross-statement retry budget for a single run (§P2.7). When set,
    /// adapters construct a [`crate::retry_budget::RetryBudget`] from this
    /// value and decrement it on every retry; once exhausted, remaining
    /// statements fail fast with adapter-specific `RetryBudgetExhausted`
    /// errors instead of burning the warehouse's rate-limit quota.
    ///
    /// `None` (default) keeps legacy behaviour — per-statement
    /// [`RetryConfig::max_retries`] is the only bound. `Some(0)` means no
    /// retries are allowed for the whole run.
    #[serde(default)]
    pub max_retries_per_run: Option<u32>,
}

impl RetryConfig {
    /// Build a [`crate::circuit_breaker::CircuitBreaker`] shaped by
    /// this retry config. When `circuit_breaker_recovery_timeout_secs`
    /// is set, the breaker is created with timed half-open recovery;
    /// otherwise it is manual-reset only.
    #[must_use]
    pub fn build_circuit_breaker(&self) -> crate::circuit_breaker::CircuitBreaker {
        match self.circuit_breaker_recovery_timeout_secs {
            Some(secs) => crate::circuit_breaker::CircuitBreaker::with_recovery_timeout(
                self.circuit_breaker_threshold,
                std::time::Duration::from_secs(secs),
            ),
            None => crate::circuit_breaker::CircuitBreaker::new(self.circuit_breaker_threshold),
        }
    }
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: default_max_retries(),
            initial_backoff_ms: default_initial_backoff_ms(),
            max_backoff_ms: default_max_backoff_ms(),
            backoff_multiplier: default_backoff_multiplier(),
            jitter: default_jitter(),
            circuit_breaker_threshold: default_circuit_breaker_threshold(),
            circuit_breaker_recovery_timeout_secs: None,
            max_retries_per_run: None,
        }
    }
}

fn default_circuit_breaker_threshold() -> u32 {
    5
}

fn default_max_retries() -> u32 {
    3
}
fn default_initial_backoff_ms() -> u64 {
    1000
}
fn default_max_backoff_ms() -> u64 {
    30000
}
fn default_backoff_multiplier() -> f64 {
    2.0
}
fn default_jitter() -> bool {
    true
}

/// `[resilience]` — the run loop's classified-retry policy.
///
/// This is a **distinct layer** from the per-adapter `[adapter.*.retry]`
/// (which retries individual statements inside a connector) and from the
/// run-level `[retry]` budget ([`RunRetryConfig`], which caps connector
/// retries). `[resilience]` governs whether the run loop re-runs a whole
/// *model* whose materialization failed, classifying the failure via
/// [`crate::failure_class::FailureClass`] and retrying only a *proven*
/// transient one.
///
/// # Not default-OFF, but conservative
///
/// Unlike the skip / reuse gates, this layer is **on by default** — a model
/// that fails transiently today *will* be retried once this ships. The lever
/// that keeps that safe is [`Self::transient_max_retries`] (default `2`): a
/// small, bounded budget with capped exponential backoff. Set
/// `transient_max_retries = 0` (or `enabled = false`) to restore the prior
/// single-attempt behaviour, e.g. in CI where a fast-fail is preferred.
///
/// Permanent and Unknown failures are **never** retried regardless of these
/// settings — that is a property of the classifier, not of this config.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ResilienceConfig {
    /// Master switch for run-loop classified retry. Default `true`. When
    /// `false`, every model is attempted exactly once (no classification, no
    /// backoff) — the behaviour before this layer existed.
    #[serde(default = "default_resilience_enabled")]
    pub enabled: bool,
    /// Maximum re-runs of a model that failed with a *transient* class.
    /// Conservative default: `2` (so at most three attempts total). `0`
    /// disables retry while leaving the classifier active for observability.
    #[serde(default = "default_transient_max_retries")]
    pub transient_max_retries: u32,
    /// Initial backoff (ms) before the first retry.
    #[serde(default = "default_resilience_initial_backoff_ms")]
    pub initial_backoff_ms: u64,
    /// Maximum backoff (ms) — caps the exponential growth.
    #[serde(default = "default_resilience_max_backoff_ms")]
    pub max_backoff_ms: u64,
    /// Multiplier applied to the backoff after each retry.
    #[serde(default = "default_backoff_multiplier")]
    pub backoff_multiplier: f64,
    /// Add ±25 % jitter so concurrent runs don't retry in lockstep.
    #[serde(default = "default_jitter")]
    pub jitter: bool,
    /// Trip the run-loop breaker after this many *consecutive* transient
    /// model failures; once tripped, no further model is retried for the rest
    /// of the run (they still get their one attempt). Default: `3`. `0`
    /// disables the breaker.
    #[serde(default = "default_resilience_breaker_threshold")]
    pub circuit_breaker_threshold: u32,
    /// Optional ceiling on the *total* number of retries across all models in
    /// one run — a global budget separate from the per-adapter one. Default
    /// `Some(8)`: a conservative cap so one flaky layer can't spin the whole
    /// run. `None` removes the ceiling (per-model `transient_max_retries` is
    /// then the only bound); `Some(0)` forbids all retries.
    #[serde(default = "default_resilience_max_retries_per_run")]
    pub max_retries_per_run: Option<u32>,
    /// Continue disjoint subgraphs when a model fails, instead of aborting the
    /// whole run at the first failure. Default `false` (fail-fast — the run
    /// stops at the first failing model, exactly as before this knob existed).
    ///
    /// When `true`, a failed model and its downstream closure are *withheld*
    /// (never built on the failure's stale/missing output), while unrelated
    /// subtrees still materialize; the run reports `PartialFailure` with a
    /// containment manifest naming what failed and its blast radius. This
    /// changes no data semantics — everything withheld is already withheld by
    /// today's fail-fast run; it only *narrows* the withholding to the actual
    /// blast radius. The closure is conservative (computed from the resolved
    /// dependency graph, contain-more on any doubt).
    #[serde(default = "default_contain_failures")]
    pub contain_failures: bool,
    /// Opt in to policy-governed auto-apply of **additive** source drift.
    /// Default `false` — a run detecting a new nullable upstream column
    /// evolves the target exactly as it does today (unconditionally), with no
    /// policy gate. When `true`, any drift mutation must first clear the
    /// policy plane: only a *provably additive* change with an `allow` verdict
    /// for the `schema_change.additive` capability is auto-applied; anything
    /// else (a drop, retype, narrowing, or a scope without the grant) is
    /// refused and left for review rather than mutated. Has no effect unless a
    /// `[policy]` block grants the capability, so both the opt-in **and** a
    /// policy rule are required to change behaviour.
    #[serde(default = "default_auto_apply_additive_drift")]
    pub auto_apply_additive_drift: bool,
}

impl ResilienceConfig {
    /// Project this policy's backoff knobs onto a [`RetryConfig`] so the run
    /// loop can reuse the shared [`crate::retry::compute_backoff`] helper —
    /// one backoff implementation across adapters and the run loop.
    #[must_use]
    pub fn backoff_config(&self) -> RetryConfig {
        RetryConfig {
            max_retries: self.transient_max_retries,
            initial_backoff_ms: self.initial_backoff_ms,
            max_backoff_ms: self.max_backoff_ms,
            backoff_multiplier: self.backoff_multiplier,
            jitter: self.jitter,
            circuit_breaker_threshold: self.circuit_breaker_threshold,
            circuit_breaker_recovery_timeout_secs: None,
            max_retries_per_run: self.max_retries_per_run,
        }
    }
}

impl Default for ResilienceConfig {
    fn default() -> Self {
        Self {
            enabled: default_resilience_enabled(),
            transient_max_retries: default_transient_max_retries(),
            initial_backoff_ms: default_resilience_initial_backoff_ms(),
            max_backoff_ms: default_resilience_max_backoff_ms(),
            backoff_multiplier: default_backoff_multiplier(),
            jitter: default_jitter(),
            circuit_breaker_threshold: default_resilience_breaker_threshold(),
            max_retries_per_run: default_resilience_max_retries_per_run(),
            contain_failures: default_contain_failures(),
            auto_apply_additive_drift: default_auto_apply_additive_drift(),
        }
    }
}

fn default_resilience_enabled() -> bool {
    true
}
fn default_transient_max_retries() -> u32 {
    2
}
fn default_resilience_initial_backoff_ms() -> u64 {
    500
}
fn default_resilience_max_backoff_ms() -> u64 {
    30_000
}
fn default_resilience_breaker_threshold() -> u32 {
    3
}
fn default_resilience_max_retries_per_run() -> Option<u32> {
    Some(8)
}
fn default_contain_failures() -> bool {
    false
}
fn default_auto_apply_additive_drift() -> bool {
    false
}

/// Schema pattern configuration from TOML, converted to [`SchemaPattern`] at runtime.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SchemaPatternConfig {
    pub prefix: String,
    pub separator: String,
    pub components: Vec<String>,
}

impl SchemaPatternConfig {
    /// Converts the config representation into a usable SchemaPattern.
    pub fn to_schema_pattern(&self) -> Result<SchemaPattern, crate::schema::SchemaError> {
        let components = SchemaPattern::parse_components(&self.components)?;
        Ok(SchemaPattern {
            prefix: self.prefix.clone(),
            separator: self.separator.clone(),
            components,
        })
    }
}

/// A metadata column added during replication (e.g., `_loaded_by`).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MetadataColumnConfig {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: String,
    pub value: String,
}

/// Data quality checks configuration (row count, column match, freshness, null rate, custom).
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ChecksConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub row_count: AggregateCheckToggle,
    #[serde(default)]
    pub column_match: AggregateCheckToggle,
    #[serde(default)]
    pub freshness: Option<FreshnessConfig>,
    #[serde(default)]
    pub null_rate: Option<NullRateConfig>,
    #[serde(default)]
    pub custom: Vec<CustomCheckConfig>,
    /// Cross-source overlap check — flags the same business key appearing in
    /// more than one sibling source feeding a shared consolidation target.
    /// Disabled when unset. See [`CrossSourceOverlapConfig`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cross_source_overlap: Option<CrossSourceOverlapConfig>,
    /// Row-level assertions (`not_null`, `unique`, `accepted_values`,
    /// `relationships`, `expression`, `row_count_range`) executed against
    /// specific tables in the quality pipeline. Each entry targets a single
    /// table by name and reuses the `TestDecl` surface from declarative
    /// model tests.
    #[serde(default)]
    pub assertions: Vec<QualityAssertion>,
    /// Row quarantine — split/tag/drop rows that violate error-severity
    /// row-level assertions. Disabled when unset. See [`QuarantineConfig`].
    #[serde(default)]
    pub quarantine: Option<QuarantineConfig>,
    /// Row count anomaly detection threshold (percentage deviation from baseline).
    /// Default: 50.0 (50% deviation triggers anomaly). Set to 0 to disable.
    #[serde(default = "default_anomaly_threshold_pct")]
    pub anomaly_threshold_pct: f64,
    /// When `true` (default), the quality run exits non-zero if any
    /// error-severity check fails. Set `false` to force the pipeline to
    /// always succeed and leave failure handling to downstream consumers
    /// of the JSON output.
    #[serde(default = "default_fail_on_error")]
    pub fail_on_error: bool,
}

fn default_anomaly_threshold_pct() -> f64 {
    50.0
}

fn default_fail_on_error() -> bool {
    true
}

/// Replication `strategy` values the runner recognizes. Anything else parses
/// cleanly but silently falls back to `full_refresh` at run time, so the
/// `rocky validate` lint warns on unrecognized values (V035).
///
/// Keep this in lockstep with `build_replication_strategy_with_override` in
/// `rocky-cli` (`commands/run.rs`) — a drift test there pins the relationship.
pub const RECOGNIZED_REPLICATION_STRATEGIES: &[&str] = &[
    "incremental",
    "merge",
    "view",
    "materialized_view",
    "dynamic_table",
    "full_refresh",
];

impl ChecksConfig {
    /// The check kinds a user has *explicitly opted into* — an `Option` is
    /// `Some` or a `Vec` is non-empty. Toggle-style checks (`row_count`,
    /// `column_match`) and the always-defaulted `anomaly_threshold_pct` are
    /// deliberately excluded so the inert-config lint never fires on a default.
    pub fn configured_explicit_kinds(&self) -> Vec<crate::checks::CheckKind> {
        use crate::checks::CheckKind;
        let mut kinds = Vec::new();
        if self.freshness.is_some() {
            kinds.push(CheckKind::Freshness);
        }
        if self.null_rate.is_some() {
            kinds.push(CheckKind::NullRate);
        }
        if !self.custom.is_empty() {
            kinds.push(CheckKind::Custom);
        }
        if self.cross_source_overlap.is_some() {
            kinds.push(CheckKind::CrossSourceOverlap);
        }
        if !self.assertions.is_empty() {
            kinds.push(CheckKind::Assertions);
        }
        kinds
    }
}

/// Per-check-kind toggle that accepts either a plain boolean (legacy form)
/// or a struct with `enabled` + `severity`.
///
/// ```toml
/// # Legacy — still supported
/// row_count = true
///
/// # New — per-check severity
/// [pipeline.x.checks.row_count]
/// enabled  = true
/// severity = "warning"
/// ```
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum AggregateCheckToggle {
    /// Legacy boolean toggle. Severity defaults to `error`.
    Bool(bool),
    /// Explicit struct form with per-check severity.
    Detailed {
        #[serde(default)]
        enabled: bool,
        #[serde(default)]
        severity: crate::tests::TestSeverity,
    },
}

impl Default for AggregateCheckToggle {
    fn default() -> Self {
        AggregateCheckToggle::Bool(false)
    }
}

impl AggregateCheckToggle {
    /// Whether this check is enabled.
    pub fn enabled(&self) -> bool {
        match self {
            AggregateCheckToggle::Bool(b) => *b,
            AggregateCheckToggle::Detailed { enabled, .. } => *enabled,
        }
    }

    /// Severity reported when this check fails. Defaults to `error`.
    pub fn severity(&self) -> crate::tests::TestSeverity {
        match self {
            AggregateCheckToggle::Bool(_) => crate::tests::TestSeverity::Error,
            AggregateCheckToggle::Detailed { severity, .. } => *severity,
        }
    }
}

/// A single row-level assertion attached to a quality pipeline, scoped to
/// one table in the pipeline's `tables` list.
///
/// ```toml
/// [[pipeline.nightly_dq.checks.assertions]]
/// name     = "orders_customer_id_not_null"  # optional
/// table    = "orders"
/// type     = "not_null"
/// column   = "customer_id"
/// severity = "error"
/// ```
///
/// The `type`-specific fields (and `column`, `severity`) are flattened
/// from `TestDecl` — the same surface used by declarative model tests.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct QualityAssertion {
    /// Table name this assertion applies to. Must match a table discovered
    /// from one of the pipeline's `[[tables]]` entries (by unqualified
    /// table name).
    pub table: String,
    /// Optional identifier used as the `CheckResult.name` in the JSON
    /// output. When unset, a synthesized `"{kind}:{column}"` name is
    /// used — which can collide if multiple assertions share the same
    /// table, kind, and column. Set `name` explicitly to disambiguate.
    #[serde(default)]
    pub name: Option<String>,
    /// The declarative test (flattened: `type`, `column`, severity, and
    /// type-specific fields like `values` / `to_table`).
    #[serde(flatten)]
    pub test: crate::tests::TestDecl,
}

impl QualityAssertion {
    /// The `CheckResult.name` this assertion emits — the explicit `name` if
    /// set, else a synthesized `"{kind}:{column}"`. Shared by the runners (via
    /// `run_table_assertions`) and the `rocky discover` check-name projection
    /// so the declared name byte-matches the emitted one.
    pub fn resolved_name(&self) -> String {
        self.name.clone().unwrap_or_else(|| {
            format!(
                "{}:{}",
                crate::tests::test_type_kind(&self.test.test_type),
                self.test.column.as_deref().unwrap_or("-")
            )
        })
    }
}

/// How to handle rows that fail error-severity row-level assertions.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum QuarantineMode {
    /// Write a `<table>__valid` table with passing rows and a
    /// `<table>__quarantine` table with failing rows (plus per-assertion
    /// `_error_<name>` label columns). Original table untouched. Default.
    #[default]
    Split,
    /// Replace `<table>` in place with a copy that has per-assertion
    /// `_error_<name>` label columns set for failing rows. Rewrites the
    /// source — use with care; not recommended when the source is a raw
    /// replication target.
    Tag,
    /// Write only the `<table>__valid` half. Failing rows are discarded.
    /// Irreversible at run-time.
    Drop,
}

/// Row quarantine configuration. When enabled, error-severity row-level
/// assertions (`not_null`, `accepted_values`, `expression`) on a given
/// table are compiled into a single boolean row predicate. Rows matching
/// the predicate go to the `__valid` table; rows that don't go to the
/// `__quarantine` table (or are dropped / tagged, per `mode`).
///
/// Aggregate / set-based assertions (`unique`, `relationships`,
/// `row_count_range`) are **not** lowered — they stay observational and
/// produce `CheckResult` entries as before.
///
/// ```toml
/// [pipeline.nightly_dq.checks.quarantine]
/// enabled = true
/// mode    = "split"          # "split" (default) | "tag" | "drop"
/// # suffix_valid       = "__valid"
/// # suffix_quarantine  = "__quarantine"
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct QuarantineConfig {
    /// Enable quarantine. Default: `false`.
    #[serde(default)]
    pub enabled: bool,
    /// How to split rows — see [`QuarantineMode`].
    #[serde(default)]
    pub mode: QuarantineMode,
    /// Table-name suffix for the passing-rows table. Default `"__valid"`.
    #[serde(default = "default_suffix_valid")]
    pub suffix_valid: String,
    /// Table-name suffix for the failing-rows table. Default `"__quarantine"`.
    #[serde(default = "default_suffix_quarantine")]
    pub suffix_quarantine: String,
}

impl Default for QuarantineConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            mode: QuarantineMode::default(),
            suffix_valid: default_suffix_valid(),
            suffix_quarantine: default_suffix_quarantine(),
        }
    }
}

fn default_suffix_valid() -> String {
    "__valid".to_string()
}

fn default_suffix_quarantine() -> String {
    "__quarantine".to_string()
}

/// Project-level freshness defaults.
///
/// Top-level `[freshness]` block on `rocky.toml`. Provides defaults
/// inherited by per-model
/// [`crate::models::ModelFreshnessConfig`] declarations that omit one
/// or more fields. Independent of the
/// [`ChecksConfig::freshness`](FreshnessConfig) check (which lives
/// under `[checks.freshness]` and feeds the data-quality test pipeline).
///
/// All fields are optional. A project-level `[freshness]` with no
/// `expected_lag_seconds` is treated as "no project default" for the
/// W005 soft-warn — the suppression still requires a concrete TTL.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProjectFreshnessConfig {
    /// Default maximum lag in seconds before models are considered
    /// stale. When set, every model without its own `freshness` block
    /// inherits this value (plus the other fields). When `None`, no
    /// project-level default applies — per-model declarations are the
    /// only source of freshness metadata.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_lag_seconds: Option<u64>,
    /// Default timestamp column used to evaluate freshness at runtime.
    /// Inherited by per-model freshness blocks that don't specify their
    /// own `time_column`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_column: Option<String>,
    /// Default severity reported when the freshness check trips.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub severity: Option<crate::tests::TestSeverity>,
}

impl ProjectFreshnessConfig {
    /// True when the project carries an actionable freshness default
    /// (an `expected_lag_seconds` value). The W005 compile-time soft
    /// warn treats a model as covered if either the model itself or
    /// the project supplies a TTL.
    pub fn has_default(&self) -> bool {
        self.expected_lag_seconds.is_some()
    }
}

/// Freshness check configuration with optional per-schema overrides.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FreshnessConfig {
    pub threshold_seconds: u64,
    /// Per-schema freshness overrides. Key is a schema pattern (e.g., "raw__us_west__shopify"),
    /// value overrides threshold_seconds for matching schemas.
    #[serde(default)]
    pub overrides: std::collections::HashMap<String, u64>,
    /// Severity reported when freshness lag exceeds the threshold.
    #[serde(default)]
    pub severity: crate::tests::TestSeverity,
}

/// Null rate check configuration: columns to check, threshold, and sample size.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NullRateConfig {
    pub columns: Vec<String>,
    pub threshold: f64,
    #[serde(default = "default_sample_percent")]
    pub sample_percent: u32,
    /// Severity reported when a column's null rate exceeds the threshold.
    #[serde(default)]
    pub severity: crate::tests::TestSeverity,
}

fn default_sample_percent() -> u32 {
    10
}

/// A user-defined SQL check with a name, query template, and pass/fail threshold.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CustomCheckConfig {
    pub name: String,
    pub sql: String,
    #[serde(default)]
    pub threshold: u64,
    /// Severity reported when this check fails.
    #[serde(default)]
    pub severity: crate::tests::TestSeverity,
}

/// Cross-source overlap check: flags the same business key appearing in more
/// than one *sibling* source feeding a shared consolidation target (the
/// "same account onboarded twice under two paths" case). Siblings are grouped
/// by the runner; this config carries only the key + thresholds.
///
/// `keys` (a column tuple) and `key_expr` (a derived SQL expression) are
/// mutually exclusive — exactly one must be set, mirroring `unique` /
/// `unique_expr`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CrossSourceOverlapConfig {
    /// Business-key columns whose shared value across sibling tables signals a
    /// duplicate. Mutually exclusive with `key_expr`.
    #[serde(default)]
    pub keys: Vec<String>,
    /// Derived business-key expression (e.g. `md5(a || '-' || b)`), for sources
    /// without a single natural key. Mutually exclusive with `keys`. Passed
    /// through verbatim (trusted config, like `unique_expr`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_expr: Option<String>,
    /// Severity reported when the overlap-key count exceeds `max_overlap_rows`.
    #[serde(default)]
    pub severity: crate::tests::TestSeverity,
    /// Overlap-key count above which the check fails. Default 0 — any overlap
    /// fails.
    #[serde(default)]
    pub max_overlap_rows: u64,
    /// Maximum overlapping keys attached to the result for triage.
    #[serde(default = "default_overlap_sample")]
    pub sample: usize,
}

fn default_overlap_sample() -> usize {
    20
}

impl CrossSourceOverlapConfig {
    /// Resolve the configured key into a list of SQL key-expressions: the
    /// `keys` column list, or a single-element list holding `key_expr`.
    ///
    /// Enforces the mutual-exclusion invariant: exactly one of `keys` /
    /// `key_expr` must be set (both-set or neither-set is an error). Column
    /// names in `keys` are validated as SQL identifiers; `key_expr` is trusted
    /// config and passed through verbatim.
    pub fn resolved_key_exprs(&self) -> Result<Vec<String>, String> {
        let has_keys = !self.keys.is_empty();
        let has_expr = self
            .key_expr
            .as_deref()
            .is_some_and(|e| !e.trim().is_empty());
        match (has_keys, has_expr) {
            (true, true) => Err(
                "cross_source_overlap: set exactly one of `keys` or `key_expr`, not both".into(),
            ),
            (false, false) => {
                Err("cross_source_overlap: one of `keys` or `key_expr` is required".into())
            }
            (true, false) => {
                for c in &self.keys {
                    rocky_sql::validation::validate_identifier(c)
                        .map_err(|e| format!("cross_source_overlap key column '{c}': {e}"))?;
                }
                Ok(self.keys.clone())
            }
            (false, true) => Ok(vec![self.key_expr.clone().unwrap()]),
        }
    }
}

/// Governance settings: auto-creation of catalogs/schemas, tags, isolation, and grants.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GovernanceConfig {
    #[serde(default)]
    pub auto_create_catalogs: bool,
    #[serde(default)]
    pub auto_create_schemas: bool,
    /// Optional prefix prepended to auto-generated component tag keys
    /// (e.g., `"ge_"` turns `client` → `ge_client`). Does not affect
    /// keys in `[governance.tags]` — those are used verbatim.
    #[serde(default)]
    pub tag_prefix: Option<String>,
    /// Tags applied to every managed catalog and schema.
    #[serde(default)]
    pub tags: std::collections::BTreeMap<String, String>,
    /// Workspace isolation settings.
    #[serde(default)]
    pub isolation: Option<IsolationConfig>,
    /// Permissions granted on every managed catalog.
    #[serde(default)]
    pub grants: Vec<GrantConfig>,
    /// Permissions granted on every managed schema.
    #[serde(default)]
    pub schema_grants: Vec<GrantConfig>,
}

impl GovernanceConfig {
    /// Build the full tag map for a catalog, schema, or table.
    ///
    /// Component-derived keys are prefixed with `tag_prefix` (if set),
    /// then merged with the static `[governance.tags]` entries (used verbatim).
    pub fn build_tags(
        &self,
        components: &indexmap::IndexMap<String, serde_json::Value>,
    ) -> std::collections::BTreeMap<String, String> {
        let prefix = self.tag_prefix.as_deref().unwrap_or("");
        let mut tags: std::collections::BTreeMap<String, String> = components
            .iter()
            .filter_map(|(k, v)| v.as_str().map(|s| (format!("{prefix}{k}"), s.to_string())))
            .collect();
        tags.extend(self.tags.clone());
        tags
    }
}

/// Workspace binding access level.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum BindingType {
    #[default]
    ReadWrite,
    ReadOnly,
}

impl std::fmt::Display for BindingType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_api_str())
    }
}

impl BindingType {
    /// Returns the Databricks API string representation.
    pub fn as_api_str(&self) -> &'static str {
        match self {
            Self::ReadWrite => "BINDING_TYPE_READ_WRITE",
            Self::ReadOnly => "BINDING_TYPE_READ_ONLY",
        }
    }
}

/// A workspace binding with ID and access level.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceBindingConfig {
    pub id: u64,
    #[serde(default)]
    pub binding_type: BindingType,
}

/// Workspace isolation configuration (Databricks-specific).
///
/// ```toml
/// [governance.isolation]
/// enabled = true
///
/// [[governance.isolation.workspace_ids]]
/// id = 7474656540609532
/// binding_type = "READ_WRITE"
///
/// [[governance.isolation.workspace_ids]]
/// id = 7474647537929812
/// binding_type = "READ_ONLY"
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct IsolationConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub workspace_ids: Vec<WorkspaceBindingConfig>,
}

/// A permission grant to apply to catalogs or schemas.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GrantConfig {
    pub principal: String,
    pub permissions: Vec<String>,
}

/// A single entry in the top-level `[role.*]` block, declaring a
/// hierarchical role with optional inheritance and a list of
/// permissions.
///
/// ```toml
/// [role.reader]
/// permissions = ["SELECT", "USE CATALOG", "USE SCHEMA"]
///
/// [role.analytics_engineer]
/// inherits = ["reader"]
/// permissions = ["MODIFY"]
///
/// [role.admin]
/// inherits = ["analytics_engineer"]
/// permissions = ["MANAGE"]
/// ```
///
/// Resolution happens at reconcile time via
/// [`crate::role_graph::flatten_role_graph`], which walks the
/// `inherits` DAG and unions permissions from the role and every
/// transitive ancestor. Cycles and unknown parents are caught as
/// structured [`crate::role_graph::RoleGraphError`] values.
///
/// Permission strings must match the canonical uppercase spellings of
/// [`rocky_ir::Permission`] (`"SELECT"`, `"USE CATALOG"`, ...).
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RoleConfig {
    /// Immediate parent role names. Rocky walks these transitively at
    /// reconcile time; cycles are rejected. Defaults to `[]` when
    /// omitted.
    #[serde(default)]
    pub inherits: Vec<String>,
    /// Permissions this role grants. Rocky unions these with every
    /// ancestor's permissions before passing the flattened set to the
    /// governance adapter. Defaults to `[]` (permissionless grouping
    /// roles are legal — they exist only for inheritance).
    #[serde(default)]
    pub permissions: Vec<String>,
}

/// One entry in the top-level `[mask]` block. A scalar value (`pii =
/// "hash"`) binds a classification tag to a default masking strategy; a
/// nested table (`[mask.prod] pii = "none"`) overrides strategies for a
/// specific environment.
///
/// Serde deserializes the outer `[mask]` map as `BTreeMap<String,
/// MaskEntry>`; scalars are tried first, then the nested table shape.
/// Unknown strategy spellings (e.g., `"mask"`) hard-fail at config load
/// time — Rocky never silently accepts something it can't emit SQL for.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum MaskEntry {
    /// Default masking strategy for a classification tag.
    Strategy(rocky_ir::MaskStrategy),
    /// Per-env override map: `[mask.<env>]` section with
    /// `<classification> = "<strategy>"` pairs.
    EnvOverride(std::collections::BTreeMap<String, rocky_ir::MaskStrategy>),
}

/// Advisory settings for column classification.
///
/// ```toml
/// [classifications]
/// allow_unmasked = ["internal"]
/// ```
///
/// Any classification tag listed in `allow_unmasked` suppresses the W004
/// "tag has no masking strategy" compiler warning. This is the escape
/// hatch for teams that want to tag columns for discovery/lineage without
/// requiring a matching `[mask]` strategy for every tag.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ClassificationsConfig {
    /// Classification tags that are allowed to appear in a model's
    /// `[classification]` block without a corresponding `[mask]` strategy.
    #[serde(default)]
    pub allow_unmasked: Vec<String>,
}

/// Per-run governance override, merged additively on top of rocky.toml defaults.
///
/// Passed via `--governance-override` CLI flag as JSON string or `@file.json`.
/// Used for per-client workspace bindings and grants from external sources.
///
/// # `workspace_ids` semantics
///
/// `workspace_ids` is `Option<Vec<_>>` rather than `Vec<_>` so the engine
/// can distinguish "key absent" from "empty list":
///
/// | Payload | Meaning |
/// |---|---|
/// | key absent (`None`) | Skip workspace-binding reconciliation for this run |
/// | `Some(vec![])` without `allow_empty_workspace_ids` | Error — refuses to revoke every binding |
/// | `Some(vec![])` with `allow_empty_workspace_ids = true` | Explicit full revoke |
/// | `Some(non-empty)` | Reconcile bindings to exactly this set |
///
/// The empty-list guard protects callers from a footgun where a
/// misconfigured permission store or off-by-one serializer silently
/// strips every workspace binding from the target catalog.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct GovernanceOverride {
    /// Desired workspace-binding set for this run.
    ///
    /// See the struct-level docs for the `None` vs `Some(empty)` vs
    /// `Some(non-empty)` behaviour. The `rocky run` reconciler rejects
    /// `Some(empty)` unless [`Self::allow_empty_workspace_ids`] is
    /// `true`.
    #[serde(default)]
    pub workspace_ids: Option<Vec<WorkspaceBindingConfig>>,
    /// Opt-in consent for a full workspace-binding revoke.
    ///
    /// Defaults to `false`. Set to `true` when `workspace_ids` is
    /// intentionally `[]` — e.g. decommissioning a catalog. Kept on the
    /// override (not as a CLI flag) so the intent is auditable per-run
    /// via the `RunRecord` audit trail.
    #[serde(default)]
    pub allow_empty_workspace_ids: bool,
    #[serde(default)]
    pub grants: Vec<GrantConfig>,
    #[serde(default)]
    pub schema_grants: Vec<GrantConfig>,
}

/// Reasons why a [`GovernanceOverride`] fails the pre-run safety check.
///
/// Currently only fires for the empty-`workspace_ids` footgun; new
/// variants are expected when FR-009's sibling checks ship (e.g. an
/// empty `grants` list on a catalog that has existing grants).
#[derive(Debug, thiserror::Error)]
pub enum GovernanceOverrideError {
    /// `workspace_ids = []` without `allow_empty_workspace_ids = true`.
    ///
    /// `catalogs` is the list of target catalogs the current run would
    /// have reconciled; surfaced in the error message so operators
    /// know which catalog the (accidental) full revoke would have hit.
    #[error(
        "governance_override.workspace_ids is empty — rocky run would revoke every workspace \
         binding on {catalogs}. Set `allow_empty_workspace_ids = true` in the override to \
         intentionally revoke all bindings, or omit the `workspace_ids` key entirely to skip \
         binding reconciliation for this run."
    )]
    EmptyWorkspaceIds {
        /// Comma-separated target catalogs, surfaced for operator
        /// diagnostics. Empty string when the caller can't enumerate
        /// catalogs up front (validators that run before config
        /// resolution).
        catalogs: String,
    },
}

impl GovernanceOverride {
    /// Fail fast when the override encodes a silent full-revoke.
    ///
    /// Called from `rocky run` before the reconciler fires. Returns
    /// `Ok(())` for every safe shape (including `workspace_ids` absent
    /// and `workspace_ids` non-empty); only `Some(vec![])` without the
    /// opt-in flag errors out.
    ///
    /// # Errors
    ///
    /// Returns [`GovernanceOverrideError::EmptyWorkspaceIds`] when
    /// `workspace_ids = Some(empty)` and
    /// `allow_empty_workspace_ids = false`.
    pub fn validate_workspace_ids(&self, catalogs: &str) -> Result<(), GovernanceOverrideError> {
        let Some(ws_ids) = self.workspace_ids.as_ref() else {
            return Ok(());
        };
        if ws_ids.is_empty() && !self.allow_empty_workspace_ids {
            return Err(GovernanceOverrideError::EmptyWorkspaceIds {
                catalogs: catalogs.to_string(),
            });
        }
        Ok(())
    }
}

/// Schema evolution configuration.
///
/// Controls how Rocky handles columns that disappear from the source but
/// still exist in the target table. Instead of immediately dropping them,
/// Rocky can keep them for a grace period (filling with NULL) so downstream
/// consumers have time to adapt.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SchemaEvolutionConfig {
    /// Number of days to keep a dropped column before removing it from the
    /// target table. During this window the column is filled with NULL for
    /// new rows and a warning is emitted on every run.
    /// Default: 7.
    #[serde(default = "default_grace_period_days")]
    pub grace_period_days: u32,
}

fn default_grace_period_days() -> u32 {
    7
}

impl Default for SchemaEvolutionConfig {
    fn default() -> Self {
        Self {
            grace_period_days: default_grace_period_days(),
        }
    }
}

/// Cost estimation configuration.
///
/// Controls pricing assumptions used by [`crate::optimize::recommend_strategy`]
/// when analyzing materialization costs and generating recommendations.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CostSection {
    /// Cost per GB of storage per month (default: $0.023).
    #[serde(default = "default_storage_cost")]
    pub storage_cost_per_gb_month: f64,
    /// Cost per DBU (default: $0.40).
    #[serde(default = "default_compute_cost_per_dbu")]
    pub compute_cost_per_dbu: f64,
    /// Warehouse size for cost estimation (e.g., "Small", "Medium", "Large").
    #[serde(default = "default_warehouse_size")]
    pub warehouse_size: String,
    /// Minimum runs before making cost recommendations.
    #[serde(default = "default_min_history_runs")]
    pub min_history_runs: usize,
}

fn default_storage_cost() -> f64 {
    0.023
}
fn default_compute_cost_per_dbu() -> f64 {
    0.40
}
fn default_warehouse_size() -> String {
    "Medium".to_string()
}
fn default_min_history_runs() -> usize {
    5
}

impl Default for CostSection {
    fn default() -> Self {
        CostSection {
            storage_cost_per_gb_month: default_storage_cost(),
            compute_cost_per_dbu: default_compute_cost_per_dbu(),
            warehouse_size: default_warehouse_size(),
            min_history_runs: default_min_history_runs(),
        }
    }
}

/// What to do when a [`BudgetConfig`] limit is exceeded by an actual run.
///
/// `Warn` always fires the `budget_breach` event; `Error` additionally
/// causes `rocky run` to exit with a non-zero status so orchestrators
/// can gate downstream work on the breach.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "snake_case")]
pub enum BudgetBreachAction {
    #[default]
    Warn,
    Error,
}

/// Declarative run-level budget for cost, duration, and data volume.
/// All limits are optional; when unset the dimension is not enforced.
///
/// A breach is detected at end of run by comparing [`BudgetConfig`]
/// against the observed [`crate::cost::compute_observed_cost_usd`]
/// total, the run wall clock, and the aggregate `bytes_scanned` summed
/// across every materialization. Limits are independent and composed
/// with all-OR — any single dimension breach trips the
/// `budget_breach` event. Per-model budgets are deferred to a later
/// wave; the first iteration enforces run-level totals only.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BudgetConfig {
    /// Maximum allowed run cost in USD. When set and exceeded, emits
    /// `budget_breach` on the event bus; when paired with
    /// `on_breach = "error"`, also fails the run.
    #[serde(default)]
    pub max_usd: Option<f64>,

    /// Maximum allowed run wall-clock duration in milliseconds.
    #[serde(default)]
    pub max_duration_ms: Option<u64>,

    /// Maximum allowed total bytes scanned across every materialization
    /// in the run. Useful for CI gates that want to fail when a
    /// regression bloats scan volume even if the dollar cost stays
    /// within `max_usd` (e.g. a BigQuery query that suddenly stops
    /// pruning partitions).
    ///
    /// Aggregated from per-model `bytes_scanned` figures the adapter
    /// reports — today that's BigQuery's `totalBytesBilled`; Databricks
    /// / Snowflake / DuckDB still inherit `None`, in which case the
    /// dimension is skipped rather than treated as zero (matching
    /// `max_usd`).
    #[serde(default)]
    pub max_bytes_scanned: Option<u64>,

    /// What to do when a limit is breached. Defaults to `warn` — fire the
    /// event, keep the run successful. Set to `error` to fail the run.
    #[serde(default)]
    pub on_breach: BudgetBreachAction,
}

impl BudgetConfig {
    /// True when no limit is configured — skip budget checking entirely.
    #[must_use]
    pub fn is_unset(&self) -> bool {
        self.max_usd.is_none() && self.max_duration_ms.is_none() && self.max_bytes_scanned.is_none()
    }

    /// Compare observed totals against each configured limit.
    ///
    /// Returns one [`BudgetBreach`] per breached dimension; an empty
    /// vector when within budget or when no limits are configured.
    /// `observed_cost_usd` is `None` when the adapters didn't produce
    /// enough data to compute a cost (e.g. BigQuery with no bytes
    /// reported); in that case the cost dimension is skipped rather
    /// than treated as zero. `observed_bytes_scanned` follows the same
    /// rule — `None` when no materialization reported a byte count.
    #[must_use]
    pub fn check_breaches(
        &self,
        observed_cost_usd: Option<f64>,
        observed_duration_ms: u64,
        observed_bytes_scanned: Option<u64>,
    ) -> Vec<BudgetBreach> {
        let mut breaches = Vec::new();

        if let (Some(limit), Some(actual)) = (self.max_usd, observed_cost_usd)
            && actual > limit
        {
            breaches.push(BudgetBreach {
                limit_type: BudgetLimitType::MaxUsd,
                limit,
                actual,
            });
        }

        if let Some(limit) = self.max_duration_ms
            && observed_duration_ms > limit
        {
            breaches.push(BudgetBreach {
                limit_type: BudgetLimitType::MaxDurationMs,
                limit: limit as f64,
                actual: observed_duration_ms as f64,
            });
        }

        if let (Some(limit), Some(actual)) = (self.max_bytes_scanned, observed_bytes_scanned)
            && actual > limit
        {
            breaches.push(BudgetBreach {
                limit_type: BudgetLimitType::MaxBytesScanned,
                limit: limit as f64,
                actual: actual as f64,
            });
        }

        breaches
    }
}

/// Which budget limit was breached. Surfaced on [`BudgetBreach`] so
/// subscribers can filter on a stable tag rather than string-match.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum BudgetLimitType {
    MaxUsd,
    MaxDurationMs,
    MaxBytesScanned,
}

impl BudgetLimitType {
    /// String tag used in `budget_breach` [`crate::hooks`] / event
    /// payload `limit_type` fields.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            BudgetLimitType::MaxUsd => "max_usd",
            BudgetLimitType::MaxDurationMs => "max_duration_ms",
            BudgetLimitType::MaxBytesScanned => "max_bytes_scanned",
        }
    }
}

/// One breached budget dimension from [`BudgetConfig::check_breaches`].
///
/// `limit` and `actual` are stored as `f64` so a single struct carries
/// both the USD dimension (naturally `f64`) and the duration dimension
/// (originally `u64` milliseconds, widened without loss up to 2^53).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct BudgetBreach {
    pub limit_type: BudgetLimitType,
    pub limit: f64,
    pub actual: f64,
}

/// Per-model `[budget]` overrides declared in a model sidecar.
///
/// Same field set as [`BudgetConfig`], but every field — including
/// `on_breach` — is `Option`. Absence vs. explicit value matters so a
/// partial sidecar block (e.g. only `max_usd`) inherits the missing
/// fields from the project-level `[budget]`. Resolved against the
/// project-level config via [`ModelBudgetConfig::resolve`] before any
/// breach check runs.
///
/// Precedence is "per-model is the local authority": when a field is
/// explicitly set on the sidecar it wins, even when the project-level
/// has its own value. A per-model `on_breach = "warn"` overrides a
/// project-level `on_breach = "error"` for that one model.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ModelBudgetConfig {
    /// Maximum allowed branch cost in USD for this single model. Inherits
    /// the project-level value when unset.
    #[serde(default)]
    pub max_usd: Option<f64>,

    /// Maximum allowed branch wall-clock duration in milliseconds for this
    /// single model. Inherits the project-level value when unset.
    #[serde(default)]
    pub max_duration_ms: Option<u64>,

    /// Maximum allowed branch bytes scanned for this single model.
    /// Inherits the project-level value when unset. Today this dimension
    /// only fires for BigQuery (the only adapter that populates
    /// `bytes_scanned`); other adapters skip it rather than treat it as
    /// zero. Mirrors [`BudgetConfig::max_bytes_scanned`] semantics.
    #[serde(default)]
    pub max_bytes_scanned: Option<u64>,

    /// Action to take when a per-model limit is breached. When unset,
    /// inherits the project-level [`BudgetConfig::on_breach`]. When set
    /// explicitly it takes precedence over the project-level value for
    /// this one model — a sidecar `on_breach = "warn"` keeps a local
    /// breach advisory even when the project enforces with
    /// `on_breach = "error"`.
    #[serde(default)]
    pub on_breach: Option<BudgetBreachAction>,
}

impl ModelBudgetConfig {
    /// True when no field is set — caller should fall back to the
    /// project-level config rather than building a degenerate
    /// [`BudgetConfig`].
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.max_usd.is_none()
            && self.max_duration_ms.is_none()
            && self.max_bytes_scanned.is_none()
            && self.on_breach.is_none()
    }

    /// Compose this per-model override against a project-level
    /// [`BudgetConfig`] into a fully-resolved [`BudgetConfig`].
    ///
    /// Field-level inheritance: each field on the returned config is
    /// the per-model value when explicitly set, otherwise the
    /// project-level value. The result feeds directly into
    /// [`BudgetConfig::check_breaches`].
    #[must_use]
    pub fn resolve(&self, project: &BudgetConfig) -> BudgetConfig {
        BudgetConfig {
            max_usd: self.max_usd.or(project.max_usd),
            max_duration_ms: self.max_duration_ms.or(project.max_duration_ms),
            max_bytes_scanned: self.max_bytes_scanned.or(project.max_bytes_scanned),
            on_breach: self.on_breach.unwrap_or(project.on_breach),
        }
    }
}

/// Valkey/Redis cache configuration for distributed caching.
///
/// Note: this type has existed since the early cache experiments and is
/// currently not wired into [`RockyConfig`] — see `CHANGELOG.md` for the
/// history. Kept `pub` because downstream tooling may still reference it,
/// but the top-level `[cache]` config surface is now [`CacheConfig`]
/// (schemas section).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValkeyCacheConfig {
    // SECURITY: may embed credentials (`redis://:password@host`). Never log
    // this value; redact it if it ever reaches an error/trace message. (Not
    // wrapped in RedactedString because the cache client consumes it as a
    // plain connection URL — threading RedactedString through that
    // construction is invasive; this comment is the guard instead.)
    pub valkey_url: String,
}

/// Top-level `[cache]` configuration.
///
/// Holds every cache surface that lives at the project level (i.e. has a
/// `rocky.toml` knob). Today that's only the schema cache, but the shape
/// is deliberately extensible: if a future `[cache.query]` or
/// `[cache.plan]` surfaces, it lands as a new field on this struct with a
/// `#[serde(default)]` attribute and its own `*Config` type — no breaking
/// change to existing `rocky.toml` files.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct CacheConfig {
    /// Schema cache. Stores `DESCRIBE TABLE` results in `state.redb` so
    /// leaf models typecheck against real warehouse types without a live
    /// round-trip on every compile.
    pub schemas: SchemaCacheConfig,
}

/// `[cache.schemas]` — schema cache configuration.
///
/// Controls the DESCRIBE-result cache. Defaults are chosen so the
/// feature is useful out of the box: the cache is on, entries live for
/// 24 hours, and nothing replicates off-machine until the user opts in.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct SchemaCacheConfig {
    /// Enable schema cache reads + writes. Defaults to `true`. Set to
    /// `false` for strict CI where every typecheck should resolve against
    /// the current warehouse and never fall back to a cached entry.
    pub enabled: bool,
    /// TTL for cache entries in seconds. Defaults to 86400 (24 hours).
    /// Lower it for high-DDL-churn teams; raise it for projects whose
    /// sources change on a weekly or slower cadence.
    pub ttl_seconds: u64,
    /// Replicate the schema cache via `state_sync` to the remote backend.
    /// Defaults to `false`: a dev on a fresh clone should not inherit
    /// another machine's stale type stamps. Opt in to `true` for teams
    /// that want cross-machine cache warm-up via a shared state backend.
    pub replicate: bool,
}

impl Default for SchemaCacheConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            ttl_seconds: 86_400,
            replicate: false,
        }
    }
}

impl SchemaCacheConfig {
    /// Convenience: TTL as a `chrono::Duration` for the read path.
    pub fn ttl(&self) -> chrono::Duration {
        chrono::Duration::seconds(self.ttl_seconds as i64)
    }

    /// Apply an optional `--cache-ttl <seconds>` CLI override.
    ///
    /// Precedence: CLI flag > env var (future) > `[cache.schemas]
    /// ttl_seconds` > default (86400s).
    ///
    /// `override_seconds = Some(0)` is deliberately meaningful: every
    /// cached entry reports as expired on read, so the typecheck path
    /// degrades to an empty map. To fully disable the cache set
    /// `[cache.schemas] enabled = false` — `--cache-ttl 0` is the
    /// "everything is stale" knob, not the "off" knob.
    pub fn with_ttl_override(self, override_seconds: Option<u64>) -> Self {
        match override_seconds {
            Some(secs) => Self {
                ttl_seconds: secs,
                ..self
            },
            None => self,
        }
    }
}

/// Matches `${VAR}` and `${VAR:-default}` env-var substitution placeholders.
static ENV_VAR_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\$\{([^}]+)\}").expect("valid regex"));

/// Record of a single environment variable substitution performed during
/// config loading. Returned by [`substitute_env_vars_with_report`] so
/// post-substitution diagnostics (§P2.9) can cite the env var that supplied
/// a bogus value instead of leaving the operator to guess.
#[derive(Debug, Clone)]
pub struct EnvVarSubstitution {
    /// Name of the env var as it appeared in `${NAME}` / `${NAME:-default}`.
    pub name: String,
    /// Value that was actually substituted (either `$NAME` or its default).
    pub value: String,
}

/// Substitutes `${VAR_NAME}` and `${VAR_NAME:-default}` patterns with environment variable values.
///
/// - `${VAR}` — required, errors if not set
/// - `${VAR:-fallback}` — uses `fallback` if VAR is not set or empty
pub fn substitute_env_vars(input: &str) -> Result<String, ConfigError> {
    substitute_env_vars_with_report(input).map(|(text, _)| text)
}

/// Like [`substitute_env_vars`], but also returns a report of every
/// substitution performed. Used by the config loader to attach env-var
/// origin context to post-substitution parse errors (§P2.9).
pub fn substitute_env_vars_with_report(
    input: &str,
) -> Result<(String, Vec<EnvVarSubstitution>), ConfigError> {
    let re = &*ENV_VAR_RE;
    let mut result = String::with_capacity(input.len());
    let mut last_end = 0;
    // Track the first missing env var and its byte span for diagnostics.
    let mut first_missing: Option<(String, std::ops::Range<usize>)> = None;
    let mut substitutions: Vec<EnvVarSubstitution> = Vec::new();

    for cap in re.captures_iter(input) {
        let full_match = cap.get(0).expect("capture group 0 always exists");
        let match_start = full_match.start();
        let match_end = full_match.end();

        // Check if this match is inside a TOML comment line — find the line
        // containing this match and skip if it starts with '#'.
        let line_start = input[..match_start].rfind('\n').map_or(0, |p| p + 1);
        if input[line_start..].trim_start().starts_with('#') {
            // Inside a comment — copy verbatim and skip substitution.
            result.push_str(&input[last_end..match_end]);
            last_end = match_end;
            continue;
        }

        // Copy everything between the last match and this one.
        result.push_str(&input[last_end..match_start]);

        let expr = &cap[1];
        if let Some(sep_pos) = expr.find(":-") {
            let var_name = &expr[..sep_pos];
            let default_value = &expr[sep_pos + 2..];
            let value = std::env::var(var_name)
                .ok()
                .filter(|v| !v.is_empty())
                .unwrap_or_else(|| default_value.to_string());
            result.push_str(&value);
            substitutions.push(EnvVarSubstitution {
                name: var_name.to_string(),
                value,
            });
        } else {
            match std::env::var(expr) {
                Ok(value) => {
                    result.push_str(&value);
                    substitutions.push(EnvVarSubstitution {
                        name: expr.to_string(),
                        value,
                    });
                }
                Err(_) => {
                    if first_missing.is_none() {
                        first_missing = Some((expr.to_string(), match_start..match_end));
                    }
                    // Keep the placeholder verbatim so the rest of the string
                    // stays intact for context.
                    result.push_str(&input[match_start..match_end]);
                }
            }
        }
        last_end = match_end;
    }

    if let Some((name, span)) = first_missing {
        return Err(ConfigError::MissingEnvVar {
            name,
            span: Some(span),
        });
    }

    // Copy the tail of the input after the last match.
    result.push_str(&input[last_end..]);
    Ok((result, substitutions))
}

/// Formats a list of env-var substitutions into a one-line human-readable
/// hint appended to post-substitution parse errors. Values are truncated to
/// avoid dumping secrets / multi-KB strings into a log line; the operator
/// just needs enough context to find the misbehaving env var.
fn format_env_var_hint(substitutions: &[EnvVarSubstitution]) -> String {
    if substitutions.is_empty() {
        return String::new();
    }
    const MAX_VALUE_LEN: usize = 40;
    // De-dup by name, keeping first occurrence.
    let mut seen = std::collections::HashSet::new();
    let parts: Vec<String> = substitutions
        .iter()
        .filter(|sub| seen.insert(&sub.name))
        .map(|sub| {
            let truncated = if sub.value.len() > MAX_VALUE_LEN {
                // Truncate on a UTF-8 char boundary — a fixed byte slice panics
                // when byte MAX_VALUE_LEN lands inside a multibyte character.
                let end = (0..=MAX_VALUE_LEN)
                    .rev()
                    .find(|&i| sub.value.is_char_boundary(i))
                    .unwrap_or(0);
                format!("{}…", &sub.value[..end])
            } else {
                sub.value.clone()
            };
            format!("{}={:?}", sub.name, truncated)
        })
        .collect();
    format!(
        "\n  hint: config had these env var substitutions — check one is the culprit: {}",
        parts.join(", ")
    )
}

// ===========================================================================
// Config v2: Named adapters + named pipelines
// ===========================================================================

/// A single imported producer-project snapshot.
///
/// Declared as `[imports.<name>]` in `rocky.toml`. A producer project
/// publishes a serialized snapshot of its compiled project (via
/// `rocky publish-ir`); a consumer project vendors that snapshot file and
/// references it here so `rocky compile` can verify that the columns the
/// consumer reads still exist in the producer's output.
///
/// ```toml
/// [imports.orders]
/// path = "vendor/orders"        # directory holding the vendored snapshots
/// snapshot = "current.json"     # the producer's current published snapshot
/// baseline = "baseline.json"    # optional prior snapshot used for diffing
/// pin = "*"                     # optional recipe-hash pin ("*" = trust any)
/// ```
///
/// `pin` and `baseline` answer different questions and are complementary, not
/// redundant. `pin` is a whole-project drift tripwire: a concrete recipe hash
/// that, when set, makes `rocky compile` fail (E033) if the vendored snapshot
/// differs at all. `baseline` is the column-level "before" image: the only
/// input that lets the breaking-change diff emit the column codes
/// (E030/E031/E032/W030/W031) for changes the consumer actually reads. Leave
/// `pin` at `"*"` (or unset) to fail only on changes that touch your reads;
/// set a concrete pin to fail on any drift. Run `rocky imports update` after
/// reviewing a producer change to advance `baseline` to the current snapshot
/// (the explicit accept) — nothing advances it automatically.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ImportEntry {
    /// Directory (relative to `rocky.toml`) holding the vendored snapshot
    /// files.
    pub path: String,

    /// Filename of the producer's current published snapshot, relative to
    /// `path`.
    pub snapshot: String,

    /// Optional filename of a prior/pinned snapshot used as the diff
    /// baseline, relative to `path`. When set, `rocky compile` diffs
    /// `baseline` against `snapshot` to detect columns the producer
    /// dropped.
    #[serde(default)]
    pub baseline: Option<String>,

    /// Optional recipe-hash pin (hex). When set to a concrete hash, the
    /// snapshot's recipe hash must match or compilation fails. `"*"` (or
    /// absent) trusts whatever snapshot is vendored.
    #[serde(default)]
    pub pin: Option<String>,
}

/// Top-level Rocky configuration (v2 format).
///
/// Uses named adapters and named pipelines:
/// ```toml
/// [adapter.databricks_prod]
/// type = "databricks"
/// ...
///
/// [pipeline.raw_replication]
/// type = "replication"
/// ...
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RockyConfig {
    /// Global state persistence configuration.
    #[serde(default)]
    pub state: StateConfig,

    /// Named adapter configurations (keyed by adapter name).
    #[serde(default, rename = "adapter", alias = "adapters")]
    #[schemars(with = "AdaptersFieldSchema")]
    pub adapters: IndexMap<String, AdapterConfig>,

    /// Named pipeline configurations (keyed by pipeline name).
    #[serde(default, rename = "pipeline", alias = "pipelines")]
    #[schemars(with = "PipelinesFieldSchema")]
    pub pipelines: IndexMap<String, PipelineConfig>,

    /// Shell hooks configuration.
    #[serde(default, rename = "hook", alias = "hooks")]
    pub hooks: HooksConfig,

    /// Cost estimation configuration.
    #[serde(default)]
    pub cost: CostSection,

    /// Declarative run-level budget. See [`BudgetConfig`] for the
    /// semantics of each limit and the breach action.
    #[serde(default)]
    pub budget: BudgetConfig,

    /// Schema evolution configuration (grace-period column drops).
    #[serde(default)]
    pub schema_evolution: SchemaEvolutionConfig,

    /// Run-level retry budget shared across every adapter for this run.
    ///
    /// When set, `rocky run` builds a single
    /// [`crate::retry_budget::RetryBudget`] from
    /// [`RunRetryConfig::max_retries_per_run`] and passes it to every
    /// connector via `with_retry_budget(...)`. One bad table that burns
    /// through retries on adapter A then has less budget available for
    /// adapter B's retries — the protection §P2.7 added within a single
    /// adapter now extends across the whole run.
    ///
    /// Unset (the default) preserves per-adapter semantics: each adapter
    /// still honours its own `retry.max_retries_per_run` independently.
    /// That's the backward-compatible path and stays the right choice
    /// when adapters have wildly different rate limits.
    #[serde(default)]
    pub retry: Option<RunRetryConfig>,

    /// Dialect-portability lint configuration. Consumed by `rocky compile`
    /// to drive P001 (and, when wired, future) diagnostics. The CLI's
    /// `--target-dialect` flag, when set, takes precedence over
    /// [`PortabilityConfig::target_dialect`].
    #[serde(default)]
    pub portability: PortabilityConfig,

    /// Project-level cache configuration. Today this is just
    /// `[cache.schemas]` (schema cache for `DESCRIBE TABLE` results);
    /// future cache surfaces live as sibling fields under
    /// [`CacheConfig`].
    #[serde(default)]
    pub cache: CacheConfig,

    /// Workspace-default column-masking strategies plus optional per-env
    /// overrides. See [`MaskEntry`] for the TOML shape:
    ///
    /// ```toml
    /// [mask]
    /// pii = "hash"            # default strategy for "pii" classification
    /// confidential = "redact" # default strategy for "confidential"
    ///
    /// [mask.prod]
    /// pii = "none"            # prod override: do not mask pii
    /// confidential = "partial"
    /// ```
    ///
    /// Resolved per model at apply time via
    /// [`RockyConfig::resolve_mask_for_env`].
    #[serde(default)]
    pub mask: std::collections::BTreeMap<String, MaskEntry>,

    /// Advisory settings for column classification — currently just the
    /// `allow_unmasked` list that suppresses W004 warnings.
    #[serde(default)]
    pub classifications: ClassificationsConfig,

    /// Hierarchical role declarations reconciled against the warehouse's
    /// native role/group system.
    ///
    /// See [`RoleConfig`] for the TOML shape and
    /// [`crate::role_graph::flatten_role_graph`] for the inheritance
    /// resolution semantics (DAG walk with cycle detection).
    #[serde(default, rename = "role", alias = "roles")]
    pub roles: std::collections::BTreeMap<String, RoleConfig>,

    /// AI intent layer configuration. Currently scopes the per-request and
    /// cumulative-retry token budget for `rocky ai` / `ai-explain` /
    /// `ai-sync` / `ai-test`. See [`AiSection`].
    #[serde(default)]
    pub ai: AiSection,

    /// Branch-level configuration. Currently scopes the optional approval
    /// gate consumed by `rocky branch promote`. See [`BranchSection`].
    #[serde(default)]
    pub branch: BranchSection,

    /// Project-level freshness defaults inherited by per-model
    /// [`crate::models::ModelFreshnessConfig`] declarations that omit
    /// individual fields. See [`ProjectFreshnessConfig`] for the TOML
    /// shape:
    ///
    /// ```toml
    /// [freshness]
    /// expected_lag_seconds = 3600
    /// time_column = "updated_at"
    /// severity = "warning"
    /// ```
    ///
    /// Inheritance is field-by-field: a per-model `[freshness]` table
    /// always wins for the fields it sets; absent fields fall through to
    /// the project-level default. Models with no per-model `[freshness]`
    /// at all inherit the project default when it carries an
    /// `expected_lag_seconds` value (the required field).
    #[serde(default)]
    pub freshness: ProjectFreshnessConfig,

    /// Project-level schedule defaults for native demand reconciliation.
    /// Supplies the fallback timezone for per-pipeline `[…schedule]` cron
    /// blocks and the resident-loop poll cadence. See
    /// [`ScheduleDefaultsConfig`].
    #[serde(default)]
    pub schedule: ScheduleDefaultsConfig,

    /// Imported producer-project snapshots, keyed by import name.
    ///
    /// Each `[imports.<name>]` block points at a vendored snapshot of a
    /// producer project's compiled IR. During `rocky compile`, the
    /// consumer's column references are checked against the producer's
    /// published schema: a column the producer dropped but the consumer
    /// still reads surfaces as an error (E030), and a recipe-hash mismatch
    /// against a configured `pin` surfaces as E033. Empty by default — a
    /// project with no imports incurs no extra work.
    #[serde(default)]
    pub imports: IndexMap<String, ImportEntry>,

    /// Opt-in run-execution tuning for the `--skip-unchanged` model-skip
    /// gate. Default-OFF: an absent `[run]` block (or one that leaves every
    /// field at its default) keeps `rocky run`'s behavior byte-identical to
    /// before the gate existed. See [`RunConfig`].
    #[serde(default)]
    pub run: RunConfig,

    /// Auditable reuse for content-addressed models — two orthogonal knobs.
    /// `enabled` (byte-level point-to reuse) is **default-OFF**: an absent
    /// `[reuse]` block writes no input-match spine and pays no per-model hashing
    /// for it. `column_level` (column-level skip) is **default-ON** but engages
    /// only on the content-addressed path, so on the common
    /// non-content-addressed run it is inert. When `enabled`, an eligible
    /// content-addressed model whose inputs match a prior strong run may
    /// **point-to** that run's parquet (zero-copy) instead of re-executing its
    /// SQL — fail-closed to BUILD on any doubt. See [`ReuseConfig`].
    #[serde(default)]
    pub reuse: ReuseConfig,

    /// `[gc]` — storage-reclamation settings for `rocky gc` / `rocky apply
    /// <gc-plan>`. Default: physical byte-deletion stays disarmed; an applied
    /// eviction is recorded as tombstone + retired ledger row only. See
    /// [`GcConfig`].
    #[serde(default)]
    pub gc: GcConfig,

    /// Agent-authority policy plane. Declares, per
    /// `(principal, capability, scope)`, whether an action is allowed,
    /// requires human review, or is denied; enforced at `apply`, `promote`,
    /// and the MCP write tools, with every decision recorded to the
    /// ledger. Absent `[policy]` block ⇒ no
    /// rules and the default posture applies (agents on mutating actions
    /// fall to `default_agent_effect`, humans are never gated). See
    /// [`PolicyConfig`] and [`crate::policy`] for the evaluator.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<PolicyConfig>,

    /// `[resilience]` — the run loop's classified-retry policy. Governs
    /// whether a model whose materialization fails *transiently* is re-run,
    /// with a conservative bounded budget and capped backoff. On by default
    /// (a transient failure is retried), but every knob is small and explicit;
    /// see [`ResilienceConfig`]. Permanent / Unknown failures never retry.
    #[serde(default)]
    pub resilience: ResilienceConfig,
}

/// `[run]` — opt-in tuning for the model-skip gate.
///
/// The gate lets `rocky run` skip re-materializing a model whose logic and
/// upstream data both *appear* unchanged since the last successful build.
/// It is a best-effort optimization, **not** a guarantee of result-
/// equivalence: non-deterministic SQL is excluded, and any ambiguity rebuilds.
/// Every field defaults to the safe (no-skip) choice — the whole feature is
/// off unless `skip_unchanged = true` (or the `--skip-unchanged` flag) is set.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RunConfig {
    /// Master switch for the model-skip gate. `false` (default) ⇒ every
    /// selected model always builds, exactly as before. The `--skip-unchanged`
    /// CLI flag turns the gate on for a single invocation regardless of this
    /// value.
    #[serde(default)]
    pub skip_unchanged: bool,

    /// Allow a rowcount-only data-stability signal (`COUNT(*)`) when an
    /// upstream has no tracked timestamp column. Default `false`: without an
    /// explicit opt-in, a model whose upstreams are not watermarkable is not
    /// skip-eligible. Rowcount equality is weaker than a watermark: it can miss
    /// a same-size in-place `UPDATE` (or a matched insert+delete) that mutates
    /// values without changing the row count, so it stays behind this switch.
    #[serde(default)]
    pub skip_rowcount_fallback: bool,

    /// Treat an upstream `MAX(ts)` that moved by fewer than this many seconds
    /// as unchanged for the B3 freshness comparison — the late-arriving-but-
    /// irrelevant micro-update analog of a freshness SLA threshold. Default
    /// `0`: any movement at all forces a rebuild.
    #[serde(default)]
    pub lag_tolerance_seconds: u64,
}

/// `[reuse]` — auditable reuse for content-addressed models.
///
/// When `enabled = true`, a successful run records, per model, an input-match
/// index entry and an offline-verifiable provenance record (the model's logic
/// key, upstream input identities, output blake3(s), and proof class). That
/// spine is the *input* side; on a later run, an eligible model whose
/// recomputed `input_hash` hits the index for a prior **strong** run may
/// **point-to** that run's already-written parquet — a zero-copy commit that
/// skips the SQL — provided every clause of the runner's fail-closed reuse
/// decision holds. Any doubt builds.
///
/// **`enabled` is default-OFF.** `enabled = false` (the default) writes no
/// input-match spine: no extra normalize+hash work, no extra state write, no
/// point-to decision. The point-to path is strong (byte-identical) only on the
/// content-addressed/UniForm write path. Column-level skip is a separate,
/// orthogonal knob — [`ReuseConfig::column_level`], **default-ON** — that
/// engages only on that same content-addressed path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReuseConfig {
    /// Master switch for auditable reuse: populates the input-match spine and
    /// arms the point-to decision. `false` (default) ⇒ the spine is never
    /// written, no per-model hashing cost is paid, and no model reuses.
    #[serde(default)]
    pub enabled: bool,

    /// **Column-level skip** for content-addressed models. When `true`, an
    /// unpartitioned content-addressed model whose logic, environment, and
    /// every provably-consumed upstream column are unchanged since its last
    /// successful build is **skipped** — its SQL does not run and no new commit
    /// is written; the prior output stays authoritative. Skipping on a value
    /// change to a consumed column is precisely the silent-staleness bug this
    /// gate is built to avoid, so the decision is fail-closed: any unproven
    /// input (a non-deterministic model, a changed recipe/env, an
    /// un-enumerable consumed set, a missing or moved column hash) forces a
    /// build.
    ///
    /// **Default-ON.** `true` (the default) lets a content-addressed model skip
    /// when its consumed inputs are provably unchanged; set
    /// `column_level = false` to restore the always-build behavior. Independent
    /// of [`Self::enabled`] (byte-level point-to reuse) — the two are orthogonal
    /// on the content-addressed path, and off that path the feature does not
    /// apply (so on the common non-content-addressed run this default is inert).
    #[serde(default = "default_reuse_column_level")]
    pub column_level: bool,
}

/// The default for [`ReuseConfig::column_level`] — **on**. Content-addressed
/// column-level skip is enabled by default; it only engages on the
/// content-addressed path and fails closed on any unproven input.
fn default_reuse_column_level() -> bool {
    true
}

impl Default for ReuseConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            column_level: default_reuse_column_level(),
        }
    }
}

/// `[gc]` — storage-reclamation settings for `rocky gc` and its
/// review-gated `rocky apply <gc-plan>`.
///
/// Eviction is **ledger-only**: an approved apply writes the durable tombstone
/// and retires the artifact's ledger row (the eviction of record, always
/// restorable from the recorded recipe). Physical byte-deletion is not
/// performed — reclaiming bytes safely requires a protocol-aware VACUUM
/// (retention windows + TOCTOU-safe deletion), which is future work.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GcConfig {
    /// Reserved for a future protocol-aware VACUUM.
    ///
    /// `false` (the default): `rocky apply <gc-plan>` tombstones + retires the
    /// ledger row and leaves the bytes in place. `true` is currently a **hard
    /// error** at apply time — physical reclamation of content-addressed bytes
    /// requires a protocol-aware VACUUM (Delta tombstone-retention windows +
    /// TOCTOU-safe deletion against concurrent re-adds) that is not yet
    /// implemented, so the flag fails loudly rather than silently deleting or
    /// silently no-op'ing.
    #[serde(default)]
    pub physical_delete: bool,
}

/// Who is attempting an action.
///
/// `agent` is a non-human caller (an AI harness authoring, applying, or
/// remediating). `human` is a person. In v0 the principal is supplied
/// explicitly (`rocky policy check --principal …`); auto-detection is a
/// later phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PolicyPrincipal {
    /// A person.
    Human,
    /// A non-human caller (AI agent / automation).
    Agent,
}

/// The class of action a policy rule governs.
///
/// `read` is always allowed (short-circuit). The mutating verbs (`propose`
/// … `quarantine`) name coarse operations; `schema_change.additive`,
/// `schema_change.breaking`, and `value_change` are *refinements* of the
/// apply/promote verbs — a rule naming a bare verb (`apply`/`promote`)
/// matches those refinements too, but a rule naming a refinement matches
/// only that exact refinement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PolicyCapability {
    /// Read model output / metadata. Always allowed.
    Read,
    /// Draft a plan for later review.
    Propose,
    /// Apply a plan against the warehouse.
    Apply,
    /// Promote a branch / environment.
    Promote,
    /// Backfill historical partitions.
    Backfill,
    /// Garbage-collect / reclaim storage.
    Gc,
    /// Restore a gc-evicted artifact from its tombstone (rebuild + verify).
    Restore,
    /// Retry a failed run.
    Retry,
    /// Quarantine a partition / model.
    Quarantine,
    /// An additive schema change (refinement of apply/promote).
    #[serde(rename = "schema_change.additive")]
    SchemaChangeAdditive,
    /// A breaking schema change (refinement of apply/promote).
    #[serde(rename = "schema_change.breaking")]
    SchemaChangeBreaking,
    /// A value-only data change (refinement of apply/promote).
    #[serde(rename = "value_change")]
    ValueChange,
}

impl PolicyCapability {
    /// `true` when this capability is a refinement of a bare mutation verb
    /// (`schema_change.*` / `value_change`). A rule naming a refinement
    /// carries one extra "capability" constraint over a rule naming the
    /// bare verb, and only matches its exact refinement input.
    pub fn is_refinement(self) -> bool {
        matches!(
            self,
            PolicyCapability::SchemaChangeAdditive
                | PolicyCapability::SchemaChangeBreaking
                | PolicyCapability::ValueChange
        )
    }

    /// Whether a rule naming `self` matches an input capability `input`.
    ///
    /// Exact match always holds. Additionally, a rule naming the bare
    /// `apply` or `promote` verb matches any of the refinement inputs
    /// (`schema_change.*` / `value_change`) — the refinements happen *at*
    /// apply/promote time, so a policy on the bare verb governs them too.
    /// A rule naming a refinement matches only that exact refinement.
    pub fn matches_input(self, input: PolicyCapability) -> bool {
        if self == input {
            return true;
        }
        matches!(self, PolicyCapability::Apply | PolicyCapability::Promote) && input.is_refinement()
    }
}

/// The verdict a policy rule (or the default posture) yields.
///
/// Ordered by restrictiveness for incomparable-rule tie-breaking:
/// `Deny` is a hard override (handled separately), and among non-deny
/// verdicts `RequireReview` is more restrictive than `Allow`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PolicyEffect {
    /// Permit the action outright.
    Allow,
    /// Permit only after human review. The safe default posture.
    #[default]
    RequireReview,
    /// Refuse the action. A hard override — no `allow` overturns it.
    Deny,
}

/// Scope of a policy rule — the AND of every present predicate. A model
/// matches the scope only when it satisfies *all* set keys.
///
/// `any = true` is the empty scope (matches every model, zero
/// constraints) and is mutually exclusive with every other key.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PolicyScope {
    /// Match every model. Mutually exclusive with all other keys; carries
    /// zero constraints, so any rule with a real predicate outranks it.
    #[serde(default)]
    pub any: bool,

    /// Glob selectors over the model name (`*`/`?`). Satisfied when the
    /// model name matches at least one pattern.
    #[serde(default)]
    pub models: Vec<String>,

    /// Required model tags (AND of `key = value` pairs). Satisfied when the
    /// model carries every listed tag with the exact value.
    #[serde(default)]
    pub tags: std::collections::BTreeMap<String, String>,

    /// Classification guard (positive). Satisfied when the model has at
    /// least one column classified with any listed value (e.g. `["pii"]`).
    #[serde(default)]
    pub classifications: Vec<String>,

    /// Classification guard (negative). Satisfied when the model has *no*
    /// column classified with any listed value — e.g.
    /// `exclude_classifications = ["pii"]` matches only non-PII models.
    #[serde(default)]
    pub exclude_classifications: Vec<String>,

    /// Contract-boundary guard. Satisfied when the model's contracted
    /// status equals this value. (v0 reads contracted status best-effort
    /// from a sibling `.contract.toml`; see [`crate::policy`].)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contracted: Option<bool>,

    /// Medallion/semantic layer guard. Satisfied when the model's `layer`
    /// tag equals this value (v0 reads layer from the model's `layer` tag).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub layer: Option<String>,

    /// Blast-radius guard: maximum transitive downstream count. Enforced as a
    /// **post-match ceiling on `allow`**, not a scope predicate: the rule
    /// still matches, and when its effect is `allow` and the target's
    /// transitive downstream reachability exceeds the ceiling — or cannot be
    /// computed — the effect degrades to `require_review` (fail-closed).
    /// `deny` / `require_review` rules are unaffected.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_downstreams: Option<u64>,
}

impl PolicyScope {
    /// `true` when any key other than `any` is set.
    fn has_real_predicate(&self) -> bool {
        !self.models.is_empty()
            || !self.tags.is_empty()
            || !self.classifications.is_empty()
            || !self.exclude_classifications.is_empty()
            || self.contracted.is_some()
            || self.layer.is_some()
            || self.max_downstreams.is_some()
    }
}

/// An autonomy budget on a policy rule — the SRE error-budget move applied to
/// agent authority.
///
/// A rule that carries `autonomy_budget = { failures = 3, window = "7d" }`
/// tolerates at most `failures - 1` post-apply verification failures inside a
/// rolling `window`. The moment the count reaches `failures`, the budget is
/// exhausted and the rule **automatically degrades to `require_review`** at
/// enforcement time (an `allow` can no longer stand un-reviewed). This is a
/// one-directional breaker: it only ever tightens the rule, never widens it.
/// Widening autonomy remains a deliberate human act on scorecard evidence.
///
/// The failure count is a *projection over the existing decision ledger* — it
/// is never a persisted counter, so recovery is automatic: once the failing
/// applies age out of the window the count falls below the limit and the rule
/// returns to its authored effect. The rule is never made more permissive than
/// the effect the author wrote.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AutonomyBudget {
    /// Verify-after failures within `window` that exhaust the budget. The
    /// `failures`-th failure trips the breaker (must be `>= 1`).
    pub failures: u64,
    /// Rolling window over which failures are counted, as a `<N>d` / `<N>h`
    /// duration (e.g. `"7d"`, `"24h"`). Failures older than this do not count.
    pub window: String,
}

/// Parse a `<N>d` / `<N>h` budget/window duration string into a [`Duration`].
///
/// Accepts a positive integer followed by a `d` (days) or `h` (hours) unit,
/// case-insensitive — `"7d"`, `"24h"`, `"30D"`. Returns `None` for anything
/// malformed or out of `chrono`'s representable range, so callers can fail
/// closed (a budget whose window will not parse never degrades a rule, and
/// config validation rejects it up front).
pub fn parse_window_duration(raw: &str) -> Option<chrono::Duration> {
    let raw = raw.trim();
    let unit = raw.chars().next_back()?;
    let digits = &raw[..raw.len() - unit.len_utf8()];
    let n: i64 = digits.parse().ok()?;
    if n <= 0 {
        return None;
    }
    match unit {
        'd' | 'D' => chrono::Duration::try_days(n),
        'h' | 'H' => chrono::Duration::try_hours(n),
        _ => None,
    }
}

/// One `[[policy.rules]]` entry: `(principal, capability, scope) → effect`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PolicyRule {
    /// Who this rule applies to.
    pub principal: PolicyPrincipal,
    /// Which capability this rule governs.
    pub capability: PolicyCapability,
    /// The models this rule covers. Defaults to the empty scope, which is
    /// *not* `any` — an all-default scope with no `any = true` matches
    /// nothing and is rejected at validation.
    #[serde(default)]
    pub scope: PolicyScope,
    /// The verdict when this rule matches.
    pub effect: PolicyEffect,
    /// Post-apply verification: named checks that must pass after a mutation
    /// governed by this rule lands. Once the apply's run completes, each named
    /// check is confirmed against the run's executed checks; a failing **or
    /// absent** named check halts the apply (fail closed) and raises an alert.
    /// Auto-rollback runs only where a rollback substrate exists; today none
    /// does, so a failure is halt-only and the mutation stands until a human
    /// reverts it. Empty ⇒ no post-apply gate.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub verify_after: Vec<String>,
    /// Optional v1 conditional refinements not yet promoted to typed fields.
    /// **Parsed and ignored** — captured as opaque JSON so a config authored
    /// against a later version still loads.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conditions: Option<serde_json::Value>,
    /// Optional autonomy budget: a rolling failure ceiling that degrades this
    /// rule to `require_review` when its verify-after failures exhaust it. See
    /// [`AutonomyBudget`]. Absent ⇒ the rule's effect is never budget-degraded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub autonomy_budget: Option<AutonomyBudget>,
}

/// One `[[policy.tests]]` scenario: a self-contained assertion over the
/// policy evaluator.
///
/// A scenario names a `principal`, a `capability`, a synthetic target model
/// (its attributes spelled out inline), and the `expect`ed effect. The
/// `rocky policy test` runner constructs a
/// [`crate::policy::ModelAttributes`] from these fields *verbatim* — the same
/// value the evaluator receives at a real enforcement seam — feeds it to
/// [`crate::policy::evaluate`], and asserts the resolved effect equals
/// `expect`. Because the attributes are declared, not compiled, a scenario is
/// stable regardless of the current project graph: it pins the *policy's*
/// behaviour, which is exactly what a policy edit must not silently change.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PolicyTest {
    /// Human-readable name for the scenario, echoed in the pass/fail report.
    pub name: String,
    /// The principal attempting the action.
    pub principal: PolicyPrincipal,
    /// The capability being attempted.
    pub capability: PolicyCapability,
    /// The effect the evaluator must resolve for this scenario. A mismatch
    /// fails the scenario (and the `rocky policy test` run).
    pub expect: PolicyEffect,
    /// Synthetic model name, matched against rule `scope.models` globs.
    /// Empty (the default) matches no name-scoped rule — only `any`/attribute
    /// rules apply.
    #[serde(default)]
    pub model: String,
    /// Synthetic model-level tags (matched against rule `scope.tags`).
    #[serde(default)]
    pub tags: std::collections::BTreeMap<String, String>,
    /// Synthetic column classifications present on the model (matched against
    /// `scope.classifications` / `scope.exclude_classifications`).
    #[serde(default)]
    pub classifications: Vec<String>,
    /// Whether the synthetic model sits behind a contract (matched against
    /// `scope.contracted`).
    #[serde(default)]
    pub contracted: bool,
    /// Synthetic medallion/semantic layer (matched against `scope.layer`).
    /// When omitted, the runner derives it from `tags["layer"]`, mirroring how
    /// a real enforcement seam reads the model's `layer` tag — so a
    /// `tags = { layer = ... }` scenario matches a `scope.layer` rule without
    /// restating the value. An explicit value here always wins.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub layer: Option<String>,
    /// Synthetic direct downstream count. Informational — the
    /// `max_downstreams` ceiling reads `reachable_downstreams`.
    #[serde(default)]
    pub downstreams: u64,
    /// Synthetic transitive blast radius, compared against a rule's
    /// `max_downstreams` ceiling. Omit (the default `null`) to model an
    /// **uncomputable** blast radius — the ceiling then fails closed, exactly
    /// as at a real seam where the graph did not compile.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reachable_downstreams: Option<u64>,
}

/// The `[policy]` block: agent-authority policy for this project.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PolicyConfig {
    /// Schema version. Must be `1`.
    pub version: u32,
    /// Effect for an `agent` on a mutating capability when no rule matches.
    /// Defaults to `require_review` (the safe posture).
    #[serde(default)]
    pub default_agent_effect: PolicyEffect,
    /// Ordered list of rules. Evaluated as a set (order only breaks final
    /// ties); see [`crate::policy::evaluate`].
    #[serde(default, rename = "rules")]
    pub rules: Vec<PolicyRule>,
    /// Scenario assertions run by `rocky policy test`. Each pins the effect
    /// the evaluator must resolve for a `(principal, capability, target)`
    /// triple, so a policy edit that would silently open a hole fails CI.
    /// Never read by any enforcement path — purely a testing surface.
    #[serde(default, rename = "tests", skip_serializing_if = "Vec::is_empty")]
    pub tests: Vec<PolicyTest>,
}

impl PolicyConfig {
    /// The default posture applied when no `[policy]` block is present:
    /// version 1, agents on mutating actions require review, no rules.
    pub fn default_posture() -> Self {
        PolicyConfig {
            version: 1,
            default_agent_effect: PolicyEffect::RequireReview,
            rules: Vec::new(),
            tests: Vec::new(),
        }
    }
}

/// Validates the `[policy]` block: `version` must be 1, and every rule's
/// scope must be well-formed (`any = true` is mutually exclusive with all
/// other scope keys; a scope with neither `any` nor any real predicate is
/// rejected). Returns every problem found.
pub fn validate_policy(config: &RockyConfig) -> Vec<ConfigError> {
    let mut errors = Vec::new();
    let Some(policy) = &config.policy else {
        return errors;
    };
    if policy.version != 1 {
        errors.push(ConfigError::PolicyUnsupportedVersion {
            version: policy.version,
        });
    }
    for (idx, rule) in policy.rules.iter().enumerate() {
        let has_real = rule.scope.has_real_predicate();
        if rule.scope.any && has_real {
            errors.push(ConfigError::PolicyScopeAnyConflict { rule_index: idx });
        }
        if !rule.scope.any && !has_real {
            errors.push(ConfigError::PolicyScopeEmpty { rule_index: idx });
        }
        if let Some(budget) = &rule.autonomy_budget {
            if budget.failures == 0 {
                errors.push(ConfigError::PolicyBudgetZeroFailures { rule_index: idx });
            }
            if parse_window_duration(&budget.window).is_none() {
                errors.push(ConfigError::PolicyBudgetInvalidWindow {
                    rule_index: idx,
                    window: budget.window.clone(),
                });
            }
        }
    }
    errors
}

impl RockyConfig {
    /// Resolve `[mask]` + `[mask.<env>]` into a flat `classification → strategy`
    /// map for the active environment.
    ///
    /// Resolution order:
    /// 1. All top-level scalar entries (`pii = "hash"`) become defaults.
    /// 2. If `env` is `Some(name)` and a matching `[mask.<name>]` override
    ///    table exists, its entries overlay the defaults (same-key wins).
    ///
    /// When `env` is `None` or no matching override table exists, the
    /// returned map contains only the top-level defaults. Non-matching
    /// override tables are ignored for this env (they may apply to other
    /// envs in the same config).
    pub fn resolve_mask_for_env(
        &self,
        env: Option<&str>,
    ) -> std::collections::BTreeMap<String, rocky_ir::MaskStrategy> {
        let mut out = std::collections::BTreeMap::new();

        // Pass 1: every scalar entry is a workspace-default.
        for (key, entry) in &self.mask {
            if let MaskEntry::Strategy(s) = entry {
                out.insert(key.clone(), *s);
            }
        }

        // Pass 2: if env matches a nested override table, overlay.
        if let Some(env_name) = env
            && let Some(MaskEntry::EnvOverride(overrides)) = self.mask.get(env_name)
        {
            for (k, v) in overrides {
                out.insert(k.clone(), *v);
            }
        }

        out
    }

    /// Flatten the `[role.*]` config into a deterministic `name →
    /// ResolvedRole` map, ready to pass to
    /// [`crate::traits::GovernanceAdapter::reconcile_role_graph`].
    ///
    /// # Errors
    ///
    /// Returns [`crate::role_graph::RoleGraphError`] on cycles, unknown
    /// parents, or unknown permission spellings.
    pub fn role_graph(
        &self,
    ) -> Result<
        std::collections::BTreeMap<String, rocky_ir::ResolvedRole>,
        crate::role_graph::RoleGraphError,
    > {
        crate::role_graph::flatten_role_graph(&self.roles)
    }
}

/// Project-wide dialect portability configuration.
///
/// Lives at the top level because a Rocky project targets one warehouse;
/// per-pipeline overrides aren't supported yet (no demand signal). The
/// `allow` list applies to every model — a per-model override is the
/// `-- rocky-allow: <constructs>` pragma in the model SQL itself, parsed
/// by [`rocky_sql::pragma`].
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PortabilityConfig {
    /// Target dialect for the portability lint. When unset, no lint runs
    /// (matches the wave-1 "flag opt-in" behavior). The CLI flag overrides
    /// this if both are present.
    #[serde(default)]
    pub target_dialect: Option<rocky_sql::transpile::Dialect>,

    /// Project-wide allow-list of construct labels (case-insensitive,
    /// matched against `PortabilityIssue::construct`). Useful for blanket
    /// exemptions like `allow = ["QUALIFY"]` when a project standardizes on
    /// a specific extension. For per-model exemptions prefer the
    /// `-- rocky-allow: <construct>` pragma over expanding this list.
    #[serde(default)]
    pub allow: Vec<String>,
}

/// Top-level retry configuration applied across every adapter for this
/// run. See [`RockyConfig::retry`] for the cross-adapter semantics this
/// unlocks.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RunRetryConfig {
    /// Total number of retries allowed across every adapter for this run.
    /// `None` means no cross-adapter cap (each adapter's own
    /// `retry.max_retries_per_run` still applies in isolation).
    #[serde(default)]
    pub max_retries_per_run: Option<u32>,
}

/// Configuration for the AI intent layer (`rocky ai`, `rocky ai-explain`,
/// `rocky ai-sync`, `rocky ai-test`).
///
/// `max_tokens` doubles as:
/// 1. The per-request `max_tokens` cap on the Anthropic Messages API.
/// 2. The cumulative output-token budget across the compile-verify retry
///    loop — when the running total exceeds this value, the loop fail-stops
///    instead of issuing another retry. This bounds the worst-case spend
///    when the LLM produces runaway responses that fail validation.
///
/// The default ([`DEFAULT_AI_MAX_TOKENS`]) preserves Rocky's pre-1.x
/// hard-coded behaviour. Increase for projects that legitimately need
/// longer generations (large model surfaces, verbose tests).
///
/// ```toml
/// [ai]
/// max_tokens = 8192
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AiSection {
    /// Per-request `max_tokens` and cumulative output-token budget across
    /// retries. Default [`DEFAULT_AI_MAX_TOKENS`].
    #[serde(default = "default_ai_max_tokens")]
    pub max_tokens: u32,
}

impl Default for AiSection {
    fn default() -> Self {
        Self {
            max_tokens: DEFAULT_AI_MAX_TOKENS,
        }
    }
}

/// Default value for [`AiSection::max_tokens`]. Mirrors
/// `rocky_ai::client::DEFAULT_MAX_TOKENS`; duplicated as a `const` here so
/// the schema generator and TOML parser don't need to depend on the AI
/// crate (which would invert the dependency graph).
pub const DEFAULT_AI_MAX_TOKENS: u32 = 4096;

/// Default `max_age_seconds` for [`BranchApprovalConfig`]. 24 hours — long
/// enough for a same-day approve/promote cycle, short enough that an
/// abandoned approval doesn't sit on a stale branch indefinitely.
pub const DEFAULT_APPROVAL_MAX_AGE_SECONDS: u64 = 86400;

/// Default minimum number of valid approvals required when the gate is
/// enabled.
pub const DEFAULT_APPROVAL_MIN_APPROVERS: u32 = 1;

/// Top-level `[branch]` configuration section.
///
/// Currently scopes the optional approval gate consumed by
/// `rocky branch promote`. Default-constructed (no `[branch]` block in
/// `rocky.toml`) leaves the gate disabled — `branch promote` skips the
/// approval loop and behaves like the unguarded baseline.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BranchSection {
    #[serde(default)]
    pub approval: BranchApprovalConfig,
}

/// `[branch.approval]` configuration block.
///
/// Defaults are deliberately permissive — a project that doesn't add the
/// section keeps the v0 `branch promote` behaviour. Flipping
/// `required = true` opts in to the gate; the rest of the knobs tune the
/// strictness once the gate is on.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BranchApprovalConfig {
    /// When true, `branch promote` refuses to run unless at least
    /// `min_approvers` valid approval artifacts are on disk for the branch.
    /// When false (default), the gate is bypassed silently.
    #[serde(default)]
    pub required: bool,

    /// Minimum number of valid approvals required when `required = true`.
    /// "Valid" means: signature verifies, branch_state_hash matches the
    /// current state, signed within `max_age_seconds`, and (when
    /// `allowed_signers` is non-empty) the signer's email is in the list.
    #[serde(default = "default_approval_min_approvers")]
    pub min_approvers: u32,

    /// When non-empty, only approvals from these signer emails count toward
    /// `min_approvers`. Empty (default) accepts any signer.
    #[serde(default)]
    pub allowed_signers: Vec<String>,

    /// Approvals older than this many seconds are rejected even if their
    /// branch_state_hash still matches. Default 86400 (24h).
    #[serde(default = "default_approval_max_age_seconds")]
    pub max_age_seconds: u64,
}

impl Default for BranchApprovalConfig {
    fn default() -> Self {
        Self {
            required: false,
            min_approvers: DEFAULT_APPROVAL_MIN_APPROVERS,
            allowed_signers: Vec::new(),
            max_age_seconds: DEFAULT_APPROVAL_MAX_AGE_SECONDS,
        }
    }
}

fn default_approval_min_approvers() -> u32 {
    DEFAULT_APPROVAL_MIN_APPROVERS
}

fn default_approval_max_age_seconds() -> u64 {
    DEFAULT_APPROVAL_MAX_AGE_SECONDS
}

fn default_ai_max_tokens() -> u32 {
    DEFAULT_AI_MAX_TOKENS
}

/// Schema-only helper that mirrors the deserializer's acceptance of both
/// flat (`[adapter] type = "..."`) and named (`[adapter.foo] type = "..."`)
/// adapter shapes.
///
/// [`normalize_toml_shorthands`] rewrites the flat form into
/// `adapter.default` before [`RockyConfig`] is deserialized, so the Rust
/// type only sees the named form. The IDE schema, however, validates the
/// raw TOML — so it must accept both shapes directly. 38 of 46 committed
/// POC `rocky.toml` files use the flat form; the schema would reject all
/// of them without this anyOf.
#[doc(hidden)]
#[derive(serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(untagged)]
pub enum AdaptersFieldSchema {
    /// Flat `[adapter]` block with `type` directly under it (single adapter).
    Flat(Box<AdapterConfig>),
    /// Named `[adapter.<name>]` blocks (one or more adapters keyed by name).
    Nested(IndexMap<String, AdapterConfig>),
}

/// Schema-only helper mirroring [`AdaptersFieldSchema`] for `[pipeline.*]`.
///
/// The flat-pipeline shorthand exists in [`normalize_toml_shorthands`] but
/// is unused across all committed POCs; including it in the schema is
/// defensive — a user typing `[pipeline] source = ...` shouldn't see a
/// false IDE error. The pipeline payload references [`PipelineConfig`]
/// directly, whose hand-written `JsonSchema` impl emits an `anyOf` of the
/// five pipeline variants.
#[doc(hidden)]
#[derive(serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(untagged)]
pub enum PipelinesFieldSchema {
    /// Flat `[pipeline]` block (single pipeline, auto-named `default`).
    Flat(Box<PipelineConfig>),
    /// Named `[pipeline.<name>]` blocks.
    Nested(IndexMap<String, PipelineConfig>),
}

/// Role an adapter block plays in the pipeline.
///
/// Set via `kind = "data"` or `kind = "discovery"` in an `[adapter.*]`
/// block. Required on discovery-only adapter types (`fivetran`,
/// `airbyte`, `iceberg`, `manual`) so the adapter's role is self-evident
/// in the raw config file — a reader shouldn't have to know the Rust
/// trait surface of each adapter to tell whether it moves data.
///
/// Optional on single-role warehouse types (defaults to `Data`) and on
/// the dual-capable DuckDB adapter (absent means "register both roles").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum AdapterKind {
    /// Warehouse data movement (reads / writes table bytes).
    Data,
    /// Metadata-only discovery (enumerates source schemas / connectors).
    Discovery,
}

/// Configuration for a named adapter instance.
///
/// The `type` field determines which adapter crate handles this config.
/// Adapter-specific fields are captured via `serde(flatten)`.
///
/// Credential fields (`token`, `client_secret`, `api_key`, `api_secret`,
/// `password`, `oauth_token`) are wrapped in [`RedactedString`] so that
/// `Debug` output never leaks secrets.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AdapterConfig {
    /// Adapter type: "databricks", "fivetran", "duckdb", etc.
    #[serde(rename = "type")]
    pub adapter_type: String,

    /// Role this adapter block plays. See [`AdapterKind`] for the
    /// required-vs-optional rules per adapter type.
    #[serde(default)]
    pub kind: Option<AdapterKind>,

    // -- Databricks fields --
    pub host: Option<String>,
    pub http_path: Option<String>,
    pub token: Option<RedactedString>,
    pub client_id: Option<String>,
    pub client_secret: Option<RedactedString>,
    pub timeout_secs: Option<u64>,

    // -- Fivetran fields --
    pub destination_id: Option<String>,
    pub api_key: Option<RedactedString>,
    pub api_secret: Option<RedactedString>,

    // -- Snowflake fields --
    /// Snowflake account identifier (e.g., "xy12345.us-east-1").
    pub account: Option<String>,
    /// Snowflake warehouse to use for query execution.
    pub warehouse: Option<String>,
    /// Snowflake username (for password or key-pair auth).
    pub username: Option<String>,
    /// Snowflake password (for password auth).
    pub password: Option<RedactedString>,
    /// OAuth access token (pre-obtained from an IdP).
    pub oauth_token: Option<RedactedString>,
    /// Path to RSA private key file (PEM) for key-pair auth.
    pub private_key_path: Option<String>,
    /// Programmatic Access Token (issued via Snowsight User Profile).
    /// Sent as a Bearer token with the `PROGRAMMATIC_ACCESS_TOKEN`
    /// token-type header — distinct from `oauth_token`.
    pub pat: Option<RedactedString>,
    /// Snowflake role to use for the session.
    pub role: Option<String>,
    /// Default database for the session.
    pub database: Option<String>,

    // -- BigQuery fields --
    /// Google Cloud project ID.
    pub project_id: Option<String>,
    /// BigQuery processing location (e.g., "US", "EU", "us-central1").
    pub location: Option<String>,

    // -- DuckDB fields --
    /// Optional file path for a persistent DuckDB database.
    /// When unset, the adapter uses an in-memory database.
    /// A persistent path is required when the same DuckDB adapter is also used
    /// as a discovery source — discovery and warehouse share the same database.
    pub path: Option<String>,

    /// Retry policy for this adapter.
    #[serde(default)]
    pub retry: RetryConfig,

    /// Optional cache backend for the Fivetran state envelope.
    ///
    /// When set on a `type = "fivetran"` adapter, the resolved
    /// envelope is read from and written to the configured cache
    /// backend so concurrent `rocky` processes share one fetcher per
    /// org. Ignored on every other adapter type. When absent the
    /// adapter behaves as if `backend = "none"` — every fetch goes
    /// straight to the Fivetran API.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache: Option<FivetranCacheConfig>,

    /// Optional cross-pod rate-limit budget backend (Fivetran-only).
    ///
    /// Phase 1 ratelimit coordination is per-host (file in
    /// `${TMPDIR}/rocky-fivetran-ratelimit/`). Setting `backend = "valkey"`
    /// here lifts the budget into a shared store so several pods on
    /// different hosts observe the same `wake_at` window after one of
    /// them is throttled. Ignored on non-fivetran adapters. When absent
    /// the adapter falls back to the per-host file backend.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ratelimit: Option<FivetranRatelimitConfig>,

    /// Optional distributed cache-stampede lock (Fivetran-only).
    ///
    /// On a cold-start herd, N processes simultaneously miss the cache,
    /// fan out N API calls, and write back N times. The stampede lock
    /// elects a single leader to issue the API call; followers poll the
    /// cache until the leader publishes the envelope. Ignored on
    /// non-fivetran adapters. When absent the adapter behaves as if
    /// every process is the leader (the pre-stampede behavior).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stampede: Option<FivetranStampedeConfig>,

    /// Optional per-account circuit breaker (Fivetran-only).
    ///
    /// Trips after `failure_threshold` consecutive remote failures and
    /// short-circuits subsequent HTTP attempts with a `CircuitOpen`
    /// error until a cooldown elapses. Coordinated across processes
    /// via the configured backend (Valkey). Ignored on non-fivetran
    /// adapters; absent block defaults to `AlwaysClosed` (no breaker).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub circuit_breaker: Option<FivetranCircuitBreakerConfig>,

    /// Escape hatch for adapter-specific keys this struct doesn't model.
    ///
    /// `AdapterConfig` is `#[serde(deny_unknown_fields)]` so typos at the
    /// top level (`tooken` for `token`) surface as parse errors rather
    /// than silent ignores. That guard, however, also blocks legitimate
    /// adapter-specific settings — an out-of-tree Trino adapter wanting
    /// a `default_schema` slot, a Postgres adapter wanting a non-standard
    /// `application_name`, etc. — from ever reaching the adapter.
    ///
    /// Authors of such adapters declare the keys under a nested `[extra]`
    /// table:
    ///
    /// ```toml
    /// [adapter.my_trino]
    /// type = "trino"
    /// host = "https://trino.example.com"
    /// token = "${TRINO_JWT}"
    ///
    /// [adapter.my_trino.extra]
    /// default_schema = "analytics"
    /// x_trino_user = "service-account"
    /// ```
    ///
    /// Top-level typos still error (`tooken = "..."` is still rejected);
    /// only keys nested under `[adapter.<name>.extra]` flow through to the
    /// adapter unchanged. Adapters read these via `.extra.get("...")` and
    /// validate them themselves — Rocky doesn't schema-check the contents.
    ///
    /// Values are `serde_json::Value` so the field survives `just codegen`
    /// (`toml::Value` doesn't derive `JsonSchema`); TOML scalars / tables /
    /// arrays still round-trip through serde because they all map to the
    /// JSON shape.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub extra: std::collections::BTreeMap<String, serde_json::Value>,
}

impl std::fmt::Debug for AdapterConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdapterConfig")
            .field("adapter_type", &self.adapter_type)
            .field("kind", &self.kind)
            .field("host", &self.host)
            .field("http_path", &self.http_path)
            .field("token", &self.token.as_ref().map(|_| "***"))
            .field("client_id", &self.client_id)
            .field("client_secret", &self.client_secret.as_ref().map(|_| "***"))
            .field("timeout_secs", &self.timeout_secs)
            .field("destination_id", &self.destination_id)
            .field("api_key", &self.api_key.as_ref().map(|_| "***"))
            .field("api_secret", &self.api_secret.as_ref().map(|_| "***"))
            .field("account", &self.account)
            .field("warehouse", &self.warehouse)
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| "***"))
            .field("oauth_token", &self.oauth_token.as_ref().map(|_| "***"))
            .field("private_key_path", &self.private_key_path)
            .field("role", &self.role)
            .field("database", &self.database)
            .field("project_id", &self.project_id)
            .field("location", &self.location)
            .field("path", &self.path)
            .field("retry", &self.retry)
            .field("cache", &self.cache)
            .field("ratelimit", &self.ratelimit)
            .field("stampede", &self.stampede)
            .field("circuit_breaker", &self.circuit_breaker)
            .field("extra", &self.extra)
            .finish()
    }
}

/// Persistent state cache configuration for the Fivetran adapter (FR-A).
///
/// Cache backends shared across processes let several `rocky` invocations
/// against one Fivetran org dedupe their discover fetches — the first
/// process pays the API cost, every subsequent process within the TTL
/// window reads the canonical envelope from the cache. See
/// `engine/crates/rocky-fivetran/src/state_cache/` for the implementation
/// details.
///
/// ## TOML shape
///
/// ```toml
/// [adapter.fivetran.cache]
/// backend = "tiered"                              # "none" | "file" | "object_store" | "valkey" | "tiered"
/// file_root = ".rocky/fivetran-state/"            # required for backend = "file"
/// object_store_url = "s3://my-bucket/rocky/fv/"   # required for backend = "object_store" / "tiered"
/// valkey_url = "rediss://valkey:6379/"            # required for backend = "valkey" / "tiered"
/// valkey_ttl_seconds = 600                        # default 600
/// ```
///
/// ## Backend selection
///
/// - `none` — disable the cache; default when the block is absent.
/// - `file` — local-filesystem JSON files under `file_root`.
/// - `object_store` — S3 / GCS / Azure / `file://`; URL parsed by
///   `object_store::parse_url`. Credentials come from the SDK default
///   chain (`AWS_*` env vars, IAM role, `GOOGLE_APPLICATION_CREDENTIALS`,
///   etc.) — Rocky doesn't introduce its own credential surface.
/// - `valkey` — Redis / Valkey; requires building rocky-fivetran with
///   the `valkey` Cargo feature. URL accepts `redis://` and `rediss://`
///   (TLS).
/// - `tiered` — composes Valkey (primary, fast) + object-store
///   (secondary, durable). Requires both URLs.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FivetranCacheConfig {
    /// Which backend to instantiate. Defaults to
    /// [`FivetranCacheBackend::None`] so a stub `[adapter.fivetran.cache]`
    /// block with no `backend` key is well-defined.
    #[serde(default)]
    pub backend: FivetranCacheBackend,

    /// Root directory for `backend = "file"`. Required for that
    /// backend; ignored otherwise. May contain `${VAR}` / `${VAR:-default}`
    /// env-var references; resolved at config-load time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_root: Option<String>,

    /// URL passed to `object_store::parse_url` for `backend = "object_store"`
    /// or `backend = "tiered"` (secondary layer). Examples:
    /// `s3://bucket/prefix/`, `gs://bucket/prefix/`, `az://container/prefix/`,
    /// `file:///path/`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_store_url: Option<String>,

    /// Connection URL for `backend = "valkey"` or `backend = "tiered"`
    /// (primary layer). Standard `redis://` (plain) or `rediss://` (TLS).
    // SECURITY: may embed credentials (`redis://:password@host`). Never log
    // this value; redact it if it ever reaches an error/trace message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub valkey_url: Option<String>,

    /// TTL applied to every `SET` against the Valkey backend. Defaults
    /// to 600s when unset. Ignored for non-Valkey backends.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub valkey_ttl_seconds: Option<u64>,
}

/// Cache backend selector for [`FivetranCacheConfig`]. See the parent
/// struct for the TOML shape and per-backend semantics.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FivetranCacheBackend {
    /// No-op backend; every fetch goes to the Fivetran API. Default
    /// when `[adapter.fivetran.cache]` is absent.
    #[default]
    None,
    /// Local-filesystem JSON files. Cheapest, no external dependency.
    File,
    /// S3 / GCS / Azure / `file://` via the `object_store` crate.
    ObjectStore,
    /// Valkey / Redis. Requires the `valkey` Cargo feature.
    Valkey,
    /// Valkey (primary) + object_store (secondary). Requires the
    /// `valkey` Cargo feature.
    Tiered,
}

impl FivetranCacheBackend {
    /// Stable wire-form string used in error messages and OTLP attrs.
    /// Pinned so a refactor can't silently re-tag the backend tags
    /// dashboards filter on.
    pub fn as_str(self) -> &'static str {
        match self {
            FivetranCacheBackend::None => "none",
            FivetranCacheBackend::File => "file",
            FivetranCacheBackend::ObjectStore => "object_store",
            FivetranCacheBackend::Valkey => "valkey",
            FivetranCacheBackend::Tiered => "tiered",
        }
    }
}

impl FivetranCacheConfig {
    /// Validate the cross-field requirements (which fields are
    /// required for which backend). Returns the first error
    /// encountered so the caller can surface it at config-load time
    /// rather than at first cache touch.
    pub fn validate(&self, adapter_name: &str) -> Result<(), ConfigError> {
        let backend = self.backend;
        let need = |field: &str, value: Option<&String>| -> Result<(), ConfigError> {
            if value.map(String::is_empty).unwrap_or(true) {
                Err(ConfigError::FivetranCacheMissingField {
                    adapter: adapter_name.to_string(),
                    backend: backend.as_str().to_string(),
                    field: field.to_string(),
                })
            } else {
                Ok(())
            }
        };
        match self.backend {
            FivetranCacheBackend::None => Ok(()),
            FivetranCacheBackend::File => need("file_root", self.file_root.as_ref()),
            FivetranCacheBackend::ObjectStore => {
                need("object_store_url", self.object_store_url.as_ref())
            }
            FivetranCacheBackend::Valkey => need("valkey_url", self.valkey_url.as_ref()),
            FivetranCacheBackend::Tiered => {
                need("object_store_url", self.object_store_url.as_ref())?;
                need("valkey_url", self.valkey_url.as_ref())
            }
        }
    }
}

/// Cross-pod rate-limit budget backend selector for the Fivetran adapter
/// (FR-B Phase 2). See [`FivetranRatelimitConfig`] for the TOML shape.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FivetranRatelimitBackend {
    /// Per-host file lock (Phase 1 behavior). Default when the block is
    /// absent.
    #[default]
    File,
    /// Shared Valkey key. Requires the `valkey` Cargo feature.
    Valkey,
}

impl FivetranRatelimitBackend {
    /// Stable wire-form string for log messages and OTLP attrs.
    pub fn as_str(self) -> &'static str {
        match self {
            FivetranRatelimitBackend::File => "file",
            FivetranRatelimitBackend::Valkey => "valkey",
        }
    }
}

/// Cross-pod rate-limit budget config for the Fivetran adapter
/// (FR-B Phase 2).
///
/// Phase 1 (shipped in v1.37) writes `wake_at_epoch_ms` to a per-host
/// file under `${TMPDIR}/rocky-fivetran-ratelimit/<account_hash>.json`.
/// That works when several `rocky` processes share a host, but a
/// Kubernetes deployment with one rocky-cli per pod sees N independent
/// budgets even though every pod talks to the same Fivetran org. Lifting
/// the budget into Valkey makes a 429 on one pod throttle every other
/// pod for the same `Retry-After` window.
///
/// ## TOML shape
///
/// ```toml
/// [adapter.fivetran.ratelimit]
/// backend = "valkey"
/// valkey_url = "rediss://valkey:6379/"
/// ```
///
/// ## Fail-open
///
/// When the configured backend is unreachable the client falls back to
/// the per-host file backend. The cluster regresses to Phase 1 behavior
/// (each host enforces its own budget) rather than blocking traffic.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FivetranRatelimitConfig {
    /// Which backend to instantiate. Defaults to
    /// [`FivetranRatelimitBackend::File`] so a stub block with no
    /// `backend` key is well-defined.
    #[serde(default)]
    pub backend: FivetranRatelimitBackend,

    /// Connection URL for `backend = "valkey"`. Standard `redis://`
    /// (plain) or `rediss://` (TLS).
    // SECURITY: may embed credentials (`redis://:password@host`). Never log
    // this value; redact it if it ever reaches an error/trace message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub valkey_url: Option<String>,

    /// Cap (seconds) applied to the Valkey key's `EXPIRE` so an
    /// abandoned `wake_at` value never outlives its useful window.
    /// Defaults to 600s when unset; ignored for the file backend.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_wake_seconds: Option<u64>,
}

impl FivetranRatelimitConfig {
    /// Validate that the configured backend has all the fields it needs.
    /// Surfaces errors at config-load time via
    /// [`validate_fivetran_resilience`] so a misconfigured backend
    /// doesn't silently degrade to fail-open at first request.
    pub fn validate(&self, adapter_name: &str) -> Result<(), ConfigError> {
        match self.backend {
            FivetranRatelimitBackend::File => Ok(()),
            FivetranRatelimitBackend::Valkey => {
                if self.valkey_url.as_deref().is_none_or(str::is_empty) {
                    Err(ConfigError::FivetranRatelimitMissingField {
                        adapter: adapter_name.to_string(),
                        backend: self.backend.as_str().to_string(),
                        field: "valkey_url".into(),
                    })
                } else {
                    Ok(())
                }
            }
        }
    }
}

/// Distributed cache-stampede lock backend selector for the Fivetran
/// adapter (Layer 1). See [`FivetranStampedeConfig`] for the TOML
/// shape.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FivetranStampedeBackend {
    /// No-op lock; every caller is treated as the leader. Default when
    /// the block is absent.
    #[default]
    None,
    /// Distributed lock via Valkey `SET NX EX`. Requires the `valkey`
    /// Cargo feature.
    Valkey,
}

impl FivetranStampedeBackend {
    /// Stable wire-form string for log messages and OTLP attrs.
    pub fn as_str(self) -> &'static str {
        match self {
            FivetranStampedeBackend::None => "none",
            FivetranStampedeBackend::Valkey => "valkey",
        }
    }
}

/// Distributed cache-stampede protection config for the Fivetran
/// adapter (Layer 1).
///
/// On a cold-start burst, N processes can simultaneously observe the
/// cache miss, fan out N API calls, and write back N copies of the
/// same envelope. The stampede lock elects a single leader (via
/// `SET <key>:lock <id> NX EX <ttl>` against Valkey) to do the API
/// call; followers wait on the cache key with bounded polling until
/// the leader's write becomes visible, then return.
///
/// ## TOML shape
///
/// ```toml
/// [adapter.fivetran.stampede]
/// backend = "valkey"
/// valkey_url = "rediss://valkey:6379/"
/// lock_ttl_seconds = 60
/// poll_timeout_seconds = 30
/// ```
///
/// ## Fail-open
///
/// When the lock backend is unreachable the client falls through to
/// the direct HTTP path (no stampede protection, but no block).
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FivetranStampedeConfig {
    /// Which backend to instantiate. Defaults to
    /// [`FivetranStampedeBackend::None`] so a stub block with no
    /// `backend` key is well-defined.
    #[serde(default)]
    pub backend: FivetranStampedeBackend,

    /// Connection URL for `backend = "valkey"`. Standard `redis://`
    /// (plain) or `rediss://` (TLS).
    // SECURITY: may embed credentials (`redis://:password@host`). Never log
    // this value; redact it if it ever reaches an error/trace message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub valkey_url: Option<String>,

    /// Lock TTL applied to the `SET NX EX <ttl>` call. The lock
    /// auto-expires after this many seconds so a crashed leader
    /// doesn't park every follower forever. Defaults to 60s.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lock_ttl_seconds: Option<u64>,

    /// Hard cap on how long a follower will poll the cache before
    /// falling through to a direct HTTP fetch. Defaults to 30s.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub poll_timeout_seconds: Option<u64>,
}

impl FivetranStampedeConfig {
    /// Validate that the configured backend has all the fields it
    /// needs. Surfaces errors at config-load time via
    /// [`validate_fivetran_resilience`].
    pub fn validate(&self, adapter_name: &str) -> Result<(), ConfigError> {
        match self.backend {
            FivetranStampedeBackend::None => Ok(()),
            FivetranStampedeBackend::Valkey => {
                if self.valkey_url.as_deref().is_none_or(str::is_empty) {
                    Err(ConfigError::FivetranStampedeMissingField {
                        adapter: adapter_name.to_string(),
                        backend: self.backend.as_str().to_string(),
                        field: "valkey_url".into(),
                    })
                } else {
                    Ok(())
                }
            }
        }
    }
}

/// Circuit-breaker backend selector for the Fivetran adapter (Layer
/// 3). See [`FivetranCircuitBreakerConfig`] for the TOML shape.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FivetranCircuitBreakerBackend {
    /// No-op breaker; state is always `Closed`. Default when the
    /// block is absent.
    #[default]
    None,
    /// Shared per-account state machine in Valkey. Requires the
    /// `valkey` Cargo feature.
    Valkey,
}

impl FivetranCircuitBreakerBackend {
    /// Stable wire-form string for log messages and OTLP attrs.
    pub fn as_str(self) -> &'static str {
        match self {
            FivetranCircuitBreakerBackend::None => "none",
            FivetranCircuitBreakerBackend::Valkey => "valkey",
        }
    }
}

/// Circuit-breaker config for the Fivetran adapter (Layer 3).
///
/// Trips after `failure_threshold` consecutive remote failures (5xx,
/// network errors, exhausted retries) and short-circuits subsequent
/// HTTP attempts with `FivetranError::CircuitOpen` until
/// `cooldown_seconds` elapses. State transitions follow the standard
/// `Closed → Open → HalfOpen → Closed` pattern, with exponentially
/// extended cooldown on repeated half-open failures (capped at
/// `cooldown_max_seconds`).
///
/// State is shared across processes via Valkey so a Fivetran outage
/// trips one breaker for the entire org rather than each process
/// independently tripping its own.
///
/// ## TOML shape
///
/// ```toml
/// [adapter.fivetran.circuit_breaker]
/// backend = "valkey"
/// valkey_url = "rediss://valkey:6379/"
/// failure_threshold = 5
/// window_seconds = 60
/// cooldown_seconds = 300
/// cooldown_max_seconds = 3600
/// ```
///
/// ## Fail-open
///
/// When the state store is unreachable the client behaves as if the
/// breaker is `Closed` — coordination failure must not refuse live
/// traffic.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FivetranCircuitBreakerConfig {
    /// Which backend to instantiate. Defaults to
    /// [`FivetranCircuitBreakerBackend::None`] so a stub block with
    /// no `backend` key is well-defined.
    #[serde(default)]
    pub backend: FivetranCircuitBreakerBackend,

    /// Connection URL for `backend = "valkey"`. Standard `redis://`
    /// (plain) or `rediss://` (TLS).
    // SECURITY: may embed credentials (`redis://:password@host`). Never log
    // this value; redact it if it ever reaches an error/trace message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub valkey_url: Option<String>,

    /// Consecutive failures required to transition `Closed → Open`.
    /// Defaults to 5 when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_threshold: Option<u32>,

    /// Failure-counting window in seconds. Failures older than the
    /// window are not counted toward `failure_threshold`. Defaults
    /// to 60s when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window_seconds: Option<u64>,

    /// Initial cooldown (seconds) before the breaker transitions
    /// `Open → HalfOpen` for a probe. Defaults to 300s.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cooldown_seconds: Option<u64>,

    /// Upper bound on the exponentially-extended cooldown after
    /// repeated half-open failures. Defaults to 3600s.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cooldown_max_seconds: Option<u64>,
}

impl FivetranCircuitBreakerConfig {
    /// Validate that the configured backend has all the fields it
    /// needs. Surfaces errors at config-load time via
    /// [`validate_fivetran_resilience`].
    pub fn validate(&self, adapter_name: &str) -> Result<(), ConfigError> {
        match self.backend {
            FivetranCircuitBreakerBackend::None => Ok(()),
            FivetranCircuitBreakerBackend::Valkey => {
                if self.valkey_url.as_deref().is_none_or(str::is_empty) {
                    Err(ConfigError::FivetranCircuitBreakerMissingField {
                        adapter: adapter_name.to_string(),
                        backend: self.backend.as_str().to_string(),
                        field: "valkey_url".into(),
                    })
                } else {
                    Ok(())
                }
            }
        }
    }
}

/// Pipeline configuration — discriminated by the `type` field in TOML.
///
/// Defaults to [`Replication`](PipelineConfig::Replication) when `type` is
/// absent, preserving backward compatibility with existing configs.
///
/// ```toml
/// [pipeline.raw_replication]
/// # type = "replication"  # optional, this is the default
/// strategy = "incremental"
/// ...
/// ```
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PipelineConfig {
    Replication(Box<ReplicationPipelineConfig>),
    Transformation(Box<TransformationPipelineConfig>),
    Quality(Box<QualityPipelineConfig>),
    Snapshot(Box<SnapshotPipelineConfig>),
    Load(Box<LoadPipelineConfig>),
}

impl<'de> Deserialize<'de> for PipelineConfig {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error;

        // Deserialize into a generic TOML table to inspect the "type" field.
        let mut table = toml::Table::deserialize(deserializer)?;

        let pipeline_type = table
            .remove("type")
            .and_then(|v| v.as_str().map(String::from))
            .unwrap_or_else(|| "replication".into());

        let remaining = toml::Value::Table(table);

        match pipeline_type.as_str() {
            "replication" => remaining
                .try_into::<ReplicationPipelineConfig>()
                .map(|r| PipelineConfig::Replication(Box::new(r)))
                .map_err(D::Error::custom),
            "transformation" => remaining
                .try_into::<TransformationPipelineConfig>()
                .map(|t| PipelineConfig::Transformation(Box::new(t)))
                .map_err(D::Error::custom),
            "quality" => remaining
                .try_into::<QualityPipelineConfig>()
                .map(|q| PipelineConfig::Quality(Box::new(q)))
                .map_err(D::Error::custom),
            "snapshot" => remaining
                .try_into::<SnapshotPipelineConfig>()
                .map(|s| PipelineConfig::Snapshot(Box::new(s)))
                .map_err(D::Error::custom),
            "load" => remaining
                .try_into::<LoadPipelineConfig>()
                .map(|l| PipelineConfig::Load(Box::new(l)))
                .map_err(D::Error::custom),
            other => Err(D::Error::custom(format!(
                "unsupported pipeline type: '{other}'. Supported types: replication, transformation, quality, snapshot, load"
            ))),
        }
    }
}

impl JsonSchema for PipelineConfig {
    fn schema_name() -> String {
        "PipelineConfig".to_owned()
    }

    fn json_schema(generator: &mut schemars::r#gen::SchemaGenerator) -> schemars::schema::Schema {
        // Hand-written to mirror the custom `Deserialize` impl. A
        // `#[derive(JsonSchema)]` would derive from the struct shape and
        // miss two things: (a) the back-compat default that lets a
        // `[pipeline.x]` block omit `type` and parse as Replication, and
        // (b) the lowercased-on-disk discriminator value. Both are
        // empirically required: 43 of 47 committed POC pipelines omit
        // `type` (back-compat replication); the schema must accept that.
        //
        // Each arm composes the variant subschema with a `type` property
        // constraint via `allOf`. The variant structs deliberately omit
        // `serde(deny_unknown_fields)` — see the comment on
        // [`ReplicationPipelineConfig`] for the JSON Schema evaluation
        // detail that forces that choice. We use `anyOf` rather than
        // `oneOf` (which requires exactly-one match) because variant
        // shapes can overlap once `type` is omitted from the back-compat
        // arm; `anyOf` is the standard shape for tagged unions.
        use schemars::schema::{InstanceType, Schema, SchemaObject, SubschemaValidation};

        fn type_constraint(name: &'static str, required: bool) -> Schema {
            let const_schema = Schema::Object(SchemaObject {
                instance_type: Some(InstanceType::String.into()),
                enum_values: Some(vec![serde_json::Value::String(name.to_owned())]),
                ..Default::default()
            });
            let mut obj = SchemaObject {
                instance_type: Some(InstanceType::Object.into()),
                ..Default::default()
            };
            obj.object()
                .properties
                .insert("type".to_owned(), const_schema);
            if required {
                obj.object().required.insert("type".to_owned());
            }
            Schema::Object(obj)
        }

        fn arm(variant_schema: Schema, type_name: &'static str, type_required: bool) -> Schema {
            Schema::Object(SchemaObject {
                subschemas: Some(Box::new(SubschemaValidation {
                    all_of: Some(vec![
                        variant_schema,
                        type_constraint(type_name, type_required),
                    ]),
                    ..Default::default()
                })),
                ..Default::default()
            })
        }

        let replication = generator.subschema_for::<ReplicationPipelineConfig>();
        let transformation = generator.subschema_for::<TransformationPipelineConfig>();
        let quality = generator.subschema_for::<QualityPipelineConfig>();
        let snapshot = generator.subschema_for::<SnapshotPipelineConfig>();
        let load = generator.subschema_for::<LoadPipelineConfig>();

        let arms = vec![
            arm(replication, "replication", false),
            arm(transformation, "transformation", true),
            arm(quality, "quality", true),
            arm(snapshot, "snapshot", true),
            arm(load, "load", true),
        ];

        Schema::Object(SchemaObject {
            metadata: Some(Box::new(schemars::schema::Metadata {
                description: Some(
                    "Pipeline configuration. The `type` field selects one of five variants — `replication` (default when omitted), `transformation`, `quality`, `snapshot`, or `load`. Each variant has its own field set; see the per-variant subschemas in `definitions`.".to_owned(),
                ),
                ..Default::default()
            })),
            subschemas: Some(Box::new(SubschemaValidation {
                any_of: Some(arms),
                ..Default::default()
            })),
            ..Default::default()
        })
    }
}

impl PipelineConfig {
    /// Returns the pipeline type as a display string.
    pub fn pipeline_type_str(&self) -> &'static str {
        match self {
            Self::Replication(_) => "replication",
            Self::Transformation(_) => "transformation",
            Self::Quality(_) => "quality",
            Self::Snapshot(_) => "snapshot",
            Self::Load(_) => "load",
        }
    }

    /// Returns the execution config regardless of variant.
    pub fn execution(&self) -> &ExecutionConfig {
        match self {
            Self::Replication(r) => &r.execution,
            Self::Transformation(t) => &t.execution,
            Self::Quality(q) => &q.execution,
            Self::Snapshot(s) => &s.execution,
            Self::Load(l) => &l.execution,
        }
    }

    /// Returns the checks config regardless of variant.
    pub fn checks(&self) -> &ChecksConfig {
        match self {
            Self::Replication(r) => &r.checks,
            Self::Transformation(t) => &t.checks,
            Self::Quality(q) => &q.checks,
            Self::Snapshot(s) => &s.checks,
            Self::Load(l) => &l.checks,
        }
    }

    /// The check kinds the `rocky run` runner for this pipeline type actually
    /// executes — the single source of truth shared by the `rocky validate`
    /// inert-config lint and the `rocky discover` check-name projection.
    ///
    /// Keep this in lockstep with the runners in `rocky-cli`: `commands/run.rs`
    /// (replication) and `commands/run_local.rs` (`run_quality`).
    /// Transformation, snapshot, and load pipelines run no pipeline-level
    /// checks today, so they report an empty set.
    pub fn executed_check_kinds(&self) -> &'static [crate::checks::CheckKind] {
        use crate::checks::CheckKind::{Assertions, Custom, RowCount};
        match self {
            Self::Replication(_) => ReplicationPipelineConfig::EXECUTED_CHECK_KINDS,
            Self::Quality(_) => &[RowCount, Custom, Assertions],
            Self::Transformation(_) | Self::Snapshot(_) | Self::Load(_) => &[],
        }
    }

    /// Returns the target adapter name regardless of variant.
    pub fn target_adapter(&self) -> &str {
        match self {
            Self::Replication(r) => &r.target.adapter,
            Self::Transformation(t) => &t.target.adapter,
            Self::Quality(q) => &q.target.adapter,
            Self::Snapshot(s) => &s.target.adapter,
            Self::Load(l) => &l.target.adapter,
        }
    }

    /// Returns the `depends_on` list for pipeline chaining.
    pub fn depends_on(&self) -> &[String] {
        match self {
            Self::Replication(r) => &r.depends_on,
            Self::Transformation(t) => &t.depends_on,
            Self::Quality(q) => &q.depends_on,
            Self::Snapshot(s) => &s.depends_on,
            Self::Load(l) => &l.depends_on,
        }
    }

    /// Returns the optional `[schedule]` block for native demand
    /// reconciliation, or `None` when the pipeline declares no schedule.
    pub fn schedule(&self) -> Option<&ScheduleConfig> {
        match self {
            Self::Replication(r) => r.schedule.as_ref(),
            Self::Transformation(t) => t.schedule.as_ref(),
            Self::Quality(q) => q.schedule.as_ref(),
            Self::Snapshot(s) => s.schedule.as_ref(),
            Self::Load(l) => l.schedule.as_ref(),
        }
    }

    /// Unwraps the replication config, returning `None` for other types.
    pub fn as_replication(&self) -> Option<&ReplicationPipelineConfig> {
        match self {
            Self::Replication(r) => Some(r.as_ref()),
            _ => None,
        }
    }

    /// Unwraps the transformation config, returning `None` for other types.
    pub fn as_transformation(&self) -> Option<&TransformationPipelineConfig> {
        match self {
            Self::Transformation(t) => Some(t.as_ref()),
            _ => None,
        }
    }

    /// Unwraps the quality config, returning `None` for other types.
    pub fn as_quality(&self) -> Option<&QualityPipelineConfig> {
        match self {
            Self::Quality(q) => Some(q.as_ref()),
            _ => None,
        }
    }

    /// Unwraps the snapshot config, returning `None` for other types.
    pub fn as_snapshot(&self) -> Option<&SnapshotPipelineConfig> {
        match self {
            Self::Snapshot(s) => Some(s.as_ref()),
            _ => None,
        }
    }

    /// Unwraps the load config, returning `None` for other types.
    pub fn as_load(&self) -> Option<&LoadPipelineConfig> {
        match self {
            Self::Load(l) => Some(l.as_ref()),
            _ => None,
        }
    }
}

/// Per-pipeline schedule declaration for native demand reconciliation.
///
/// Every field is optional; an absent `[pipeline.<name>.schedule]` block leaves
/// the pipeline with today's behavior (it runs only when invoked directly). A
/// pipeline may combine demand sources — `cron`, `after`, and `freshness` union,
/// so any one of them makes the pipeline due.
///
/// Unlike the five pipeline variant structs (which deliberately omit
/// `deny_unknown_fields` so the IDE schema can inject the `type` discriminator),
/// this struct **denies unknown fields**: a typo inside `[…schedule]` is a
/// silent scheduling bug — a misspelled `corn` key would leave a pipeline
/// unscheduled with no error — so the deserializer rejects it outright.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ScheduleConfig {
    /// Standard 5-field cron expression (minute hour day-of-month month
    /// day-of-week). When set, the pipeline is due at each occurrence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cron: Option<String>,

    /// IANA timezone name (e.g. `"Europe/Lisbon"`) the `cron` occurrences are
    /// evaluated in. Falls back to the project `[schedule].timezone`, which
    /// itself defaults to `"UTC"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timezone: Option<String>,

    /// Upstream pipelines this one runs after. The pipeline is due once every
    /// listed pipeline has a successful run that completed after this
    /// pipeline's own latest success started (a partial-success run does not
    /// count as an upstream success).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub after: Vec<String>,

    /// When `true`, the pipeline is due once its own run-staleness exceeds its
    /// freshness budget (resolved from per-model `max_lag_seconds`, falling
    /// back to the project `[freshness].expected_lag_seconds`). This is
    /// run-staleness only — the reconciler never queries the warehouse.
    #[serde(default)]
    pub freshness: bool,

    /// Catch-up policy when more than one cron occurrence elapsed since the
    /// last fire: `"latest"` (default) fires one demand at the most recent
    /// missed occurrence; `"skip"` advances the anchor and runs nothing.
    /// `"all"` is rejected at validation — runs are watermark-driven, not
    /// windowed, so replaying every missed occurrence is pure cost.
    #[serde(default = "default_catchup")]
    pub catchup: String,

    /// In-tick immediate re-submissions on failure. The reconciler never
    /// sleeps between attempts; minutes-scale spacing is the always-on
    /// cross-tick throttle, not this knob. Default `0` (off).
    #[serde(default)]
    pub retry: ScheduleRetryConfig,

    /// Scheduler-level timeout in minutes for a launched run. `0` (default)
    /// means no scheduler-level timeout — the run's own limits apply.
    #[serde(default)]
    pub timeout_minutes: u64,

    /// When `false`, demand is suppressed but the config is kept. Default
    /// `true`.
    #[serde(default = "default_true_config")]
    pub enabled: bool,
}

/// In-tick retry budget for a scheduled pipeline.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ScheduleRetryConfig {
    /// Maximum number of *additional* immediate re-submissions after the first
    /// attempt fails. `0` means the first attempt is the only one.
    #[serde(default)]
    pub max: u32,
}

/// Project-level schedule defaults (`[schedule]`).
///
/// Supplies the fallback timezone for per-pipeline `cron` blocks that omit
/// their own, plus the resident-loop poll cadence consumed by a later phase.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ScheduleDefaultsConfig {
    /// Default IANA timezone for per-pipeline cron evaluation. Default `"UTC"`.
    #[serde(default = "default_schedule_timezone")]
    pub timezone: String,

    /// Poll cadence in seconds for the resident scheduler loop. Not consumed
    /// by the one-shot tick; reserved for the resident-loop phase. Default
    /// `15`.
    #[serde(default = "default_poll_interval_seconds")]
    pub poll_interval_seconds: u64,
}

impl Default for ScheduleDefaultsConfig {
    fn default() -> Self {
        Self {
            timezone: default_schedule_timezone(),
            poll_interval_seconds: default_poll_interval_seconds(),
        }
    }
}

fn default_catchup() -> String {
    "latest".to_string()
}

fn default_schedule_timezone() -> String {
    "UTC".to_string()
}

fn default_poll_interval_seconds() -> u64 {
    15
}

/// Replication pipeline configuration.
///
/// Copies tables from a source to a target using schema pattern discovery,
/// with optional incremental strategy, metadata columns, governance, and
/// data quality checks.
// `deny_unknown_fields` is intentionally absent from the 5 pipeline variant
// structs: the IDE schema's `PipelineConfig::json_schema` impl injects the
// `type` discriminator via an `allOf` branch, which under JSON Schema's
// Draft-07 evaluation model is rejected by the variant struct's
// `additionalProperties: false`. The deserializer for `PipelineConfig`
// strips `type` before delegating to the variant — runtime parsing is
// unaffected — but the IDE validates raw TOML, so the schema must permit
// `type` as an explicit property without falling back to declaring it on
// every variant struct. Net: pipeline-block typos slip past the IDE
// schema; everything outside the pipeline section stays strict.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ReplicationPipelineConfig {
    /// Replication strategy. Accepted values:
    ///
    /// - `"incremental"` — append rows past the source-side watermark.
    /// - `"full_refresh"` — `CREATE OR REPLACE TABLE … AS SELECT …`.
    /// - `"merge"` — upserts the watermarked delta into the target via
    ///   `MERGE INTO … USING (delta) ON merge_keys WHEN MATCHED UPDATE SET *
    ///   WHEN NOT MATCHED INSERT *`. Requires `merge_keys` (or
    ///   `merge_keys_fallback`) to be set.
    /// - `"view"` — emits a `CREATE OR REPLACE VIEW` over the source. Every
    ///   read against the target re-runs the SELECT; no row movement.
    /// - `"materialized_view"` — warehouse-managed materialized view.
    ///   Supported on Databricks, Snowflake, and BigQuery; DuckDB/Trino
    ///   surface an unsupported-dialect error.
    /// - `"dynamic_table"` — Snowflake-only. Not configurable at the
    ///   pipeline level today (the strategy needs a `target_lag` specifier
    ///   that the pipeline block doesn't expose); declare it on a
    ///   transformation model's sidecar TOML instead.
    #[serde(default = "default_strategy")]
    pub strategy: String,

    /// Timestamp column for incremental strategy.
    #[serde(default = "default_timestamp_column")]
    pub timestamp_column: String,

    /// Unique-key columns for `strategy = "merge"`. The `MERGE` statement
    /// joins source rows onto the target on these columns; matched rows are
    /// updated, unmatched rows are inserted.
    ///
    /// Required when `strategy = "merge"` and `merge_keys_fallback` is
    /// absent. Ignored for other strategies.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub merge_keys: Option<Vec<String>>,

    /// Fallback unique-key columns used when `merge_keys` is not configured.
    /// Provides a single source of merge-key defaults for callers that
    /// derive keys from another source (e.g. the discovery adapter) but
    /// still want pipeline-level control.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub merge_keys_fallback: Option<Vec<String>>,

    /// Metadata columns added during replication.
    #[serde(default)]
    pub metadata_columns: Vec<MetadataColumnConfig>,

    /// Source configuration.
    pub source: PipelineSourceConfig,

    /// Target configuration.
    pub target: PipelineTargetConfig,

    /// Data quality checks.
    #[serde(default)]
    pub checks: ChecksConfig,

    /// Execution settings (concurrency, retries, etc.).
    #[serde(default)]
    pub execution: ExecutionConfig,

    /// Pipeline dependencies for chaining.
    #[serde(default)]
    pub depends_on: Vec<String>,

    /// Optional native-schedule declaration. See [`ScheduleConfig`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule: Option<ScheduleConfig>,

    /// Per-`(connector, table)` overrides applied on top of the pipeline
    /// defaults. Each rule matches against a discovered connector +
    /// table pair and, when matched, replaces selected pipeline-level
    /// fields with override-supplied values.
    ///
    /// Resolution is **per-field most-specific-match-wins**: for each
    /// overrideable field, the most specific matching rule that
    /// explicitly sets that field wins; less-specific rules contribute
    /// values only for fields they actually set. Specificity ranking
    /// (highest to lowest):
    ///
    /// 1. Both `match.connector` AND `match.table` set, with
    ///    `match.table` a literal (no glob).
    /// 2. Both set, with `match.table` containing a `*` or `?` glob.
    /// 3. Only `match.connector` set.
    /// 4. Only `match.table` set.
    ///
    /// Ties at the same specificity tier for a `(connector, table)`
    /// pair are rejected at parse time as ambiguous.
    ///
    /// Empty by default — projects that don't need per-table tweaking
    /// pay nothing in JSON output, schema surface area, or runtime
    /// cost.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub table_overrides: Vec<TableOverride>,

    /// When `true`, the replication runner skips ("prunes") any table whose
    /// source is provably unchanged since the last successful copy — detected
    /// via the adapter's `source_change_marker` (a `DESCRIBE DETAIL`-derived
    /// marker on Databricks; adapters without a cheap change signal always
    /// copy). A pruned table runs no copy and no data checks: the runner emits
    /// no materialization and records it under `excluded_tables` with reason
    /// `"unchanged_since_last_copy"`.
    ///
    /// Downstream continuity — and preserving the table's prior check results —
    /// is then the orchestrator's job. The Dagster integration treats a pruned,
    /// unmaterialized key as unchanged via `satisfy_empty_outputs`; enabling
    /// pruning without an orchestrator that handles unmaterialized keys drops
    /// the table from the run.
    ///
    /// Defaults to `false` — opt in per pipeline, since silently skipping
    /// copies is a behavior change. The marker is compared against the
    /// target's recorded last-copied value (never wall-clock), so a failed
    /// prior run cannot cause a false skip. Pass `--no-prune` to `rocky run`
    /// to force a full pass (e.g. after a manual target-side mutation).
    #[serde(default)]
    pub prune_unchanged: bool,
}

/// Backward-compatible alias for [`ReplicationPipelineConfig`].
pub type PipelineConfigV2 = ReplicationPipelineConfig;

/// A per-`(connector, table)` override rule on a replication pipeline.
///
/// Authored as one TOML array entry under
/// `[[pipeline.<name>.table_overrides]]`. Each field except `match_` is
/// optional — `None` means "inherit from the pipeline-level default."
/// The resolver applies overrides with per-field most-specific-wins
/// semantics; see [`ReplicationPipelineConfig::table_overrides`] for the
/// ranking.
///
/// # Example
///
/// ```toml
/// [[pipeline.raw.table_overrides]]
/// match.connector = "stripe_main"
/// match.table     = "pii_users"
/// merge_keys      = ["user_id", "tenant_id"]
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TableOverride {
    /// Match criteria — at least one of `connector` or `table` must be
    /// set. A rule with neither (or a `match` block omitted entirely)
    /// is rejected at parse time (use pipeline-level fields to change
    /// defaults for all tables). `default` lets serde deserialize an
    /// override missing the whole `match` block; the validator catches
    /// the resulting empty match.
    #[serde(rename = "match", default)]
    pub match_: TableMatch,

    /// Override for [`ReplicationPipelineConfig::strategy`]. `None`
    /// inherits the pipeline default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub strategy: Option<String>,

    /// Override for [`ReplicationPipelineConfig::merge_keys`]. `None`
    /// inherits the pipeline default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub merge_keys: Option<Vec<String>>,

    /// Override for
    /// [`ReplicationPipelineConfig::merge_keys_fallback`]. `None`
    /// inherits the pipeline default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub merge_keys_fallback: Option<Vec<String>>,

    /// Override for [`ReplicationPipelineConfig::timestamp_column`].
    /// `None` inherits the pipeline default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp_column: Option<String>,

    /// When `Some(false)`, the matched `(connector, table)` pairs are
    /// dropped from the run and surfaced under
    /// `RunOutput.excluded_tables` with
    /// `reason = "table_override_disabled"`. Net-new field with no
    /// pipeline-level counterpart — disabling a whole pipeline is
    /// already covered by removing the pipeline block.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
}

/// Match criteria for a [`TableOverride`].
///
/// At least one of `connector` or `table` must be set (`None` for both
/// is rejected at parse time as redundant — use pipeline-level fields
/// to change defaults globally).
///
/// `connector` matches against either
/// [`crate::source::DiscoveredConnector::id`] (the stable
/// adapter-side identifier — e.g. a Fivetran connector_id) **or**
/// [`crate::source::DiscoveredConnector::schema`] (the human-meaningful
/// source schema name — e.g. `src__acme__shopify`). String equality on
/// either match wins.
///
/// `table` matches against
/// [`crate::source::DiscoveredTable::name`]. Supports `*` (zero or more
/// characters) and `?` (exactly one character) glob wildcards; presence
/// of either character switches the value from literal to glob.
///
/// # Reserved
///
/// `table` and `id` are reserved as schema-pattern component names —
/// configs that use either as a component in
/// [`SchemaPatternConfig::components`] are rejected at parse time to
/// avoid colliding with `--filter table=` and `--filter id=`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TableMatch {
    /// Connector match — equals either `DiscoveredConnector.id` or
    /// `DiscoveredConnector.schema`. `None` matches every connector.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connector: Option<String>,

    /// Table-name match. Literal when no `*`/`?`; glob otherwise.
    /// `None` matches every table.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
}

/// The set of fields a [`TableOverride`] can change, after the
/// per-field most-specific-match-wins resolver has merged the matching
/// rules for a given `(connector, table)` pair.
///
/// Fields that no matching rule set remain `None` so callers can
/// distinguish "use pipeline default" from "explicitly cleared."
/// Construct via [`resolve_table_override`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResolvedTableOverride {
    pub strategy: Option<String>,
    pub merge_keys: Option<Vec<String>>,
    pub merge_keys_fallback: Option<Vec<String>>,
    pub timestamp_column: Option<String>,
    pub enabled: Option<bool>,
}

impl ResolvedTableOverride {
    /// Returns `true` when this override carries no field overrides at
    /// all — equivalent to "no rule matched."
    pub fn is_empty(&self) -> bool {
        self.strategy.is_none()
            && self.merge_keys.is_none()
            && self.merge_keys_fallback.is_none()
            && self.timestamp_column.is_none()
            && self.enabled.is_none()
    }
}

/// Specificity tier of a [`TableMatch`] for a given `(connector,
/// table)` pair, used by [`resolve_table_override`] to break per-field
/// ties.
///
/// Variants are ordered from most-specific to least-specific so the
/// resolver can sort by `(rule_tier, declaration_order)` and pick the
/// first rule that sets each field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum MatchSpecificity {
    /// Both `connector` and `table` set, with a literal table.
    BothLiteralTable = 0,
    /// Both `connector` and `table` set, with a glob table.
    BothGlobTable = 1,
    /// Only `connector` set.
    ConnectorOnly = 2,
    /// Only `table` set.
    TableOnly = 3,
}

impl TableMatch {
    /// Returns `true` when the rule's connector half (if any) matches
    /// the given discovered connector. A rule with no `connector`
    /// constraint matches everything.
    ///
    /// `connector_schema` is `DiscoveredConnector.schema`. Both the id
    /// and schema axes are checked — OR semantics — so users can write
    /// `match.connector = "stripe_main"` against either identity.
    pub fn matches_connector(&self, connector_id: &str, connector_schema: &str) -> bool {
        let Some(pattern) = self.connector.as_deref() else {
            return true;
        };
        pattern == connector_id || pattern == connector_schema
    }

    /// Returns `true` when the rule's table half (if any) matches the
    /// given discovered table name. A rule with no `table` constraint
    /// matches every table; a value containing `*` or `?` is treated
    /// as a glob, otherwise literal equality.
    pub fn matches_table(&self, table_name: &str) -> bool {
        let Some(pattern) = self.table.as_deref() else {
            return true;
        };
        if pattern_is_glob(pattern) {
            glob_match(pattern, table_name)
        } else {
            pattern == table_name
        }
    }

    /// Returns `true` when both halves match. The full match predicate
    /// used by [`resolve_table_override`].
    pub fn matches(&self, connector_id: &str, connector_schema: &str, table_name: &str) -> bool {
        self.matches_connector(connector_id, connector_schema) && self.matches_table(table_name)
    }

    /// Specificity tier — see [`MatchSpecificity`]. Panics if both
    /// halves are `None` (parse-time validation must reject that case
    /// via [`validate_replication_overrides`]).
    fn specificity(&self) -> MatchSpecificity {
        match (self.connector.as_deref(), self.table.as_deref()) {
            (Some(_), Some(t)) if pattern_is_glob(t) => MatchSpecificity::BothGlobTable,
            (Some(_), Some(_)) => MatchSpecificity::BothLiteralTable,
            (Some(_), None) => MatchSpecificity::ConnectorOnly,
            (None, Some(_)) => MatchSpecificity::TableOnly,
            (None, None) => unreachable!(
                "empty TableMatch should be rejected by validate_replication_overrides"
            ),
        }
    }
}

/// Returns `true` when `pattern` contains a glob metacharacter
/// (`*` or `?`).
fn pattern_is_glob(pattern: &str) -> bool {
    pattern.contains('*') || pattern.contains('?')
}

/// Hand-rolled `*`/`?` glob matcher. `*` matches zero or more
/// characters; `?` matches exactly one. No character classes, no
/// alternation. Returns `true` when `pattern` matches `text` against
/// the entire input.
///
/// Implementation is a straightforward backtracking matcher over byte
/// slices. Table names are ASCII (warehouse identifier conventions),
/// so byte-level comparison is correct.
pub(crate) fn glob_match(pattern: &str, text: &str) -> bool {
    let pat = pattern.as_bytes();
    let txt = text.as_bytes();
    let mut pi = 0usize;
    let mut ti = 0usize;
    // Snapshots used to back up after a failed `*` consumption.
    let mut star_pi: Option<usize> = None;
    let mut star_ti: usize = 0;

    while ti < txt.len() {
        if pi < pat.len() && pat[pi] == b'*' {
            star_pi = Some(pi);
            star_ti = ti;
            pi += 1;
        } else if pi < pat.len() && (pat[pi] == b'?' || pat[pi] == txt[ti]) {
            pi += 1;
            ti += 1;
        } else if let Some(sp) = star_pi {
            pi = sp + 1;
            star_ti += 1;
            ti = star_ti;
        } else {
            return false;
        }
    }

    // Consume any trailing `*`s.
    while pi < pat.len() && pat[pi] == b'*' {
        pi += 1;
    }

    pi == pat.len()
}

/// Validates a glob pattern. Today the only invalid forms are an empty
/// pattern (`""`) — every other byte sequence is a valid `*`/`?` glob.
/// Returns the offending pattern string when invalid.
fn validate_glob_pattern(pattern: &str) -> Result<(), String> {
    if pattern.is_empty() {
        return Err("empty glob pattern".to_string());
    }
    Ok(())
}

/// Resolves the effective per-field override for a `(connector_id,
/// connector_schema, table_name)` triple against an ordered list of
/// [`TableOverride`] rules.
///
/// Walks every matching rule in increasing specificity order
/// (most-specific first; ties broken by declaration order). For each
/// overrideable field, the first rule that explicitly sets it wins;
/// less-specific rules contribute values only for fields they actually
/// set. Returns an empty [`ResolvedTableOverride`] when no rule
/// matches.
///
/// # Example
///
/// Given:
///
/// ```toml
/// [[pipeline.raw.table_overrides]]
/// match.connector  = "stripe_main"
/// timestamp_column = "_stripe_synced"
///
/// [[pipeline.raw.table_overrides]]
/// match.connector = "stripe_main"
/// match.table     = "pii_users"
/// merge_keys      = ["user_id", "tenant_id"]
/// ```
///
/// For `("conn_xyz", "stripe_main", "pii_users")` the resolver
/// returns `merge_keys = ["user_id", "tenant_id"]` (from the
/// more-specific rule) **and** `timestamp_column = "_stripe_synced"`
/// (from the less-specific rule). Both are applied — per-field, not
/// whole-rule.
pub fn resolve_table_override(
    rules: &[TableOverride],
    connector_id: &str,
    connector_schema: &str,
    table_name: &str,
) -> ResolvedTableOverride {
    // Collect matching rules paired with their specificity tier.
    let mut matched: Vec<(MatchSpecificity, usize, &TableOverride)> = rules
        .iter()
        .enumerate()
        .filter(|(_, rule)| {
            rule.match_
                .matches(connector_id, connector_schema, table_name)
        })
        .map(|(idx, rule)| (rule.match_.specificity(), idx, rule))
        .collect();

    // Sort by (specificity, declaration index) — most specific first.
    matched.sort_by_key(|(spec, idx, _)| (*spec, *idx));

    let mut resolved = ResolvedTableOverride::default();
    for (_, _, rule) in &matched {
        if resolved.strategy.is_none() && rule.strategy.is_some() {
            resolved.strategy = rule.strategy.clone();
        }
        if resolved.merge_keys.is_none() && rule.merge_keys.is_some() {
            resolved.merge_keys = rule.merge_keys.clone();
        }
        if resolved.merge_keys_fallback.is_none() && rule.merge_keys_fallback.is_some() {
            resolved.merge_keys_fallback = rule.merge_keys_fallback.clone();
        }
        if resolved.timestamp_column.is_none() && rule.timestamp_column.is_some() {
            resolved.timestamp_column = rule.timestamp_column.clone();
        }
        if resolved.enabled.is_none() && rule.enabled.is_some() {
            resolved.enabled = rule.enabled;
        }
    }
    resolved
}

/// Transformation pipeline configuration.
///
/// Orchestrates `.sql` / `.rocky` model compilation and execution as a
/// first-class pipeline, with its own execution, checks, and governance
/// settings. Model-level strategy (incremental, merge, time_interval, etc.)
/// is defined in each model's sidecar TOML, not at the pipeline level.
///
/// ```toml
/// [pipeline.silver]
/// type = "transformation"
/// models = "models/**"
///
/// [pipeline.silver.target]
/// adapter = "databricks_prod"
/// [pipeline.silver.target.governance]
/// auto_create_schemas = true
///
/// [pipeline.silver.execution]
/// concurrency = 8
/// ```
// See note on [`ReplicationPipelineConfig`] for why no `deny_unknown_fields`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct TransformationPipelineConfig {
    /// Glob pattern for model files, relative to the config file directory.
    /// Default: `"models/**"`.
    #[serde(default = "default_models_glob")]
    pub models: String,

    /// Target configuration (adapter + governance).
    pub target: TransformationTargetConfig,

    /// Data quality checks run after model execution.
    #[serde(default)]
    pub checks: ChecksConfig,

    /// Execution settings (concurrency, retries, etc.).
    #[serde(default)]
    pub execution: ExecutionConfig,

    /// Pipeline dependencies for chaining.
    #[serde(default)]
    pub depends_on: Vec<String>,

    /// Optional native-schedule declaration. See [`ScheduleConfig`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule: Option<ScheduleConfig>,
}

fn default_models_glob() -> String {
    "models/**".to_string()
}

/// Target configuration for transformation pipelines.
///
/// Unlike replication targets (which use `catalog_template` / `schema_template`
/// for dynamic routing), transformation targets only need an adapter reference
/// and optional governance — the actual catalog/schema/table is defined per-model
/// in sidecar TOML files.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TransformationTargetConfig {
    /// Name of the adapter to use (references a key in `[adapter.*]`).
    /// Defaults to `"default"`.
    #[serde(default = "default_adapter_name")]
    pub adapter: String,

    /// Governance settings for the target.
    #[serde(default)]
    pub governance: GovernanceConfig,
}

// ---- Quality pipeline ----

/// Quality pipeline configuration — standalone data quality checks.
///
/// Runs checks against existing tables without any data movement.
///
/// ```toml
/// [pipeline.nightly_dq]
/// type = "quality"
///
/// [pipeline.nightly_dq.target]
/// adapter = "databricks_prod"
///
/// [[pipeline.nightly_dq.tables]]
/// catalog = "acme_warehouse"
/// schema = "raw__us_west__shopify"
///
/// [pipeline.nightly_dq.checks]
/// enabled = true
/// freshness = { threshold_seconds = 86400 }
/// ```
// See note on [`ReplicationPipelineConfig`] for why no `deny_unknown_fields`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct QualityPipelineConfig {
    /// Target adapter for running check queries.
    pub target: QualityTargetConfig,

    /// Tables to check. Each entry specifies catalog + schema, and
    /// optionally a specific table (omit for all tables in the schema).
    #[serde(default)]
    pub tables: Vec<TableRef>,

    /// Data quality checks to run.
    pub checks: ChecksConfig,

    /// Execution settings (concurrency, retries, etc.).
    #[serde(default)]
    pub execution: ExecutionConfig,

    /// Pipeline dependencies for chaining.
    #[serde(default)]
    pub depends_on: Vec<String>,

    /// Optional native-schedule declaration. See [`ScheduleConfig`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule: Option<ScheduleConfig>,
}

/// Target configuration for quality pipelines (adapter reference only).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct QualityTargetConfig {
    /// Name of the adapter to use (references a key in `[adapter.*]`).
    #[serde(default = "default_adapter_name")]
    pub adapter: String,
}

/// A reference to a specific catalog/schema/table for quality checks
/// and snapshot pipelines.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TableRef {
    pub catalog: String,
    pub schema: String,
    /// Specific table name. When `None`, all tables in the schema are checked.
    #[serde(default)]
    pub table: Option<String>,
}

// ---- Snapshot pipeline (SCD Type 2) ----

/// Snapshot pipeline configuration — SCD Type 2 slowly-changing dimension capture.
///
/// Tracks historical changes to a source table by maintaining `valid_from` /
/// `valid_to` columns in the target history table.
///
/// ```toml
/// [pipeline.customer_history]
/// type = "snapshot"
/// unique_key = ["customer_id"]
/// updated_at = "updated_at"
///
/// [pipeline.customer_history.source]
/// adapter = "databricks_prod"
/// catalog = "raw_catalog"
/// schema = "raw__us_west__shopify"
/// table = "customers"
///
/// [pipeline.customer_history.target]
/// adapter = "databricks_prod"
/// catalog = "acme_warehouse"
/// schema = "silver__scd"
/// table = "customers_history"
/// ```
// See note on [`ReplicationPipelineConfig`] for why no `deny_unknown_fields`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SnapshotPipelineConfig {
    /// Column(s) that uniquely identify a row in the source table.
    pub unique_key: Vec<String>,

    /// Column used to detect changes (compared between runs).
    pub updated_at: String,

    /// When true, rows deleted from the source get their `valid_to` set
    /// to the current timestamp in the target. Default: false.
    #[serde(default)]
    pub invalidate_hard_deletes: bool,

    /// Source table reference (single table, not pattern-based discovery).
    pub source: SnapshotSourceConfig,

    /// Target history table.
    pub target: SnapshotTargetConfig,

    /// Data quality checks run after snapshot.
    #[serde(default)]
    pub checks: ChecksConfig,

    /// Execution settings.
    #[serde(default)]
    pub execution: ExecutionConfig,

    /// Pipeline dependencies for chaining.
    #[serde(default)]
    pub depends_on: Vec<String>,

    /// Optional native-schedule declaration. See [`ScheduleConfig`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule: Option<ScheduleConfig>,
}

/// Source table for a snapshot pipeline (explicit single-table reference).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SnapshotSourceConfig {
    /// Name of the adapter to use (references a key in `[adapter.*]`).
    #[serde(default = "default_adapter_name")]
    pub adapter: String,
    pub catalog: String,
    pub schema: String,
    pub table: String,
}

/// Target table for a snapshot pipeline (explicit single-table reference + governance).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SnapshotTargetConfig {
    /// Name of the adapter to use (references a key in `[adapter.*]`).
    #[serde(default = "default_adapter_name")]
    pub adapter: String,
    pub catalog: String,
    pub schema: String,
    pub table: String,
    #[serde(default)]
    pub governance: GovernanceConfig,
}

// ---- Load pipeline (file ingestion) ----

/// Load pipeline configuration -- ingest files (CSV, Parquet, JSONL) into a warehouse.
///
/// Loads data from a local directory into a target catalog/schema. The format
/// can be auto-detected from file extensions or set explicitly.
///
/// ```toml
/// [pipeline.load_data]
/// type = "load"
/// source_dir = "data/"
/// format = "csv"
///
/// [pipeline.load_data.target]
/// adapter = "prod"
/// catalog = "warehouse"
/// schema = "raw"
///
/// [pipeline.load_data.options]
/// batch_size = 5000
/// create_table = true
/// truncate_first = false
/// csv_delimiter = ","
/// csv_has_header = true
/// ```
// See note on [`ReplicationPipelineConfig`] for why no `deny_unknown_fields`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct LoadPipelineConfig {
    /// Directory or glob pattern for source files, relative to the config file.
    pub source_dir: String,

    /// Explicit file format. When omitted, auto-detected from file extensions.
    #[serde(default)]
    pub format: Option<LoadFileFormat>,

    /// Target table location.
    pub target: LoadTargetConfig,

    /// Load options (batch size, create/truncate behavior, CSV settings).
    #[serde(default)]
    pub options: LoadOptionsConfig,

    /// Optional data contract that gates the load. When set, each file is
    /// loaded into a staging table, validated against the contract, and
    /// promoted to the target only if validation passes. On failure the
    /// staging table is dropped and the target is left untouched.
    #[serde(default)]
    pub contract: Option<crate::contracts::ContractConfig>,

    /// Data quality checks run after loading.
    #[serde(default)]
    pub checks: ChecksConfig,

    /// Execution settings (concurrency, retries, etc.).
    #[serde(default)]
    pub execution: ExecutionConfig,

    /// Pipeline dependencies for chaining.
    #[serde(default)]
    pub depends_on: Vec<String>,

    /// Optional native-schedule declaration. See [`ScheduleConfig`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule: Option<ScheduleConfig>,
}

/// File format for load pipelines, parsed from TOML.
///
/// Mirrors `rocky_adapter_sdk::FileFormat` but lives in rocky-core to
/// avoid a hard dependency from config parsing to the adapter SDK.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum LoadFileFormat {
    Csv,
    Parquet,
    #[serde(alias = "json_lines")]
    JsonLines,
}

impl std::fmt::Display for LoadFileFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Csv => write!(f, "csv"),
            Self::Parquet => write!(f, "parquet"),
            Self::JsonLines => write!(f, "json_lines"),
        }
    }
}

/// Target configuration for load pipelines.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct LoadTargetConfig {
    /// Name of the adapter to use (references a key in `[adapter.*]`).
    #[serde(default = "default_adapter_name")]
    pub adapter: String,

    /// Target catalog name.
    pub catalog: String,

    /// Target schema name.
    pub schema: String,

    /// Optional explicit table name. When omitted, derives from the file name
    /// (e.g., `orders.csv` -> table `orders`).
    #[serde(default)]
    pub table: Option<String>,

    /// Governance settings for the target.
    #[serde(default)]
    pub governance: GovernanceConfig,
}

/// Load-specific options parsed from TOML.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct LoadOptionsConfig {
    /// Number of rows per INSERT batch. Default: 10,000.
    #[serde(default = "default_load_batch_size")]
    pub batch_size: usize,

    /// Create the target table if it does not exist. Default: true.
    #[serde(default = "default_true_config")]
    pub create_table: bool,

    /// Truncate the target table before loading. Default: false.
    #[serde(default)]
    pub truncate_first: bool,

    /// CSV-specific: field delimiter character. Default: `,`.
    #[serde(default = "default_csv_delimiter_config")]
    pub csv_delimiter: String,

    /// CSV-specific: whether the first row is a header. Default: true.
    #[serde(default = "default_true_config")]
    pub csv_has_header: bool,
}

fn default_load_batch_size() -> usize {
    10_000
}

fn default_true_config() -> bool {
    true
}

fn default_csv_delimiter_config() -> String {
    ",".to_string()
}

impl Default for LoadOptionsConfig {
    fn default() -> Self {
        Self {
            batch_size: default_load_batch_size(),
            create_table: true,
            truncate_first: false,
            csv_delimiter: default_csv_delimiter_config(),
            csv_has_header: true,
        }
    }
}

fn default_adapter_name() -> String {
    "default".to_string()
}

fn default_strategy() -> String {
    "incremental".to_string()
}

/// Default watermark column used by Fivetran-sourced pipelines.
pub const DEFAULT_TIMESTAMP_COLUMN: &str = "_fivetran_synced";

/// Common watermark column names used in compaction dedup detection.
pub const WATERMARK_COLUMNS: &[&str] = &[DEFAULT_TIMESTAMP_COLUMN, "_loaded_at", "_synced_at"];

fn default_timestamp_column() -> String {
    DEFAULT_TIMESTAMP_COLUMN.to_string()
}

/// Pipeline source configuration.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PipelineSourceConfig {
    /// Name of the adapter to use (references a key in `[adapter.*]`).
    /// Defaults to `"default"` — resolved against the adapter map in
    /// [`normalize_rocky_config`].
    #[serde(default = "default_adapter_name")]
    pub adapter: String,

    /// Source catalog name.
    pub catalog: Option<String>,

    /// Schema pattern for parsing source schema names.
    pub schema_pattern: SchemaPatternConfig,

    /// Optional discovery configuration.
    #[serde(default)]
    pub discovery: Option<DiscoveryConfig>,
}

/// Action when `rocky discover` detects a cross-source collision — the same
/// external object id mapped to more than one target path.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum OnCollision {
    /// Collision detection disabled (default — fully backwards compatible).
    #[default]
    Off,
    /// Report collisions in `collision_candidates` and emit an event, but do
    /// not fail the discover.
    Warn,
    /// Report collisions and fail the discover, so a colliding onboard cannot
    /// silently create a catalog/table.
    Error,
}

/// Discovery configuration within a pipeline source.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DiscoveryConfig {
    /// Name of the adapter to use for discovery (references a key in `[adapter.*]`).
    /// Defaults to `"default"`.
    #[serde(default = "default_adapter_name")]
    pub adapter: String,

    /// When `true`, `rocky discover` diffs the discovered source inventory
    /// against the prior persisted snapshot and reports first-seen sources in
    /// `new_sources`. Off by default — the diff and its state write only happen
    /// when opted in, so existing projects pay nothing.
    #[serde(default)]
    pub report_new_sources: bool,

    /// What to do when discover finds the same external object id mapped to
    /// more than one target path (likely the same object onboarded twice).
    /// `off` (default) skips detection entirely; `warn` reports
    /// `collision_candidates` + emits an event; `error` additionally fails the
    /// discover. Only adapters that supply `external_object_id` (e.g. Fivetran)
    /// participate; others are silently skipped.
    #[serde(default)]
    pub on_collision: OnCollision,
}

/// Pipeline target configuration.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PipelineTargetConfig {
    /// Name of the adapter to use (references a key in `[adapter.*]`).
    /// Defaults to `"default"`.
    #[serde(default = "default_adapter_name")]
    pub adapter: String,

    /// Template for the target catalog name.
    pub catalog_template: String,

    /// Template for the target schema name.
    pub schema_template: String,

    /// Separator for joining variadic components in target templates.
    ///
    /// When a source pattern uses `"__"` as its separator but the target
    /// templates use `"_"` between placeholders, set this to `"_"` so that
    /// multi-valued components (e.g., `{hierarchies}`) are joined correctly.
    ///
    /// Defaults to the source pattern separator when not set.
    #[serde(default)]
    pub separator: Option<String>,

    /// Governance settings for the target.
    #[serde(default)]
    pub governance: GovernanceConfig,
}

/// Collects every `kind`-field and pipeline-role issue in a parsed
/// `RockyConfig`.
///
/// Runs after deserialization so errors point readers at the exact
/// config fix (e.g. "add `kind = \"discovery\"` to
/// [adapter.fivetran_main]") rather than failing deep inside the CLI
/// adapter registry at runtime.
///
/// Returns all issues, not just the first, so `rocky validate` can
/// surface each one as its own structured diagnostic. Production load
/// paths use [`load_rocky_config`], which fails fast on the first
/// returned error.
///
/// Orthogonal to [`crate::config`]-level issues like unknown adapter
/// types or missing pipeline adapter references — those are surfaced
/// by the `rocky validate` command as rich diagnostics (V017 / V022)
/// and by the CLI registry at instantiation time. This function
/// narrows its focus to the `kind` invariants so the two error paths
/// don't double-report.
pub fn validate_adapter_kinds(config: &RockyConfig) -> Vec<ConfigError> {
    use crate::adapter_capability::capability_for;

    let mut errors = Vec::new();

    for (name, adapter) in &config.adapters {
        let Some(cap) = capability_for(&adapter.adapter_type) else {
            continue;
        };

        match (adapter.kind, cap.supports_data, cap.supports_discovery) {
            // Discovery-only type — `kind = "discovery"` is required so
            // the role is self-evident in the raw config file.
            (None, false, true) => {
                errors.push(ConfigError::AdapterMissingDiscoveryKind {
                    name: name.clone(),
                    adapter_type: adapter.adapter_type.clone(),
                });
            }
            // Declared `kind` must be a role the adapter actually supports.
            (Some(AdapterKind::Data), false, _) => {
                errors.push(ConfigError::AdapterKindUnsupported {
                    name: name.clone(),
                    adapter_type: adapter.adapter_type.clone(),
                    declared: "data".to_owned(),
                    supported: "discovery".to_owned(),
                });
            }
            (Some(AdapterKind::Discovery), _, false) => {
                errors.push(ConfigError::AdapterKindUnsupported {
                    name: name.clone(),
                    adapter_type: adapter.adapter_type.clone(),
                    declared: "discovery".to_owned(),
                    supported: "data".to_owned(),
                });
            }
            _ => {}
        }
    }

    for (pipeline_name, pipeline) in &config.pipelines {
        let PipelineConfig::Replication(replication) = pipeline else {
            continue;
        };

        // Skip missing / unknown-type references — those are reported by
        // `rocky validate` (V022 / V017) or by the registry at runtime.
        if let Some(source_cfg) = config.adapters.get(&replication.source.adapter)
            && capability_for(&source_cfg.adapter_type).is_some()
            && !adapter_role_active(source_cfg, AdapterKind::Data)
        {
            errors.push(ConfigError::PipelineSourceAdapterNotData {
                pipeline: pipeline_name.clone(),
                adapter: replication.source.adapter.clone(),
            });
        }

        if let Some(discovery) = &replication.source.discovery
            && let Some(disc_cfg) = config.adapters.get(&discovery.adapter)
            && capability_for(&disc_cfg.adapter_type).is_some()
            && !adapter_role_active(disc_cfg, AdapterKind::Discovery)
        {
            errors.push(ConfigError::PipelineDiscoveryAdapterNotDiscovery {
                pipeline: pipeline_name.clone(),
                adapter: discovery.adapter.clone(),
            });
        }
    }

    errors
}

/// Collects strategy-level issues on replication pipelines.
///
/// Today this checks one rule: `strategy = "merge"` requires
/// `merge_keys` (or `merge_keys_fallback`). Surfacing it here means
/// `rocky validate` can emit the issue as a structured diagnostic, and
/// production load paths (via [`load_rocky_config`]) fail fast before
/// any network call.
///
/// Returns one error per offending pipeline. Other strategy strings
/// (`"incremental"`, `"full_refresh"`, anything else) pass through
/// unchanged — unknown values still fall back to `FullRefresh` at
/// dispatch time. Tightening that fallback into a typed error is a
/// follow-up.
pub fn validate_replication_strategies(config: &RockyConfig) -> Vec<ConfigError> {
    let mut errors = Vec::new();
    for (pipeline_name, pipeline) in &config.pipelines {
        let PipelineConfig::Replication(replication) = pipeline else {
            continue;
        };
        if replication.strategy == "merge" && replication.resolved_merge_keys().is_none() {
            errors.push(ConfigError::ReplicationMergeMissingKeys {
                pipeline: pipeline_name.clone(),
            });
        }
    }
    errors
}

/// Parse-time validation for `[[table_overrides]]` blocks.
///
/// Catches the structural classes the resolver and runner can't
/// recover from gracefully:
///
/// - **T1.1** Duplicate fully-specific rules — two entries with the
///   same `(match.connector, match.table)` literal pair. The resolver
///   would otherwise have to break the tie by declaration order,
///   which is the trust-failure mode the rest of `rocky.toml` avoids.
/// - **T1.2** Empty `match` block — both `connector` and `table` are
///   `None`. Use pipeline-level fields to change defaults globally.
/// - **T1.4** Unreachable merge keys — an override sets
///   `strategy = "merge"` and supplies no `merge_keys` /
///   `merge_keys_fallback`, **and** the pipeline default supplies
///   neither either. The effective config would crash at run time.
/// - **T1.6** Malformed glob in `match.table` — an empty pattern
///   today; the surface is intentionally narrow to keep future
///   `globset`-style extensions backwards-compatible.
///
/// Zero-match rules are intentionally **not** caught here — connectors
/// come and go in real production environments, and a rule that
/// doesn't match today may match tomorrow. The runner emits a soft
/// warning via `RunOutput.override_warnings` instead.
pub fn validate_replication_overrides(config: &RockyConfig) -> Vec<ConfigError> {
    let mut errors = Vec::new();
    for (pipeline_name, pipeline) in &config.pipelines {
        let PipelineConfig::Replication(replication) = pipeline else {
            continue;
        };
        let overrides = &replication.table_overrides;

        // T1.2 + T1.6 — per-rule structural checks.
        for (idx, rule) in overrides.iter().enumerate() {
            if rule.match_.connector.is_none() && rule.match_.table.is_none() {
                errors.push(ConfigError::TableOverrideEmptyMatch {
                    pipeline: pipeline_name.clone(),
                    rule_index: idx,
                });
            }
            if let Some(table_pat) = rule.match_.table.as_deref() {
                if pattern_is_glob(table_pat) {
                    if let Err(reason) = validate_glob_pattern(table_pat) {
                        errors.push(ConfigError::TableOverrideInvalidGlob {
                            pipeline: pipeline_name.clone(),
                            rule_index: idx,
                            reason,
                        });
                    }
                } else if table_pat.is_empty() {
                    errors.push(ConfigError::TableOverrideInvalidGlob {
                        pipeline: pipeline_name.clone(),
                        rule_index: idx,
                        reason: "empty table pattern".to_string(),
                    });
                }
            }

            // T1.4 — strategy="merge" with no reachable merge keys.
            if rule.strategy.as_deref() == Some("merge") {
                let override_has_keys =
                    rule.merge_keys.is_some() || rule.merge_keys_fallback.is_some();
                let pipeline_has_keys = replication.resolved_merge_keys().is_some();
                if !override_has_keys && !pipeline_has_keys {
                    errors.push(ConfigError::TableOverrideMergeMissingKeys {
                        pipeline: pipeline_name.clone(),
                        rule_index: idx,
                    });
                }
            }
        }

        // T1.1 — duplicate fully-specific (connector, table) literal
        // pairs. We compare by the raw strings; two glob patterns that
        // happen to overlap (e.g. `_x_*` vs `*_y`) are not a duplicate
        // — same-tier ambiguity for an actual `(connector, table)` is
        // a per-resolution concern surfaced at run time below if any
        // such pair is ever discovered (see T1.3 note).
        for (i, a) in overrides.iter().enumerate() {
            if a.match_.connector.is_none() || a.match_.table.is_none() {
                continue;
            }
            let a_table = a.match_.table.as_deref().unwrap_or_default();
            if pattern_is_glob(a_table) {
                continue;
            }
            for (j, b) in overrides.iter().enumerate().skip(i + 1) {
                if b.match_.connector.is_none() || b.match_.table.is_none() {
                    continue;
                }
                let b_table = b.match_.table.as_deref().unwrap_or_default();
                if pattern_is_glob(b_table) {
                    continue;
                }
                if a.match_.connector == b.match_.connector && a.match_.table == b.match_.table {
                    errors.push(ConfigError::TableOverrideDuplicate {
                        pipeline: pipeline_name.clone(),
                        first_index: i,
                        second_index: j,
                        connector: a.match_.connector.clone().unwrap_or_default(),
                        table: a_table.to_string(),
                    });
                }
            }
        }
    }
    errors
}

/// Parse-time validation that schema-pattern components don't use the
/// reserved names `table` or `id`. Both are reserved for
/// [`--filter`-style keys](crate::source::DiscoveredConnector::id) and
/// the new `[[table_overrides]]` match grammar — a schema pattern
/// component that shadows either would create ambiguity that no
/// runtime resolver can untangle.
///
/// Returns one error per reserved component. Rename the component in
/// `[pipeline.x.source.schema_pattern]` (e.g. `source_table` instead
/// of `table`) to resolve.
pub fn validate_schema_pattern_reserved_components(config: &RockyConfig) -> Vec<ConfigError> {
    const RESERVED: &[&str] = &["table", "id"];
    let mut errors = Vec::new();
    for (pipeline_name, pipeline) in &config.pipelines {
        let PipelineConfig::Replication(replication) = pipeline else {
            continue;
        };
        for component in &replication.source.schema_pattern.components {
            // `regions...` style variadic components — strip the
            // suffix before comparing.
            let name = component.trim_end_matches("...");
            if RESERVED.contains(&name) {
                errors.push(ConfigError::SchemaPatternReservedComponent {
                    pipeline: pipeline_name.clone(),
                    component: name.to_string(),
                });
            }
        }
    }
    errors
}

/// Does this adapter actively serve `role`?
///
/// An adapter block actively serves a role when:
/// - its type supports that role, AND
/// - the user hasn't narrowed `kind` to a *different* role.
///
/// `kind = None` on a dual-role type (DuckDB) means both roles are active.
fn adapter_role_active(adapter: &AdapterConfig, role: AdapterKind) -> bool {
    let Some(cap) = crate::adapter_capability::capability_for(&adapter.adapter_type) else {
        return false;
    };

    let supports = match role {
        AdapterKind::Data => cap.supports_data,
        AdapterKind::Discovery => cap.supports_discovery,
    };
    if !supports {
        return false;
    }

    match adapter.kind {
        None => true,
        Some(declared) => declared == role,
    }
}

/// Parses a Rocky configuration from a TOML file **without** running
/// [`validate_adapter_kinds`].
///
/// Env var substitution, deprecation remapping, and shorthand
/// normalization still run — the only thing skipped is the `kind`-field
/// invariant check. Intended for `rocky validate`, which wants to
/// collect `kind` issues as structured diagnostics (V032 / V033)
/// instead of bailing on the first one. Production code paths should
/// use [`load_rocky_config`] so misuse fails fast.
pub fn parse_rocky_config(path: &Path) -> Result<RockyConfig, ConfigError> {
    let raw = read_config_file(path)?;
    parse_rocky_config_str(&raw)
}

/// Read the raw config file bytes, mapping a missing file to
/// [`ConfigError::FileNotFound`]. The single read site for every loader in
/// this module — the fingerprinted loader hashes exactly this string's bytes.
fn read_config_file(path: &Path) -> Result<String, ConfigError> {
    std::fs::read_to_string(path).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            ConfigError::FileNotFound {
                path: path.to_path_buf(),
            }
        } else {
            ConfigError::ReadFile(e)
        }
    })
}

/// Parse an already-read raw config string — env-var substitution,
/// deprecation remapping, and shorthand normalization included; `kind`
/// validation excluded (see [`parse_rocky_config`]).
fn parse_rocky_config_str(raw: &str) -> Result<RockyConfig, ConfigError> {
    let (substituted, substitutions) = substitute_env_vars_with_report(raw)?;
    let env_var_hint = format_env_var_hint(&substitutions);
    let to_parse_err = |source: toml::de::Error| -> ConfigError {
        if env_var_hint.is_empty() {
            ConfigError::ParseToml(source)
        } else {
            ConfigError::ParseTomlWithEnvContext {
                source,
                env_var_hint: env_var_hint.clone(),
            }
        }
    };
    let mut value: toml::Value = toml::from_str(&substituted).map_err(to_parse_err)?;
    apply_deprecations(&mut value);
    normalize_toml_shorthands(&mut value);
    let mut config: RockyConfig = value.try_into().map_err(to_parse_err)?;
    apply_single_adapter_discovery_default(&mut config);
    Ok(config)
}

/// When exactly one `[adapter]` is defined and a replication pipeline
/// omits `[source.discovery]` entirely, materialize the discovery block
/// pointing at that one adapter so callers downstream (e.g. `rocky
/// compact --catalog`) don't have to special-case "no discovery
/// configured".
///
/// Mirrors what serde already does for `source.adapter` and
/// `target.adapter` via `#[serde(default = "default_adapter_name")]`,
/// except the auto-wire uses the *actual* adapter name (which may be
/// `"default"` under the canonical `[adapter]` shorthand, or any name
/// when written as `[adapter.<name>]`) rather than the hardcoded
/// `"default"` literal — otherwise a single-adapter project named
/// `[adapter.local]` would wire discovery at `"default"` and fail the
/// existing `V022` "no adapter named 'default'" diagnostic.
///
/// Multi-adapter configs are left alone — the existing "no discovery
/// configured" path keeps emitting its warning and the user has to wire
/// it explicitly.
fn apply_single_adapter_discovery_default(config: &mut RockyConfig) {
    let sole_adapter = match config.adapters.len() {
        1 => config
            .adapters
            .keys()
            .next()
            .expect("len == 1 guarantees one key")
            .clone(),
        _ => return,
    };
    for pipeline in config.pipelines.values_mut() {
        let PipelineConfig::Replication(replication) = pipeline else {
            continue;
        };
        if replication.source.discovery.is_none() {
            replication.source.discovery = Some(DiscoveryConfig {
                adapter: sole_adapter.clone(),
                report_new_sources: false,
                on_collision: OnCollision::Off,
            });
        }
    }
}

/// Loads, parses, and validates a Rocky configuration (v2 format only).
///
/// Environment variables are substituted before parsing. Supports both named
/// adapter form (`[adapter.foo]`) and unnamed shorthand (`[adapter]` with a
/// `type` key) for single-adapter projects. Same for `[pipeline]` vs
/// `[pipeline.name]`.
///
/// Deprecated config keys are automatically remapped to their new names and
/// a `tracing::warn!` is emitted for each one. Callers that need to surface
/// deprecation warnings programmatically (e.g., `rocky doctor`, `rocky
/// validate`) can use [`check_config_deprecations`] on the raw TOML string.
///
/// Also validates adapter `kind` fields and pipeline adapter references —
/// see [`validate_adapter_kinds`]. Fails fast on the first such issue;
/// `rocky validate` uses [`parse_rocky_config`] + [`validate_adapter_kinds`]
/// directly so it can emit every issue as its own diagnostic.
pub fn load_rocky_config(path: &Path) -> Result<RockyConfig, ConfigError> {
    let config = parse_rocky_config(path)?;
    validate_loaded_config(&config)?;
    Ok(config)
}

/// The fail-fast validation chain shared by [`load_rocky_config`] and
/// [`load_rocky_config_fingerprinted`]. Returns the first issue, matching
/// `load_rocky_config`'s historical behavior.
fn validate_loaded_config(config: &RockyConfig) -> Result<(), ConfigError> {
    if let Some(first) = validate_adapter_kinds(config).into_iter().next() {
        return Err(first);
    }
    if let Some(first) = validate_replication_strategies(config).into_iter().next() {
        return Err(first);
    }
    if let Some(first) = validate_schema_pattern_reserved_components(config)
        .into_iter()
        .next()
    {
        return Err(first);
    }
    if let Some(first) = validate_replication_overrides(config).into_iter().next() {
        return Err(first);
    }
    if let Some(first) = validate_fivetran_cache(config).into_iter().next() {
        return Err(first);
    }
    if let Some(first) = validate_fivetran_resilience(config).into_iter().next() {
        return Err(first);
    }
    if let Some(first) = validate_policy(config).into_iter().next() {
        return Err(first);
    }
    Ok(())
}

/// A parsed [`RockyConfig`] paired with the fingerprint of the exact raw
/// bytes it was loaded from.
///
/// The execute-from-owned unit for config-snapshot threading (#1120): a
/// decision gate loads ONE `LoadedConfig` and threads it (usually as an
/// `Arc<LoadedConfig>`) through every downstream read and into execution, so
/// a `rocky.toml` swap timed between the gate and execution can neither
/// redirect what runs nor desynchronize the recorded `config_hash` from the
/// config that actually executed.
#[derive(Debug, Clone)]
pub struct LoadedConfig {
    /// The parsed, validated configuration.
    pub config: RockyConfig,
    /// 16-hex-char fingerprint of the raw file bytes captured at load time —
    /// see [`config_fingerprint_bytes`].
    pub fingerprint: String,
}

/// Stable 16-char hex fingerprint of a config file's raw bytes.
///
/// Uses [`std::hash::DefaultHasher`] (SipHash with fixed keys), so the value
/// is deterministic across processes for the same bytes — intra-release
/// stable, not cross-release stable. Deliberately hashes the RAW bytes, never
/// a serde serialization of the parsed config: `FreshnessConfig::overrides`
/// is a `std::collections::HashMap` reachable from [`RockyConfig`], so a
/// serialized form would have nondeterministic ordering across processes.
///
/// This is the single hashing implementation behind both the CLI's
/// path-based `config_fingerprint` (rocky-cli `output.rs`) and
/// [`load_rocky_config_fingerprinted`], so fingerprints of an unswapped file
/// are byte-identical wherever they are computed (`RunRecord::config_hash`
/// history stays comparable).
pub fn config_fingerprint_bytes(raw: &[u8]) -> String {
    use std::hash::{DefaultHasher, Hasher};
    let mut hasher = DefaultHasher::new();
    hasher.write(raw);
    format!("{:016x}", hasher.finish())
}

/// Loads, parses, and validates a Rocky configuration (like
/// [`load_rocky_config`]), additionally capturing the fingerprint of the
/// exact raw bytes read — hashed BEFORE env-var substitution and parsing, so
/// the fingerprint describes the on-disk snapshot this config came from.
///
/// # Errors
///
/// Same error surface as [`load_rocky_config`]: read, parse, and the
/// fail-fast validation chain.
pub fn load_rocky_config_fingerprinted(path: &Path) -> Result<LoadedConfig, ConfigError> {
    let raw = read_config_file(path)?;
    let fingerprint = config_fingerprint_bytes(raw.as_bytes());
    let config = parse_rocky_config_str(&raw)?;
    validate_loaded_config(&config)?;
    Ok(LoadedConfig {
        config,
        fingerprint,
    })
}

/// Validate every `[adapter.<name>.cache]` block's cross-field
/// requirements at config-load time so a misconfigured backend errors
/// up front rather than at the first cache touch. Skips non-Fivetran
/// adapter blocks even if they accidentally carry a `cache` key — the
/// shape is fivetran-specific and a future change can lift that gate
/// without breaking the call shape here.
pub fn validate_fivetran_cache(config: &RockyConfig) -> Vec<ConfigError> {
    let mut errors = Vec::new();
    for (name, adapter_cfg) in &config.adapters {
        if adapter_cfg.adapter_type != "fivetran" {
            continue;
        }
        if let Some(cache_cfg) = &adapter_cfg.cache
            && let Err(e) = cache_cfg.validate(name)
        {
            errors.push(e);
        }
    }
    errors
}

/// Validate every `[adapter.<name>.{ratelimit,stampede,circuit_breaker}]`
/// block's cross-field requirements at config-load time so a
/// misconfigured backend errors up front rather than at first request.
/// Skips non-Fivetran adapter blocks; the shape is fivetran-specific.
pub fn validate_fivetran_resilience(config: &RockyConfig) -> Vec<ConfigError> {
    let mut errors = Vec::new();
    for (name, adapter_cfg) in &config.adapters {
        if adapter_cfg.adapter_type != "fivetran" {
            continue;
        }
        if let Some(rl) = &adapter_cfg.ratelimit
            && let Err(e) = rl.validate(name)
        {
            errors.push(e);
        }
        if let Some(st) = &adapter_cfg.stampede
            && let Err(e) = st.validate(name)
        {
            errors.push(e);
        }
        if let Some(cb) = &adapter_cfg.circuit_breaker
            && let Err(e) = cb.validate(name)
        {
            errors.push(e);
        }
    }
    errors
}

/// Checks a raw TOML string for deprecated config keys without loading
/// the full config.
///
/// Returns a list of [`DeprecationWarning`]s for each deprecated key found.
/// This is useful for diagnostic commands (`rocky doctor`, `rocky validate`)
/// that want to report deprecation warnings to the user without modifying
/// the TOML in place.
pub fn check_config_deprecations(toml_str: &str) -> Result<Vec<DeprecationWarning>, ConfigError> {
    let mut value: toml::Value = toml::from_str(toml_str)?;
    Ok(apply_deprecations(&mut value))
}

/// Pre-pass on the raw TOML value tree to support shorthand forms:
///
/// - `[adapter]` with a `type` key → wrapped as `[adapter.default]`
/// - `[pipeline]` with a `source` key → wrapped as `[pipeline.default]`
///
/// This runs before serde deserialization so the rest of the code only
/// sees the canonical named form.
fn normalize_toml_shorthands(value: &mut toml::Value) {
    if let Some(table) = value.as_table_mut() {
        // Handle bare [adapter] → [adapter.default]
        if let Some(adapter_val) = table.get("adapter")
            && is_bare_adapter(adapter_val)
        {
            let adapter = table.remove("adapter").expect("key was just found above");
            let mut wrapper = toml::map::Map::new();
            wrapper.insert("default".to_string(), adapter);
            table.insert("adapter".to_string(), toml::Value::Table(wrapper));
        }

        // Handle bare [pipeline] → [pipeline.default]
        if let Some(pipeline_val) = table.get("pipeline")
            && is_bare_pipeline(pipeline_val)
        {
            let pipeline = table.remove("pipeline").expect("key was just found above");
            let mut wrapper = toml::map::Map::new();
            wrapper.insert("default".to_string(), pipeline);
            table.insert("pipeline".to_string(), toml::Value::Table(wrapper));
        }
    }
}

/// Detect a bare `[adapter]` table (has a `type` key directly, meaning it's
/// an `AdapterConfig` rather than a map of named adapters).
fn is_bare_adapter(val: &toml::Value) -> bool {
    val.as_table().is_some_and(|t| t.contains_key("type"))
}

/// Detect a bare `[pipeline]` table (a single pipeline config rather than a map
/// of named pipelines). Checks for keys that indicate a pipeline config:
/// `source` (replication), `models` (transformation), `tables` (quality),
/// `unique_key` (snapshot), `source_dir` (load), or an explicit `type` field.
fn is_bare_pipeline(val: &toml::Value) -> bool {
    val.as_table().is_some_and(|t| {
        t.contains_key("source")
            || t.contains_key("type")
            || t.contains_key("models")
            || t.contains_key("tables")
            || t.contains_key("unique_key")
            || t.contains_key("source_dir")
    })
}

impl ReplicationPipelineConfig {
    /// Check kinds the replication runner (`rocky-cli` `run.rs`) executes — the
    /// single source of truth shared by [`PipelineConfig::executed_check_kinds`],
    /// the `rocky validate` inert-config lint, and the `rocky discover`
    /// check-name projection (`rocky discover` is replication-only). Keep in
    /// lockstep with the check-execution blocks in `run.rs`.
    pub const EXECUTED_CHECK_KINDS: &'static [crate::checks::CheckKind] = &[
        crate::checks::CheckKind::RowCount,
        crate::checks::CheckKind::ColumnMatch,
        crate::checks::CheckKind::Freshness,
        crate::checks::CheckKind::NullRate,
        crate::checks::CheckKind::Custom,
        crate::checks::CheckKind::CrossSourceOverlap,
        crate::checks::CheckKind::Assertions,
        crate::checks::CheckKind::Anomaly,
    ];

    /// Builds a SchemaPattern from the source configuration.
    pub fn schema_pattern(&self) -> Result<SchemaPattern, crate::schema::SchemaError> {
        self.source.schema_pattern.to_schema_pattern()
    }

    /// Returns the resolved merge keys, preferring `merge_keys` over
    /// `merge_keys_fallback`. `None` when neither is set.
    pub fn resolved_merge_keys(&self) -> Option<&Vec<String>> {
        self.merge_keys
            .as_ref()
            .or(self.merge_keys_fallback.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_env_var_hint_truncates_on_char_boundary() {
        // Regression: a fixed `&value[..40]` slice panicked when byte 40 landed
        // inside a multibyte character. A long value with multibyte chars
        // spanning the boundary must truncate cleanly (no panic).
        let value = "é".repeat(30); // 60 bytes; byte 40 is mid-character
        let subs = vec![EnvVarSubstitution {
            name: "SECRET".to_string(),
            value,
        }];
        let hint = format_env_var_hint(&subs); // must not panic
        assert!(hint.contains("SECRET"));
        assert!(hint.contains('…'));
    }

    /// WP-01 PR-B: `load_rocky_config_fingerprinted` is deterministic across
    /// repeated loads — including for a config whose `FreshnessConfig::
    /// overrides` `HashMap` carries multiple keys. The fingerprint hashes the
    /// RAW file bytes (never a serde serialization), so `HashMap` iteration
    /// order cannot leak into the value. A serialization-based fingerprint
    /// would flake this test across processes/seeds.
    #[test]
    fn fingerprinted_load_is_deterministic_across_repeated_loads() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rocky.toml");
        std::fs::write(
            &path,
            r#"
[adapter.db]
type = "duckdb"
path = "wh.duckdb"

[pipeline.silver]
type = "transformation"
models = "models/**"

[pipeline.silver.target]
adapter = "db"

[pipeline.silver.checks]
enabled = true

[pipeline.silver.checks.freshness]
threshold_seconds = 3600

[pipeline.silver.checks.freshness.overrides]
"raw__us_west__shopify" = 7200
"raw__eu__stripe" = 1800
"raw__apac__ads" = 900
"#,
        )
        .unwrap();

        let first = load_rocky_config_fingerprinted(&path).unwrap();
        assert_eq!(
            first.config.pipelines["silver"]
                .as_transformation()
                .unwrap()
                .checks
                .freshness
                .as_ref()
                .unwrap()
                .overrides
                .len(),
            3,
            "the overrides HashMap must actually carry multiple keys"
        );
        assert_eq!(
            first.fingerprint,
            config_fingerprint_bytes(&std::fs::read(&path).unwrap()),
            "the captured fingerprint is the raw-bytes fingerprint of the file"
        );
        for _ in 0..50 {
            let again = load_rocky_config_fingerprinted(&path).unwrap();
            assert_eq!(again.fingerprint, first.fingerprint);
        }

        // A single-byte change to the raw file moves the fingerprint.
        let mut bytes = std::fs::read(&path).unwrap();
        bytes.push(b'\n');
        std::fs::write(&path, &bytes).unwrap();
        let swapped = load_rocky_config_fingerprinted(&path).unwrap();
        assert_ne!(swapped.fingerprint, first.fingerprint);
    }

    #[test]
    fn test_imports_block_parses() {
        let toml_str = r#"
[imports.orders]
path = "vendor/orders"
snapshot = "current.json"
baseline = "baseline.json"
pin = "*"
"#;
        let config: RockyConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.imports.len(), 1);
        let entry = &config.imports["orders"];
        assert_eq!(entry.path, "vendor/orders");
        assert_eq!(entry.snapshot, "current.json");
        assert_eq!(entry.baseline.as_deref(), Some("baseline.json"));
        assert_eq!(entry.pin.as_deref(), Some("*"));
    }

    #[test]
    fn reuse_column_level_defaults_on_via_both_paths() {
        // `[reuse]` block absent entirely ⇒ `ReuseConfig::default()`.
        let d = ReuseConfig::default();
        assert!(d.column_level, "column_level defaults ON");
        assert!(!d.enabled, "point-to reuse (enabled) stays default-OFF");

        // The whole parent config with no `[reuse]` block resolves the same.
        let parent: RockyConfig = toml::from_str("").unwrap();
        assert!(parent.reuse.column_level);
        assert!(!parent.reuse.enabled);

        // `[reuse]` present but `column_level` omitted ⇒ serde field default.
        let present: ReuseConfig = toml::from_str("").unwrap();
        assert!(present.column_level, "omitted column_level defaults ON");

        // Explicit override is honored — proves the default is real, not a
        // hardcoded value (non-vacuous: fails on the old default-OFF too).
        let off: ReuseConfig = toml::from_str("column_level = false").unwrap();
        assert!(
            !off.column_level,
            "explicit `column_level = false` is honored"
        );
        let on: ReuseConfig = toml::from_str("column_level = true").unwrap();
        assert!(on.column_level);
    }

    #[test]
    fn gc_physical_delete_defaults_off_via_both_paths() {
        // `[gc]` block absent entirely ⇒ `GcConfig::default()`.
        let d = GcConfig::default();
        assert!(!d.physical_delete, "physical_delete defaults OFF");

        // The whole parent config with no `[gc]` block resolves the same.
        let parent: RockyConfig = toml::from_str("").unwrap();
        assert!(!parent.gc.physical_delete);

        // `[gc]` present but `physical_delete` omitted ⇒ serde field default.
        let present: GcConfig = toml::from_str("").unwrap();
        assert!(!present.physical_delete);

        // Explicit opt-in is honored — proves the default is real, not a
        // hardcoded value (non-vacuous: fails if the flag were ignored).
        let armed: RockyConfig = toml::from_str("[gc]\nphysical_delete = true").unwrap();
        assert!(armed.gc.physical_delete);

        // Unknown keys under `[gc]` are rejected (deny_unknown_fields), so a
        // typo can never silently arm or disarm deletion.
        assert!(toml::from_str::<GcConfig>("physical_deletes = true").is_err());
    }

    #[test]
    fn test_imports_block_absent_is_empty() {
        let config: RockyConfig = toml::from_str("").unwrap();
        assert!(config.imports.is_empty());
    }

    #[test]
    fn test_imports_minimal_entry() {
        let toml_str = r#"
[imports.shipments]
path = "vendor/shipments"
snapshot = "snap.json"
"#;
        let config: RockyConfig = toml::from_str(toml_str).unwrap();
        let entry = &config.imports["shipments"];
        assert!(entry.baseline.is_none());
        assert!(entry.pin.is_none());
    }

    #[test]
    fn test_env_var_substitution() {
        // SAFETY: test-only, no concurrent reads of this variable
        unsafe { std::env::set_var("ROCKY_TEST_VAR", "hello_world") };
        let result = substitute_env_vars("prefix_${ROCKY_TEST_VAR}_suffix").unwrap();
        assert_eq!(result, "prefix_hello_world_suffix");
        unsafe { std::env::remove_var("ROCKY_TEST_VAR") };
    }

    #[test]
    fn test_env_var_missing() {
        // SAFETY: test-only, no concurrent reads of this variable
        unsafe { std::env::remove_var("ROCKY_DEFINITELY_NOT_SET") };
        let result = substitute_env_vars("${ROCKY_DEFINITELY_NOT_SET}");
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ConfigError::MissingEnvVar { .. }
        ));
    }

    #[test]
    fn test_no_env_vars() {
        let result = substitute_env_vars("no variables here").unwrap();
        assert_eq!(result, "no variables here");
    }

    #[test]
    fn test_multiple_env_vars() {
        // SAFETY: test-only, no concurrent reads of these variables
        unsafe {
            std::env::set_var("ROCKY_TEST_A", "alpha");
            std::env::set_var("ROCKY_TEST_B", "beta");
        }
        let result = substitute_env_vars("${ROCKY_TEST_A}_and_${ROCKY_TEST_B}").unwrap();
        assert_eq!(result, "alpha_and_beta");
        unsafe {
            std::env::remove_var("ROCKY_TEST_A");
            std::env::remove_var("ROCKY_TEST_B");
        }
    }

    #[test]
    fn test_env_var_with_default() {
        unsafe { std::env::remove_var("ROCKY_UNSET_VAR") };
        let result = substitute_env_vars("${ROCKY_UNSET_VAR:-fallback_value}").unwrap();
        assert_eq!(result, "fallback_value");
    }

    #[test]
    fn test_env_var_default_overridden_by_env() {
        unsafe { std::env::set_var("ROCKY_SET_VAR", "real_value") };
        let result = substitute_env_vars("${ROCKY_SET_VAR:-fallback}").unwrap();
        assert_eq!(result, "real_value");
        unsafe { std::env::remove_var("ROCKY_SET_VAR") };
    }

    #[test]
    fn test_env_var_empty_default() {
        unsafe { std::env::remove_var("ROCKY_EMPTY_DEFAULT") };
        let result = substitute_env_vars("${ROCKY_EMPTY_DEFAULT:-}").unwrap();
        assert_eq!(result, "");
    }

    #[test]
    fn test_substitute_env_vars_with_report_records_substitutions() {
        // P2.9: substitute_env_vars_with_report should return the list of
        // substituted env vars alongside the final text so post-parse errors
        // can cite the env var origin.
        unsafe {
            std::env::set_var("ROCKY_TEST_REPORT_A", "-10");
            std::env::set_var("ROCKY_TEST_REPORT_B", "42");
        }
        let (text, report) = substitute_env_vars_with_report(
            "timeout_secs = ${ROCKY_TEST_REPORT_A}\nretries = ${ROCKY_TEST_REPORT_B}",
        )
        .unwrap();
        assert_eq!(text, "timeout_secs = -10\nretries = 42");
        assert_eq!(report.len(), 2);
        assert_eq!(report[0].name, "ROCKY_TEST_REPORT_A");
        assert_eq!(report[0].value, "-10");
        assert_eq!(report[1].name, "ROCKY_TEST_REPORT_B");
        assert_eq!(report[1].value, "42");
        unsafe {
            std::env::remove_var("ROCKY_TEST_REPORT_A");
            std::env::remove_var("ROCKY_TEST_REPORT_B");
        }
    }

    #[test]
    fn test_substitute_report_includes_default_values() {
        // When the env var is unset and a default kicks in, the default value
        // should still be recorded so the operator sees what was actually
        // substituted.
        unsafe { std::env::remove_var("ROCKY_TEST_UNSET_WITH_DEFAULT") };
        let (text, report) =
            substitute_env_vars_with_report("timeout = ${ROCKY_TEST_UNSET_WITH_DEFAULT:-30}")
                .unwrap();
        assert_eq!(text, "timeout = 30");
        assert_eq!(report.len(), 1);
        assert_eq!(report[0].name, "ROCKY_TEST_UNSET_WITH_DEFAULT");
        assert_eq!(report[0].value, "30");
    }

    #[test]
    fn test_parse_rocky_config_enriches_toml_error_with_env_vars() {
        // End-to-end P2.9: a negative number supplied by an env var produces
        // a TOML parse error; the enriched error should mention the env var
        // names so the operator can find the culprit.
        use std::io::Write;

        // Build a minimal config that substitutes an env var into a field
        // serde will reject as negative u64.
        unsafe { std::env::set_var("ROCKY_TEST_BAD_TIMEOUT", "-10") };
        let toml_source = r#"
[state]
path = "./state.db"

[adapter.foo]
type = "duckdb"
path = "/tmp/x.duckdb"

[pipeline.p]
type = "replication"
source = { adapter = "foo", schema = "x" }
target = { adapter = "foo", schema = "y" }
conflict_action = "abort"
max_retries = ${ROCKY_TEST_BAD_TIMEOUT}
"#;
        let mut tmp = tempfile::NamedTempFile::new().expect("tempfile");
        tmp.write_all(toml_source.as_bytes()).expect("write");
        let err = parse_rocky_config(tmp.path()).expect_err("should reject negative max_retries");

        let msg = err.to_string();
        // The enriched variant should include the env var name in the hint.
        assert!(
            matches!(err, ConfigError::ParseTomlWithEnvContext { .. }),
            "expected ParseTomlWithEnvContext, got {err:?}",
        );
        assert!(
            msg.contains("ROCKY_TEST_BAD_TIMEOUT"),
            "missing env var in error: {msg}"
        );
        assert!(
            msg.contains("env var substitutions"),
            "missing hint prefix in error: {msg}"
        );

        unsafe { std::env::remove_var("ROCKY_TEST_BAD_TIMEOUT") };
    }

    #[test]
    fn test_schema_pattern_from_config() {
        let config = SchemaPatternConfig {
            prefix: "src__".to_string(),
            separator: "__".to_string(),
            components: vec![
                "tenant".to_string(),
                "regions...".to_string(),
                "source".to_string(),
            ],
        };

        let pattern = config.to_schema_pattern().unwrap();
        assert_eq!(pattern.prefix, "src__");
        assert_eq!(pattern.separator, "__");
        assert_eq!(pattern.components.len(), 3);
        assert!(matches!(
            &pattern.components[1],
            crate::schema::PatternComponent::VariableLength { name } if name == "regions"
        ));
    }

    // --- adapter kind validation ---

    fn parse(toml_str: &str) -> RockyConfig {
        let mut value: toml::Value = toml::from_str(toml_str).unwrap();
        normalize_toml_shorthands(&mut value);
        value.try_into().unwrap()
    }

    fn expect_single_error(cfg: &RockyConfig) -> ConfigError {
        let mut errors = validate_adapter_kinds(cfg);
        assert_eq!(
            errors.len(),
            1,
            "expected exactly one kind error, got {errors:?}"
        );
        errors.remove(0)
    }

    // --- [policy] parsing + validation ---

    #[test]
    fn policy_block_parses_and_validates_clean() {
        let cfg = parse(
            r#"
[policy]
version = 1
default_agent_effect = "require_review"

[[policy.rules]]
principal = "agent"
capability = "apply"
scope = { contracted = true }
effect = "deny"

[[policy.rules]]
principal = "agent"
capability = "schema_change.additive"
scope = { layer = "bronze", exclude_classifications = ["pii"] }
effect = "allow"
"#,
        );
        let policy = cfg.policy.as_ref().expect("[policy] present");
        assert_eq!(policy.version, 1);
        assert_eq!(policy.default_agent_effect, PolicyEffect::RequireReview);
        assert_eq!(policy.rules.len(), 2);
        assert_eq!(policy.rules[0].capability, PolicyCapability::Apply);
        assert_eq!(policy.rules[0].scope.contracted, Some(true));
        assert_eq!(
            policy.rules[1].capability,
            PolicyCapability::SchemaChangeAdditive
        );
        assert!(validate_policy(&cfg).is_empty());
    }

    #[test]
    fn policy_default_agent_effect_defaults_to_require_review() {
        let cfg = parse(
            r#"
[policy]
version = 1
"#,
        );
        let policy = cfg.policy.as_ref().unwrap();
        assert_eq!(policy.default_agent_effect, PolicyEffect::RequireReview);
        assert!(policy.rules.is_empty());
    }

    #[test]
    fn policy_rejects_unsupported_version() {
        let cfg = parse(
            r#"
[policy]
version = 2
"#,
        );
        let errors = validate_policy(&cfg);
        assert!(
            matches!(
                errors.as_slice(),
                [ConfigError::PolicyUnsupportedVersion { version: 2 }]
            ),
            "got {errors:?}"
        );
    }

    #[test]
    fn policy_rejects_any_with_other_scope_keys() {
        let cfg = parse(
            r#"
[policy]
version = 1

[[policy.rules]]
principal = "agent"
capability = "apply"
scope = { any = true, contracted = true }
effect = "deny"
"#,
        );
        let errors = validate_policy(&cfg);
        assert!(
            matches!(
                errors.as_slice(),
                [ConfigError::PolicyScopeAnyConflict { rule_index: 0 }]
            ),
            "got {errors:?}"
        );
    }

    #[test]
    fn policy_rejects_empty_scope() {
        let cfg = parse(
            r#"
[policy]
version = 1

[[policy.rules]]
principal = "agent"
capability = "apply"
scope = {}
effect = "deny"
"#,
        );
        let errors = validate_policy(&cfg);
        assert!(
            matches!(
                errors.as_slice(),
                [ConfigError::PolicyScopeEmpty { rule_index: 0 }]
            ),
            "got {errors:?}"
        );
    }

    #[test]
    fn policy_rejects_unknown_capability_at_parse_time() {
        let err = toml::from_str::<RockyConfig>(
            r#"
[policy]
version = 1

[[policy.rules]]
principal = "agent"
capability = "delete_everything"
scope = { any = true }
effect = "deny"
"#,
        );
        assert!(err.is_err(), "unknown capability must fail to deserialize");
    }

    #[test]
    fn policy_conditions_are_parsed_and_ignored() {
        // v1 rule `conditions` load without error in v0 (opaque, ignored).
        let cfg = parse(
            r#"
[policy]
version = 1

[[policy.rules]]
principal = "agent"
capability = "apply"
scope = { any = true }
effect = "require_review"
conditions = { time_window = "business_hours" }
"#,
        );
        assert!(validate_policy(&cfg).is_empty());
        assert!(cfg.policy.unwrap().rules[0].conditions.is_some());
    }

    #[test]
    fn policy_rule_parses_verify_after() {
        // `verify_after` is a first-class rule field (not opaque `conditions`),
        // so it parses under `deny_unknown_fields` and round-trips typed.
        let cfg = parse(
            r#"
[policy]
version = 1

[[policy.rules]]
principal = "agent"
capability = "schema_change.additive"
scope = { layer = "bronze" }
effect = "allow"
verify_after = ["row_count_drift", "not_null_keys"]
"#,
        );
        assert!(validate_policy(&cfg).is_empty());
        let rule = &cfg.policy.unwrap().rules[0];
        assert_eq!(rule.verify_after, vec!["row_count_drift", "not_null_keys"]);
    }

    #[test]
    fn policy_rule_without_verify_after_defaults_empty() {
        let cfg = parse(
            r#"
[policy]
version = 1

[[policy.rules]]
principal = "agent"
capability = "apply"
scope = { any = true }
effect = "allow"
"#,
        );
        assert!(cfg.policy.unwrap().rules[0].verify_after.is_empty());
    }

    #[test]
    fn policy_rule_parses_autonomy_budget() {
        // `autonomy_budget` is a first-class typed field: it parses under
        // `deny_unknown_fields` and validates clean.
        let cfg = parse(
            r#"
[policy]
version = 1

[[policy.rules]]
principal = "agent"
capability = "schema_change.additive"
scope = { layer = "bronze" }
effect = "allow"
verify_after = ["row_count_drift"]
autonomy_budget = { failures = 3, window = "7d" }
"#,
        );
        assert!(validate_policy(&cfg).is_empty());
        let budget = cfg.policy.unwrap().rules[0]
            .autonomy_budget
            .clone()
            .unwrap();
        assert_eq!(budget.failures, 3);
        assert_eq!(budget.window, "7d");
    }

    #[test]
    fn policy_rule_without_autonomy_budget_defaults_none() {
        let cfg = parse(
            r#"
[policy]
version = 1

[[policy.rules]]
principal = "agent"
capability = "apply"
scope = { any = true }
effect = "allow"
"#,
        );
        assert!(cfg.policy.unwrap().rules[0].autonomy_budget.is_none());
    }

    #[test]
    fn policy_budget_zero_failures_rejected() {
        let cfg = parse(
            r#"
[policy]
version = 1

[[policy.rules]]
principal = "agent"
capability = "apply"
scope = { any = true }
effect = "allow"
autonomy_budget = { failures = 0, window = "7d" }
"#,
        );
        let errors = validate_policy(&cfg);
        assert!(
            matches!(
                errors.as_slice(),
                [ConfigError::PolicyBudgetZeroFailures { rule_index: 0 }]
            ),
            "got {errors:?}"
        );
    }

    #[test]
    fn policy_budget_invalid_window_rejected() {
        let cfg = parse(
            r#"
[policy]
version = 1

[[policy.rules]]
principal = "agent"
capability = "apply"
scope = { any = true }
effect = "allow"
autonomy_budget = { failures = 2, window = "banana" }
"#,
        );
        let errors = validate_policy(&cfg);
        assert!(
            matches!(
                errors.as_slice(),
                [ConfigError::PolicyBudgetInvalidWindow { rule_index: 0, window }] if window == "banana"
            ),
            "got {errors:?}"
        );
    }

    #[test]
    fn parse_window_duration_units_and_rejections() {
        assert_eq!(parse_window_duration("7d"), chrono::Duration::try_days(7));
        assert_eq!(
            parse_window_duration("24h"),
            chrono::Duration::try_hours(24)
        );
        assert_eq!(parse_window_duration("30D"), chrono::Duration::try_days(30));
        assert!(parse_window_duration("0d").is_none());
        assert!(parse_window_duration("-1d").is_none());
        assert!(parse_window_duration("7").is_none());
        assert!(parse_window_duration("7w").is_none());
        assert!(parse_window_duration("").is_none());
        assert!(parse_window_duration("d").is_none());
    }

    #[test]
    fn no_policy_block_leaves_policy_none() {
        let cfg = parse(
            r#"
[adapter.local]
type = "duckdb"
"#,
        );
        assert!(cfg.policy.is_none());
        assert!(validate_policy(&cfg).is_empty());
    }

    #[test]
    fn discovery_only_adapter_requires_kind_field() {
        let cfg = parse(
            r#"
[adapter.fivetran_main]
type = "fivetran"
destination_id = "d"
api_key = "k"
api_secret = "s"
"#,
        );
        match expect_single_error(&cfg) {
            ConfigError::AdapterMissingDiscoveryKind { name, adapter_type } => {
                assert_eq!(name, "fivetran_main");
                assert_eq!(adapter_type, "fivetran");
            }
            other => panic!("expected AdapterMissingDiscoveryKind, got {other:?}"),
        }
    }

    #[test]
    fn discovery_only_adapter_with_kind_field_validates() {
        let cfg = parse(
            r#"
[adapter.fivetran_main]
type = "fivetran"
kind = "discovery"
destination_id = "d"
api_key = "k"
api_secret = "s"
"#,
        );
        assert!(validate_adapter_kinds(&cfg).is_empty());
    }

    #[test]
    fn data_only_adapter_rejects_discovery_kind() {
        let cfg = parse(
            r#"
[adapter.db]
type = "databricks"
kind = "discovery"
host = "h"
http_path = "p"
token = "t"
"#,
        );
        let err = expect_single_error(&cfg);
        assert!(matches!(
            err,
            ConfigError::AdapterKindUnsupported { ref declared, .. }
                if declared == "discovery"
        ));
    }

    #[test]
    fn data_only_adapter_without_kind_validates() {
        let cfg = parse(
            r#"
[adapter.db]
type = "databricks"
host = "h"
http_path = "p"
token = "t"
"#,
        );
        assert!(validate_adapter_kinds(&cfg).is_empty());
    }

    #[test]
    fn duckdb_without_kind_validates_as_both_roles() {
        let cfg = parse(
            r#"
[adapter.local]
type = "duckdb"

[pipeline.poc]
type = "replication"
strategy = "full_refresh"

[pipeline.poc.source]
adapter = "local"

[pipeline.poc.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.poc.source.discovery]
adapter = "local"

[pipeline.poc.target]
adapter = "local"
catalog_template = "poc"
schema_template = "demo"
"#,
        );
        assert!(validate_adapter_kinds(&cfg).is_empty());
    }

    #[test]
    fn pipeline_discovery_reference_to_data_only_adapter_errors() {
        let cfg = parse(
            r#"
[adapter.db]
type = "databricks"
host = "h"
http_path = "p"
token = "t"

[pipeline.poc]
type = "replication"
strategy = "full_refresh"

[pipeline.poc.source]
adapter = "db"

[pipeline.poc.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.poc.source.discovery]
adapter = "db"

[pipeline.poc.target]
adapter = "db"
catalog_template = "poc"
schema_template = "demo"
"#,
        );
        assert!(matches!(
            expect_single_error(&cfg),
            ConfigError::PipelineDiscoveryAdapterNotDiscovery { .. }
        ));
    }

    #[test]
    fn pipeline_source_reference_to_discovery_only_adapter_errors() {
        // Fivetran's role is narrowed to discovery; trying to use it as
        // a data source at `source.adapter` should be rejected at parse.
        let cfg = parse(
            r#"
[adapter.fivetran_main]
type = "fivetran"
kind = "discovery"
destination_id = "d"
api_key = "k"
api_secret = "s"

[pipeline.poc]
type = "replication"
strategy = "full_refresh"

[pipeline.poc.source]
adapter = "fivetran_main"

[pipeline.poc.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.poc.target]
adapter = "fivetran_main"
catalog_template = "poc"
schema_template = "demo"
"#,
        );
        assert!(matches!(
            expect_single_error(&cfg),
            ConfigError::PipelineSourceAdapterNotData { .. }
        ));
    }

    #[test]
    fn unknown_adapter_type_is_ignored_by_kind_validation() {
        // Unknown adapter types are reported by `rocky validate` (V017)
        // and by the registry at runtime — not by kind validation.
        let cfg = parse(
            r#"
[adapter.mystery]
type = "postgres"
"#,
        );
        assert!(validate_adapter_kinds(&cfg).is_empty());
    }

    #[test]
    fn multiple_kind_issues_are_all_collected() {
        // Two unrelated issues in the same config — both must surface
        // so `rocky validate` can emit one diagnostic per issue.
        let cfg = parse(
            r#"
[adapter.fivetran_main]
type = "fivetran"
# missing `kind = "discovery"` — issue 1
destination_id = "d"
api_key = "k"
api_secret = "s"

[adapter.db]
type = "databricks"
kind = "discovery"           # issue 2: databricks is data-only
host = "h"
http_path = "p"
token = "t"
"#,
        );
        let errors = validate_adapter_kinds(&cfg);
        assert_eq!(errors.len(), 2, "both issues should surface: {errors:?}");
        assert!(
            errors
                .iter()
                .any(|e| matches!(e, ConfigError::AdapterMissingDiscoveryKind { .. }))
        );
        assert!(
            errors
                .iter()
                .any(|e| matches!(e, ConfigError::AdapterKindUnsupported { .. }))
        );
    }

    // --- v2 config tests ---

    #[test]
    fn test_parse_v2_config() {
        let toml_str = r#"
[state]
backend = "local"

[adapter.databricks_prod]
type = "databricks"
host = "workspace.cloud.databricks.com"
http_path = "/sql/1.0/warehouses/abc123"
client_id = "client_id"
client_secret = "client_secret"
timeout_secs = 300

[adapter.fivetran_main]
type = "fivetran"
kind = "discovery"
destination_id = "dest_123"
api_key = "key_abc"
api_secret = "secret_xyz"

[pipeline.raw_replication]
type = "replication"
strategy = "incremental"
timestamp_column = "_fivetran_synced"
metadata_columns = [
    { name = "_loaded_by", type = "STRING", value = "NULL" }
]

[pipeline.raw_replication.source]
adapter = "databricks_prod"
catalog = "raw_catalog"

[pipeline.raw_replication.source.schema_pattern]
prefix = "src__"
separator = "__"
components = ["source"]

[pipeline.raw_replication.source.discovery]
adapter = "fivetran_main"

[pipeline.raw_replication.target]
adapter = "databricks_prod"
catalog_template = "warehouse"
schema_template = "raw__{source}"

[pipeline.raw_replication.target.governance]
auto_create_catalogs = true
auto_create_schemas = true

[pipeline.raw_replication.target.governance.tags]
managed_by = "rocky"

[[pipeline.raw_replication.target.governance.grants]]
principal = "group:data_engineers"
permissions = ["USE CATALOG", "MANAGE"]

[pipeline.raw_replication.checks]
enabled = true
row_count = true

[pipeline.raw_replication.execution]
concurrency = 16
fail_fast = false
"#;

        let config: RockyConfig = toml::from_str(toml_str).unwrap();

        // Adapters
        assert_eq!(config.adapters.len(), 2);
        assert_eq!(
            config.adapters["databricks_prod"].adapter_type,
            "databricks"
        );
        assert_eq!(
            config.adapters["databricks_prod"].host.as_deref(),
            Some("workspace.cloud.databricks.com")
        );
        assert_eq!(config.adapters["fivetran_main"].adapter_type, "fivetran");
        assert_eq!(
            config.adapters["fivetran_main"].destination_id.as_deref(),
            Some("dest_123")
        );

        // Pipeline
        assert_eq!(config.pipelines.len(), 1);
        let pc = &config.pipelines["raw_replication"];
        assert_eq!(pc.pipeline_type_str(), "replication");
        let pipeline = pc.as_replication().unwrap();
        assert_eq!(pipeline.strategy, "incremental");
        assert_eq!(pipeline.source.adapter, "databricks_prod");
        assert_eq!(pipeline.source.catalog.as_deref(), Some("raw_catalog"));
        assert_eq!(
            pipeline.source.discovery.as_ref().unwrap().adapter,
            "fivetran_main"
        );
        assert_eq!(pipeline.target.adapter, "databricks_prod");
        assert_eq!(pipeline.target.catalog_template, "warehouse");
        assert!(pipeline.target.governance.auto_create_catalogs);
        assert_eq!(pipeline.target.governance.grants.len(), 1);
        assert!(pipeline.checks.enabled);
        assert_eq!(pipeline.execution.concurrency, ConcurrencyMode::Fixed(16));
    }

    #[test]
    fn test_v2_multiple_pipelines() {
        let toml_str = r#"
[adapter.db1]
type = "databricks"
host = "host1"
http_path = "/path1"

[adapter.db2]
type = "databricks"
host = "host2"
http_path = "/path2"

[pipeline.bronze]
type = "replication"
strategy = "incremental"
timestamp_column = "_synced"

[pipeline.bronze.source]
adapter = "db1"

[pipeline.bronze.source.schema_pattern]
prefix = "src__"
separator = "__"
components = ["client", "connector"]

[pipeline.bronze.target]
adapter = "db1"
catalog_template = "{client}"
schema_template = "raw__{connector}"

[pipeline.silver]
type = "replication"
strategy = "full_refresh"
timestamp_column = "_synced"

[pipeline.silver.source]
adapter = "db1"

[pipeline.silver.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["connector"]

[pipeline.silver.target]
adapter = "db2"
catalog_template = "analytics"
schema_template = "silver__{connector}"
"#;

        let config: RockyConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.adapters.len(), 2);
        assert_eq!(config.pipelines.len(), 2);
        let bronze = config.pipelines["bronze"].as_replication().unwrap();
        let silver = config.pipelines["silver"].as_replication().unwrap();
        assert_eq!(bronze.source.adapter, "db1");
        assert_eq!(silver.target.adapter, "db2");
        assert_eq!(silver.strategy, "full_refresh");
    }

    #[test]
    fn test_v2_schema_pattern() {
        let toml_str = r#"
[adapter.db]
type = "databricks"
host = "h"
http_path = "/p"

[pipeline.p1]
type = "replication"

[pipeline.p1.source]
adapter = "db"

[pipeline.p1.source.schema_pattern]
prefix = "q__raw__"
separator = "__"
components = ["client", "hierarchies...", "connector"]

[pipeline.p1.target]
adapter = "db"
catalog_template = "{client}"
schema_template = "raw__{hierarchies}__{connector}"
"#;

        let config: RockyConfig = toml::from_str(toml_str).unwrap();
        let p1 = config.pipelines["p1"].as_replication().unwrap();
        let pattern = p1.schema_pattern().unwrap();
        assert_eq!(pattern.prefix, "q__raw__");
        assert_eq!(pattern.components.len(), 3);
    }

    #[test]
    fn test_target_separator_field() {
        // When target.separator is set, it should be used for template expansion
        // instead of the source pattern separator.
        let toml_str = r#"
[adapter.db]
type = "databricks"
host = "h"
http_path = "/p"

[pipeline.p1]
type = "replication"

[pipeline.p1.source]
adapter = "db"

[pipeline.p1.source.schema_pattern]
prefix = "q__raw__"
separator = "__"
components = ["client", "hierarchies...", "connector"]

[pipeline.p1.target]
adapter = "db"
catalog_template = "{client}"
schema_template = "raw_{hierarchies}_{connector}"
separator = "_"
"#;

        let config: RockyConfig = toml::from_str(toml_str).unwrap();
        let p1 = config.pipelines["p1"].as_replication().unwrap();
        assert_eq!(p1.target.separator.as_deref(), Some("_"));

        let pattern = p1.schema_pattern().unwrap();
        let parsed = pattern
            .parse("q__raw__contoso__eu__alb__redditads")
            .unwrap();
        let target_sep = p1.target.separator.as_deref().unwrap_or(&pattern.separator);
        let schema = parsed.resolve_template(&p1.target.schema_template, target_sep);
        assert_eq!(schema, "raw_eu_alb_redditads");
    }

    #[test]
    fn test_target_separator_defaults_to_source() {
        // When target.separator is omitted, falls back to source pattern separator.
        let toml_str = r#"
[adapter.db]
type = "databricks"
host = "h"
http_path = "/p"

[pipeline.p1]
type = "replication"

[pipeline.p1.source]
adapter = "db"

[pipeline.p1.source.schema_pattern]
prefix = "q__raw__"
separator = "__"
components = ["client", "hierarchies...", "connector"]

[pipeline.p1.target]
adapter = "db"
catalog_template = "{client}"
schema_template = "raw__{hierarchies}__{connector}"
"#;

        let config: RockyConfig = toml::from_str(toml_str).unwrap();
        let p1 = config.pipelines["p1"].as_replication().unwrap();
        assert_eq!(p1.target.separator, None);

        let pattern = p1.schema_pattern().unwrap();
        let parsed = pattern
            .parse("q__raw__acme__emea__france__facebook_ads")
            .unwrap();
        let target_sep = p1.target.separator.as_deref().unwrap_or(&pattern.separator);
        let schema = parsed.resolve_template(&p1.target.schema_template, target_sep);
        assert_eq!(schema, "raw__emea__france__facebook_ads");
    }

    #[test]
    fn test_cost_section_defaults_in_rocky_config() {
        let toml_str = r#"
[adapter.db]
type = "databricks"
host = "h"
http_path = "/p"

[pipeline.p1]
type = "replication"

[pipeline.p1.source]
adapter = "db"

[pipeline.p1.source.schema_pattern]
prefix = "src__"
separator = "__"
components = ["client"]

[pipeline.p1.target]
adapter = "db"
catalog_template = "{client}"
schema_template = "raw"
"#;
        let config: RockyConfig = toml::from_str(toml_str).unwrap();
        // cost section should use defaults when omitted
        assert!((config.cost.storage_cost_per_gb_month - 0.023).abs() < f64::EPSILON);
        assert!((config.cost.compute_cost_per_dbu - 0.40).abs() < f64::EPSILON);
        assert_eq!(config.cost.warehouse_size, "Medium");
        assert_eq!(config.cost.min_history_runs, 5);
    }

    #[test]
    fn test_cost_section_custom_values() {
        let toml_str = r#"
[cost]
storage_cost_per_gb_month = 0.10
compute_cost_per_dbu = 0.75
warehouse_size = "Large"
min_history_runs = 10

[adapter.db]
type = "databricks"
host = "h"
http_path = "/p"

[pipeline.p1]
type = "replication"

[pipeline.p1.source]
adapter = "db"

[pipeline.p1.source.schema_pattern]
prefix = "src__"
separator = "__"
components = ["client"]

[pipeline.p1.target]
adapter = "db"
catalog_template = "{client}"
schema_template = "raw"
"#;
        let config: RockyConfig = toml::from_str(toml_str).unwrap();
        assert!((config.cost.storage_cost_per_gb_month - 0.10).abs() < f64::EPSILON);
        assert!((config.cost.compute_cost_per_dbu - 0.75).abs() < f64::EPSILON);
        assert_eq!(config.cost.warehouse_size, "Large");
        assert_eq!(config.cost.min_history_runs, 10);
    }

    // --- workspace binding config tests ---

    #[test]
    fn test_workspace_ids_structured_objects() {
        let toml_str = r#"
enabled = true

[[workspace_ids]]
id = 12345
binding_type = "READ_WRITE"

[[workspace_ids]]
id = 67890
binding_type = "READ_ONLY"
"#;
        let config: IsolationConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.workspace_ids.len(), 2);
        assert_eq!(config.workspace_ids[0].id, 12345);
        assert_eq!(config.workspace_ids[0].binding_type, BindingType::ReadWrite);
        assert_eq!(config.workspace_ids[1].id, 67890);
        assert_eq!(config.workspace_ids[1].binding_type, BindingType::ReadOnly);
    }

    #[test]
    fn test_workspace_ids_default_binding_type() {
        let toml_str = r#"
enabled = true

[[workspace_ids]]
id = 12345
"#;
        let config: IsolationConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.workspace_ids.len(), 1);
        assert_eq!(config.workspace_ids[0].id, 12345);
        assert_eq!(config.workspace_ids[0].binding_type, BindingType::ReadWrite);
    }

    #[test]
    fn test_binding_type_api_str() {
        assert_eq!(
            BindingType::ReadWrite.as_api_str(),
            "BINDING_TYPE_READ_WRITE"
        );
        assert_eq!(BindingType::ReadOnly.as_api_str(), "BINDING_TYPE_READ_ONLY");
    }

    #[test]
    fn test_governance_override_json_structured() {
        let json = r#"{"workspace_ids": [{"id": 12345, "binding_type": "READ_ONLY"}]}"#;
        let ov: GovernanceOverride = serde_json::from_str(json).unwrap();
        let ws_ids = ov.workspace_ids.expect("workspace_ids key was present");
        assert_eq!(ws_ids.len(), 1);
        assert_eq!(ws_ids[0].id, 12345);
        assert_eq!(ws_ids[0].binding_type, BindingType::ReadOnly);
        assert!(!ov.allow_empty_workspace_ids);
    }

    /// FR-009 — payload → `validate_workspace_ids` outcome table.
    ///
    /// Covers the five rows called out in the spec:
    /// 1. `{}` — key absent → Ok (skip reconciliation).
    /// 2. `null` override not exercised here (caller passes
    ///    `Option<&GovernanceOverride>` and handles `None`).
    /// 3. `{"workspace_ids": ["ws-a"]}` → Ok (reconcile to set).
    /// 4. `{"workspace_ids": []}` → Err (silent full-revoke footgun).
    /// 5. `{"workspace_ids": [], "allow_empty_workspace_ids": true}`
    ///    → Ok (intentional full revoke).
    #[test]
    fn test_validate_workspace_ids_key_absent_is_ok() {
        let ov: GovernanceOverride = serde_json::from_str("{}").unwrap();
        assert!(ov.workspace_ids.is_none());
        ov.validate_workspace_ids("cat_a")
            .expect("key-absent is a safe no-op");
    }

    #[test]
    fn test_validate_workspace_ids_non_empty_is_ok() {
        let json = r#"{"workspace_ids": [{"id": 42, "binding_type": "READ_WRITE"}]}"#;
        let ov: GovernanceOverride = serde_json::from_str(json).unwrap();
        ov.validate_workspace_ids("cat_a")
            .expect("non-empty reconcile is safe");
    }

    #[test]
    fn test_validate_workspace_ids_empty_without_flag_errors() {
        let ov: GovernanceOverride = serde_json::from_str(r#"{"workspace_ids": []}"#).unwrap();
        let err = ov
            .validate_workspace_ids("cat_a")
            .expect_err("empty list without opt-in must error");
        let msg = err.to_string();
        assert!(
            msg.contains("cat_a"),
            "error should surface the target catalog, got {msg:?}"
        );
        assert!(
            msg.contains("allow_empty_workspace_ids"),
            "error should point at the escape hatch, got {msg:?}"
        );
    }

    #[test]
    fn test_validate_workspace_ids_empty_with_flag_is_ok() {
        let json = r#"{"workspace_ids": [], "allow_empty_workspace_ids": true}"#;
        let ov: GovernanceOverride = serde_json::from_str(json).unwrap();
        assert!(ov.allow_empty_workspace_ids);
        ov.validate_workspace_ids("cat_a")
            .expect("explicit opt-in authorises the full revoke");
    }

    /// Guard against a typo in the opt-in flag silently leaking through
    /// — the flag must be `true`, not merely present.
    #[test]
    fn test_validate_workspace_ids_empty_with_false_flag_errors() {
        let json = r#"{"workspace_ids": [], "allow_empty_workspace_ids": false}"#;
        let ov: GovernanceOverride = serde_json::from_str(json).unwrap();
        ov.validate_workspace_ids("cat_a")
            .expect_err("allow_empty_workspace_ids=false is equivalent to absent");
    }

    #[test]
    fn test_workspace_ids_in_full_pipeline_config() {
        let toml_str = r#"
[adapter.db]
type = "databricks"
host = "h"
http_path = "/p"

[pipeline.p1]
type = "replication"

[pipeline.p1.source]
adapter = "db"

[pipeline.p1.source.schema_pattern]
prefix = "src__"
separator = "__"
components = ["client"]

[pipeline.p1.target]
adapter = "db"
catalog_template = "{client}"
schema_template = "raw"

[pipeline.p1.target.governance]
auto_create_catalogs = true

[pipeline.p1.target.governance.isolation]
enabled = true

[[pipeline.p1.target.governance.isolation.workspace_ids]]
id = 111
binding_type = "READ_WRITE"

[[pipeline.p1.target.governance.isolation.workspace_ids]]
id = 222
binding_type = "READ_ONLY"
"#;
        let config: RockyConfig = toml::from_str(toml_str).unwrap();
        let p1 = config.pipelines["p1"].as_replication().unwrap();
        let isolation = p1.target.governance.isolation.as_ref().unwrap();
        assert!(isolation.enabled);
        assert_eq!(isolation.workspace_ids.len(), 2);
        assert_eq!(isolation.workspace_ids[0].id, 111);
        assert_eq!(
            isolation.workspace_ids[0].binding_type,
            BindingType::ReadWrite
        );
        assert_eq!(isolation.workspace_ids[1].id, 222);
        assert_eq!(
            isolation.workspace_ids[1].binding_type,
            BindingType::ReadOnly
        );
    }

    // --- Shorthand form tests ---

    #[test]
    fn test_bare_adapter_wraps_as_default() {
        let toml_str = r#"
[adapter]
type = "duckdb"
path = "test.duckdb"

[pipeline.poc]
source.schema_pattern = { prefix = "raw__", separator = "__", components = ["source"] }
target = { catalog_template = "main", schema_template = "staging" }
"#;
        let mut value: toml::Value = toml::from_str(toml_str).unwrap();
        normalize_toml_shorthands(&mut value);
        let config: RockyConfig = value.try_into().unwrap();
        assert_eq!(config.adapters.len(), 1);
        assert!(config.adapters.contains_key("default"));
        assert_eq!(config.adapters["default"].adapter_type, "duckdb");
        assert_eq!(
            config.adapters["default"].path.as_deref(),
            Some("test.duckdb")
        );
    }

    #[test]
    fn test_named_adapter_not_wrapped() {
        // Named adapters ([adapter.foo]) must NOT be wrapped
        let toml_str = r#"
[adapter.local]
type = "duckdb"

[pipeline.poc]
source.adapter = "local"
source.schema_pattern = { prefix = "raw__", separator = "__", components = ["source"] }
target.adapter = "local"
target.catalog_template = "main"
target.schema_template = "staging"
"#;
        let mut value: toml::Value = toml::from_str(toml_str).unwrap();
        normalize_toml_shorthands(&mut value);
        let config: RockyConfig = value.try_into().unwrap();
        assert_eq!(config.adapters.len(), 1);
        assert!(config.adapters.contains_key("local"));
        assert!(!config.adapters.contains_key("default"));
    }

    #[test]
    fn test_pipeline_type_defaults_to_replication() {
        let toml_str = r#"
[adapter.local]
type = "duckdb"

[pipeline.poc]
source.adapter = "local"
source.schema_pattern = { prefix = "raw__", separator = "__", components = ["source"] }
target.adapter = "local"
target.catalog_template = "main"
target.schema_template = "staging"
"#;
        let config: RockyConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.pipelines["poc"].pipeline_type_str(), "replication");
    }

    #[test]
    fn test_adapter_ref_defaults_to_default() {
        // When adapter refs are omitted, they should default to "default"
        let toml_str = r#"
[adapter]
type = "duckdb"

[pipeline.poc]
source.schema_pattern = { prefix = "raw__", separator = "__", components = ["source"] }
target = { catalog_template = "main", schema_template = "staging" }
"#;
        let mut value: toml::Value = toml::from_str(toml_str).unwrap();
        normalize_toml_shorthands(&mut value);
        let config: RockyConfig = value.try_into().unwrap();
        let poc = config.pipelines["poc"].as_replication().unwrap();
        assert_eq!(poc.source.adapter, "default");
        assert_eq!(poc.target.adapter, "default");
    }

    #[test]
    fn test_bare_pipeline_wraps_as_default() {
        let toml_str = r#"
[adapter]
type = "duckdb"

[pipeline]
source.schema_pattern = { prefix = "raw__", separator = "__", components = ["source"] }
target = { catalog_template = "main", schema_template = "staging" }
"#;
        let mut value: toml::Value = toml::from_str(toml_str).unwrap();
        normalize_toml_shorthands(&mut value);
        let config: RockyConfig = value.try_into().unwrap();
        assert_eq!(config.pipelines.len(), 1);
        assert!(config.pipelines.contains_key("default"));
        assert_eq!(
            config.pipelines["default"].pipeline_type_str(),
            "replication"
        );
    }

    #[test]
    fn test_minimal_duckdb_config() {
        // The smallest possible Rocky config: unnamed adapter, unnamed pipeline,
        // all defaults. This is what the new `rocky init` should produce.
        let toml_str = r#"
[adapter]
type = "duckdb"
path = "playground.duckdb"

[pipeline]
source.schema_pattern = { prefix = "raw__", separator = "__", components = ["source"] }
target = { catalog_template = "main", schema_template = "staging__{source}" }
"#;
        let mut value: toml::Value = toml::from_str(toml_str).unwrap();
        normalize_toml_shorthands(&mut value);
        let config: RockyConfig = value.try_into().unwrap();

        assert_eq!(config.adapters.len(), 1);
        assert_eq!(config.adapters["default"].adapter_type, "duckdb");
        assert_eq!(config.pipelines.len(), 1);
        assert_eq!(
            config.pipelines["default"].pipeline_type_str(),
            "replication"
        );
        let default_pipeline = config.pipelines["default"].as_replication().unwrap();
        assert_eq!(default_pipeline.source.adapter, "default");
        assert_eq!(default_pipeline.target.adapter, "default");
        assert_eq!(default_pipeline.target.schema_template, "staging__{source}");
        // When [state] is completely omitted, StateConfig::default() gives Local backend.
        assert_eq!(config.state.backend, StateBackend::Local);
    }

    /// Single-adapter shorthand with `[source.discovery]` entirely absent
    /// should auto-wire discovery to the lone adapter — `parse_rocky_config`
    /// materializes `DiscoveryConfig { adapter = "default" }` so callers
    /// like `rocky compact --catalog` don't have to special-case "no
    /// discovery configured" when the project shape is unambiguous.
    #[test]
    fn test_single_adapter_autowires_discovery() {
        use std::io::Write;

        let toml_str = r#"
[adapter]
type = "duckdb"
path = ":memory:"

[pipeline.poc]
source.schema_pattern = { prefix = "raw__", separator = "__", components = ["source"] }
target = { catalog_template = "main", schema_template = "staging" }
"#;
        let mut tmp = tempfile::NamedTempFile::new().expect("tempfile");
        tmp.write_all(toml_str.as_bytes()).expect("write");
        let config = parse_rocky_config(tmp.path()).expect("parse");

        let poc = config.pipelines["poc"]
            .as_replication()
            .expect("replication");
        let discovery = poc.source.discovery.as_ref().expect("discovery auto-wired");
        assert_eq!(discovery.adapter, "default");
    }

    /// Auto-wire follows the actual adapter name, not the literal
    /// `"default"`. A project that writes `[adapter.local]` explicitly
    /// must auto-wire discovery to `"local"` — otherwise `source.adapter`
    /// (which serde defaults to `"default"`) is in conflict with a
    /// hardcoded `"default"` discovery ref and the downstream registry
    /// fails with `V022` "no adapter named 'default'".
    #[test]
    fn test_single_named_adapter_autowires_to_its_name() {
        use std::io::Write;

        let toml_str = r#"
[adapter.local]
type = "duckdb"
path = ":memory:"

[pipeline.poc]
[pipeline.poc.source]
adapter = "local"
schema_pattern = { prefix = "raw__", separator = "__", components = ["source"] }

[pipeline.poc.target]
adapter = "local"
catalog_template = "main"
schema_template = "staging"
"#;
        let mut tmp = tempfile::NamedTempFile::new().expect("tempfile");
        tmp.write_all(toml_str.as_bytes()).expect("write");
        let config = parse_rocky_config(tmp.path()).expect("parse");

        let poc = config.pipelines["poc"]
            .as_replication()
            .expect("replication");
        let discovery = poc.source.discovery.as_ref().expect("discovery auto-wired");
        assert_eq!(discovery.adapter, "local");
    }

    /// Multi-adapter project: `[source.discovery]` absent must stay
    /// `None` so the existing "no discovery adapter configured" code
    /// path keeps its signal — refusing to guess which adapter handles
    /// discovery in an ambiguous setup is the safe behavior.
    #[test]
    fn test_multi_adapter_does_not_autowire_discovery() {
        use std::io::Write;

        let toml_str = r#"
[adapter.warehouse]
type = "duckdb"
path = ":memory:"

[adapter.source_fetch]
type = "fivetran"
kind = "discovery"
destination_id = "d"
api_key = "k"
api_secret = "s"

[pipeline.poc]
[pipeline.poc.source]
adapter = "warehouse"
schema_pattern = { prefix = "raw__", separator = "__", components = ["source"] }

[pipeline.poc.target]
adapter = "warehouse"
catalog_template = "main"
schema_template = "staging"
"#;
        let mut tmp = tempfile::NamedTempFile::new().expect("tempfile");
        tmp.write_all(toml_str.as_bytes()).expect("write");
        let config = parse_rocky_config(tmp.path()).expect("parse");

        let poc = config.pipelines["poc"]
            .as_replication()
            .expect("replication");
        assert!(
            poc.source.discovery.is_none(),
            "multi-adapter project must not auto-wire discovery"
        );
    }

    /// When the user did supply `[source.discovery]` explicitly, the
    /// auto-wire pass must leave it alone — even on a single-adapter
    /// project — so an operator-chosen discovery adapter never gets
    /// silently replaced.
    #[test]
    fn test_explicit_discovery_not_overwritten() {
        use std::io::Write;

        let toml_str = r#"
[adapter]
type = "duckdb"
path = ":memory:"

[pipeline.poc]
source.schema_pattern = { prefix = "raw__", separator = "__", components = ["source"] }
source.discovery = { adapter = "default" }
target = { catalog_template = "main", schema_template = "staging" }
"#;
        let mut tmp = tempfile::NamedTempFile::new().expect("tempfile");
        tmp.write_all(toml_str.as_bytes()).expect("write");
        let config = parse_rocky_config(tmp.path()).expect("parse");

        let poc = config.pipelines["poc"]
            .as_replication()
            .expect("replication");
        assert_eq!(
            poc.source.discovery.as_ref().expect("present").adapter,
            "default"
        );
    }

    #[test]
    fn test_unsupported_pipeline_type_errors() {
        let toml_str = r#"
[adapter.db]
type = "duckdb"

[pipeline.p1]
type = "unknown_type"

[pipeline.p1.source]
adapter = "db"
[pipeline.p1.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.p1.target]
adapter = "db"
catalog_template = "main"
schema_template = "staging"
"#;
        let result: Result<RockyConfig, _> = toml::from_str(toml_str);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("unsupported pipeline type"),
            "error was: {err}"
        );
    }

    #[test]
    fn test_parse_transformation_pipeline() {
        let toml_str = r#"
[adapter.db]
type = "duckdb"

[pipeline.silver]
type = "transformation"
models = "models/silver/**"

[pipeline.silver.target]
adapter = "db"

[pipeline.silver.target.governance]
auto_create_schemas = true

[pipeline.silver.checks]
enabled = true
freshness = { threshold_seconds = 3600 }

[pipeline.silver.execution]
concurrency = 4
fail_fast = true
"#;
        let config: RockyConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.pipelines.len(), 1);

        let pc = &config.pipelines["silver"];
        assert_eq!(pc.pipeline_type_str(), "transformation");
        assert!(pc.as_replication().is_none());

        let t = pc.as_transformation().unwrap();
        assert_eq!(t.models, "models/silver/**");
        assert_eq!(t.target.adapter, "db");
        assert!(t.target.governance.auto_create_schemas);
        assert!(t.checks.enabled);
        assert_eq!(t.checks.freshness.as_ref().unwrap().threshold_seconds, 3600);
        assert_eq!(t.execution.concurrency, ConcurrencyMode::Fixed(4));
        assert!(t.execution.fail_fast);
    }

    #[test]
    fn test_transformation_pipeline_defaults() {
        let toml_str = r#"
[adapter.db]
type = "duckdb"

[pipeline.transforms]
type = "transformation"

[pipeline.transforms.target]
adapter = "db"
"#;
        let config: RockyConfig = toml::from_str(toml_str).unwrap();
        let t = config.pipelines["transforms"].as_transformation().unwrap();
        assert_eq!(t.models, "models/**");
        assert_eq!(t.execution.concurrency, ConcurrencyMode::Adaptive);
        assert!(!t.execution.fail_fast);
        assert!(!t.checks.enabled);
    }

    #[test]
    fn test_mixed_replication_and_transformation_pipelines() {
        let toml_str = r#"
[adapter.db]
type = "duckdb"

[pipeline.raw]
type = "replication"
strategy = "full_refresh"

[pipeline.raw.source]
adapter = "db"
[pipeline.raw.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.raw.target]
adapter = "db"
catalog_template = "main"
schema_template = "staging__{source}"

[pipeline.silver]
type = "transformation"
models = "models/**"

[pipeline.silver.target]
adapter = "db"
"#;
        let config: RockyConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.pipelines.len(), 2);
        assert!(config.pipelines["raw"].as_replication().is_some());
        assert!(config.pipelines["silver"].as_transformation().is_some());

        // Shared accessors work on both
        assert_eq!(config.pipelines["raw"].pipeline_type_str(), "replication");
        assert_eq!(
            config.pipelines["silver"].pipeline_type_str(),
            "transformation"
        );
        assert_eq!(config.pipelines["raw"].target_adapter(), "db");
        assert_eq!(config.pipelines["silver"].target_adapter(), "db");
    }

    #[test]
    fn test_bare_transformation_pipeline() {
        // A bare [pipeline] with type = "transformation" should be auto-wrapped
        let toml_str = r#"
[adapter]
type = "duckdb"

[pipeline]
type = "transformation"

[pipeline.target]
adapter = "default"
"#;
        let mut value: toml::Value = toml::from_str(toml_str).unwrap();
        normalize_toml_shorthands(&mut value);
        let config: RockyConfig = value.try_into().unwrap();
        assert_eq!(config.pipelines.len(), 1);
        assert!(config.pipelines.contains_key("default"));
        assert_eq!(
            config.pipelines["default"].pipeline_type_str(),
            "transformation"
        );
    }

    // --- Quality pipeline tests ---

    #[test]
    fn test_parse_quality_pipeline() {
        let toml_str = r#"
[adapter.db]
type = "duckdb"

[pipeline.nightly_dq]
type = "quality"

[pipeline.nightly_dq.target]
adapter = "db"

[[pipeline.nightly_dq.tables]]
catalog = "main"
schema = "raw__shopify"
table = "orders"

[[pipeline.nightly_dq.tables]]
catalog = "main"
schema = "raw__stripe"

[pipeline.nightly_dq.checks]
enabled = true
row_count = true
custom = [
    { name = "no_nulls", sql = "SELECT COUNT(*) FROM {table} WHERE id IS NOT NULL", threshold = 1 }
]

[pipeline.nightly_dq.execution]
concurrency = 2
fail_fast = true
"#;
        let config: RockyConfig = toml::from_str(toml_str).unwrap();
        let pc = &config.pipelines["nightly_dq"];
        assert_eq!(pc.pipeline_type_str(), "quality");

        let q = pc.as_quality().unwrap();
        assert_eq!(q.target.adapter, "db");
        assert_eq!(q.tables.len(), 2);
        assert_eq!(q.tables[0].catalog, "main");
        assert_eq!(q.tables[0].table.as_deref(), Some("orders"));
        assert!(q.tables[1].table.is_none()); // all tables in schema
        assert!(q.checks.enabled);
        assert!(q.checks.row_count.enabled());
        assert_eq!(q.checks.custom.len(), 1);
        assert_eq!(q.execution.concurrency, ConcurrencyMode::Fixed(2));
    }

    // --- Phase 1+2: unified check surface + severity ---

    #[test]
    fn test_parse_quality_pipeline_with_assertions() {
        let toml_str = r#"
[adapter.db]
type = "duckdb"
path = "poc.duckdb"

[pipeline.nightly_dq]
type = "quality"

[pipeline.nightly_dq.target]
adapter = "db"

[[pipeline.nightly_dq.tables]]
catalog = "poc"
schema = "staging"
table = "orders"

[pipeline.nightly_dq.checks]
enabled = true
row_count = true

[[pipeline.nightly_dq.checks.assertions]]
table = "orders"
type = "not_null"
column = "customer_id"

[[pipeline.nightly_dq.checks.assertions]]
name = "orders_status_allowed"
table = "orders"
type = "accepted_values"
column = "status"
values = ["pending", "shipped"]
severity = "warning"

[[pipeline.nightly_dq.checks.assertions]]
table = "orders"
type = "expression"
expression = "total >= 0"
"#;
        let config: RockyConfig = toml::from_str(toml_str).unwrap();
        let q = config.pipelines["nightly_dq"].as_quality().unwrap();
        assert_eq!(q.checks.assertions.len(), 3);
        assert_eq!(q.checks.assertions[0].table, "orders");
        assert!(q.checks.assertions[0].name.is_none());
        assert!(matches!(
            q.checks.assertions[0].test.test_type,
            crate::tests::TestType::NotNull
        ));
        assert_eq!(
            q.checks.assertions[1].name.as_deref(),
            Some("orders_status_allowed")
        );
        assert_eq!(
            q.checks.assertions[1].test.severity,
            crate::tests::TestSeverity::Warning
        );
        assert!(matches!(
            q.checks.assertions[2].test.test_type,
            crate::tests::TestType::Expression { .. }
        ));
    }

    #[test]
    fn test_aggregate_check_toggle_bool_legacy() {
        let toml_str = r#"
[adapter.db]
type = "duckdb"
path = "p"

[pipeline.q]
type = "quality"

[pipeline.q.target]
adapter = "db"

[[pipeline.q.tables]]
catalog = "c"
schema = "s"

[pipeline.q.checks]
enabled = true
row_count = true
column_match = false
"#;
        let config: RockyConfig = toml::from_str(toml_str).unwrap();
        let q = config.pipelines["q"].as_quality().unwrap();
        assert!(q.checks.row_count.enabled());
        assert_eq!(
            q.checks.row_count.severity(),
            crate::tests::TestSeverity::Error
        );
        assert!(!q.checks.column_match.enabled());
    }

    #[test]
    fn test_aggregate_check_toggle_detailed() {
        let toml_str = r#"
[adapter.db]
type = "duckdb"
path = "p"

[pipeline.q]
type = "quality"

[pipeline.q.target]
adapter = "db"

[[pipeline.q.tables]]
catalog = "c"
schema = "s"

[pipeline.q.checks]
enabled = true

[pipeline.q.checks.row_count]
enabled = true
severity = "warning"
"#;
        let config: RockyConfig = toml::from_str(toml_str).unwrap();
        let q = config.pipelines["q"].as_quality().unwrap();
        assert!(q.checks.row_count.enabled());
        assert_eq!(
            q.checks.row_count.severity(),
            crate::tests::TestSeverity::Warning
        );
    }

    #[test]
    fn test_fail_on_error_defaults_true() {
        let toml_str = r#"
[adapter.db]
type = "duckdb"
path = "p"

[pipeline.q]
type = "quality"

[pipeline.q.target]
adapter = "db"

[[pipeline.q.tables]]
catalog = "c"
schema = "s"

[pipeline.q.checks]
enabled = true
"#;
        let config: RockyConfig = toml::from_str(toml_str).unwrap();
        let q = config.pipelines["q"].as_quality().unwrap();
        assert!(q.checks.fail_on_error);
    }

    #[test]
    fn test_fail_on_error_false_escape_hatch() {
        let toml_str = r#"
[adapter.db]
type = "duckdb"
path = "p"

[pipeline.q]
type = "quality"

[pipeline.q.target]
adapter = "db"

[[pipeline.q.tables]]
catalog = "c"
schema = "s"

[pipeline.q.checks]
enabled = true
fail_on_error = false
"#;
        let config: RockyConfig = toml::from_str(toml_str).unwrap();
        let q = config.pipelines["q"].as_quality().unwrap();
        assert!(!q.checks.fail_on_error);
    }

    // --- Snapshot pipeline tests ---

    #[test]
    fn test_parse_snapshot_pipeline() {
        let toml_str = r#"
[adapter.db]
type = "duckdb"

[pipeline.customer_history]
type = "snapshot"
unique_key = ["customer_id"]
updated_at = "updated_at"
invalidate_hard_deletes = true

[pipeline.customer_history.source]
adapter = "db"
catalog = "raw"
schema = "shopify"
table = "customers"

[pipeline.customer_history.target]
adapter = "db"
catalog = "warehouse"
schema = "scd"
table = "customers_history"

[pipeline.customer_history.target.governance]
auto_create_schemas = true

[pipeline.customer_history.execution]
concurrency = 1
"#;
        let config: RockyConfig = toml::from_str(toml_str).unwrap();
        let pc = &config.pipelines["customer_history"];
        assert_eq!(pc.pipeline_type_str(), "snapshot");

        let s = pc.as_snapshot().unwrap();
        assert_eq!(s.unique_key, vec!["customer_id"]);
        assert_eq!(s.updated_at, "updated_at");
        assert!(s.invalidate_hard_deletes);
        assert_eq!(s.source.catalog, "raw");
        assert_eq!(s.source.table, "customers");
        assert_eq!(s.target.catalog, "warehouse");
        assert_eq!(s.target.table, "customers_history");
        assert!(s.target.governance.auto_create_schemas);
        assert_eq!(s.execution.concurrency, ConcurrencyMode::Fixed(1));
    }

    // --- Pipeline chaining (depends_on) tests ---

    #[test]
    fn test_pipeline_depends_on() {
        let toml_str = r#"
[adapter.db]
type = "duckdb"

[pipeline.raw]
type = "replication"
strategy = "full_refresh"

[pipeline.raw.source]
adapter = "db"
[pipeline.raw.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.raw.target]
adapter = "db"
catalog_template = "main"
schema_template = "staging__{source}"

[pipeline.silver]
type = "transformation"
depends_on = ["raw"]

[pipeline.silver.target]
adapter = "db"

[pipeline.gold]
type = "transformation"
models = "models/gold/**"
depends_on = ["silver"]

[pipeline.gold.target]
adapter = "db"
"#;
        let config: RockyConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.pipelines.len(), 3);

        let raw = &config.pipelines["raw"];
        assert!(raw.depends_on().is_empty());

        let silver = &config.pipelines["silver"];
        assert_eq!(silver.depends_on(), &["raw"]);

        let gold = &config.pipelines["gold"];
        assert_eq!(gold.depends_on(), &["silver"]);
    }

    #[test]
    fn test_all_four_pipeline_types() {
        let toml_str = r#"
[adapter.db]
type = "duckdb"

[pipeline.ingest]
type = "replication"

[pipeline.ingest.source]
adapter = "db"
[pipeline.ingest.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.ingest.target]
adapter = "db"
catalog_template = "main"
schema_template = "staging"

[pipeline.transform]
type = "transformation"
depends_on = ["ingest"]

[pipeline.transform.target]
adapter = "db"

[pipeline.validate]
type = "quality"
depends_on = ["transform"]

[pipeline.validate.target]
adapter = "db"

[[pipeline.validate.tables]]
catalog = "main"
schema = "staging"

[pipeline.validate.checks]
enabled = true
row_count = true

[pipeline.history]
type = "snapshot"
unique_key = ["id"]
updated_at = "modified_at"

[pipeline.history.source]
adapter = "db"
catalog = "main"
schema = "staging"
table = "customers"

[pipeline.history.target]
adapter = "db"
catalog = "main"
schema = "scd"
table = "customers_history"
"#;
        let config: RockyConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.pipelines.len(), 4);
        assert_eq!(
            config.pipelines["ingest"].pipeline_type_str(),
            "replication"
        );
        assert_eq!(
            config.pipelines["transform"].pipeline_type_str(),
            "transformation"
        );
        assert_eq!(config.pipelines["validate"].pipeline_type_str(), "quality");
        assert_eq!(config.pipelines["history"].pipeline_type_str(), "snapshot");

        // All share target_adapter accessor
        for pc in config.pipelines.values() {
            assert_eq!(pc.target_adapter(), "db");
        }
    }

    // --- Load pipeline tests ---

    #[test]
    fn test_parse_load_pipeline() {
        let toml_str = r#"
[adapter.prod]
type = "databricks"
host = "h"
http_path = "/p"

[pipeline.load_data]
type = "load"
source_dir = "data/"
format = "csv"

[pipeline.load_data.target]
adapter = "prod"
catalog = "warehouse"
schema = "raw"

[pipeline.load_data.options]
batch_size = 5000
create_table = true
truncate_first = true
csv_delimiter = "\t"
csv_has_header = false

[pipeline.load_data.checks]
enabled = true
row_count = true

[pipeline.load_data.execution]
concurrency = 2
"#;
        let config: RockyConfig = toml::from_str(toml_str).unwrap();
        let pc = &config.pipelines["load_data"];
        assert_eq!(pc.pipeline_type_str(), "load");

        let l = pc.as_load().unwrap();
        assert_eq!(l.source_dir, "data/");
        assert_eq!(l.format, Some(LoadFileFormat::Csv));
        assert_eq!(l.target.adapter, "prod");
        assert_eq!(l.target.catalog, "warehouse");
        assert_eq!(l.target.schema, "raw");
        assert!(l.target.table.is_none());
        assert_eq!(l.options.batch_size, 5000);
        assert!(l.options.create_table);
        assert!(l.options.truncate_first);
        assert_eq!(l.options.csv_delimiter, "\t");
        assert!(!l.options.csv_has_header);
        assert!(l.checks.enabled);
        assert!(l.checks.row_count.enabled());
        assert_eq!(l.execution.concurrency, ConcurrencyMode::Fixed(2));
    }

    #[test]
    fn test_load_pipeline_defaults() {
        let toml_str = r#"
[adapter.db]
type = "duckdb"

[pipeline.ingest]
type = "load"
source_dir = "files/"

[pipeline.ingest.target]
adapter = "db"
catalog = "main"
schema = "staging"
"#;
        let config: RockyConfig = toml::from_str(toml_str).unwrap();
        let l = config.pipelines["ingest"].as_load().unwrap();
        assert_eq!(l.source_dir, "files/");
        assert!(l.format.is_none());
        assert_eq!(l.target.adapter, "db");
        assert!(l.target.table.is_none());
        // Options defaults
        assert_eq!(l.options.batch_size, 10_000);
        assert!(l.options.create_table);
        assert!(!l.options.truncate_first);
        assert_eq!(l.options.csv_delimiter, ",");
        assert!(l.options.csv_has_header);
        // Checks default to disabled
        assert!(!l.checks.enabled);
        assert_eq!(l.execution.concurrency, ConcurrencyMode::Adaptive);
    }

    #[test]
    fn test_load_pipeline_with_explicit_table() {
        let toml_str = r#"
[adapter.db]
type = "duckdb"

[pipeline.load_orders]
type = "load"
source_dir = "data/orders/"
format = "parquet"

[pipeline.load_orders.target]
adapter = "db"
catalog = "warehouse"
schema = "raw"
table = "orders"
"#;
        let config: RockyConfig = toml::from_str(toml_str).unwrap();
        let l = config.pipelines["load_orders"].as_load().unwrap();
        assert_eq!(l.format, Some(LoadFileFormat::Parquet));
        assert_eq!(l.target.table.as_deref(), Some("orders"));
    }

    #[test]
    fn test_load_pipeline_jsonlines_format() {
        let toml_str = r#"
[adapter.db]
type = "duckdb"

[pipeline.events]
type = "load"
source_dir = "logs/"
format = "json_lines"

[pipeline.events.target]
adapter = "db"
catalog = "analytics"
schema = "raw"
"#;
        let config: RockyConfig = toml::from_str(toml_str).unwrap();
        let l = config.pipelines["events"].as_load().unwrap();
        assert_eq!(l.format, Some(LoadFileFormat::JsonLines));
    }

    #[test]
    fn test_load_pipeline_depends_on() {
        let toml_str = r#"
[adapter.db]
type = "duckdb"

[pipeline.load_raw]
type = "load"
source_dir = "data/"

[pipeline.load_raw.target]
adapter = "db"
catalog = "main"
schema = "raw"

[pipeline.transform]
type = "transformation"
depends_on = ["load_raw"]

[pipeline.transform.target]
adapter = "db"
"#;
        let config: RockyConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.pipelines["load_raw"].depends_on(), &[] as &[String]);
        assert_eq!(config.pipelines["transform"].depends_on(), &["load_raw"]);
    }

    #[test]
    fn test_load_file_format_display() {
        assert_eq!(LoadFileFormat::Csv.to_string(), "csv");
        assert_eq!(LoadFileFormat::Parquet.to_string(), "parquet");
        assert_eq!(LoadFileFormat::JsonLines.to_string(), "json_lines");
    }

    #[test]
    fn test_bare_load_pipeline_wraps_as_default() {
        let toml_str = r#"
[adapter]
type = "duckdb"

[pipeline]
type = "load"
source_dir = "data/"

[pipeline.target]
catalog = "main"
schema = "raw"
"#;
        let mut value: toml::Value = toml::from_str(toml_str).unwrap();
        normalize_toml_shorthands(&mut value);
        let config: RockyConfig = value.try_into().unwrap();
        assert_eq!(config.pipelines.len(), 1);
        assert!(config.pipelines.contains_key("default"));
        assert_eq!(config.pipelines["default"].pipeline_type_str(), "load");
    }

    #[test]
    fn test_all_five_pipeline_types_together() {
        let toml_str = r#"
[adapter.db]
type = "duckdb"

[pipeline.ingest]
type = "replication"

[pipeline.ingest.source]
adapter = "db"
[pipeline.ingest.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.ingest.target]
adapter = "db"
catalog_template = "main"
schema_template = "staging"

[pipeline.load_files]
type = "load"
source_dir = "data/"

[pipeline.load_files.target]
adapter = "db"
catalog = "main"
schema = "raw"

[pipeline.transform]
type = "transformation"
depends_on = ["ingest", "load_files"]

[pipeline.transform.target]
adapter = "db"

[pipeline.validate]
type = "quality"
depends_on = ["transform"]

[pipeline.validate.target]
adapter = "db"

[[pipeline.validate.tables]]
catalog = "main"
schema = "staging"

[pipeline.validate.checks]
enabled = true
row_count = true

[pipeline.history]
type = "snapshot"
unique_key = ["id"]
updated_at = "modified_at"

[pipeline.history.source]
adapter = "db"
catalog = "main"
schema = "staging"
table = "customers"

[pipeline.history.target]
adapter = "db"
catalog = "main"
schema = "scd"
table = "customers_history"
"#;
        let config: RockyConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.pipelines.len(), 5);
        assert_eq!(
            config.pipelines["ingest"].pipeline_type_str(),
            "replication"
        );
        assert_eq!(config.pipelines["load_files"].pipeline_type_str(), "load");
        assert_eq!(
            config.pipelines["transform"].pipeline_type_str(),
            "transformation"
        );
        assert_eq!(config.pipelines["validate"].pipeline_type_str(), "quality");
        assert_eq!(config.pipelines["history"].pipeline_type_str(), "snapshot");

        // All share target_adapter accessor
        for pc in config.pipelines.values() {
            assert_eq!(pc.target_adapter(), "db");
        }

        // Load-specific assertions
        let load = config.pipelines["load_files"].as_load().unwrap();
        assert_eq!(load.source_dir, "data/");
        assert_eq!(load.target.catalog, "main");
        assert_eq!(load.target.schema, "raw");
    }

    #[test]
    fn adapter_config_debug_hides_secrets() {
        let cfg = AdapterConfig {
            adapter_type: "databricks".into(),
            kind: None,
            host: Some("workspace.cloud.databricks.com".into()),
            http_path: Some("/sql/1.0/warehouses/abc".into()),
            token: Some(RedactedString::new("dapi_SUPER_SECRET_TOKEN".into())),
            client_id: Some("client_123".into()),
            client_secret: Some(RedactedString::new("oauth_SECRET_VALUE".into())),
            timeout_secs: Some(120),
            destination_id: None,
            api_key: Some(RedactedString::new("fivetran_KEY_SECRET".into())),
            api_secret: Some(RedactedString::new("fivetran_SECRET_VALUE".into())),
            account: None,
            warehouse: None,
            username: None,
            password: Some(RedactedString::new("db_PASSWORD_123".into())),
            oauth_token: Some(RedactedString::new("oauth_TOKEN_XYZ".into())),
            private_key_path: None,
            pat: None,
            role: None,
            database: None,
            project_id: None,
            location: None,
            path: None,
            retry: RetryConfig::default(),
            cache: None,
            ratelimit: None,
            stampede: None,
            circuit_breaker: None,
            extra: std::collections::BTreeMap::new(),
        };

        let debug = format!("{cfg:?}");

        // Secrets must NOT appear in Debug output.
        assert!(
            !debug.contains("dapi_SUPER_SECRET_TOKEN"),
            "token leaked: {debug}"
        );
        assert!(
            !debug.contains("oauth_SECRET_VALUE"),
            "client_secret leaked: {debug}"
        );
        assert!(
            !debug.contains("fivetran_KEY_SECRET"),
            "api_key leaked: {debug}"
        );
        assert!(
            !debug.contains("fivetran_SECRET_VALUE"),
            "api_secret leaked: {debug}"
        );
        assert!(
            !debug.contains("db_PASSWORD_123"),
            "password leaked: {debug}"
        );
        assert!(
            !debug.contains("oauth_TOKEN_XYZ"),
            "oauth_token leaked: {debug}"
        );

        // Redaction placeholder must appear for each secret field.
        assert!(debug.contains("***"), "expected *** in debug output");

        // Non-secret fields must still be visible.
        assert!(
            debug.contains("workspace.cloud.databricks.com"),
            "host missing: {debug}"
        );
        assert!(debug.contains("client_123"), "client_id missing: {debug}");
    }

    // ---------------------------------------------------------------
    // AdapterConfig.extra — escape hatch for adapter-specific TOML keys
    // ---------------------------------------------------------------
    //
    // The `extra` field exists so out-of-tree adapters (e.g. rocky-trino
    // wanting `default_schema`, an arbitrary adapter wanting an
    // `x_trino_user`-style mandatory header value) can carry adapter-
    // specific config without the `deny_unknown_fields` guard rejecting
    // every such file. The tests below pin the contract:
    //
    // 1. Known top-level fields parse alongside `[extra]` content.
    // 2. Top-level typos are still rejected (deny_unknown_fields kept).
    // 3. `[extra]` accepts string / int / bool / table values
    //    (TOML scalars round-trip through serde_json::Value).
    // 4. Missing `[extra]` parses as an empty map (`#[serde(default)]`).
    // 5. Empty `extra` round-trips without leaking an empty TOML table.

    #[test]
    fn adapter_extra_carries_through_unknown_keys() {
        let toml_str = r#"
type = "trino"
host = "https://trino.example.com"
token = "jwt-token-here"

[extra]
default_schema = "analytics"
x_trino_user = "service-account"
"#;
        let cfg: AdapterConfig = toml::from_str(toml_str).expect("extra keys should parse");
        assert_eq!(cfg.adapter_type, "trino");
        assert_eq!(
            cfg.extra
                .get("default_schema")
                .and_then(serde_json::Value::as_str),
            Some("analytics"),
        );
        assert_eq!(
            cfg.extra
                .get("x_trino_user")
                .and_then(serde_json::Value::as_str),
            Some("service-account"),
        );
    }

    #[test]
    fn adapter_extra_does_not_disable_top_level_typo_detection() {
        // A typo at the top level must still be rejected — `extra` is the
        // *only* escape hatch, not a global pass-through.
        let toml_str = r#"
type = "trino"
host = "https://trino.example.com"
tooken = "this-is-a-typo"
"#;
        let result: Result<AdapterConfig, _> = toml::from_str(toml_str);
        assert!(
            result.is_err(),
            "top-level typos must still fail with deny_unknown_fields"
        );
    }

    #[test]
    fn adapter_extra_accepts_mixed_value_types() {
        let toml_str = r#"
type = "custom"

[extra]
str_key = "hello"
int_key = 42
bool_key = true
"#;
        let cfg: AdapterConfig = toml::from_str(toml_str).expect("mixed-type extra should parse");
        assert_eq!(
            cfg.extra.get("str_key").and_then(serde_json::Value::as_str),
            Some("hello")
        );
        assert_eq!(
            cfg.extra.get("int_key").and_then(serde_json::Value::as_i64),
            Some(42)
        );
        assert_eq!(
            cfg.extra
                .get("bool_key")
                .and_then(serde_json::Value::as_bool),
            Some(true)
        );
    }

    #[test]
    fn adapter_extra_accepts_nested_table() {
        // Nested tables under [extra] flow through as JSON objects, so
        // adapters can carry structured config (auth profiles, region
        // maps, etc.) without inventing a top-level key per shape.
        let toml_str = r#"
type = "custom"

[extra.auth]
mode = "jwt"
issuer = "https://idp.example.com"
"#;
        let cfg: AdapterConfig = toml::from_str(toml_str).expect("nested table should parse");
        let auth = cfg.extra.get("auth").expect("auth sub-table present");
        assert_eq!(
            auth.get("mode").and_then(serde_json::Value::as_str),
            Some("jwt")
        );
        assert_eq!(
            auth.get("issuer").and_then(serde_json::Value::as_str),
            Some("https://idp.example.com")
        );
    }

    #[test]
    fn adapter_extra_defaults_to_empty_when_absent() {
        let toml_str = r#"
type = "duckdb"
path = ":memory:"
"#;
        let cfg: AdapterConfig =
            toml::from_str(toml_str).expect("config without extra should parse");
        assert!(cfg.extra.is_empty());
    }

    #[test]
    fn adapter_extra_skips_serializing_when_empty() {
        // skip_serializing_if must hold: an empty `extra` should not appear
        // in JSON-serialized output, so plan/config snapshots stay tight
        // and existing fixtures aren't perturbed.
        let cfg: AdapterConfig = toml::from_str(
            r#"
type = "duckdb"
path = ":memory:"
"#,
        )
        .unwrap();
        let json = serde_json::to_string(&cfg).unwrap();
        assert!(
            !json.contains("\"extra\""),
            "empty extra leaked into JSON: {json}"
        );
    }

    /// Integration guard for the `ReplicationPlan.config_snapshot` leak
    /// path: a `RockyConfig` containing populated `AdapterConfig`
    /// credentials must serialize through `serde_json::to_value` with
    /// every secret replaced by `"***"`. This is the same code path
    /// `rocky-cli/src/commands/plan.rs:build_and_persist_replication_plan`
    /// takes when snapshotting config into the plan payload — without
    /// the default-redact `Serialize` impl on `RedactedString`, the
    /// snapshot would embed cleartext credentials into orchestrator-
    /// visible JSON.
    #[test]
    fn rocky_config_serde_json_redacts_adapter_secrets() {
        let cfg = parse(
            r#"
[adapter.dbx]
type = "databricks"
host = "workspace.cloud.databricks.com"
http_path = "/sql/1.0/warehouses/abc"
token = "dapi_SUPER_SECRET_TOKEN"
client_id = "client_123"
client_secret = "oauth_SECRET_VALUE"

[adapter.fv]
type = "fivetran"
kind = "discovery"
destination_id = "d1"
api_key = "fivetran_KEY_SECRET"
api_secret = "fivetran_SECRET_VALUE"

[adapter.sf]
type = "snowflake"
account = "acct"
warehouse = "wh"
database = "db"
username = "u"
password = "db_PASSWORD_123"
oauth_token = "oauth_TOKEN_XYZ"
pat = "pat_LEAKED_HERE"
"#,
        );

        let serialized =
            serde_json::to_string(&cfg).expect("RockyConfig must serialize via serde_json");

        // None of the cleartext secrets may appear anywhere in the
        // snapshot.
        for cleartext in [
            "dapi_SUPER_SECRET_TOKEN",
            "oauth_SECRET_VALUE",
            "fivetran_KEY_SECRET",
            "fivetran_SECRET_VALUE",
            "db_PASSWORD_123",
            "oauth_TOKEN_XYZ",
            "pat_LEAKED_HERE",
        ] {
            assert!(
                !serialized.contains(cleartext),
                "{cleartext} leaked into serde_json output: {serialized}"
            );
        }
        // Redaction placeholder must appear in the snapshot.
        assert!(serialized.contains("***"), "missing *** in {serialized}");
        // Non-secret fields still serialize normally.
        assert!(
            serialized.contains("workspace.cloud.databricks.com"),
            "host missing from snapshot"
        );
        assert!(
            serialized.contains("client_123"),
            "client_id missing from snapshot"
        );
    }

    /// Companion guard for the `config_snapshot` leak: a populated
    /// `[state].valkey_url` (which may embed a redis password) must serialize
    /// to `"***"` through the same `serde_json` path `rocky plan` snapshots
    /// into the replication plan. Regression for the field being a plain
    /// `String` outside the redaction model — that left the credential
    /// cleartext in the on-disk plan file and in `Debug` output.
    #[test]
    fn rocky_config_serde_json_redacts_state_valkey_url() {
        let cfg = parse(
            r#"
[adapter.default]
type = "duckdb"
path = ":memory:"

[state]
backend = "valkey"
valkey_url = "rediss://default:VALKEY_PASSWORD_SECRET@valkey.example:6379"
"#,
        );

        let serialized =
            serde_json::to_string(&cfg).expect("RockyConfig must serialize via serde_json");
        assert!(
            !serialized.contains("VALKEY_PASSWORD_SECRET"),
            "state.valkey_url password leaked into serde_json output: {serialized}"
        );
        assert!(serialized.contains("***"), "missing *** in {serialized}");

        // Debug must also redact (StateConfig derives Debug).
        let dbg = format!("{:?}", cfg.state);
        assert!(
            !dbg.contains("VALKEY_PASSWORD_SECRET"),
            "state.valkey_url password leaked into Debug: {dbg}"
        );
    }

    // --- Config deprecation framework tests ---

    /// Helper: temporarily override DEPRECATED_KEYS for testing by calling
    /// the internal apply_deprecations_with_registry function.
    fn apply_test_deprecations(
        value: &mut toml::Value,
        registry: &[DeprecatedKey],
    ) -> Vec<DeprecationWarning> {
        let mut warnings = Vec::new();

        let Some(root) = value.as_table_mut() else {
            return warnings;
        };

        for dep in registry {
            let old_segments: Vec<&str> = dep.old_key.split('.').collect();
            let new_segments: Vec<&str> = dep.new_key.split('.').collect();

            if old_segments.is_empty() || new_segments.is_empty() {
                continue;
            }

            let old_leaf = old_segments[old_segments.len() - 1];
            let new_leaf = new_segments[new_segments.len() - 1];
            let parent_path = &old_segments[..old_segments.len() - 1];

            let Some(parent) = navigate_to_table_mut(root, parent_path) else {
                continue;
            };

            let Some(old_value) = parent.remove(old_leaf) else {
                continue;
            };

            let warning = DeprecationWarning {
                old_key: dep.old_key.to_string(),
                new_key: dep.new_key.to_string(),
                since_version: dep.since_version.to_string(),
                message: dep.message.to_string(),
            };

            if !parent.contains_key(new_leaf) {
                parent.insert(new_leaf.to_string(), old_value);
            }

            warnings.push(warning);
        }

        warnings
    }

    #[test]
    fn test_deprecation_remaps_old_key_to_new() {
        let registry = [DeprecatedKey {
            old_key: "state.type",
            new_key: "state.backend",
            since_version: "1.1.0",
            message: "Rename 'type' to 'backend' in the [state] section.",
        }];

        let toml_str = r#"
[state]
type = "s3"
"#;
        let mut value: toml::Value = toml::from_str(toml_str).unwrap();
        let warnings = apply_test_deprecations(&mut value, &registry);

        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].old_key, "state.type");
        assert_eq!(warnings[0].new_key, "state.backend");
        assert_eq!(warnings[0].since_version, "1.1.0");

        // The value should have been remapped
        let state = value
            .as_table()
            .unwrap()
            .get("state")
            .unwrap()
            .as_table()
            .unwrap();
        assert!(state.get("type").is_none(), "old key should be removed");
        assert_eq!(
            state.get("backend").unwrap().as_str().unwrap(),
            "s3",
            "value should be remapped to new key"
        );
    }

    #[test]
    fn test_deprecation_new_key_takes_precedence() {
        let registry = [DeprecatedKey {
            old_key: "state.type",
            new_key: "state.backend",
            since_version: "1.1.0",
            message: "Rename 'type' to 'backend'.",
        }];

        let toml_str = r#"
[state]
type = "old_value"
backend = "new_value"
"#;
        let mut value: toml::Value = toml::from_str(toml_str).unwrap();
        let warnings = apply_test_deprecations(&mut value, &registry);

        // Should still warn about the deprecated key
        assert_eq!(warnings.len(), 1);

        // But the new key's value should take precedence
        let state = value
            .as_table()
            .unwrap()
            .get("state")
            .unwrap()
            .as_table()
            .unwrap();
        assert!(state.get("type").is_none(), "old key should be removed");
        assert_eq!(
            state.get("backend").unwrap().as_str().unwrap(),
            "new_value",
            "new key value should take precedence"
        );
    }

    #[test]
    fn test_no_deprecation_warnings_when_no_deprecated_keys() {
        let registry = [DeprecatedKey {
            old_key: "state.type",
            new_key: "state.backend",
            since_version: "1.1.0",
            message: "Rename 'type' to 'backend'.",
        }];

        let toml_str = r#"
[state]
backend = "local"
"#;
        let mut value: toml::Value = toml::from_str(toml_str).unwrap();
        let warnings = apply_test_deprecations(&mut value, &registry);

        assert!(
            warnings.is_empty(),
            "no warnings when no deprecated keys are present"
        );
    }

    #[test]
    fn test_empty_deprecation_registry() {
        let registry: [DeprecatedKey; 0] = [];

        let toml_str = r#"
[state]
backend = "local"
"#;
        let mut value: toml::Value = toml::from_str(toml_str).unwrap();
        let warnings = apply_test_deprecations(&mut value, &registry);

        assert!(
            warnings.is_empty(),
            "empty registry should produce no warnings"
        );
    }

    #[test]
    fn test_deprecation_with_nested_keys() {
        let registry = [DeprecatedKey {
            old_key: "pipeline.my_pipe.source.dest_id",
            new_key: "pipeline.my_pipe.source.destination_id",
            since_version: "1.2.0",
            message: "Use 'destination_id' instead of 'dest_id'.",
        }];

        let toml_str = r#"
[pipeline.my_pipe.source]
dest_id = "dest_123"
"#;
        let mut value: toml::Value = toml::from_str(toml_str).unwrap();
        let warnings = apply_test_deprecations(&mut value, &registry);

        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].old_key, "pipeline.my_pipe.source.dest_id");

        let source = value
            .as_table()
            .unwrap()
            .get("pipeline")
            .unwrap()
            .as_table()
            .unwrap()
            .get("my_pipe")
            .unwrap()
            .as_table()
            .unwrap()
            .get("source")
            .unwrap()
            .as_table()
            .unwrap();
        assert!(source.get("dest_id").is_none());
        assert_eq!(
            source.get("destination_id").unwrap().as_str().unwrap(),
            "dest_123"
        );
    }

    #[test]
    fn test_deprecation_missing_parent_path_is_noop() {
        let registry = [DeprecatedKey {
            old_key: "nonexistent.section.old_key",
            new_key: "nonexistent.section.new_key",
            since_version: "1.0.0",
            message: "Test.",
        }];

        let toml_str = r#"
[state]
backend = "local"
"#;
        let mut value: toml::Value = toml::from_str(toml_str).unwrap();
        let warnings = apply_test_deprecations(&mut value, &registry);

        assert!(
            warnings.is_empty(),
            "missing parent path should be silently skipped"
        );
    }

    #[test]
    fn test_deprecation_top_level_key() {
        let registry = [DeprecatedKey {
            old_key: "old_top_level",
            new_key: "new_top_level",
            since_version: "1.3.0",
            message: "Renamed top-level key.",
        }];

        let toml_str = r#"
old_top_level = "value"
"#;
        let mut value: toml::Value = toml::from_str(toml_str).unwrap();
        let warnings = apply_test_deprecations(&mut value, &registry);

        assert_eq!(warnings.len(), 1);
        let root = value.as_table().unwrap();
        assert!(root.get("old_top_level").is_none());
        assert_eq!(
            root.get("new_top_level").unwrap().as_str().unwrap(),
            "value"
        );
    }

    #[test]
    fn test_deprecation_multiple_keys() {
        let registry = [
            DeprecatedKey {
                old_key: "state.type",
                new_key: "state.backend",
                since_version: "1.1.0",
                message: "Rename 'type' to 'backend'.",
            },
            DeprecatedKey {
                old_key: "cost.compute_cost",
                new_key: "cost.compute_cost_per_dbu",
                since_version: "1.2.0",
                message: "Use 'compute_cost_per_dbu'.",
            },
        ];

        let toml_str = r#"
[state]
type = "s3"

[cost]
compute_cost = 0.55
"#;
        let mut value: toml::Value = toml::from_str(toml_str).unwrap();
        let warnings = apply_test_deprecations(&mut value, &registry);

        assert_eq!(warnings.len(), 2);
        assert_eq!(warnings[0].old_key, "state.type");
        assert_eq!(warnings[1].old_key, "cost.compute_cost");

        let state = value
            .as_table()
            .unwrap()
            .get("state")
            .unwrap()
            .as_table()
            .unwrap();
        assert_eq!(state.get("backend").unwrap().as_str().unwrap(), "s3");

        let cost = value
            .as_table()
            .unwrap()
            .get("cost")
            .unwrap()
            .as_table()
            .unwrap();
        assert!(
            (cost
                .get("compute_cost_per_dbu")
                .unwrap()
                .as_float()
                .unwrap()
                - 0.55)
                .abs()
                < f64::EPSILON
        );
    }

    #[test]
    fn test_current_deprecated_keys_registry_works() {
        // Verifies that the actual DEPRECATED_KEYS constant (currently empty)
        // doesn't cause errors when applied to a valid config.
        let toml_str = r#"
[adapter.db]
type = "duckdb"

[pipeline.p1]
type = "replication"

[pipeline.p1.source]
adapter = "db"
[pipeline.p1.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.p1.target]
adapter = "db"
catalog_template = "main"
schema_template = "staging"
"#;
        let mut value: toml::Value = toml::from_str(toml_str).unwrap();
        let warnings = apply_deprecations(&mut value);
        assert!(warnings.is_empty());

        // Config should still parse correctly
        normalize_toml_shorthands(&mut value);
        let config: RockyConfig = value.try_into().unwrap();
        assert_eq!(config.pipelines.len(), 1);
    }

    #[test]
    fn test_check_config_deprecations_fn() {
        // The public check_config_deprecations function should work on raw TOML
        // and return warnings without modifying the caller's state.
        let toml_str = r#"
[adapter.db]
type = "duckdb"

[pipeline.p1]
type = "replication"

[pipeline.p1.source]
adapter = "db"
[pipeline.p1.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.p1.target]
adapter = "db"
catalog_template = "main"
schema_template = "staging"
"#;
        let warnings = check_config_deprecations(toml_str).unwrap();
        assert!(warnings.is_empty());
    }

    #[test]
    fn test_execution_config_defaults_adaptive() {
        let cfg = ExecutionConfig::default();
        assert_eq!(cfg.concurrency, ConcurrencyMode::Adaptive);
        assert!(cfg.concurrency.is_adaptive());
        assert_eq!(cfg.concurrency.max_concurrency(), 32);
    }

    #[test]
    fn test_execution_config_adaptive_from_toml() {
        let toml_str = r#"
concurrency = "adaptive"
"#;
        let cfg: ExecutionConfig = toml::from_str(toml_str).unwrap();
        assert!(cfg.concurrency.is_adaptive());
        assert_eq!(cfg.concurrency.max_concurrency(), 32);
    }

    #[test]
    fn test_execution_config_fixed_from_toml() {
        let toml_str = r#"
concurrency = 8
"#;
        let cfg: ExecutionConfig = toml::from_str(toml_str).unwrap();
        assert!(!cfg.concurrency.is_adaptive());
        assert_eq!(cfg.concurrency, ConcurrencyMode::Fixed(8));
        assert_eq!(cfg.concurrency.max_concurrency(), 8);
    }

    #[test]
    fn test_execution_config_serial_from_toml() {
        let toml_str = r#"
concurrency = 1
"#;
        let cfg: ExecutionConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(cfg.concurrency, ConcurrencyMode::Fixed(1));
        assert_eq!(cfg.concurrency.max_concurrency(), 1);
    }

    #[test]
    fn test_execution_config_zero_clamped_to_one() {
        let toml_str = r#"
concurrency = 0
"#;
        let cfg: ExecutionConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(cfg.concurrency, ConcurrencyMode::Fixed(1));
        assert_eq!(cfg.concurrency.max_concurrency(), 1);
    }

    #[test]
    fn test_execution_config_omitted_defaults_to_adaptive() {
        // Omitting concurrency entirely defaults to adaptive
        let toml_str = r#"
fail_fast = true
"#;
        let cfg: ExecutionConfig = toml::from_str(toml_str).unwrap();
        assert!(cfg.concurrency.is_adaptive());
    }

    #[test]
    fn test_execution_config_invalid_string_errors() {
        let toml_str = r#"
concurrency = "turbo"
"#;
        let result: Result<ExecutionConfig, _> = toml::from_str(toml_str);
        assert!(result.is_err());
    }

    #[test]
    fn test_concurrency_mode_display() {
        assert_eq!(ConcurrencyMode::Adaptive.to_string(), "adaptive");
        assert_eq!(ConcurrencyMode::Fixed(8).to_string(), "8");
    }

    #[test]
    fn test_concurrency_mode_roundtrip() {
        // Adaptive roundtrip
        let adaptive = ConcurrencyMode::Adaptive;
        let json = serde_json::to_string(&adaptive).unwrap();
        assert_eq!(json, r#""adaptive""#);

        // Fixed roundtrip
        let fixed = ConcurrencyMode::Fixed(16);
        let json = serde_json::to_string(&fixed).unwrap();
        assert_eq!(json, "16");
    }

    #[test]
    fn configured_explicit_kinds_reports_only_opt_in_checks() {
        use crate::checks::CheckKind;

        // An empty/default checks block opts into nothing — row_count and
        // column_match toggles and the always-defaulted anomaly threshold are
        // not "explicitly configured", so the inert lint never fires on them.
        assert!(
            ChecksConfig::default()
                .configured_explicit_kinds()
                .is_empty()
        );

        let cfg: ChecksConfig = toml::from_str(
            r#"
null_rate = { columns = ["a"], threshold = 0.1 }
freshness = { threshold_seconds = 3600 }

[[custom]]
name = "c"
sql = "SELECT 1"
threshold = 1
"#,
        )
        .unwrap();
        let kinds = cfg.configured_explicit_kinds();
        assert!(kinds.contains(&CheckKind::NullRate));
        assert!(kinds.contains(&CheckKind::Freshness));
        assert!(kinds.contains(&CheckKind::Custom));
        assert!(!kinds.contains(&CheckKind::RowCount));
        assert!(!kinds.contains(&CheckKind::Anomaly));
    }

    // -- Budget ------------------------------------------------------------

    #[test]
    fn budget_defaults_are_unset() {
        let cfg = BudgetConfig::default();
        assert!(cfg.is_unset());
        assert_eq!(cfg.on_breach, BudgetBreachAction::Warn);
    }

    #[test]
    fn budget_parses_from_toml() {
        let toml_str = r#"
max_usd = 25.0
max_duration_ms = 900000
max_bytes_scanned = 1099511627776
on_breach = "error"
"#;
        let cfg: BudgetConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(cfg.max_usd, Some(25.0));
        assert_eq!(cfg.max_duration_ms, Some(900_000));
        assert_eq!(cfg.max_bytes_scanned, Some(1_099_511_627_776));
        assert_eq!(cfg.on_breach, BudgetBreachAction::Error);
    }

    #[test]
    fn budget_rejects_unknown_fields() {
        let toml_str = r#"
max_usd = 10.0
max_rows = 1000000
"#;
        let result: Result<BudgetConfig, _> = toml::from_str(toml_str);
        assert!(result.is_err(), "unknown fields must be rejected");
    }

    #[test]
    fn budget_check_breaches_empty_when_under_limits() {
        let cfg = BudgetConfig {
            max_usd: Some(10.0),
            max_duration_ms: Some(60_000),
            max_bytes_scanned: Some(1_000_000),
            on_breach: BudgetBreachAction::Warn,
        };
        assert!(
            cfg.check_breaches(Some(9.0), 30_000, Some(500_000))
                .is_empty()
        );
    }

    #[test]
    fn budget_check_breaches_flags_usd_over() {
        let cfg = BudgetConfig {
            max_usd: Some(10.0),
            max_duration_ms: None,
            max_bytes_scanned: None,
            on_breach: BudgetBreachAction::Error,
        };
        let breaches = cfg.check_breaches(Some(15.0), 0, None);
        assert_eq!(breaches.len(), 1);
        assert_eq!(breaches[0].limit_type, BudgetLimitType::MaxUsd);
        assert_eq!(breaches[0].limit, 10.0);
        assert_eq!(breaches[0].actual, 15.0);
    }

    #[test]
    fn budget_check_breaches_flags_duration_over() {
        let cfg = BudgetConfig {
            max_usd: None,
            max_duration_ms: Some(60_000),
            max_bytes_scanned: None,
            on_breach: BudgetBreachAction::Warn,
        };
        let breaches = cfg.check_breaches(None, 120_000, None);
        assert_eq!(breaches.len(), 1);
        assert_eq!(breaches[0].limit_type, BudgetLimitType::MaxDurationMs);
    }

    #[test]
    fn budget_check_breaches_skips_usd_when_unknown() {
        // When the adapters didn't produce enough data to compute cost,
        // the USD dimension is skipped rather than treated as zero.
        let cfg = BudgetConfig {
            max_usd: Some(10.0),
            max_duration_ms: None,
            max_bytes_scanned: None,
            on_breach: BudgetBreachAction::Warn,
        };
        assert!(cfg.check_breaches(None, 0, None).is_empty());
    }

    #[test]
    fn budget_check_breaches_handles_all_limits() {
        let cfg = BudgetConfig {
            max_usd: Some(5.0),
            max_duration_ms: Some(60_000),
            max_bytes_scanned: Some(1_000_000),
            on_breach: BudgetBreachAction::Warn,
        };
        let breaches = cfg.check_breaches(Some(12.0), 90_000, Some(2_000_000));
        assert_eq!(breaches.len(), 3);
    }

    #[test]
    fn budget_check_breaches_flags_bytes_over() {
        // HackerNews-driven feature: a CI gate that fails the run when
        // total scan volume crosses a threshold (e.g. 1 TB).
        let cfg = BudgetConfig {
            max_usd: None,
            max_duration_ms: None,
            max_bytes_scanned: Some(1_000_000),
            on_breach: BudgetBreachAction::Error,
        };
        let breaches = cfg.check_breaches(None, 0, Some(2_000_000));
        assert_eq!(breaches.len(), 1);
        assert_eq!(breaches[0].limit_type, BudgetLimitType::MaxBytesScanned);
        assert!((breaches[0].limit - 1_000_000.0).abs() < f64::EPSILON);
        assert!((breaches[0].actual - 2_000_000.0).abs() < f64::EPSILON);
    }

    #[test]
    fn budget_check_breaches_skips_bytes_when_unknown() {
        // Adapters that don't report `bytes_scanned` (Databricks /
        // Snowflake / DuckDB today) skip the dimension rather than
        // treating "no data" as zero.
        let cfg = BudgetConfig {
            max_usd: None,
            max_duration_ms: None,
            max_bytes_scanned: Some(1_000_000),
            on_breach: BudgetBreachAction::Warn,
        };
        assert!(cfg.check_breaches(None, 0, None).is_empty());
    }

    #[test]
    fn budget_check_breaches_no_breach_when_bytes_unset_default() {
        // Default `BudgetConfig` (no `max_bytes_scanned`) never trips a
        // bytes breach regardless of the observed scan volume.
        let cfg = BudgetConfig::default();
        let breaches = cfg.check_breaches(None, 0, Some(u64::MAX));
        assert!(breaches.is_empty());
    }

    // -- ModelBudgetConfig (per-model overrides) ----------------------------

    #[test]
    fn model_budget_default_is_empty() {
        let m = ModelBudgetConfig::default();
        assert!(m.is_empty());
        assert!(m.max_usd.is_none());
        assert!(m.on_breach.is_none());
    }

    #[test]
    fn model_budget_parses_from_toml() {
        // Partial block: only `max_usd` and `on_breach` set; the other
        // two fields stay `None` so they inherit from the project-level
        // config when resolved.
        let toml_str = r#"
max_usd = 0.50
on_breach = "warn"
"#;
        let m: ModelBudgetConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(m.max_usd, Some(0.50));
        assert!(m.max_duration_ms.is_none());
        assert!(m.max_bytes_scanned.is_none());
        assert_eq!(m.on_breach, Some(BudgetBreachAction::Warn));
    }

    #[test]
    fn model_budget_rejects_unknown_fields() {
        let toml_str = r#"
max_usd = 1.0
max_rows = 1000
"#;
        let result: Result<ModelBudgetConfig, _> = toml::from_str(toml_str);
        assert!(result.is_err(), "unknown fields must be rejected");
    }

    #[test]
    fn model_budget_resolve_field_inheritance() {
        let project = BudgetConfig {
            max_usd: Some(10.0),
            max_duration_ms: Some(60_000),
            max_bytes_scanned: Some(1_000_000),
            on_breach: BudgetBreachAction::Error,
        };
        // Override one field; the rest inherit.
        let over = ModelBudgetConfig {
            max_usd: Some(2.0),
            max_duration_ms: None,
            max_bytes_scanned: None,
            on_breach: None,
        };
        let resolved = over.resolve(&project);
        assert_eq!(resolved.max_usd, Some(2.0));
        assert_eq!(resolved.max_duration_ms, Some(60_000));
        assert_eq!(resolved.max_bytes_scanned, Some(1_000_000));
        // `on_breach` falls back to project when per-model is `None`.
        assert_eq!(resolved.on_breach, BudgetBreachAction::Error);
    }

    #[test]
    fn model_budget_resolve_per_model_on_breach_wins() {
        // The load-bearing precedence call: per-model `on_breach`,
        // when explicitly set, is the local authority. A per-model
        // `warn` overrides a project-level `error` for that one model.
        let project = BudgetConfig {
            max_usd: Some(10.0),
            on_breach: BudgetBreachAction::Error,
            ..BudgetConfig::default()
        };
        let over = ModelBudgetConfig {
            on_breach: Some(BudgetBreachAction::Warn),
            ..ModelBudgetConfig::default()
        };
        assert_eq!(over.resolve(&project).on_breach, BudgetBreachAction::Warn);

        // Reverse direction also holds — per-model `error` over project
        // `warn`.
        let project_warn = BudgetConfig {
            on_breach: BudgetBreachAction::Warn,
            ..BudgetConfig::default()
        };
        let over_err = ModelBudgetConfig {
            on_breach: Some(BudgetBreachAction::Error),
            ..ModelBudgetConfig::default()
        };
        assert_eq!(
            over_err.resolve(&project_warn).on_breach,
            BudgetBreachAction::Error
        );
    }

    // -----------------------------------------------------------------------
    // Cache / schema cache config
    // -----------------------------------------------------------------------

    #[test]
    fn schema_cache_config_defaults() {
        let cfg = SchemaCacheConfig::default();
        assert!(cfg.enabled);
        assert_eq!(cfg.ttl_seconds, 86_400);
        assert!(!cfg.replicate);
        assert_eq!(cfg.ttl(), chrono::Duration::seconds(86_400));
    }

    #[test]
    fn cache_config_defaults_schemas_section() {
        let cfg = CacheConfig::default();
        assert!(cfg.schemas.enabled);
        assert_eq!(cfg.schemas.ttl_seconds, 86_400);
        assert!(!cfg.schemas.replicate);
    }

    #[test]
    fn parse_cache_schemas_full_block() {
        // Parse as a full `RockyConfig` so the `[cache.schemas]` TOML path
        // lands on `RockyConfig.cache.schemas` — matches how users
        // actually author `rocky.toml`.
        let toml_str = r#"
            [adapter.default]
            type = "duckdb"

            [cache.schemas]
            enabled = true
            ttl_seconds = 3600
            replicate = true
        "#;
        let cfg: RockyConfig = toml::from_str(toml_str).unwrap();
        assert!(cfg.cache.schemas.enabled);
        assert_eq!(cfg.cache.schemas.ttl_seconds, 3600);
        assert!(cfg.cache.schemas.replicate);
    }

    #[test]
    fn parse_cache_schemas_partial_override_keeps_other_defaults() {
        // Setting only ttl_seconds: enabled + replicate should remain at
        // their defaults. This is the #[serde(default)] contract.
        let toml_str = r#"
            [adapter.default]
            type = "duckdb"

            [cache.schemas]
            ttl_seconds = 7200
        "#;
        let cfg: RockyConfig = toml::from_str(toml_str).unwrap();
        assert!(cfg.cache.schemas.enabled); // default true
        assert_eq!(cfg.cache.schemas.ttl_seconds, 7200);
        assert!(!cfg.cache.schemas.replicate); // default false
    }

    #[test]
    fn parse_cache_schemas_empty_section_uses_all_defaults() {
        // A bare `[cache]` block with no children should defer every
        // schema-cache knob to its default. Verifies the
        // `#[serde(default)]` on `CacheConfig.schemas`.
        let toml_str = r#"
            [adapter.default]
            type = "duckdb"

            [cache]
        "#;
        let cfg: RockyConfig = toml::from_str(toml_str).unwrap();
        assert!(cfg.cache.schemas.enabled);
        assert_eq!(cfg.cache.schemas.ttl_seconds, 86_400);
        assert!(!cfg.cache.schemas.replicate);
    }

    #[test]
    fn rocky_config_default_includes_cache_defaults() {
        // When a user's rocky.toml omits `[cache]` entirely, the top-level
        // RockyConfig still exposes a usable `.cache` field with the
        // shipped defaults — this is the zero-config contract.
        let toml_str = r#"
            [adapter.default]
            type = "duckdb"
        "#;
        let cfg: RockyConfig = toml::from_str(toml_str).unwrap();
        assert!(cfg.cache.schemas.enabled);
        assert_eq!(cfg.cache.schemas.ttl_seconds, 86_400);
        assert!(!cfg.cache.schemas.replicate);
    }

    #[test]
    fn with_ttl_override_none_returns_unchanged_config() {
        let cfg = SchemaCacheConfig {
            enabled: true,
            ttl_seconds: 7200,
            replicate: false,
        };
        let overridden = cfg.clone().with_ttl_override(None);
        assert_eq!(overridden.ttl_seconds, 7200);
        assert_eq!(overridden.enabled, cfg.enabled);
        assert_eq!(overridden.replicate, cfg.replicate);
    }

    #[test]
    fn with_ttl_override_some_replaces_ttl_only() {
        // CLI flag wins over the config value. Other fields are untouched
        // — enabled/replicate are not a CLI-flag surface today.
        let cfg = SchemaCacheConfig {
            enabled: true,
            ttl_seconds: 86_400,
            replicate: true,
        };
        let overridden = cfg.with_ttl_override(Some(60));
        assert_eq!(overridden.ttl_seconds, 60);
        assert!(overridden.enabled);
        assert!(overridden.replicate);
    }

    #[test]
    fn with_ttl_override_zero_flips_every_entry_to_stale() {
        // `--cache-ttl 0` is the "everything is instantly stale" knob, not
        // the "off" knob. Setting it to 0 means every entry in the cache
        // has `cached_at > now - 0s`, so `is_expired` returns true for
        // everything and the read path degrades to an empty map.
        let cfg = SchemaCacheConfig::default().with_ttl_override(Some(0));
        assert_eq!(cfg.ttl_seconds, 0);
        // An entry cached one millisecond ago is already stale at ttl=0
        // (is_expired uses strict `>`). Smoke-test the boundary here so
        // the invariant doesn't drift.
        let entry = crate::schema_cache::SchemaCacheEntry {
            columns: vec![],
            cached_at: chrono::Utc::now() - chrono::Duration::milliseconds(1),
        };
        assert!(entry.is_expired(chrono::Utc::now(), cfg.ttl()));
    }

    // ----------------------------------------------------------------------
    // [mask] + [classifications] parsing
    // ----------------------------------------------------------------------

    #[test]
    fn parse_mask_defaults_only() {
        let toml_str = r#"
            [adapter.default]
            type = "duckdb"

            [mask]
            pii = "hash"
            confidential = "redact"
        "#;
        let cfg: RockyConfig = toml::from_str(toml_str).unwrap();
        let resolved = cfg.resolve_mask_for_env(None);
        assert_eq!(
            resolved.get("pii").copied(),
            Some(rocky_ir::MaskStrategy::Hash)
        );
        assert_eq!(
            resolved.get("confidential").copied(),
            Some(rocky_ir::MaskStrategy::Redact)
        );
    }

    #[test]
    fn parse_mask_env_override_wins_over_default() {
        let toml_str = r#"
            [adapter.default]
            type = "duckdb"

            [mask]
            pii = "hash"
            confidential = "redact"

            [mask.prod]
            pii = "none"
            confidential = "partial"
        "#;
        let cfg: RockyConfig = toml::from_str(toml_str).unwrap();

        // No env: scalars only.
        let default = cfg.resolve_mask_for_env(None);
        assert_eq!(
            default.get("pii").copied(),
            Some(rocky_ir::MaskStrategy::Hash)
        );

        // prod env: overrides win.
        let prod = cfg.resolve_mask_for_env(Some("prod"));
        assert_eq!(prod.get("pii").copied(), Some(rocky_ir::MaskStrategy::None));
        assert_eq!(
            prod.get("confidential").copied(),
            Some(rocky_ir::MaskStrategy::Partial)
        );

        // Non-matching env ignores the override table.
        let staging = cfg.resolve_mask_for_env(Some("staging"));
        assert_eq!(
            staging.get("pii").copied(),
            Some(rocky_ir::MaskStrategy::Hash)
        );
    }

    #[test]
    fn parse_mask_env_override_only_adds_keys_for_its_env() {
        let toml_str = r#"
            [adapter.default]
            type = "duckdb"

            [mask]
            pii = "hash"

            [mask.prod]
            confidential = "partial"
        "#;
        let cfg: RockyConfig = toml::from_str(toml_str).unwrap();

        // prod inherits defaults AND gets override-only keys.
        let prod = cfg.resolve_mask_for_env(Some("prod"));
        assert_eq!(
            prod.get("pii").copied(),
            Some(rocky_ir::MaskStrategy::Hash),
            "prod should inherit the default pii"
        );
        assert_eq!(
            prod.get("confidential").copied(),
            Some(rocky_ir::MaskStrategy::Partial),
            "prod should gain confidential from its override"
        );

        // default env (None) only has the scalar default.
        let default = cfg.resolve_mask_for_env(None);
        assert!(!default.contains_key("confidential"));
    }

    #[test]
    fn parse_mask_rejects_unknown_strategy() {
        let toml_str = r#"
            [adapter.default]
            type = "duckdb"

            [mask]
            pii = "scramble"
        "#;
        let err =
            toml::from_str::<RockyConfig>(toml_str).expect_err("unknown strategy should fail");
        assert!(
            err.to_string().contains("scramble") || err.to_string().contains("unknown variant"),
            "expected error to mention bad strategy, got: {err}"
        );
    }

    #[test]
    fn parse_mask_empty_block_resolves_to_empty_map() {
        let toml_str = r#"
            [adapter.default]
            type = "duckdb"

            [mask]
        "#;
        let cfg: RockyConfig = toml::from_str(toml_str).unwrap();
        assert!(cfg.resolve_mask_for_env(None).is_empty());
        assert!(cfg.resolve_mask_for_env(Some("prod")).is_empty());
    }

    #[test]
    fn parse_classifications_allow_unmasked() {
        let toml_str = r#"
            [adapter.default]
            type = "duckdb"

            [classifications]
            allow_unmasked = ["internal", "lineage_only"]
        "#;
        let cfg: RockyConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(
            cfg.classifications.allow_unmasked,
            vec!["internal".to_string(), "lineage_only".to_string()]
        );
    }

    #[test]
    fn rocky_config_default_omits_mask_and_classifications() {
        // Zero-config contract: no [mask] / [classifications] block means
        // both fields are empty. This is the shipping default.
        let toml_str = r#"
            [adapter.default]
            type = "duckdb"
        "#;
        let cfg: RockyConfig = toml::from_str(toml_str).unwrap();
        assert!(cfg.mask.is_empty());
        assert!(cfg.classifications.allow_unmasked.is_empty());
    }

    // ---------- Phase C — [plan_store] removal (C-7) ----------

    #[test]
    fn plan_store_block_is_rejected_after_c7() {
        // C-7 dropped the `[plan_store]` config block entirely along
        // with the v1 reader. `deny_unknown_fields` on `RockyConfig`
        // now rejects any project still carrying the stale block,
        // which is the desired loud failure for in-flight migrations
        // — operators must remove the block (or the explicit
        // `format = "v1"` setting) before upgrading.
        let toml_str = r#"
            [adapter.default]
            type = "duckdb"

            [plan_store]
            format = "v1"
        "#;
        let err = toml::from_str::<RockyConfig>(toml_str)
            .expect_err("[plan_store] should be rejected as an unknown field after C-7");
        let msg = err.to_string();
        assert!(
            msg.contains("plan_store") || msg.contains("unknown field"),
            "error should mention the removed [plan_store] block, got: {msg}"
        );
    }

    /// Returns a baseline replication-pipeline TOML used by the merge-strategy
    /// tests. Callers concatenate strategy-specific overrides on top.
    fn merge_pipeline_toml_base() -> &'static str {
        r#"
[adapter.default]
type = "duckdb"
path = "/tmp/x.duckdb"

[pipeline.bronze]
type = "replication"
"#
    }

    #[test]
    fn replication_strategy_merge_parses_merge_keys() {
        let mut toml_str = String::from(merge_pipeline_toml_base());
        toml_str.push_str(
            r#"
strategy = "merge"
merge_keys = ["id", "tenant_id"]

[pipeline.bronze.source]
catalog = "raw_catalog"

[pipeline.bronze.source.schema_pattern]
prefix = "src__"
separator = "__"
components = ["source"]

[pipeline.bronze.target]
catalog_template = "wh"
schema_template = "raw__{source}"
"#,
        );
        let config: RockyConfig = toml::from_str(&toml_str).unwrap();
        let pipeline = config.pipelines["bronze"].as_replication().unwrap();
        assert_eq!(pipeline.strategy, "merge");
        assert_eq!(
            pipeline.merge_keys.as_deref(),
            Some(&["id".to_string(), "tenant_id".to_string()][..])
        );
        assert!(pipeline.merge_keys_fallback.is_none());
        // resolved_merge_keys prefers merge_keys.
        assert_eq!(
            pipeline.resolved_merge_keys(),
            Some(&vec!["id".to_string(), "tenant_id".to_string()])
        );
    }

    #[test]
    fn replication_strategy_merge_falls_back_to_merge_keys_fallback() {
        let mut toml_str = String::from(merge_pipeline_toml_base());
        toml_str.push_str(
            r#"
strategy = "merge"
merge_keys_fallback = ["pk"]

[pipeline.bronze.source]
catalog = "raw_catalog"

[pipeline.bronze.source.schema_pattern]
prefix = "src__"
separator = "__"
components = ["source"]

[pipeline.bronze.target]
catalog_template = "wh"
schema_template = "raw__{source}"
"#,
        );
        let config: RockyConfig = toml::from_str(&toml_str).unwrap();
        let pipeline = config.pipelines["bronze"].as_replication().unwrap();
        assert!(pipeline.merge_keys.is_none());
        assert_eq!(
            pipeline.merge_keys_fallback.as_deref(),
            Some(&["pk".to_string()][..])
        );
        assert_eq!(
            pipeline.resolved_merge_keys(),
            Some(&vec!["pk".to_string()])
        );
    }

    #[test]
    fn replication_strategy_merge_prefers_merge_keys_over_fallback() {
        let mut toml_str = String::from(merge_pipeline_toml_base());
        toml_str.push_str(
            r#"
strategy = "merge"
merge_keys = ["primary"]
merge_keys_fallback = ["fallback_only"]

[pipeline.bronze.source]
catalog = "raw_catalog"

[pipeline.bronze.source.schema_pattern]
prefix = "src__"
separator = "__"
components = ["source"]

[pipeline.bronze.target]
catalog_template = "wh"
schema_template = "raw__{source}"
"#,
        );
        let config: RockyConfig = toml::from_str(&toml_str).unwrap();
        let pipeline = config.pipelines["bronze"].as_replication().unwrap();
        assert_eq!(
            pipeline.resolved_merge_keys(),
            Some(&vec!["primary".to_string()])
        );
    }

    #[test]
    fn validate_replication_strategies_rejects_merge_without_keys() {
        let mut toml_str = String::from(merge_pipeline_toml_base());
        toml_str.push_str(
            r#"
strategy = "merge"

[pipeline.bronze.source]
catalog = "raw_catalog"

[pipeline.bronze.source.schema_pattern]
prefix = "src__"
separator = "__"
components = ["source"]

[pipeline.bronze.target]
catalog_template = "wh"
schema_template = "raw__{source}"
"#,
        );
        let config: RockyConfig = toml::from_str(&toml_str).unwrap();
        let errors = validate_replication_strategies(&config);
        assert_eq!(errors.len(), 1);
        // Pin the exact error string — it's the contract callers (CLI / `rocky
        // validate`) surface to operators and must stay stable.
        assert_eq!(
            errors[0].to_string(),
            "pipeline 'bronze' uses strategy = \"merge\" but neither merge_keys \
             nor merge_keys_fallback is configured. \
             Set [pipeline.bronze.merge_keys = [\"col1\", \"col2\"]] in your rocky.toml."
        );
        assert!(
            matches!(errors[0], ConfigError::ReplicationMergeMissingKeys { .. }),
            "wrong variant: {:?}",
            errors[0]
        );
    }

    #[test]
    fn validate_replication_strategies_accepts_merge_with_keys() {
        let mut toml_str = String::from(merge_pipeline_toml_base());
        toml_str.push_str(
            r#"
strategy = "merge"
merge_keys = ["id"]

[pipeline.bronze.source]
catalog = "raw_catalog"

[pipeline.bronze.source.schema_pattern]
prefix = "src__"
separator = "__"
components = ["source"]

[pipeline.bronze.target]
catalog_template = "wh"
schema_template = "raw__{source}"
"#,
        );
        let config: RockyConfig = toml::from_str(&toml_str).unwrap();
        let errors = validate_replication_strategies(&config);
        assert!(
            errors.is_empty(),
            "merge with merge_keys should validate cleanly, got: {errors:?}"
        );
    }

    #[test]
    fn validate_replication_strategies_accepts_merge_with_fallback_only() {
        let mut toml_str = String::from(merge_pipeline_toml_base());
        toml_str.push_str(
            r#"
strategy = "merge"
merge_keys_fallback = ["id"]

[pipeline.bronze.source]
catalog = "raw_catalog"

[pipeline.bronze.source.schema_pattern]
prefix = "src__"
separator = "__"
components = ["source"]

[pipeline.bronze.target]
catalog_template = "wh"
schema_template = "raw__{source}"
"#,
        );
        let config: RockyConfig = toml::from_str(&toml_str).unwrap();
        let errors = validate_replication_strategies(&config);
        assert!(
            errors.is_empty(),
            "merge with merge_keys_fallback should validate cleanly, got: {errors:?}"
        );
    }

    #[test]
    fn validate_replication_strategies_skips_non_merge_strategies() {
        let mut toml_str = String::from(merge_pipeline_toml_base());
        toml_str.push_str(
            r#"
strategy = "incremental"
timestamp_column = "_synced"

[pipeline.bronze.source]
catalog = "raw_catalog"

[pipeline.bronze.source.schema_pattern]
prefix = "src__"
separator = "__"
components = ["source"]

[pipeline.bronze.target]
catalog_template = "wh"
schema_template = "raw__{source}"
"#,
        );
        let config: RockyConfig = toml::from_str(&toml_str).unwrap();
        // No merge_keys, but strategy != "merge" — must not produce errors.
        let errors = validate_replication_strategies(&config);
        assert!(
            errors.is_empty(),
            "non-merge strategies must pass: {errors:?}"
        );
    }

    #[test]
    fn load_rocky_config_fails_fast_on_merge_without_keys() {
        use std::io::Write;

        let mut toml_str = String::from(merge_pipeline_toml_base());
        toml_str.push_str(
            r#"
strategy = "merge"

[pipeline.bronze.source]
catalog = "raw_catalog"

[pipeline.bronze.source.schema_pattern]
prefix = "src__"
separator = "__"
components = ["source"]

[pipeline.bronze.target]
catalog_template = "wh"
schema_template = "raw__{source}"
"#,
        );
        let mut tmp = tempfile::NamedTempFile::new().expect("tempfile");
        tmp.write_all(toml_str.as_bytes()).expect("write");
        let err = load_rocky_config(tmp.path())
            .expect_err("load_rocky_config should reject merge without keys");
        assert!(
            matches!(err, ConfigError::ReplicationMergeMissingKeys { ref pipeline }
                if pipeline == "bronze"),
            "unexpected error: {err:?}"
        );
    }

    // ---------------- Table-override tests (PR-B3) ----------------

    /// TOML base for table-override tests. Pipeline name is `bronze`,
    /// strategy is `merge` with pipeline-level `merge_keys = ["id"]`,
    /// schema pattern uses `source` component only.
    fn override_pipeline_toml_base() -> String {
        let mut s = String::from(merge_pipeline_toml_base());
        s.push_str(
            r#"
strategy = "merge"
merge_keys = ["id"]

[pipeline.bronze.source]
catalog = "raw_catalog"

[pipeline.bronze.source.schema_pattern]
prefix = "src__"
separator = "__"
components = ["source"]

[pipeline.bronze.target]
catalog_template = "wh"
schema_template = "raw__{source}"
"#,
        );
        s
    }

    #[test]
    fn glob_match_literal_no_wildcards() {
        assert!(glob_match("pii_users", "pii_users"));
        assert!(!glob_match("pii_users", "pii_orders"));
    }

    #[test]
    fn glob_match_star_prefix_suffix_middle() {
        assert!(glob_match("_diagnostics_*", "_diagnostics_x"));
        assert!(glob_match("_diagnostics_*", "_diagnostics_"));
        assert!(!glob_match("_diagnostics_*", "diagnostics_x"));
        assert!(glob_match("*_temp", "users_temp"));
        assert!(!glob_match("*_temp", "users_temp_extra"));
        assert!(glob_match("a*b*c", "aXbYc"));
        assert!(glob_match("a*b*c", "abc"));
        assert!(!glob_match("a*b*c", "ac"));
    }

    #[test]
    fn glob_match_question_mark_single_char() {
        assert!(glob_match("u?ers", "users"));
        assert!(!glob_match("u?ers", "uers"));
        assert!(!glob_match("u?ers", "useers"));
    }

    #[test]
    fn glob_match_full_anchor() {
        // Patterns are anchored — partial matches do not pass.
        assert!(!glob_match("pii", "pii_users"));
        assert!(!glob_match("users", "pii_users"));
    }

    #[test]
    fn pattern_is_glob_detects_wildcards() {
        assert!(pattern_is_glob("_diagnostics_*"));
        assert!(pattern_is_glob("u?ers"));
        assert!(!pattern_is_glob("pii_users"));
        assert!(!pattern_is_glob(""));
    }

    #[test]
    fn table_overrides_parse_minimal() {
        let mut toml_str = override_pipeline_toml_base();
        toml_str.push_str(
            r#"
[[pipeline.bronze.table_overrides]]
match.connector = "stripe_main"
match.table     = "pii_users"
merge_keys      = ["user_id", "tenant_id"]
"#,
        );
        let config: RockyConfig = toml::from_str(&toml_str).unwrap();
        let pipeline = config.pipelines["bronze"].as_replication().unwrap();
        assert_eq!(pipeline.table_overrides.len(), 1);
        let rule = &pipeline.table_overrides[0];
        assert_eq!(rule.match_.connector.as_deref(), Some("stripe_main"));
        assert_eq!(rule.match_.table.as_deref(), Some("pii_users"));
        assert_eq!(
            rule.merge_keys.as_deref(),
            Some(&["user_id".to_string(), "tenant_id".to_string()][..])
        );
    }

    #[test]
    fn table_overrides_serialize_skips_when_empty() {
        // `skip_serializing_if = "Vec::is_empty"` keeps the JSON shape
        // stable for replication pipelines that don't opt into
        // overrides.
        let toml_str = override_pipeline_toml_base();
        let config: RockyConfig = toml::from_str(&toml_str).unwrap();
        let pipeline = config.pipelines["bronze"].as_replication().unwrap();
        let json = serde_json::to_string(pipeline).unwrap();
        assert!(
            !json.contains("table_overrides"),
            "JSON output should omit empty table_overrides: {json}"
        );
    }

    #[test]
    fn resolve_table_override_no_rules_returns_empty() {
        let resolved = resolve_table_override(&[], "conn_x", "stripe_main", "users");
        assert!(resolved.is_empty());
    }

    #[test]
    fn resolve_table_override_per_field_most_specific_wins() {
        // Less-specific rule sets `timestamp_column`, more-specific
        // rule sets `merge_keys`. Both must apply for the matched
        // table — per-field, not whole-rule.
        let rules = vec![
            TableOverride {
                match_: TableMatch {
                    connector: Some("stripe_main".to_string()),
                    table: None,
                },
                strategy: None,
                merge_keys: None,
                merge_keys_fallback: None,
                timestamp_column: Some("_stripe_synced".to_string()),
                enabled: None,
            },
            TableOverride {
                match_: TableMatch {
                    connector: Some("stripe_main".to_string()),
                    table: Some("pii_users".to_string()),
                },
                strategy: None,
                merge_keys: Some(vec!["user_id".to_string(), "tenant_id".to_string()]),
                merge_keys_fallback: None,
                timestamp_column: None,
                enabled: None,
            },
        ];
        let resolved = resolve_table_override(&rules, "conn_xyz", "stripe_main", "pii_users");
        assert_eq!(
            resolved.merge_keys,
            Some(vec!["user_id".to_string(), "tenant_id".to_string()])
        );
        assert_eq!(resolved.timestamp_column.as_deref(), Some("_stripe_synced"));
    }

    #[test]
    fn resolve_table_override_more_specific_wins_for_same_field() {
        // Both rules set `merge_keys`. The more-specific rule wins.
        let rules = vec![
            TableOverride {
                match_: TableMatch {
                    connector: Some("stripe_main".to_string()),
                    table: None,
                },
                strategy: None,
                merge_keys: Some(vec!["pipeline_id".to_string()]),
                merge_keys_fallback: None,
                timestamp_column: None,
                enabled: None,
            },
            TableOverride {
                match_: TableMatch {
                    connector: Some("stripe_main".to_string()),
                    table: Some("pii_users".to_string()),
                },
                strategy: None,
                merge_keys: Some(vec!["user_id".to_string()]),
                merge_keys_fallback: None,
                timestamp_column: None,
                enabled: None,
            },
        ];
        let resolved = resolve_table_override(&rules, "conn_xyz", "stripe_main", "pii_users");
        assert_eq!(resolved.merge_keys, Some(vec!["user_id".to_string()]));
    }

    #[test]
    fn resolve_table_override_connector_matches_id_or_schema() {
        // `match.connector` matches against either the discovered id
        // or the discovered schema name (OR semantics).
        let rule = TableOverride {
            match_: TableMatch {
                connector: Some("stripe_main".to_string()),
                table: Some("users".to_string()),
            },
            strategy: Some("incremental".to_string()),
            merge_keys: None,
            merge_keys_fallback: None,
            timestamp_column: None,
            enabled: None,
        };
        // Match against schema.
        let by_schema = resolve_table_override(
            std::slice::from_ref(&rule),
            "conn_abc",
            "stripe_main",
            "users",
        );
        assert_eq!(by_schema.strategy.as_deref(), Some("incremental"));
        // Match against id.
        let by_id = resolve_table_override(
            std::slice::from_ref(&rule),
            "stripe_main",
            "some_schema",
            "users",
        );
        assert_eq!(by_id.strategy.as_deref(), Some("incremental"));
        // No match against either.
        let no_match = resolve_table_override(&[rule], "conn_z", "other_schema", "users");
        assert!(no_match.is_empty());
    }

    #[test]
    fn resolve_table_override_glob_matches() {
        let rule = TableOverride {
            match_: TableMatch {
                connector: None,
                table: Some("_diagnostics_*".to_string()),
            },
            strategy: None,
            merge_keys: None,
            merge_keys_fallback: None,
            timestamp_column: None,
            enabled: Some(false),
        };
        let resolved = resolve_table_override(
            std::slice::from_ref(&rule),
            "c",
            "stripe_main",
            "_diagnostics_x",
        );
        assert_eq!(resolved.enabled, Some(false));
        let unmatched = resolve_table_override(&[rule], "c", "stripe_main", "users_diagnostics_no");
        assert!(unmatched.is_empty());
    }

    #[test]
    fn resolve_table_override_literal_beats_glob_at_same_pair_tier() {
        // Both rules match `(stripe_main, pii_users)`. The literal one
        // is more specific than the glob one.
        let rules = vec![
            TableOverride {
                match_: TableMatch {
                    connector: Some("stripe_main".to_string()),
                    table: Some("pii_*".to_string()),
                },
                strategy: None,
                merge_keys: Some(vec!["from_glob".to_string()]),
                merge_keys_fallback: None,
                timestamp_column: None,
                enabled: None,
            },
            TableOverride {
                match_: TableMatch {
                    connector: Some("stripe_main".to_string()),
                    table: Some("pii_users".to_string()),
                },
                strategy: None,
                merge_keys: Some(vec!["from_literal".to_string()]),
                merge_keys_fallback: None,
                timestamp_column: None,
                enabled: None,
            },
        ];
        let resolved = resolve_table_override(&rules, "c", "stripe_main", "pii_users");
        assert_eq!(resolved.merge_keys, Some(vec!["from_literal".to_string()]));
    }

    #[test]
    fn validate_replication_overrides_rejects_empty_match() {
        let mut toml_str = override_pipeline_toml_base();
        toml_str.push_str(
            r#"
[[pipeline.bronze.table_overrides]]
merge_keys = ["x"]
"#,
        );
        let config: RockyConfig = toml::from_str(&toml_str).unwrap();
        let errors = validate_replication_overrides(&config);
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            errors[0],
            ConfigError::TableOverrideEmptyMatch { rule_index: 0, .. }
        ));
    }

    #[test]
    fn validate_replication_overrides_rejects_duplicate_fully_specific_literal() {
        let mut toml_str = override_pipeline_toml_base();
        toml_str.push_str(
            r#"
[[pipeline.bronze.table_overrides]]
match.connector = "stripe_main"
match.table     = "pii_users"
merge_keys      = ["user_id"]

[[pipeline.bronze.table_overrides]]
match.connector = "stripe_main"
match.table     = "pii_users"
timestamp_column = "synced_at"
"#,
        );
        let config: RockyConfig = toml::from_str(&toml_str).unwrap();
        let errors = validate_replication_overrides(&config);
        assert_eq!(errors.len(), 1, "got: {errors:?}");
        assert!(matches!(
            errors[0],
            ConfigError::TableOverrideDuplicate {
                first_index: 0,
                second_index: 1,
                ..
            }
        ));
    }

    #[test]
    fn validate_replication_overrides_allows_duplicate_glob_table_patterns() {
        // Two glob rules with overlapping match sets are NOT treated
        // as duplicates at parse time — the resolver disambiguates by
        // declaration order if they tie on specificity. Only literal
        // fully-specific dupes fail-fast.
        let mut toml_str = override_pipeline_toml_base();
        toml_str.push_str(
            r#"
[[pipeline.bronze.table_overrides]]
match.connector = "stripe_main"
match.table     = "pii_*"
merge_keys      = ["x"]

[[pipeline.bronze.table_overrides]]
match.connector = "stripe_main"
match.table     = "*_users"
merge_keys      = ["y"]
"#,
        );
        let config: RockyConfig = toml::from_str(&toml_str).unwrap();
        let errors = validate_replication_overrides(&config);
        assert!(errors.is_empty(), "got: {errors:?}");
    }

    #[test]
    fn validate_replication_overrides_rejects_unreachable_merge_keys() {
        // Pipeline default is `strategy="incremental"` (no
        // merge_keys), and the override sets `strategy="merge"`
        // without keys → unreachable.
        let mut toml_str = String::from(merge_pipeline_toml_base());
        toml_str.push_str(
            r#"
strategy = "incremental"
timestamp_column = "_synced"

[pipeline.bronze.source]
catalog = "raw_catalog"

[pipeline.bronze.source.schema_pattern]
prefix = "src__"
separator = "__"
components = ["source"]

[pipeline.bronze.target]
catalog_template = "wh"
schema_template = "raw__{source}"

[[pipeline.bronze.table_overrides]]
match.connector = "stripe_main"
match.table     = "audit_log"
strategy        = "merge"
"#,
        );
        let config: RockyConfig = toml::from_str(&toml_str).unwrap();
        let errors = validate_replication_overrides(&config);
        assert_eq!(errors.len(), 1, "got: {errors:?}");
        assert!(matches!(
            errors[0],
            ConfigError::TableOverrideMergeMissingKeys { rule_index: 0, .. }
        ));
    }

    #[test]
    fn validate_replication_overrides_unreachable_keys_accepts_pipeline_default() {
        // Pipeline default supplies merge_keys, so an override that
        // sets strategy="merge" without its own keys is fine —
        // inheritance covers it.
        let mut toml_str = override_pipeline_toml_base();
        toml_str.push_str(
            r#"
[[pipeline.bronze.table_overrides]]
match.connector = "stripe_main"
match.table     = "audit_log"
strategy        = "merge"
"#,
        );
        let config: RockyConfig = toml::from_str(&toml_str).unwrap();
        let errors = validate_replication_overrides(&config);
        assert!(errors.is_empty(), "got: {errors:?}");
    }

    #[test]
    fn validate_replication_overrides_rejects_empty_glob_pattern() {
        let mut toml_str = override_pipeline_toml_base();
        toml_str.push_str(
            r#"
[[pipeline.bronze.table_overrides]]
match.connector = "stripe_main"
match.table     = ""
merge_keys      = ["x"]
"#,
        );
        let config: RockyConfig = toml::from_str(&toml_str).unwrap();
        let errors = validate_replication_overrides(&config);
        assert!(
            errors
                .iter()
                .any(|e| matches!(e, ConfigError::TableOverrideInvalidGlob { .. })),
            "expected TableOverrideInvalidGlob: {errors:?}"
        );
    }

    #[test]
    fn validate_schema_pattern_reserved_components_rejects_table() {
        let mut toml_str = String::from(merge_pipeline_toml_base());
        toml_str.push_str(
            r#"
strategy = "incremental"

[pipeline.bronze.source]
catalog = "raw_catalog"

[pipeline.bronze.source.schema_pattern]
prefix = "src__"
separator = "__"
components = ["client", "table"]

[pipeline.bronze.target]
catalog_template = "wh"
schema_template = "raw__{client}__{table}"
"#,
        );
        let config: RockyConfig = toml::from_str(&toml_str).unwrap();
        let errors = validate_schema_pattern_reserved_components(&config);
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            &errors[0],
            ConfigError::SchemaPatternReservedComponent { component, .. } if component == "table"
        ));
    }

    #[test]
    fn validate_schema_pattern_reserved_components_rejects_id() {
        let mut toml_str = String::from(merge_pipeline_toml_base());
        toml_str.push_str(
            r#"
strategy = "incremental"

[pipeline.bronze.source]
catalog = "raw_catalog"

[pipeline.bronze.source.schema_pattern]
prefix = "src__"
separator = "__"
components = ["client", "id"]

[pipeline.bronze.target]
catalog_template = "wh"
schema_template = "raw__{client}__{id}"
"#,
        );
        let config: RockyConfig = toml::from_str(&toml_str).unwrap();
        let errors = validate_schema_pattern_reserved_components(&config);
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            &errors[0],
            ConfigError::SchemaPatternReservedComponent { component, .. } if component == "id"
        ));
    }

    #[test]
    fn validate_schema_pattern_reserved_components_strips_variadic_suffix() {
        // `regions...` is fine; reserving `regions` would block a
        // common user pattern. The variadic stripping must look at
        // the bare name.
        let mut toml_str = String::from(merge_pipeline_toml_base());
        toml_str.push_str(
            r#"
strategy = "incremental"

[pipeline.bronze.source]
catalog = "raw_catalog"

[pipeline.bronze.source.schema_pattern]
prefix = "src__"
separator = "__"
components = ["client", "id..."]

[pipeline.bronze.target]
catalog_template = "wh"
schema_template = "raw__{client}"
"#,
        );
        let config: RockyConfig = toml::from_str(&toml_str).unwrap();
        let errors = validate_schema_pattern_reserved_components(&config);
        // `id...` (variadic) is rejected the same as `id` (literal)
        // because the bare name is still reserved.
        assert_eq!(errors.len(), 1);
    }

    #[test]
    fn load_rocky_config_fails_fast_on_empty_match() {
        use std::io::Write;
        let mut toml_str = override_pipeline_toml_base();
        toml_str.push_str(
            r#"
[[pipeline.bronze.table_overrides]]
strategy = "incremental"
"#,
        );
        let mut tmp = tempfile::NamedTempFile::new().expect("tempfile");
        tmp.write_all(toml_str.as_bytes()).expect("write");
        let err = load_rocky_config(tmp.path())
            .expect_err("load_rocky_config should reject empty match block");
        assert!(
            matches!(
                err,
                ConfigError::TableOverrideEmptyMatch { rule_index: 0, .. }
            ),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn merge_keys_omitted_from_serialized_output_when_none() {
        // `skip_serializing_if = "Option::is_none"` keeps the JSON / TOML
        // shape stable for existing replication pipelines — adding the
        // field doesn't perturb projects that never opt into merge.
        let mut toml_str = String::from(merge_pipeline_toml_base());
        toml_str.push_str(
            r#"
strategy = "incremental"
timestamp_column = "_synced"

[pipeline.bronze.source]
catalog = "raw_catalog"

[pipeline.bronze.source.schema_pattern]
prefix = "src__"
separator = "__"
components = ["source"]

[pipeline.bronze.target]
catalog_template = "wh"
schema_template = "raw__{source}"
"#,
        );
        let config: RockyConfig = toml::from_str(&toml_str).unwrap();
        let pipeline = config.pipelines["bronze"].as_replication().unwrap();
        let json = serde_json::to_string(pipeline).unwrap();
        assert!(
            !json.contains("merge_keys"),
            "JSON output should omit unset merge_keys/merge_keys_fallback: {json}"
        );
    }

    // ---------------------------------------------------------------
    // FivetranCacheConfig validation (FR-A)
    // ---------------------------------------------------------------

    fn fivetran_cache_base() -> String {
        // Minimal scaffolding so the fivetran adapter parses cleanly
        // (api_key + api_secret + destination_id required) and the
        // `validate_fivetran_cache` walk has something to find.
        r#"
[adapter.fivetran_main]
type = "fivetran"
kind = "discovery"
api_key = "k"
api_secret = "s"
destination_id = "dest_x"

[pipeline.bronze]
type = "replication"
strategy = "incremental"
adapter = "fivetran_main"

[pipeline.bronze.source]
adapter = "fivetran_main"

[pipeline.bronze.source.schema_pattern]
prefix = "src__"
separator = "__"
components = ["source"]

[pipeline.bronze.target]
catalog_template = "wh"
schema_template = "raw__{source}"
"#
        .to_string()
    }

    #[test]
    fn validate_fivetran_cache_passes_when_block_absent() {
        let toml_str = fivetran_cache_base();
        let config: RockyConfig = toml::from_str(&toml_str).unwrap();
        let errors = validate_fivetran_cache(&config);
        assert!(
            errors.is_empty(),
            "no cache block must validate: {errors:?}"
        );
    }

    #[test]
    fn validate_fivetran_cache_none_backend_passes() {
        let mut toml_str = fivetran_cache_base();
        toml_str.push_str(
            r#"
[adapter.fivetran_main.cache]
backend = "none"
"#,
        );
        let config: RockyConfig = toml::from_str(&toml_str).unwrap();
        let errors = validate_fivetran_cache(&config);
        assert!(errors.is_empty(), "none backend must validate: {errors:?}");
    }

    #[test]
    fn validate_fivetran_cache_file_requires_file_root() {
        let mut toml_str = fivetran_cache_base();
        toml_str.push_str(
            r#"
[adapter.fivetran_main.cache]
backend = "file"
"#,
        );
        let config: RockyConfig = toml::from_str(&toml_str).unwrap();
        let errors = validate_fivetran_cache(&config);
        assert_eq!(errors.len(), 1, "got: {errors:?}");
        assert!(matches!(
            errors[0],
            ConfigError::FivetranCacheMissingField { ref field, ref backend, .. }
                if field == "file_root" && backend == "file"
        ));
    }

    #[test]
    fn validate_fivetran_cache_object_store_requires_url() {
        let mut toml_str = fivetran_cache_base();
        toml_str.push_str(
            r#"
[adapter.fivetran_main.cache]
backend = "object_store"
"#,
        );
        let config: RockyConfig = toml::from_str(&toml_str).unwrap();
        let errors = validate_fivetran_cache(&config);
        assert_eq!(errors.len(), 1, "got: {errors:?}");
        assert!(matches!(
            errors[0],
            ConfigError::FivetranCacheMissingField { ref field, ref backend, .. }
                if field == "object_store_url" && backend == "object_store"
        ));
    }

    #[test]
    fn validate_fivetran_cache_valkey_requires_url() {
        let mut toml_str = fivetran_cache_base();
        toml_str.push_str(
            r#"
[adapter.fivetran_main.cache]
backend = "valkey"
"#,
        );
        let config: RockyConfig = toml::from_str(&toml_str).unwrap();
        let errors = validate_fivetran_cache(&config);
        assert_eq!(errors.len(), 1, "got: {errors:?}");
        assert!(matches!(
            errors[0],
            ConfigError::FivetranCacheMissingField { ref field, ref backend, .. }
                if field == "valkey_url" && backend == "valkey"
        ));
    }

    #[test]
    fn validate_fivetran_cache_tiered_requires_both_urls() {
        // Both URLs missing — validate returns the first one
        // encountered (object_store_url, by current source order).
        let mut toml_str = fivetran_cache_base();
        toml_str.push_str(
            r#"
[adapter.fivetran_main.cache]
backend = "tiered"
"#,
        );
        let config: RockyConfig = toml::from_str(&toml_str).unwrap();
        let errors = validate_fivetran_cache(&config);
        assert_eq!(errors.len(), 1, "got: {errors:?}");
        assert!(matches!(
            errors[0],
            ConfigError::FivetranCacheMissingField { ref field, ref backend, .. }
                if (field == "object_store_url" || field == "valkey_url") && backend == "tiered"
        ));
    }

    #[test]
    fn validate_fivetran_cache_tiered_partial_misses_valkey() {
        // object_store_url present; valkey_url missing — must error
        // on `valkey_url`.
        let mut toml_str = fivetran_cache_base();
        toml_str.push_str(
            r#"
[adapter.fivetran_main.cache]
backend = "tiered"
object_store_url = "s3://b/p/"
"#,
        );
        let config: RockyConfig = toml::from_str(&toml_str).unwrap();
        let errors = validate_fivetran_cache(&config);
        assert_eq!(errors.len(), 1, "got: {errors:?}");
        assert!(matches!(
            errors[0],
            ConfigError::FivetranCacheMissingField { ref field, .. } if field == "valkey_url"
        ));
    }

    #[test]
    fn validate_fivetran_cache_tiered_fully_specified_passes() {
        let mut toml_str = fivetran_cache_base();
        toml_str.push_str(
            r#"
[adapter.fivetran_main.cache]
backend = "tiered"
object_store_url = "s3://b/p/"
valkey_url = "redis://localhost:6379/"
"#,
        );
        let config: RockyConfig = toml::from_str(&toml_str).unwrap();
        let errors = validate_fivetran_cache(&config);
        assert!(
            errors.is_empty(),
            "fully-specified tiered must validate: {errors:?}"
        );
    }

    #[test]
    fn validate_fivetran_cache_ignores_non_fivetran_adapter() {
        // A cache block dangling off a databricks adapter (won't be
        // wired in code, but mustn't fail validation either — the
        // walk filters by adapter type).
        let toml_str = r#"
[adapter.warehouse]
type = "databricks"
host = "x.cloud.databricks.com"
http_path = "/sql/1.0/warehouses/abc"
token = "t"

[pipeline.bronze]
type = "replication"
strategy = "incremental"
adapter = "warehouse"

[pipeline.bronze.source]
adapter = "warehouse"
catalog = "src"

[pipeline.bronze.source.schema_pattern]
prefix = "src__"
separator = "__"
components = ["source"]

[pipeline.bronze.target]
catalog_template = "wh"
schema_template = "raw__{source}"
"#;
        let config: RockyConfig = toml::from_str(toml_str).unwrap();
        let errors = validate_fivetran_cache(&config);
        assert!(
            errors.is_empty(),
            "non-fivetran adapter ignored: {errors:?}"
        );
    }

    // ---------------------------------------------------------------
    // FivetranRatelimitConfig + FivetranStampedeConfig +
    // FivetranCircuitBreakerConfig validation (resilience layers)
    // ---------------------------------------------------------------

    #[test]
    fn validate_fivetran_resilience_passes_when_blocks_absent() {
        let toml_str = fivetran_cache_base();
        let config: RockyConfig = toml::from_str(&toml_str).unwrap();
        let errors = validate_fivetran_resilience(&config);
        assert!(errors.is_empty(), "absent blocks must validate: {errors:?}");
    }

    #[test]
    fn validate_fivetran_ratelimit_file_default_passes() {
        let mut toml_str = fivetran_cache_base();
        toml_str.push_str(
            r#"
[adapter.fivetran_main.ratelimit]
backend = "file"
"#,
        );
        let config: RockyConfig = toml::from_str(&toml_str).unwrap();
        let errors = validate_fivetran_resilience(&config);
        assert!(errors.is_empty(), "file backend must validate: {errors:?}");
    }

    #[test]
    fn validate_fivetran_ratelimit_valkey_requires_url() {
        let mut toml_str = fivetran_cache_base();
        toml_str.push_str(
            r#"
[adapter.fivetran_main.ratelimit]
backend = "valkey"
"#,
        );
        let config: RockyConfig = toml::from_str(&toml_str).unwrap();
        let errors = validate_fivetran_resilience(&config);
        assert_eq!(errors.len(), 1, "got: {errors:?}");
        assert!(matches!(
            errors[0],
            ConfigError::FivetranRatelimitMissingField { ref field, ref backend, .. }
                if field == "valkey_url" && backend == "valkey"
        ));
    }

    #[test]
    fn validate_fivetran_ratelimit_valkey_with_url_passes() {
        let mut toml_str = fivetran_cache_base();
        toml_str.push_str(
            r#"
[adapter.fivetran_main.ratelimit]
backend = "valkey"
valkey_url = "redis://localhost:6379/"
"#,
        );
        let config: RockyConfig = toml::from_str(&toml_str).unwrap();
        let errors = validate_fivetran_resilience(&config);
        assert!(
            errors.is_empty(),
            "valkey backend with url must validate: {errors:?}"
        );
    }

    #[test]
    fn validate_fivetran_stampede_none_default_passes() {
        let mut toml_str = fivetran_cache_base();
        toml_str.push_str(
            r#"
[adapter.fivetran_main.stampede]
backend = "none"
"#,
        );
        let config: RockyConfig = toml::from_str(&toml_str).unwrap();
        let errors = validate_fivetran_resilience(&config);
        assert!(errors.is_empty(), "none backend must validate: {errors:?}");
    }

    #[test]
    fn validate_fivetran_stampede_valkey_requires_url() {
        let mut toml_str = fivetran_cache_base();
        toml_str.push_str(
            r#"
[adapter.fivetran_main.stampede]
backend = "valkey"
"#,
        );
        let config: RockyConfig = toml::from_str(&toml_str).unwrap();
        let errors = validate_fivetran_resilience(&config);
        assert_eq!(errors.len(), 1, "got: {errors:?}");
        assert!(matches!(
            errors[0],
            ConfigError::FivetranStampedeMissingField { ref field, ref backend, .. }
                if field == "valkey_url" && backend == "valkey"
        ));
    }

    #[test]
    fn validate_fivetran_circuit_breaker_none_default_passes() {
        let mut toml_str = fivetran_cache_base();
        toml_str.push_str(
            r#"
[adapter.fivetran_main.circuit_breaker]
backend = "none"
"#,
        );
        let config: RockyConfig = toml::from_str(&toml_str).unwrap();
        let errors = validate_fivetran_resilience(&config);
        assert!(errors.is_empty(), "none backend must validate: {errors:?}");
    }

    #[test]
    fn validate_fivetran_circuit_breaker_valkey_requires_url() {
        let mut toml_str = fivetran_cache_base();
        toml_str.push_str(
            r#"
[adapter.fivetran_main.circuit_breaker]
backend = "valkey"
"#,
        );
        let config: RockyConfig = toml::from_str(&toml_str).unwrap();
        let errors = validate_fivetran_resilience(&config);
        assert_eq!(errors.len(), 1, "got: {errors:?}");
        assert!(matches!(
            errors[0],
            ConfigError::FivetranCircuitBreakerMissingField { ref field, ref backend, .. }
                if field == "valkey_url" && backend == "valkey"
        ));
    }

    #[test]
    fn validate_fivetran_resilience_full_block_passes() {
        // All three blocks present with valid Valkey URLs.
        let mut toml_str = fivetran_cache_base();
        toml_str.push_str(
            r#"
[adapter.fivetran_main.ratelimit]
backend = "valkey"
valkey_url = "redis://localhost:6379/"

[adapter.fivetran_main.stampede]
backend = "valkey"
valkey_url = "redis://localhost:6379/"
lock_ttl_seconds = 60
poll_timeout_seconds = 30

[adapter.fivetran_main.circuit_breaker]
backend = "valkey"
valkey_url = "redis://localhost:6379/"
failure_threshold = 5
window_seconds = 60
cooldown_seconds = 300
cooldown_max_seconds = 3600
"#,
        );
        let config: RockyConfig = toml::from_str(&toml_str).unwrap();
        let errors = validate_fivetran_resilience(&config);
        assert!(
            errors.is_empty(),
            "fully-specified resilience must validate: {errors:?}"
        );
    }

    #[test]
    fn validate_fivetran_resilience_ignores_non_fivetran_adapter() {
        // ratelimit block on a non-fivetran adapter is silently
        // ignored — won't be wired in code, mustn't error in
        // validation.
        let toml_str = r#"
[adapter.warehouse]
type = "databricks"
host = "x.cloud.databricks.com"
http_path = "/sql/1.0/warehouses/abc"
token = "t"

[adapter.warehouse.ratelimit]
backend = "valkey"
# valkey_url intentionally absent — would fail validation if checked

[pipeline.bronze]
type = "replication"
strategy = "incremental"
adapter = "warehouse"

[pipeline.bronze.source]
adapter = "warehouse"
catalog = "src"

[pipeline.bronze.source.schema_pattern]
prefix = "src__"
separator = "__"
components = ["source"]

[pipeline.bronze.target]
catalog_template = "wh"
schema_template = "raw__{source}"
"#;
        // Note: TOML parsing won't accept the ratelimit block on a
        // non-fivetran adapter when `deny_unknown_fields` is on at
        // AdapterConfig — but the field is declared on AdapterConfig
        // itself (as Option<...>), so parsing succeeds; the validator
        // is what filters.
        let parsed: Result<RockyConfig, _> = toml::from_str(toml_str);
        if let Ok(config) = parsed {
            let errors = validate_fivetran_resilience(&config);
            assert!(
                errors.is_empty(),
                "non-fivetran adapter must be ignored by validator: {errors:?}"
            );
        }
    }

    #[test]
    fn retry_config_threads_through_fivetran_adapter() {
        // Regression: the `[adapter.fivetran.retry]` block is part
        // of AdapterConfig (shared with every adapter type). This
        // test pins that a fivetran adapter's `retry` deserializes
        // and round-trips through `load_rocky_config`. The bonus
        // task item flagged this knob as "API-only"; this confirms
        // it's also TOML-wired.
        let toml_str = r#"
[adapter.fivetran_main]
type = "fivetran"
kind = "discovery"
api_key = "k"
api_secret = "s"
destination_id = "dest_x"

[adapter.fivetran_main.retry]
max_retries = 8
initial_backoff_ms = 1000
max_backoff_ms = 60000

[pipeline.bronze]
type = "replication"
strategy = "incremental"
adapter = "fivetran_main"

[pipeline.bronze.source]
adapter = "fivetran_main"

[pipeline.bronze.source.schema_pattern]
prefix = "src__"
separator = "__"
components = ["source"]

[pipeline.bronze.target]
catalog_template = "wh"
schema_template = "raw__{source}"
"#;
        let config: RockyConfig = toml::from_str(toml_str).expect("retry block must deserialize");
        let fv = &config.adapters["fivetran_main"];
        assert_eq!(fv.retry.max_retries, 8);
        assert_eq!(fv.retry.initial_backoff_ms, 1000);
        assert_eq!(fv.retry.max_backoff_ms, 60000);
    }

    #[test]
    fn test_cross_source_overlap_key_mutual_exclusion() {
        let cfg = |keys: Vec<&str>, key_expr: Option<&str>| CrossSourceOverlapConfig {
            keys: keys.into_iter().map(String::from).collect(),
            key_expr: key_expr.map(String::from),
            severity: crate::tests::TestSeverity::Error,
            max_overlap_rows: 0,
            sample: 20,
        };
        // keys only → the column list.
        assert_eq!(
            cfg(vec!["a", "b"], None).resolved_key_exprs().unwrap(),
            vec!["a".to_string(), "b".to_string()]
        );
        // key_expr only → single-element list.
        assert_eq!(
            cfg(vec![], Some("md5(a||b)")).resolved_key_exprs().unwrap(),
            vec!["md5(a||b)".to_string()]
        );
        // both set → error; neither set → error.
        assert!(cfg(vec!["a"], Some("x")).resolved_key_exprs().is_err());
        assert!(cfg(vec![], None).resolved_key_exprs().is_err());
        // invalid column name is rejected (injection guard on `keys`).
        assert!(cfg(vec!["a; DROP"], None).resolved_key_exprs().is_err());
    }
}
