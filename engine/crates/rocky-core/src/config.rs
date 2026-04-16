use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use indexmap::IndexMap;
use regex::Regex;
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

/// Controls parallelism and error handling for table processing.
///
/// Rocky processes tables within a run concurrently using async tasks.
/// Tune `concurrency` based on your warehouse capacity.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
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

/// State persistence configuration.
///
/// Controls where Rocky stores watermarks and anomaly history between runs.
/// On ephemeral environments (EKS pods), use S3, GCS, or Valkey for persistence.
///
/// When both S3 and Valkey are configured (`backend = "tiered"`):
/// - Download: Valkey first (fast), S3 fallback (durable)
/// - Upload: write to both Valkey + S3
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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
    /// Valkey/Redis URL for state persistence
    pub valkey_url: Option<String>,
    /// Valkey key prefix (default: "rocky:state:")
    pub valkey_prefix: Option<String>,
}

/// Retry policy for transient warehouse errors (HTTP 429/503, rate limits, timeouts).
///
/// Rocky retries transient errors with exponential backoff and optional jitter
/// to prevent thundering herd across concurrent runs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryConfig {
    /// Maximum number of retry attempts. Set to 0 to disable retries (e.g. for CI).
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

/// Schema pattern configuration from TOML, converted to [`SchemaPattern`] at runtime.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataColumnConfig {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: String,
    pub value: String,
}

/// Data quality checks configuration (row count, column match, freshness, null rate, custom).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ChecksConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub row_count: bool,
    #[serde(default)]
    pub column_match: bool,
    #[serde(default)]
    pub freshness: Option<FreshnessConfig>,
    #[serde(default)]
    pub null_rate: Option<NullRateConfig>,
    #[serde(default)]
    pub custom: Vec<CustomCheckConfig>,
    /// Row count anomaly detection threshold (percentage deviation from baseline).
    /// Default: 50.0 (50% deviation triggers anomaly). Set to 0 to disable.
    #[serde(default = "default_anomaly_threshold_pct")]
    pub anomaly_threshold_pct: f64,
}

fn default_anomaly_threshold_pct() -> f64 {
    50.0
}

/// Freshness check configuration with optional per-schema overrides.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FreshnessConfig {
    pub threshold_seconds: u64,
    /// Per-schema freshness overrides. Key is a schema pattern (e.g., "raw__us_west__shopify"),
    /// value overrides threshold_seconds for matching schemas.
    #[serde(default)]
    pub overrides: std::collections::HashMap<String, u64>,
}

/// Null rate check configuration: columns to check, threshold, and sample size.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NullRateConfig {
    pub columns: Vec<String>,
    pub threshold: f64,
    #[serde(default = "default_sample_percent")]
    pub sample_percent: u32,
}

fn default_sample_percent() -> u32 {
    10
}

/// A user-defined SQL check with a name, query template, and pass/fail threshold.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomCheckConfig {
    pub name: String,
    pub sql: String,
    #[serde(default)]
    pub threshold: u64,
}

/// Governance settings: auto-creation of catalogs/schemas, tags, isolation, and grants.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IsolationConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub workspace_ids: Vec<WorkspaceBindingConfig>,
}

/// A permission grant to apply to catalogs or schemas.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrantConfig {
    pub principal: String,
    pub permissions: Vec<String>,
}

/// Per-run governance override, merged additively on top of rocky.toml defaults.
///
/// Passed via `--governance-override` CLI flag as JSON string or `@file.json`.
/// Used for per-client workspace bindings and grants from external sources.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GovernanceOverride {
    #[serde(default)]
    pub workspace_ids: Vec<WorkspaceBindingConfig>,
    #[serde(default)]
    pub grants: Vec<GrantConfig>,
    #[serde(default)]
    pub schema_grants: Vec<GrantConfig>,
}

/// Schema evolution configuration.
///
/// Controls how Rocky handles columns that disappear from the source but
/// still exist in the target table. Instead of immediately dropping them,
/// Rocky can keep them for a grace period (filling with NULL) so downstream
/// consumers have time to adapt.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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

/// Valkey/Redis cache configuration for distributed caching.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    pub valkey_url: String,
}

/// Matches `${VAR}` and `${VAR:-default}` env-var substitution placeholders.
static ENV_VAR_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\$\{([^}]+)\}").expect("valid regex"));

/// Substitutes `${VAR_NAME}` and `${VAR_NAME:-default}` patterns with environment variable values.
///
/// - `${VAR}` — required, errors if not set
/// - `${VAR:-fallback}` — uses `fallback` if VAR is not set or empty
pub fn substitute_env_vars(input: &str) -> Result<String, ConfigError> {
    let re = &*ENV_VAR_RE;
    let mut result = String::with_capacity(input.len());
    let mut last_end = 0;
    // Track the first missing env var and its byte span for diagnostics.
    let mut first_missing: Option<(String, std::ops::Range<usize>)> = None;

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
        } else {
            match std::env::var(expr) {
                Ok(value) => {
                    result.push_str(&value);
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
    Ok(result)
}

// ===========================================================================
// Config v2: Named adapters + named pipelines
// ===========================================================================

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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RockyConfig {
    /// Global state persistence configuration.
    #[serde(default)]
    pub state: StateConfig,

    /// Named adapter configurations (keyed by adapter name).
    #[serde(default, rename = "adapter", alias = "adapters")]
    pub adapters: IndexMap<String, AdapterConfig>,

    /// Named pipeline configurations (keyed by pipeline name).
    #[serde(default, rename = "pipeline", alias = "pipelines")]
    pub pipelines: IndexMap<String, PipelineConfig>,

    /// Shell hooks configuration.
    #[serde(default, rename = "hook", alias = "hooks")]
    pub hooks: HooksConfig,

    /// Cost estimation configuration.
    #[serde(default)]
    pub cost: CostSection,

    /// Schema evolution configuration (grace-period column drops).
    #[serde(default)]
    pub schema_evolution: SchemaEvolutionConfig,
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Clone, Serialize, Deserialize)]
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
            .finish()
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

/// Replication pipeline configuration.
///
/// Copies tables from a source to a target using schema pattern discovery,
/// with optional incremental strategy, metadata columns, governance, and
/// data quality checks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationPipelineConfig {
    /// Replication strategy: "incremental" or "full_refresh".
    #[serde(default = "default_strategy")]
    pub strategy: String,

    /// Timestamp column for incremental strategy.
    #[serde(default = "default_timestamp_column")]
    pub timestamp_column: String,

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

    /// Pipeline dependencies for chaining (Phase 4).
    #[serde(default)]
    pub depends_on: Vec<String>,
}

/// Backward-compatible alias for [`ReplicationPipelineConfig`].
pub type PipelineConfigV2 = ReplicationPipelineConfig;

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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
}

/// Target configuration for quality pipelines (adapter reference only).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QualityTargetConfig {
    /// Name of the adapter to use (references a key in `[adapter.*]`).
    #[serde(default = "default_adapter_name")]
    pub adapter: String,
}

/// A reference to a specific catalog/schema/table for quality checks
/// and snapshot pipelines.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
}

/// Source table for a snapshot pipeline (explicit single-table reference).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotSourceConfig {
    /// Name of the adapter to use (references a key in `[adapter.*]`).
    #[serde(default = "default_adapter_name")]
    pub adapter: String,
    pub catalog: String,
    pub schema: String,
    pub table: String,
}

/// Target table for a snapshot pipeline (explicit single-table reference + governance).
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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

    /// Data quality checks run after loading.
    #[serde(default)]
    pub checks: ChecksConfig,

    /// Execution settings (concurrency, retries, etc.).
    #[serde(default)]
    pub execution: ExecutionConfig,

    /// Pipeline dependencies for chaining.
    #[serde(default)]
    pub depends_on: Vec<String>,
}

/// File format for load pipelines, parsed from TOML.
///
/// Mirrors `rocky_adapter_sdk::FileFormat` but lives in rocky-core to
/// avoid a hard dependency from config parsing to the adapter SDK.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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

/// Discovery configuration within a pipeline source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryConfig {
    /// Name of the adapter to use for discovery (references a key in `[adapter.*]`).
    /// Defaults to `"default"`.
    #[serde(default = "default_adapter_name")]
    pub adapter: String,
}

/// Pipeline target configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
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

/// Validates the `kind` field on every adapter block and every
/// `source.adapter` / `source.discovery.adapter` reference.
///
/// Runs after deserialization so parse-time errors point readers at the
/// exact config fix (e.g. "add `kind = \"discovery\"` to
/// [adapter.fivetran_main]") rather than failing deep inside the CLI
/// adapter registry at runtime.
///
/// Orthogonal to [`crate::config`]-level issues like unknown adapter types
/// or missing pipeline adapter references — those are surfaced by the
/// `rocky validate` command as rich diagnostics and by the CLI registry
/// at instantiation time. This function narrows its focus to the `kind`
/// invariants so the two error paths don't double-report.
pub fn validate_adapter_kinds(config: &RockyConfig) -> Result<(), ConfigError> {
    use crate::adapter_capability::capability_for;

    for (name, adapter) in &config.adapters {
        let Some(cap) = capability_for(&adapter.adapter_type) else {
            continue;
        };

        match (adapter.kind, cap.supports_data, cap.supports_discovery) {
            // Discovery-only type — `kind = "discovery"` is required so
            // the role is self-evident in the raw config file.
            (None, false, true) => {
                return Err(ConfigError::AdapterMissingDiscoveryKind {
                    name: name.clone(),
                    adapter_type: adapter.adapter_type.clone(),
                });
            }
            // Declared `kind` must be a role the adapter actually supports.
            (Some(AdapterKind::Data), false, _) => {
                return Err(ConfigError::AdapterKindUnsupported {
                    name: name.clone(),
                    adapter_type: adapter.adapter_type.clone(),
                    declared: "data".to_owned(),
                    supported: "discovery".to_owned(),
                });
            }
            (Some(AdapterKind::Discovery), _, false) => {
                return Err(ConfigError::AdapterKindUnsupported {
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
        if let Some(source_cfg) = config.adapters.get(&replication.source.adapter) {
            if capability_for(&source_cfg.adapter_type).is_some()
                && !adapter_role_active(source_cfg, AdapterKind::Data)
            {
                return Err(ConfigError::PipelineSourceAdapterNotData {
                    pipeline: pipeline_name.clone(),
                    adapter: replication.source.adapter.clone(),
                });
            }
        }

        if let Some(discovery) = &replication.source.discovery {
            if let Some(disc_cfg) = config.adapters.get(&discovery.adapter) {
                if capability_for(&disc_cfg.adapter_type).is_some()
                    && !adapter_role_active(disc_cfg, AdapterKind::Discovery)
                {
                    return Err(ConfigError::PipelineDiscoveryAdapterNotDiscovery {
                        pipeline: pipeline_name.clone(),
                        adapter: discovery.adapter.clone(),
                    });
                }
            }
        }
    }

    Ok(())
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

/// Loads and parses a Rocky configuration from a TOML file (v2 format only).
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
/// see [`validate_adapter_kinds`].
pub fn load_rocky_config(path: &Path) -> Result<RockyConfig, ConfigError> {
    let raw = std::fs::read_to_string(path).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            ConfigError::FileNotFound {
                path: path.to_path_buf(),
            }
        } else {
            ConfigError::ReadFile(e)
        }
    })?;
    let substituted = substitute_env_vars(&raw)?;
    let mut value: toml::Value = toml::from_str(&substituted)?;
    apply_deprecations(&mut value);
    normalize_toml_shorthands(&mut value);
    let config: RockyConfig = value.try_into()?;
    validate_adapter_kinds(&config)?;
    Ok(config)
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
        if let Some(adapter_val) = table.get("adapter") {
            if is_bare_adapter(adapter_val) {
                let adapter = table.remove("adapter").expect("key was just found above");
                let mut wrapper = toml::map::Map::new();
                wrapper.insert("default".to_string(), adapter);
                table.insert("adapter".to_string(), toml::Value::Table(wrapper));
            }
        }

        // Handle bare [pipeline] → [pipeline.default]
        if let Some(pipeline_val) = table.get("pipeline") {
            if is_bare_pipeline(pipeline_val) {
                let pipeline = table.remove("pipeline").expect("key was just found above");
                let mut wrapper = toml::map::Map::new();
                wrapper.insert("default".to_string(), pipeline);
                table.insert("pipeline".to_string(), toml::Value::Table(wrapper));
            }
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
    /// Builds a SchemaPattern from the source configuration.
    pub fn schema_pattern(&self) -> Result<SchemaPattern, crate::schema::SchemaError> {
        self.source.schema_pattern.to_schema_pattern()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let err = validate_adapter_kinds(&cfg).unwrap_err();
        match err {
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
        validate_adapter_kinds(&cfg).expect("config should validate");
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
        let err = validate_adapter_kinds(&cfg).unwrap_err();
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
        validate_adapter_kinds(&cfg).expect("config should validate");
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
        validate_adapter_kinds(&cfg).expect("duckdb should serve both roles without kind");
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
        let err = validate_adapter_kinds(&cfg).unwrap_err();
        assert!(matches!(
            err,
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
        let err = validate_adapter_kinds(&cfg).unwrap_err();
        assert!(matches!(
            err,
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
        validate_adapter_kinds(&cfg).expect("unknown adapter type is someone else's problem");
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
        assert_eq!(ov.workspace_ids.len(), 1);
        assert_eq!(ov.workspace_ids[0].id, 12345);
        assert_eq!(ov.workspace_ids[0].binding_type, BindingType::ReadOnly);
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
        assert!(q.checks.row_count);
        assert_eq!(q.checks.custom.len(), 1);
        assert_eq!(q.execution.concurrency, ConcurrencyMode::Fixed(2));
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
        assert!(l.checks.row_count);
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
            role: None,
            database: None,
            project_id: None,
            location: None,
            path: None,
            retry: RetryConfig::default(),
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
}
