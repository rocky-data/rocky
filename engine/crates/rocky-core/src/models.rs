use std::num::NonZeroU32;
use std::path::Path;
use std::str::FromStr;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::config::{ModelBudgetConfig, substitute_env_vars};
use crate::lakehouse::{LakehouseFormat, LakehouseOptions};
use crate::retention::{RetentionParseError, RetentionPolicy};
use crate::tests::TestDecl;
use crate::unit_test::UnitTestDef;
use rocky_ir::dag::DagNode;
use rocky_ir::{
    CostBudget, GovernanceConfig, MaterializationStrategy, ModelIr, SourceRef, TargetRef, TimeGrain,
};

/// Errors from loading and parsing model files.
#[derive(Debug, Error)]
pub enum ModelError {
    #[error("failed to read model file: {0}")]
    ReadFile(#[from] std::io::Error),

    #[error("model file '{path}' has no TOML frontmatter (expected ---toml ... --- block)")]
    MissingFrontmatter { path: String },

    #[error("failed to parse frontmatter in '{path}': {source}")]
    ParseFrontmatter {
        path: String,
        source: toml::de::Error,
    },

    #[error(
        "model '{model}' is missing target.{field} (set it in the sidecar or in _defaults.toml)"
    )]
    MissingTarget { model: String, field: String },

    #[error("_defaults.toml must not declare per-model field '{field}'")]
    InvalidDefaultsField { field: String },

    #[error("model '{model}' has invalid retention value '{value}': {reason}")]
    InvalidRetention {
        model: String,
        value: String,
        reason: String,
    },

    #[error("model '{model}' has an invalid surrogate_key: {reason}")]
    InvalidSurrogateKey { model: String, reason: String },

    #[error(
        "model '{model}' references unknown config group '{group}' (expected models/groups/{group}.toml)"
    )]
    UnknownGroup { model: String, group: String },

    #[error("model '{model}' has an invalid config group: {reason}")]
    InvalidGroup { model: String, reason: String },

    #[error(
        "model '{model}' overrides '{field}', which its enforced group '{group}' controls; remove the local override or set the group's enforce = false"
    )]
    GroupOverride {
        model: String,
        group: String,
        field: String,
    },

    #[error(
        "model '{model}' supplies [args] for config group '{group}'s schema_template but also pins its own target.schema; the pin overrides the template, so the [args] are dead. Remove the pinned schema to route via [args], or remove [args] if the pinned schema is intended."
    )]
    GroupArgsWithPinnedSchema { model: String, group: String },

    #[error(
        "model '{model}' references unknown named test '{name}' (expected a [{name}] entry in test_definitions.toml)"
    )]
    UnknownTest { model: String, name: String },

    #[error("failed to substitute env vars in '{path}': {source}")]
    EnvSubstitution {
        path: String,
        #[source]
        source: Box<crate::config::ConfigError>,
    },
}

/// Per-model `[governance]` sidecar block.
///
/// Distinct from the model's `[tags]` block: `[tags]` is Dagster-only asset
/// metadata (projected onto the asset's Dagster tags, never written to the
/// warehouse), whereas `[governance.tags]` are **applied to the warehouse
/// securable** after the model materializes — `ALTER VIEW ... SET TAGS` for
/// view-format models, `ALTER TABLE ... SET TAGS` otherwise. This mirrors the
/// replication path's target-governance tags, scoped to a single model.
///
/// ```toml
/// name = "fct_orders"
///
/// [governance.tags]
/// domain = "finance"
/// tier = "gold"
/// ```
///
/// Best-effort at apply time: a failure warns but never aborts the run, the
/// same posture as classification/retention governance. Empty maps are
/// skipped (Unity Catalog rejects `SET TAGS ()`).
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct ModelGovernanceConfig {
    /// Unity Catalog tags applied to the model's target securable
    /// (table or view) after materialization. Keys and values are free-form
    /// strings, used verbatim — no prefix is applied.
    #[serde(default)]
    pub tags: std::collections::BTreeMap<String, String>,
}

/// TOML frontmatter in a model SQL file.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ModelConfig {
    pub name: String,
    #[serde(default)]
    pub depends_on: Vec<String>,
    #[serde(default)]
    pub strategy: StrategyConfig,
    pub target: TargetConfig,
    #[serde(default)]
    pub sources: Vec<SourceConfig>,
    /// Per-model adapter override. When set, this model materializes using
    /// a different adapter than the pipeline default. References a key in
    /// `[adapter.*]` from `rocky.toml`.
    ///
    /// Enables cross-warehouse models within a single transformation pipeline
    /// — e.g., most models target Databricks but one targets Snowflake.
    #[serde(default)]
    pub adapter: Option<String>,
    /// Natural language description of what this model does.
    /// Used by `rocky ai sync` to propose updates when upstream schemas change.
    #[serde(default)]
    pub intent: Option<String>,
    /// Per-model freshness expectation. Declarative-only — the compiler does
    /// not enforce anything; downstream consumers (`dagster-rocky` to attach
    /// `FreshnessPolicy`, `rocky doctor --freshness` to surface stale
    /// models, the upcoming Dagster UI freshness badge) read this field
    /// from the compile JSON output.
    #[serde(default)]
    pub freshness: Option<ModelFreshnessConfig>,
    /// Declarative tests for this model. Parsed from `[[tests]]` arrays in
    /// the sidecar TOML. See [`crate::tests`] for the test types and SQL
    /// generation.
    #[serde(default)]
    pub tests: Vec<TestDecl>,
    /// Lakehouse table format. When set, the model is materialized using
    /// format-specific DDL (e.g., `USING DELTA`, `USING ICEBERG`) instead
    /// of the default `CREATE TABLE AS`. See [`LakehouseFormat`].
    #[serde(default)]
    pub format: Option<LakehouseFormat>,
    /// Format-specific options: partitioning, clustering, table properties,
    /// and comments. Only used when `format` is set.
    #[serde(default)]
    pub format_options: Option<LakehouseOptions>,
    /// Per-column classification tags. Keys are column names, values are
    /// free-form classification strings (e.g., `email = "pii"`). Parsed
    /// from a `[classification]` sidecar block:
    ///
    /// ```toml
    /// name = "users"
    ///
    /// [classification]
    /// email = "pii"
    /// phone = "pii"
    /// ssn = "confidential"
    /// ```
    ///
    /// Rocky resolves each value against `[mask]` / `[mask.<env>]` in
    /// `rocky.toml` to pick the masking strategy, then applies both the
    /// column tag and the mask via [`GovernanceAdapter`]. Tags are
    /// free-form strings — no enum — so teams can coin new classifications
    /// without touching the engine.
    ///
    /// [`GovernanceAdapter`]: crate::traits::GovernanceAdapter
    #[serde(default)]
    pub classification: std::collections::BTreeMap<String, String>,
    /// Model-level governance tags. Keys and values are free-form strings
    /// parsed from a `[tags]` sidecar block:
    ///
    /// ```toml
    /// name = "fct_orders"
    ///
    /// [tags]
    /// domain = "finance"
    /// tier = "gold"
    /// ```
    ///
    /// Unlike [`classification`](ModelConfig::classification) (which is keyed
    /// by *column* and drives masking), these describe the model as a whole.
    /// A model that opts into a config [`GroupConfig`] inherits the group's
    /// `[tags]` as a shared baseline; the model's own `[tags]` override per
    /// key (sidecar > group), mirroring the schema/strategy precedence.
    /// Surfaced on the `rocky compile` JSON output (`ModelDetail.tags`) so
    /// orchestrators (`dagster-rocky` projects them onto the asset's Dagster
    /// tags) can see the governed fan-out end-to-end.
    #[serde(default)]
    pub tags: std::collections::BTreeMap<String, String>,
    /// Per-model governance applied to the warehouse securable. Today this
    /// carries `[governance.tags]` — Unity Catalog tags written to the
    /// model's target table/view after it materializes. Parsed from a
    /// `[governance]` sidecar block; merged over any group-declared
    /// `[governance.tags]` in [`resolve_model_config`] (sidecar > group).
    ///
    /// Unlike [`tags`](ModelConfig::tags) (Dagster-only asset metadata), these
    /// are emitted as `ALTER TABLE/VIEW ... SET TAGS` during `rocky run`. See
    /// [`ModelGovernanceConfig`].
    #[serde(default)]
    pub governance: ModelGovernanceConfig,
    /// Declarative data-retention policy for this model. Parsed from the
    /// `retention` sidecar key:
    ///
    /// ```toml
    /// name = "events_daily"
    /// retention = "90d"   # grammar: \d+[dy] (days or years); null disables
    /// ```
    ///
    /// After the DAG run completes, Rocky forwards the resolved
    /// [`RetentionPolicy`] to the governance adapter via
    /// [`GovernanceAdapter::apply_retention_policy`]. On Databricks this
    /// compiles to a pair of Delta `TBLPROPERTIES`; on Snowflake to
    /// `DATA_RETENTION_TIME_IN_DAYS`. Best-effort — failures warn but
    /// never abort the run.
    ///
    /// Garbage inputs (`"abc"`, `"90"`, `"-3d"`, …) are rejected at
    /// sidecar parse time via [`ModelError::InvalidRetention`].
    ///
    /// [`RetentionPolicy`]: crate::retention::RetentionPolicy
    /// [`GovernanceAdapter::apply_retention_policy`]: crate::traits::GovernanceAdapter::apply_retention_policy
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention: Option<RetentionPolicy>,

    /// Per-model `[budget]` overrides. Same field set as the project-level
    /// [`crate::config::BudgetConfig`] but every field is `Option` —
    /// absent fields inherit from the project-level config. Resolved
    /// against the project-level via
    /// [`crate::config::ModelBudgetConfig::resolve`] when the
    /// pre-merge cost projection runs.
    ///
    /// ```toml
    /// name = "fct_orders_huge"
    ///
    /// [budget]
    /// max_usd = 5.0
    /// on_breach = "error"
    /// ```
    ///
    /// Per-model fields are the local authority — a sidecar
    /// `on_breach = "warn"` overrides a project-level
    /// `on_breach = "error"` for this one model.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub budget: Option<ModelBudgetConfig>,

    /// Per-model overrides for the opt-in `--skip-unchanged` gate. Parsed
    /// from a `[skip]` sidecar block. `None` ⇒ the model follows the gate's
    /// automatic eligibility rules (static determinism scan + plain
    /// strategy). See [`SkipConfig`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skip: Option<SkipConfig>,

    /// Pre-substitution value of the `name` field as it appeared in the
    /// sidecar TOML (or the filename stem when `name` was omitted).
    ///
    /// Captured before `${VAR}` / `${VAR:-default}` env substitution so
    /// lint rules that judge author intent (L001 — name matches filename)
    /// can compare against what the user wrote, not what the env
    /// resolved to. Internal only — never serialized, never appears in
    /// JSON schema.
    #[serde(default, skip)]
    #[schemars(skip)]
    pub name_declared: String,

    /// Pre-substitution value of `target.table` as it appeared in the
    /// sidecar TOML (or `name_declared` when `target.table` was omitted).
    ///
    /// Captured before env substitution so L002 (target.table matches
    /// name) can compare raw templates against raw templates — a
    /// `${VAR:-X}` default that happens to collapse to the model's
    /// `name` no longer trips the lint. Internal only — never
    /// serialized, never appears in JSON schema.
    #[serde(default, skip)]
    #[schemars(skip)]
    pub target_table_declared: String,
}

/// Per-model freshness configuration.
///
/// Declares the maximum allowed lag between successive materializations of
/// the model plus the optional timestamp column used by the runtime
/// freshness check.
///
/// The compiler does not enforce the TTL — it's metadata consumed by
/// downstream observability tooling (`dagster-rocky` `FreshnessPolicy`,
/// `rocky doctor --freshness`, etc.). The compiler does however soft-warn
/// (W005) when a model has at least one temporal output column but no
/// `freshness` declaration anywhere in scope (per-model or project-level
/// default).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ModelFreshnessConfig {
    /// Maximum lag in seconds before the model is considered stale.
    ///
    /// Accepts both `max_lag_seconds` (legacy field name, preserved for
    /// existing sidecar fixtures + dagster Pydantic + VS Code bindings)
    /// and `expected_lag_seconds` (the documented public-facing name
    /// matching dbt freshness + SQLMesh defaults). Both deserialize to
    /// the same field; the serialized name stays `max_lag_seconds` so
    /// existing JSON/codegen consumers keep working unchanged.
    #[serde(alias = "expected_lag_seconds")]
    pub max_lag_seconds: u64,
    /// Optional timestamp column used to evaluate freshness at runtime
    /// (`MAX(time_column) < NOW() - INTERVAL max_lag_seconds`). When
    /// unset the runtime falls back to the model's last-materialization
    /// timestamp from the state store.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_column: Option<String>,
    /// Severity reported when the freshness check trips. Default
    /// `warning` keeps the runtime check non-blocking — switch to
    /// `error` to fail the pipeline on stale data.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub severity: Option<crate::tests::TestSeverity>,
}

impl ModelFreshnessConfig {
    /// Construct from a project-level default (see
    /// [`crate::config::ProjectFreshnessConfig`]). Used when a model
    /// declares no `[freshness]` block of its own but the project has a
    /// `[freshness]` section that should inherit.
    pub fn from_project_default(default: &crate::config::ProjectFreshnessConfig) -> Option<Self> {
        let max_lag_seconds = default.expected_lag_seconds?;
        Some(Self {
            max_lag_seconds,
            time_column: default.time_column.clone(),
            severity: default.severity,
        })
    }
}

/// Target table coordinates for a model.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct TargetConfig {
    pub catalog: String,
    pub schema: String,
    pub table: String,
}

/// Source table reference for a model's input dependency.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SourceConfig {
    pub catalog: String,
    pub schema: String,
    pub table: String,
}

/// Materialization strategy for a model, defaulting to full refresh.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type")]
pub enum StrategyConfig {
    #[default]
    #[serde(rename = "full_refresh")]
    FullRefresh,
    #[serde(rename = "incremental")]
    Incremental { timestamp_column: String },
    #[serde(rename = "merge")]
    Merge {
        unique_key: Vec<String>,
        #[serde(default)]
        update_columns: Option<Vec<String>>,
    },
    /// Partition-keyed materialization. Each run targets specific partitions
    /// rather than appending past a watermark. Idempotent and backfill-friendly.
    ///
    /// The model SQL must reference `@start_date` and `@end_date` placeholders,
    /// which the runtime substitutes per partition with quoted timestamp literals.
    #[serde(rename = "time_interval")]
    TimeInterval {
        /// Column on the model output that holds the partition value.
        /// Must be a non-nullable date or timestamp column. Validated by the
        /// compiler against the typed output schema.
        time_column: String,
        /// Partition granularity (`hour`, `day`, `month`, `year`).
        granularity: TimeGrain,
        /// Recompute the previous N partitions on each run, in addition to
        /// whatever the CLI selected. Standard handling for late-arriving data.
        #[serde(default)]
        lookback: u32,
        /// Combine N consecutive partitions into one SQL statement when
        /// backfilling. Defaults to 1 (atomic per-partition replacement).
        #[serde(default = "default_batch_size")]
        batch_size: NonZeroU32,
        /// Lower bound for `--missing` discovery, in canonical key format
        /// (e.g., `"2024-01-01"` for daily). Required when `--missing` is used.
        #[serde(default)]
        first_partition: Option<String>,
    },
    /// Ephemeral model — inlined as CTE in downstream queries, no table created.
    #[serde(rename = "ephemeral")]
    Ephemeral,
    /// Delete+Insert: delete matching rows by partition key, then insert.
    #[serde(rename = "delete_insert")]
    DeleteInsert {
        /// Column(s) used to identify the partition to delete.
        partition_by: Vec<String>,
    },
    /// Microbatch: alias for time_interval with hourly defaults.
    /// dbt-compatible naming for partition-based incremental processing.
    #[serde(rename = "microbatch")]
    Microbatch {
        /// Timestamp column for micro-batch boundaries.
        timestamp_column: String,
        /// Batch granularity (default: Hour).
        #[serde(default = "default_microbatch_granularity")]
        granularity: TimeGrain,
    },
    /// SQL view — no physical storage. Every read against the target
    /// re-runs the model SELECT. Supported on every Rocky-targeted
    /// warehouse.
    #[serde(rename = "view")]
    View,
    /// Materialized view — warehouse manages refresh. Supported on
    /// Databricks, Snowflake, and BigQuery. DuckDB / Trino surface a
    /// "not supported" error at SQL-gen time.
    #[serde(rename = "materialized_view")]
    MaterializedView,
    /// Snowflake dynamic table — warehouse manages lag-based refresh.
    /// `target_lag` is a Snowflake lag specifier (e.g. `"1 minute"`,
    /// `"5 hours"`, `"downstream"`). Non-Snowflake adapters surface a
    /// "not supported" error at SQL-gen time.
    #[serde(rename = "dynamic_table")]
    DynamicTable {
        /// Snowflake lag specifier — alphanumeric + space only. Examples:
        /// `"1 minute"`, `"5 hours"`, `"downstream"`.
        target_lag: String,
    },
    /// Content-addressed write to a Delta UniForm table via the
    /// `rocky-iceberg` writer. The runtime executes the model SQL,
    /// converts the result to Arrow, blake3-hashes the Parquet bytes,
    /// uploads to `storage_prefix`, and emits a Delta log commit.
    /// Cross-engine reads (DuckDB iceberg_scan, etc.) require MSCK
    /// REPAIR after each commit; the runtime issues this automatically.
    #[serde(rename = "content_addressed")]
    ContentAddressed {
        /// Object-store key prefix that holds `_delta_log/` + Parquet
        /// files for the target table. Typically
        /// `s3://<bucket>/<path>/<table>` for AWS-backed deployments.
        storage_prefix: String,
        /// Logical partition column names. Empty for unpartitioned
        /// tables. The runtime asserts this matches the table's
        /// declared partition columns at materialization time.
        #[serde(default)]
        partition_columns: Vec<String>,
    },
}

fn default_batch_size() -> NonZeroU32 {
    NonZeroU32::new(1).expect("1 is non-zero")
}

fn default_microbatch_granularity() -> TimeGrain {
    TimeGrain::Hour
}

// ---------------------------------------------------------------------------
// Raw deserialization types (all fields optional for inference)
// ---------------------------------------------------------------------------

/// Permissive deserialization target for model sidecar TOML. Fields
/// absent here are inferred from filename, defaults file, or fallbacks.
/// Resolved into the strict [`ModelConfig`] by [`resolve_model_config`].
#[derive(Debug, Clone, Deserialize)]
pub struct RawModelConfig {
    pub name: Option<String>,
    #[serde(default)]
    pub depends_on: Vec<String>,
    #[serde(default)]
    pub strategy: Option<StrategyConfig>,
    pub target: Option<RawTargetConfig>,
    #[serde(default)]
    pub sources: Vec<SourceConfig>,
    /// Per-model adapter override (references an `[adapter.*]` key).
    #[serde(default)]
    pub adapter: Option<String>,
    #[serde(default)]
    pub intent: Option<String>,
    #[serde(default)]
    pub freshness: Option<ModelFreshnessConfig>,
    #[serde(default)]
    pub tests: Vec<TestDecl>,
    #[serde(default)]
    pub format: Option<LakehouseFormat>,
    #[serde(default)]
    pub format_options: Option<LakehouseOptions>,
    /// Column classification tags from the `[classification]` sidecar
    /// block. See [`ModelConfig::classification`].
    #[serde(default)]
    pub classification: std::collections::BTreeMap<String, String>,
    /// Model-level governance tags from the `[tags]` sidecar block. Merged
    /// over any group-declared tags in [`resolve_model_config`]. See
    /// [`ModelConfig::tags`].
    #[serde(default)]
    pub tags: std::collections::BTreeMap<String, String>,
    /// Per-model `[governance]` block — warehouse-applied tags. Merged over
    /// any group-declared `[governance.tags]` in [`resolve_model_config`].
    /// See [`ModelConfig::governance`].
    #[serde(default)]
    pub governance: ModelGovernanceConfig,
    /// Raw retention string from the `retention` sidecar key. Parsed into
    /// [`RetentionPolicy`] by [`resolve_model_config`]. Kept as
    /// `Option<String>` at the toml layer so parse errors surface as
    /// [`ModelError::InvalidRetention`] with the offending input visible,
    /// rather than a generic toml deserialization error.
    #[serde(default)]
    pub retention: Option<String>,

    /// Per-model `[budget]` overrides. See [`ModelConfig::budget`].
    #[serde(default)]
    pub budget: Option<ModelBudgetConfig>,

    /// Per-model `[skip]` overrides for the `--skip-unchanged` gate. See
    /// [`ModelConfig::skip`].
    #[serde(default)]
    pub skip: Option<SkipConfig>,

    /// Name of a config group (`models/groups/<name>.toml`) this model opts
    /// into. The group supplies shared routing (`schema_template`) and
    /// `strategy` for a fan-out of models; per-model sidecar fields still win
    /// over the group, which in turn wins over `_defaults.toml`. See
    /// [`GroupConfig`] and [`load_groups_from_dir`].
    #[serde(default)]
    pub group: Option<String>,

    /// Values that fill `{placeholder}`s in the group's `schema_template`
    /// (e.g. `{ region = "emea" }` resolving `mart_{region}` → `mart_emea`).
    /// Ignored when the model declares no `group`.
    #[serde(default)]
    pub args: std::collections::BTreeMap<String, String>,

    /// `[[use_test]]` references that apply named test definitions (from
    /// `test_definitions.toml`) to this model, resolved into concrete
    /// [`TestDecl`]s and appended to `tests` at load.
    #[serde(default)]
    pub use_test: Vec<TestRef>,
}

/// `[skip]` — per-model overrides for the opt-in model-skip gate.
///
/// The gate is conservative by default: a model is auto-skip-eligible only
/// when a static scan finds its SQL deterministic and it uses a plain
/// materialization strategy. This block lets a model owner override that
/// per model — either to force a model to always build, or to assert (and
/// take responsibility for) that a model the scan flagged is in fact pure
/// and therefore safe to treat as skip-eligible.
///
/// ```toml
/// name = "fct_orders"
///
/// [skip]
/// eligible = true        # opt this model in/out of the gate explicitly
/// deterministic = true   # owner asserts the SQL is pure → re-eligible
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SkipConfig {
    /// Explicit eligibility override. `Some(false)` ⇒ this model always
    /// builds, even when the gate is on and everything else looks unchanged
    /// (use for known-volatile models the static scan might miss).
    /// `Some(true)` ⇒ the model is eligible (subject to the other gate
    /// clauses). `None` ⇒ fall back to the automatic eligibility rules.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub eligible: Option<bool>,

    /// Owner assertion that this model's SQL is deterministic. `Some(true)`
    /// is the only way a model the static non-determinism scan flagged
    /// (timestamps, randomness, unresolved UDFs, …) becomes skip-eligible —
    /// an explicit, auditable, per-model opt-in. `Some(false)` forces the
    /// model to be treated as non-deterministic (never auto-skipped).
    /// `None` ⇒ trust the static scan.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deterministic: Option<bool>,
}

/// Permissive target config — all fields optional.
#[derive(Debug, Clone, Deserialize)]
pub struct RawTargetConfig {
    pub catalog: Option<String>,
    pub schema: Option<String>,
    pub table: Option<String>,
}

// ---------------------------------------------------------------------------
// Directory-level defaults (_defaults.toml)
// ---------------------------------------------------------------------------

/// Defaults loaded from `_defaults.toml` in a models directory.
///
/// Applied to every model in the directory before per-model sidecar
/// overrides. The `name`, `depends_on`, and `sources` fields are
/// per-model and cannot be defaulted.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct DirDefaults {
    #[serde(default)]
    pub target: Option<DirDefaultsTarget>,
    #[serde(default)]
    pub strategy: Option<StrategyConfig>,
    #[serde(default)]
    pub intent: Option<String>,
    #[serde(default)]
    pub freshness: Option<ModelFreshnessConfig>,
}

/// Target defaults (catalog and schema only — table is per-model).
#[derive(Debug, Clone, Deserialize)]
pub struct DirDefaultsTarget {
    pub catalog: Option<String>,
    pub schema: Option<String>,
}

/// Validates and loads a `_defaults.toml` file. Rejects per-model fields.
///
/// `${VAR}` and `${VAR:-default}` placeholders are resolved before parsing,
/// matching `rocky.toml` and per-model sidecar behavior. Lets directory-level
/// defaults (e.g. `target.schema = "${ROCKY_SCHEMA:-public}"`) be set per
/// orchestrator subprocess.
pub fn load_dir_defaults(path: &Path) -> Result<DirDefaults, ModelError> {
    let raw_content = std::fs::read_to_string(path)?;
    let content =
        substitute_env_vars(&raw_content).map_err(|source| ModelError::EnvSubstitution {
            path: path.display().to_string(),
            source: Box::new(source),
        })?;

    // Check for per-model fields that shouldn't be in defaults
    let raw: toml::Value = toml::from_str(&content).map_err(|e| ModelError::ParseFrontmatter {
        path: path.display().to_string(),
        source: e,
    })?;
    if let Some(table) = raw.as_table() {
        for field in &["name", "depends_on", "sources"] {
            if table.contains_key(*field) {
                return Err(ModelError::InvalidDefaultsField {
                    field: field.to_string(),
                });
            }
        }
    }

    let defaults: DirDefaults =
        toml::from_str(&content).map_err(|e| ModelError::ParseFrontmatter {
            path: path.display().to_string(),
            source: e,
        })?;
    Ok(defaults)
}

// ---------------------------------------------------------------------------
// Config groups (models/groups/<name>.toml)
// ---------------------------------------------------------------------------

/// A reusable config group: one definition that a fan-out of models opts into
/// by name (`group = "<name>"` in their sidecar). The group supplies shared
/// **routing** (`schema_template`, filled per model from its `args`) and a
/// shared `strategy`.
///
/// Precedence at resolution is **per-model sidecar > group > `_defaults.toml`**:
/// a model can still override anything the group sets, the group overrides the
/// directory defaults. Loaded from `models/groups/<name>.toml`, where the file
/// stem is the group name (mirroring how a model's name defaults to its `.sql`
/// stem). See [`load_groups_from_dir`].
///
/// ```toml
/// # models/groups/daily_marts.toml
/// schema_template = "mart_{region}"
///
/// [strategy]
/// type = "merge"
/// unique_key = ["id"]
/// ```
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GroupConfig {
    /// Target-schema template with `{placeholder}`s filled from each model's
    /// `args` (e.g. `mart_{region}`). Uses the same `{name}` / `{name:SEP}`
    /// grammar as schema-pattern templates.
    #[serde(default)]
    pub schema_template: Option<String>,
    /// Shared materialization strategy for every model in the group.
    #[serde(default)]
    pub strategy: Option<StrategyConfig>,
    /// Governance tags applied to every model in the group as a shared
    /// baseline. A member model's own `[tags]` override per key
    /// (sidecar > group). This is how a group makes its whole fan-out
    /// uniformly attributable — `[tags] domain = "finance"` once on the group
    /// tags every member model, and `dagster-rocky` projects them onto each
    /// asset. See [`ModelConfig::tags`].
    #[serde(default)]
    pub tags: std::collections::BTreeMap<String, String>,
    /// Group-level `[governance]` baseline. Its `[governance.tags]` apply to
    /// every member model's warehouse securable; a member's own
    /// `[governance.tags]` override per key (sidecar > group). Mirrors the
    /// `[tags]` precedence. See [`ModelConfig::governance`].
    #[serde(default)]
    pub governance: ModelGovernanceConfig,
    /// When `true`, the group's fields are **enforced**, not just defaulted: a
    /// member model that locally pins a field the group also sets (its target
    /// `schema`, or its `strategy`) fails the load with
    /// [`ModelError::GroupOverride`]. This is the governance guarantee — a model
    /// in an enforced group cannot quietly route or materialize itself
    /// differently from the rest of the group. Defaults to `false` (groups are
    /// overridable defaults unless the author opts in).
    #[serde(default)]
    pub enforce: bool,
}

/// Load config groups from `<models_dir>/groups/*.toml`. Each file defines one
/// group whose name is the file stem; `${VAR}` placeholders resolve at load
/// time (matching `rocky.toml` / sidecars). Returns an empty map when the
/// `groups/` directory is absent.
///
/// The model loader does not recurse into subdirectories, so `models/groups/`
/// is never walked as model sidecars — group files and model files cannot
/// collide.
pub fn load_groups_from_dir(
    models_dir: &Path,
) -> Result<std::collections::HashMap<String, GroupConfig>, ModelError> {
    let groups_dir = models_dir.join("groups");
    if !groups_dir.exists() {
        return Ok(std::collections::HashMap::new());
    }

    let mut out = std::collections::HashMap::new();
    for entry in std::fs::read_dir(&groups_dir)? {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) != Some("toml") {
            continue;
        }
        let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
            continue;
        };
        let raw = std::fs::read_to_string(&path)?;
        let content = substitute_env_vars(&raw).map_err(|source| ModelError::EnvSubstitution {
            path: path.display().to_string(),
            source: Box::new(source),
        })?;
        let group: GroupConfig =
            toml::from_str(&content).map_err(|e| ModelError::ParseFrontmatter {
                path: path.display().to_string(),
                source: e,
            })?;
        out.insert(stem.to_string(), group);
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Named test definitions (models/test_definitions.toml)
// ---------------------------------------------------------------------------

/// A reusable, named data-quality test: a [`crate::tests::TestType`] (the check
/// logic + its parameters) defined once and applied by name to many models /
/// columns via a `[[use_test]]` reference. Mirrors dbt's generic tests, but
/// declarative — no macro expansion.
///
/// ```toml
/// # models/test_definitions.toml
/// [positive_amount]
/// type = "expression"
/// expression = "amount > 0"
///
/// [known_status]
/// type = "accepted_values"
/// values = ["pending", "shipped", "delivered"]
/// ```
#[derive(Debug, Clone, Deserialize)]
pub struct NamedTest {
    /// The check logic and its type-specific parameters.
    #[serde(flatten)]
    pub test_type: crate::tests::TestType,
    /// Default column the test binds to. A `[[use_test]]` reference may supply
    /// or override the column at the use site.
    #[serde(default)]
    pub column: Option<String>,
}

/// A `[[use_test]]` reference in a model sidecar: apply the named test `name`
/// (from `test_definitions.toml`) to this model, optionally binding/overriding
/// the column, severity, and row filter at the use site.
///
/// `deny_unknown_fields` so a mistyped key in a `[[use_test]]` block (e.g.
/// `colum =` or `filer =`) is rejected at load instead of being silently
/// ignored, which would apply the test with the wrong binding.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TestRef {
    /// Name of the definition in `test_definitions.toml`.
    pub name: String,
    /// Column to bind the test to; falls back to the definition's `column`.
    #[serde(default)]
    pub column: Option<String>,
    /// Failure severity at this use site (defaults to `error`).
    #[serde(default)]
    pub severity: crate::tests::TestSeverity,
    /// Optional row-scoping SQL filter (same contract as an inline test).
    #[serde(default)]
    pub filter: Option<String>,
}

/// Load named test definitions from `<models_dir>/test_definitions.toml`, a
/// table of `name -> definition`. `${VAR}` placeholders resolve at load time.
/// Returns an empty map when the file is absent.
pub fn load_test_definitions_from_dir(
    models_dir: &Path,
) -> Result<std::collections::HashMap<String, NamedTest>, ModelError> {
    let path = models_dir.join("test_definitions.toml");
    if !path.exists() {
        return Ok(std::collections::HashMap::new());
    }
    let raw = std::fs::read_to_string(&path)?;
    let content = substitute_env_vars(&raw).map_err(|source| ModelError::EnvSubstitution {
        path: path.display().to_string(),
        source: Box::new(source),
    })?;
    let defs: std::collections::HashMap<String, NamedTest> =
        toml::from_str(&content).map_err(|e| ModelError::ParseFrontmatter {
            path: path.display().to_string(),
            source: e,
        })?;
    Ok(defs)
}

/// Directory-level shared config threaded into per-model resolution: config
/// groups and named test definitions. Cheap to pass by value (two borrows).
#[derive(Default, Clone, Copy)]
pub struct ModelLoadContext<'a> {
    /// Config groups (`models/groups/*.toml`), keyed by group name.
    pub groups: Option<&'a std::collections::HashMap<String, GroupConfig>>,
    /// Named test definitions (`models/test_definitions.toml`), keyed by name.
    pub test_defs: Option<&'a std::collections::HashMap<String, NamedTest>>,
}

/// Resolve a model's `[[use_test]]` references into concrete [`TestDecl`]s by
/// looking each name up in `test_defs`. An unknown reference is a hard error.
fn resolve_test_refs(
    refs: &[TestRef],
    test_defs: Option<&std::collections::HashMap<String, NamedTest>>,
    model: &str,
) -> Result<Vec<TestDecl>, ModelError> {
    refs.iter()
        .map(|r| {
            let def =
                test_defs
                    .and_then(|m| m.get(&r.name))
                    .ok_or_else(|| ModelError::UnknownTest {
                        model: model.to_string(),
                        name: r.name.clone(),
                    })?;
            Ok(TestDecl {
                test_type: def.test_type.clone(),
                column: r.column.clone().or_else(|| def.column.clone()),
                severity: r.severity,
                filter: r.filter.clone(),
            })
        })
        .collect()
}

/// Resolve a group's `schema_template` against a model's `args`, returning the
/// concrete target schema. Errors if the template references a placeholder the
/// model did not supply (so a misconfigured fan-out fails loudly at load rather
/// than routing a model to a literal `mart_{region}` schema).
fn resolve_group_schema(
    template: &str,
    args: &std::collections::BTreeMap<String, String>,
    model: &str,
    group: &str,
) -> Result<String, ModelError> {
    use crate::schema::{ParsedSchema, SchemaValue};
    let values = args
        .iter()
        .map(|(k, v)| (k.clone(), SchemaValue::Single(v.clone())))
        .collect();
    let parsed = ParsedSchema { values };
    // `resolve_template` leaves unknown `{placeholder}`s untouched; a residual
    // `{` therefore means the model omitted a value the template needs.
    let resolved = parsed.resolve_template(template, "_");
    if resolved.contains('{') {
        return Err(ModelError::InvalidGroup {
            model: model.to_string(),
            reason: format!(
                "group '{group}' schema_template '{template}' has unfilled placeholder(s); \
                 supply them under the model's [args]"
            ),
        });
    }
    // The resolved schema flows into `format_table_ref`, which validates every
    // component with `validate_identifier`. Validate here too — with the same
    // validator — so an `[args]` value carrying a bad character (e.g. a hyphen
    // or quote) fails at load with a clear message instead of at SQL-gen, and
    // load/run never disagree on what's acceptable.
    if rocky_sql::validation::validate_identifier(&resolved).is_err() {
        return Err(ModelError::InvalidGroup {
            model: model.to_string(),
            reason: format!(
                "group '{group}' schema_template resolved to '{resolved}', which is not a valid \
                 SQL identifier; check the model's [args] values"
            ),
        });
    }
    Ok(resolved)
}

/// Raw, pre-substitution view of the lint-relevant sidecar fields.
///
/// Captured from the TOML *before* `${VAR}` / `${VAR:-default}` env
/// substitution runs, so lints that judge author intent (L001, L002)
/// can compare against what the user actually wrote rather than what
/// the env resolved to.
///
/// Absent fields use the same fallback chain as the post-substitution
/// resolver — `name` falls back to the filename stem, `target.table`
/// falls back to `name`.
#[derive(Debug, Clone, Default)]
pub(crate) struct DeclaredModelFields {
    /// Raw `name` as it appeared in the sidecar, pre-substitution.
    /// Defaults to the filename stem when omitted.
    pub name: Option<String>,
    /// Raw `target.table` as it appeared in the sidecar, pre-substitution.
    /// Defaults to `name` (declared) when omitted.
    pub target_table: Option<String>,
}

/// Resolves a [`RawModelConfig`] into a strict [`ModelConfig`] by applying
/// filename inference and directory defaults.
///
/// Precedence: explicit sidecar field > directory default > filename inference.
fn resolve_model_config(
    raw: RawModelConfig,
    file_stem: &str,
    defaults: Option<&DirDefaults>,
    ctx: &ModelLoadContext,
    declared: DeclaredModelFields,
) -> Result<ModelConfig, ModelError> {
    let name = raw.name.unwrap_or_else(|| file_stem.to_string());
    let name_declared = declared.name.unwrap_or_else(|| file_stem.to_string());

    // Resolve the opted-into config group (if any) once. An unknown group is a
    // hard error so a typo doesn't silently skip the shared routing/strategy.
    let group: Option<&GroupConfig> = match raw.group.as_deref() {
        None => None,
        Some(g) => {
            Some(
                ctx.groups
                    .and_then(|m| m.get(g))
                    .ok_or_else(|| ModelError::UnknownGroup {
                        model: name.clone(),
                        group: g.to_string(),
                    })?,
            )
        }
    };

    // Resolve `[[use_test]]` references into concrete tests (appended to any
    // inline `[[tests]]`), looking each name up in the test-definition registry.
    let mut tests = raw.tests;
    tests.extend(resolve_test_refs(&raw.use_test, ctx.test_defs, &name)?);

    // Governance tags: the group's `[tags]` are the shared baseline; the
    // model's own `[tags]` override per key (sidecar > group), mirroring the
    // schema/strategy precedence. `extend` lets a member tag a sibling-shared
    // key with its own value without dropping the rest of the group's tags.
    let mut tags = group.map(|g| g.tags.clone()).unwrap_or_default();
    tags.extend(raw.tags);

    // Warehouse-applied governance tags follow the same precedence: the
    // group's `[governance.tags]` are the shared baseline; the model's own
    // `[governance.tags]` override per key (sidecar > group). These are
    // emitted as `ALTER TABLE/VIEW ... SET TAGS` at run time, distinct from
    // the Dagster-only `[tags]` above.
    let mut governance_tags = group.map(|g| g.governance.tags.clone()).unwrap_or_default();
    governance_tags.extend(raw.governance.tags);
    let governance = ModelGovernanceConfig {
        tags: governance_tags,
    };

    // Enforcement: when the group sets `enforce = true`, a member model may not
    // locally override a field the group controls. Checked here, before the
    // fields are consumed by the resolution chain below, using sidecar
    // provenance (`raw.<field>.is_some()` ⇒ the model set it locally).
    if let Some(g) = group
        && g.enforce
    {
        let group_name = raw.group.as_deref().unwrap_or_default();
        if g.strategy.is_some() && raw.strategy.is_some() {
            return Err(ModelError::GroupOverride {
                model: name.clone(),
                group: group_name.to_string(),
                field: "strategy".into(),
            });
        }
        if g.schema_template.is_some()
            && raw
                .target
                .as_ref()
                .and_then(|t| t.schema.as_ref())
                .is_some()
        {
            return Err(ModelError::GroupOverride {
                model: name.clone(),
                group: group_name.to_string(),
                field: "target.schema".into(),
            });
        }
    }

    // Misplacement guard (C5): `[args]` exist solely to fill a group's
    // `schema_template`. If a member also pins its own `target.schema`, the pin
    // wins (sidecar > group) and the `[args]` become dead — an incoherent
    // config that usually means the author believes `[args]` is routing the
    // model when the pin actually sends it elsewhere. Hard-error regardless of
    // `enforce` (this is a coherence check, not enforcement): an enforced group
    // already rejects the pin above, so this only adds the non-enforced case. A
    // legitimate non-enforced override pins a schema with NO `[args]`, which
    // stays allowed. (`[args]` is template-only; if a future feature consumes
    // args elsewhere, revisit this guard.)
    if let Some(g) = group
        && g.schema_template.is_some()
        && !raw.args.is_empty()
        && raw
            .target
            .as_ref()
            .and_then(|t| t.schema.as_ref())
            .is_some()
    {
        return Err(ModelError::GroupArgsWithPinnedSchema {
            model: name.clone(),
            group: raw.group.as_deref().unwrap_or_default().to_string(),
        });
    }

    // Precedence: per-model sidecar > group > directory defaults.
    let strategy = raw
        .strategy
        .or_else(|| group.and_then(|g| g.strategy.clone()))
        .or_else(|| defaults.and_then(|d| d.strategy.clone()))
        .unwrap_or_default();

    let intent = raw
        .intent
        .or_else(|| defaults.and_then(|d| d.intent.clone()));

    let freshness = raw
        .freshness
        .or_else(|| defaults.and_then(|d| d.freshness.clone()));

    // Resolve target
    let raw_target = raw.target.unwrap_or(RawTargetConfig {
        catalog: None,
        schema: None,
        table: None,
    });
    let dir_target = defaults.and_then(|d| d.target.as_ref());

    let catalog = raw_target
        .catalog
        .or_else(|| dir_target.and_then(|t| t.catalog.clone()))
        .ok_or_else(|| ModelError::MissingTarget {
            model: name.clone(),
            field: "catalog".into(),
        })?;

    // Group routing: when the sidecar doesn't pin a schema, fall back to the
    // group's `schema_template` (filled from the model's `args`) before the
    // directory default — keeping the sidecar > group > defaults precedence.
    //
    // Resolution is lazy: a model that pins its own `target.schema` overrides
    // the group, so the template is skipped entirely. This keeps a legitimate
    // pin-without-args override from erroring on an unfilled placeholder for a
    // template it never uses. (Pin *with* `[args]` is rejected by the
    // misplacement guard above; this branch handles the allowed pin-without-args
    // case and the no-pin routing case.)
    let group_schema = if raw_target.schema.is_some() {
        None
    } else {
        match group.and_then(|g| g.schema_template.as_deref()) {
            Some(template) => Some(resolve_group_schema(
                template,
                &raw.args,
                &name,
                raw.group.as_deref().unwrap_or_default(),
            )?),
            None => None,
        }
    };
    let schema = raw_target
        .schema
        .or(group_schema)
        .or_else(|| dir_target.and_then(|t| t.schema.clone()))
        .ok_or_else(|| ModelError::MissingTarget {
            model: name.clone(),
            field: "schema".into(),
        })?;

    let table = raw_target.table.unwrap_or_else(|| name.clone());
    let target_table_declared = declared
        .target_table
        .unwrap_or_else(|| name_declared.clone());

    // Parse `retention = "<N>[dy]"` into a typed RetentionPolicy. Garbage
    // inputs surface as ModelError::InvalidRetention so the diagnostic
    // names the model and the offending value.
    let retention = match raw.retention.as_deref() {
        None => None,
        Some(value) => {
            let policy = RetentionPolicy::from_str(value).map_err(|e: RetentionParseError| {
                ModelError::InvalidRetention {
                    model: name.clone(),
                    value: value.to_string(),
                    reason: e.to_string(),
                }
            })?;
            Some(policy)
        }
    };

    Ok(ModelConfig {
        name,
        depends_on: raw.depends_on,
        strategy,
        target: TargetConfig {
            catalog,
            schema,
            table,
        },
        sources: raw.sources,
        adapter: raw.adapter,
        intent,
        freshness,
        tests,
        format: raw.format,
        format_options: raw.format_options,
        classification: raw.classification,
        tags,
        governance,
        retention,
        budget: raw.budget,
        skip: raw.skip,
        name_declared,
        target_table_declared,
    })
}

/// Extract the pre-substitution `name` and `target.table` from raw TOML
/// text so L001/L002 lints can compare author intent against the
/// filename stem and the model name without env-resolved values muddying
/// the picture.
///
/// Returns an all-`None` [`DeclaredModelFields`] when the TOML fails to
/// parse — the post-substitution pass will surface that as the
/// authoritative error, and the lint pass falls back to the resolved
/// values (which match exactly when no env vars are involved).
pub(crate) fn extract_declared_fields(raw_toml: &str) -> DeclaredModelFields {
    let Ok(raw_value) = toml::from_str::<toml::Value>(raw_toml) else {
        return DeclaredModelFields::default();
    };

    let Some(table) = raw_value.as_table() else {
        return DeclaredModelFields::default();
    };

    let name = table
        .get("name")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let target_table = table
        .get("target")
        .and_then(|v| v.as_table())
        .and_then(|t| t.get("table"))
        .and_then(|v| v.as_str())
        .map(str::to_string);

    DeclaredModelFields { name, target_table }
}

/// A parsed model file (frontmatter + SQL body).
#[derive(Debug, Clone)]
pub struct Model {
    pub config: ModelConfig,
    pub sql: String,
    /// Path to the source file (for diagnostics).
    pub file_path: String,
    /// Path to an auto-discovered `<stem>.contract.toml` file, if present
    /// alongside the model's `.sql` file. The contract itself is parsed by
    /// the compiler crate — we only stash the path here to avoid a
    /// dependency from rocky-core → rocky-compiler.
    pub contract_path: Option<std::path::PathBuf>,
}

impl Model {
    /// Converts to a DagNode for topological sorting.
    pub fn to_dag_node(&self) -> DagNode {
        DagNode {
            name: self.config.name.clone(),
            depends_on: self.config.depends_on.clone(),
        }
    }

    /// Construct the per-model [`ModelIr`] for this transformation model.
    ///
    /// Lowers the [`StrategyConfig`] to a [`MaterializationStrategy`] and
    /// assembles the IR directly. The model's own `name` overrides the
    /// `target.table`-derived default so the IR carries the project-unique
    /// identifier rather than the warehouse table name.
    ///
    /// `typed_columns`, `lineage_edges`, and `column_masks` stay empty here
    /// — they are populated by the compiler / governance layers downstream
    /// when richer typed data is available.
    pub fn to_model_ir(&self) -> ModelIr {
        let strategy = match &self.config.strategy {
            StrategyConfig::FullRefresh => MaterializationStrategy::FullRefresh,
            StrategyConfig::Incremental { timestamp_column } => {
                MaterializationStrategy::Incremental {
                    timestamp_column: timestamp_column.clone(),
                }
            }
            StrategyConfig::Merge {
                unique_key,
                update_columns,
            } => MaterializationStrategy::Merge {
                unique_key: unique_key
                    .iter()
                    .map(|s| std::sync::Arc::from(s.as_str()))
                    .collect(),
                update_columns: match update_columns {
                    Some(cols) => rocky_ir::ColumnSelection::Explicit(
                        cols.iter()
                            .map(|s| std::sync::Arc::from(s.as_str()))
                            .collect(),
                    ),
                    None => rocky_ir::ColumnSelection::All,
                },
            },
            StrategyConfig::TimeInterval {
                time_column,
                granularity,
                ..
            } => MaterializationStrategy::TimeInterval {
                time_column: time_column.clone(),
                granularity: *granularity,
                // Window is populated by the runtime per partition; static
                // planning emits the strategy without a concrete window.
                window: None,
            },
            StrategyConfig::Ephemeral => MaterializationStrategy::Ephemeral,
            StrategyConfig::DeleteInsert { partition_by } => {
                MaterializationStrategy::DeleteInsert {
                    partition_by: partition_by
                        .iter()
                        .map(|s| std::sync::Arc::from(s.as_str()))
                        .collect(),
                }
            }
            StrategyConfig::Microbatch {
                timestamp_column,
                granularity,
            } => MaterializationStrategy::Microbatch {
                timestamp_column: timestamp_column.clone(),
                granularity: *granularity,
            },
            StrategyConfig::ContentAddressed {
                storage_prefix,
                partition_columns,
            } => MaterializationStrategy::ContentAddressed {
                storage_prefix: storage_prefix.clone(),
                partition_columns: partition_columns.clone(),
            },
            StrategyConfig::View => MaterializationStrategy::View,
            StrategyConfig::MaterializedView => MaterializationStrategy::MaterializedView,
            StrategyConfig::DynamicTable { target_lag } => MaterializationStrategy::DynamicTable {
                target_lag: target_lag.clone(),
            },
        };

        // Collapse a fully-absent budget (both cost fields None) into
        // cost_ceiling = None to keep the recipe-hash clean. Policy-only
        // sidecars (e.g. only `on_breach = "warn"` set) also yield None.
        let cost_ceiling = self.config.budget.as_ref().and_then(|b| {
            let candidate = CostBudget {
                max_usd: b.max_usd,
                max_bytes_scanned: b.max_bytes_scanned,
            };
            if candidate.is_empty() {
                None
            } else {
                Some(candidate)
            }
        });

        ModelIr {
            name: std::sync::Arc::from(self.config.name.as_str()),
            sql: self.sql.clone(),
            typed_columns: Vec::new(),
            lineage_edges: Vec::new(),
            materialization: strategy,
            governance: GovernanceConfig {
                permissions_file: None,
                auto_create_catalogs: false,
                auto_create_schemas: false,
            },
            target: TargetRef {
                catalog: self.config.target.catalog.clone(),
                schema: self.config.target.schema.clone(),
                table: self.config.target.table.clone(),
            },
            column_masks: Vec::new(),
            source: None,
            sources: self
                .config
                .sources
                .iter()
                .map(|s| SourceRef {
                    catalog: s.catalog.clone(),
                    schema: s.schema.clone(),
                    table: s.table.clone(),
                })
                .collect(),
            columns: None,
            metadata_columns: Vec::new(),
            unique_key: Vec::new(),
            updated_at: None,
            invalidate_hard_deletes: false,
            format: self.config.format.clone(),
            format_options: self.config.format_options.clone(),
            cost_ceiling,
        }
    }
}

/// Loads a model from a sidecar pair: `name.sql` + `name.toml`.
///
/// With inference, the sidecar can omit `name` (defaults to filename stem),
/// `target.table` (defaults to name), and `target.catalog`/`target.schema`
/// (inherited from `defaults` if provided).
///
/// `${VAR}` and `${VAR:-default}` placeholders in the sidecar TOML are
/// resolved before parsing, matching the substitution behavior of
/// `rocky.toml`. This lets an orchestrator inject per-model
/// `target.catalog` / `target.schema` / `target.table` via subprocess env
/// without rewriting source files.
///
/// ```text
/// models/
/// ├── _defaults.toml      ← optional: target.catalog, target.schema
/// ├── fct_orders.sql      ← pure SQL
/// ├── fct_orders.toml     ← config (only the diff from defaults)
/// ```
pub fn load_model_pair(
    sql_path: &Path,
    toml_path: &Path,
    defaults: Option<&DirDefaults>,
) -> Result<Model, ModelError> {
    load_model_pair_with_context(sql_path, toml_path, defaults, &ModelLoadContext::default())
}

/// [`load_model_pair`] with directory-level config resolution: config-group
/// references (`group = "<name>"`) and named-test references (`[[use_test]]`)
/// resolve against the [`ModelLoadContext`]. Bare `load_model_pair` passes an
/// empty context, preserving its behavior for every existing caller.
pub fn load_model_pair_with_context(
    sql_path: &Path,
    toml_path: &Path,
    defaults: Option<&DirDefaults>,
    ctx: &ModelLoadContext,
) -> Result<Model, ModelError> {
    let sql = {
        let s = std::fs::read_to_string(sql_path)?;
        let trimmed = s.trim();
        if trimmed.len() == s.len() {
            s // no trimming needed, reuse allocation
        } else {
            trimmed.to_string()
        }
    };
    let raw_toml = std::fs::read_to_string(toml_path)?;
    let declared = extract_declared_fields(&raw_toml);
    let toml_content =
        substitute_env_vars(&raw_toml).map_err(|source| ModelError::EnvSubstitution {
            path: toml_path.display().to_string(),
            source: Box::new(source),
        })?;
    let raw: RawModelConfig =
        toml::from_str(&toml_content).map_err(|e| ModelError::ParseFrontmatter {
            path: toml_path.display().to_string(),
            source: e,
        })?;

    let file_stem = sql_path
        .file_stem()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_default();

    let config = resolve_model_config(raw, &file_stem, defaults, ctx, declared)?;

    // Check for sibling contract file
    let contract_path = sql_path.with_extension("contract.toml");
    let contract_path = if contract_path.exists() {
        Some(contract_path)
    } else {
        None
    };

    Ok(Model {
        config,
        sql,
        file_path: sql_path.display().to_string(),
        contract_path,
    })
}

/// Parses a model from a SQL string with TOML frontmatter (legacy/inline format).
///
/// Prefer `load_model_pair()` with separate `.sql` + `.toml` files for full
/// editor support (syntax highlighting, linting, autocomplete).
pub fn parse_model_inline(
    content: &str,
    file_path: &str,
    defaults: Option<&DirDefaults>,
) -> Result<Model, ModelError> {
    parse_model_inline_with_context(content, file_path, defaults, &ModelLoadContext::default())
}

/// [`parse_model_inline`] with directory-level config resolution (groups +
/// named tests). See [`load_model_pair_with_context`]. Bare `parse_model_inline`
/// passes an empty context.
pub fn parse_model_inline_with_context(
    content: &str,
    file_path: &str,
    defaults: Option<&DirDefaults>,
    ctx: &ModelLoadContext,
) -> Result<Model, ModelError> {
    let (frontmatter, sql) =
        split_frontmatter(content).ok_or_else(|| ModelError::MissingFrontmatter {
            path: file_path.to_string(),
        })?;

    let declared = extract_declared_fields(frontmatter);

    let frontmatter =
        substitute_env_vars(frontmatter).map_err(|source| ModelError::EnvSubstitution {
            path: file_path.to_string(),
            source: Box::new(source),
        })?;

    let raw: RawModelConfig =
        toml::from_str(&frontmatter).map_err(|e| ModelError::ParseFrontmatter {
            path: file_path.to_string(),
            source: e,
        })?;

    let file_stem = Path::new(file_path)
        .file_stem()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_default();

    let config = resolve_model_config(raw, &file_stem, defaults, ctx, declared)?;

    // Check for sibling contract file (inline models can have them too)
    let contract_file = Path::new(file_path).with_extension("contract.toml");
    let contract_path = if contract_file.exists() {
        Some(contract_file)
    } else {
        None
    };

    Ok(Model {
        config,
        sql: sql.to_string(),
        file_path: file_path.to_string(),
        contract_path,
    })
}

/// Loads all models from a directory.
///
/// Supports two formats (sidecar preferred):
/// 1. **Sidecar:** `name.sql` + `name.toml` (separate files, full editor support)
/// 2. **Inline:** `name.sql` with `---toml` frontmatter (single file, legacy)
///
/// If `_defaults.toml` exists in the directory, it is loaded as
/// directory-level defaults for `target.catalog`, `target.schema`,
/// and `strategy`. Per-model sidecars override these defaults.
pub fn load_models_from_dir(dir: &Path) -> Result<Vec<Model>, ModelError> {
    if !dir.exists() {
        return Ok(Vec::new());
    }

    // Load optional directory defaults and config groups (once, upfront)
    let defaults_path = dir.join("_defaults.toml");
    let defaults = if defaults_path.exists() {
        Some(load_dir_defaults(&defaults_path)?)
    } else {
        None
    };
    let groups = load_groups_from_dir(dir)?;
    let test_defs = load_test_definitions_from_dir(dir)?;
    let ctx = ModelLoadContext {
        groups: (!groups.is_empty()).then_some(&groups),
        test_defs: (!test_defs.is_empty()).then_some(&test_defs),
    };

    // Collect all .sql file paths first (fs::read_dir is not Send)
    let sql_files: Vec<std::path::PathBuf> = std::fs::read_dir(dir)?
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();
            if path.file_name()?.to_str()? == "_defaults.toml" {
                return None;
            }
            if path.extension()?.to_str()? == "sql" {
                Some(path)
            } else {
                None
            }
        })
        .collect();

    // Load models in parallel
    use rayon::prelude::*;
    let mut models: Vec<Model> = sql_files
        .par_iter()
        .map(|path| {
            let toml_path = path.with_extension("toml");
            if toml_path.exists() {
                load_model_pair_with_context(path, &toml_path, defaults.as_ref(), &ctx)
            } else {
                let content = std::fs::read_to_string(path)?;
                parse_model_inline_with_context(
                    &content,
                    &path.display().to_string(),
                    defaults.as_ref(),
                    &ctx,
                )
            }
        })
        .collect::<Result<Vec<_>, _>>()?;

    models.sort_unstable_by(|a, b| a.config.name.cmp(&b.config.name));
    Ok(models)
}

/// Minimal sidecar view capturing only the model `name` and its fixture-driven
/// unit tests (`[[test]]` blocks), ignoring all other config keys.
#[derive(Deserialize)]
struct UnitTestSidecar {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    test: Vec<UnitTestDef>,
}

/// Load fixture-driven unit tests (`[[test]]` blocks) declared across a models
/// directory, keyed by model name.
///
/// Loaded independently of [`load_models_from_dir`] — but matching its flat,
/// sidecar-preferred discovery — so the test runner can pair each model's unit
/// tests with its compiled SQL without threading them through the compiler IR.
/// A model's name is its sidecar `name` field, or the file stem. Files that
/// declare no `[[test]]` blocks are omitted from the map.
pub fn load_unit_tests_from_dir(
    dir: &Path,
) -> Result<std::collections::HashMap<String, Vec<UnitTestDef>>, ModelError> {
    let mut out = std::collections::HashMap::new();
    if !dir.exists() {
        return Ok(out);
    }
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) != Some("sql") {
            continue;
        }
        let stem = path
            .file_stem()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_default();

        // Sidecar `.toml` is preferred; fall back to inline `---toml` frontmatter.
        let toml_path = path.with_extension("toml");
        let toml_src = if toml_path.exists() {
            std::fs::read_to_string(&toml_path)?
        } else {
            let content = std::fs::read_to_string(&path)?;
            match split_frontmatter(&content) {
                Some((frontmatter, _)) => frontmatter.to_string(),
                None => continue,
            }
        };

        let substituted =
            substitute_env_vars(&toml_src).map_err(|source| ModelError::EnvSubstitution {
                path: path.display().to_string(),
                source: Box::new(source),
            })?;
        let sidecar: UnitTestSidecar =
            toml::from_str(&substituted).map_err(|e| ModelError::ParseFrontmatter {
                path: path.display().to_string(),
                source: e,
            })?;
        if sidecar.test.is_empty() {
            continue;
        }
        out.insert(sidecar.name.unwrap_or(stem), sidecar.test);
    }
    Ok(out)
}

/// Per-column documentation entry from a sidecar `[columns.<name>]` table.
#[derive(Deserialize)]
struct ColumnDoc {
    #[serde(default)]
    description: Option<String>,
}

/// Minimal sidecar view capturing only the model `name` and its per-column
/// `[columns]` documentation, ignoring all other config keys.
#[derive(Deserialize)]
struct ColumnDocsSidecar {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    columns: std::collections::HashMap<String, ColumnDoc>,
}

/// Load per-column documentation (`[columns.<name>] description = "…"`) declared
/// across a models directory, keyed by model name then column name.
///
/// Loaded independently of [`load_models_from_dir`] — matching its flat,
/// sidecar-preferred discovery — so `rocky docs` / `rocky catalog` can attach
/// descriptions without threading them through the compiler IR. A model's name
/// is its sidecar `name` field, or the file stem. Files declaring no column
/// descriptions are omitted from the map.
pub fn load_column_docs_from_dir(
    dir: &Path,
) -> Result<std::collections::HashMap<String, std::collections::HashMap<String, String>>, ModelError>
{
    let mut out = std::collections::HashMap::new();
    if !dir.exists() {
        return Ok(out);
    }
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) != Some("sql") {
            continue;
        }
        let stem = path
            .file_stem()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_default();

        let toml_path = path.with_extension("toml");
        let toml_src = if toml_path.exists() {
            std::fs::read_to_string(&toml_path)?
        } else {
            let content = std::fs::read_to_string(&path)?;
            match split_frontmatter(&content) {
                Some((frontmatter, _)) => frontmatter.to_string(),
                None => continue,
            }
        };

        let substituted =
            substitute_env_vars(&toml_src).map_err(|source| ModelError::EnvSubstitution {
                path: path.display().to_string(),
                source: Box::new(source),
            })?;
        let sidecar: ColumnDocsSidecar =
            toml::from_str(&substituted).map_err(|e| ModelError::ParseFrontmatter {
                path: path.display().to_string(),
                source: e,
            })?;
        let descriptions: std::collections::HashMap<String, String> = sidecar
            .columns
            .into_iter()
            .filter_map(|(col, doc)| doc.description.map(|d| (col, d)))
            .collect();
        if descriptions.is_empty() {
            continue;
        }
        out.insert(sidecar.name.unwrap_or(stem), descriptions);
    }
    Ok(out)
}

/// A surrogate-key computed column declared in a model sidecar
/// `[[surrogate_key]]` block: an output column `name` whose value is a
/// deterministic hash of `columns`, injected into the materialized SELECT.
///
/// `deny_unknown_fields` so a typo in a `[[surrogate_key]]` block (e.g.
/// `colums = [...]`) fails the load loudly rather than silently dropping the
/// columns and computing the hash over nothing.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SurrogateKeySpec {
    pub name: String,
    pub columns: Vec<String>,
}

/// Minimal sidecar view capturing the model `name` and its `[[surrogate_key]]`
/// blocks, ignoring all other config keys.
#[derive(Deserialize)]
struct SurrogateKeySidecar {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    surrogate_key: Vec<SurrogateKeySpec>,
}

/// Load surrogate-key specs (`[[surrogate_key]]` blocks) declared across a
/// models directory, keyed by model name.
///
/// Loaded independently of [`load_models_from_dir`] — matching its flat,
/// sidecar-preferred discovery — so the materialization path can pair each
/// model with its key columns without threading them through the compiler IR.
/// A model's name is its sidecar `name` field, or the file stem. Files that
/// declare no `[[surrogate_key]]` blocks are omitted from the map.
pub fn load_surrogate_keys_from_dir(
    dir: &Path,
) -> Result<std::collections::HashMap<String, Vec<SurrogateKeySpec>>, ModelError> {
    let mut out = std::collections::HashMap::new();
    if !dir.exists() {
        return Ok(out);
    }
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) != Some("sql") {
            continue;
        }
        let stem = path
            .file_stem()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_default();

        let toml_path = path.with_extension("toml");
        let toml_src = if toml_path.exists() {
            std::fs::read_to_string(&toml_path)?
        } else {
            let content = std::fs::read_to_string(&path)?;
            match split_frontmatter(&content) {
                Some((frontmatter, _)) => frontmatter.to_string(),
                None => continue,
            }
        };

        let substituted =
            substitute_env_vars(&toml_src).map_err(|source| ModelError::EnvSubstitution {
                path: path.display().to_string(),
                source: Box::new(source),
            })?;
        let sidecar: SurrogateKeySidecar =
            toml::from_str(&substituted).map_err(|e| ModelError::ParseFrontmatter {
                path: path.display().to_string(),
                source: e,
            })?;
        if sidecar.surrogate_key.is_empty() {
            continue;
        }
        let model_name = sidecar.name.unwrap_or(stem);
        for spec in &sidecar.surrogate_key {
            validate_surrogate_key_spec(spec, &model_name)?;
        }
        out.insert(model_name, sidecar.surrogate_key);
    }
    Ok(out)
}

/// Resolve surrogate-key specs into dialect-correct [`rocky_ir::MetadataColumn`]s
/// for injection into a model's SELECT. Each spec becomes a metadata column
/// whose value is the dialect's `surrogate_key_expr` over the spec columns and
/// whose type is the dialect's string type — rendered by `select_clause` as
/// `CAST(<hash> AS <str>) AS <name>`.
pub fn surrogate_key_metadata_columns(
    specs: &[SurrogateKeySpec],
    dialect: &dyn crate::traits::SqlDialect,
) -> Vec<rocky_ir::MetadataColumn> {
    specs
        .iter()
        .map(|spec| {
            let cols: Vec<&str> = spec.columns.iter().map(String::as_str).collect();
            rocky_ir::MetadataColumn {
                name: spec.name.clone(),
                data_type: dialect.string_type_name().to_string(),
                value: dialect.surrogate_key_expr(&cols),
            }
        })
        .collect()
}

/// Validate a surrogate-key spec: a non-empty, valid-identifier output `name`
/// and at least one valid-identifier input column. These are the compile-time
/// invariants for the injected column, enforced at load so malformed config
/// fails loudly rather than emitting broken SQL.
fn validate_surrogate_key_spec(spec: &SurrogateKeySpec, model: &str) -> Result<(), ModelError> {
    let invalid = |reason: String| ModelError::InvalidSurrogateKey {
        model: model.to_string(),
        reason,
    };
    if rocky_sql::validation::validate_identifier(&spec.name).is_err() {
        return Err(invalid(format!(
            "output column name '{}' is not a valid SQL identifier",
            spec.name
        )));
    }
    if spec.columns.is_empty() {
        return Err(invalid(format!(
            "key '{}' must list at least one input column",
            spec.name
        )));
    }
    for col in &spec.columns {
        if rocky_sql::validation::validate_identifier(col).is_err() {
            return Err(invalid(format!(
                "key '{}' references invalid column identifier '{col}'",
                spec.name
            )));
        }
    }
    Ok(())
}

/// Wrap a transformation model's SELECT to append the declared surrogate-key
/// columns: `SELECT *, CAST(<hash> AS <str>) AS <name> FROM (<sql>) __rocky_keyed`.
/// A no-op when `specs` is empty. Used by both the materialization path and the
/// skip-gate IR, so a model that *gains* a key re-materializes rather than being
/// skipped as unchanged.
///
/// # Precondition
///
/// Every spec must already be validated by [`validate_surrogate_key_spec`] —
/// [`load_surrogate_keys_from_dir`] does this at load. The spec's `name` and
/// `columns` are interpolated into SQL without re-escaping (per the
/// validate-before-interpolation rule), so passing an unvalidated spec is a
/// SQL-injection vector. A `debug_assert!` re-checks the precondition in debug
/// builds.
pub fn apply_surrogate_keys(
    model_ir: &mut rocky_ir::ModelIr,
    specs: &[SurrogateKeySpec],
    dialect: &dyn crate::traits::SqlDialect,
) {
    if specs.is_empty() {
        return;
    }
    debug_assert!(
        specs
            .iter()
            .all(|s| validate_surrogate_key_spec(s, "").is_ok()),
        "apply_surrogate_keys received an unvalidated SurrogateKeySpec — callers must validate \
         via validate_surrogate_key_spec (load_surrogate_keys_from_dir does) before its fields \
         are interpolated into SQL"
    );
    let additions = surrogate_key_metadata_columns(specs, dialect)
        .iter()
        .map(|m| format!("CAST({} AS {}) AS {}", m.value, m.data_type, m.name))
        .collect::<Vec<_>>()
        .join(", ");
    let inner = model_ir.sql.trim().trim_end_matches(';');
    model_ir.sql = format!("SELECT *, {additions}\nFROM (\n{inner}\n) AS __rocky_keyed");
}

fn split_frontmatter(content: &str) -> Option<(&str, &str)> {
    let content = content.trim_start();

    // `strip_prefix` / `split_once` return &str slices on char boundaries, so
    // malformed input (including non-UTF-8-boundary markers in the SQL body)
    // can't panic the way raw `content[7..]` byte slicing could.
    let after_start = content.strip_prefix("---toml")?;
    let (frontmatter, sql) = after_start.split_once("\n---")?;

    Some((frontmatter.trim(), sql.trim()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_model_full_refresh() {
        let content = r#"---toml
name = "stg_orders"
depends_on = []
target = { catalog = "analytics", schema = "staging", table = "orders" }
---

SELECT * FROM raw.orders
"#;
        let model = parse_model_inline(content, "stg_orders.sql", None).unwrap();
        assert_eq!(model.config.name, "stg_orders");
        assert!(model.config.depends_on.is_empty());
        assert!(model.sql.contains("SELECT * FROM raw.orders"));
        assert!(matches!(model.config.strategy, StrategyConfig::FullRefresh));
    }

    #[test]
    fn test_parse_model_merge() {
        let content = r#"---toml
name = "dim_customers"
depends_on = ["stg_customers"]

[strategy]
type = "merge"
unique_key = ["customer_id"]

[target]
catalog = "analytics"
schema = "marts"
table = "dim_customers"
---

SELECT customer_id, name, email
FROM analytics.staging.customers
"#;
        let model = parse_model_inline(content, "dim_customers.sql", None).unwrap();
        assert_eq!(model.config.name, "dim_customers");
        assert_eq!(model.config.depends_on, vec!["stg_customers"]);
        assert!(matches!(
            model.config.strategy,
            StrategyConfig::Merge { .. }
        ));
    }

    #[test]
    fn test_parse_model_content_addressed() {
        let content = r#"---toml
name = "fct_events"

[strategy]
type = "content_addressed"
storage_prefix = "s3://example-bucket/dataset/fct_events"
partition_columns = ["region"]

[target]
catalog = "analytics"
schema = "marts"
table = "fct_events"
---

SELECT id, payload, region FROM raw.events
"#;
        let model = parse_model_inline(content, "fct_events.sql", None).unwrap();
        match &model.config.strategy {
            StrategyConfig::ContentAddressed {
                storage_prefix,
                partition_columns,
            } => {
                assert_eq!(storage_prefix, "s3://example-bucket/dataset/fct_events");
                assert_eq!(partition_columns, &vec!["region".to_string()]);
            }
            other => panic!("expected ContentAddressed, got {other:?}"),
        }

        // Lowering must produce the IR variant with the same fields.
        let ir = model.to_model_ir();
        match &ir.materialization {
            rocky_ir::MaterializationStrategy::ContentAddressed {
                storage_prefix,
                partition_columns,
            } => {
                assert_eq!(storage_prefix, "s3://example-bucket/dataset/fct_events");
                assert_eq!(partition_columns, &vec!["region".to_string()]);
            }
            other => panic!("expected IR ContentAddressed, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_model_content_addressed_unpartitioned() {
        // partition_columns is optional in the TOML; default is an empty vec.
        let content = r#"---toml
name = "fct_events"

[strategy]
type = "content_addressed"
storage_prefix = "s3://example-bucket/dataset/fct_events"

[target]
catalog = "analytics"
schema = "marts"
table = "fct_events"
---

SELECT id, payload FROM raw.events
"#;
        let model = parse_model_inline(content, "fct_events.sql", None).unwrap();
        match &model.config.strategy {
            StrategyConfig::ContentAddressed {
                storage_prefix,
                partition_columns,
            } => {
                assert_eq!(storage_prefix, "s3://example-bucket/dataset/fct_events");
                assert!(partition_columns.is_empty());
            }
            other => panic!("expected ContentAddressed, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_model_incremental() {
        let content = r#"---toml
name = "fct_orders"
depends_on = ["stg_orders", "dim_customers"]

[strategy]
type = "incremental"
timestamp_column = "updated_at"

[target]
catalog = "analytics"
schema = "marts"
table = "fct_orders"

[[sources]]
catalog = "analytics"
schema = "staging"
table = "orders"

[[sources]]
catalog = "analytics"
schema = "marts"
table = "dim_customers"
---

SELECT o.order_id, c.name, o.amount
FROM analytics.staging.orders o
JOIN analytics.marts.dim_customers c ON o.customer_id = c.customer_id
"#;
        let model = parse_model_inline(content, "fct_orders.sql", None).unwrap();
        assert_eq!(model.config.name, "fct_orders");
        assert_eq!(model.config.depends_on.len(), 2);
        assert_eq!(model.config.sources.len(), 2);

        let ir = model.to_model_ir();
        assert_eq!(ir.sources.len(), 2);
        assert!(ir.sql.contains("JOIN"));
    }

    #[test]
    fn test_parse_model_missing_frontmatter() {
        let content = "SELECT * FROM raw.orders";
        let result = parse_model_inline(content, "bad.sql", None);
        assert!(matches!(result, Err(ModelError::MissingFrontmatter { .. })));
    }

    #[test]
    fn test_to_dag_node() {
        let model = parse_model_inline(
            r#"---toml
name = "b"
depends_on = ["a"]
target = { catalog = "c", schema = "s", table = "t" }
---
SELECT 1
"#,
            "b.sql",
            None,
        )
        .unwrap();
        let node = model.to_dag_node();
        assert_eq!(node.name, "b");
        assert_eq!(node.depends_on, vec!["a"]);
    }

    #[test]
    fn test_to_plan_merge() {
        let model = parse_model_inline(
            r#"---toml
name = "dim"
[strategy]
type = "merge"
unique_key = ["id"]
update_columns = ["name", "status"]
[target]
catalog = "c"
schema = "s"
table = "t"
---
SELECT id, name, status FROM raw.src
"#,
            "dim.sql",
            None,
        )
        .unwrap();
        let ir = model.to_model_ir();
        assert!(matches!(
            ir.materialization,
            MaterializationStrategy::Merge { .. }
        ));
        if let MaterializationStrategy::Merge { update_columns, .. } = &ir.materialization {
            assert!(matches!(
                update_columns,
                rocky_ir::ColumnSelection::Explicit(_)
            ));
        }
    }

    #[test]
    fn test_parse_model_view_lowers_to_ir() {
        // `type = "view"` deserializes via `#[serde(rename = "view")]` on
        // `StrategyConfig::View` and lowers to `MaterializationStrategy::View`.
        let content = r#"---toml
name = "v_active_customers"
depends_on = ["dim_customers"]

[strategy]
type = "view"

[target]
catalog = "analytics"
schema = "marts"
table = "v_active_customers"
---
SELECT customer_id, name FROM analytics.marts.dim_customers WHERE status = 'active'
"#;
        let model = parse_model_inline(content, "v_active_customers.sql", None).unwrap();
        assert!(matches!(model.config.strategy, StrategyConfig::View));
        let ir = model.to_model_ir();
        assert!(matches!(ir.materialization, MaterializationStrategy::View));
    }

    #[test]
    fn test_parse_model_materialized_view_lowers_to_ir() {
        let content = r#"---toml
name = "mv_orders_daily"

[strategy]
type = "materialized_view"

[target]
catalog = "analytics"
schema = "marts"
table = "mv_orders_daily"
---
SELECT DATE(created_at) AS day, COUNT(*) FROM analytics.marts.fct_orders GROUP BY 1
"#;
        let model = parse_model_inline(content, "mv_orders_daily.sql", None).unwrap();
        assert!(matches!(
            model.config.strategy,
            StrategyConfig::MaterializedView
        ));
        let ir = model.to_model_ir();
        assert!(matches!(
            ir.materialization,
            MaterializationStrategy::MaterializedView
        ));
    }

    #[test]
    fn test_parse_model_dynamic_table_lowers_to_ir() {
        let content = r#"---toml
name = "dt_orders_recent"

[strategy]
type = "dynamic_table"
target_lag = "1 minute"

[target]
catalog = "analytics"
schema = "marts"
table = "dt_orders_recent"
---
SELECT id, customer_id, total FROM analytics.marts.fct_orders
"#;
        let model = parse_model_inline(content, "dt_orders_recent.sql", None).unwrap();
        match &model.config.strategy {
            StrategyConfig::DynamicTable { target_lag } => {
                assert_eq!(target_lag, "1 minute");
            }
            other => panic!("expected DynamicTable, got {other:?}"),
        }
        let ir = model.to_model_ir();
        match &ir.materialization {
            MaterializationStrategy::DynamicTable { target_lag } => {
                assert_eq!(target_lag, "1 minute");
            }
            other => panic!("expected IR DynamicTable, got {other:?}"),
        }
    }

    #[test]
    fn test_split_frontmatter() {
        let content = "---toml\nname = \"test\"\n---\n\nSELECT 1";
        let (fm, sql) = split_frontmatter(content).unwrap();
        assert_eq!(fm, "name = \"test\"");
        assert_eq!(sql, "SELECT 1");
    }

    #[test]
    fn test_to_model_ir_carries_model_name_not_target_table() {
        // The model name (`fct_orders`) and target.table (`fct_orders_v2`)
        // differ — `to_model_ir` must use the project-unique model name on
        // `ModelIr::name`, not whatever `From<&Plan>` would default to from
        // the target table.
        let model = parse_model_inline(
            r#"---toml
name = "fct_orders"
[target]
catalog = "analytics"
schema = "marts"
table = "fct_orders_v2"
---
SELECT 1
"#,
            "fct_orders.sql",
            None,
        )
        .unwrap();

        let ir = model.to_model_ir();
        assert_eq!(&*ir.name, "fct_orders");
        assert_eq!(ir.target.table, "fct_orders_v2");
    }

    // --- Sidecar format tests ---

    #[test]
    fn test_load_model_pair() {
        let dir = tempfile::TempDir::new().unwrap();

        std::fs::write(
            dir.path().join("fct_orders.toml"),
            r#"
name = "fct_orders"
depends_on = ["stg_orders"]

[strategy]
type = "merge"
unique_key = ["order_id"]

[target]
catalog = "analytics"
schema = "marts"
table = "fct_orders"
"#,
        )
        .unwrap();

        std::fs::write(
            dir.path().join("fct_orders.sql"),
            "SELECT order_id, amount FROM analytics.staging.orders",
        )
        .unwrap();

        let model = load_model_pair(
            &dir.path().join("fct_orders.sql"),
            &dir.path().join("fct_orders.toml"),
            None,
        )
        .unwrap();

        assert_eq!(model.config.name, "fct_orders");
        assert_eq!(model.config.depends_on, vec!["stg_orders"]);
        assert!(model.sql.contains("SELECT order_id"));
        assert!(matches!(
            model.config.strategy,
            StrategyConfig::Merge { .. }
        ));
    }

    #[test]
    fn test_load_models_from_dir_sidecar() {
        let dir = tempfile::TempDir::new().unwrap();

        // Model A: sidecar format
        std::fs::write(
            dir.path().join("model_a.toml"),
            r#"
name = "model_a"
target = { catalog = "c", schema = "s", table = "a" }
"#,
        )
        .unwrap();
        std::fs::write(dir.path().join("model_a.sql"), "SELECT 1").unwrap();

        // Model B: sidecar format
        std::fs::write(
            dir.path().join("model_b.toml"),
            r#"
name = "model_b"
depends_on = ["model_a"]
target = { catalog = "c", schema = "s", table = "b" }
"#,
        )
        .unwrap();
        std::fs::write(dir.path().join("model_b.sql"), "SELECT 2").unwrap();

        let models = load_models_from_dir(dir.path()).unwrap();
        assert_eq!(models.len(), 2);
        assert_eq!(models[0].config.name, "model_a");
        assert_eq!(models[1].config.name, "model_b");
    }

    #[test]
    fn test_load_models_from_dir_mixed() {
        let dir = tempfile::TempDir::new().unwrap();

        // Model A: sidecar format (pure SQL + TOML)
        std::fs::write(
            dir.path().join("sidecar.toml"),
            r#"
name = "sidecar"
target = { catalog = "c", schema = "s", table = "t" }
"#,
        )
        .unwrap();
        std::fs::write(dir.path().join("sidecar.sql"), "SELECT 1").unwrap();

        // Model B: inline frontmatter (legacy)
        std::fs::write(
            dir.path().join("inline.sql"),
            "---toml\nname = \"inline\"\n\n[target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"t\"\n---\n\nSELECT 2",
        )
        .unwrap();

        let models = load_models_from_dir(dir.path()).unwrap();
        assert_eq!(models.len(), 2);

        let names: Vec<&str> = models.iter().map(|m| m.config.name.as_str()).collect();
        assert!(names.contains(&"sidecar"));
        assert!(names.contains(&"inline"));
    }

    // ----- time_interval strategy tests -----

    #[test]
    fn test_time_interval_toml_deserialization_minimal() {
        // Bare minimum: just type, time_column, granularity. Defaults fill in
        // lookback=0, batch_size=1, first_partition=None.
        let toml_str = r#"
name = "fct_daily_orders"
target = { catalog = "c", schema = "s", table = "t" }

[strategy]
type = "time_interval"
time_column = "order_date"
granularity = "day"
"#;
        let cfg: ModelConfig = toml::from_str(toml_str).unwrap();
        match cfg.strategy {
            StrategyConfig::TimeInterval {
                ref time_column,
                granularity,
                lookback,
                batch_size,
                ref first_partition,
            } => {
                assert_eq!(time_column, "order_date");
                assert_eq!(granularity, TimeGrain::Day);
                assert_eq!(lookback, 0);
                assert_eq!(batch_size.get(), 1);
                assert!(first_partition.is_none());
            }
            _ => panic!("expected TimeInterval strategy"),
        }
    }

    #[test]
    fn test_time_interval_toml_full() {
        let toml_str = r#"
name = "fct_hourly_events"
target = { catalog = "c", schema = "s", table = "t" }

[strategy]
type = "time_interval"
time_column = "event_at"
granularity = "hour"
lookback = 3
batch_size = 7
first_partition = "2024-01-01T00"
"#;
        let cfg: ModelConfig = toml::from_str(toml_str).unwrap();
        match cfg.strategy {
            StrategyConfig::TimeInterval {
                granularity,
                lookback,
                batch_size,
                first_partition,
                ..
            } => {
                assert_eq!(granularity, TimeGrain::Hour);
                assert_eq!(lookback, 3);
                assert_eq!(batch_size.get(), 7);
                assert_eq!(first_partition.as_deref(), Some("2024-01-01T00"));
            }
            _ => panic!("expected TimeInterval"),
        }
    }

    #[test]
    fn test_time_interval_batch_size_zero_rejected() {
        // batch_size = 0 must be rejected by NonZeroU32 deserialization.
        let toml_str = r#"
name = "x"
target = { catalog = "c", schema = "s", table = "t" }
[strategy]
type = "time_interval"
time_column = "d"
granularity = "day"
batch_size = 0
"#;
        let result: Result<ModelConfig, _> = toml::from_str(toml_str);
        assert!(result.is_err(), "batch_size = 0 should fail to deserialize");
    }

    #[test]
    fn test_time_grain_format_strings() {
        assert_eq!(TimeGrain::Hour.format_str(), "%Y-%m-%dT%H");
        assert_eq!(TimeGrain::Day.format_str(), "%Y-%m-%d");
        assert_eq!(TimeGrain::Month.format_str(), "%Y-%m");
        assert_eq!(TimeGrain::Year.format_str(), "%Y");
    }

    #[test]
    fn test_time_grain_format_key() {
        use chrono::TimeZone;
        let t = chrono::Utc
            .with_ymd_and_hms(2026, 4, 7, 13, 30, 45)
            .unwrap();
        assert_eq!(TimeGrain::Hour.format_key(t), "2026-04-07T13");
        assert_eq!(TimeGrain::Day.format_key(t), "2026-04-07");
        assert_eq!(TimeGrain::Month.format_key(t), "2026-04");
        assert_eq!(TimeGrain::Year.format_key(t), "2026");
    }

    #[test]
    fn test_time_grain_truncate_drops_subwindow_state() {
        use chrono::TimeZone;
        let t = chrono::Utc
            .with_ymd_and_hms(2026, 4, 7, 13, 30, 45)
            .unwrap();
        assert_eq!(
            TimeGrain::Hour.truncate(t),
            chrono::Utc.with_ymd_and_hms(2026, 4, 7, 13, 0, 0).unwrap()
        );
        assert_eq!(
            TimeGrain::Day.truncate(t),
            chrono::Utc.with_ymd_and_hms(2026, 4, 7, 0, 0, 0).unwrap()
        );
        assert_eq!(
            TimeGrain::Month.truncate(t),
            chrono::Utc.with_ymd_and_hms(2026, 4, 1, 0, 0, 0).unwrap()
        );
        assert_eq!(
            TimeGrain::Year.truncate(t),
            chrono::Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap()
        );
    }

    #[test]
    fn test_time_grain_next_calendar_correct() {
        use chrono::TimeZone;

        // Hour: trivial
        let t = chrono::Utc.with_ymd_and_hms(2026, 4, 7, 13, 0, 0).unwrap();
        assert_eq!(
            TimeGrain::Hour.next(t),
            chrono::Utc.with_ymd_and_hms(2026, 4, 7, 14, 0, 0).unwrap()
        );

        // Day: trivial
        assert_eq!(
            TimeGrain::Day.next(chrono::Utc.with_ymd_and_hms(2026, 4, 7, 0, 0, 0).unwrap()),
            chrono::Utc.with_ymd_and_hms(2026, 4, 8, 0, 0, 0).unwrap()
        );

        // Month: 2024-02-01 → 2024-03-01 (29 days, leap year). NOT 30.
        let feb_2024 = chrono::Utc.with_ymd_and_hms(2024, 2, 1, 0, 0, 0).unwrap();
        assert_eq!(
            TimeGrain::Month.next(feb_2024),
            chrono::Utc.with_ymd_and_hms(2024, 3, 1, 0, 0, 0).unwrap()
        );

        // Month: 2025-02-01 → 2025-03-01 (28 days, non-leap)
        let feb_2025 = chrono::Utc.with_ymd_and_hms(2025, 2, 1, 0, 0, 0).unwrap();
        assert_eq!(
            TimeGrain::Month.next(feb_2025),
            chrono::Utc.with_ymd_and_hms(2025, 3, 1, 0, 0, 0).unwrap()
        );

        // Month: 2025-12 → 2026-01 (year boundary)
        let dec_2025 = chrono::Utc.with_ymd_and_hms(2025, 12, 1, 0, 0, 0).unwrap();
        assert_eq!(
            TimeGrain::Month.next(dec_2025),
            chrono::Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap()
        );

        // Year: 2024 → 2025
        assert_eq!(
            TimeGrain::Year.next(chrono::Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap()),
            chrono::Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap()
        );
    }

    #[test]
    fn test_time_interval_to_plan_drops_window() {
        // to_plan() emits a TimeInterval IR variant with window=None — the
        // runtime fills it in per partition.
        let toml_str = r#"
name = "m"
target = { catalog = "c", schema = "s", table = "t" }
[strategy]
type = "time_interval"
time_column = "d"
granularity = "day"
"#;
        let cfg: ModelConfig = toml::from_str(toml_str).unwrap();
        let model = Model {
            config: cfg,
            sql: "SELECT @start_date AS d".into(),
            file_path: "fake.sql".into(),
            contract_path: None,
        };
        let ir = model.to_model_ir();
        match ir.materialization {
            MaterializationStrategy::TimeInterval {
                time_column,
                granularity,
                window,
            } => {
                assert_eq!(time_column, "d");
                assert_eq!(granularity, TimeGrain::Day);
                assert!(window.is_none());
            }
            _ => panic!("expected TimeInterval IR variant"),
        }
    }

    // ----- Inference + defaults tests -----

    #[test]
    fn test_infer_name_from_filename() {
        let dir = tempfile::TempDir::new().unwrap();
        // No `name` field in the sidecar — should be inferred from filename
        std::fs::write(
            dir.path().join("fct_orders.toml"),
            r#"
[target]
catalog = "c"
schema = "s"
"#,
        )
        .unwrap();
        std::fs::write(dir.path().join("fct_orders.sql"), "SELECT 1").unwrap();

        let model = load_model_pair(
            &dir.path().join("fct_orders.sql"),
            &dir.path().join("fct_orders.toml"),
            None,
        )
        .unwrap();
        assert_eq!(model.config.name, "fct_orders");
    }

    #[test]
    fn test_infer_target_table_from_name() {
        let dir = tempfile::TempDir::new().unwrap();
        // `target.table` omitted — should default to `name`
        std::fs::write(
            dir.path().join("dim_customers.toml"),
            r#"
name = "dim_customers"
[target]
catalog = "c"
schema = "s"
"#,
        )
        .unwrap();
        std::fs::write(dir.path().join("dim_customers.sql"), "SELECT 1").unwrap();

        let model = load_model_pair(
            &dir.path().join("dim_customers.sql"),
            &dir.path().join("dim_customers.toml"),
            None,
        )
        .unwrap();
        assert_eq!(model.config.target.table, "dim_customers");
    }

    #[test]
    fn test_dir_defaults_supply_catalog_schema() {
        let dir = tempfile::TempDir::new().unwrap();

        // _defaults.toml with target catalog + schema
        std::fs::write(
            dir.path().join("_defaults.toml"),
            r#"
[target]
catalog = "poc"
schema = "demo"
"#,
        )
        .unwrap();

        // Minimal sidecar — no target at all
        std::fs::write(dir.path().join("m1.toml"), "").unwrap();
        std::fs::write(dir.path().join("m1.sql"), "SELECT 1").unwrap();

        // Sidecar that overrides schema
        std::fs::write(
            dir.path().join("m2.toml"),
            r#"
[target]
schema = "marts"
"#,
        )
        .unwrap();
        std::fs::write(dir.path().join("m2.sql"), "SELECT 2").unwrap();

        let models = load_models_from_dir(dir.path()).unwrap();
        assert_eq!(models.len(), 2);

        let m1 = models.iter().find(|m| m.config.name == "m1").unwrap();
        assert_eq!(m1.config.target.catalog, "poc");
        assert_eq!(m1.config.target.schema, "demo");
        assert_eq!(m1.config.target.table, "m1"); // inferred from filename

        let m2 = models.iter().find(|m| m.config.name == "m2").unwrap();
        assert_eq!(m2.config.target.catalog, "poc"); // from defaults
        assert_eq!(m2.config.target.schema, "marts"); // overridden
        assert_eq!(m2.config.target.table, "m2"); // inferred from filename
    }

    #[test]
    fn test_defaults_file_excluded_from_model_walk() {
        let dir = tempfile::TempDir::new().unwrap();

        std::fs::write(
            dir.path().join("_defaults.toml"),
            r#"
[target]
catalog = "c"
schema = "s"
"#,
        )
        .unwrap();

        std::fs::write(dir.path().join("only.toml"), "").unwrap();
        std::fs::write(dir.path().join("only.sql"), "SELECT 1").unwrap();

        let models = load_models_from_dir(dir.path()).unwrap();
        // Should only find "only", not "_defaults"
        assert_eq!(models.len(), 1);
        assert_eq!(models[0].config.name, "only");
    }

    #[test]
    fn test_defaults_rejects_per_model_fields() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(
            dir.path().join("_defaults.toml"),
            r#"
name = "this_should_fail"
[target]
catalog = "c"
schema = "s"
"#,
        )
        .unwrap();

        let result = load_dir_defaults(&dir.path().join("_defaults.toml"));
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("name"));
    }

    #[test]
    fn test_missing_target_catalog_errors() {
        let dir = tempfile::TempDir::new().unwrap();
        // No defaults, no catalog in sidecar → should error
        std::fs::write(dir.path().join("bad.toml"), "").unwrap();
        std::fs::write(dir.path().join("bad.sql"), "SELECT 1").unwrap();

        let result = load_models_from_dir(dir.path());
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("catalog"));
    }

    #[test]
    fn test_filename_with_dot_in_stem() {
        let dir = tempfile::TempDir::new().unwrap();
        // v2.fct_orders.sql → file_stem = "v2.fct_orders"
        std::fs::write(
            dir.path().join("v2.fct_orders.toml"),
            r#"
[target]
catalog = "c"
schema = "s"
"#,
        )
        .unwrap();
        std::fs::write(dir.path().join("v2.fct_orders.sql"), "SELECT 1").unwrap();

        let model = load_model_pair(
            &dir.path().join("v2.fct_orders.sql"),
            &dir.path().join("v2.fct_orders.toml"),
            None,
        )
        .unwrap();
        assert_eq!(model.config.name, "v2.fct_orders");
        assert_eq!(model.config.target.table, "v2.fct_orders");
    }

    #[test]
    fn test_explicit_name_overrides_filename() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(
            dir.path().join("file_name.toml"),
            r#"
name = "explicit_name"
[target]
catalog = "c"
schema = "s"
"#,
        )
        .unwrap();
        std::fs::write(dir.path().join("file_name.sql"), "SELECT 1").unwrap();

        let model = load_model_pair(
            &dir.path().join("file_name.sql"),
            &dir.path().join("file_name.toml"),
            None,
        )
        .unwrap();
        assert_eq!(model.config.name, "explicit_name");
        assert_eq!(model.config.target.table, "explicit_name");
    }

    #[test]
    fn test_dir_defaults_strategy_inherited() {
        let dir = tempfile::TempDir::new().unwrap();

        std::fs::write(
            dir.path().join("_defaults.toml"),
            r#"
[target]
catalog = "c"
schema = "s"

[strategy]
type = "incremental"
timestamp_column = "updated_at"
"#,
        )
        .unwrap();

        std::fs::write(dir.path().join("m.toml"), "").unwrap();
        std::fs::write(dir.path().join("m.sql"), "SELECT 1").unwrap();

        let models = load_models_from_dir(dir.path()).unwrap();
        assert_eq!(models.len(), 1);
        assert!(matches!(
            models[0].config.strategy,
            StrategyConfig::Incremental { .. }
        ));
    }

    // ------------------------------------------------------------------
    // [classification] sidecar block
    // ------------------------------------------------------------------

    #[test]
    fn test_parse_classification_block() {
        let content = r#"---toml
name = "users"
target = { catalog = "analytics", schema = "raw", table = "users" }

[classification]
email = "pii"
phone = "pii"
ssn = "confidential"
---

SELECT email, phone, ssn FROM raw.users
"#;
        let model = parse_model_inline(content, "users.sql", None).unwrap();
        assert_eq!(
            model.config.classification.get("email"),
            Some(&"pii".to_string())
        );
        assert_eq!(
            model.config.classification.get("phone"),
            Some(&"pii".to_string())
        );
        assert_eq!(
            model.config.classification.get("ssn"),
            Some(&"confidential".to_string())
        );
    }

    #[test]
    fn test_parse_classification_free_form_tags() {
        // Tags are free-form strings — teams should be able to coin
        // classifications beyond the canonical pii/confidential without
        // touching the engine.
        let content = r#"---toml
name = "telemetry"
target = { catalog = "ops", schema = "raw", table = "events" }

[classification]
user_id = "gdpr_subject"
ip_address = "location_adjacent"
---

SELECT * FROM raw.events
"#;
        let model = parse_model_inline(content, "telemetry.sql", None).unwrap();
        assert_eq!(
            model.config.classification.get("user_id"),
            Some(&"gdpr_subject".to_string())
        );
        assert_eq!(
            model.config.classification.get("ip_address"),
            Some(&"location_adjacent".to_string())
        );
    }

    #[test]
    fn test_parse_governance_tags_block() {
        // `[governance.tags]` lands in `config.governance.tags` (warehouse-
        // applied), and the Dagster-only `[tags]` stays separate and untouched.
        let content = r#"---toml
name = "fct_orders"
target = { catalog = "warehouse", schema = "marts", table = "fct_orders" }

[tags]
owner = "analytics"

[governance.tags]
domain = "finance"
tier = "gold"
---

SELECT 1
"#;
        let model = parse_model_inline(content, "fct_orders.sql", None).unwrap();
        assert_eq!(
            model.config.governance.tags.get("domain"),
            Some(&"finance".to_string())
        );
        assert_eq!(
            model.config.governance.tags.get("tier"),
            Some(&"gold".to_string())
        );
        // `[tags]` (Dagster-only) is NOT promoted into governance.tags.
        assert!(
            !model.config.governance.tags.contains_key("owner"),
            "[tags] must not leak into [governance.tags]"
        );
        assert_eq!(
            model.config.tags.get("owner"),
            Some(&"analytics".to_string())
        );
        assert!(
            !model.config.tags.contains_key("domain"),
            "[governance.tags] must not leak into [tags]"
        );
    }

    #[test]
    fn test_parse_no_governance_block_defaults_empty() {
        let content = r#"---toml
name = "no_gov"
target = { catalog = "c", schema = "s", table = "t" }
---

SELECT 1
"#;
        let model = parse_model_inline(content, "no_gov.sql", None).unwrap();
        assert!(model.config.governance.tags.is_empty());
    }

    #[test]
    fn test_parse_no_classification_block_defaults_empty() {
        let content = r#"---toml
name = "no_class"
target = { catalog = "c", schema = "s", table = "t" }
---

SELECT 1
"#;
        let model = parse_model_inline(content, "no_class.sql", None).unwrap();
        assert!(model.config.classification.is_empty());
    }

    // ---- retention sidecar parsing ----

    #[test]
    fn test_parse_retention_days() {
        let content = r#"---toml
name = "events_daily"
target = { catalog = "ops", schema = "raw", table = "events" }
retention = "90d"
---

SELECT * FROM raw.events
"#;
        let model = parse_model_inline(content, "events_daily.sql", None).unwrap();
        assert_eq!(model.config.retention.map(|r| r.duration_days), Some(90));
    }

    #[test]
    fn test_parse_retention_years_flat_365() {
        let content = r#"---toml
name = "long_history"
target = { catalog = "ops", schema = "raw", table = "events" }
retention = "3y"
---

SELECT * FROM raw.events
"#;
        let model = parse_model_inline(content, "long_history.sql", None).unwrap();
        assert_eq!(
            model.config.retention.map(|r| r.duration_days),
            Some(3 * 365)
        );
    }

    #[test]
    fn test_parse_no_retention_defaults_none() {
        let content = r#"---toml
name = "no_retention"
target = { catalog = "c", schema = "s", table = "t" }
---

SELECT 1
"#;
        let model = parse_model_inline(content, "no_retention.sql", None).unwrap();
        assert!(model.config.retention.is_none());
    }

    #[test]
    fn test_parse_retention_rejects_garbage() {
        // Every reject case from the waveplan — "abc", "90", "-3d", "-1y" —
        // surfaces as InvalidRetention with the value + reason threaded
        // through from RetentionParseError.
        for bad in &["abc", "90", "-3d", "-1y", "90days"] {
            let content = format!(
                r#"---toml
name = "m"
target = {{ catalog = "c", schema = "s", table = "t" }}
retention = "{bad}"
---

SELECT 1
"#
            );
            let err = parse_model_inline(&content, "m.sql", None).unwrap_err();
            match err {
                ModelError::InvalidRetention { value, .. } => {
                    assert_eq!(value, *bad, "value field should be {bad}, got {value}");
                }
                other => panic!("expected InvalidRetention for {bad:?}, got {other}"),
            }
        }
    }

    #[test]
    fn test_parse_sidecar_classification_from_toml_file() {
        use tempfile::tempdir;
        let dir = tempdir().unwrap();
        std::fs::write(
            dir.path().join("users.toml"),
            r#"
name = "users"

[target]
catalog = "analytics"
schema = "raw"
table = "users"

[classification]
email = "pii"
ssn = "confidential"
"#,
        )
        .unwrap();
        std::fs::write(
            dir.path().join("users.sql"),
            "SELECT email, ssn FROM upstream.users",
        )
        .unwrap();

        let models = load_models_from_dir(dir.path()).unwrap();
        assert_eq!(models.len(), 1);
        let m = &models[0];
        assert_eq!(
            m.config.classification.get("email"),
            Some(&"pii".to_string())
        );
        assert_eq!(
            m.config.classification.get("ssn"),
            Some(&"confidential".to_string())
        );
    }

    #[test]
    fn load_column_docs_reads_sidecar_columns_table() {
        use tempfile::tempdir;
        let dir = tempdir().unwrap();
        std::fs::write(
            dir.path().join("orders.toml"),
            r#"
[target]
catalog = "wh"
schema = "main"

[columns.order_id]
description = "Unique order identifier"

[columns.amount]
description = "Order total in USD"
"#,
        )
        .unwrap();
        std::fs::write(
            dir.path().join("orders.sql"),
            "SELECT order_id, amount FROM upstream",
        )
        .unwrap();
        // A model with no `[columns]` table is omitted from the map.
        std::fs::write(
            dir.path().join("plain.toml"),
            "[target]\ncatalog = \"wh\"\nschema = \"main\"\n",
        )
        .unwrap();
        std::fs::write(dir.path().join("plain.sql"), "SELECT 1 AS x").unwrap();

        let docs = load_column_docs_from_dir(dir.path()).unwrap();
        assert_eq!(docs.len(), 1);
        let orders = docs.get("orders").unwrap();
        assert_eq!(
            orders.get("order_id").map(String::as_str),
            Some("Unique order identifier")
        );
        assert_eq!(
            orders.get("amount").map(String::as_str),
            Some("Order total in USD")
        );
        assert!(!docs.contains_key("plain"));
    }

    #[test]
    fn load_surrogate_keys_reads_sidecar_blocks() {
        use tempfile::tempdir;
        let dir = tempdir().unwrap();
        std::fs::write(
            dir.path().join("keyed.toml"),
            r#"
[target]
catalog = "wh"
schema = "main"

[[surrogate_key]]
name = "order_key"
columns = ["order_id"]
"#,
        )
        .unwrap();
        std::fs::write(
            dir.path().join("keyed.sql"),
            "SELECT order_id FROM upstream",
        )
        .unwrap();
        // A model with no `[[surrogate_key]]` block is omitted from the map.
        std::fs::write(
            dir.path().join("plain.toml"),
            "[target]\ncatalog = \"wh\"\nschema = \"main\"\n",
        )
        .unwrap();
        std::fs::write(dir.path().join("plain.sql"), "SELECT 1 AS x").unwrap();

        let specs = load_surrogate_keys_from_dir(dir.path()).unwrap();
        assert_eq!(specs.len(), 1);
        let keyed = specs.get("keyed").unwrap();
        assert_eq!(keyed.len(), 1);
        assert_eq!(keyed[0].name, "order_key");
        assert_eq!(keyed[0].columns, vec!["order_id".to_string()]);
        assert!(!specs.contains_key("plain"));
    }

    /// A malformed `[[surrogate_key]]` block fails the load with
    /// [`ModelError::InvalidSurrogateKey`] rather than emitting broken SQL.
    /// Covers all three guard arms: an empty column list, a non-identifier
    /// output name, and a non-identifier input column.
    #[test]
    fn load_surrogate_keys_rejects_malformed_config() {
        use tempfile::tempdir;
        let cases = [
            // empty input columns
            ("order_key", "columns = []"),
            // output name with a SQL-injecting character
            ("bad name; DROP", "columns = [\"order_id\"]"),
            // input column with an injecting character
            ("order_key", "columns = [\"order_id); DROP\"]"),
        ];
        for (name, columns_line) in cases {
            let dir = tempdir().unwrap();
            std::fs::write(
                dir.path().join("m.toml"),
                format!(
                    "[target]\ncatalog = \"wh\"\nschema = \"main\"\n\n\
                     [[surrogate_key]]\nname = \"{name}\"\n{columns_line}\n"
                ),
            )
            .unwrap();
            std::fs::write(dir.path().join("m.sql"), "SELECT order_id FROM upstream").unwrap();

            let err = load_surrogate_keys_from_dir(dir.path()).unwrap_err();
            assert!(
                matches!(err, ModelError::InvalidSurrogateKey { .. }),
                "expected InvalidSurrogateKey for name={name:?} {columns_line}, got {err:?}"
            );
        }
    }

    #[test]
    fn load_surrogate_keys_rejects_unknown_field() {
        use tempfile::tempdir;
        let dir = tempdir().unwrap();
        std::fs::write(
            dir.path().join("m.toml"),
            "[target]\ncatalog = \"wh\"\nschema = \"main\"\n\n\
             [[surrogate_key]]\nname = \"order_key\"\ncolumns = [\"order_id\"]\n\
             algoritm = \"sha256\"\n",
        )
        .unwrap();
        std::fs::write(dir.path().join("m.sql"), "SELECT order_id FROM upstream").unwrap();

        // A mistyped key in a [[surrogate_key]] block (deny_unknown_fields) fails
        // the load loudly instead of silently dropping the misspelled setting.
        let err = load_surrogate_keys_from_dir(dir.path()).unwrap_err();
        assert!(
            matches!(err, ModelError::ParseFrontmatter { .. }),
            "expected ParseFrontmatter for unknown [[surrogate_key]] field, got {err:?}"
        );
    }

    // ----- config groups (models/groups/<name>.toml) -----

    /// Write a `groups/<name>.toml` group definition under `dir`.
    fn write_group(dir: &Path, name: &str, body: &str) {
        let groups = dir.join("groups");
        std::fs::create_dir_all(&groups).unwrap();
        std::fs::write(groups.join(format!("{name}.toml")), body).unwrap();
    }

    /// A model opting into a group inherits the group's strategy and a
    /// `schema_template` filled from the model's `[args]`.
    #[test]
    fn group_supplies_strategy_and_routes_schema() {
        let dir = tempfile::tempdir().unwrap();
        write_group(
            dir.path(),
            "daily_marts",
            "schema_template = \"mart_{region}\"\n\n[strategy]\ntype = \"merge\"\nunique_key = [\"id\"]\nupdate_columns = [\"v\"]\n",
        );
        std::fs::write(
            dir.path().join("orders.toml"),
            "group = \"daily_marts\"\n\n[target]\ncatalog = \"wh\"\n\n[args]\nregion = \"emea\"\n",
        )
        .unwrap();
        std::fs::write(dir.path().join("orders.sql"), "SELECT id, v FROM upstream").unwrap();

        let models = load_models_from_dir(dir.path()).unwrap();
        assert_eq!(models.len(), 1);
        let m = &models[0];
        assert_eq!(m.config.target.schema, "mart_emea");
        assert!(matches!(m.config.strategy, StrategyConfig::Merge { .. }));
    }

    /// Per-model sidecar fields win over the group (sidecar > group).
    #[test]
    fn sidecar_overrides_group() {
        let dir = tempfile::tempdir().unwrap();
        write_group(
            dir.path(),
            "daily_marts",
            "schema_template = \"mart_{region}\"\n\n[strategy]\ntype = \"merge\"\nunique_key = [\"id\"]\nupdate_columns = [\"v\"]\n",
        );
        std::fs::write(
            dir.path().join("orders.toml"),
            // Sidecar pins both schema and strategy — the group must not override.
            // No `[args]`: pinning the schema is a legitimate override, but pairing
            // it with `[args]` would trip the misplacement guard (the args could
            // only fill the now-bypassed template).
            "group = \"daily_marts\"\n\n[target]\ncatalog = \"wh\"\nschema = \"custom\"\n\n[strategy]\ntype = \"full_refresh\"\n",
        )
        .unwrap();
        std::fs::write(dir.path().join("orders.sql"), "SELECT id, v FROM upstream").unwrap();

        let m = &load_models_from_dir(dir.path()).unwrap()[0];
        assert_eq!(m.config.target.schema, "custom");
        assert!(matches!(m.config.strategy, StrategyConfig::FullRefresh));
    }

    /// The group wins over `_defaults.toml` (group > directory defaults).
    #[test]
    fn group_overrides_dir_defaults() {
        let dir = tempfile::tempdir().unwrap();
        // Directory default sets full_refresh; the group sets merge.
        std::fs::write(
            dir.path().join("_defaults.toml"),
            "[target]\ncatalog = \"wh\"\nschema = \"fallback\"\n\n[strategy]\ntype = \"full_refresh\"\n",
        )
        .unwrap();
        write_group(
            dir.path(),
            "daily_marts",
            "[strategy]\ntype = \"merge\"\nunique_key = [\"id\"]\nupdate_columns = [\"v\"]\n",
        );
        // Model sets neither strategy nor schema locally — the group's strategy
        // must win over the directory default; schema falls through to defaults.
        std::fs::write(dir.path().join("orders.toml"), "group = \"daily_marts\"\n").unwrap();
        std::fs::write(dir.path().join("orders.sql"), "SELECT id, v FROM upstream").unwrap();

        let m = &load_models_from_dir(dir.path()).unwrap()[0];
        assert!(
            matches!(m.config.strategy, StrategyConfig::Merge { .. }),
            "group strategy must override the directory default"
        );
        assert_eq!(m.config.target.schema, "fallback");
    }

    /// A config group's `[tags]` are inherited by every member as a shared
    /// baseline; the member's own `[tags]` override per key (sidecar > group)
    /// without dropping the rest of the group's tags.
    #[test]
    fn group_tags_inherited_and_model_tags_override() {
        let dir = tempfile::tempdir().unwrap();
        write_group(
            dir.path(),
            "finance",
            "[tags]\ndomain = \"finance\"\ntier = \"gold\"\n",
        );
        std::fs::write(
            dir.path().join("orders.toml"),
            // Overrides `tier`, adds `owner`, leaves `domain` to the group.
            "group = \"finance\"\n\n[target]\ncatalog = \"wh\"\nschema = \"s\"\n\n\
             [tags]\ntier = \"silver\"\nowner = \"data-eng\"\n",
        )
        .unwrap();
        std::fs::write(dir.path().join("orders.sql"), "SELECT id FROM upstream").unwrap();

        let m = &load_models_from_dir(dir.path()).unwrap()[0];
        assert_eq!(
            m.config.tags.get("domain").map(String::as_str),
            Some("finance")
        );
        assert_eq!(
            m.config.tags.get("tier").map(String::as_str),
            Some("silver")
        );
        assert_eq!(
            m.config.tags.get("owner").map(String::as_str),
            Some("data-eng")
        );
    }

    /// A model with its own `[tags]` and no group resolves them verbatim.
    #[test]
    fn model_tags_without_group() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("orders.toml"),
            "[target]\ncatalog = \"wh\"\nschema = \"s\"\n\n[tags]\nowner = \"data-eng\"\n",
        )
        .unwrap();
        std::fs::write(dir.path().join("orders.sql"), "SELECT id FROM upstream").unwrap();

        let m = &load_models_from_dir(dir.path()).unwrap()[0];
        assert_eq!(
            m.config.tags.get("owner").map(String::as_str),
            Some("data-eng")
        );
    }

    /// An unknown `group` reference is a hard error (typo protection).
    #[test]
    fn unknown_group_errors() {
        let dir = tempfile::tempdir().unwrap();
        write_group(
            dir.path(),
            "daily_marts",
            "schema_template = \"mart_{region}\"\n",
        );
        std::fs::write(
            dir.path().join("orders.toml"),
            "group = \"nope\"\n\n[target]\ncatalog = \"wh\"\nschema = \"s\"\n",
        )
        .unwrap();
        std::fs::write(dir.path().join("orders.sql"), "SELECT 1 AS id").unwrap();

        let err = load_models_from_dir(dir.path()).unwrap_err();
        assert!(
            matches!(err, ModelError::UnknownGroup { .. }),
            "got {err:?}"
        );
    }

    /// A `schema_template` placeholder the model doesn't supply fails loudly,
    /// rather than routing to a literal `mart_{region}` schema.
    #[test]
    fn missing_group_template_arg_errors() {
        let dir = tempfile::tempdir().unwrap();
        write_group(
            dir.path(),
            "daily_marts",
            "schema_template = \"mart_{region}\"\n",
        );
        std::fs::write(
            dir.path().join("orders.toml"),
            // No [args] region → the template can't resolve.
            "group = \"daily_marts\"\n\n[target]\ncatalog = \"wh\"\n",
        )
        .unwrap();
        std::fs::write(dir.path().join("orders.sql"), "SELECT 1 AS id").unwrap();

        let err = load_models_from_dir(dir.path()).unwrap_err();
        assert!(
            matches!(err, ModelError::InvalidGroup { .. }),
            "got {err:?}"
        );
    }

    /// An `enforce = true` group rejects a member model that locally overrides
    /// a field the group controls (its strategy, or its routing schema).
    #[test]
    fn enforced_group_rejects_local_override() {
        for (override_block, field) in [
            ("[strategy]\ntype = \"full_refresh\"\n", "strategy"),
            (
                "[target]\ncatalog = \"wh\"\nschema = \"escape\"\n",
                "target.schema",
            ),
        ] {
            let dir = tempfile::tempdir().unwrap();
            write_group(
                dir.path(),
                "locked",
                "enforce = true\nschema_template = \"mart_{region}\"\n\n[strategy]\ntype = \"merge\"\nunique_key = [\"id\"]\nupdate_columns = [\"v\"]\n",
            );
            std::fs::write(
                dir.path().join("orders.toml"),
                format!("group = \"locked\"\n\n[args]\nregion = \"emea\"\n\n{override_block}"),
            )
            .unwrap();
            std::fs::write(dir.path().join("orders.sql"), "SELECT id, v FROM upstream").unwrap();

            let err = load_models_from_dir(dir.path()).unwrap_err();
            assert!(
                matches!(&err, ModelError::GroupOverride { field: f, .. } if f == field),
                "expected GroupOverride for {field}, got {err:?}"
            );
        }
    }

    /// An `enforce = true` group accepts a model that takes everything from the
    /// group (only supplies its `[args]`), and a non-enforced group still allows
    /// local overrides (enforcement is strictly opt-in).
    #[test]
    fn enforcement_is_opt_in_and_allows_conforming_models() {
        // Conforming model under an enforced group: OK.
        let dir = tempfile::tempdir().unwrap();
        write_group(
            dir.path(),
            "locked",
            "enforce = true\nschema_template = \"mart_{region}\"\n\n[strategy]\ntype = \"merge\"\nunique_key = [\"id\"]\nupdate_columns = [\"v\"]\n",
        );
        std::fs::write(
            dir.path().join("orders.toml"),
            "group = \"locked\"\n\n[target]\ncatalog = \"wh\"\n\n[args]\nregion = \"emea\"\n",
        )
        .unwrap();
        std::fs::write(dir.path().join("orders.sql"), "SELECT id, v FROM upstream").unwrap();
        let m = &load_models_from_dir(dir.path()).unwrap()[0];
        assert_eq!(m.config.target.schema, "mart_emea");

        // The same override under a NON-enforced group is allowed (pin a schema
        // with no `[args]` — the template is bypassed, not filled).
        let dir2 = tempfile::tempdir().unwrap();
        write_group(
            dir2.path(),
            "flexible",
            "schema_template = \"mart_{region}\"\n\n[strategy]\ntype = \"merge\"\nunique_key = [\"id\"]\nupdate_columns = [\"v\"]\n",
        );
        std::fs::write(
            dir2.path().join("orders.toml"),
            "group = \"flexible\"\n\n[target]\ncatalog = \"wh\"\nschema = \"escape\"\n",
        )
        .unwrap();
        std::fs::write(dir2.path().join("orders.sql"), "SELECT id, v FROM upstream").unwrap();
        let m2 = &load_models_from_dir(dir2.path()).unwrap()[0];
        assert_eq!(
            m2.config.target.schema, "escape",
            "non-enforced group allows override"
        );
    }

    /// C5 misplacement guard: a group member that supplies `[args]` (which only
    /// fill the group's `schema_template`) AND pins its own `target.schema` is
    /// an incoherent config — the pin bypasses the template, leaving the args
    /// dead. It is a hard error regardless of `enforce`.
    #[test]
    fn group_args_with_pinned_schema_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        // Non-enforced group with a placeholder template.
        write_group(dir.path(), "marts", "schema_template = \"mart_{region}\"\n");
        std::fs::write(
            dir.path().join("orders.toml"),
            // Pins schema AND supplies args → the args can never route anything.
            "group = \"marts\"\n\n[target]\ncatalog = \"wh\"\nschema = \"escape\"\n\n[args]\nregion = \"emea\"\n",
        )
        .unwrap();
        std::fs::write(dir.path().join("orders.sql"), "SELECT 1 AS id").unwrap();

        let err = load_models_from_dir(dir.path()).unwrap_err();
        assert!(
            matches!(err, ModelError::GroupArgsWithPinnedSchema { ref model, ref group }
                if model == "orders" && group == "marts"),
            "got {err:?}"
        );
    }

    /// The allowed companion to the guard: a group member may pin its own schema
    /// as long as it supplies no `[args]` — the placeholder template is bypassed
    /// (and never resolved), so an unfilled `{region}` is not an error.
    #[test]
    fn group_member_pins_schema_without_args_is_allowed() {
        let dir = tempfile::tempdir().unwrap();
        write_group(dir.path(), "marts", "schema_template = \"mart_{region}\"\n");
        std::fs::write(
            dir.path().join("orders.toml"),
            "group = \"marts\"\n\n[target]\ncatalog = \"wh\"\nschema = \"escape\"\n",
        )
        .unwrap();
        std::fs::write(dir.path().join("orders.sql"), "SELECT 1 AS id").unwrap();

        let m = &load_models_from_dir(dir.path()).unwrap()[0];
        assert_eq!(
            m.config.target.schema, "escape",
            "a pin without args overrides the group, bypassing the template"
        );
    }

    /// A group `schema_template` that resolves to a non-identifier (because an
    /// `[args]` value carries an illegal character) is rejected at load with the
    /// same `validate_identifier` `format_table_ref` applies at SQL-gen — so the
    /// failure surfaces early with a clear message instead of as broken SQL.
    #[test]
    fn group_schema_resolving_to_invalid_identifier_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        write_group(dir.path(), "marts", "schema_template = \"mart_{region}\"\n");
        std::fs::write(
            dir.path().join("orders.toml"),
            // `us-west` has a hyphen → resolved schema `mart_us-west` is not a
            // valid SQL identifier (no pinned schema, so the template resolves).
            "group = \"marts\"\n\n[target]\ncatalog = \"wh\"\n\n[args]\nregion = \"us-west\"\n",
        )
        .unwrap();
        std::fs::write(dir.path().join("orders.sql"), "SELECT 1 AS id").unwrap();

        let err = load_models_from_dir(dir.path()).unwrap_err();
        assert!(
            matches!(err, ModelError::InvalidGroup { ref model, .. } if model == "orders"),
            "got {err:?}"
        );
    }

    // ----- named test definitions (test_definitions.toml + [[use_test]]) -----

    /// A `[[use_test]]` reference resolves a named definition onto a column,
    /// the use-site column overriding the definition's default.
    #[test]
    fn named_test_applied_by_reference() {
        use crate::tests::TestType;
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("test_definitions.toml"),
            // `known_status` binds to a default column; `positive` has none.
            "[known_status]\ntype = \"accepted_values\"\ncolumn = \"status\"\nvalues = [\"ok\", \"bad\"]\n\n[positive]\ntype = \"expression\"\nexpression = \"amount > 0\"\n",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("orders.toml"),
            "[target]\ncatalog = \"wh\"\nschema = \"main\"\n\n\
             [[use_test]]\nname = \"known_status\"\n\n\
             [[use_test]]\nname = \"positive\"\nseverity = \"warning\"\n",
        )
        .unwrap();
        std::fs::write(dir.path().join("orders.sql"), "SELECT 1 AS id").unwrap();

        let m = &load_models_from_dir(dir.path()).unwrap()[0];
        assert_eq!(m.config.tests.len(), 2);
        // Definition default column is used when the reference omits one.
        let status = &m.config.tests[0];
        assert!(matches!(status.test_type, TestType::AcceptedValues { .. }));
        assert_eq!(status.column.as_deref(), Some("status"));
        // The reference's severity is applied.
        let positive = &m.config.tests[1];
        assert!(matches!(positive.test_type, TestType::Expression { .. }));
        assert_eq!(positive.severity, crate::tests::TestSeverity::Warning);
    }

    /// A `[[use_test]]` column overrides the definition's default column, and
    /// inline `[[tests]]` coexist with references.
    #[test]
    fn named_test_column_override_and_inline_coexist() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("test_definitions.toml"),
            "[not_empty]\ntype = \"not_null\"\ncolumn = \"id\"\n",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("orders.toml"),
            "[target]\ncatalog = \"wh\"\nschema = \"main\"\n\n\
             [[tests]]\ntype = \"unique\"\ncolumn = \"id\"\n\n\
             [[use_test]]\nname = \"not_empty\"\ncolumn = \"email\"\n",
        )
        .unwrap();
        std::fs::write(dir.path().join("orders.sql"), "SELECT 1 AS id").unwrap();

        let m = &load_models_from_dir(dir.path()).unwrap()[0];
        assert_eq!(m.config.tests.len(), 2, "inline test + resolved reference");
        // The reference's column overrides the definition default ("id").
        assert_eq!(m.config.tests[1].column.as_deref(), Some("email"));
    }

    /// A mistyped key in a `[[use_test]]` block (deny_unknown_fields) is a hard
    /// error, so a typo like `colum =` can't silently drop the column binding.
    #[test]
    fn use_test_rejects_unknown_field() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("test_definitions.toml"),
            "[not_empty]\ntype = \"not_null\"\ncolumn = \"id\"\n",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("orders.toml"),
            "[target]\ncatalog = \"wh\"\nschema = \"main\"\n\n\
             [[use_test]]\nname = \"not_empty\"\ncolum = \"email\"\n",
        )
        .unwrap();
        std::fs::write(dir.path().join("orders.sql"), "SELECT 1 AS id").unwrap();

        let err = load_models_from_dir(dir.path()).unwrap_err();
        assert!(
            matches!(err, ModelError::ParseFrontmatter { .. }),
            "expected ParseFrontmatter for unknown [[use_test]] field, got {err:?}"
        );
    }

    /// An unknown `[[use_test]]` name is a hard error (typo protection).
    #[test]
    fn unknown_named_test_errors() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("test_definitions.toml"),
            "[known]\ntype = \"not_null\"\ncolumn = \"id\"\n",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("orders.toml"),
            "[target]\ncatalog = \"wh\"\nschema = \"main\"\n\n[[use_test]]\nname = \"nope\"\n",
        )
        .unwrap();
        std::fs::write(dir.path().join("orders.sql"), "SELECT 1 AS id").unwrap();

        let err = load_models_from_dir(dir.path()).unwrap_err();
        assert!(matches!(err, ModelError::UnknownTest { .. }), "got {err:?}");
    }

    /// FR-001 Option A: `${VAR}` and `${VAR:-default}` placeholders in a
    /// model sidecar `.toml` resolve at load time, mirroring `rocky.toml`.
    #[test]
    fn test_sidecar_env_var_substitution() {
        let dir = tempfile::TempDir::new().unwrap();

        // SAFETY: test-only, no concurrent reads of these variables.
        unsafe {
            std::env::set_var("ROCKY_TEST_SIDECAR_CATALOG", "warehouse_prod");
            std::env::set_var("ROCKY_TEST_SIDECAR_SCHEMA", "silver");
        }

        std::fs::write(
            dir.path().join("stg_events.toml"),
            r#"
[strategy]
type = "full_refresh"

[target]
catalog = "${ROCKY_TEST_SIDECAR_CATALOG}"
schema  = "${ROCKY_TEST_SIDECAR_SCHEMA:-fallback_schema}"
table   = "${ROCKY_TEST_SIDECAR_TABLE:-stg_events}"
"#,
        )
        .unwrap();
        std::fs::write(dir.path().join("stg_events.sql"), "SELECT 1 AS id").unwrap();

        let model = load_model_pair(
            &dir.path().join("stg_events.sql"),
            &dir.path().join("stg_events.toml"),
            None,
        )
        .unwrap();

        assert_eq!(model.config.target.catalog, "warehouse_prod");
        assert_eq!(model.config.target.schema, "silver");
        // Default branch: ROCKY_TEST_SIDECAR_TABLE not set, fallback wins.
        assert_eq!(model.config.target.table, "stg_events");

        unsafe {
            std::env::remove_var("ROCKY_TEST_SIDECAR_CATALOG");
            std::env::remove_var("ROCKY_TEST_SIDECAR_SCHEMA");
        }
    }

    /// Missing `${VAR}` (no default) at sidecar load time surfaces as a
    /// `ModelError::EnvSubstitution` — same shape `rocky.toml` already has.
    #[test]
    fn test_sidecar_env_var_missing_errors() {
        let dir = tempfile::TempDir::new().unwrap();

        // SAFETY: test-only.
        unsafe { std::env::remove_var("ROCKY_TEST_SIDECAR_NEVER_SET") };

        std::fs::write(
            dir.path().join("bad.toml"),
            r#"
[target]
catalog = "${ROCKY_TEST_SIDECAR_NEVER_SET}"
schema  = "s"
"#,
        )
        .unwrap();
        std::fs::write(dir.path().join("bad.sql"), "SELECT 1").unwrap();

        let result = load_model_pair(
            &dir.path().join("bad.sql"),
            &dir.path().join("bad.toml"),
            None,
        );

        assert!(matches!(result, Err(ModelError::EnvSubstitution { .. })));
    }

    /// `_defaults.toml` also goes through env-var substitution so the same
    /// orchestrator-injected env values apply to directory-level defaults.
    #[test]
    fn test_dir_defaults_env_var_substitution() {
        let dir = tempfile::TempDir::new().unwrap();

        // SAFETY: test-only.
        unsafe {
            std::env::set_var("ROCKY_TEST_DEFAULTS_CATALOG", "from_env_default");
        }

        std::fs::write(
            dir.path().join("_defaults.toml"),
            r#"
[target]
catalog = "${ROCKY_TEST_DEFAULTS_CATALOG}"
schema  = "${ROCKY_TEST_DEFAULTS_SCHEMA:-public}"
"#,
        )
        .unwrap();

        let defaults = load_dir_defaults(&dir.path().join("_defaults.toml")).unwrap();
        let target = defaults.target.expect("target should be set");
        assert_eq!(target.catalog.as_deref(), Some("from_env_default"));
        assert_eq!(target.schema.as_deref(), Some("public"));

        unsafe {
            std::env::remove_var("ROCKY_TEST_DEFAULTS_CATALOG");
        }
    }

    /// Inline `---toml` frontmatter format also resolves env vars before
    /// parsing — mirrors the sidecar path so legacy single-file models
    /// stay in lockstep.
    #[test]
    fn test_inline_frontmatter_env_var_substitution() {
        // SAFETY: test-only.
        unsafe {
            std::env::set_var("ROCKY_TEST_INLINE_CATALOG", "inline_warehouse");
        }

        let content = r#"---toml
name = "inline_model"
[target]
catalog = "${ROCKY_TEST_INLINE_CATALOG}"
schema  = "${ROCKY_TEST_INLINE_SCHEMA:-staging}"
table   = "inline_model"
---

SELECT 1
"#;

        let model = parse_model_inline(content, "inline_model.sql", None).unwrap();
        assert_eq!(model.config.target.catalog, "inline_warehouse");
        assert_eq!(model.config.target.schema, "staging");

        unsafe {
            std::env::remove_var("ROCKY_TEST_INLINE_CATALOG");
        }
    }

    /// `name_declared` and `target_table_declared` must capture the raw
    /// `${VAR:-...}` templates from the sidecar, not the post-substitution
    /// values. This is what lets L001/L002 distinguish an intentional
    /// template that happens to resolve to a "redundant" value from an
    /// actually redundant literal.
    #[test]
    fn test_declared_fields_capture_raw_env_templates() {
        // SAFETY: test-only.
        unsafe {
            std::env::remove_var("ROCKY_TABLE_OVERRIDE");
        }

        let content = r#"---toml
name = "customer_facts"
[target]
catalog = "analytics"
schema = "marts"
table = "${ROCKY_TABLE_OVERRIDE:-customer_facts}"
---

SELECT 1
"#;
        let model = parse_model_inline(content, "customer_facts.sql", None).unwrap();

        // Resolved values: env unset, so the default collapses to the literal.
        assert_eq!(model.config.name, "customer_facts");
        assert_eq!(model.config.target.table, "customer_facts");

        // Declared values: still hold the raw template.
        assert_eq!(model.config.name_declared, "customer_facts");
        assert_eq!(
            model.config.target_table_declared,
            "${ROCKY_TABLE_OVERRIDE:-customer_facts}"
        );
    }

    /// When the sidecar omits `name` and/or `target.table` entirely, the
    /// declared values must fall back to the same chain the resolved
    /// values use — filename stem for `name`, and `name_declared` for
    /// `target.table` — so the lints behave identically on minimal
    /// configs that never touched env substitution.
    #[test]
    fn test_declared_fields_fallback_to_filename_stem() {
        let content = r#"---toml
[target]
catalog = "analytics"
schema = "marts"
---

SELECT 1
"#;
        let model = parse_model_inline(content, "stg_orders.sql", None).unwrap();
        assert_eq!(model.config.name, "stg_orders");
        assert_eq!(model.config.target.table, "stg_orders");
        assert_eq!(model.config.name_declared, "stg_orders");
        assert_eq!(model.config.target_table_declared, "stg_orders");
    }

    // ----- cost_ceiling / CostBudget sidecar tests -----

    #[test]
    fn test_sidecar_budget_populates_cost_ceiling() {
        let content = r#"---toml
[target]
catalog = "analytics"
schema = "marts"

[budget]
max_usd = 5.0
---

SELECT 1
"#;
        let model = parse_model_inline(content, "fct_orders.sql", None).unwrap();
        let ir = model.to_model_ir();
        let ceiling = ir
            .cost_ceiling
            .expect("cost_ceiling must be Some when [budget] max_usd is set");
        assert_eq!(ceiling.max_usd, Some(5.0));
        assert_eq!(ceiling.max_bytes_scanned, None);
    }

    #[test]
    fn test_sidecar_budget_policy_only_yields_none_cost_ceiling() {
        // A sidecar with only `on_breach` set (no cost dimensions) must NOT
        // produce a cost_ceiling — collapsing vacuous budgets keeps recipe-hashes clean.
        let content = r#"---toml
[target]
catalog = "analytics"
schema = "marts"

[budget]
on_breach = "warn"
---

SELECT 1
"#;
        let model = parse_model_inline(content, "fct_orders.sql", None).unwrap();
        let ir = model.to_model_ir();
        assert!(
            ir.cost_ceiling.is_none(),
            "policy-only [budget] must yield cost_ceiling = None"
        );
    }

    #[test]
    fn test_sidecar_no_budget_yields_none_cost_ceiling() {
        let content = r#"---toml
[target]
catalog = "analytics"
schema = "marts"
---

SELECT 1
"#;
        let model = parse_model_inline(content, "fct_orders.sql", None).unwrap();
        let ir = model.to_model_ir();
        assert!(
            ir.cost_ceiling.is_none(),
            "absent [budget] block must yield cost_ceiling = None"
        );
    }
}
