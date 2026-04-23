use std::num::NonZeroU32;
use std::path::Path;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::dag::DagNode;
use crate::ir::{
    GovernanceConfig, MaterializationStrategy, SourceRef, TargetRef, TransformationPlan,
};
use crate::lakehouse::{LakehouseFormat, LakehouseOptions};
use crate::tests::TestDecl;

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
}

/// Per-model freshness configuration.
///
/// Declares the maximum allowed lag between successive materializations of
/// the model. The compiler does not validate this — it's a metadata field
/// consumed by downstream observability tools.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ModelFreshnessConfig {
    /// Maximum lag in seconds before the model is considered stale.
    pub max_lag_seconds: u64,
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
}

fn default_batch_size() -> NonZeroU32 {
    NonZeroU32::new(1).expect("1 is non-zero")
}

fn default_microbatch_granularity() -> TimeGrain {
    TimeGrain::Hour
}

/// Partition granularity for `time_interval` materialization.
///
/// The granularity determines:
/// - The canonical partition key format (see [`TimeGrain::format_str`]).
/// - How `@start_date` / `@end_date` placeholders are computed per partition.
/// - What column types are valid (`hour` requires TIMESTAMP; others accept DATE).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TimeGrain {
    Hour,
    Day,
    Month,
    Year,
}

impl TimeGrain {
    /// `chrono` format string for the canonical partition key of this grain.
    pub fn format_str(self) -> &'static str {
        match self {
            TimeGrain::Hour => "%Y-%m-%dT%H",
            TimeGrain::Day => "%Y-%m-%d",
            TimeGrain::Month => "%Y-%m",
            TimeGrain::Year => "%Y",
        }
    }

    /// Format a UTC timestamp as the canonical partition key for this grain.
    pub fn format_key(self, t: chrono::DateTime<chrono::Utc>) -> String {
        t.format(self.format_str()).to_string()
    }

    /// Truncate a UTC timestamp to the start of its containing partition window.
    ///
    /// For `Hour` this drops minutes/seconds; for `Day` this drops the time
    /// portion; for `Month` this snaps to the first of the month; for `Year`
    /// this snaps to January 1st. Used by `--latest` to find "the partition
    /// containing now()".
    pub fn truncate(self, t: chrono::DateTime<chrono::Utc>) -> chrono::DateTime<chrono::Utc> {
        use chrono::{Datelike, TimeZone, Timelike};
        match self {
            TimeGrain::Hour => chrono::Utc
                .with_ymd_and_hms(t.year(), t.month(), t.day(), t.hour(), 0, 0)
                .single()
                .expect("hour truncation is unambiguous"),
            TimeGrain::Day => chrono::Utc
                .with_ymd_and_hms(t.year(), t.month(), t.day(), 0, 0, 0)
                .single()
                .expect("day truncation is unambiguous"),
            TimeGrain::Month => chrono::Utc
                .with_ymd_and_hms(t.year(), t.month(), 1, 0, 0, 0)
                .single()
                .expect("first of month is unambiguous"),
            TimeGrain::Year => chrono::Utc
                .with_ymd_and_hms(t.year(), 1, 1, 0, 0, 0)
                .single()
                .expect("January 1st is unambiguous"),
        }
    }

    /// Advance a UTC timestamp to the start of the next partition window.
    ///
    /// Uses `chrono::Months` for month/year so calendar boundaries (28/29/30/31)
    /// are honored — never `Duration::days(30)`. The input is expected to already
    /// be a window start (i.e., the result of [`TimeGrain::truncate`]); behavior
    /// for non-truncated input is unspecified.
    pub fn next(self, t: chrono::DateTime<chrono::Utc>) -> chrono::DateTime<chrono::Utc> {
        match self {
            TimeGrain::Hour => t + chrono::Duration::hours(1),
            TimeGrain::Day => t + chrono::Duration::days(1),
            TimeGrain::Month => t
                .checked_add_months(chrono::Months::new(1))
                .expect("month overflow is impossible for any plausible date"),
            TimeGrain::Year => t
                .checked_add_months(chrono::Months::new(12))
                .expect("year overflow is impossible for any plausible date"),
        }
    }
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
pub fn load_dir_defaults(path: &Path) -> Result<DirDefaults, ModelError> {
    let content = std::fs::read_to_string(path)?;

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

/// Resolves a [`RawModelConfig`] into a strict [`ModelConfig`] by applying
/// filename inference and directory defaults.
///
/// Precedence: explicit sidecar field > directory default > filename inference.
fn resolve_model_config(
    raw: RawModelConfig,
    file_stem: &str,
    defaults: Option<&DirDefaults>,
) -> Result<ModelConfig, ModelError> {
    let name = raw.name.unwrap_or_else(|| file_stem.to_string());

    let strategy = raw
        .strategy
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

    let schema = raw_target
        .schema
        .or_else(|| dir_target.and_then(|t| t.schema.clone()))
        .ok_or_else(|| ModelError::MissingTarget {
            model: name.clone(),
            field: "schema".into(),
        })?;

    let table = raw_target.table.unwrap_or_else(|| name.clone());

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
        tests: raw.tests,
        format: raw.format,
        format_options: raw.format_options,
        classification: raw.classification,
    })
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

    /// Converts to a TransformationPlan for SQL generation.
    pub fn to_plan(&self) -> TransformationPlan {
        let strategy = match &self.config.strategy {
            StrategyConfig::FullRefresh => MaterializationStrategy::FullRefresh,
            StrategyConfig::Incremental { timestamp_column } => {
                MaterializationStrategy::Incremental {
                    timestamp_column: timestamp_column.clone(),
                    watermark: None,
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
                    Some(cols) => crate::ir::ColumnSelection::Explicit(
                        cols.iter()
                            .map(|s| std::sync::Arc::from(s.as_str()))
                            .collect(),
                    ),
                    None => crate::ir::ColumnSelection::All,
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
        };

        TransformationPlan {
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
            target: TargetRef {
                catalog: self.config.target.catalog.clone(),
                schema: self.config.target.schema.clone(),
                table: self.config.target.table.clone(),
            },
            strategy,
            sql: self.sql.clone(),
            governance: GovernanceConfig {
                permissions_file: None,
                auto_create_catalogs: false,
                auto_create_schemas: false,
            },
            format: self.config.format.clone(),
            format_options: self.config.format_options.clone(),
        }
    }
}

/// Loads a model from a sidecar pair: `name.sql` + `name.toml`.
///
/// With inference, the sidecar can omit `name` (defaults to filename stem),
/// `target.table` (defaults to name), and `target.catalog`/`target.schema`
/// (inherited from `defaults` if provided).
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
    let sql = {
        let s = std::fs::read_to_string(sql_path)?;
        let trimmed = s.trim();
        if trimmed.len() == s.len() {
            s // no trimming needed, reuse allocation
        } else {
            trimmed.to_string()
        }
    };
    let toml_content = std::fs::read_to_string(toml_path)?;
    let raw: RawModelConfig =
        toml::from_str(&toml_content).map_err(|e| ModelError::ParseFrontmatter {
            path: toml_path.display().to_string(),
            source: e,
        })?;

    let file_stem = sql_path
        .file_stem()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_default();

    let config = resolve_model_config(raw, &file_stem, defaults)?;

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
    let (frontmatter, sql) =
        split_frontmatter(content).ok_or_else(|| ModelError::MissingFrontmatter {
            path: file_path.to_string(),
        })?;

    let raw: RawModelConfig =
        toml::from_str(frontmatter).map_err(|e| ModelError::ParseFrontmatter {
            path: file_path.to_string(),
            source: e,
        })?;

    let file_stem = Path::new(file_path)
        .file_stem()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_default();

    let config = resolve_model_config(raw, &file_stem, defaults)?;

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

    // Load optional directory defaults (once, upfront)
    let defaults_path = dir.join("_defaults.toml");
    let defaults = if defaults_path.exists() {
        Some(load_dir_defaults(&defaults_path)?)
    } else {
        None
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
                load_model_pair(path, &toml_path, defaults.as_ref())
            } else {
                let content = std::fs::read_to_string(path)?;
                parse_model_inline(&content, &path.display().to_string(), defaults.as_ref())
            }
        })
        .collect::<Result<Vec<_>, _>>()?;

    models.sort_unstable_by(|a, b| a.config.name.cmp(&b.config.name));
    Ok(models)
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

        let plan = model.to_plan();
        assert_eq!(plan.sources.len(), 2);
        assert!(plan.sql.contains("JOIN"));
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
        let plan = model.to_plan();
        assert!(matches!(
            plan.strategy,
            MaterializationStrategy::Merge { .. }
        ));
        if let MaterializationStrategy::Merge { update_columns, .. } = &plan.strategy {
            assert!(matches!(
                update_columns,
                crate::ir::ColumnSelection::Explicit(_)
            ));
        }
    }

    #[test]
    fn test_split_frontmatter() {
        let content = "---toml\nname = \"test\"\n---\n\nSELECT 1";
        let (fm, sql) = split_frontmatter(content).unwrap();
        assert_eq!(fm, "name = \"test\"");
        assert_eq!(sql, "SELECT 1");
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
        let plan = model.to_plan();
        match plan.strategy {
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
        assert_eq!(model.config.classification.get("email"), Some(&"pii".to_string()));
        assert_eq!(model.config.classification.get("phone"), Some(&"pii".to_string()));
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
}
