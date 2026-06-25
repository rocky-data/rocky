//! dbt manifest.json parser.
//!
//! Parses dbt's compiled manifest to extract fully resolved SQL (no Jinja),
//! dependency information, and model configuration. This is the "fast path"
//! for dbt import: since manifest.json contains `compiled_code` with all
//! Jinja macros, variables, and conditionals already expanded, no regex-based
//! Jinja parsing is needed.

use std::collections::HashMap;
use std::io::BufReader;
use std::path::Path;

use serde::Deserialize;

/// Parsed dbt manifest.json.
#[derive(Debug, Clone)]
pub struct DbtManifest {
    pub metadata: DbtManifestMetadata,
    pub nodes: HashMap<String, DbtManifestNode>,
    pub sources: HashMap<String, DbtManifestSource>,
    /// Unit-test definitions keyed by `unit_test.<project>.<model>.<name>`.
    /// Empty when the manifest predates dbt's unit-test feature.
    pub unit_tests: HashMap<String, DbtManifestUnitTest>,
    /// Counts of resource classes the importer does not translate, captured at
    /// parse time so the sweep can report them.
    pub dropped: DbtDroppedCounts,
}

/// Counts of dbt resource classes the importer skips. Surfaced (not silently
/// ignored) via the import warning sweep.
#[derive(Debug, Clone, Default)]
pub struct DbtDroppedCounts {
    pub snapshots: usize,
    pub metrics: usize,
    pub semantic_models: usize,
    pub exposures: usize,
}

/// Manifest-level metadata.
#[derive(Debug, Clone)]
pub struct DbtManifestMetadata {
    pub dbt_schema_version: String,
    pub dbt_version: String,
    pub generated_at: String,
    pub project_name: String,
}

/// A model/test/seed node in the manifest.
#[derive(Debug, Clone)]
pub struct DbtManifestNode {
    pub unique_id: String,
    pub name: String,
    pub resource_type: String,
    pub compiled_code: Option<String>,
    pub raw_code: String,
    pub depends_on: DbtDependsOn,
    pub config: DbtNodeConfig,
    pub columns: HashMap<String, DbtManifestColumn>,
    pub description: Option<String>,
    pub tags: Vec<String>,
    pub schema: String,
    pub database: String,
}

/// Dependency information for a manifest node.
#[derive(Debug, Clone, Default)]
pub struct DbtDependsOn {
    pub nodes: Vec<String>,
    pub macros: Vec<String>,
}

/// Configuration for a manifest node.
#[derive(Debug, Clone)]
pub struct DbtNodeConfig {
    pub materialized: String,
    pub schema: Option<String>,
    pub unique_key: Option<UniqueKeyValue>,
    pub incremental_strategy: Option<String>,
    /// `event_time` field (dbt 1.8+ microbatch).
    pub event_time: Option<String>,
    /// `batch_size` field (dbt 1.8+ microbatch). One of
    /// `hour` / `day` / `month` / `year`.
    pub batch_size: Option<String>,
    /// `lookback` field (dbt 1.8+ microbatch). Number of prior batches to
    /// recompute on each run.
    pub lookback: Option<u32>,
    /// `partition_by` — list of partition column names. dbt-databricks +
    /// other warehouse-specific configs accept either a single string or a
    /// list; both forms normalize here.
    pub partition_by: Option<Vec<String>>,
    /// `databricks_tags` — key/value tag pairs (dbt-databricks).
    pub databricks_tags: std::collections::BTreeMap<String, String>,
    /// `pre_hook` — SQL statements run before the model materializes.
    /// Accepts dbt's string-or-list shape.
    pub pre_hook: Vec<String>,
    /// `post_hook` — SQL statements run after the model materializes.
    pub post_hook: Vec<String>,
    /// `on_schema_change` — dbt-databricks drift policy. One of `ignore`,
    /// `fail`, `append_new_columns`, `sync_all_columns`.
    pub on_schema_change: Option<String>,
    /// `alias` — overrides the output relation (table) name. When set, the
    /// model materializes to this name instead of the node name.
    pub alias: Option<String>,
    /// `merge_update_columns` — explicit MERGE UPDATE column list (dbt).
    pub merge_update_columns: Option<Vec<String>>,
    /// `merge_exclude_columns` — columns excluded from the MERGE UPDATE (dbt).
    pub merge_exclude_columns: Option<Vec<String>>,
    /// `contract` — dbt model contract enforcement (`{ enforced: true }`).
    /// Rocky does not auto-generate `{model}.contract.toml` on import; the
    /// importer surfaces this so the migration is never silently lossy.
    pub contract: Option<DbtContractConfig>,
}

/// dbt model `contract` config — whether the model enforces a declared
/// column-level contract (`config: { contract: { enforced: true } }`).
#[derive(Debug, Clone, Default)]
pub struct DbtContractConfig {
    /// `enforced` — when true, dbt validates the model output against the
    /// declared column types/constraints at build time.
    pub enforced: bool,
}

/// unique_key can be a single string or a list.
#[derive(Debug, Clone)]
pub enum UniqueKeyValue {
    Single(String),
    Multiple(Vec<String>),
}

/// A column definition in the manifest.
#[derive(Debug, Clone)]
pub struct DbtManifestColumn {
    pub name: String,
    pub description: Option<String>,
    /// `data_type` — declared physical type (part of a dbt model contract).
    /// Dropped on import (Rocky infers types from the model SQL); surfaced
    /// alongside the contract warning so the loss is visible.
    pub data_type: Option<String>,
    /// `constraints` — declared column constraints (`not_null`, `primary_key`,
    /// `check`, …) from a dbt model contract. Dropped on import; the count is
    /// surfaced in the contract warning.
    pub constraints: Vec<String>,
}

/// A unit-test definition extracted from `manifest.unit_tests`.
///
/// Maps 1:1 to dbt's unit-test schema (v12+): a name, a target model, a
/// list of `given` input fixtures, a single `expect` expectation, and
/// optional description/tags. `overrides` is intentionally not captured —
/// it's rare in practice and the importer skips it with a warning.
#[derive(Debug, Clone)]
pub struct DbtManifestUnitTest {
    pub unique_id: String,
    pub name: String,
    /// Bare model name the unit test asserts on (no `ref()` wrapper).
    pub model: String,
    pub given: Vec<DbtUnitTestGiven>,
    pub expect: DbtUnitTestExpect,
    pub description: Option<String>,
    pub tags: Vec<String>,
}

/// One input fixture for a unit test. `input` is the dbt-side reference
/// (typically `ref('upstream')` or `source('a','b')`); the converter
/// strips the wrapper down to a bare ref before emitting.
#[derive(Debug, Clone)]
pub struct DbtUnitTestGiven {
    pub input: String,
    pub rows: Vec<serde_json::Value>,
    /// dbt fixture format. `None` or `Some("dict")` means inline rows;
    /// `csv` / `sql` are recognised but unsupported by the importer
    /// (skipped with a warning).
    pub format: Option<String>,
}

/// Expectation block for a unit test. `format` follows the same rule as
/// `DbtUnitTestGiven::format`.
#[derive(Debug, Clone)]
pub struct DbtUnitTestExpect {
    pub rows: Vec<serde_json::Value>,
    pub format: Option<String>,
}

/// A source definition in the manifest.
#[derive(Debug, Clone)]
pub struct DbtManifestSource {
    pub unique_id: String,
    pub name: String,
    pub source_name: String,
    pub database: String,
    pub schema: String,
}

// --- Raw deserialization types ---

#[derive(Deserialize)]
struct RawManifest {
    #[serde(default)]
    metadata: RawMetadata,
    #[serde(default)]
    nodes: HashMap<String, RawNode>,
    #[serde(default)]
    sources: HashMap<String, RawManifestSource>,
    #[serde(default)]
    unit_tests: HashMap<String, RawUnitTest>,
    // Count-only: resource classes the importer does not translate. Captured
    // so the sweep can report them rather than silently ignoring them.
    #[serde(default)]
    metrics: HashMap<String, serde_json::Value>,
    #[serde(default)]
    semantic_models: HashMap<String, serde_json::Value>,
    #[serde(default)]
    exposures: HashMap<String, serde_json::Value>,
}

#[derive(Deserialize, Default)]
struct RawUnitTest {
    #[serde(default)]
    unique_id: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    model: String,
    #[serde(default)]
    given: Vec<RawUnitTestGiven>,
    #[serde(default)]
    expect: Option<RawUnitTestExpect>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    tags: Vec<String>,
}

#[derive(Deserialize, Default)]
struct RawUnitTestGiven {
    #[serde(default)]
    input: String,
    #[serde(default)]
    rows: serde_json::Value,
    #[serde(default)]
    format: Option<String>,
}

#[derive(Deserialize, Default)]
struct RawUnitTestExpect {
    #[serde(default)]
    rows: serde_json::Value,
    #[serde(default)]
    format: Option<String>,
}

#[derive(Deserialize, Default)]
struct RawMetadata {
    #[serde(default)]
    dbt_schema_version: Option<String>,
    #[serde(default)]
    dbt_version: Option<String>,
    #[serde(default)]
    generated_at: Option<String>,
    #[serde(default)]
    project_name: Option<String>,
}

#[derive(Deserialize)]
struct RawNode {
    #[serde(default)]
    unique_id: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    resource_type: String,
    #[serde(default)]
    compiled_code: Option<String>,
    #[serde(default)]
    raw_code: Option<String>,
    #[serde(default)]
    depends_on: Option<RawDependsOn>,
    #[serde(default)]
    config: Option<RawNodeConfig>,
    #[serde(default)]
    columns: HashMap<String, RawColumn>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default)]
    schema: Option<String>,
    #[serde(default)]
    database: Option<String>,
}

#[derive(Deserialize, Default)]
struct RawDependsOn {
    #[serde(default)]
    nodes: Vec<String>,
    #[serde(default)]
    macros: Vec<String>,
}

#[derive(Deserialize, Default)]
struct RawNodeConfig {
    #[serde(default)]
    materialized: Option<String>,
    #[serde(default)]
    schema: Option<String>,
    #[serde(default)]
    unique_key: Option<serde_json::Value>,
    #[serde(default)]
    incremental_strategy: Option<String>,
    #[serde(default)]
    event_time: Option<String>,
    #[serde(default)]
    batch_size: Option<String>,
    #[serde(default)]
    lookback: Option<u32>,
    #[serde(default)]
    partition_by: Option<serde_json::Value>,
    #[serde(default)]
    databricks_tags: Option<std::collections::BTreeMap<String, String>>,
    #[serde(default)]
    pre_hook: Option<serde_json::Value>,
    #[serde(default, rename = "pre-hook")]
    pre_hook_dashed: Option<serde_json::Value>,
    #[serde(default)]
    post_hook: Option<serde_json::Value>,
    #[serde(default, rename = "post-hook")]
    post_hook_dashed: Option<serde_json::Value>,
    #[serde(default)]
    on_schema_change: Option<String>,
    /// `alias` — overrides the relation (table) name. dbt routes the model's
    /// output to this name instead of the file/node name; dropping it silently
    /// lands the data in the wrong table.
    #[serde(default)]
    alias: Option<String>,
    /// `merge_update_columns` — the explicit columns to UPDATE on a MERGE
    /// match. Dropping it silently turns a column-scoped merge into update-all.
    #[serde(default)]
    merge_update_columns: Option<Vec<String>>,
    /// `merge_exclude_columns` — columns excluded from the MERGE UPDATE.
    #[serde(default)]
    merge_exclude_columns: Option<Vec<String>>,
    /// `contract` — dbt model contract enforcement block.
    #[serde(default)]
    contract: Option<RawContract>,
}

#[derive(Deserialize, Default)]
struct RawContract {
    #[serde(default)]
    enforced: bool,
}

#[derive(Deserialize)]
struct RawColumn {
    #[serde(default)]
    name: String,
    #[serde(default)]
    description: Option<String>,
    /// `data_type` — declared physical type (model contract).
    #[serde(default)]
    data_type: Option<String>,
    /// `constraints` — declared column constraints (model contract). dbt emits
    /// these as objects (`{ type: "not_null", ... }`); we keep just the
    /// constraint `type` for the dropped-contract warning.
    #[serde(default)]
    constraints: Vec<RawConstraint>,
}

#[derive(Deserialize)]
struct RawConstraint {
    #[serde(default, rename = "type")]
    constraint_type: Option<String>,
}

#[derive(Deserialize)]
struct RawManifestSource {
    #[serde(default)]
    unique_id: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    source_name: String,
    #[serde(default)]
    database: Option<String>,
    #[serde(default)]
    schema: Option<String>,
}

/// Parse a manifest.json file.
///
/// Uses a buffered reader for efficiency with large manifests. Only model
/// nodes are retained; tests, seeds, and snapshots are filtered out.
pub fn parse_manifest(path: &Path) -> Result<DbtManifest, String> {
    let file =
        std::fs::File::open(path).map_err(|e| format!("failed to open {}: {e}", path.display()))?;

    let reader = BufReader::new(file);
    let raw: RawManifest = serde_json::from_reader(reader)
        .map_err(|e| format!("failed to parse {}: {e}", path.display()))?;

    let metadata = DbtManifestMetadata {
        dbt_schema_version: raw.metadata.dbt_schema_version.unwrap_or_default(),
        dbt_version: raw.metadata.dbt_version.unwrap_or_default(),
        generated_at: raw.metadata.generated_at.unwrap_or_default(),
        project_name: raw.metadata.project_name.unwrap_or_default(),
    };

    let dropped = DbtDroppedCounts {
        snapshots: raw
            .nodes
            .values()
            .filter(|n| n.resource_type == "snapshot")
            .count(),
        metrics: raw.metrics.len(),
        semantic_models: raw.semantic_models.len(),
        exposures: raw.exposures.len(),
    };

    let nodes = raw
        .nodes
        .into_iter()
        .filter(|(_, n)| n.resource_type == "model")
        .map(|(id, n)| {
            let node = convert_node(n);
            (id, node)
        })
        .collect();

    let sources = raw
        .sources
        .into_iter()
        .map(|(id, s)| {
            let source = DbtManifestSource {
                unique_id: s.unique_id,
                name: s.name,
                source_name: s.source_name,
                database: s.database.unwrap_or_default(),
                schema: s.schema.unwrap_or_default(),
            };
            (id, source)
        })
        .collect();

    let unit_tests = raw
        .unit_tests
        .into_iter()
        .map(|(id, ut)| (id, convert_unit_test(ut)))
        .collect();

    Ok(DbtManifest {
        metadata,
        nodes,
        sources,
        unit_tests,
        dropped,
    })
}

fn convert_unit_test(raw: RawUnitTest) -> DbtManifestUnitTest {
    let given = raw.given.into_iter().map(convert_given).collect();
    let expect = raw.expect.map(convert_expect).unwrap_or(DbtUnitTestExpect {
        rows: Vec::new(),
        format: None,
    });
    DbtManifestUnitTest {
        unique_id: raw.unique_id,
        name: raw.name,
        model: raw.model,
        given,
        expect,
        description: raw.description.filter(|d| !d.is_empty()),
        tags: raw.tags,
    }
}

fn convert_given(raw: RawUnitTestGiven) -> DbtUnitTestGiven {
    DbtUnitTestGiven {
        input: raw.input,
        rows: rows_from_json(raw.rows),
        format: raw.format.filter(|s| !s.is_empty()),
    }
}

fn convert_expect(raw: RawUnitTestExpect) -> DbtUnitTestExpect {
    DbtUnitTestExpect {
        rows: rows_from_json(raw.rows),
        format: raw.format.filter(|s| !s.is_empty()),
    }
}

/// Normalize a `rows` field. dbt emits this as an array of objects when
/// `format = "dict"`; for `csv`/`sql` fixtures the field can be a string
/// or absent. We retain the JSON array form and let downstream stages
/// decide what to do with non-array shapes.
fn rows_from_json(v: serde_json::Value) -> Vec<serde_json::Value> {
    match v {
        serde_json::Value::Array(arr) => arr,
        serde_json::Value::Null => Vec::new(),
        other => vec![other],
    }
}

fn convert_node(raw: RawNode) -> DbtManifestNode {
    let depends_on = raw.depends_on.unwrap_or_default();
    let config = raw.config.unwrap_or_default();

    let unique_key = config.unique_key.and_then(|v| match v {
        serde_json::Value::String(s) => Some(UniqueKeyValue::Single(s)),
        serde_json::Value::Array(arr) => {
            let keys: Vec<String> = arr
                .into_iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect();
            if keys.is_empty() {
                None
            } else {
                Some(UniqueKeyValue::Multiple(keys))
            }
        }
        _ => None,
    });

    let partition_by = config.partition_by.and_then(json_string_or_list);

    let pre_hook = json_string_or_list(
        config
            .pre_hook
            .or(config.pre_hook_dashed)
            .unwrap_or(serde_json::Value::Null),
    )
    .unwrap_or_default();
    let post_hook = json_string_or_list(
        config
            .post_hook
            .or(config.post_hook_dashed)
            .unwrap_or(serde_json::Value::Null),
    )
    .unwrap_or_default();

    let columns = raw
        .columns
        .into_iter()
        .map(|(k, c)| {
            let constraints = c
                .constraints
                .into_iter()
                .filter_map(|x| x.constraint_type)
                .collect();
            (
                k,
                DbtManifestColumn {
                    name: c.name,
                    description: c.description,
                    data_type: c.data_type,
                    constraints,
                },
            )
        })
        .collect();

    DbtManifestNode {
        unique_id: raw.unique_id,
        name: raw.name,
        resource_type: raw.resource_type,
        compiled_code: raw.compiled_code,
        raw_code: raw.raw_code.unwrap_or_default(),
        depends_on: DbtDependsOn {
            nodes: depends_on.nodes,
            macros: depends_on.macros,
        },
        config: DbtNodeConfig {
            materialized: config.materialized.unwrap_or_else(|| "view".to_string()),
            schema: config.schema,
            unique_key,
            incremental_strategy: config.incremental_strategy,
            event_time: config.event_time,
            batch_size: config.batch_size,
            lookback: config.lookback,
            partition_by,
            databricks_tags: config.databricks_tags.unwrap_or_default(),
            pre_hook,
            post_hook,
            on_schema_change: config.on_schema_change,
            alias: config.alias,
            merge_update_columns: config.merge_update_columns,
            merge_exclude_columns: config.merge_exclude_columns,
            contract: config.contract.map(|c| DbtContractConfig {
                enforced: c.enforced,
            }),
        },
        columns,
        description: raw.description.filter(|d| !d.is_empty()),
        tags: raw.tags,
        schema: raw.schema.unwrap_or_default(),
        database: raw.database.unwrap_or_default(),
    }
}

/// Normalize a manifest field that dbt accepts as either a single string
/// or a list of strings into `Option<Vec<String>>`. Returns `None` for
/// null, empty list, or non-string values.
fn json_string_or_list(v: serde_json::Value) -> Option<Vec<String>> {
    match v {
        serde_json::Value::String(s) if !s.is_empty() => Some(vec![s]),
        serde_json::Value::Array(arr) => {
            let items: Vec<String> = arr
                .into_iter()
                .filter_map(|v| match v {
                    serde_json::Value::String(s) if !s.is_empty() => Some(s),
                    _ => None,
                })
                .collect();
            if items.is_empty() { None } else { Some(items) }
        }
        _ => None,
    }
}

/// Extract model name from a dbt unique_id.
///
/// `"model.my_project.stg_orders"` -> `"stg_orders"`
pub fn extract_model_name(unique_id: &str) -> &str {
    unique_id.rsplit('.').next().unwrap_or(unique_id)
}

/// Convert manifest `depends_on.nodes` to Rocky model names.
///
/// Filters to only model dependencies (not sources or tests).
pub fn depends_on_to_rocky(nodes: &[String]) -> Vec<String> {
    nodes
        .iter()
        .filter(|n| n.starts_with("model."))
        .map(|n| extract_model_name(n).to_string())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_manifest_json() -> String {
        serde_json::json!({
            "metadata": {
                "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12/manifest.json",
                "dbt_version": "1.7.4",
                "generated_at": "2024-01-15T10:30:00Z",
                "project_name": "my_analytics"
            },
            "nodes": {
                "model.my_analytics.stg_orders": {
                    "unique_id": "model.my_analytics.stg_orders",
                    "name": "stg_orders",
                    "resource_type": "model",
                    "compiled_code": "SELECT id, amount, created_at FROM raw_db.raw_schema.orders",
                    "raw_code": "SELECT id, amount, created_at FROM {{ source('raw', 'orders') }}",
                    "depends_on": {
                        "nodes": ["source.my_analytics.raw.orders"],
                        "macros": []
                    },
                    "config": {
                        "materialized": "view",
                        "schema": "staging"
                    },
                    "columns": {
                        "id": { "name": "id", "description": "Order ID" },
                        "amount": { "name": "amount", "description": "Order amount" }
                    },
                    "description": "Staged orders from raw source",
                    "tags": ["staging"],
                    "schema": "staging",
                    "database": "analytics"
                },
                "model.my_analytics.fct_revenue": {
                    "unique_id": "model.my_analytics.fct_revenue",
                    "name": "fct_revenue",
                    "resource_type": "model",
                    "compiled_code": "SELECT o.id, o.amount FROM analytics.staging.stg_orders o WHERE o.amount > 0",
                    "raw_code": "SELECT o.id, o.amount FROM {{ ref('stg_orders') }} o WHERE o.amount > 0",
                    "depends_on": {
                        "nodes": ["model.my_analytics.stg_orders"],
                        "macros": []
                    },
                    "config": {
                        "materialized": "incremental",
                        "unique_key": "id",
                        "incremental_strategy": "merge"
                    },
                    "columns": {},
                    "description": "Revenue fact table",
                    "tags": ["marts", "finance"],
                    "schema": "marts",
                    "database": "analytics"
                },
                "test.my_analytics.not_null_orders_id.abc123": {
                    "unique_id": "test.my_analytics.not_null_orders_id.abc123",
                    "name": "not_null_orders_id",
                    "resource_type": "test",
                    "compiled_code": "SELECT count(*) FROM ...",
                    "raw_code": "...",
                    "depends_on": { "nodes": [], "macros": [] },
                    "config": { "materialized": "test" },
                    "columns": {},
                    "tags": [],
                    "schema": "",
                    "database": ""
                }
            },
            "sources": {
                "source.my_analytics.raw.orders": {
                    "unique_id": "source.my_analytics.raw.orders",
                    "name": "orders",
                    "source_name": "raw",
                    "database": "raw_db",
                    "schema": "raw_schema"
                }
            }
        })
        .to_string()
    }

    #[test]
    fn test_parse_manifest_metadata() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("manifest.json");
        std::fs::write(&path, sample_manifest_json()).unwrap();

        let manifest = parse_manifest(&path).unwrap();
        assert_eq!(manifest.metadata.dbt_version, "1.7.4");
        assert_eq!(manifest.metadata.project_name, "my_analytics");
        assert_eq!(manifest.metadata.generated_at, "2024-01-15T10:30:00Z");
    }

    #[test]
    fn test_parse_manifest_filters_non_models() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("manifest.json");
        std::fs::write(&path, sample_manifest_json()).unwrap();

        let manifest = parse_manifest(&path).unwrap();
        // Only model nodes, not tests
        assert_eq!(manifest.nodes.len(), 2);
        assert!(manifest.nodes.contains_key("model.my_analytics.stg_orders"));
        assert!(
            manifest
                .nodes
                .contains_key("model.my_analytics.fct_revenue")
        );
    }

    #[test]
    fn test_parse_manifest_compiled_code() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("manifest.json");
        std::fs::write(&path, sample_manifest_json()).unwrap();

        let manifest = parse_manifest(&path).unwrap();
        let stg = &manifest.nodes["model.my_analytics.stg_orders"];
        assert_eq!(
            stg.compiled_code.as_deref(),
            Some("SELECT id, amount, created_at FROM raw_db.raw_schema.orders")
        );
        // raw_code still has Jinja
        assert!(stg.raw_code.contains("{{ source("));
    }

    #[test]
    fn test_parse_manifest_node_without_compiled_code() {
        let json = serde_json::json!({
            "metadata": {},
            "nodes": {
                "model.proj.no_compile": {
                    "unique_id": "model.proj.no_compile",
                    "name": "no_compile",
                    "resource_type": "model",
                    "raw_code": "SELECT 1",
                    "depends_on": { "nodes": [], "macros": [] },
                    "config": { "materialized": "table" },
                    "columns": {},
                    "tags": [],
                    "schema": "public",
                    "database": "db"
                }
            },
            "sources": {}
        })
        .to_string();

        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("manifest.json");
        std::fs::write(&path, json).unwrap();

        let manifest = parse_manifest(&path).unwrap();
        let node = &manifest.nodes["model.proj.no_compile"];
        assert!(node.compiled_code.is_none());
        assert_eq!(node.raw_code, "SELECT 1");
    }

    #[test]
    fn test_parse_manifest_sources() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("manifest.json");
        std::fs::write(&path, sample_manifest_json()).unwrap();

        let manifest = parse_manifest(&path).unwrap();
        assert_eq!(manifest.sources.len(), 1);
        let src = &manifest.sources["source.my_analytics.raw.orders"];
        assert_eq!(src.name, "orders");
        assert_eq!(src.source_name, "raw");
        assert_eq!(src.database, "raw_db");
        assert_eq!(src.schema, "raw_schema");
    }

    #[test]
    fn test_extract_model_name() {
        assert_eq!(
            extract_model_name("model.my_project.stg_orders"),
            "stg_orders"
        );
        assert_eq!(extract_model_name("stg_orders"), "stg_orders");
    }

    #[test]
    fn test_depends_on_to_rocky() {
        let nodes = vec![
            "model.proj.stg_orders".to_string(),
            "source.proj.raw.orders".to_string(),
            "model.proj.dim_customers".to_string(),
        ];
        let deps = depends_on_to_rocky(&nodes);
        assert_eq!(deps, vec!["stg_orders", "dim_customers"]);
    }

    #[test]
    fn test_node_config_unique_key_string() {
        let json = serde_json::json!({
            "metadata": {},
            "nodes": {
                "model.p.m": {
                    "unique_id": "model.p.m",
                    "name": "m",
                    "resource_type": "model",
                    "raw_code": "SELECT 1",
                    "config": {
                        "materialized": "incremental",
                        "unique_key": "id"
                    },
                    "columns": {},
                    "tags": [],
                    "schema": "s",
                    "database": "d"
                }
            },
            "sources": {}
        })
        .to_string();

        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("manifest.json");
        std::fs::write(&path, json).unwrap();

        let manifest = parse_manifest(&path).unwrap();
        let node = &manifest.nodes["model.p.m"];
        assert!(matches!(
            node.config.unique_key,
            Some(UniqueKeyValue::Single(ref s)) if s == "id"
        ));
    }

    #[test]
    fn test_node_config_unique_key_array() {
        let json = serde_json::json!({
            "metadata": {},
            "nodes": {
                "model.p.m": {
                    "unique_id": "model.p.m",
                    "name": "m",
                    "resource_type": "model",
                    "raw_code": "SELECT 1",
                    "config": {
                        "materialized": "incremental",
                        "unique_key": ["id", "date"]
                    },
                    "columns": {},
                    "tags": [],
                    "schema": "s",
                    "database": "d"
                }
            },
            "sources": {}
        })
        .to_string();

        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("manifest.json");
        std::fs::write(&path, json).unwrap();

        let manifest = parse_manifest(&path).unwrap();
        let node = &manifest.nodes["model.p.m"];
        assert!(matches!(
            node.config.unique_key,
            Some(UniqueKeyValue::Multiple(ref keys)) if keys == &["id", "date"]
        ));
    }

    #[test]
    fn test_parse_unit_tests_basic() {
        let json = serde_json::json!({
            "metadata": { "project_name": "p" },
            "nodes": {},
            "sources": {},
            "unit_tests": {
                "unit_test.p.stg_orders.stamps_order_key": {
                    "unique_id": "unit_test.p.stg_orders.stamps_order_key",
                    "name": "stamps_order_key",
                    "model": "stg_orders",
                    "given": [
                        {
                            "input": "ref('int_orders')",
                            "rows": [{ "order_id": 1001, "customer_id": 50 }]
                        }
                    ],
                    "expect": {
                        "rows": [{ "order_key": "abc", "order_id": 1001 }],
                        "format": "dict"
                    },
                    "description": "order key stamped via md5",
                    "tags": ["mart"]
                }
            }
        })
        .to_string();

        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("manifest.json");
        std::fs::write(&path, json).unwrap();

        let manifest = parse_manifest(&path).unwrap();
        assert_eq!(manifest.unit_tests.len(), 1);
        let ut = &manifest.unit_tests["unit_test.p.stg_orders.stamps_order_key"];
        assert_eq!(ut.name, "stamps_order_key");
        assert_eq!(ut.model, "stg_orders");
        assert_eq!(ut.given.len(), 1);
        assert_eq!(ut.given[0].input, "ref('int_orders')");
        assert_eq!(ut.given[0].rows.len(), 1);
        assert_eq!(ut.expect.rows.len(), 1);
        assert_eq!(ut.expect.format.as_deref(), Some("dict"));
        assert_eq!(ut.description.as_deref(), Some("order key stamped via md5"));
        assert_eq!(ut.tags, vec!["mart"]);
    }

    #[test]
    fn test_parse_unit_tests_absent_field_is_empty() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("manifest.json");
        std::fs::write(&path, sample_manifest_json()).unwrap();

        let manifest = parse_manifest(&path).unwrap();
        // Sample manifest predates the unit_tests collection.
        assert!(manifest.unit_tests.is_empty());
    }

    #[test]
    fn test_parse_unit_tests_csv_format_round_trips() {
        let json = serde_json::json!({
            "metadata": { "project_name": "p" },
            "nodes": {},
            "sources": {},
            "unit_tests": {
                "unit_test.p.m.csv_case": {
                    "unique_id": "unit_test.p.m.csv_case",
                    "name": "csv_case",
                    "model": "m",
                    "given": [
                        { "input": "ref('u')", "rows": "id,amount\n1,10\n", "format": "csv" }
                    ],
                    "expect": { "rows": "id\n1\n", "format": "csv" },
                    "tags": []
                }
            }
        })
        .to_string();

        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("manifest.json");
        std::fs::write(&path, json).unwrap();

        let manifest = parse_manifest(&path).unwrap();
        let ut = &manifest.unit_tests["unit_test.p.m.csv_case"];
        // CSV format is round-tripped as `format` metadata; downstream
        // stages decide whether to skip it.
        assert_eq!(ut.expect.format.as_deref(), Some("csv"));
        assert_eq!(ut.given[0].format.as_deref(), Some("csv"));
    }

    #[test]
    fn test_node_description_populated() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("manifest.json");
        std::fs::write(&path, sample_manifest_json()).unwrap();

        let manifest = parse_manifest(&path).unwrap();
        let stg = &manifest.nodes["model.my_analytics.stg_orders"];
        assert_eq!(
            stg.description.as_deref(),
            Some("Staged orders from raw source")
        );
    }
}
