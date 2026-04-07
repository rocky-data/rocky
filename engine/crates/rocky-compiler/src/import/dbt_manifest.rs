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
}

#[derive(Deserialize)]
struct RawColumn {
    #[serde(default)]
    name: String,
    #[serde(default)]
    description: Option<String>,
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

    Ok(DbtManifest {
        metadata,
        nodes,
        sources,
    })
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

    let columns = raw
        .columns
        .into_iter()
        .map(|(k, c)| {
            (
                k,
                DbtManifestColumn {
                    name: c.name,
                    description: c.description,
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
        },
        columns,
        description: raw.description.filter(|d| !d.is_empty()),
        tags: raw.tags,
        schema: raw.schema.unwrap_or_default(),
        database: raw.database.unwrap_or_default(),
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
