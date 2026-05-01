mod ai;
mod archive;
#[cfg(feature = "duckdb")]
pub mod bench;
mod branch;
#[cfg(feature = "duckdb")]
mod ci;
mod ci_diff;
mod compact;
mod compare;
mod compile;
mod compliance;
mod cost;
mod dag;
mod discover;
mod docs;
mod doctor;
mod estimate;
mod export_schemas;
mod fmt;
pub mod groups;
mod history;
mod hooks;
mod import_dbt;
mod init;
mod init_adapter;
mod lineage;
mod lineage_diff;
mod list;
mod load;
mod lsp;
mod metrics;
mod optimize;
mod plan;
mod playground;
mod preview;
mod profile_storage;
mod replay;
mod retention_status;
mod run;
mod run_audit;
mod run_dag_exec;
mod run_local;
mod seed;
mod serve;
mod shell;
mod snapshot;
mod state;
#[cfg(feature = "duckdb")]
mod test;
mod test_adapter;
mod trace;
mod validate;
mod validate_migration;
mod watch;

pub use ai::{run_ai, run_ai_explain, run_ai_sync, run_ai_test};
pub use archive::{run_archive, run_archive_catalog};
#[cfg(feature = "duckdb")]
pub use bench::run_bench;
pub use branch::{
    run_branch_compare, run_branch_create, run_branch_delete, run_branch_list, run_branch_show,
};
#[cfg(feature = "duckdb")]
pub use ci::run_ci;
pub use ci_diff::run_ci_diff;
pub use compact::{run_compact, run_compact_catalog, run_measure_dedup};
pub use compare::compare;
pub use compile::run_compile;
pub use compliance::run_compliance;
pub use cost::run_cost;
pub use dag::run_dag;
pub use discover::discover;
pub use docs::run_docs;
pub use doctor::doctor;
pub use estimate::run_estimate;
pub use export_schemas::export_schemas;
pub use fmt::run_fmt;
pub use history::run_history;
pub use hooks::{run_hooks_list, run_hooks_test};
pub use import_dbt::run_import_dbt;
pub use init::init;
pub use init_adapter::run_init_adapter;
pub use lineage::run_lineage;
pub use lineage_diff::run_lineage_diff;
pub use list::{
    list_adapters, list_consumers, list_deps, list_models, list_pipelines, list_sources,
};
pub use load::run_load;
pub use lsp::run_lsp;
pub use metrics::run_metrics;
pub use optimize::run_optimize;
pub use plan::plan;
pub use playground::{run_playground, run_playground_with_template};
pub use preview::{run_preview_cost, run_preview_create, run_preview_diff};
pub use profile_storage::run_profile_storage;
pub use replay::run_replay;
pub use retention_status::run_retention_status;
// Re-exported so the `rocky` bin can build a clap ValueEnum for
// `--target-dialect` without taking a direct dep on rocky-sql.
pub use rocky_sql::transpile::Dialect;
pub use run::{Interrupted, PartialFailure, PartitionRunOptions, run};
pub use run_dag_exec::run_with_dag;
pub use seed::run_seed;
pub use serve::run_serve;
pub use shell::run_shell;
pub use snapshot::run_snapshot;
pub use state::{state_clear_schema_cache, state_show};
#[cfg(feature = "duckdb")]
pub use test::run_declarative_tests;
#[cfg(feature = "duckdb")]
pub use test::run_test;
pub use test_adapter::{run_test_adapter, run_test_adapter_builtin};
pub use trace::run_trace;
pub use validate::validate;
pub use validate_migration::run_validate_migration;
pub use watch::run_watch;

use anyhow::Result;
use indexmap::IndexMap;

// --- helpers ---

/// Parses a filter string like "client=acme" into (key, value).
pub(crate) fn parse_filter(filter: &str) -> Result<(String, String)> {
    let parts: Vec<&str> = filter.splitn(2, '=').collect();
    if parts.len() != 2 {
        anyhow::bail!("invalid filter '{filter}': expected key=value (e.g., client=acme)");
    }
    Ok((parts[0].to_string(), parts[1].to_string()))
}

/// Checks if a connector matches a filter.
///
/// The special key `id` matches against the connector's unique identifier.
/// All other keys match against parsed schema components.
pub(crate) fn matches_filter(
    conn: &rocky_core::source::DiscoveredConnector,
    parsed: &rocky_core::schema::ParsedSchema,
    filter_key: &str,
    filter_value: &str,
) -> bool {
    if filter_key == "id" {
        return conn.id == filter_value;
    }
    match parsed.get(filter_key) {
        Some(val) => val == filter_value,
        None => {
            // Check multi-valued components (e.g., regions contains value)
            parsed
                .get_multiple(filter_key)
                .is_some_and(|vals| vals.iter().any(|v| v == filter_value))
        }
    }
}

/// Converts a ParsedSchema into a JSON-compatible components map.
/// Preserves the insertion order from the schema pattern definition.
pub(crate) fn parsed_to_json_map(
    parsed: &rocky_core::schema::ParsedSchema,
) -> IndexMap<String, serde_json::Value> {
    parsed
        .values
        .iter()
        .map(|(k, v)| {
            let json_val = match v {
                rocky_core::schema::SchemaValue::Single(s) => serde_json::Value::String(s.clone()),
                rocky_core::schema::SchemaValue::Multiple(arr) => serde_json::Value::Array(
                    arr.iter()
                        .map(|s| serde_json::Value::String(s.clone()))
                        .collect(),
                ),
            };
            (k.clone(), json_val)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocky_core::schema::{ParsedSchema, SchemaValue};
    use rocky_core::source::DiscoveredConnector;

    #[test]
    fn test_parse_filter_valid() {
        let (key, value) = parse_filter("client=acme").unwrap();
        assert_eq!(key, "client");
        assert_eq!(value, "acme");
    }

    #[test]
    fn test_parse_filter_value_with_equals() {
        let (key, value) = parse_filter("key=val=ue").unwrap();
        assert_eq!(key, "key");
        assert_eq!(value, "val=ue");
    }

    #[test]
    fn test_parse_filter_invalid() {
        assert!(parse_filter("noequalssign").is_err());
    }

    #[test]
    fn test_matches_filter_by_id() {
        let conn = DiscoveredConnector {
            id: "conn_123".into(),
            schema: "src__acme__us_west__shopify".into(),
            source_type: "fivetran".into(),
            last_sync_at: None,
            tables: vec![],
            metadata: Default::default(),
        };
        let parsed = ParsedSchema {
            values: IndexMap::from([("client".into(), SchemaValue::Single("acme".into()))]),
        };
        assert!(matches_filter(&conn, &parsed, "id", "conn_123"));
        assert!(!matches_filter(&conn, &parsed, "id", "other_id"));
    }

    #[test]
    fn test_matches_filter_by_component() {
        let conn = DiscoveredConnector {
            id: "conn_123".into(),
            schema: "src__acme__us_west__shopify".into(),
            source_type: "fivetran".into(),
            last_sync_at: None,
            tables: vec![],
            metadata: Default::default(),
        };
        let parsed = ParsedSchema {
            values: IndexMap::from([
                ("client".into(), SchemaValue::Single("acme".into())),
                ("source".into(), SchemaValue::Single("shopify".into())),
            ]),
        };
        assert!(matches_filter(&conn, &parsed, "client", "acme"));
        assert!(!matches_filter(&conn, &parsed, "client", "globex"));
        assert!(matches_filter(&conn, &parsed, "source", "shopify"));
    }

    #[test]
    fn test_matches_filter_multi_valued() {
        let conn = DiscoveredConnector {
            id: "conn_123".into(),
            schema: "test".into(),
            source_type: "fivetran".into(),
            last_sync_at: None,
            tables: vec![],
            metadata: Default::default(),
        };
        let parsed = ParsedSchema {
            values: IndexMap::from([(
                "regions".into(),
                SchemaValue::Multiple(vec!["us_west".into(), "eu_central".into()]),
            )]),
        };
        assert!(matches_filter(&conn, &parsed, "regions", "us_west"));
        assert!(matches_filter(&conn, &parsed, "regions", "eu_central"));
        assert!(!matches_filter(&conn, &parsed, "regions", "ap_south"));
    }

    #[test]
    fn test_matches_filter_missing_key() {
        let conn = DiscoveredConnector {
            id: "conn_123".into(),
            schema: "test".into(),
            source_type: "fivetran".into(),
            last_sync_at: None,
            tables: vec![],
            metadata: Default::default(),
        };
        let parsed = ParsedSchema {
            values: IndexMap::from([("client".into(), SchemaValue::Single("acme".into()))]),
        };
        assert!(!matches_filter(&conn, &parsed, "nonexistent", "value"));
    }

    #[test]
    fn test_parsed_to_json_map_single() {
        let parsed = ParsedSchema {
            values: IndexMap::from([
                ("client".into(), SchemaValue::Single("acme".into())),
                ("source".into(), SchemaValue::Single("shopify".into())),
            ]),
        };
        let map = parsed_to_json_map(&parsed);
        assert_eq!(map["client"], serde_json::Value::String("acme".into()));
        assert_eq!(map["source"], serde_json::Value::String("shopify".into()));
    }

    #[test]
    fn test_parsed_to_json_map_multi() {
        let parsed = ParsedSchema {
            values: IndexMap::from([(
                "regions".into(),
                SchemaValue::Multiple(vec!["us_west".into(), "eu".into()]),
            )]),
        };
        let map = parsed_to_json_map(&parsed);
        let regions = map["regions"].as_array().unwrap();
        assert_eq!(regions.len(), 2);
        assert_eq!(regions[0], "us_west");
        assert_eq!(regions[1], "eu");
    }
}
