mod adapter;
mod ai;
mod ai_contract;
pub mod apply;
mod archive;
mod audit;
mod backfill;
#[cfg(feature = "duckdb")]
pub mod bench;
mod branch;
mod brief;
mod catalog;
#[cfg(feature = "duckdb")]
mod ci;
mod ci_diff;
mod column_skip;
mod compact;
mod compare;
mod compile;
mod completions;
mod compliance;
mod containment;
mod cost;
mod dag;
mod discover;
mod docs;
mod doctor;
mod drift_governance;
mod emit_sql;
mod estimate;
mod export_openapi;
mod export_schemas;
mod fmt;
mod freeze_fence;
mod gc;
pub mod groups;
mod history;
mod hooks;
mod import_dbt;
mod imports_check;
mod imports_update;
mod init;
mod init_adapter;
mod lineage;
mod lineage_diff;
mod list;
mod load;
mod lsp;
mod metrics;
mod optimize;
pub mod plan;
mod playground;
mod policy;
mod preview;
mod preview_rows;
mod profile;
mod profile_storage;
mod publish_ir;
mod replay;
mod resilience;
mod restore;
mod retention_status;
mod reuse_decision;
mod review;
mod run;
mod run_audit;
mod run_content_addressed;
mod run_dag_exec;
mod run_local;
mod run_watch;
pub mod scheduler;
mod seed;
mod serve;
mod shell;
mod skip_gate;
mod snapshot;
mod state;
#[cfg(feature = "duckdb")]
mod test;
mod test_adapter;
mod tick;
mod trace;
mod validate;
mod validate_migration;
mod watch;

pub use adapter::{
    discover_adapters_on_path, resolve_adapter_command, run_adapter_info, run_adapter_list,
};
pub use ai::{
    SchemaBuckets, build_schema_context, run_ai, run_ai_explain, run_ai_sync, run_ai_test,
};
pub use ai_contract::run_ai_contract;
pub use apply::{
    PolicyGate, evaluate_apply_policy, evaluate_apply_policy_with_policy,
    marker_freezes_before_gate, run_apply, run_apply_inline_for_run,
};
pub use archive::{run_archive, run_archive_apply, run_archive_catalog};
pub use audit::{
    compute_audit_for, compute_audit_scorecard, run_audit, run_audit_for, run_audit_scorecard,
};
pub use backfill::run_backfill;
#[cfg(feature = "duckdb")]
pub use bench::run_bench;
pub use branch::{
    run_branch_approve, run_branch_compare, run_branch_create, run_branch_delete, run_branch_list,
    run_branch_promote, run_branch_promote_from_plan, run_branch_show,
};
pub use brief::{BriefSince, compute_brief, run_brief};
pub use catalog::{
    CatalogFormat, compute_catalog_output, default_out_dir as catalog_default_out_dir, run_catalog,
};
#[cfg(feature = "duckdb")]
pub use ci::run_ci;
pub use ci_diff::{extract_base_compile, project_ir_from_compile, run_ci_diff};
pub use compact::{run_compact, run_compact_apply, run_compact_catalog, run_measure_dedup};
pub use compare::compare;
pub use compile::{compile_output, run_compile};
pub use completions::run_completions;
pub use compliance::run_compliance;
pub use cost::run_cost;
pub use dag::{dag_output, run_dag};
pub use discover::discover;
pub use docs::run_docs;
pub use doctor::doctor;
pub use emit_sql::run_emit_sql;
pub use estimate::run_estimate;
pub use export_openapi::export_openapi;
pub use export_schemas::{export_schemas, schemas_hash};
pub use fmt::run_fmt;
pub use gc::{run_gc_derivable, run_gc_plan};
pub use history::{history_runs_output, model_history_output, recipe_history_output, run_history};
pub use hooks::{run_hooks_list, run_hooks_test};
pub use import_dbt::run_import_dbt;
pub use imports_update::run_imports_update;
pub use init::init;
pub use init_adapter::run_init_adapter;
pub use lineage::{column_lineage_output, lineage_output, run_lineage};
pub use lineage_diff::run_lineage_diff;
pub use list::{
    list_adapters, list_adapters_output, list_consumers, list_deps, list_models,
    list_models_output, list_pipelines, list_pipelines_output, list_sources, list_sources_output,
};
pub use load::run_load;
pub use lsp::run_lsp;
pub use metrics::{metrics_output, run_metrics};
pub use optimize::{optimize_output, run_optimize};
pub use plan::{
    ModelNotFound, PlanRunOptions, compute_embedded_capabilities, plan, plan_preview_output,
    plan_promote, populate_governance_actions,
};
pub use playground::{run_playground, run_playground_with_template};
pub use policy::{run_policy_check, run_policy_freeze, run_policy_test};
pub use preview::{
    PreviewDiffAlgorithmSelector, run_preview_cost, run_preview_create, run_preview_diff,
};
pub use preview_rows::run_preview_rows;
pub use profile::run_profile;
pub use profile_storage::run_profile_storage;
pub use publish_ir::run_publish_ir;
pub use replay::{run_replay, run_replay_check, run_replay_execute, run_replay_execute_warehouse};
pub use restore::run_restore_plan;
pub use retention_status::run_retention_status;
pub use review::{compute_review, compute_review_queue, run_review, run_review_queue};
// Re-exported so the `rocky` bin can build a clap ValueEnum for
// `--target-dialect` without taking a direct dep on rocky-sql.
pub use rocky_sql::transpile::Dialect;
pub use run::{
    DeferOptions, Interrupted, PartialFailure, PartitionRunOptions, SkipRunOptions, run,
};
pub use run_dag_exec::run_with_dag;
pub use run_watch::run_watch as run_with_watch;
pub use seed::run_seed;
pub use serve::run_serve;
pub use shell::run_shell;
pub use snapshot::run_snapshot;
pub use state::{state_clear_schema_cache, state_retention_sweep, state_show};
#[cfg(feature = "duckdb")]
pub use test::run_declarative_tests;
#[cfg(feature = "duckdb")]
pub use test::{run_test, test_output};
pub use test_adapter::{run_test_adapter, run_test_adapter_builtin};
pub use tick::run_tick;
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
/// Reserved keys:
/// - `id` matches against `DiscoveredConnector.id`.
/// - `table` is also reserved (used for per-table filtering); the
///   connector-level filter never accepts it as a schema-component
///   axis. Callers that need to filter by table name must apply the
///   `table=` filter on the discovered table list separately. See
///   [`filter_table_matches`].
///
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
    if filter_key == "table" {
        // `table=` is consumed at the table level — every connector
        // passes the connector-level filter; the table loop filters
        // the discovered tables individually via
        // `filter_table_matches`.
        return true;
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

/// Returns true when a `--filter table=<literal>` filter (or no
/// table filter at all) matches the given table name.
///
/// CLI `--filter table=` accepts literals only — no globs. Globs live
/// in the TOML `[[table_overrides]]` `match.table` grammar instead, to
/// avoid CLI shell-quoting footguns (`--filter table=*_temp` would be
/// shell-globbed before reaching Rocky on most shells).
///
/// When `filter` is `None` or the key isn't `"table"`, every table
/// matches (the connector-level filter has already decided whether
/// this connector is in scope).
pub(crate) fn filter_table_matches(filter: Option<&(String, String)>, table_name: &str) -> bool {
    let Some((key, value)) = filter else {
        return true;
    };
    if key != "table" {
        return true;
    }
    table_name == value
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
            external_object_ids: Vec::new(),
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
            external_object_ids: Vec::new(),
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
            external_object_ids: Vec::new(),
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
            external_object_ids: Vec::new(),
        };
        let parsed = ParsedSchema {
            values: IndexMap::from([("client".into(), SchemaValue::Single("acme".into()))]),
        };
        assert!(!matches_filter(&conn, &parsed, "nonexistent", "value"));
    }

    #[test]
    fn test_matches_filter_table_passes_at_connector_level() {
        // `table=` is the new reserved key — at the connector level,
        // every connector passes, and the table-level filter
        // (`filter_table_matches`) handles the actual subsetting.
        let conn = DiscoveredConnector {
            id: "conn_123".into(),
            schema: "test".into(),
            source_type: "fivetran".into(),
            last_sync_at: None,
            tables: vec![],
            metadata: Default::default(),
            external_object_ids: Vec::new(),
        };
        let parsed = ParsedSchema {
            values: IndexMap::from([("client".into(), SchemaValue::Single("acme".into()))]),
        };
        assert!(matches_filter(&conn, &parsed, "table", "anything"));
    }

    #[test]
    fn test_filter_table_matches_no_filter_matches_all() {
        assert!(filter_table_matches(None, "users"));
        assert!(filter_table_matches(None, "_diagnostics_x"));
    }

    #[test]
    fn test_filter_table_matches_non_table_key_passes() {
        // Connector-level filters don't subset at the table level.
        let f = ("client".to_string(), "acme".to_string());
        assert!(filter_table_matches(Some(&f), "users"));
    }

    #[test]
    fn test_filter_table_matches_table_literal_subsets() {
        let f = ("table".to_string(), "pii_users".to_string());
        assert!(filter_table_matches(Some(&f), "pii_users"));
        assert!(!filter_table_matches(Some(&f), "orders"));
    }

    #[test]
    fn test_filter_table_matches_no_glob_in_cli() {
        // CLI accepts literals only — globs live in TOML
        // [[table_overrides]]. A `*` in the filter value is treated
        // literally (won't match anything in practice).
        let f = ("table".to_string(), "_diagnostics_*".to_string());
        assert!(filter_table_matches(Some(&f), "_diagnostics_*"));
        assert!(!filter_table_matches(Some(&f), "_diagnostics_x"));
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

    /// End-to-end key agreement for the `rocky cost --by tenant` plumbing:
    /// parse a real `src__{tenant}__{regions...}__{source}` schema with the
    /// canonical pattern, run it through `parsed_to_json_map`, and apply the
    /// *exact* extraction `run.rs` uses to populate `TableTask::tenant`.
    /// This pins the contract that the run-side component map keys the tenant
    /// under the literal `"tenant"` as a `Value::String` — if the schema
    /// parser ever renamed that component, this fails instead of the feature
    /// silently producing an all-`<unattributed>` cost rollup.
    #[test]
    fn parsed_to_json_map_keys_tenant_as_string_for_cost_attribution() {
        use rocky_core::schema::SchemaPattern;

        let pattern = SchemaPattern {
            prefix: "src__".to_string(),
            separator: "__".to_string(),
            components: SchemaPattern::parse_components(&[
                "tenant".to_string(),
                "regions...".to_string(),
                "source".to_string(),
            ])
            .unwrap(),
        };
        let parsed = pattern.parse("src__acme__us_west__shopify").unwrap();
        let components = parsed_to_json_map(&parsed);

        // The exact expression in run.rs's TableTask builder.
        let tenant = components
            .get("tenant")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        assert_eq!(tenant.as_deref(), Some("acme"));

        // A pattern without a `{tenant}` component yields no tenant — the
        // extraction returns None and the cost rollup buckets the model as
        // unattributed.
        let no_tenant_pattern = SchemaPattern {
            prefix: "src__".to_string(),
            separator: "__".to_string(),
            components: SchemaPattern::parse_components(&["source".to_string()]).unwrap(),
        };
        let parsed_no_tenant = no_tenant_pattern.parse("src__shopify").unwrap();
        let components_no_tenant = parsed_to_json_map(&parsed_no_tenant);
        assert!(
            components_no_tenant
                .get("tenant")
                .and_then(|v| v.as_str())
                .is_none()
        );
    }
}
