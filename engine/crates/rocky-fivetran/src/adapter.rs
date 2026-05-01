//! Fivetran discovery adapter implementing [`DiscoveryAdapter`].
//!
//! Calls the Fivetran REST API to discover connectors and their enabled tables
//! in a destination. This is a metadata-only operation.

use async_trait::async_trait;
use futures::stream::{self, StreamExt};
use indexmap::IndexMap;
use tracing::warn;

use rocky_core::source::{
    DiscoveredConnector, DiscoveredTable, DiscoveryResult, FailedSource, FailedSourceErrorClass,
};
use rocky_core::traits::{AdapterError, AdapterResult, DiscoveryAdapter};

use crate::client::{FivetranClient, FivetranError};
use crate::connector::{self as ft_connector, Connector};
use crate::schema as ft_schema;

/// Fivetran source discovery adapter.
///
/// Discovers connectors and their enabled tables from a Fivetran destination
/// by calling the Fivetran REST API.
pub struct FivetranDiscoveryAdapter {
    client: FivetranClient,
    destination_id: String,
}

impl FivetranDiscoveryAdapter {
    pub fn new(client: FivetranClient, destination_id: String) -> Self {
        Self {
            client,
            destination_id,
        }
    }
}

#[async_trait]
impl DiscoveryAdapter for FivetranDiscoveryAdapter {
    async fn discover(&self, schema_prefix: &str) -> AdapterResult<DiscoveryResult> {
        let connectors =
            ft_connector::discover_connectors(&self.client, &self.destination_id, schema_prefix)
                .await
                .map_err(AdapterError::new)?;

        let discover_concurrency = 10;

        let fetched: Vec<(Connector, Result<ft_schema::SchemaConfig, FivetranError>)> =
            stream::iter(connectors)
                .map(|conn| {
                    let client = &self.client;
                    async move {
                        let schema_result = ft_schema::get_schema_config(client, &conn.id).await;
                        (conn, schema_result)
                    }
                })
                .buffer_unordered(discover_concurrency)
                .collect()
                .await;

        let mut connectors = Vec::new();
        let mut failed = Vec::new();
        for (conn, schema_result) in fetched {
            match schema_result {
                Ok(schema_config) => {
                    let raw_tables: Vec<DiscoveredTable> = schema_config
                        .enabled_tables()
                        .iter()
                        .map(|t| DiscoveredTable {
                            name: t.table_name.clone(),
                            row_count: None,
                        })
                        .collect();
                    let tables = dedup_tables_by_name(raw_tables, &conn.id);
                    let metadata = metadata_from_connector(&conn);
                    connectors.push(DiscoveredConnector {
                        id: conn.id,
                        schema: conn.schema,
                        source_type: conn.service,
                        last_sync_at: conn.succeeded_at,
                        tables,
                        metadata,
                    });
                }
                Err(e) => {
                    let error_class = classify_fivetran_error(&e);
                    warn!(
                        connector = conn.id.as_str(),
                        error = %e,
                        error_class = %error_class,
                        "schema fetch failed, surfacing as failed source"
                    );
                    failed.push(FailedSource {
                        id: conn.id,
                        schema: conn.schema,
                        source_type: conn.service,
                        error_class,
                        message: e.to_string(),
                    });
                }
            }
        }

        Ok(DiscoveryResult { connectors, failed })
    }
}

/// Map a [`FivetranError`] onto the coarse [`FailedSourceErrorClass`].
///
/// The classes are operating-mode signals for downstream consumers, not a
/// faithful taxonomy of the underlying transport. The `RetryBudgetExhausted`
/// variant maps to `Transient` because the budget is per-run — a fresh
/// discover invocation gets a new budget.
fn classify_fivetran_error(err: &FivetranError) -> FailedSourceErrorClass {
    match err {
        FivetranError::RateLimited => FailedSourceErrorClass::RateLimit,
        FivetranError::RetryBudgetExhausted { .. } => FailedSourceErrorClass::Transient,
        FivetranError::UnexpectedResponse(_) => FailedSourceErrorClass::Unknown,
        // `code` here is either the raw HTTP status display (e.g. "503
        // Service Unavailable") from `client::get`, or the API envelope's
        // `code` field (e.g. "Unauthorized"). Lead-digit + canonical-reason
        // matching covers both.
        FivetranError::Api { code, .. } => classify_api_code(code),
        FivetranError::Http(reqwest_err) => {
            if reqwest_err.is_timeout() {
                FailedSourceErrorClass::Timeout
            } else if reqwest_err.is_connect() {
                FailedSourceErrorClass::Transient
            } else if let Some(status) = reqwest_err.status() {
                classify_status_code(status.as_u16())
            } else {
                FailedSourceErrorClass::Unknown
            }
        }
    }
}

fn classify_api_code(code: &str) -> FailedSourceErrorClass {
    let trimmed = code.trim();
    // Lead numeric prefix: "503 Service Unavailable" → 503.
    if let Some(num) = trimmed
        .split_whitespace()
        .next()
        .and_then(|s| s.parse::<u16>().ok())
    {
        return classify_status_code(num);
    }
    // Envelope codes are alphanumeric tokens like "Unauthorized" or
    // "RateLimited" — the upstream Fivetran API sends these for non-success
    // envelopes wrapped in HTTP 200. Match the documented vocabulary and
    // leave anything else as Unknown.
    match trimmed {
        "Unauthorized" | "Forbidden" | "AuthFailed" => FailedSourceErrorClass::Auth,
        "RateLimited" | "TooManyRequests" => FailedSourceErrorClass::RateLimit,
        _ => FailedSourceErrorClass::Unknown,
    }
}

fn classify_status_code(status: u16) -> FailedSourceErrorClass {
    match status {
        401 | 403 => FailedSourceErrorClass::Auth,
        408 => FailedSourceErrorClass::Timeout,
        429 => FailedSourceErrorClass::RateLimit,
        500..=599 => FailedSourceErrorClass::Transient,
        _ => FailedSourceErrorClass::Unknown,
    }
}

/// Deduplicate the per-connector table list by table name, preserving the
/// first occurrence.
///
/// Fivetran's `/v1/connectors/{id}/schemas` response can list two distinct
/// logical schema-entry keys that resolve to the same destination name —
/// for example, when Fivetran auto-renames a table (`do_not_alter_*` prefix
/// after a breaking schema change) the original logical key may stay
/// alongside a fresh entry whose `name_in_destination` matches the renamed
/// table that already exists. `SchemaConfig::enabled_tables()` faithfully
/// forwards both rows, which leaves duplicate `DiscoveredTable` records in
/// the discover output and breaks downstream consumers (e.g. dagster-rocky's
/// `multi_asset` decorator rejects duplicate `AssetCheckSpec`s).
///
/// Dedup is Fivetran-specific: this is an upstream API quirk, not something
/// other discovery adapters share, so it lives here rather than in
/// `rocky-core`. A WARN is emitted with the connector id and the duplicate
/// count so the noise is visible — silently dropping rows would hide a real
/// upstream-config problem worth surfacing to operators.
fn dedup_tables_by_name(tables: Vec<DiscoveredTable>, source_id: &str) -> Vec<DiscoveredTable> {
    let mut seen = std::collections::HashSet::with_capacity(tables.len());
    let mut deduped = Vec::with_capacity(tables.len());
    let mut dropped: std::collections::BTreeMap<String, usize> = std::collections::BTreeMap::new();
    for table in tables {
        if seen.insert(table.name.clone()) {
            deduped.push(table);
        } else {
            *dropped.entry(table.name.clone()).or_insert(0) += 1;
        }
    }
    if !dropped.is_empty() {
        let total: usize = dropped.values().sum();
        let detail = dropped
            .iter()
            .map(|(name, count)| format!("{name}={count}"))
            .collect::<Vec<_>>()
            .join(",");
        warn!(
            source_id,
            duplicates = total,
            duplicate_names = detail.as_str(),
            "fivetran discover: dropped duplicate table records — Fivetran returned multiple \
             schema entries that resolve to the same destination table name (likely an \
             auto-rename collision); first occurrence kept"
        );
    }
    deduped
}

/// Project a stable, namespaced subset of Fivetran connector config into the
/// adapter-neutral metadata map.
///
/// Keys are flat `fivetran.*` strings so downstream consumers can branch on
/// connector type (stock vs custom report, schema_prefix, etc.) without
/// re-calling the Fivetran API. Values are verbatim [`serde_json::Value`]
/// clones — Rocky relays the service-specific payload rather than modelling
/// every Fivetran service.
///
/// Iteration order is stable (insertion order via [`IndexMap`]) so the
/// discover JSON output stays byte-stable across runs — important for the
/// dagster fixture corpus and the `codegen-drift` CI check.
fn metadata_from_connector(conn: &Connector) -> IndexMap<String, serde_json::Value> {
    let mut metadata = IndexMap::new();
    // Core identity — always populated even when `config` is empty so
    // downstream consumers have a stable signal for "Fivetran surfaced this".
    metadata.insert(
        "fivetran.service".into(),
        serde_json::Value::String(conn.service.clone()),
    );
    metadata.insert(
        "fivetran.connector_id".into(),
        serde_json::Value::String(conn.id.clone()),
    );

    // Service-specific projections — each is only stamped when Fivetran
    // actually returned it. Keep the set tight: adding a key is cheap
    // later, removing one is a breaking change for downstream consumers.
    for key in ["schema_prefix", "custom_tables"] {
        if let Some(value) = conn.config.get(key)
            && !value.is_null()
        {
            metadata.insert(format!("fivetran.{key}"), value.clone());
        }
    }
    // `config.reports` is Fivetran's wire name for the user-defined custom
    // report list (ad-platform connectors ship both stock tables and
    // user-defined reports). Rocky exposes it under the semantically
    // clearer `fivetran.custom_reports` key so downstream consumers don't
    // conflate it with any future "all reports" surface.
    if let Some(reports) = conn.config.get("reports")
        && !reports.is_null()
    {
        metadata.insert("fivetran.custom_reports".into(), reports.clone());
    }

    metadata
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::ConnectorStatus;

    fn sample_connector(config: serde_json::Value) -> Connector {
        Connector {
            id: "conn_ads".into(),
            group_id: "group_xyz".into(),
            service: "facebook_ads".into(),
            schema: "src__acme__na__fb_ads".into(),
            status: ConnectorStatus {
                setup_state: "connected".into(),
                sync_state: "scheduled".into(),
            },
            succeeded_at: None,
            failed_at: None,
            config,
        }
    }

    #[test]
    fn metadata_projects_core_identity_when_config_empty() {
        let conn = sample_connector(serde_json::Value::Null);
        let metadata = metadata_from_connector(&conn);
        assert_eq!(metadata["fivetran.service"], "facebook_ads");
        assert_eq!(metadata["fivetran.connector_id"], "conn_ads");
        assert!(!metadata.contains_key("fivetran.custom_reports"));
        assert!(!metadata.contains_key("fivetran.custom_tables"));
    }

    #[test]
    fn metadata_projects_custom_reports_and_tables_verbatim() {
        let conn = sample_connector(serde_json::json!({
            "schema_prefix": "fb_ads",
            "custom_tables": [
                {"table_name": "ads_insights", "breakdowns": ["age"]}
            ],
            "reports": [
                {"name": "custom_report_revenue", "report_type": "ads_insights"}
            ],
            "ignored_field": "not projected"
        }));
        let metadata = metadata_from_connector(&conn);
        assert_eq!(metadata["fivetran.schema_prefix"], "fb_ads");
        assert!(metadata["fivetran.custom_tables"].is_array());
        assert_eq!(
            metadata["fivetran.custom_tables"][0]["table_name"],
            "ads_insights"
        );
        // Fivetran's wire-level `config.reports` is surfaced under the
        // semantically clearer `fivetran.custom_reports` Rocky key so
        // downstream consumers can branch on stock-vs-custom without
        // re-parsing connector strings.
        assert_eq!(
            metadata["fivetran.custom_reports"][0]["name"],
            "custom_report_revenue"
        );
        assert!(!metadata.contains_key("fivetran.reports"));
        // Unknown fields are intentionally not projected — keeps the
        // downstream surface stable.
        assert!(!metadata.contains_key("fivetran.ignored_field"));
    }

    #[test]
    fn metadata_skips_null_projections() {
        let conn = sample_connector(serde_json::json!({
            "reports": null,
            "custom_tables": [],
        }));
        let metadata = metadata_from_connector(&conn);
        // Explicit null drops through.
        assert!(!metadata.contains_key("fivetran.custom_reports"));
        // Empty arrays still stamp (distinguishable from "not configured").
        assert!(metadata.contains_key("fivetran.custom_tables"));
    }

    #[test]
    fn dedup_tables_drops_duplicates_preserving_first_occurrence() {
        // Reproduces the Fivetran auto-rename collision: two schema entries
        // resolve to the same destination table name. `enabled_tables()`
        // faithfully forwards both; the adapter must dedup before handing
        // the list to downstream consumers (Dagster's `multi_asset`
        // rejects duplicate `AssetCheckSpec`s).
        let input = vec![
            DiscoveredTable {
                name: "do_not_alter_dpm_raw_youtube".into(),
                row_count: Some(100),
            },
            DiscoveredTable {
                name: "orders".into(),
                row_count: None,
            },
            // Duplicate of the first entry, with different metadata to
            // confirm we keep the first occurrence rather than the last.
            DiscoveredTable {
                name: "do_not_alter_dpm_raw_youtube".into(),
                row_count: Some(999),
            },
            DiscoveredTable {
                name: "orders".into(),
                row_count: Some(42),
            },
        ];
        let deduped = dedup_tables_by_name(input, "conn_test");
        assert_eq!(deduped.len(), 2);
        // First occurrence preserved (row_count: Some(100), not Some(999)).
        let yt = deduped
            .iter()
            .find(|t| t.name == "do_not_alter_dpm_raw_youtube")
            .expect("youtube table should be present");
        assert_eq!(yt.row_count, Some(100));
        let orders = deduped
            .iter()
            .find(|t| t.name == "orders")
            .expect("orders table should be present");
        assert_eq!(orders.row_count, None);
    }

    #[test]
    fn dedup_tables_is_noop_when_no_duplicates() {
        let input = vec![
            DiscoveredTable {
                name: "a".into(),
                row_count: None,
            },
            DiscoveredTable {
                name: "b".into(),
                row_count: None,
            },
        ];
        let deduped = dedup_tables_by_name(input.clone(), "conn_test");
        assert_eq!(deduped.len(), 2);
        assert_eq!(deduped[0].name, "a");
        assert_eq!(deduped[1].name, "b");
    }

    #[test]
    fn dedup_tables_handles_empty_input() {
        let deduped = dedup_tables_by_name(Vec::new(), "conn_test");
        assert!(deduped.is_empty());
    }

    #[test]
    fn metadata_iteration_order_is_stable() {
        // Stability matters for the dagster fixture corpus + codegen-drift
        // CI — an IndexMap preserves insertion order so two runs produce
        // byte-identical JSON.
        let conn = sample_connector(serde_json::json!({
            "schema_prefix": "fb_ads",
            "custom_tables": [],
            "reports": [],
        }));
        let metadata = metadata_from_connector(&conn);
        let keys: Vec<&str> = metadata.keys().map(String::as_str).collect();
        assert_eq!(
            keys,
            vec![
                "fivetran.service",
                "fivetran.connector_id",
                "fivetran.schema_prefix",
                "fivetran.custom_tables",
                "fivetran.custom_reports",
            ]
        );
    }
}
