//! Fivetran discovery adapter implementing [`DiscoveryAdapter`].
//!
//! Calls the Fivetran REST API to discover connectors and their enabled tables
//! in a destination. This is a metadata-only operation.

use async_trait::async_trait;
use futures::stream::{self, StreamExt};
use indexmap::IndexMap;
use tracing::warn;

use rocky_core::source::{DiscoveredConnector, DiscoveredTable};
use rocky_core::traits::{AdapterError, AdapterResult, DiscoveryAdapter};

use crate::client::FivetranClient;
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
    async fn discover(&self, schema_prefix: &str) -> AdapterResult<Vec<DiscoveredConnector>> {
        let connectors =
            ft_connector::discover_connectors(&self.client, &self.destination_id, schema_prefix)
                .await
                .map_err(AdapterError::new)?;

        let discover_concurrency = 10;

        let result: Vec<DiscoveredConnector> = stream::iter(connectors)
            .map(|conn| {
                let client = &self.client;
                async move {
                    let schema_result = ft_schema::get_schema_config(client, &conn.id).await;
                    (conn, schema_result)
                }
            })
            .buffer_unordered(discover_concurrency)
            .filter_map(|(conn, schema_result)| async move {
                match schema_result {
                    Ok(schema_config) => {
                        let tables = schema_config
                            .enabled_tables()
                            .iter()
                            .map(|t| DiscoveredTable {
                                name: t.table_name.clone(),
                                row_count: None,
                            })
                            .collect();
                        let metadata = metadata_from_connector(&conn);
                        Some(DiscoveredConnector {
                            id: conn.id,
                            schema: conn.schema,
                            source_type: conn.service,
                            last_sync_at: conn.succeeded_at,
                            tables,
                            metadata,
                        })
                    }
                    Err(e) => {
                        warn!(
                            connector = conn.id,
                            error = %e,
                            "failed to fetch schema config, skipping"
                        );
                        None
                    }
                }
            })
            .collect()
            .await;

        Ok(result)
    }
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
