use chrono::{DateTime, Utc};
use serde::Deserialize;
use tracing::debug;

use crate::client::{FivetranClient, FivetranError, encode_path_segment};

/// A Fivetran connector as returned by the API.
///
/// `config` is the service-specific configuration blob returned by
/// `GET /v1/connectors/{id}` (and `GET /v1/groups/{groupId}/connectors`
/// — the listing endpoint returns the same per-connector shape). Rocky
/// keeps it as an opaque `serde_json::Value` rather than trying to model
/// every Fivetran service: the discovery adapter projects a namespaced
/// subset into [`rocky_core::source::DiscoveredConnector::metadata`], and
/// downstream consumers that need richer structure (e.g. `custom_reports`
/// vs stock tables) read the projected fields directly.
#[derive(Debug, Clone, Deserialize)]
pub struct Connector {
    pub id: String,
    pub group_id: String,
    pub service: String,
    pub schema: String,
    pub status: ConnectorStatus,
    pub succeeded_at: Option<DateTime<Utc>>,
    pub failed_at: Option<DateTime<Utc>>,
    /// Raw, service-specific connector configuration. Absent (=`Value::Null`)
    /// when Fivetran omits the field — connectors in partial setup states may
    /// not include it.
    #[serde(default)]
    pub config: serde_json::Value,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ConnectorStatus {
    pub setup_state: String,
    pub sync_state: String,
}

impl Connector {
    /// Returns true if the connector is fully set up and active.
    pub fn is_connected(&self) -> bool {
        self.status.setup_state == "connected"
    }

    /// Returns true if the connector's schema matches the given prefix.
    pub fn matches_prefix(&self, prefix: &str) -> bool {
        self.schema.starts_with(prefix)
    }
}

/// Discovers connectors in a Fivetran destination (group).
pub async fn list_connectors(
    client: &FivetranClient,
    group_id: &str,
) -> Result<Vec<Connector>, FivetranError> {
    let path = format!("/v1/groups/{}/connectors", encode_path_segment(group_id));
    let connectors: Vec<Connector> = client.get_all_pages(&path).await?;
    debug!(group_id, count = connectors.len(), "discovered connectors");
    Ok(connectors)
}

/// Lists connectors filtered to only connected ones matching a schema prefix.
pub async fn discover_connectors(
    client: &FivetranClient,
    group_id: &str,
    schema_prefix: &str,
) -> Result<Vec<Connector>, FivetranError> {
    let all = list_connectors(client, group_id).await?;
    let filtered: Vec<Connector> = all
        .into_iter()
        .filter(|c| c.is_connected() && c.matches_prefix(schema_prefix))
        .collect();
    debug!(
        group_id,
        prefix = schema_prefix,
        count = filtered.len(),
        "filtered connectors"
    );
    Ok(filtered)
}

/// Gets a single connector by ID.
pub async fn get_connector(
    client: &FivetranClient,
    connector_id: &str,
) -> Result<Connector, FivetranError> {
    let path = format!("/v1/connectors/{}", encode_path_segment(connector_id));
    client.get(&path).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_connector(schema: &str, setup_state: &str) -> Connector {
        Connector {
            id: "conn_123".into(),
            group_id: "group_abc".into(),
            service: "shopify".into(),
            schema: schema.into(),
            status: ConnectorStatus {
                setup_state: setup_state.into(),
                sync_state: "scheduled".into(),
            },
            succeeded_at: None,
            failed_at: None,
            config: serde_json::Value::Null,
        }
    }

    #[test]
    fn test_is_connected() {
        assert!(sample_connector("schema", "connected").is_connected());
        assert!(!sample_connector("schema", "incomplete").is_connected());
        assert!(!sample_connector("schema", "broken").is_connected());
    }

    #[test]
    fn test_matches_prefix() {
        let conn = sample_connector("src__acme__na__fb_ads", "connected");
        assert!(conn.matches_prefix("src__"));
        assert!(conn.matches_prefix("src__acme"));
        assert!(!conn.matches_prefix("q__staging__"));
    }

    #[test]
    fn test_deserialize_connector() {
        let json = r#"{
            "id": "conn_abc",
            "group_id": "group_xyz",
            "service": "stripe",
            "schema": "src__globex__emea__stripe",
            "status": {
                "setup_state": "connected",
                "sync_state": "scheduled"
            },
            "succeeded_at": "2026-03-29T10:00:00Z",
            "failed_at": null
        }"#;

        let conn: Connector = serde_json::from_str(json).unwrap();
        assert_eq!(conn.id, "conn_abc");
        assert_eq!(conn.service, "stripe");
        assert!(conn.is_connected());
        assert!(conn.succeeded_at.is_some());
        assert!(conn.failed_at.is_none());
        // Missing `config` field defaults to `Value::Null` so the adapter
        // can fall back cleanly when Fivetran omits it on partial-setup
        // connectors.
        assert!(conn.config.is_null());
    }

    #[test]
    fn test_deserialize_connector_with_config_blob() {
        // Real Fivetran ad-platform connectors return a rich `config` object.
        // Rocky captures it opaquely so downstream consumers can project
        // service-specific fields (custom_reports, custom_tables,
        // schema_prefix, ...) without re-calling the API.
        let json = r#"{
            "id": "conn_ads",
            "group_id": "group_xyz",
            "service": "facebook_ads",
            "schema": "src__acme__na__fb_ads",
            "status": {
                "setup_state": "connected",
                "sync_state": "scheduled"
            },
            "succeeded_at": null,
            "failed_at": null,
            "config": {
                "schema_prefix": "fb_ads",
                "custom_tables": [
                    {"table_name": "ads_insights", "breakdowns": ["age", "gender"]}
                ],
                "reports": [
                    {"name": "custom_report_revenue", "report_type": "ads_insights"}
                ]
            }
        }"#;

        let conn: Connector = serde_json::from_str(json).unwrap();
        assert_eq!(conn.service, "facebook_ads");
        assert_eq!(conn.config["schema_prefix"], "fb_ads");
        assert!(conn.config["custom_tables"].is_array());
        assert!(conn.config["reports"].is_array());
        assert_eq!(
            conn.config["reports"][0]["name"].as_str(),
            Some("custom_report_revenue")
        );
    }
}
