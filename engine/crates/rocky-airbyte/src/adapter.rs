//! Airbyte discovery adapter implementing [`DiscoveryAdapter`].
//!
//! Calls the Airbyte REST API to discover connections and their configured
//! streams in a workspace. This is a metadata-only operation.

use async_trait::async_trait;
use tracing::{debug, warn};

use rocky_core::source::{DiscoveredConnector, DiscoveredTable};
use rocky_core::traits::{AdapterError, AdapterResult, DiscoveryAdapter};

use crate::client::{AirbyteClient, Connection, ConnectionStatus};

/// Airbyte source discovery adapter.
///
/// Discovers connections and their configured streams from Airbyte
/// by calling the Airbyte Configuration API v1. Connections are filtered
/// to active-only and optionally by a namespace prefix (analogous to
/// Fivetran's schema prefix filtering).
pub struct AirbyteDiscoveryAdapter {
    client: AirbyteClient,
}

impl AirbyteDiscoveryAdapter {
    pub fn new(client: AirbyteClient) -> Self {
        Self { client }
    }
}

#[async_trait]
impl DiscoveryAdapter for AirbyteDiscoveryAdapter {
    async fn discover(&self, schema_prefix: &str) -> AdapterResult<Vec<DiscoveredConnector>> {
        let connections = self
            .client
            .list_connections()
            .await
            .map_err(AdapterError::new)?;

        let result: Vec<DiscoveredConnector> = connections
            .into_iter()
            .filter(|c| c.status == ConnectionStatus::Active)
            .filter(|c| matches_prefix(c, schema_prefix))
            .map(map_connection)
            .collect();

        debug!(
            prefix = schema_prefix,
            count = result.len(),
            "discovered Airbyte connections"
        );

        Ok(result)
    }

    async fn ping(&self) -> AdapterResult<()> {
        // Use list_connections as a connectivity check -- the Airbyte API
        // doesn't expose a dedicated health/me endpoint in v1.
        self.client
            .list_connections()
            .await
            .map(|_| ())
            .map_err(AdapterError::new)
    }
}

/// Check if a connection's namespace format matches the given prefix.
///
/// Airbyte connections use `namespace_format` to define the schema name
/// in the destination. We filter on this field, falling back to the
/// connection name if no namespace format is set.
fn matches_prefix(conn: &Connection, prefix: &str) -> bool {
    if prefix.is_empty() {
        return true;
    }

    if let Some(ns) = &conn.namespace_format {
        if ns.starts_with(prefix) {
            return true;
        }
    }

    // Fall back to connection name for connections without namespace_format.
    conn.name.starts_with(prefix)
}

/// Map an Airbyte [`Connection`] to a [`DiscoveredConnector`].
fn map_connection(conn: Connection) -> DiscoveredConnector {
    let schema = conn
        .namespace_format
        .clone()
        .unwrap_or_else(|| conn.name.clone());

    let tables = match conn.configurations {
        Some(cfg) => cfg
            .streams
            .into_iter()
            .map(|s| DiscoveredTable {
                name: s.name,
                row_count: None,
            })
            .collect(),
        None => {
            warn!(
                connection_id = conn.connection_id,
                "connection has no stream configurations, yielding empty table list"
            );
            vec![]
        }
    };

    DiscoveredConnector {
        id: conn.connection_id,
        schema,
        source_type: "airbyte".to_string(),
        last_sync_at: conn.last_sync_at,
        tables,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use crate::client::{
        Connection, ConnectionConfigurations, ConnectionStatus, StreamConfiguration,
    };

    use super::*;

    fn sample_connection(
        id: &str,
        name: &str,
        namespace: Option<&str>,
        status: ConnectionStatus,
        streams: Vec<StreamConfiguration>,
    ) -> Connection {
        Connection {
            connection_id: id.into(),
            name: name.into(),
            source_id: "src_test".into(),
            destination_id: "dst_test".into(),
            status,
            namespace_format: namespace.map(|s| s.to_string()),
            configurations: if streams.is_empty() {
                None
            } else {
                Some(ConnectionConfigurations { streams })
            },
            last_sync_at: Some(Utc::now()),
        }
    }

    fn sample_stream(name: &str) -> StreamConfiguration {
        StreamConfiguration {
            name: name.into(),
            sync_mode: Some("incremental".into()),
            destination_sync_mode: Some("append_dedup".into()),
            primary_key: None,
            cursor_field: None,
        }
    }

    #[test]
    fn test_matches_prefix_namespace() {
        let conn = sample_connection(
            "c1",
            "My Connection",
            Some("src__acme__na__shopify"),
            ConnectionStatus::Active,
            vec![],
        );
        assert!(matches_prefix(&conn, "src__"));
        assert!(matches_prefix(&conn, "src__acme"));
        assert!(!matches_prefix(&conn, "q__staging__"));
    }

    #[test]
    fn test_matches_prefix_empty() {
        let conn = sample_connection("c1", "Anything", None, ConnectionStatus::Active, vec![]);
        assert!(matches_prefix(&conn, ""));
    }

    #[test]
    fn test_matches_prefix_fallback_to_name() {
        let conn = sample_connection(
            "c1",
            "src__globex__emea__stripe",
            None,
            ConnectionStatus::Active,
            vec![],
        );
        assert!(matches_prefix(&conn, "src__globex"));
        assert!(!matches_prefix(&conn, "q__staging__"));
    }

    #[test]
    fn test_map_connection_with_streams() {
        let conn = sample_connection(
            "conn_abc",
            "Postgres Prod",
            Some("src__acme__us__postgres"),
            ConnectionStatus::Active,
            vec![sample_stream("orders"), sample_stream("customers")],
        );

        let discovered = map_connection(conn);
        assert_eq!(discovered.id, "conn_abc");
        assert_eq!(discovered.schema, "src__acme__us__postgres");
        assert_eq!(discovered.source_type, "airbyte");
        assert_eq!(discovered.tables.len(), 2);
        assert_eq!(discovered.tables[0].name, "orders");
        assert_eq!(discovered.tables[1].name, "customers");
        assert!(discovered.tables[0].row_count.is_none());
    }

    #[test]
    fn test_map_connection_no_namespace_uses_name() {
        let conn = sample_connection(
            "conn_xyz",
            "My Shopify Connection",
            None,
            ConnectionStatus::Active,
            vec![sample_stream("products")],
        );

        let discovered = map_connection(conn);
        assert_eq!(discovered.schema, "My Shopify Connection");
    }

    #[test]
    fn test_map_connection_no_configurations() {
        let conn = Connection {
            connection_id: "conn_empty".into(),
            name: "Empty Connection".into(),
            source_id: "src_test".into(),
            destination_id: "dst_test".into(),
            status: ConnectionStatus::Active,
            namespace_format: Some("src__test".into()),
            configurations: None,
            last_sync_at: None,
        };

        let discovered = map_connection(conn);
        assert!(discovered.tables.is_empty());
        assert!(discovered.last_sync_at.is_none());
    }

    #[test]
    fn test_map_connection_preserves_last_sync() {
        let now = Utc::now();
        let conn = Connection {
            connection_id: "conn_ts".into(),
            name: "With Timestamp".into(),
            source_id: "src_test".into(),
            destination_id: "dst_test".into(),
            status: ConnectionStatus::Active,
            namespace_format: None,
            configurations: Some(ConnectionConfigurations {
                streams: vec![sample_stream("events")],
            }),
            last_sync_at: Some(now),
        };

        let discovered = map_connection(conn);
        assert_eq!(discovered.last_sync_at, Some(now));
    }

    /// Simulates the full discover flow: filter by status + prefix, then map.
    #[test]
    fn test_discover_filter_and_map() {
        let connections = vec![
            sample_connection(
                "c1",
                "Active matching",
                Some("src__acme__na__pg"),
                ConnectionStatus::Active,
                vec![sample_stream("users"), sample_stream("orders")],
            ),
            sample_connection(
                "c2",
                "Active non-matching",
                Some("other__schema"),
                ConnectionStatus::Active,
                vec![sample_stream("events")],
            ),
            sample_connection(
                "c3",
                "Inactive matching",
                Some("src__acme__eu__stripe"),
                ConnectionStatus::Inactive,
                vec![sample_stream("invoices")],
            ),
            sample_connection(
                "c4",
                "Another active matching",
                Some("src__acme__latam__shopify"),
                ConnectionStatus::Active,
                vec![sample_stream("products"), sample_stream("inventory")],
            ),
        ];

        let prefix = "src__acme";
        let result: Vec<DiscoveredConnector> = connections
            .into_iter()
            .filter(|c| c.status == ConnectionStatus::Active)
            .filter(|c| matches_prefix(c, prefix))
            .map(map_connection)
            .collect();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].id, "c1");
        assert_eq!(result[0].tables.len(), 2);
        assert_eq!(result[1].id, "c4");
        assert_eq!(result[1].tables.len(), 2);
    }
}
