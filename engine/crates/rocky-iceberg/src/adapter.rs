//! Iceberg discovery adapter implementing [`DiscoveryAdapter`].
//!
//! Lists namespaces from an Iceberg REST Catalog, filters by prefix,
//! and maps tables to [`DiscoveredConnector`]. This is a metadata-only
//! operation -- it does not read Iceberg data files.

use async_trait::async_trait;
use tracing::{debug, warn};

use rocky_core::source::{DiscoveredConnector, DiscoveredTable};
use rocky_core::traits::{AdapterError, AdapterResult, DiscoveryAdapter};

use crate::client::IcebergCatalogClient;

/// Iceberg source discovery adapter.
///
/// Discovers namespaces and tables from an Iceberg REST Catalog. Each
/// namespace that matches the configured prefix is mapped to a
/// [`DiscoveredConnector`], with its tables as [`DiscoveredTable`] entries.
///
/// The `source_type` is set to `"iceberg"` for all discovered connectors.
/// The `last_sync_at` field is always `None` because the Iceberg REST
/// Catalog API does not expose sync timestamps at the namespace level.
pub struct IcebergDiscoveryAdapter {
    client: IcebergCatalogClient,
}

impl IcebergDiscoveryAdapter {
    pub fn new(client: IcebergCatalogClient) -> Self {
        Self { client }
    }
}

#[async_trait]
impl DiscoveryAdapter for IcebergDiscoveryAdapter {
    async fn discover(&self, schema_prefix: &str) -> AdapterResult<Vec<DiscoveredConnector>> {
        let namespaces = self
            .client
            .list_namespaces()
            .await
            .map_err(AdapterError::new)?;

        let matching: Vec<&String> = namespaces
            .iter()
            .filter(|ns| matches_prefix(ns, schema_prefix))
            .collect();

        debug!(
            prefix = schema_prefix,
            total = namespaces.len(),
            matched = matching.len(),
            "filtered Iceberg namespaces"
        );

        let mut connectors = Vec::with_capacity(matching.len());

        for ns in matching {
            match self.client.list_tables(ns).await {
                Ok(table_ids) => {
                    let tables = table_ids
                        .into_iter()
                        .map(|id| DiscoveredTable {
                            name: id.name,
                            row_count: None,
                        })
                        .collect();

                    connectors.push(DiscoveredConnector {
                        id: ns.clone(),
                        schema: ns.clone(),
                        source_type: "iceberg".to_string(),
                        last_sync_at: None,
                        tables,
                    });
                }
                Err(e) => {
                    warn!(
                        namespace = ns.as_str(),
                        error = %e,
                        "failed to list tables in namespace, skipping"
                    );
                }
            }
        }

        debug!(
            prefix = schema_prefix,
            count = connectors.len(),
            "discovered Iceberg connectors"
        );

        Ok(connectors)
    }

    async fn ping(&self) -> AdapterResult<()> {
        self.client
            .list_namespaces()
            .await
            .map(|_| ())
            .map_err(AdapterError::new)
    }
}

/// Check if a namespace matches the given prefix.
///
/// An empty prefix matches all namespaces. Otherwise the namespace
/// must start with the prefix string.
fn matches_prefix(namespace: &str, prefix: &str) -> bool {
    if prefix.is_empty() {
        return true;
    }
    namespace.starts_with(prefix)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_matches_prefix_empty() {
        assert!(matches_prefix("anything", ""));
        assert!(matches_prefix("", ""));
    }

    #[test]
    fn test_matches_prefix_exact() {
        assert!(matches_prefix("raw_shopify", "raw_"));
        assert!(matches_prefix("raw_shopify", "raw_shopify"));
    }

    #[test]
    fn test_matches_prefix_no_match() {
        assert!(!matches_prefix("staging_orders", "raw_"));
        assert!(!matches_prefix("ra", "raw_"));
    }

    #[test]
    fn test_matches_prefix_multi_level() {
        assert!(matches_prefix("analytics.staging", "analytics"));
        assert!(matches_prefix("analytics.staging", "analytics."));
        assert!(!matches_prefix("analytics.staging", "staging"));
    }

    /// Simulates the full discover flow offline: filter namespaces by
    /// prefix and map tables to DiscoveredConnector.
    #[test]
    fn test_discover_mapping_logic() {
        // Simulate namespaces returned by the catalog.
        let all_namespaces = [
            "raw_shopify".to_string(),
            "raw_stripe".to_string(),
            "staging_orders".to_string(),
            "raw_facebook_ads".to_string(),
        ];

        let prefix = "raw_";

        let matching: Vec<&String> = all_namespaces
            .iter()
            .filter(|ns| matches_prefix(ns, prefix))
            .collect();

        assert_eq!(matching.len(), 3);
        assert!(matching.contains(&&"raw_shopify".to_string()));
        assert!(matching.contains(&&"raw_stripe".to_string()));
        assert!(matching.contains(&&"raw_facebook_ads".to_string()));

        // Simulate building DiscoveredConnectors from matched namespaces.
        let connector = DiscoveredConnector {
            id: "raw_shopify".to_string(),
            schema: "raw_shopify".to_string(),
            source_type: "iceberg".to_string(),
            last_sync_at: None,
            tables: vec![
                DiscoveredTable {
                    name: "orders".to_string(),
                    row_count: None,
                },
                DiscoveredTable {
                    name: "customers".to_string(),
                    row_count: None,
                },
            ],
        };

        assert_eq!(connector.id, "raw_shopify");
        assert_eq!(connector.schema, "raw_shopify");
        assert_eq!(connector.source_type, "iceberg");
        assert!(connector.last_sync_at.is_none());
        assert_eq!(connector.tables.len(), 2);
        assert_eq!(connector.tables[0].name, "orders");
        assert_eq!(connector.tables[1].name, "customers");
    }

    #[test]
    fn test_discover_empty_prefix_matches_all() {
        let namespaces = ["a".to_string(), "b".to_string(), "c".to_string()];
        let matching: Vec<&String> = namespaces
            .iter()
            .filter(|ns| matches_prefix(ns, ""))
            .collect();
        assert_eq!(matching.len(), 3);
    }

    #[test]
    fn test_discover_no_matches() {
        let namespaces = [
            "staging_orders".to_string(),
            "staging_customers".to_string(),
        ];
        let matching: Vec<&String> = namespaces
            .iter()
            .filter(|ns| matches_prefix(ns, "raw_"))
            .collect();
        assert!(matching.is_empty());
    }

    #[test]
    fn test_discovered_connector_source_type() {
        let connector = DiscoveredConnector {
            id: "test_ns".to_string(),
            schema: "test_ns".to_string(),
            source_type: "iceberg".to_string(),
            last_sync_at: None,
            tables: vec![],
        };
        assert_eq!(connector.source_type, "iceberg");
    }
}
