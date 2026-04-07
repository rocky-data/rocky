//! Fivetran discovery adapter implementing [`DiscoveryAdapter`].
//!
//! Calls the Fivetran REST API to discover connectors and their enabled tables
//! in a destination. This is a metadata-only operation.

use async_trait::async_trait;
use futures::stream::{self, StreamExt};
use tracing::warn;

use rocky_core::source::{DiscoveredConnector, DiscoveredTable};
use rocky_core::traits::{AdapterError, AdapterResult, DiscoveryAdapter};

use crate::client::FivetranClient;
use crate::connector as ft_connector;
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
                        Some(DiscoveredConnector {
                            id: conn.id,
                            schema: conn.schema,
                            source_type: conn.service,
                            last_sync_at: conn.succeeded_at,
                            tables,
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
