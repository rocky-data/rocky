//! Async Airbyte REST API client.
//!
//! Wraps the Airbyte Configuration API v1 to list and inspect connections.
//! Connections are Airbyte's unit of replication (source + destination + streams).

use std::time::Duration;

use chrono::{DateTime, Utc};
use reqwest::Client;
use rocky_core::config::RetryConfig;
use rocky_core::redacted::RedactedString;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{debug, warn};

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

#[derive(Debug, Error)]
pub enum AirbyteError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("API error ({status}): {message}")]
    Api { status: u16, message: String },

    #[error("unexpected response format: {0}")]
    UnexpectedResponse(String),

    #[error("rate limited -- retry after backoff")]
    RateLimited,
}

// ---------------------------------------------------------------------------
// API types
// ---------------------------------------------------------------------------

/// Top-level response envelope for list endpoints.
#[derive(Debug, Clone, Deserialize)]
pub struct ConnectionsListResponse {
    pub data: Vec<Connection>,
    #[serde(default)]
    pub next: Option<String>,
}

/// An Airbyte connection (source -> destination replication job).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Connection {
    pub connection_id: String,
    pub name: String,
    pub source_id: String,
    pub destination_id: String,
    pub status: ConnectionStatus,
    #[serde(default)]
    pub namespace_format: Option<String>,
    #[serde(default)]
    pub configurations: Option<ConnectionConfigurations>,
    /// Last time data was successfully synced through this connection.
    #[serde(default)]
    pub last_sync_at: Option<DateTime<Utc>>,
}

/// Connection lifecycle status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ConnectionStatus {
    Active,
    Inactive,
    Deprecated,
}

/// Wrapper around the stream configurations for a connection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionConfigurations {
    pub streams: Vec<StreamConfiguration>,
}

/// A single stream (table) within a connection.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamConfiguration {
    pub name: String,
    #[serde(default)]
    pub sync_mode: Option<String>,
    #[serde(default)]
    pub destination_sync_mode: Option<String>,
    #[serde(default)]
    pub primary_key: Option<Vec<Vec<String>>>,
    #[serde(default)]
    pub cursor_field: Option<Vec<String>>,
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

/// Async Airbyte REST client with optional Bearer auth and configurable retry.
///
/// Targets the Airbyte Configuration API v1:
/// - `GET /v1/connections` -- list all connections
/// - `GET /v1/connections/{connectionId}` -- get a single connection
pub struct AirbyteClient {
    client: Client,
    base_url: String,
    auth_token: Option<RedactedString>,
    retry: RetryConfig,
}

impl AirbyteClient {
    /// Create a new client.
    ///
    /// `api_url` should be the Airbyte API root, e.g. `https://api.airbyte.com`.
    /// `auth_token` is optional -- Cloud deployments require it (Bearer token),
    /// self-hosted OSS instances may not require auth.
    pub fn new(api_url: &str, auth_token: Option<String>) -> Self {
        Self::with_retry(api_url, auth_token, RetryConfig::default())
    }

    /// Create a new client with custom retry configuration.
    pub fn with_retry(api_url: &str, auth_token: Option<String>, retry: RetryConfig) -> Self {
        let base_url = api_url.trim_end_matches('/').to_string();
        AirbyteClient {
            client: Client::new(),
            base_url,
            auth_token: auth_token.map(RedactedString::new),
            retry,
        }
    }

    /// List all connections visible to the authenticated user.
    ///
    /// Follows pagination cursors (`next` field) to collect all pages.
    pub async fn list_connections(&self) -> Result<Vec<Connection>, AirbyteError> {
        let mut all = Vec::new();
        let mut url = format!("{}/v1/connections", self.base_url);

        loop {
            let resp: ConnectionsListResponse = self.get(&url).await?;
            all.extend(resp.data);

            match resp.next {
                Some(next_url) if !next_url.is_empty() => {
                    url = next_url;
                }
                _ => break,
            }
        }

        debug!(count = all.len(), "listed Airbyte connections");
        Ok(all)
    }

    /// Fetch a single connection by ID.
    pub async fn get_connection(&self, id: &str) -> Result<Connection, AirbyteError> {
        let url = format!("{}/v1/connections/{}", self.base_url, id);
        self.get(&url).await
    }

    /// Internal GET with retry on transient errors.
    async fn get<T: serde::de::DeserializeOwned>(&self, url: &str) -> Result<T, AirbyteError> {
        for attempt in 0..=self.retry.max_retries {
            debug!(url, attempt, "GET");

            let mut req = self.client.get(url);
            if let Some(token) = &self.auth_token {
                req = req.bearer_auth(token.expose());
            }

            let resp = match req.send().await {
                Ok(r) => r,
                Err(e)
                    if attempt < self.retry.max_retries && (e.is_connect() || e.is_timeout()) =>
                {
                    let backoff = retry_backoff(&self.retry, attempt);
                    warn!(
                        attempt = attempt + 1,
                        backoff_ms = backoff.as_millis() as u64,
                        error = %e,
                        "transient HTTP error, retrying"
                    );
                    tokio::time::sleep(backoff).await;
                    continue;
                }
                Err(e) => return Err(e.into()),
            };

            if resp.status() == 429 {
                if attempt < self.retry.max_retries {
                    let backoff = retry_backoff(&self.retry, attempt);
                    warn!(
                        attempt = attempt + 1,
                        backoff_ms = backoff.as_millis() as u64,
                        "rate limited, retrying"
                    );
                    tokio::time::sleep(backoff).await;
                    continue;
                }
                return Err(AirbyteError::RateLimited);
            }

            if resp.status().is_server_error() && attempt < self.retry.max_retries {
                let status = resp.status().as_u16();
                let backoff = retry_backoff(&self.retry, attempt);
                warn!(
                    attempt = attempt + 1,
                    status,
                    backoff_ms = backoff.as_millis() as u64,
                    "server error, retrying"
                );
                tokio::time::sleep(backoff).await;
                continue;
            }

            if !resp.status().is_success() {
                let status = resp.status().as_u16();
                let body = resp.text().await.unwrap_or_default();
                return Err(AirbyteError::Api {
                    status,
                    message: body,
                });
            }

            return resp.json().await.map_err(|e| {
                AirbyteError::UnexpectedResponse(format!("failed to parse response: {e}"))
            });
        }

        unreachable!("retry loop should always return")
    }
}

/// Computes exponential backoff from RetryConfig.
fn retry_backoff(cfg: &RetryConfig, attempt: u32) -> Duration {
    let base = (cfg.initial_backoff_ms as f64) * cfg.backoff_multiplier.powi(attempt as i32);
    let ms = base.min(cfg.max_backoff_ms as f64) as u64;
    Duration::from_millis(ms)
}

impl std::fmt::Debug for AirbyteClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AirbyteClient")
            .field("base_url", &self.base_url)
            .field("auth_token", &self.auth_token.as_ref().map(|_| "***"))
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_debug_hides_secrets() {
        let client =
            AirbyteClient::new("https://api.airbyte.com", Some("secret_token_value".into()));
        let debug = format!("{client:?}");
        assert!(!debug.contains("secret_token_value"));
        assert!(debug.contains("***"));
    }

    #[test]
    fn test_debug_without_token() {
        let client = AirbyteClient::new("https://localhost:8000", None);
        let debug = format!("{client:?}");
        assert!(debug.contains("localhost"));
        assert!(!debug.contains("secret"));
    }

    #[test]
    fn test_base_url_trailing_slash_stripped() {
        let client = AirbyteClient::new("https://api.airbyte.com/", None);
        assert_eq!(client.base_url, "https://api.airbyte.com");
    }

    #[test]
    fn test_connection_status_deserialize() {
        let json = r#""active""#;
        let status: ConnectionStatus = serde_json::from_str(json).unwrap();
        assert_eq!(status, ConnectionStatus::Active);

        let json = r#""inactive""#;
        let status: ConnectionStatus = serde_json::from_str(json).unwrap();
        assert_eq!(status, ConnectionStatus::Inactive);

        let json = r#""deprecated""#;
        let status: ConnectionStatus = serde_json::from_str(json).unwrap();
        assert_eq!(status, ConnectionStatus::Deprecated);
    }

    #[test]
    fn test_connection_deserialize() {
        let json = r#"{
            "connectionId": "conn_abc123",
            "name": "Postgres -> Databricks",
            "sourceId": "src_001",
            "destinationId": "dst_001",
            "status": "active",
            "namespaceFormat": "src__acme__na__postgres",
            "configurations": {
                "streams": [
                    {
                        "name": "orders",
                        "syncMode": "incremental",
                        "destinationSyncMode": "append_dedup",
                        "primaryKey": [["id"]],
                        "cursorField": ["updated_at"]
                    },
                    {
                        "name": "customers",
                        "syncMode": "full_refresh",
                        "destinationSyncMode": "overwrite"
                    }
                ]
            },
            "lastSyncAt": "2026-04-10T14:30:00Z"
        }"#;

        let conn: Connection = serde_json::from_str(json).unwrap();
        assert_eq!(conn.connection_id, "conn_abc123");
        assert_eq!(conn.name, "Postgres -> Databricks");
        assert_eq!(conn.source_id, "src_001");
        assert_eq!(conn.destination_id, "dst_001");
        assert_eq!(conn.status, ConnectionStatus::Active);
        assert_eq!(
            conn.namespace_format.as_deref(),
            Some("src__acme__na__postgres")
        );

        let streams = conn.configurations.unwrap().streams;
        assert_eq!(streams.len(), 2);
        assert_eq!(streams[0].name, "orders");
        assert_eq!(streams[0].sync_mode.as_deref(), Some("incremental"));
        assert_eq!(streams[0].primary_key, Some(vec![vec!["id".to_string()]]));
        assert_eq!(
            streams[0].cursor_field,
            Some(vec!["updated_at".to_string()])
        );
        assert_eq!(streams[1].name, "customers");
        assert!(streams[1].primary_key.is_none());
    }

    #[test]
    fn test_connection_deserialize_minimal() {
        let json = r#"{
            "connectionId": "conn_minimal",
            "name": "Minimal connection",
            "sourceId": "src_002",
            "destinationId": "dst_002",
            "status": "inactive"
        }"#;

        let conn: Connection = serde_json::from_str(json).unwrap();
        assert_eq!(conn.connection_id, "conn_minimal");
        assert_eq!(conn.status, ConnectionStatus::Inactive);
        assert!(conn.namespace_format.is_none());
        assert!(conn.configurations.is_none());
        assert!(conn.last_sync_at.is_none());
    }

    #[test]
    fn test_connections_list_response_deserialize() {
        let json = r#"{
            "data": [
                {
                    "connectionId": "conn_1",
                    "name": "Connection 1",
                    "sourceId": "src_1",
                    "destinationId": "dst_1",
                    "status": "active"
                },
                {
                    "connectionId": "conn_2",
                    "name": "Connection 2",
                    "sourceId": "src_2",
                    "destinationId": "dst_2",
                    "status": "inactive"
                }
            ],
            "next": "https://api.airbyte.com/v1/connections?after=conn_2"
        }"#;

        let resp: ConnectionsListResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.data.len(), 2);
        assert_eq!(resp.data[0].connection_id, "conn_1");
        assert_eq!(resp.data[1].connection_id, "conn_2");
        assert_eq!(
            resp.next.as_deref(),
            Some("https://api.airbyte.com/v1/connections?after=conn_2")
        );
    }

    #[test]
    fn test_connections_list_response_last_page() {
        let json = r#"{
            "data": [
                {
                    "connectionId": "conn_3",
                    "name": "Connection 3",
                    "sourceId": "src_3",
                    "destinationId": "dst_3",
                    "status": "active"
                }
            ]
        }"#;

        let resp: ConnectionsListResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.data.len(), 1);
        assert!(resp.next.is_none());
    }

    #[test]
    fn test_stream_configuration_deserialize() {
        let json = r#"{
            "name": "invoices",
            "syncMode": "incremental",
            "destinationSyncMode": "append_dedup",
            "primaryKey": [["id"], ["line_id"]],
            "cursorField": ["updated_at"]
        }"#;

        let stream: StreamConfiguration = serde_json::from_str(json).unwrap();
        assert_eq!(stream.name, "invoices");
        assert_eq!(stream.sync_mode.as_deref(), Some("incremental"));
        assert_eq!(stream.primary_key.as_ref().unwrap().len(), 2);
    }
}
