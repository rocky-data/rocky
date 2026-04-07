//! Async Iceberg REST Catalog API client.
//!
//! Wraps the [Iceberg REST Catalog API](https://iceberg.apache.org/spec/#rest-catalog)
//! to list namespaces and tables. This is a metadata-only client -- it does not
//! read Iceberg data files.

use std::time::Duration;

use reqwest::Client;
use rocky_core::config::RetryConfig;
use rocky_core::redacted::RedactedString;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{debug, warn};

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

/// Errors returned by the Iceberg REST Catalog client.
#[derive(Debug, Error)]
pub enum IcebergError {
    /// Transport-level HTTP error (connection refused, timeout, TLS, etc.).
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    /// Non-success status code from the catalog API.
    #[error("API error ({status}): {message}")]
    Api { status: u16, message: String },

    /// Response body could not be parsed into the expected type.
    #[error("unexpected response format: {0}")]
    UnexpectedResponse(String),

    /// 429 Too Many Requests after all retries exhausted.
    #[error("rate limited -- retry after backoff")]
    RateLimited,
}

// ---------------------------------------------------------------------------
// API types
// ---------------------------------------------------------------------------

/// A fully qualified Iceberg table identifier (namespace + table name).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableIdentifier {
    /// The namespace (database/schema) this table belongs to.
    pub namespace: String,
    /// The table name within the namespace.
    pub name: String,
}

/// Response body for `GET /v1/namespaces`.
///
/// The Iceberg REST spec returns namespaces as an array of string arrays
/// (each namespace is a multi-level path). We flatten single-level
/// namespaces to plain strings.
#[derive(Debug, Clone, Deserialize)]
pub struct ListNamespacesResponse {
    pub namespaces: Vec<Vec<String>>,
}

/// Response body for `GET /v1/namespaces/{ns}/tables`.
#[derive(Debug, Clone, Deserialize)]
pub struct ListTablesResponse {
    pub identifiers: Vec<TableIdentifierResponse>,
}

/// Wire format for a table identifier in the REST response.
#[derive(Debug, Clone, Deserialize)]
pub struct TableIdentifierResponse {
    pub namespace: Vec<String>,
    pub name: String,
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

/// Async Iceberg REST Catalog client with optional Bearer auth and
/// configurable retry.
///
/// Targets the Iceberg REST Catalog spec v1:
/// - `GET /v1/namespaces` -- list all namespaces
/// - `GET /v1/namespaces/{ns}/tables` -- list tables in a namespace
pub struct IcebergCatalogClient {
    client: Client,
    base_url: String,
    auth_token: Option<RedactedString>,
    retry: RetryConfig,
}

impl IcebergCatalogClient {
    /// Create a new client.
    ///
    /// `catalog_url` is the Iceberg REST catalog base URL, e.g.
    /// `https://my-iceberg-catalog.example.com`. The trailing slash is
    /// stripped if present. `auth_token` is optional -- some catalogs
    /// require Bearer auth, others (e.g., local dev) do not.
    pub fn new(catalog_url: String, auth_token: Option<String>) -> Self {
        Self::with_retry(catalog_url, auth_token, RetryConfig::default())
    }

    /// Create a new client with custom retry configuration.
    pub fn with_retry(catalog_url: String, auth_token: Option<String>, retry: RetryConfig) -> Self {
        let base_url = catalog_url.trim_end_matches('/').to_string();
        IcebergCatalogClient {
            client: Client::new(),
            base_url,
            auth_token: auth_token.map(RedactedString::new),
            retry,
        }
    }

    /// List all namespaces in the catalog.
    ///
    /// Multi-level namespace paths are joined with `.` (e.g., `["a", "b"]`
    /// becomes `"a.b"`).
    pub async fn list_namespaces(&self) -> Result<Vec<String>, IcebergError> {
        let url = format!("{}/v1/namespaces", self.base_url);
        let resp: ListNamespacesResponse = self.get(&url).await?;

        let namespaces: Vec<String> = resp
            .namespaces
            .into_iter()
            .map(|parts| parts.join("."))
            .collect();

        debug!(count = namespaces.len(), "listed Iceberg namespaces");
        Ok(namespaces)
    }

    /// List all tables in a namespace.
    ///
    /// `namespace` should be the dot-joined namespace string (e.g., `"my_db"`
    /// or `"a.b"`). Multi-level namespaces are encoded with the `%1F`
    /// separator per the Iceberg REST spec.
    pub async fn list_tables(&self, namespace: &str) -> Result<Vec<TableIdentifier>, IcebergError> {
        let encoded_ns = encode_namespace(namespace);
        let url = format!("{}/v1/namespaces/{}/tables", self.base_url, encoded_ns);
        let resp: ListTablesResponse = self.get(&url).await?;

        let tables: Vec<TableIdentifier> = resp
            .identifiers
            .into_iter()
            .map(|id| TableIdentifier {
                namespace: id.namespace.join("."),
                name: id.name,
            })
            .collect();

        debug!(
            namespace,
            count = tables.len(),
            "listed Iceberg tables in namespace"
        );
        Ok(tables)
    }

    /// Internal GET with retry on transient errors.
    async fn get<T: serde::de::DeserializeOwned>(&self, url: &str) -> Result<T, IcebergError> {
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
                return Err(IcebergError::RateLimited);
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
                return Err(IcebergError::Api {
                    status,
                    message: body,
                });
            }

            return resp.json().await.map_err(|e| {
                IcebergError::UnexpectedResponse(format!("failed to parse response: {e}"))
            });
        }

        unreachable!("retry loop should always return")
    }
}

/// Encode a dot-separated namespace for the URL path.
///
/// The Iceberg REST spec uses `%1F` (unit separator) to encode
/// multi-level namespaces in URL paths. Single-level namespaces
/// are passed through as-is.
fn encode_namespace(namespace: &str) -> String {
    namespace.replace('.', "%1F")
}

/// Computes exponential backoff from RetryConfig.
fn retry_backoff(cfg: &RetryConfig, attempt: u32) -> Duration {
    let base = (cfg.initial_backoff_ms as f64) * cfg.backoff_multiplier.powi(attempt as i32);
    let ms = base.min(cfg.max_backoff_ms as f64) as u64;
    Duration::from_millis(ms)
}

impl std::fmt::Debug for IcebergCatalogClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergCatalogClient")
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
        let client = IcebergCatalogClient::new(
            "https://iceberg.example.com".into(),
            Some("secret_token_value".into()),
        );
        let debug = format!("{client:?}");
        assert!(!debug.contains("secret_token_value"));
        assert!(debug.contains("***"));
    }

    #[test]
    fn test_debug_without_token() {
        let client = IcebergCatalogClient::new("https://localhost:8181".into(), None);
        let debug = format!("{client:?}");
        assert!(debug.contains("localhost"));
        assert!(!debug.contains("secret"));
    }

    #[test]
    fn test_base_url_trailing_slash_stripped() {
        let client = IcebergCatalogClient::new("https://iceberg.example.com/".into(), None);
        assert_eq!(client.base_url, "https://iceberg.example.com");
    }

    #[test]
    fn test_encode_namespace_single_level() {
        assert_eq!(encode_namespace("my_database"), "my_database");
    }

    #[test]
    fn test_encode_namespace_multi_level() {
        assert_eq!(encode_namespace("a.b.c"), "a%1Fb%1Fc");
    }

    #[test]
    fn test_list_namespaces_response_deserialize() {
        let json = r#"{
            "namespaces": [
                ["default"],
                ["production"],
                ["analytics", "staging"]
            ]
        }"#;

        let resp: ListNamespacesResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.namespaces.len(), 3);
        assert_eq!(resp.namespaces[0], vec!["default"]);
        assert_eq!(resp.namespaces[1], vec!["production"]);
        assert_eq!(resp.namespaces[2], vec!["analytics", "staging"]);
    }

    #[test]
    fn test_list_namespaces_response_empty() {
        let json = r#"{"namespaces": []}"#;
        let resp: ListNamespacesResponse = serde_json::from_str(json).unwrap();
        assert!(resp.namespaces.is_empty());
    }

    #[test]
    fn test_list_tables_response_deserialize() {
        let json = r#"{
            "identifiers": [
                {
                    "namespace": ["default"],
                    "name": "orders"
                },
                {
                    "namespace": ["default"],
                    "name": "customers"
                }
            ]
        }"#;

        let resp: ListTablesResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.identifiers.len(), 2);
        assert_eq!(resp.identifiers[0].namespace, vec!["default"]);
        assert_eq!(resp.identifiers[0].name, "orders");
        assert_eq!(resp.identifiers[1].name, "customers");
    }

    #[test]
    fn test_list_tables_response_empty() {
        let json = r#"{"identifiers": []}"#;
        let resp: ListTablesResponse = serde_json::from_str(json).unwrap();
        assert!(resp.identifiers.is_empty());
    }

    #[test]
    fn test_list_tables_response_multi_level_namespace() {
        let json = r#"{
            "identifiers": [
                {
                    "namespace": ["analytics", "staging"],
                    "name": "events"
                }
            ]
        }"#;

        let resp: ListTablesResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.identifiers[0].namespace, vec!["analytics", "staging"]);
        assert_eq!(resp.identifiers[0].name, "events");
    }

    #[test]
    fn test_table_identifier_equality() {
        let a = TableIdentifier {
            namespace: "default".into(),
            name: "orders".into(),
        };
        let b = TableIdentifier {
            namespace: "default".into(),
            name: "orders".into(),
        };
        let c = TableIdentifier {
            namespace: "default".into(),
            name: "customers".into(),
        };

        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn test_table_identifier_serialization() {
        let id = TableIdentifier {
            namespace: "production".into(),
            name: "events".into(),
        };

        let json = serde_json::to_string(&id).unwrap();
        let deserialized: TableIdentifier = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, id);
    }

    /// Verify that the flattening logic for namespace response works.
    #[test]
    fn test_namespace_flattening() {
        let raw_namespaces = vec![
            vec!["default".to_string()],
            vec!["analytics".to_string(), "staging".to_string()],
            vec!["a".to_string(), "b".to_string(), "c".to_string()],
        ];

        let flattened: Vec<String> = raw_namespaces
            .into_iter()
            .map(|parts| parts.join("."))
            .collect();

        assert_eq!(flattened, vec!["default", "analytics.staging", "a.b.c"]);
    }

    /// Verify that TableIdentifierResponse maps correctly.
    #[test]
    fn test_table_identifier_response_mapping() {
        let resp = TableIdentifierResponse {
            namespace: vec!["prod".to_string(), "raw".to_string()],
            name: "orders".to_string(),
        };

        let mapped = TableIdentifier {
            namespace: resp.namespace.join("."),
            name: resp.name,
        };

        assert_eq!(mapped.namespace, "prod.raw");
        assert_eq!(mapped.name, "orders");
    }
}
