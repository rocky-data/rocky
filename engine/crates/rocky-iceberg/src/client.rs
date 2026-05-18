//! Async Iceberg REST Catalog API client.
//!
//! Wraps the [Iceberg REST Catalog API](https://iceberg.apache.org/spec/#rest-catalog)
//! to list namespaces and tables, load and create / drop tables, and commit
//! multi-table transactions. This is a metadata-only client -- it does not
//! read Iceberg data files.

use std::collections::HashMap;
use std::time::Duration;

use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use reqwest::{Client, RequestBuilder};

/// Build the shared `reqwest::Client` used for every Iceberg REST
/// catalog call. `Client::new()` left both connect and request
/// timeouts unset — a stalled connection would block `rocky run`
/// forever.
fn build_http_client() -> Client {
    Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(120))
        .build()
        .expect("reqwest client builder is infallible with these options")
}
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

/// Response body for `GET /v1/namespaces/{ns}/tables/{name}` — the Iceberg
/// REST "load table" endpoint.
///
/// Wraps the [`TableMetadata`] payload that the spec returns. Other
/// top-level fields (`metadata-location`, `config`, signed storage
/// credentials) are intentionally not modelled here because PR-2 only
/// consumes the schema.
#[derive(Debug, Clone, Deserialize)]
pub struct LoadTableResponse {
    /// The table metadata document, as defined by the Iceberg spec.
    pub metadata: TableMetadata,
}

/// Partial view of the Iceberg `TableMetadata` document.
///
/// The on-the-wire document is large (snapshots, manifests, partition
/// specs, sort orders, refs, properties); this struct only models the
/// fields needed to project the current column schema onto a
/// `TableSchema` and to enumerate branch / tag references. Unknown
/// fields are ignored by serde's default behaviour.
#[derive(Debug, Clone, Deserialize)]
pub struct TableMetadata {
    /// Schema id of the current schema, used to pick the right entry
    /// out of `schemas`. The field is camel-cased on the wire as
    /// `current-schema-id`.
    #[serde(rename = "current-schema-id")]
    pub current_schema_id: i64,
    /// The set of schemas the table has ever had; the entry with
    /// `schema-id == current_schema_id` is the active one.
    pub schemas: Vec<IcebergSchema>,
    /// Named branch / tag references (keyed by ref name).
    ///
    /// Per the Iceberg v2 spec, `refs` is a top-level map embedded in
    /// table metadata that names every branch (mutable head) and tag
    /// (immutable label) attached to the table. Older catalogs may omit
    /// the field entirely; serde defaults the missing case to an empty
    /// map.
    #[serde(default)]
    pub refs: HashMap<String, SnapshotReference>,
}

/// One entry in [`TableMetadata::schemas`].
#[derive(Debug, Clone, Deserialize)]
pub struct IcebergSchema {
    /// Unique identifier for this schema revision.
    #[serde(rename = "schema-id")]
    pub schema_id: i64,
    /// Columns in declaration order.
    pub fields: Vec<IcebergField>,
}

/// One column in an [`IcebergSchema`].
///
/// `type` is left as a [`serde_json::Value`] because the Iceberg spec
/// allows it to be either a primitive type name (`"long"`,
/// `"timestamp"`) or a nested JSON object describing a list/map/struct.
/// Distillation into a string happens at consume time.
#[derive(Debug, Clone, Deserialize)]
pub struct IcebergField {
    /// Column name as the catalog stores it.
    pub name: String,
    /// Whether the column is required (i.e. `NOT NULL`). The catalog-side
    /// `nullable` projection is the inverse of this flag.
    pub required: bool,
    /// Type representation; either a primitive string or a JSON object.
    #[serde(rename = "type")]
    pub type_repr: serde_json::Value,
}

/// An entry in [`TableMetadata::refs`] — a named branch or tag.
///
/// Iceberg's spec models branches and tags as `SnapshotReference`
/// entries embedded in table metadata. Each carries the snapshot id it
/// points at and a `type` discriminator (`"branch"` or `"tag"`). Other
/// fields (`max-ref-age-ms`, `min-snapshots-to-keep`, etc.) are
/// retention hints the caller does not need to project, so they are
/// intentionally not modelled.
#[derive(Debug, Clone, Deserialize)]
pub struct SnapshotReference {
    /// The snapshot id this reference points at.
    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i64,
    /// Reference kind: `"branch"` for a mutable head, `"tag"` for an
    /// immutable label. Left as a string here so unknown future
    /// variants survive deserialisation; the catalog-agnostic
    /// projection in `IcebergCatalogClientAdapter::list_branches`
    /// classifies it.
    #[serde(rename = "type")]
    pub kind: String,
}

/// Request body for `POST /v1/namespaces/{ns}/tables`.
///
/// The Iceberg REST `CreateTableRequest` carries a name and a schema at
/// minimum; partition specs, sort orders, table properties, location
/// hints, and stage-create flags are spec-optional and intentionally
/// not modelled until a caller needs them. The fields are flat-cased to
/// match the spec's wire shape (`name`, `schema`).
#[derive(Debug, Clone, Serialize)]
pub struct CreateTableRequest {
    /// Unqualified table name. The catalog scopes it to the
    /// `{ns}`-path namespace under which the request is issued.
    pub name: String,
    /// Iceberg schema (fields + types + schema-id).
    pub schema: CreateTableSchema,
}

/// Schema payload inside [`CreateTableRequest`].
#[derive(Debug, Clone, Serialize)]
pub struct CreateTableSchema {
    /// Always `"struct"` for a top-level table schema. The spec models a
    /// schema as a struct of nested fields; the type tag is required.
    #[serde(rename = "type")]
    pub schema_type: &'static str,
    /// Schema-id; first revision is `0` by convention.
    #[serde(rename = "schema-id")]
    pub schema_id: i64,
    /// Columns in declaration order.
    pub fields: Vec<CreateTableField>,
}

/// One column inside [`CreateTableSchema`].
#[derive(Debug, Clone, Serialize)]
pub struct CreateTableField {
    /// Stable field id. Assigned sequentially from 1 by the caller.
    pub id: i64,
    /// Column name.
    pub name: String,
    /// Whether the column is required (`NOT NULL`).
    pub required: bool,
    /// Iceberg type. Primitive types are flat strings (`"long"`,
    /// `"string"`, `"double"`); nested types are a JSON object. The
    /// catalog-agnostic `TableSchema` only carries a string, so this
    /// field always serialises as a JSON string when produced from a
    /// reverse conversion — adapters that have richer schema
    /// information should call this method directly.
    #[serde(rename = "type")]
    pub type_repr: serde_json::Value,
}

/// Request body for `POST /v1/transactions/commit`.
///
/// The spec wraps a list of [`CommitTableRequest`] entries. Each entry
/// targets one table and carries the CAS pre-condition (`requirements`)
/// and the metadata mutation (`updates`).
#[derive(Debug, Clone, Serialize)]
pub struct CommitTransactionRequest {
    /// One entry per table in the transaction.
    #[serde(rename = "table-changes")]
    pub table_changes: Vec<CommitTableRequest>,
}

/// One table's contribution to a `POST /v1/transactions/commit`.
#[derive(Debug, Clone, Serialize)]
pub struct CommitTableRequest {
    /// Fully qualified table identifier (namespace parts + name).
    pub identifier: CommitTableIdentifier,
    /// Pre-conditions the commit asserts. Spec example: an
    /// `assert-ref-snapshot-id` on `main` to pin the base snapshot.
    pub requirements: Vec<serde_json::Value>,
    /// Mutations the commit applies. Spec example: `set-snapshot-ref`
    /// advancing the `main` branch to a new snapshot.
    pub updates: Vec<serde_json::Value>,
}

/// Wire-format identifier inside [`CommitTableRequest`].
#[derive(Debug, Clone, Serialize)]
pub struct CommitTableIdentifier {
    /// Namespace as a slice of parts.
    pub namespace: Vec<String>,
    /// Table name.
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
    pub fn new(catalog_url: &str, auth_token: Option<String>) -> Self {
        Self::with_retry(catalog_url, auth_token, RetryConfig::default())
    }

    /// Create a new client with custom retry configuration.
    pub fn with_retry(catalog_url: &str, auth_token: Option<String>, retry: RetryConfig) -> Self {
        let base_url = catalog_url.trim_end_matches('/').to_string();
        IcebergCatalogClient {
            client: build_http_client(),
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

    /// Load a single table's metadata.
    ///
    /// Targets `GET /v1/namespaces/{ns}/tables/{name}`, the Iceberg REST
    /// "load table" endpoint. The full [`LoadTableResponse`] is returned;
    /// callers that only want the column-level schema should reach into
    /// [`LoadTableResponse::metadata`] and select the schema entry whose
    /// `schema-id` matches `current-schema-id`.
    ///
    /// `namespace` is the multi-level namespace as a slice of parts;
    /// the parts are percent-encoded and joined with the spec-required
    /// `%1F` separator. `name` is the unqualified table name and is
    /// percent-encoded individually so caller-supplied names cannot
    /// inject path-altering bytes into the URL.
    pub async fn load_table(
        &self,
        namespace: &[String],
        name: &str,
    ) -> Result<LoadTableResponse, IcebergError> {
        let encoded_ns = encode_namespace_parts(namespace);
        let encoded_name = utf8_percent_encode(name, PATH_SAFE).to_string();
        let url = format!(
            "{}/v1/namespaces/{}/tables/{}",
            self.base_url, encoded_ns, encoded_name
        );
        let resp: LoadTableResponse = self.get(&url).await?;
        debug!(
            namespace = ?namespace,
            name,
            "loaded Iceberg table metadata"
        );
        Ok(resp)
    }

    /// Create a new table under `namespace`.
    ///
    /// Targets `POST /v1/namespaces/{ns}/tables` with the
    /// [`CreateTableRequest`] body. The catalog returns the freshly
    /// created [`LoadTableResponse`] on success — callers that only
    /// care about the side-effect can discard it. 404 from this
    /// endpoint typically means the parent namespace is missing rather
    /// than a table-not-found; the adapter rewrites the default 404
    /// mapping accordingly.
    pub async fn create_table(
        &self,
        namespace: &[String],
        request: &CreateTableRequest,
    ) -> Result<LoadTableResponse, IcebergError> {
        let encoded_ns = encode_namespace_parts(namespace);
        let url = format!("{}/v1/namespaces/{}/tables", self.base_url, encoded_ns);
        let resp: LoadTableResponse = self.post(&url, request).await?;
        debug!(
            namespace = ?namespace,
            name = %request.name,
            "created Iceberg table"
        );
        Ok(resp)
    }

    /// Drop a table from the catalog.
    ///
    /// Targets `DELETE /v1/namespaces/{ns}/tables/{name}`. The Iceberg
    /// REST spec returns 204 No Content on success; callers should not
    /// assume the underlying object storage is reclaimed (that depends
    /// on the catalog's `purge` policy).
    pub async fn drop_table(&self, namespace: &[String], name: &str) -> Result<(), IcebergError> {
        let encoded_ns = encode_namespace_parts(namespace);
        let encoded_name = utf8_percent_encode(name, PATH_SAFE).to_string();
        let url = format!(
            "{}/v1/namespaces/{}/tables/{}",
            self.base_url, encoded_ns, encoded_name
        );
        self.delete(&url).await?;
        debug!(
            namespace = ?namespace,
            name,
            "dropped Iceberg table"
        );
        Ok(())
    }

    /// Commit a multi-table transaction.
    ///
    /// Targets `POST /v1/transactions/commit`. The catalog applies all
    /// `table-changes` atomically: every requirement passes its CAS
    /// check and every update lands, or nothing does. A 409 response
    /// indicates a concurrent writer landed against the same base
    /// snapshot; the call site is expected to surface this as
    /// [`crate::CatalogError::CommitConflict`] and let the caller retry
    /// after re-reading state.
    pub async fn commit_transaction(
        &self,
        request: &CommitTransactionRequest,
    ) -> Result<(), IcebergError> {
        let url = format!("{}/v1/transactions/commit", self.base_url);
        self.post_no_response(&url, request).await?;
        debug!(
            table_count = request.table_changes.len(),
            "committed Iceberg transaction"
        );
        Ok(())
    }

    /// Internal GET with retry on transient errors.
    ///
    /// Thin wrapper over [`Self::send_with_retry`] specialised for
    /// idempotent GETs: timeouts and connect failures are both safe to
    /// retry because the request cannot have side-effected the server.
    async fn get<T: serde::de::DeserializeOwned>(&self, url: &str) -> Result<T, IcebergError> {
        let resp = self
            .send_with_retry("GET", url, RetryClass::Idempotent, || {
                self.bearer(self.client.get(url))
            })
            .await?;
        Self::parse_json(resp).await
    }

    /// Internal POST with retry, returning the deserialised body.
    ///
    /// POST is treated as non-idempotent: timeouts after the request
    /// landed cannot be safely retried (the server may have applied the
    /// effect), so only `is_connect()` failures are retried at the
    /// transport layer. 429 and 5xx are still retried — 429 is the
    /// server explicitly inviting retry, and 5xx retry on POST is
    /// industry-common; the trade-off is documented at-least-once
    /// semantics on the call site (`commit_transaction` specifically
    /// notes this).
    async fn post<TReq: serde::Serialize, TResp: serde::de::DeserializeOwned>(
        &self,
        url: &str,
        body: &TReq,
    ) -> Result<TResp, IcebergError> {
        let resp = self
            .send_with_retry("POST", url, RetryClass::NonIdempotent, || {
                self.bearer(self.client.post(url).json(body))
            })
            .await?;
        Self::parse_json(resp).await
    }

    /// Internal POST that discards the response body.
    ///
    /// Same retry semantics as [`Self::post`]; used when the spec returns
    /// 204 No Content or when the caller does not need to consume the
    /// response (`POST /v1/transactions/commit`).
    async fn post_no_response<TReq: serde::Serialize>(
        &self,
        url: &str,
        body: &TReq,
    ) -> Result<(), IcebergError> {
        self.send_with_retry("POST", url, RetryClass::NonIdempotent, || {
            self.bearer(self.client.post(url).json(body))
        })
        .await?;
        Ok(())
    }

    /// Internal DELETE with retry on transient errors.
    ///
    /// DELETE is idempotent by spec: re-issuing it against an
    /// already-deleted resource yields a 404, which the caller is
    /// expected to handle as `TableNotFound`. Same retry envelope as
    /// [`Self::get`].
    async fn delete(&self, url: &str) -> Result<(), IcebergError> {
        self.send_with_retry("DELETE", url, RetryClass::Idempotent, || {
            self.bearer(self.client.delete(url))
        })
        .await?;
        Ok(())
    }

    /// Attach Bearer auth to a request builder when a token is set.
    fn bearer(&self, mut req: RequestBuilder) -> RequestBuilder {
        if let Some(token) = &self.auth_token {
            req = req.bearer_auth(token.expose());
        }
        req
    }

    /// Send a request with retry, returning the raw success response.
    ///
    /// The `build` closure is invoked once per attempt so that POST /
    /// PUT bodies (which `RequestBuilder` consumes by value on `send`)
    /// can be rebuilt cleanly across retries. `class` controls which
    /// transport-level failures are eligible for retry; see
    /// [`RetryClass`].
    ///
    /// The success contract: returns `Ok(resp)` for `2xx`. Returns the
    /// usual [`IcebergError`] for everything else, including
    /// [`IcebergError::RateLimited`] once 429 retries are exhausted.
    /// Non-success bodies are buffered as UTF-8 lossily — the caller
    /// gets the response text as part of [`IcebergError::Api`].
    async fn send_with_retry<F>(
        &self,
        method: &'static str,
        url: &str,
        class: RetryClass,
        mut build: F,
    ) -> Result<reqwest::Response, IcebergError>
    where
        F: FnMut() -> RequestBuilder,
    {
        for attempt in 0..=self.retry.max_retries {
            debug!(method, url, attempt, "sending request");

            let req = build();
            let resp = match req.send().await {
                Ok(r) => r,
                Err(e)
                    if attempt < self.retry.max_retries && is_retriable_send_error(&e, class) =>
                {
                    let backoff = retry_backoff(&self.retry, attempt);
                    warn!(
                        method,
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
                        method,
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
                    method,
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

            return Ok(resp);
        }

        unreachable!("retry loop should always return")
    }

    /// Parse a successful response as JSON; surface deser failures as
    /// [`IcebergError::UnexpectedResponse`] rather than transport errors.
    async fn parse_json<T: serde::de::DeserializeOwned>(
        resp: reqwest::Response,
    ) -> Result<T, IcebergError> {
        resp.json()
            .await
            .map_err(|e| IcebergError::UnexpectedResponse(format!("failed to parse response: {e}")))
    }
}

/// Whether the request is safe to retry after a partial-send failure.
///
/// `Idempotent` (GET, DELETE) tolerates connect and timeout errors —
/// the server either never saw the request or, in the DELETE case,
/// applied it once and a retry is a 404. `NonIdempotent` (POST without
/// idempotency keys) only retries `is_connect()` failures, where the
/// request demonstrably did not reach the server.
#[derive(Debug, Clone, Copy)]
enum RetryClass {
    Idempotent,
    NonIdempotent,
}

/// Classify a `reqwest::Error` as retriable for the given class.
fn is_retriable_send_error(e: &reqwest::Error, class: RetryClass) -> bool {
    match class {
        RetryClass::Idempotent => e.is_connect() || e.is_timeout(),
        RetryClass::NonIdempotent => e.is_connect(),
    }
}

/// Byte set used when percent-encoding individual namespace / table-name
/// parts. Allows the RFC 3986 unreserved chars (`-`, `_`, `~`,
/// alphanumerics) and encodes everything else — including `.`, so `..`
/// traversal cannot escape the catalog prefix.
const PATH_SAFE: &AsciiSet = &NON_ALPHANUMERIC.remove(b'-').remove(b'_').remove(b'~');

/// Encode a dot-separated namespace for the URL path.
///
/// The Iceberg REST spec uses `%1F` (unit separator) to encode multi-level
/// namespaces in URL paths. Each level is also percent-encoded so
/// caller-supplied namespace parts cannot inject `/`, `?`, `#`, `..`, or
/// other path-altering bytes into the URL.
///
/// Order matters: split on `.`, percent-encode each part, then join with
/// the literal `%1F` separator. Encoding after the join would re-encode
/// the `%` of `%1F` to `%25` and break the spec-required separator.
fn encode_namespace(namespace: &str) -> String {
    namespace
        .split('.')
        .map(|part| utf8_percent_encode(part, PATH_SAFE).to_string())
        .collect::<Vec<_>>()
        .join("%1F")
}

/// Encode a multi-part namespace (already split into parts) for the URL
/// path. Mirrors [`encode_namespace`] but accepts the spec-native
/// representation (a slice of parts) instead of a dot-joined string.
pub(crate) fn encode_namespace_parts(parts: &[String]) -> String {
    parts
        .iter()
        .map(|part| utf8_percent_encode(part, PATH_SAFE).to_string())
        .collect::<Vec<_>>()
        .join("%1F")
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
            "https://iceberg.example.com",
            Some("secret_token_value".into()),
        );
        let debug = format!("{client:?}");
        assert!(!debug.contains("secret_token_value"));
        assert!(debug.contains("***"));
    }

    #[test]
    fn test_debug_without_token() {
        let client = IcebergCatalogClient::new("https://localhost:8181", None);
        let debug = format!("{client:?}");
        assert!(debug.contains("localhost"));
        assert!(!debug.contains("secret"));
    }

    #[test]
    fn test_base_url_trailing_slash_stripped() {
        let client = IcebergCatalogClient::new("https://iceberg.example.com/", None);
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
    fn test_encode_namespace_unsafe_chars_in_part() {
        // Each namespace part is percent-encoded so a hostile caller cannot
        // inject `/`, `?`, `#`, or other path-altering bytes via a name.
        // (Path-traversal via `..` is defused at the split: the input `..`
        // becomes two empty parts joined by the `%1F` separator, so the
        // server sees an empty-namespace-segment, not a parent traversal.)
        assert_eq!(
            encode_namespace("a/b"),
            "a%2Fb",
            "raw `/` would forge a different REST endpoint"
        );
        assert_eq!(
            encode_namespace("ns?x=1"),
            "ns%3Fx%3D1",
            "`?` would graft a query string onto the URL"
        );
        assert_eq!(
            encode_namespace("ns#frag"),
            "ns%23frag",
            "`#` would terminate the path on the server side"
        );
        assert_eq!(
            encode_namespace("a%2F"),
            "a%252F",
            "literal `%` round-trips so the server sees the original byte"
        );
    }

    #[test]
    fn test_encode_namespace_dot_split_neutralises_traversal() {
        // `.` is the spec-defined level separator. Splitting on `.` first
        // means `..` decomposes into empty parts joined by `%1F`, so the
        // result cannot represent a parent-directory traversal at the URL
        // path level — `/v1/namespaces/%1F%1Fns/tables` is just an
        // invalid namespace, not `/v1/namespaces/../ns/tables`.
        assert_eq!(encode_namespace(".."), "%1F%1F");
        assert_eq!(encode_namespace("..ns"), "%1F%1Fns");
    }

    #[test]
    fn test_encode_namespace_multi_level_unsafe_each_part_encoded() {
        // Order matters: each part is encoded individually before joining
        // with the literal `%1F` separator. Encoding after the join would
        // re-encode the `%` and break the Iceberg spec separator.
        assert_eq!(
            encode_namespace("a/x.b?y"),
            "a%2Fx%1Fb%3Fy",
            "each part must be encoded; `%1F` separator must remain literal"
        );
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
