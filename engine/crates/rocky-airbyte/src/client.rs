//! Async Airbyte REST API client.
//!
//! Wraps the Airbyte Configuration API v1 to list and inspect connections.
//! Connections are Airbyte's unit of replication (source + destination + streams).

use std::time::Duration;

use chrono::{DateTime, Utc};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use reqwest::Client;

/// Percent-encode a user-supplied URL path component.
///
/// Uses RFC 3986 unreserved chars minus `.` — `-`, `_`, `~`, and ASCII
/// alphanumerics pass through; `.` stays encoded so `..` cannot traverse
/// out of the parent path. Every other byte (`/`, `?`, `#`, `%`, spaces,
/// etc.) is percent-encoded. Apply at every interpolation site that takes
/// caller-controlled input (connection IDs, etc.).
fn encode_path_segment(segment: &str) -> String {
    const PATH_SAFE: &AsciiSet = &NON_ALPHANUMERIC.remove(b'-').remove(b'_').remove(b'~');
    utf8_percent_encode(segment, PATH_SAFE).to_string()
}

/// Build the shared `reqwest::Client` used for every Airbyte API call.
/// `Client::new()` left both connect and request timeouts unset — a
/// stalled connection would block `rocky run` forever.
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

#[derive(Error)]
pub enum AirbyteError {
    #[error("HTTP error: {}", rocky_core::secret_registry::render_placeholders(&.0.to_string()))]
    Http(#[from] reqwest::Error),

    #[error(
        "API error ({status}): {}",
        rocky_core::secret_registry::render_placeholders(message)
    )]
    Api { status: u16, message: String },

    #[error("unexpected response format: {}", rocky_core::secret_registry::render_placeholders(&.0.to_string()))]
    UnexpectedResponse(String),

    #[error("rate limited -- retry after backoff")]
    RateLimited,

    #[error(
        "Airbyte API returned a `next` cursor on a different origin: {} (base: {}). Refusing to follow — the Bearer token would otherwise be sent to an unrelated host.",
        rocky_core::secret_registry::render_placeholders(next),
        rocky_core::secret_registry::render_placeholders(base)
    )]
    UntrustedNextUrl { base: String, next: String },
}

/// `Debug` prints the rendered `Display` text. A derived `Debug` would print
/// the plaintext of every field and wrapped error (#1919).
impl std::fmt::Debug for AirbyteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        rocky_core::secret_registry::fmt_rendered_debug(f, "AirbyteError", self)
    }
}

/// Returns `true` when `candidate` is on the same scheme + host + port as
/// `base`. Used to gate paginated `next` URLs so a malicious or
/// compromised API can't redirect the client — and the Bearer token that
/// travels with it — to an arbitrary host.
fn same_origin(base: &str, candidate: &str) -> bool {
    let Ok(base) = url::Url::parse(base) else {
        return false;
    };
    let Ok(cand) = url::Url::parse(candidate) else {
        return false;
    };
    base.scheme() == cand.scheme()
        && base.host_str().is_some()
        && base.host_str() == cand.host_str()
        && base.port_or_known_default() == cand.port_or_known_default()
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

/// One page of `GET /v1/jobs`. Only the first page is read: the newest jobs
/// are all the sync state needs.
#[derive(Debug, Clone, Deserialize)]
pub struct JobsListResponse {
    pub data: Vec<Job>,
}

/// An Airbyte job (one sync or reset of one connection).
///
/// `status` and `job_type` stay strings, so a value Airbyte adds later does
/// not fail the whole read. [`job_is_running`] treats an unknown status as
/// running, the safe side for a wait. The times stay strings too: see
/// [`parse_job_time`].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Job {
    pub job_id: i64,
    pub status: String,
    pub job_type: String,
    #[serde(default)]
    pub connection_id: Option<String>,
    #[serde(default)]
    pub start_time: Option<String>,
    #[serde(default)]
    pub last_updated_at: Option<String>,
}

/// `true` unless the job has ended. Airbyte's ended states are `succeeded`,
/// `failed` and `cancelled`. `pending`, `running` and `incomplete` (an attempt
/// failed and Airbyte will try again) are running. A status this code does not
/// know is also taken as running: the wait then lasts longer, but it never
/// reads while a job may still write.
pub fn job_is_running(job: &Job) -> bool {
    !matches!(job.status.as_str(), "succeeded" | "failed" | "cancelled")
}

/// Parse a job time (`startTime`, `lastUpdatedAt`).
///
/// # Errors
///
/// Airbyte sends RFC 3339 (`2026-10-21T10:00:00Z`), but the API spec names no
/// format. A value that is not RFC 3339 is an error, never a guess: a wrong
/// time could make an old sync look like a new one.
pub fn parse_job_time(raw: &str) -> Result<DateTime<Utc>, AirbyteError> {
    DateTime::parse_from_rfc3339(raw.trim())
        .map(|t| t.with_timezone(&Utc))
        .map_err(|e| {
            AirbyteError::UnexpectedResponse(format!("job time {raw:?} is not RFC 3339: {e}"))
        })
}

/// Why [`AirbyteClient::post_sync_job`] did not start a sync.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncRequestError {
    /// HTTP 403: the token cannot start jobs. Holds the response body.
    Forbidden(String),
    /// The request was refused or could not be sent. No sync started.
    Failed(String),
    /// HTTP 409: Airbyte says the connection is busy (a sync already runs).
    Conflict(String),
    /// The request may have reached Airbyte (a timeout, a 5xx, a connection
    /// lost mid-request). A sync may have started.
    Unknown(String),
}

/// Longest response body kept in an error text.
const MAX_ERROR_BODY_BYTES: usize = 512;

/// Cut a response body for an error text, and render any resolved secret as
/// its `${NAME}` placeholder.
fn error_body(body: &str) -> String {
    let body = rocky_core::secret_registry::render_placeholders(body);
    if body.len() <= MAX_ERROR_BODY_BYTES {
        return body;
    }
    let mut end = MAX_ERROR_BODY_BYTES;
    while end > 0 && !body.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…(truncated)", &body[..end])
}

/// The server's clock from a response's `Date` header (`None` when absent or
/// unparseable). HTTP servers send the IMF-fixdate form
/// (`Wed, 21 Oct 2026 10:00:00 GMT`), which RFC 2822 parsing accepts.
fn parse_date_header(resp: &reqwest::Response) -> Option<DateTime<Utc>> {
    let raw = resp.headers().get(reqwest::header::DATE)?.to_str().ok()?;
    parse_date_value(raw)
}

fn parse_date_value(raw: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc2822(raw.trim())
        .ok()
        .map(|t| t.with_timezone(&Utc))
}

/// How many of a connection's newest jobs one status read fetches.
pub const JOBS_PAGE_LIMIT: u32 = 25;

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

/// Async Airbyte REST client with optional Bearer auth and configurable retry.
///
/// Targets the Airbyte Configuration API v1:
/// - `GET /v1/connections` -- list all connections
/// - `GET /v1/connections/{connectionId}` -- get a single connection
/// - `GET /v1/jobs?connectionId=...` -- the newest jobs of one connection
/// - `POST /v1/jobs` with `jobType = "sync"` -- start one sync
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
            client: build_http_client(),
            base_url,
            auth_token: auth_token.map(RedactedString::new),
            retry,
        }
    }

    /// Swap the HTTP client, so a test can set short timeouts.
    #[cfg(test)]
    pub(crate) fn with_http_client(mut self, client: Client) -> Self {
        self.client = client;
        self
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
                    if !same_origin(&self.base_url, &next_url) {
                        return Err(AirbyteError::UntrustedNextUrl {
                            base: self.base_url.clone(),
                            next: next_url,
                        });
                    }
                    url = next_url;
                }
                _ => break,
            }
        }

        debug!(count = all.len(), "listed Airbyte connections");
        Ok(all)
    }

    /// Fetch a single connection by ID.
    ///
    /// `id` is percent-encoded before being interpolated into the URL path,
    /// so caller-supplied IDs containing `/`, `?`, `#`, `..`, or `%` cannot
    /// escape the `/v1/connections/` prefix.
    pub async fn get_connection(&self, id: &str) -> Result<Connection, AirbyteError> {
        let url = format!(
            "{}/v1/connections/{}",
            self.base_url,
            encode_path_segment(id)
        );
        self.get(&url).await
    }

    /// The newest jobs of one connection, of every type, newest first
    /// (`GET /v1/jobs?connectionId=<id>&limit=<n>&orderBy=createdAt|DESC`),
    /// plus the `Date` header of the answer.
    ///
    /// Every job type is read, not only `sync`: a reset or clear job also
    /// writes to the destination, so a caller that waits must see it running.
    /// The connection id is percent-encoded into the query string.
    pub async fn list_connection_jobs(
        &self,
        connection_id: &str,
    ) -> Result<(Vec<Job>, Option<DateTime<Utc>>), AirbyteError> {
        let url = format!(
            "{}/v1/jobs?connectionId={}&limit={JOBS_PAGE_LIMIT}&orderBy=createdAt%7CDESC",
            self.base_url,
            encode_path_segment(connection_id)
        );
        let (resp, date): (JobsListResponse, _) = self.get_dated(&url).await?;
        Ok((resp.data, date))
    }

    /// Ask Airbyte to start one sync on a connection: `POST /v1/jobs` with
    /// `{"connectionId": "<id>", "jobType": "sync"}`.
    ///
    /// Always a `sync` job, never a `reset` or clear job. At most one request
    /// reaches Airbyte for each call:
    ///
    /// - A connect error or a 429 means the request was not taken, so those
    ///   retry (within the retry policy).
    /// - A timeout, a 5xx or a connection lost mid-request may mean the job
    ///   started, so those do not retry. They return
    ///   [`SyncRequestError::Unknown`].
    /// - A 403 returns [`SyncRequestError::Forbidden`].
    /// - A 409 returns [`SyncRequestError::Conflict`]: a sync already runs.
    ///
    /// `Ok` carries the `Date` header of the accepting answer.
    pub async fn post_sync_job(
        &self,
        connection_id: &str,
    ) -> Result<Option<DateTime<Utc>>, SyncRequestError> {
        let url = format!("{}/v1/jobs", self.base_url);
        let body = serde_json::json!({ "connectionId": connection_id, "jobType": "sync" });
        for attempt in 0..=self.retry.max_retries {
            debug!(connection_id, attempt, "POST sync job");
            let mut req = self.client.post(&url).json(&body);
            if let Some(token) = &self.auth_token {
                req = req.bearer_auth(token.expose());
            }
            let resp = match req.send().await {
                Ok(r) => r,
                Err(e) if e.is_connect() && attempt < self.retry.max_retries => {
                    let backoff = retry_backoff(&self.retry, attempt);
                    warn!(
                        attempt = attempt + 1,
                        backoff_ms = backoff.as_millis() as u64,
                        error = %AirbyteError::from(e),
                        "could not connect for the sync request, retrying"
                    );
                    tokio::time::sleep(backoff).await;
                    continue;
                }
                Err(e) if e.is_timeout() => {
                    return Err(SyncRequestError::Unknown(format!(
                        "no answer to the sync request: {}",
                        AirbyteError::from(e)
                    )));
                }
                // A connect error means the request never went out. Any other
                // send error (a connection lost mid-request) may have reached
                // Airbyte.
                Err(e) if e.is_connect() => {
                    return Err(SyncRequestError::Failed(AirbyteError::from(e).to_string()));
                }
                Err(e) => {
                    return Err(SyncRequestError::Unknown(format!(
                        "no answer to the sync request: {}",
                        AirbyteError::from(e)
                    )));
                }
            };

            let status = resp.status();
            let server_date = parse_date_header(&resp);
            if status == 429 {
                if attempt < self.retry.max_retries {
                    let backoff = retry_backoff(&self.retry, attempt);
                    warn!(
                        attempt = attempt + 1,
                        backoff_ms = backoff.as_millis() as u64,
                        "rate limited on the sync request, retrying"
                    );
                    tokio::time::sleep(backoff).await;
                    continue;
                }
                return Err(SyncRequestError::Failed(
                    AirbyteError::RateLimited.to_string(),
                ));
            }
            let text = resp.text().await.unwrap_or_default();
            if status == 403 {
                return Err(SyncRequestError::Forbidden(error_body(&text)));
            }
            if status.is_server_error() {
                return Err(SyncRequestError::Unknown(format!(
                    "HTTP {status} to the sync request: {}",
                    error_body(&text)
                )));
            }
            if status == 409 {
                return Err(SyncRequestError::Conflict(format!(
                    "HTTP 409 to the sync request: {}",
                    error_body(&text)
                )));
            }
            if !status.is_success() {
                return Err(SyncRequestError::Failed(format!(
                    "HTTP {status} to the sync request: {}",
                    error_body(&text)
                )));
            }
            match serde_json::from_str::<Job>(&text) {
                Ok(job) => debug!(
                    connection_id,
                    job_id = job.job_id,
                    "Airbyte started a sync job"
                ),
                Err(e) => debug!(connection_id, error = %e, "sync job accepted; body not parsed"),
            }
            return Ok(server_date);
        }
        unreachable!("retry loop should always return")
    }

    /// Internal GET with retry on transient errors.
    async fn get<T: serde::de::DeserializeOwned>(&self, url: &str) -> Result<T, AirbyteError> {
        Ok(self.get_dated(url).await?.0)
    }

    /// [`Self::get`], plus the `Date` header of the answer.
    async fn get_dated<T: serde::de::DeserializeOwned>(
        &self,
        url: &str,
    ) -> Result<(T, Option<DateTime<Utc>>), AirbyteError> {
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

            let date = parse_date_header(&resp);
            let value = resp.json().await.map_err(|e| {
                AirbyteError::UnexpectedResponse(format!("failed to parse response: {e}"))
            })?;
            return Ok((value, date));
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
    fn test_encode_path_segment_alphanumeric_passthrough() {
        // Plain IDs are unchanged.
        assert_eq!(encode_path_segment("conn_abc123"), "conn_abc123");
    }

    #[test]
    fn test_encode_path_segment_unsafe_chars() {
        // `.` is encoded so `..` cannot traverse out of `/v1/connections/`.
        assert_eq!(encode_path_segment(".."), "%2E%2E");
        // `/` is encoded so callers cannot forge a different path.
        assert_eq!(encode_path_segment("a/b"), "a%2Fb");
        // Query-injection vectors (`?`, `#`) are encoded.
        assert_eq!(encode_path_segment("a?b"), "a%3Fb");
        assert_eq!(encode_path_segment("a#b"), "a%23b");
        // Pre-encoded payloads have their literal `%` re-encoded so the
        // server sees the original byte stream.
        assert_eq!(encode_path_segment("%2F"), "%252F");
    }

    /// Regression guard: the URL `get_connection` builds for a hostile
    /// `id` must percent-encode every metacharacter so the server sees a
    /// single path segment under `/v1/connections/` rather than a forged
    /// path or a query-string injection.
    #[test]
    fn test_get_connection_url_encodes_id() {
        let raw_id = "conn/../admin?x=1#frag%2F";
        let encoded = encode_path_segment(raw_id);
        let url = format!("https://api.airbyte.com/v1/connections/{encoded}");

        let suffix = url
            .strip_prefix("https://api.airbyte.com/v1/connections/")
            .unwrap();

        // The encoded segment is what the server actually receives.
        assert_eq!(suffix, "conn%2F%2E%2E%2Fadmin%3Fx%3D1%23frag%252F");

        // Defensive: ensure no raw metacharacter survived the encoding.
        for c in ['/', '?', '#'] {
            assert!(!suffix.contains(c), "raw {c:?} leaked into URL: {suffix}");
        }
        assert!(!suffix.contains(".."), "raw `..` leaked into URL: {suffix}");
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
    fn same_origin_accepts_matching_host_and_scheme() {
        assert!(same_origin(
            "https://api.airbyte.com",
            "https://api.airbyte.com/v1/connections?after=x"
        ));
    }

    #[test]
    fn same_origin_rejects_different_host() {
        assert!(!same_origin(
            "https://api.airbyte.com",
            "https://evil.example.com/v1/connections?after=x"
        ));
    }

    #[test]
    fn same_origin_rejects_scheme_downgrade() {
        assert!(!same_origin(
            "https://api.airbyte.com",
            "http://api.airbyte.com/v1/connections?after=x"
        ));
    }

    #[test]
    fn same_origin_rejects_unparseable() {
        assert!(!same_origin("not a url", "https://api.airbyte.com"));
        assert!(!same_origin("https://api.airbyte.com", "not a url"));
    }

    #[test]
    fn same_origin_normalizes_default_port() {
        assert!(same_origin(
            "https://api.airbyte.com:443",
            "https://api.airbyte.com/v1/connections"
        ));
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
