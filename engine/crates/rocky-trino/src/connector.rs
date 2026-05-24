//! Trino REST API connector — `POST /v1/statement` + `GET nextUri` polling.
//!
//! Trino's wire protocol (per `trino.io/docs/current/develop/client-protocol.html`)
//! is a state machine, not a one-shot REST call. Every executed statement
//! flows through the same shape:
//!
//! 1. **Submit:** `POST /v1/statement` with the SQL as the **plain-text**
//!    request body (not JSON). Required headers: `X-Trino-User`,
//!    `Authorization`, optionally `X-Trino-Catalog` / `X-Trino-Schema`.
//! 2. **Response:** a `QueryResults` JSON document carrying `id`, `stats`,
//!    optional `columns`, optional `data` (a slice of rows), optional
//!    `nextUri` (the URL to poll for the next page), and optional `error`
//!    (set when the query fails).
//! 3. **Poll:** while `nextUri` is set, `GET nextUri` returns another
//!    `QueryResults` shape. Each page carries a slice of rows; the
//!    caller concatenates them.
//! 4. **Terminal state:** when `nextUri` is absent, the query is in a
//!    terminal state — `stats.state` distinguishes `FINISHED` (success)
//!    from `FAILED` / `CANCELED` (error). On the error path the `error`
//!    field carries the structured failure info.
//!
//! v0 keeps it minimal: aggregate every row as a `Vec<Vec<serde_json::Value>>`,
//! defer Arrow record-batch construction to a follow-up. The polling
//! ladder is identical to the Databricks / BigQuery shape: a fixed
//! sequence for the first 5 polls, exponential growth capped at 5s after.

use std::time::Duration;

use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue};
use serde::Deserialize;
use thiserror::Error;
use tokio::time::Instant;
use tracing::{Instrument, debug, info_span};

use crate::auth::{AuthError, TrinoAuth};

/// Default per-query timeout in seconds. Polling stops with
/// [`TrinoError::Timeout`] when the deadline is reached.
pub const DEFAULT_TIMEOUT_SECS: u64 = 300;

/// Polling delay ladder for the first 5 `nextUri` polls (ms). After step 5,
/// exponential growth bounded by [`MAX_POLL_DELAY_MS`].
const POLL_DELAY_STEPS_MS: [u64; 5] = [50, 100, 250, 500, 1000];
/// Maximum polling delay after exponential growth.
const MAX_POLL_DELAY_MS: u64 = 5000;

fn poll_delay(attempt: usize) -> Duration {
    let delay_ms = if attempt < POLL_DELAY_STEPS_MS.len() {
        POLL_DELAY_STEPS_MS[attempt]
    } else {
        let extra = attempt - POLL_DELAY_STEPS_MS.len();
        (1000u64 * (1u64 << extra.min(3))).min(MAX_POLL_DELAY_MS)
    };
    Duration::from_millis(delay_ms)
}

/// Errors surfaced by the Trino connector.
#[derive(Debug, Error)]
pub enum TrinoError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("Trino auth error: {0}")]
    Auth(#[from] AuthError),

    #[error("Trino HTTP {status}: {message}")]
    HttpStatus { status: u16, message: String },

    #[error("Trino query failed (state {state}, error_code {error_code}): {message}")]
    QueryFailed {
        state: String,
        error_code: i64,
        error_name: String,
        message: String,
    },

    #[error("Trino query timed out after {timeout_secs}s in state {last_state}")]
    Timeout {
        timeout_secs: u64,
        last_state: String,
    },

    #[error("Trino response missing expected field: {0}")]
    MalformedResponse(String),

    #[error(
        "Trino coordinator returned a nextUri pointing at a different origin: {next} (coordinator: {coordinator}). Refusing to follow — the Authorization header would otherwise be sent to an unrelated host."
    )]
    UntrustedNextUri { coordinator: String, next: String },

    #[error(
        "Trino coordinator did not honour the requested Arrow spooled-segment encoding. \
         Apache Arrow IPC is a proposed extension to the spooling protocol \
         (upstream PR trinodb/trino#26365) and is not yet supported by any shipping \
         Trino release (current: 481). Fall back to the JSON polling path via \
         `TrinoClient::execute` until Arrow encoding lands upstream."
    )]
    ArrowEncodingUnavailable,

    #[error("Trino spooled Arrow decode error: {0}")]
    ArrowDecode(String),
}

/// Returns `true` when `candidate` is on the same scheme + host + port as
/// `base`. Used to gate `nextUri` follow-up GETs so a malicious or
/// compromised coordinator can't redirect the polling loop — and the
/// `Authorization` header that travels with it — to an arbitrary host.
///
/// Both URLs must parse and carry a host. Default ports are normalized
/// via [`url::Url::port_or_known_default`].
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

/// Static configuration for a [`TrinoClient`].
#[derive(Debug, Clone)]
pub struct TrinoClientConfig {
    /// Coordinator URL — e.g. `http://localhost:8080` or
    /// `https://trino.example.com`. Trailing slashes are trimmed.
    pub coordinator_url: String,
    /// Override the `X-Trino-User` header. When `None`, the user
    /// embedded in the auth provider (Basic) is used; for JWT auth the
    /// caller MUST supply this.
    pub user: Option<String>,
    /// Default catalog set via `X-Trino-Catalog`. When `None`, queries
    /// must use fully-qualified `<catalog>.<schema>.<table>` references.
    pub default_catalog: Option<String>,
    /// Default schema set via `X-Trino-Schema`. Only meaningful when
    /// `default_catalog` is also set.
    pub default_schema: Option<String>,
    /// Request label tagged on Trino's request headers — surfaces in
    /// `system.runtime.queries` so operators can attribute traffic to
    /// Rocky.
    pub source: String,
    /// Per-query timeout. Polling aborts once this elapses.
    pub timeout: Duration,
}

impl TrinoClientConfig {
    /// Construct a config with sensible v0 defaults.
    #[must_use]
    pub fn new(coordinator_url: impl Into<String>) -> Self {
        Self {
            coordinator_url: coordinator_url.into().trim_end_matches('/').to_string(),
            user: None,
            default_catalog: None,
            default_schema: None,
            source: "rocky".to_string(),
            timeout: Duration::from_secs(DEFAULT_TIMEOUT_SECS),
        }
    }

    /// Set the explicit `X-Trino-User` value.
    #[must_use]
    pub fn with_user(mut self, user: impl Into<String>) -> Self {
        self.user = Some(user.into());
        self
    }

    /// Set the default catalog routed through `X-Trino-Catalog`.
    #[must_use]
    pub fn with_default_catalog(mut self, catalog: impl Into<String>) -> Self {
        self.default_catalog = Some(catalog.into());
        self
    }

    /// Set the default schema routed through `X-Trino-Schema`.
    #[must_use]
    pub fn with_default_schema(mut self, schema: impl Into<String>) -> Self {
        self.default_schema = Some(schema.into());
        self
    }

    /// Override the per-query timeout.
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
}

/// Async REST client driving a Trino coordinator's `/v1/statement` flow.
///
/// One client is cheap; reuse across `execute_*` calls so the underlying
/// `reqwest::Client` keeps a connection pool open to the coordinator.
pub struct TrinoClient {
    http: reqwest::Client,
    config: TrinoClientConfig,
    auth: TrinoAuth,
}

impl std::fmt::Debug for TrinoClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TrinoClient")
            .field("config", &self.config)
            .finish()
    }
}

/// Aggregated rows + columns returned from a finished query.
#[derive(Debug, Clone)]
pub struct TrinoQueryRows {
    pub columns: Vec<TrinoColumnMeta>,
    pub rows: Vec<Vec<serde_json::Value>>,
}

/// Lightweight column metadata extracted from `QueryResults.columns`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrinoColumnMeta {
    pub name: String,
    pub type_signature: String,
}

impl TrinoClient {
    /// Construct a new client. Builds the underlying `reqwest::Client`
    /// with TCP keepalive + HTTP/2 support (matching the BigQuery /
    /// Databricks adapters).
    pub fn new(config: TrinoClientConfig, auth: TrinoAuth) -> Self {
        let http = reqwest::Client::builder()
            .tcp_nodelay(true)
            .tcp_keepalive(Some(Duration::from_secs(30)))
            .pool_max_idle_per_host(16)
            .pool_idle_timeout(Duration::from_secs(300))
            .timeout(Duration::from_secs(120))
            .connect_timeout(Duration::from_secs(10))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self { http, config, auth }
    }

    /// Test-only constructor that takes an explicit `reqwest::Client` so
    /// integration tests can lower the default timeouts.
    #[doc(hidden)]
    pub fn with_http(config: TrinoClientConfig, auth: TrinoAuth, http: reqwest::Client) -> Self {
        Self { http, config, auth }
    }

    /// Read-only access to the active config (mostly for tests).
    pub fn config(&self) -> &TrinoClientConfig {
        &self.config
    }

    /// Crate-internal access to the underlying `reqwest::Client` so
    /// sibling modules (e.g. `arrow_stream`) can drive the same
    /// connection pool / timeout / TLS config without re-building one.
    pub(crate) fn http_client(&self) -> &reqwest::Client {
        &self.http
    }

    /// Crate-internal handle on the shared header set
    /// (`Authorization` + `X-Trino-User` + optional catalog/schema).
    /// Sibling modules add encoding-negotiation headers on top.
    pub(crate) fn base_headers(&self) -> Result<HeaderMap, TrinoError> {
        self.build_headers()
    }

    /// Resolve the `X-Trino-User` value. Errors if neither config nor
    /// auth carry a user.
    fn trino_user(&self) -> Result<&str, TrinoError> {
        if let Some(u) = self.config.user.as_deref() {
            return Ok(u);
        }
        self.auth.trino_user_default().ok_or_else(|| {
            TrinoError::MalformedResponse(
                "no X-Trino-User available — set TrinoClientConfig::user when using JWT auth"
                    .into(),
            )
        })
    }

    fn build_headers(&self) -> Result<HeaderMap, TrinoError> {
        let mut headers = HeaderMap::new();
        let user = self.trino_user()?;
        headers.insert(
            HeaderName::from_static("x-trino-user"),
            HeaderValue::from_str(user).map_err(|_| {
                TrinoError::MalformedResponse(format!(
                    "X-Trino-User contains invalid bytes: {user}"
                ))
            })?,
        );
        headers.insert(
            HeaderName::from_static("x-trino-source"),
            HeaderValue::from_str(&self.config.source)
                .unwrap_or_else(|_| HeaderValue::from_static("rocky")),
        );
        if let Some(c) = self.config.default_catalog.as_deref() {
            headers.insert(
                HeaderName::from_static("x-trino-catalog"),
                HeaderValue::from_str(c).map_err(|_| {
                    TrinoError::MalformedResponse(format!("X-Trino-Catalog invalid: {c}"))
                })?,
            );
        }
        if let Some(s) = self.config.default_schema.as_deref() {
            headers.insert(
                HeaderName::from_static("x-trino-schema"),
                HeaderValue::from_str(s).map_err(|_| {
                    TrinoError::MalformedResponse(format!("X-Trino-Schema invalid: {s}"))
                })?,
            );
        }
        let auth_value = self.auth.authorization_header();
        let mut auth_header = HeaderValue::from_str(&auth_value).map_err(|_| {
            TrinoError::MalformedResponse("Authorization header contained invalid bytes".into())
        })?;
        auth_header.set_sensitive(true);
        headers.insert(AUTHORIZATION, auth_header);
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("text/plain"));
        Ok(headers)
    }

    /// Execute a SQL statement and aggregate every result page into a
    /// single [`TrinoQueryRows`].
    ///
    /// Drives the `/v1/statement` state machine end-to-end:
    ///
    /// 1. POSTs the SQL body to `<coordinator>/v1/statement`.
    /// 2. Polls each `nextUri` until the response stops returning one.
    /// 3. Inspects `stats.state`: `FINISHED` returns rows; `FAILED` /
    ///    `CANCELED` surface as [`TrinoError::QueryFailed`] with the
    ///    structured `error` info.
    ///
    /// Aborts with [`TrinoError::Timeout`] if `config.timeout` elapses
    /// before the query reaches a terminal state.
    pub async fn execute(&self, sql: &str) -> Result<TrinoQueryRows, TrinoError> {
        let span = info_span!("statement.execute", adapter = "trino");
        async move { self.execute_inner(sql).await }
            .instrument(span)
            .await
    }

    async fn execute_inner(&self, sql: &str) -> Result<TrinoQueryRows, TrinoError> {
        let deadline = Instant::now() + self.config.timeout;
        let submit_url = format!("{}/v1/statement", self.config.coordinator_url);
        debug!(coordinator = %self.config.coordinator_url, "submitting Trino statement");

        let headers = self.build_headers()?;
        let resp = self
            .http
            .post(&submit_url)
            .headers(headers.clone())
            .body(sql.to_string())
            .send()
            .await?;
        let mut current = parse_response(resp).await?;

        let mut columns: Vec<TrinoColumnMeta> = Vec::new();
        let mut rows: Vec<Vec<serde_json::Value>> = Vec::new();
        let mut last_state: String;

        let mut attempt = 0_usize;
        loop {
            if let Some(cols) = current.columns.take()
                && columns.is_empty()
            {
                columns = cols.into_iter().map(TrinoColumnMeta::from_wire).collect();
            }
            if let Some(data) = current.data.take() {
                rows.extend(data);
            }
            last_state = current.stats.state.clone();

            // Terminal-state detection: Trino sends `nextUri = None` only
            // when the query is in a terminal state. `state == "FINISHED"`
            // is the happy path; everything else (FAILED, CANCELED,
            // anything unexpected) maps to QueryFailed.
            let Some(next) = current.next_uri.clone() else {
                if current.stats.state == "FINISHED" {
                    return Ok(TrinoQueryRows { columns, rows });
                }
                return Err(query_failed(&current));
            };

            if Instant::now() >= deadline {
                return Err(TrinoError::Timeout {
                    timeout_secs: self.config.timeout.as_secs(),
                    last_state,
                });
            }
            if !same_origin(&self.config.coordinator_url, &next) {
                return Err(TrinoError::UntrustedNextUri {
                    coordinator: self.config.coordinator_url.clone(),
                    next,
                });
            }

            tokio::time::sleep(poll_delay(attempt)).await;
            attempt += 1;

            let resp = self.http.get(&next).headers(headers.clone()).send().await?;
            current = parse_response(resp).await?;
        }
    }
}

/// Build a [`TrinoError::QueryFailed`] from a terminal `QueryResults`
/// document. Falls back to placeholder fields when the server didn't
/// send a structured `error` block.
fn query_failed(qr: &QueryResults) -> TrinoError {
    if let Some(err) = qr.error.as_ref() {
        TrinoError::QueryFailed {
            state: qr.stats.state.clone(),
            error_code: err.error_code.unwrap_or(-1),
            error_name: err.error_name.clone().unwrap_or_default(),
            message: err.message.clone().unwrap_or_else(|| "<no message>".into()),
        }
    } else {
        TrinoError::QueryFailed {
            state: qr.stats.state.clone(),
            error_code: -1,
            error_name: String::new(),
            message: format!(
                "Trino query reached terminal state {} with no error block",
                qr.stats.state
            ),
        }
    }
}

async fn parse_response(resp: reqwest::Response) -> Result<QueryResults, TrinoError> {
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(TrinoError::HttpStatus {
            status: status.as_u16(),
            message: body,
        });
    }
    let body = resp
        .json::<QueryResults>()
        .await
        .map_err(TrinoError::Http)?;
    Ok(body)
}

// -- Wire types -------------------------------------------------------------

/// Top-level shape of a `/v1/statement` response.
///
/// Trino sends the same envelope on the initial POST and on every
/// `nextUri` follow-up. Fields are made optional liberally because the
/// server omits them in many states (no `data` until the first row is
/// produced; no `columns` on a DDL response; no `error` until terminal
/// failure; no `nextUri` once the query is done).
#[derive(Debug, Clone, Deserialize)]
struct QueryResults {
    #[allow(dead_code)]
    id: String,
    #[serde(rename = "nextUri")]
    next_uri: Option<String>,
    columns: Option<Vec<RawColumn>>,
    data: Option<Vec<Vec<serde_json::Value>>>,
    stats: QueryStats,
    error: Option<QueryError>,
}

#[derive(Debug, Clone, Deserialize)]
struct QueryStats {
    /// Coordinator-reported state — one of `QUEUED`, `PLANNING`, `STARTING`,
    /// `RUNNING`, `FINISHING`, `FINISHED`, `FAILED`, `CANCELED`.
    state: String,
}

#[derive(Debug, Clone, Deserialize)]
struct RawColumn {
    name: String,
    /// String form like `varchar(64)`, `bigint`, `timestamp(6)` etc.
    #[serde(rename = "type")]
    type_signature: String,
}

impl TrinoColumnMeta {
    fn from_wire(raw: RawColumn) -> Self {
        Self {
            name: raw.name,
            type_signature: raw.type_signature,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct QueryError {
    message: Option<String>,
    error_code: Option<i64>,
    error_name: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_origin_accepts_matching_host_and_scheme() {
        assert!(same_origin(
            "https://trino.example.com:8080",
            "https://trino.example.com:8080/v1/statement/queued/abc/1"
        ));
    }

    #[test]
    fn same_origin_rejects_different_host() {
        assert!(!same_origin(
            "https://trino.example.com:8080",
            "https://evil.example.com:8080/v1/statement/queued/abc/1"
        ));
    }

    #[test]
    fn same_origin_rejects_different_port() {
        assert!(!same_origin(
            "https://trino.example.com:8080",
            "https://trino.example.com:9090/v1/statement/queued/abc/1"
        ));
    }

    #[test]
    fn same_origin_rejects_scheme_downgrade() {
        assert!(!same_origin(
            "https://trino.example.com",
            "http://trino.example.com/v1/statement/queued/abc/1"
        ));
    }

    #[test]
    fn poll_delay_steps_grow_then_cap() {
        assert_eq!(poll_delay(0), Duration::from_millis(50));
        assert_eq!(poll_delay(4), Duration::from_millis(1000));
        // Step 5 onwards: exponential, capped at MAX_POLL_DELAY_MS.
        assert!(poll_delay(20) <= Duration::from_millis(MAX_POLL_DELAY_MS));
    }

    #[test]
    fn config_trims_trailing_slash() {
        let c = TrinoClientConfig::new("http://trino.example.com:8080/");
        assert_eq!(c.coordinator_url, "http://trino.example.com:8080");
    }

    #[test]
    fn build_headers_uses_basic_auth_user_when_unset() {
        let auth = crate::test_helpers::test_basic_auth();
        let cfg = TrinoClientConfig::new("http://localhost:8080");
        let client = TrinoClient::new(cfg, auth);
        let headers = client.build_headers().unwrap();
        assert_eq!(headers["x-trino-user"], "alice");
        assert!(
            headers[AUTHORIZATION]
                .to_str()
                .unwrap()
                .starts_with("Basic ")
        );
        // The Authorization header is marked sensitive so wiremock /
        // tracing redact it on Debug formatting.
        assert!(headers[AUTHORIZATION].is_sensitive());
    }

    #[test]
    fn build_headers_requires_explicit_user_for_jwt() {
        let auth = TrinoAuth::jwt("eyJ.token").unwrap();
        let cfg = TrinoClientConfig::new("http://localhost:8080");
        let client = TrinoClient::new(cfg, auth);
        // No `with_user` set + JWT auth → error.
        assert!(client.build_headers().is_err());
    }

    #[test]
    fn build_headers_sets_catalog_and_schema_when_configured() {
        let auth = crate::test_helpers::test_basic_auth();
        let cfg = TrinoClientConfig::new("http://localhost:8080")
            .with_default_catalog("iceberg")
            .with_default_schema("raw");
        let client = TrinoClient::new(cfg, auth);
        let headers = client.build_headers().unwrap();
        assert_eq!(headers["x-trino-catalog"], "iceberg");
        assert_eq!(headers["x-trino-schema"], "raw");
    }

    #[tokio::test]
    async fn execute_polls_until_finished_and_concatenates_rows() {
        use wiremock::matchers::{header, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let next_uri_1 = format!("{}/v1/statement/queued/1/1", server.uri());
        let next_uri_2 = format!("{}/v1/statement/executing/1/2", server.uri());

        // 1) POST /v1/statement → 200, returns nextUri pointing at the
        //    queued endpoint. No rows yet.
        Mock::given(method("POST"))
            .and(path("/v1/statement"))
            .and(header("x-trino-user", "alice"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "20240101_000000_00001_abcde",
                "nextUri": next_uri_1,
                "stats": {"state": "QUEUED"},
            })))
            .expect(1)
            .mount(&server)
            .await;

        // 2) GET queued → 200, advances state to RUNNING + ships first
        //    row. Still has a nextUri.
        Mock::given(method("GET"))
            .and(path("/v1/statement/queued/1/1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "20240101_000000_00001_abcde",
                "nextUri": next_uri_2,
                "columns": [
                    {"name": "id", "type": "bigint"},
                    {"name": "name", "type": "varchar"}
                ],
                "data": [[1, "alice"]],
                "stats": {"state": "RUNNING"},
            })))
            .expect(1)
            .mount(&server)
            .await;

        // 3) GET executing → 200, drops nextUri (= terminal), state
        //    FINISHED, ships second row.
        Mock::given(method("GET"))
            .and(path("/v1/statement/executing/1/2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "20240101_000000_00001_abcde",
                "data": [[2, "bob"]],
                "stats": {"state": "FINISHED"},
            })))
            .expect(1)
            .mount(&server)
            .await;

        let auth = crate::test_helpers::test_basic_auth();
        let cfg = TrinoClientConfig::new(server.uri()).with_timeout(Duration::from_secs(5));
        let client = TrinoClient::new(cfg, auth);
        let out = client.execute("SELECT id, name FROM users").await.unwrap();
        assert_eq!(out.columns.len(), 2);
        assert_eq!(out.columns[0].name, "id");
        assert_eq!(out.columns[1].type_signature, "varchar");
        assert_eq!(out.rows.len(), 2);
        assert_eq!(out.rows[0][1], serde_json::Value::String("alice".into()));
        assert_eq!(out.rows[1][1], serde_json::Value::String("bob".into()));
    }

    #[tokio::test]
    async fn execute_surfaces_failed_query_with_error_info() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/statement"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "20240101_000000_00002_xyz",
                "stats": {"state": "FAILED"},
                "error": {
                    "message": "line 1:8: Column 'missing_col' cannot be resolved",
                    "errorCode": 47,
                    "errorName": "COLUMN_NOT_FOUND"
                }
            })))
            .mount(&server)
            .await;

        let auth = crate::test_helpers::test_basic_auth();
        let cfg = TrinoClientConfig::new(server.uri()).with_timeout(Duration::from_secs(5));
        let client = TrinoClient::new(cfg, auth);
        let err = client
            .execute("SELECT missing_col FROM users")
            .await
            .unwrap_err();
        match err {
            TrinoError::QueryFailed {
                state,
                error_code,
                error_name,
                message,
            } => {
                assert_eq!(state, "FAILED");
                assert_eq!(error_code, 47);
                assert_eq!(error_name, "COLUMN_NOT_FOUND");
                assert!(message.contains("missing_col"));
            }
            other => panic!("expected QueryFailed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn execute_times_out_when_query_keeps_returning_next_uri() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let stuck_uri = format!("{}/v1/statement/stuck/1/1", server.uri());

        Mock::given(method("POST"))
            .and(path("/v1/statement"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "stuck",
                "nextUri": stuck_uri,
                "stats": {"state": "QUEUED"},
            })))
            .mount(&server)
            .await;
        // Every poll re-points at itself — the query never reaches a
        // terminal state, so the deadline must trip.
        Mock::given(method("GET"))
            .and(path("/v1/statement/stuck/1/1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "stuck",
                "nextUri": stuck_uri,
                "stats": {"state": "RUNNING"},
            })))
            .mount(&server)
            .await;

        let auth = crate::test_helpers::test_basic_auth();
        // 1s timeout keeps the test fast.
        let cfg = TrinoClientConfig::new(server.uri()).with_timeout(Duration::from_secs(1));
        let client = TrinoClient::new(cfg, auth);
        let err = client.execute("SELECT 1").await.unwrap_err();
        match err {
            TrinoError::Timeout {
                timeout_secs,
                last_state,
            } => {
                assert_eq!(timeout_secs, 1);
                // Could be QUEUED or RUNNING depending on which page the
                // deadline fires after — both are valid pre-terminal
                // states.
                assert!(last_state == "RUNNING" || last_state == "QUEUED");
            }
            other => panic!("expected Timeout, got {other:?}"),
        }
    }
}
