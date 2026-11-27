//! Trino REST API connector — `POST /v1/statement` + `GET nextUri` polling.
//!
//! Trino's wire protocol (per `trino.io/docs/current/develop/client-protocol.html`)
//! is a state machine, not a one-shot REST call. Every executed statement
//! flows through the same shape:
//!
//! 1. **Submit:** `POST /v1/statement` with the SQL as the **plain-text**
//!    request body (not JSON). Required headers: `X-Trino-User`,
//!    `Authorization` and `X-Trino-Time-Zone: UTC`, optionally
//!    `X-Trino-Catalog` / `X-Trino-Schema`.
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

use std::future::Future;
use std::time::Duration;

use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue};
use rocky_core::failure_class::{FailureClass, TransientKind};
use rocky_core::statement_cancel::{
    ABANDON_CANCEL_BUDGET, AbandonCancel, InFlightStatements, StatementHandle,
};
use rocky_core::traits::AdapterError;
use serde::Deserialize;
use thiserror::Error;
use tokio::time::Instant;
use tracing::{Instrument, debug, info_span, warn};

use crate::auth::{AuthError, TrinoAuth};

/// Default per-query timeout in seconds. Polling stops with
/// [`TrinoError::Timeout`] when the deadline is reached.
pub const DEFAULT_TIMEOUT_SECS: u64 = 300;

/// Polling delay ladder for the first 5 `nextUri` polls (ms). After step 5,
/// exponential growth bounded by [`MAX_POLL_DELAY_MS`].
const POLL_DELAY_STEPS_MS: [u64; 5] = [50, 100, 250, 500, 1000];
/// Maximum polling delay after exponential growth.
const MAX_POLL_DELAY_MS: u64 = 5000;
/// How many failed `nextUri` reads in a row are read again before the
/// client gives up on the query. A rate-limited read (`429`) does not count;
/// the deadline bounds it.
const MAX_POLL_REREADS: u32 = 3;
/// `last_state` of a [`TrinoError::Timeout`] whose submit did not answer.
const NOT_SUBMITTED: &str = "SUBMITTING";

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
#[derive(Error)]
pub enum TrinoError {
    #[error("HTTP error: {}", rocky_core::secret_registry::render_placeholders(&.0.to_string()))]
    Http(#[from] reqwest::Error),

    #[error("Trino auth error: {0}")]
    Auth(#[from] AuthError),

    #[error(
        "Trino HTTP {status}: {}",
        rocky_core::secret_registry::render_placeholders(message)
    )]
    HttpStatus { status: u16, message: String },

    #[error(
        "Trino query failed (state {state}, error_code {error_code}): {}",
        rocky_core::secret_registry::render_placeholders(message)
    )]
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

    /// The client stopped waiting on query `id` while it may still run: its
    /// deadline passed, or reading its next page failed and reading it again
    /// did not help. A cancel was sent first; `cancel` says what came of it.
    /// Never retried, by the client or the run loop: a resubmit could run
    /// the query twice.
    #[error("stopped waiting on Trino query {id} ({cancel}): {cause}")]
    Abandoned {
        id: String,
        cancel: AbandonCancel,
        /// Not `#[source]`: the message already ends with it, and a source
        /// would print it twice in an `{:#}` chain.
        cause: Box<TrinoError>,
    },

    #[error("Trino response missing expected field: {}", rocky_core::secret_registry::render_placeholders(&.0.to_string()))]
    MalformedResponse(String),

    #[error(
        "Trino coordinator returned a nextUri pointing at a different origin: {} (coordinator: {}). Refusing to follow — the Authorization header would otherwise be sent to an unrelated host.",
        rocky_core::secret_registry::render_placeholders(next),
        rocky_core::secret_registry::render_placeholders(coordinator)
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

    #[error("Trino spooled Arrow decode error: {}", rocky_core::secret_registry::render_placeholders(&.0.to_string()))]
    ArrowDecode(String),
}

impl TrinoError {
    /// Whether the query this error names may still run on the cluster: a
    /// [`Self::Timeout`] (the deadline passed, or the submit did not answer
    /// in time and may have been accepted) or an [`Self::Abandoned`] (the
    /// client stopped waiting). No layer may resubmit such a query: it could
    /// run twice.
    #[must_use]
    pub fn may_still_run(&self) -> bool {
        matches!(self, Self::Timeout { .. } | Self::Abandoned { .. })
    }
}

/// `Debug` prints the rendered `Display` text. A derived `Debug` would print
/// the plaintext of every field and wrapped error (#1919).
impl std::fmt::Debug for TrinoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        rocky_core::secret_registry::fmt_rendered_debug(f, "TrinoError", self)
    }
}

/// Trino [`StandardErrorCode`](https://github.com/trinodb/trino/blob/master/core/trino-spi/src/main/java/io/trino/spi/StandardErrorCode.java)
/// names that are safe to retry: the query hit a cluster-side blip — a worker
/// went away, the coordinator was starting/stopping, or an internal
/// page-transport hiccup — not a deterministic user error. Retrying re-plans
/// the query, typically onto a healthy set of nodes.
///
/// Deliberately conservative: everything not listed (SQL/user errors like
/// `SYNTAX_ERROR` / `TABLE_NOT_FOUND` / `TYPE_MISMATCH`, permission errors,
/// resource-exhaustion the retry can't fix) is permanent. A missed transient
/// name just doesn't get retried; it is never unsafe.
fn is_retryable_error_name(name: &str) -> bool {
    matches!(
        name,
        "NO_NODES_AVAILABLE"
            | "REMOTE_TASK_ERROR"
            | "REMOTE_TASK_MISMATCH"
            | "REMOTE_HOST_GONE"
            | "PAGE_TRANSPORT_ERROR"
            | "PAGE_TRANSPORT_TIMEOUT"
            | "SERVER_STARTING_UP"
            | "SERVER_SHUTTING_DOWN"
    )
}

/// Classify a Trino [`AdapterError`] for the run loop's classified-retry layer,
/// mirroring the Snowflake/BigQuery adapters.
///
/// Downcasts to the typed [`TrinoError`]: a network / 5xx / transport
/// timeout / cluster-blip failure ⇒ [`FailureClass::Transient`]; every other
/// recognised failure (SQL/user errors, auth, the security refusals, decode
/// errors, and a query that may still run: `Timeout`, `Abandoned`) ⇒
/// [`FailureClass::Permanent`]; a non-[`TrinoError`] (e.g. the string-only
/// "table not found" guard) ⇒ [`FailureClass::Unknown`] (fail closed).
///
/// Errs toward `Permanent`/`Unknown`: a missed transient simply isn't retried
/// (no worse than the prior no-op default), and a mis-retried permanent costs
/// only wasted attempts — never data loss (the non-replacing-CTAS bootstrap
/// backstop still guards target creation). Only `Transient` is ever retried.
#[must_use]
pub(crate) fn classify_trino_failure(err: &AdapterError) -> FailureClass {
    let Some(trino_err) = err.inner().downcast_ref::<TrinoError>() else {
        return FailureClass::Unknown;
    };
    match trino_err {
        // The query may still run (or, for a submit that did not answer, may
        // have been accepted). A retry could run it twice (#2768).
        TrinoError::Timeout { .. } | TrinoError::Abandoned { .. } => FailureClass::Permanent,
        TrinoError::Http(e) if e.is_timeout() => FailureClass::Transient(TransientKind::Timeout),
        TrinoError::Http(e) if e.is_connect() => FailureClass::Transient(TransientKind::Network),
        TrinoError::Http(_) => FailureClass::Permanent,
        TrinoError::HttpStatus { status: 429, .. } => {
            FailureClass::Transient(TransientKind::RateLimit)
        }
        TrinoError::HttpStatus {
            status: 502..=504, ..
        } => FailureClass::Transient(TransientKind::ServerBusy),
        TrinoError::HttpStatus { .. } => FailureClass::Permanent,
        TrinoError::QueryFailed { error_name, .. } if is_retryable_error_name(error_name) => {
            FailureClass::Transient(TransientKind::ServerBusy)
        }
        TrinoError::QueryFailed { .. } => FailureClass::Permanent,
        TrinoError::Auth(_)
        | TrinoError::MalformedResponse(_)
        | TrinoError::UntrustedNextUri { .. }
        | TrinoError::ArrowEncodingUnavailable
        | TrinoError::ArrowDecode(_) => FailureClass::Permanent,
    }
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
#[derive(Clone)]
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

/// A `${VAR}`-resolved field prints as `${NAME}` (#1919). `TrinoClient`
/// and `TrinoAdapter` print this config in their own `Debug`.
impl std::fmt::Debug for TrinoClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use rocky_core::secret_registry::render_placeholders as render;
        f.debug_struct("TrinoClientConfig")
            .field("coordinator_url", &render(&self.coordinator_url))
            .field("user", &self.user.as_deref().map(render))
            .field(
                "default_catalog",
                &self.default_catalog.as_deref().map(render),
            )
            .field(
                "default_schema",
                &self.default_schema.as_deref().map(render),
            )
            .field("source", &render(&self.source))
            .field("timeout", &self.timeout)
            .finish()
    }
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
    /// The queries this client waits on now, so a forced exit or a fan-out
    /// abort can cancel them.
    in_flight: InFlightStatements,
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
        Self::with_http(config, auth, http)
    }

    /// Test-only constructor that takes an explicit `reqwest::Client` so
    /// integration tests can lower the default timeouts.
    #[doc(hidden)]
    pub fn with_http(config: TrinoClientConfig, auth: TrinoAuth, http: reqwest::Client) -> Self {
        Self {
            http,
            config,
            auth,
            in_flight: InFlightStatements::new(),
        }
    }

    /// The queries this client waits on now, by Trino query id.
    pub fn in_flight(&self) -> &InFlightStatements {
        &self.in_flight
    }

    /// Ask the coordinator to cancel query `id`: `DELETE /v1/query/{id}`.
    ///
    /// The query id stays the same for the query's whole life, unlike its
    /// `nextUri`, so a caller that holds only the id can cancel. The
    /// coordinator lets a user cancel a query they own. It answers a query
    /// that already ended the same way, so success does not prove the query
    /// stopped.
    ///
    /// # Errors
    ///
    /// [`TrinoError::MalformedResponse`] when `id` is not a Trino query id
    /// (only `[a-z0-9_]`), [`TrinoError::HttpStatus`] on a non-success
    /// answer, and [`TrinoError::Http`] when the request fails.
    pub async fn cancel_query(&self, id: &str) -> Result<(), TrinoError> {
        if id.is_empty()
            || !id
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        {
            return Err(TrinoError::MalformedResponse(format!(
                "not a Trino query id: {id:?}"
            )));
        }
        let url = format!("{}/v1/query/{id}", self.config.coordinator_url);
        let resp = self
            .http
            .delete(&url)
            .headers(self.build_headers()?)
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let message = resp.text().await.unwrap_or_default();
            return Err(TrinoError::HttpStatus {
                status: status.as_u16(),
                message,
            });
        }
        Ok(())
    }

    /// POST `sql` to `/v1/statement` and parse the first page with `parse`.
    /// Bounded by `deadline`: a submit that does not answer in time is a
    /// [`TrinoError::Timeout`] with no query id, which is never retried
    /// (the coordinator may have accepted it).
    pub(crate) async fn submit<T, P, Fut>(
        &self,
        sql: &str,
        headers: &HeaderMap,
        deadline: Instant,
        parse: P,
    ) -> Result<T, TrinoError>
    where
        P: FnOnce(reqwest::Response) -> Fut,
        Fut: Future<Output = Result<T, TrinoError>>,
    {
        let submit_url = format!("{}/v1/statement", self.config.coordinator_url);
        tokio::time::timeout_at(deadline, async {
            let resp = self
                .http
                .post(&submit_url)
                .headers(headers.clone())
                .body(sql.to_string())
                .send()
                .await?;
            parse(resp).await
        })
        .await
        .map_err(|_| TrinoError::Timeout {
            timeout_secs: self.config.timeout.as_secs(),
            last_state: NOT_SUBMITTED.to_string(),
        })?
    }

    /// GET the query's `next` page and parse it with `parse`.
    ///
    /// A transient failure (a transport timeout or connect error, a `502`,
    /// `503` or `504`) reads the same `nextUri` again, at most
    /// [`MAX_POLL_REREADS`] times in a row. A `429` is read again too and
    /// does not count. Every wait is bounded by `deadline`, which ends in a
    /// [`TrinoError::Timeout`]. The query is never submitted again here. An
    /// `Err` means the caller stops waiting on a query that may still run.
    pub(crate) async fn next_page<T, P, Fut>(
        &self,
        next: &str,
        headers: &HeaderMap,
        deadline: Instant,
        last_state: &str,
        parse: P,
    ) -> Result<T, TrinoError>
    where
        P: Fn(reqwest::Response) -> Fut,
        Fut: Future<Output = Result<T, TrinoError>>,
    {
        if !same_origin(&self.config.coordinator_url, next) {
            return Err(TrinoError::UntrustedNextUri {
                coordinator: self.config.coordinator_url.clone(),
                next: next.to_string(),
            });
        }
        let timed_out = || TrinoError::Timeout {
            timeout_secs: self.config.timeout.as_secs(),
            last_state: last_state.to_string(),
        };
        let mut failed_reads: u32 = 0;
        let mut rate_limited: u32 = 0;
        loop {
            if Instant::now() >= deadline {
                return Err(timed_out());
            }
            let read = tokio::time::timeout_at(deadline, async {
                let resp = self.http.get(next).headers(headers.clone()).send().await?;
                parse(resp).await
            })
            .await
            .map_err(|_| timed_out())?;
            let err = match read {
                Ok(page) => return Ok(page),
                Err(err) => err,
            };
            let wait = match &err {
                TrinoError::HttpStatus { status: 429, .. } => {
                    rate_limited = rate_limited.saturating_add(1);
                    poll_delay(rate_limited as usize)
                }
                e if failed_reads < MAX_POLL_REREADS && is_transient_read_error(e) => {
                    failed_reads += 1;
                    poll_delay(failed_reads as usize)
                }
                _ => return Err(err),
            };
            warn!(
                error = %err,
                failed_reads,
                rate_limited,
                "reading the Trino query's next page failed, reading the same page again"
            );
            tokio::time::sleep_until((Instant::now() + wait).min(deadline)).await;
        }
    }

    /// Ask the coordinator to cancel query `id`, which this client stops
    /// waiting on because of `cause`, and return the error to report.
    /// Best effort and bounded by [`ABANDON_CANCEL_BUDGET`].
    pub(crate) async fn abandon(&self, id: String, cause: TrinoError) -> TrinoError {
        let cancel = match tokio::time::timeout(ABANDON_CANCEL_BUDGET, self.cancel_query(&id)).await
        {
            Ok(Ok(())) => AbandonCancel::Requested,
            Ok(Err(e)) => AbandonCancel::Failed(e.to_string()),
            Err(_) => AbandonCancel::NoAnswer,
        };
        warn!(
            query_id = %id,
            cancel = %cancel,
            "stopped waiting on a Trino query that may still run"
        );
        TrinoError::Abandoned {
            id,
            cancel,
            cause: Box::new(cause),
        }
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
    /// (`Authorization` + `X-Trino-User` + `X-Trino-Time-Zone` + optional
    /// catalog/schema).
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
        // Pin the session to UTC. A bare `TIMESTAMP '…'` watermark literal
        // compared with a `timestamp with time zone` column is read in the
        // session zone, so a zone west of UTC would skip rows (#2395).
        headers.insert(
            HeaderName::from_static("x-trino-time-zone"),
            HeaderValue::from_static("UTC"),
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

    /// ```text
    ///   POST ──▶ query id ──▶ register ──▶ GET nextUri ... ──▶ no nextUri ──▶ Ok / QueryFailed
    ///                                         │
    ///                   deadline, or a read that reading again did not fix,
    ///                   or an untrusted nextUri ──▶ cancel (bounded) ──▶ Abandoned
    /// ```
    async fn execute_inner(&self, sql: &str) -> Result<TrinoQueryRows, TrinoError> {
        let deadline = Instant::now() + self.config.timeout;
        debug!(coordinator = %self.config.coordinator_url, "submitting Trino statement");

        let headers = self.build_headers()?;
        let first = self.submit(sql, &headers, deadline, parse_response).await?;
        let id = first.id.clone();
        // Listed until this function returns, on every path, so a forced
        // exit or a fan-out abort can cancel it.
        let _in_flight = self.in_flight.register(StatementHandle::new(id.clone()));
        match self.collect_rows(first, &headers, deadline).await {
            Ok(outcome) => outcome,
            Err(cause) => Err(self.abandon(id, cause).await),
        }
    }

    /// Read pages from `current` on until the query ends, and return how it
    /// ended (`Ok` holds the terminal outcome, which may be a failure). An
    /// `Err` means the client stops waiting on a query that may still run.
    async fn collect_rows(
        &self,
        mut current: QueryResults,
        headers: &HeaderMap,
        deadline: Instant,
    ) -> Result<Result<TrinoQueryRows, TrinoError>, TrinoError> {
        let mut columns: Vec<TrinoColumnMeta> = Vec::new();
        let mut rows: Vec<Vec<serde_json::Value>> = Vec::new();
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
            let last_state = current.stats.state.clone();

            // Terminal-state detection: Trino sends `nextUri = None` only
            // when the query is in a terminal state. `state == "FINISHED"`
            // is the happy path; everything else (FAILED, CANCELED,
            // anything unexpected) maps to QueryFailed.
            let Some(next) = current.next_uri.take() else {
                if current.stats.state == "FINISHED" {
                    return Ok(Ok(TrinoQueryRows { columns, rows }));
                }
                return Ok(Err(query_failed(&current)));
            };

            let now = Instant::now();
            if now >= deadline {
                return Err(TrinoError::Timeout {
                    timeout_secs: self.config.timeout.as_secs(),
                    last_state,
                });
            }
            tokio::time::sleep_until((now + poll_delay(attempt)).min(deadline)).await;
            attempt += 1;

            current = self
                .next_page(&next, headers, deadline, &last_state, parse_response)
                .await?;
        }
    }
}

/// Whether reading a page failed in a way that reading the same `nextUri`
/// again can fix: a transport timeout or connect error, or a `502`, `503`
/// or `504` (the client protocol asks the client to try again).
fn is_transient_read_error(err: &TrinoError) -> bool {
    match err {
        TrinoError::Http(e) => e.is_timeout() || e.is_connect(),
        TrinoError::HttpStatus { status, .. } => matches!(status, 502..=504),
        TrinoError::Auth(_)
        | TrinoError::QueryFailed { .. }
        | TrinoError::Timeout { .. }
        | TrinoError::Abandoned { .. }
        | TrinoError::MalformedResponse(_)
        | TrinoError::UntrustedNextUri { .. }
        | TrinoError::ArrowEncodingUnavailable
        | TrinoError::ArrowDecode(_) => false,
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
    fn error_text_prints_resolved_var_as_placeholder() {
        let secret = "trino-1919.internal.example.com";
        rocky_core::secret_registry::register_substitution("ROCKY_TEST_TRINO_HOST_1919", secret);
        let status = TrinoError::HttpStatus {
            status: 502,
            message: format!("bad gateway from {secret}"),
        };
        let shown = status.to_string();
        assert!(!shown.contains(secret), "{shown}");
        assert!(shown.contains("${ROCKY_TEST_TRINO_HOST_1919}"), "{shown}");
        let next = TrinoError::UntrustedNextUri {
            coordinator: format!("https://{secret}"),
            next: "https://evil.example.org/x".into(),
        };
        let shown = next.to_string();
        assert!(!shown.contains(secret), "{shown}");
        assert!(shown.contains("${ROCKY_TEST_TRINO_HOST_1919}"), "{shown}");
    }

    #[test]
    fn error_text_keeps_short_values_below_the_floor() {
        rocky_core::secret_registry::register_substitution("ROCKY_TEST_TRINO_SHORT_1919", "abc");
        let err = TrinoError::MalformedResponse("catalog abc".into());
        assert_eq!(
            err.to_string(),
            "Trino response missing expected field: catalog abc"
        );
    }

    fn query_failed(error_name: &str) -> AdapterError {
        AdapterError::new(TrinoError::QueryFailed {
            state: "FAILED".into(),
            error_code: 0,
            error_name: error_name.into(),
            message: format!("{error_name} at line 1:1"),
        })
    }

    #[test]
    fn classify_trino_failure_maps_transient_and_permanent() {
        // Cluster-side blips → Transient (retryable). These re-plan onto
        // healthy nodes on retry.
        for name in [
            "NO_NODES_AVAILABLE",
            "REMOTE_TASK_ERROR",
            "REMOTE_TASK_MISMATCH",
            "REMOTE_HOST_GONE",
            "PAGE_TRANSPORT_ERROR",
            "PAGE_TRANSPORT_TIMEOUT",
            "SERVER_STARTING_UP",
            "SERVER_SHUTTING_DOWN",
        ] {
            let class = classify_trino_failure(&query_failed(name));
            assert!(
                class.is_retryable(),
                "{name} should classify Transient: {class:?}"
            );
        }

        // User/SQL errors → Permanent, never retried (the dangerous direction).
        for name in [
            "SYNTAX_ERROR",
            "TABLE_NOT_FOUND",
            "COLUMN_NOT_FOUND",
            "TYPE_MISMATCH",
            "PERMISSION_DENIED",
            "DIVISION_BY_ZERO",
        ] {
            let class = classify_trino_failure(&query_failed(name));
            assert_eq!(class, FailureClass::Permanent, "{name} should be Permanent");
            assert!(!class.is_retryable());
        }

        // HTTP: 429 / 502 / 503 / 504 transient; other statuses permanent.
        for status in [429u16, 502, 503, 504] {
            let err = AdapterError::new(TrinoError::HttpStatus {
                status,
                message: String::new(),
            });
            assert!(
                classify_trino_failure(&err).is_retryable(),
                "HTTP {status} should be Transient"
            );
        }
        for status in [400u16, 401, 403, 404] {
            let err = AdapterError::new(TrinoError::HttpStatus {
                status,
                message: String::new(),
            });
            assert_eq!(
                classify_trino_failure(&err),
                FailureClass::Permanent,
                "HTTP {status} should be Permanent"
            );
        }

        // A query that may still run is never retried (#2768): a retry could
        // run it twice.
        let timeout = || TrinoError::Timeout {
            timeout_secs: 30,
            last_state: "RUNNING".into(),
        };
        assert_eq!(
            classify_trino_failure(&AdapterError::new(timeout())),
            FailureClass::Permanent
        );
        assert_eq!(
            classify_trino_failure(&AdapterError::new(TrinoError::Abandoned {
                id: "q1".into(),
                cancel: AbandonCancel::Requested,
                cause: Box::new(TrinoError::HttpStatus {
                    status: 503,
                    message: String::new(),
                }),
            })),
            FailureClass::Permanent
        );

        // Security refusal / capability / decode → Permanent (retry can't fix).
        for err in [
            TrinoError::UntrustedNextUri {
                coordinator: "https://a".into(),
                next: "https://b".into(),
            },
            TrinoError::ArrowEncodingUnavailable,
            TrinoError::ArrowDecode("bad ipc".into()),
            TrinoError::MalformedResponse("no id".into()),
        ] {
            assert_eq!(
                classify_trino_failure(&AdapterError::new(err)),
                FailureClass::Permanent
            );
        }

        // A non-TrinoError (e.g. the describe "table not found" guard is a
        // type-erased `msg`) → Unknown → not retryable, so callers correctly
        // treat it as absence rather than a transient blip.
        let msg = AdapterError::msg("table iceberg.raw.orders not found");
        assert_eq!(classify_trino_failure(&msg), FailureClass::Unknown);
        assert!(!classify_trino_failure(&msg).is_retryable());
    }

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
    fn config_debug_renders_substituted_fields() {
        const HOST: &str = "trino-host-1919-c5d2.example.com";
        const USER: &str = "trino-cfg-user-1919-c5d2";
        const CATALOG: &str = "trino-catalog-1919-c5d2";
        const SCHEMA: &str = "trino-schema-1919-c5d2";
        for (name, value) in [
            ("RV_TRINO_HOST_DBG", HOST),
            ("RV_TRINO_CFG_USER_DBG", USER),
            ("RV_TRINO_CATALOG_DBG", CATALOG),
            ("RV_TRINO_SCHEMA_DBG", SCHEMA),
        ] {
            rocky_core::secret_registry::register_substitution(name, value);
        }
        let cfg = TrinoClientConfig::new(format!("https://{HOST}"))
            .with_user(USER)
            .with_default_catalog(CATALOG)
            .with_default_schema(SCHEMA);
        let debug = format!("{cfg:?}");
        for value in [HOST, USER, CATALOG, SCHEMA] {
            assert!(!debug.contains(value), "Debug leaks {value}: {debug}");
        }
        assert!(debug.contains("${RV_TRINO_CFG_USER_DBG}"), "{debug}");
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
    fn build_headers_pins_the_session_time_zone_to_utc() {
        let auth = crate::test_helpers::test_basic_auth();
        let cfg = TrinoClientConfig::new("http://localhost:8080");
        let client = TrinoClient::new(cfg, auth);
        let headers = client.build_headers().unwrap();
        assert_eq!(headers["x-trino-time-zone"], "UTC");
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
        let TrinoError::Abandoned { cause, .. } = err else {
            panic!("expected Abandoned, got {err:?}");
        };
        match *cause {
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

    const QUERY_ID: &str = "20260101_000000_00001_abcde";

    fn running_page(server: &wiremock::MockServer, page: u32) -> wiremock::ResponseTemplate {
        wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": QUERY_ID,
            "nextUri": format!("{}/v1/statement/executing/{QUERY_ID}/x/{page}", server.uri()),
            "stats": {"state": "RUNNING"},
        }))
    }

    /// Mount the submit (answered at once with page 1) and the cancel of
    /// [`QUERY_ID`], each expected exactly `submits` and `cancels` times.
    async fn mount_submit_and_cancel(server: &wiremock::MockServer, submits: u64, cancels: u64) {
        use wiremock::Mock;
        use wiremock::matchers::{method, path};
        Mock::given(method("POST"))
            .and(path("/v1/statement"))
            .respond_with(running_page(server, 1))
            .expect(submits)
            .mount(server)
            .await;
        Mock::given(method("DELETE"))
            .and(path(format!("/v1/query/{QUERY_ID}")))
            .respond_with(wiremock::ResponseTemplate::new(204))
            .expect(cancels)
            .mount(server)
            .await;
    }

    fn client_for(server: &wiremock::MockServer, timeout: Duration) -> std::sync::Arc<TrinoClient> {
        let cfg = TrinoClientConfig::new(server.uri()).with_timeout(timeout);
        std::sync::Arc::new(TrinoClient::new(
            cfg,
            crate::test_helpers::test_basic_auth(),
        ))
    }

    /// #2768: the query id comes back on the submit, so the query is listed
    /// in flight while its first page read is still open, and unlisted once
    /// it ends.
    #[tokio::test]
    async fn the_query_is_listed_before_the_first_read_ends() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        mount_submit_and_cancel(&server, 1, 0).await;
        Mock::given(method("GET"))
            .and(path(format!("/v1/statement/executing/{QUERY_ID}/x/1")))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({
                        "id": QUERY_ID,
                        "stats": {"state": "FINISHED"},
                    }))
                    .set_delay(Duration::from_secs(1)),
            )
            .expect(1)
            .mount(&server)
            .await;

        let client = client_for(&server, Duration::from_secs(30));
        let running = {
            let client = client.clone();
            tokio::spawn(async move { client.execute("INSERT INTO t SELECT 1").await })
        };
        let deadline = Instant::now() + Duration::from_secs(10);
        while client.in_flight().snapshot().is_empty() {
            assert!(Instant::now() < deadline, "never listed");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            client.in_flight().snapshot(),
            vec![StatementHandle::new(QUERY_ID)]
        );
        assert!(
            !running.is_finished(),
            "listed while the first read is open"
        );
        running.await.unwrap().unwrap();
        assert!(client.in_flight().snapshot().is_empty());
    }

    /// #2768: the client deadline cancels the query and returns `Abandoned`.
    /// The query is submitted once.
    #[tokio::test]
    async fn the_client_deadline_cancels_and_does_not_resubmit() {
        use wiremock::MockServer;
        use wiremock::matchers::{method, path_regex};

        let server = MockServer::start().await;
        mount_submit_and_cancel(&server, 1, 1).await;
        wiremock::Mock::given(method("GET"))
            .and(path_regex("^/v1/statement/executing/"))
            .respond_with(running_page(&server, 1))
            .mount(&server)
            .await;

        let client = client_for(&server, Duration::from_millis(700));
        let err = client.execute("INSERT INTO t SELECT 1").await.unwrap_err();
        let TrinoError::Abandoned { id, cancel, cause } = &err else {
            panic!("expected Abandoned, got {err}");
        };
        assert_eq!(id, QUERY_ID);
        assert_eq!(*cancel, AbandonCancel::Requested);
        assert!(matches!(**cause, TrinoError::Timeout { .. }), "{cause}");
        assert!(err.may_still_run());
        assert!(client.in_flight().snapshot().is_empty());
    }

    /// #2768: a transient read error reads the same `nextUri` again. The
    /// query is neither submitted again nor cancelled.
    #[tokio::test]
    async fn a_transient_read_error_reads_the_same_page_again() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        mount_submit_and_cancel(&server, 1, 0).await;
        let page = format!("/v1/statement/executing/{QUERY_ID}/x/1");
        Mock::given(method("GET"))
            .and(path(page.clone()))
            .respond_with(ResponseTemplate::new(503).set_body_string("unavailable"))
            .up_to_n_times(2)
            .with_priority(1)
            .expect(2)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(page))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": QUERY_ID,
                "stats": {"state": "FINISHED"},
            })))
            .with_priority(2)
            .expect(1)
            .mount(&server)
            .await;

        let client = client_for(&server, Duration::from_secs(30));
        client.execute("INSERT INTO t SELECT 1").await.unwrap();
    }

    /// #2768: a rate-limited read (429) does not count as a failed read: the
    /// same page is read again more times than `MAX_POLL_REREADS`, with no
    /// cancel.
    #[tokio::test]
    async fn rate_limited_reads_do_not_abandon_the_query() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        mount_submit_and_cancel(&server, 1, 0).await;
        let page = format!("/v1/statement/executing/{QUERY_ID}/x/1");
        let limited = u64::from(MAX_POLL_REREADS) + 1;
        Mock::given(method("GET"))
            .and(path(page.clone()))
            .respond_with(ResponseTemplate::new(429).set_body_string("slow down"))
            .up_to_n_times(limited)
            .with_priority(1)
            .expect(limited)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(page))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": QUERY_ID,
                "stats": {"state": "FINISHED"},
            })))
            .with_priority(2)
            .expect(1)
            .mount(&server)
            .await;

        let client = client_for(&server, Duration::from_secs(30));
        client.execute("INSERT INTO t SELECT 1").await.unwrap();
    }

    /// #2768: a read error that reading again does not fix cancels the
    /// query, and the run loop does not retry it.
    #[tokio::test]
    async fn read_errors_that_persist_cancel_and_are_not_retried() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        mount_submit_and_cancel(&server, 1, 1).await;
        Mock::given(method("GET"))
            .and(path(format!("/v1/statement/executing/{QUERY_ID}/x/1")))
            .respond_with(ResponseTemplate::new(503).set_body_string("unavailable"))
            .expect(u64::from(MAX_POLL_REREADS) + 1)
            .mount(&server)
            .await;

        let client = client_for(&server, Duration::from_secs(30));
        let err = client.execute("INSERT INTO t SELECT 1").await.unwrap_err();
        let TrinoError::Abandoned { cancel, cause, .. } = &err else {
            panic!("expected Abandoned, got {err}");
        };
        assert_eq!(*cancel, AbandonCancel::Requested);
        assert!(
            matches!(**cause, TrinoError::HttpStatus { status: 503, .. }),
            "{cause}"
        );
        assert_eq!(
            classify_trino_failure(&AdapterError::new(err)),
            FailureClass::Permanent
        );
    }

    /// #2768: an untrusted `nextUri` stops the wait while the query runs, so
    /// the query is cancelled.
    #[tokio::test]
    async fn an_untrusted_next_uri_cancels_the_query() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/statement"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": QUERY_ID,
                "nextUri": "https://evil.example.org/v1/statement/x",
                "stats": {"state": "QUEUED"},
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .and(path(format!("/v1/query/{QUERY_ID}")))
            .respond_with(ResponseTemplate::new(204))
            .expect(1)
            .mount(&server)
            .await;

        let client = client_for(&server, Duration::from_secs(30));
        let err = client.execute("SELECT 1").await.unwrap_err();
        let TrinoError::Abandoned { cause, .. } = &err else {
            panic!("expected Abandoned, got {err}");
        };
        assert!(
            matches!(**cause, TrinoError::UntrustedNextUri { .. }),
            "{cause}"
        );
    }

    /// #2768: a query that fails on its own has ended, so nothing is
    /// cancelled and the error stays the coordinator's answer.
    #[tokio::test]
    async fn a_failed_query_is_its_outcome_not_an_abandon() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        mount_submit_and_cancel(&server, 1, 0).await;
        Mock::given(method("GET"))
            .and(path(format!("/v1/statement/executing/{QUERY_ID}/x/1")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": QUERY_ID,
                "stats": {"state": "FAILED"},
                "error": {"message": "boom", "errorCode": 1, "errorName": "SYNTAX_ERROR"},
            })))
            .expect(1)
            .mount(&server)
            .await;

        let client = client_for(&server, Duration::from_secs(30));
        let err = client.execute("SELECT x").await.unwrap_err();
        assert!(matches!(err, TrinoError::QueryFailed { .. }), "{err}");
    }

    /// A handle that is not a Trino query id is refused before any request.
    #[tokio::test]
    async fn cancel_refuses_a_handle_that_is_not_a_query_id() {
        let client = TrinoClient::new(
            TrinoClientConfig::new("http://localhost:1"),
            crate::test_helpers::test_basic_auth(),
        );
        for id in ["", "../v1/node", "a/b", "Q1"] {
            assert!(
                matches!(
                    client.cancel_query(id).await,
                    Err(TrinoError::MalformedResponse(_))
                ),
                "{id:?}"
            );
        }
    }
}
