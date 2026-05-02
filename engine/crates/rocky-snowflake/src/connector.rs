//! Snowflake SQL REST API v2 connector.
//!
//! Executes SQL statements against Snowflake using the SQL REST API:
//! - Submit: `POST https://{account}.snowflakecomputing.com/api/v2/statements`
//! - Poll: `GET https://{account}.snowflakecomputing.com/api/v2/statements/{handle}`
//! - Cancel: `POST https://{account}.snowflakecomputing.com/api/v2/statements/{handle}/cancel`
//!
//! Supports retry with exponential backoff for transient failures.

use std::sync::Arc;
use std::time::Duration;

use reqwest::{Client, RequestBuilder};
use rocky_core::circuit_breaker::{CircuitBreaker, TransitionOutcome};
use rocky_core::config::RetryConfig;
use rocky_core::retry::compute_backoff;
use rocky_observe::events::{
    ErrorClass, PipelineEvent, global_event_bus, record_span_event, set_current_span_error,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{Instrument, debug, info_span, warn};

use crate::auth::Auth;

/// Errors from the Snowflake SQL REST API.
#[derive(Debug, Error)]
pub enum ConnectorError {
    #[error("auth error: {0}")]
    Auth(#[from] crate::auth::AuthError),

    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("statement {handle} failed: {message}")]
    StatementFailed { handle: String, message: String },

    #[error("statement {handle} timed out after {seconds}s")]
    Timeout { handle: String, seconds: u64 },

    #[error("API error {status}: {body}")]
    ApiError { status: u16, body: String },

    #[error("circuit breaker tripped after {consecutive_failures} consecutive transient failures")]
    CircuitBreakerOpen { consecutive_failures: u32 },

    #[error("run-level retry budget exhausted (limit {limit}); aborting remaining retries")]
    RetryBudgetExhausted { limit: u32 },
}

/// Configuration for the Snowflake connector.
#[derive(Debug, Clone)]
pub struct ConnectorConfig {
    /// Snowflake account identifier (e.g., "xy12345.us-east-1").
    pub account: String,
    /// Warehouse to use for query execution.
    pub warehouse: String,
    /// Default database (optional — can be overridden per-statement).
    pub database: Option<String>,
    /// Default schema (optional).
    pub schema: Option<String>,
    /// Role to use for the session (optional).
    pub role: Option<String>,
    /// Maximum time to wait for a statement to complete.
    pub timeout: Duration,
    /// Retry configuration for transient failures.
    pub retry: RetryConfig,
}

// --- API request/response types ---

#[derive(Debug, Serialize)]
struct SubmitRequest {
    statement: String,
    warehouse: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    database: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    schema: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    role: Option<String>,
    timeout: u64,
}

/// Response from the Snowflake SQL REST API.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StatementResponse {
    /// Statement handle (used for polling and cancellation).
    #[serde(default)]
    pub statement_handle: String,
    /// Execution status code: "00000" = success, "333334" = async executing.
    #[serde(default)]
    pub code: String,
    /// Human-readable status message.
    #[serde(default)]
    pub message: String,
    /// Statement status string (e.g., "success", "error").
    #[serde(default)]
    pub statement_status_url: String,
    /// Result set metadata.
    pub result_set_meta_data: Option<ResultSetMetaData>,
    /// Inline result data.
    pub data: Option<Vec<Vec<serde_json::Value>>>,
}

/// Metadata about the result set columns.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResultSetMetaData {
    pub num_rows: Option<u64>,
    #[serde(default)]
    pub row_type: Vec<ColumnMetaData>,
}

/// Column metadata from a Snowflake query result.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ColumnMetaData {
    pub name: String,
    #[serde(rename = "type")]
    pub type_name: Option<String>,
    pub nullable: Option<bool>,
}

/// Parsed query result from Snowflake.
#[derive(Debug)]
pub struct QueryResult {
    pub statement_handle: String,
    pub columns: Vec<ColumnMetaData>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub total_row_count: Option<u64>,
}

/// Async Snowflake SQL connector using the REST API v2.
///
/// Cheaply cloneable (reqwest::Client and Auth use Arc internally).
/// Circuit breaker state is shared across clones.
#[derive(Clone)]
pub struct SnowflakeConnector {
    config: ConnectorConfig,
    auth: Auth,
    client: Client,
    /// Shared circuit breaker (clones share state via `Arc`). See
    /// `rocky_core::circuit_breaker::CircuitBreaker` for the timed
    /// half-open recovery semantics.
    circuit_breaker: Arc<CircuitBreaker>,
    /// Shared retry budget across the run (§P2.7). Unbounded by default —
    /// set via `config.retry.max_retries_per_run`.
    retry_budget: rocky_core::retry_budget::RetryBudget,
    /// Override for the base URL (used by tests to point at wiremock).
    #[cfg(any(test, feature = "test-support"))]
    base_url_override: Option<String>,
}

impl SnowflakeConnector {
    /// Creates a new connector with the given configuration and auth provider.
    pub fn new(config: ConnectorConfig, auth: Auth) -> Self {
        let retry_budget =
            rocky_core::retry_budget::RetryBudget::from_config(config.retry.max_retries_per_run);
        let circuit_breaker = Arc::new(config.retry.build_circuit_breaker());
        SnowflakeConnector {
            config,
            auth,
            client: Client::builder()
                .user_agent(concat!("rocky/", env!("CARGO_PKG_VERSION")))
                .tcp_nodelay(true)
                .pool_max_idle_per_host(32)
                .pool_idle_timeout(std::time::Duration::from_secs(300))
                .build()
                .unwrap_or_else(|_| Client::new()),
            circuit_breaker,
            retry_budget,
            #[cfg(any(test, feature = "test-support"))]
            base_url_override: None,
        }
    }

    /// Override the run-level [`RetryBudget`](rocky_core::retry_budget::RetryBudget).
    /// Used by `rocky run` when multiple adapters should share one budget.
    #[must_use]
    pub fn with_retry_budget(mut self, budget: rocky_core::retry_budget::RetryBudget) -> Self {
        self.retry_budget = budget;
        self
    }

    /// Creates a connector that points at a custom base URL (for testing with wiremock).
    #[cfg(any(test, feature = "test-support"))]
    #[must_use]
    pub fn with_base_url(mut self, base_url: String) -> Self {
        self.base_url_override = Some(base_url);
        self
    }

    /// Applies Snowflake auth headers to a request: the `Authorization:
    /// Bearer <token>` header and, for OAuth and key-pair JWT modes,
    /// the matching `X-Snowflake-Authorization-Token-Type` header.
    /// Password / session-token mode emits no token-type header per
    /// Snowflake's SQL API v2 spec — the server sniffs the token kind
    /// from the token itself.
    fn apply_auth_headers(&self, builder: RequestBuilder, token: &str) -> RequestBuilder {
        let builder = builder.bearer_auth(token);
        match self.auth.token_type().header_value() {
            Some(value) => builder.header("X-Snowflake-Authorization-Token-Type", value),
            None => builder,
        }
    }

    /// Base URL for the Snowflake SQL REST API.
    fn base_url(&self) -> String {
        #[cfg(any(test, feature = "test-support"))]
        if let Some(url) = &self.base_url_override {
            return format!("{}/api/v2/statements", url);
        }
        format!(
            "https://{}.snowflakecomputing.com/api/v2/statements",
            self.config.account
        )
    }

    /// Executes a SQL query and returns rows.
    pub async fn execute_sql(&self, sql: &str) -> Result<QueryResult, ConnectorError> {
        let response = self.submit_and_wait(sql).await?;

        let columns = response
            .result_set_meta_data
            .as_ref()
            .map(|m| m.row_type.clone())
            .unwrap_or_default();

        let total_row_count = response
            .result_set_meta_data
            .as_ref()
            .and_then(|m| m.num_rows);

        let rows = response.data.unwrap_or_default();

        Ok(QueryResult {
            statement_handle: response.statement_handle,
            columns,
            rows,
            total_row_count,
        })
    }

    /// Executes a SQL statement without caring about result data.
    pub async fn execute_statement(&self, sql: &str) -> Result<String, ConnectorError> {
        let response = self.submit_and_wait(sql).await?;
        Ok(response.statement_handle)
    }

    /// Submit with retry logic and circuit breaker.
    ///
    /// Wrapped in a single `statement.execute` span per logical statement
    /// — retries surface as `statement_retry` / `circuit_breaker_*` span
    /// events on the same parent (see `record_span_event` calls below)
    /// rather than spawning a new span per attempt. Mirrors the OTel
    /// `<verb>.<resource>` shape used by the `materialize.table` parent.
    async fn submit_and_wait(&self, sql: &str) -> Result<StatementResponse, ConnectorError> {
        // `statement.kind = "query"` is a best-effort default — Snowflake
        // routes DDL, DML, and SELECT through the same `/statements`
        // endpoint and the wire request doesn't surface the parsed
        // statement kind. TODO: classify via `rocky-sql` if downstream
        // consumers need to filter ddl/dml from query traffic.
        let span = info_span!(
            "statement.execute",
            adapter = "snowflake",
            statement.kind = "query",
        );
        self.submit_and_wait_inner(sql).instrument(span).await
    }

    async fn submit_and_wait_inner(&self, sql: &str) -> Result<StatementResponse, ConnectorError> {
        let retry = &self.config.retry;

        // Circuit breaker: fail fast if Open. When timed half-open
        // recovery is configured, `check()` transitions Open → HalfOpen
        // after `circuit_breaker_recovery_timeout_secs` and lets one
        // trial request through — success below closes the breaker,
        // failure re-opens.
        if let Err(e) = self.circuit_breaker.check() {
            return Err(ConnectorError::CircuitBreakerOpen {
                consecutive_failures: e.consecutive_failures,
            });
        }

        for attempt in 0..=retry.max_retries {
            match self.submit_and_wait_once(sql).await {
                Ok(resp) => {
                    if self.circuit_breaker.record_success() == TransitionOutcome::Recovered {
                        let evt = PipelineEvent::new("circuit_breaker_recovered")
                            .with_target("snowflake");
                        record_span_event(&evt);
                        global_event_bus().emit(evt);
                    }
                    return Ok(resp);
                }
                Err(err) => {
                    if is_transient(&err)
                        && self.circuit_breaker.record_failure(&err.to_string())
                            == TransitionOutcome::Tripped
                    {
                        let evt = PipelineEvent::new("circuit_breaker_tripped")
                            .with_target("snowflake")
                            .with_error(err.to_string())
                            .with_error_class(classify_error(&err));
                        record_span_event(&evt);
                        global_event_bus().emit(evt);
                    }
                    if attempt < retry.max_retries && is_transient(&err) {
                        // Run-level retry budget (§P2.7). Short-circuit if
                        // exhausted so one bad statement can't drain the
                        // adapter's rate-limit quota for the rest of the run.
                        if !self.retry_budget.try_consume() {
                            let limit = self.retry_budget.total().unwrap_or(0);
                            warn!(
                                attempt = attempt + 1,
                                max_retries = retry.max_retries,
                                budget_limit = limit,
                                error = %err,
                                "retry budget exhausted for this run; aborting further retries",
                            );
                            // Mark active span Error — terminal failure.
                            set_current_span_error(format!(
                                "retry budget exhausted (limit {limit})"
                            ));
                            return Err(ConnectorError::RetryBudgetExhausted { limit });
                        }
                        // §P2.8 retry event emission — see databricks/connector.rs
                        let evt = PipelineEvent::new("statement_retry")
                            .with_error(err.to_string())
                            .with_attempt(attempt + 1, retry.max_retries)
                            .with_error_class(classify_error(&err));
                        record_span_event(&evt);
                        global_event_bus().emit(evt);
                        // 401 from Snowflake often means the cached keypair
                        // JWT crossed the server-side expiry boundary between
                        // mint and request. Drop the cache so the next
                        // attempt mints a fresh JWT; a genuine bad-credential
                        // 401 will simply re-fail and exhaust retries.
                        if matches!(&err, ConnectorError::ApiError { status: 401, .. }) {
                            self.auth.invalidate_cache().await;
                        }
                        let backoff_ms = compute_backoff(retry, attempt);
                        warn!(
                            attempt = attempt + 1,
                            max_retries = retry.max_retries,
                            backoff_ms,
                            error = %err,
                            "transient error, retrying"
                        );
                        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                        continue;
                    }
                    // Retry exhausted (or non-transient) — terminal failure.
                    set_current_span_error(err.to_string());
                    return Err(err);
                }
            }
        }

        unreachable!("retry loop should always return")
    }

    /// Single attempt: submit SQL, poll until terminal.
    async fn submit_and_wait_once(&self, sql: &str) -> Result<StatementResponse, ConnectorError> {
        let token = self.auth.get_token().await?;
        let url = self.base_url();

        debug!(sql, "submitting SQL statement to Snowflake");

        let body = SubmitRequest {
            statement: sql.to_string(),
            warehouse: self.config.warehouse.clone(),
            database: self.config.database.clone(),
            schema: self.config.schema.clone(),
            role: self.config.role.clone(),
            timeout: self.config.timeout.as_secs(),
        };

        let request = self
            .client
            .post(&url)
            .header("Content-Type", "application/json")
            .header("Accept", "application/json")
            .json(&body);
        let resp = self.apply_auth_headers(request, &token).send().await?;

        if !resp.status().is_success() {
            let status = resp.status().as_u16();
            let body_text = resp.text().await.unwrap_or_default();
            return Err(ConnectorError::ApiError {
                status,
                body: body_text,
            });
        }

        let response: StatementResponse = resp.json().await?;

        // Check if statement completed inline
        if is_terminal_code(&response.code) {
            return check_terminal(response);
        }

        // Poll until complete
        let handle = &response.statement_handle;
        let poll_url = format!("{}/{}", self.base_url(), handle);
        let deadline = tokio::time::Instant::now() + self.config.timeout;
        let backoff_steps = [100, 200, 500, 1000, 2000];
        let mut step = 0;

        loop {
            if tokio::time::Instant::now() > deadline {
                return Err(ConnectorError::Timeout {
                    handle: handle.clone(),
                    seconds: self.config.timeout.as_secs(),
                });
            }

            let delay = Duration::from_millis(backoff_steps[step.min(backoff_steps.len() - 1)]);
            tokio::time::sleep(delay).await;
            step += 1;

            debug!(
                statement_handle = handle,
                step, "polling Snowflake statement"
            );

            let token = self.auth.get_token().await?;
            let poll_request = self
                .client
                .get(&poll_url)
                .header("Accept", "application/json");
            let poll_resp = self.apply_auth_headers(poll_request, &token).send().await?;

            if !poll_resp.status().is_success() {
                let status = poll_resp.status().as_u16();
                let body_text = poll_resp.text().await.unwrap_or_default();
                return Err(ConnectorError::ApiError {
                    status,
                    body: body_text,
                });
            }

            let poll_response: StatementResponse = poll_resp.json().await?;

            if is_terminal_code(&poll_response.code) {
                return check_terminal(poll_response);
            }
        }
    }

    /// Cancels a running statement.
    pub async fn cancel_statement(&self, handle: &str) -> Result<(), ConnectorError> {
        let token = self.auth.get_token().await?;
        let url = format!("{}/{}/cancel", self.base_url(), handle);

        let request = self.client.post(&url).header("Accept", "application/json");
        self.apply_auth_headers(request, &token)
            .send()
            .await?
            .error_for_status()
            .map_err(ConnectorError::Http)?;

        Ok(())
    }
}

/// Snowflake SQL API status codes:
/// - "00000" = ANSI SQL success
/// - "09xxxx" = Snowflake success-with-info (e.g., "090001" for DDL —
///   "Statement executed successfully.")
/// - "333334" = async, still executing
/// - Anything else = error
fn is_terminal_code(code: &str) -> bool {
    // Async-in-progress is the only non-terminal state
    code != "333334" && !code.is_empty()
}

/// Snowflake's SQL API returns codes outside the ANSI `00000` for many
/// successful statement types — DDL most notably emits `090001`, and the
/// `09xxxx` class is reserved for Snowflake's success-with-info codes.
fn is_success_code(code: &str) -> bool {
    code == "00000" || code.starts_with("09")
}

fn check_terminal(response: StatementResponse) -> Result<StatementResponse, ConnectorError> {
    if is_success_code(&response.code) {
        Ok(response)
    } else {
        Err(ConnectorError::StatementFailed {
            handle: response.statement_handle,
            message: response.message,
        })
    }
}

/// Classifies whether a connector error is transient and worth retrying.
fn is_transient(err: &ConnectorError) -> bool {
    match err {
        // 401 is transient-on-first-retry — the connector drops the auth
        // cache before retrying, so a stale cached JWT replays as a fresh
        // mint. Genuine bad credentials re-fail on every attempt.
        ConnectorError::ApiError { status, .. } => matches!(status, 401 | 429 | 502 | 503 | 504),
        ConnectorError::Http(e) => e.is_connect() || e.is_timeout(),
        ConnectorError::StatementFailed { message, .. } => {
            let msg = message.to_uppercase();
            msg.contains("THROTTL")
                || msg.contains("TEMPORARILY_UNAVAILABLE")
                || msg.contains("WAREHOUSE")
                || msg.contains("TIMEOUT")
        }
        ConnectorError::Timeout { .. } => true,
        ConnectorError::Auth(_)
        | ConnectorError::CircuitBreakerOpen { .. }
        | ConnectorError::RetryBudgetExhausted { .. } => false,
    }
}

/// Maps `ConnectorError` to the structured [`ErrorClass`] surfaced on
/// retry `PipelineEvent`s (§P2.8).
fn classify_error(err: &ConnectorError) -> ErrorClass {
    match err {
        ConnectorError::ApiError { status: 401, .. } => ErrorClass::Auth,
        ConnectorError::ApiError { status: 429, .. } => ErrorClass::RateLimit,
        ConnectorError::ApiError {
            status: 502..=504, ..
        } => ErrorClass::Transient,
        ConnectorError::ApiError { status: 400, .. } => ErrorClass::Config,
        ConnectorError::ApiError { .. } => ErrorClass::Permanent,
        ConnectorError::Http(e) if e.is_timeout() => ErrorClass::Timeout,
        ConnectorError::Http(e) if e.is_connect() => ErrorClass::Transient,
        ConnectorError::Http(_) => ErrorClass::Transient,
        ConnectorError::Timeout { .. } => ErrorClass::Timeout,
        ConnectorError::Auth(_) => ErrorClass::Auth,
        ConnectorError::CircuitBreakerOpen { .. } => ErrorClass::Transient,
        ConnectorError::RetryBudgetExhausted { .. } => ErrorClass::Permanent,
        ConnectorError::StatementFailed { .. } => {
            if is_transient(err) {
                ErrorClass::Transient
            } else {
                ErrorClass::Permanent
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_terminal_success() {
        assert!(is_terminal_code("00000"));
    }

    #[test]
    fn test_is_terminal_error() {
        assert!(is_terminal_code("002140"));
    }

    #[test]
    fn test_not_terminal_async() {
        assert!(!is_terminal_code("333334"));
    }

    #[test]
    fn test_not_terminal_empty() {
        assert!(!is_terminal_code(""));
    }

    #[test]
    fn test_check_terminal_success() {
        let resp = StatementResponse {
            statement_handle: "handle-1".into(),
            code: "00000".into(),
            message: "Statement executed successfully.".into(),
            statement_status_url: String::new(),
            result_set_meta_data: None,
            data: None,
        };
        assert!(check_terminal(resp).is_ok());
    }

    #[test]
    fn test_check_terminal_error() {
        let resp = StatementResponse {
            statement_handle: "handle-1".into(),
            code: "002140".into(),
            message: "SQL compilation error".into(),
            statement_status_url: String::new(),
            result_set_meta_data: None,
            data: None,
        };
        let err = check_terminal(resp).unwrap_err();
        assert!(matches!(err, ConnectorError::StatementFailed { .. }));
    }

    #[test]
    fn test_transient_http_429() {
        let err = ConnectorError::ApiError {
            status: 429,
            body: "rate limited".into(),
        };
        assert!(is_transient(&err));
    }

    #[test]
    fn test_transient_http_503() {
        let err = ConnectorError::ApiError {
            status: 503,
            body: "service unavailable".into(),
        };
        assert!(is_transient(&err));
    }

    #[test]
    fn test_not_transient_http_400() {
        let err = ConnectorError::ApiError {
            status: 400,
            body: "bad request".into(),
        };
        assert!(!is_transient(&err));
    }

    #[test]
    fn test_transient_http_401() {
        // 401 is treated as transient so the retry loop can drop the auth
        // cache and re-mint a fresh JWT. Bad credentials re-fail and exhaust
        // retries quickly.
        let err = ConnectorError::ApiError {
            status: 401,
            body: "token expired".into(),
        };
        assert!(is_transient(&err));
    }

    #[test]
    fn test_transient_timeout() {
        let err = ConnectorError::Timeout {
            handle: "h-1".into(),
            seconds: 120,
        };
        assert!(is_transient(&err));
    }

    #[test]
    fn test_transient_warehouse_message() {
        let err = ConnectorError::StatementFailed {
            handle: "h-1".into(),
            message: "Warehouse 'COMPUTE_WH' is suspended, resuming...".into(),
        };
        assert!(is_transient(&err));
    }

    #[test]
    fn test_not_transient_syntax_error() {
        let err = ConnectorError::StatementFailed {
            handle: "h-1".into(),
            message: "SQL compilation error: syntax error at position 5".into(),
        };
        assert!(!is_transient(&err));
    }

    #[test]
    fn test_not_transient_circuit_breaker() {
        let err = ConnectorError::CircuitBreakerOpen {
            consecutive_failures: 5,
        };
        assert!(!is_transient(&err));
    }
}
