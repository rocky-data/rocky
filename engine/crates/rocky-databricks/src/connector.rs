use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, SystemTime};

use reqwest::Client;
use rocky_core::config::RetryConfig;
use rocky_observe::events::{ErrorClass, PipelineEvent, global_event_bus};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{debug, warn};

use crate::auth::Auth;

/// Errors from the Databricks SQL Statement Execution API.
#[derive(Debug, Error)]
pub enum ConnectorError {
    #[error("auth error: {0}")]
    Auth(#[from] crate::auth::AuthError),

    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("statement {id} failed: {message}")]
    StatementFailed { id: String, message: String },

    #[error("statement {id} timed out after {seconds}s")]
    Timeout { id: String, seconds: u64 },

    #[error("statement {id} was canceled")]
    Canceled { id: String },

    #[error("unexpected statement state '{state}' for {id}")]
    UnexpectedState { id: String, state: String },

    #[error("API error {status}: {body}")]
    ApiError { status: u16, body: String },

    #[error("circuit breaker tripped after {consecutive_failures} consecutive transient failures")]
    CircuitBreakerOpen { consecutive_failures: u32 },

    #[error("run-level retry budget exhausted (limit {limit}); aborting remaining retries")]
    RetryBudgetExhausted { limit: u32 },
}

/// Configuration for the Databricks SQL connector.
#[derive(Debug, Clone)]
pub struct ConnectorConfig {
    pub host: String,
    pub warehouse_id: String,
    pub timeout: Duration,
    pub retry: RetryConfig,
}

impl ConnectorConfig {
    /// Extracts warehouse_id from an HTTP path like `/sql/1.0/warehouses/{id}`.
    pub fn warehouse_id_from_http_path(http_path: &str) -> Option<String> {
        http_path
            .rsplit('/')
            .next()
            .map(std::string::ToString::to_string)
    }
}

/// Async Databricks SQL connector using the Statement Execution API.
///
/// This type is cheaply cloneable (reqwest::Client and Auth use Arc internally),
/// making it safe to share across concurrent tokio tasks.
/// The circuit breaker state is shared across clones via Arc.
#[derive(Clone)]
pub struct DatabricksConnector {
    config: ConnectorConfig,
    auth: Auth,
    client: Client,
    /// Consecutive transient failures (shared across clones for circuit breaker).
    consecutive_failures: std::sync::Arc<AtomicU32>,
    /// Shared retry budget across the run (§P2.7). Unbounded by default —
    /// set via [`ConnectorConfig::retry::max_retries_per_run`].
    retry_budget: rocky_core::retry_budget::RetryBudget,
    /// Override for the base URL scheme + host (used by tests to point at wiremock).
    #[cfg(any(test, feature = "test-support"))]
    base_url_override: Option<String>,
}

// --- API types ---

#[derive(Debug, Serialize)]
struct SubmitRequest {
    warehouse_id: String,
    statement: String,
    wait_timeout: String,
    disposition: String,
    format: String,
}

/// Response from the Databricks SQL Statement Execution API.
#[derive(Debug, Deserialize)]
pub struct StatementResponse {
    pub statement_id: String,
    pub status: StatementStatus,
    pub manifest: Option<Manifest>,
    pub result: Option<ResultData>,
}

/// Status of a submitted SQL statement (state + optional error).
#[derive(Debug, Deserialize)]
pub struct StatementStatus {
    pub state: String,
    pub error: Option<StatusError>,
}

/// Error details from a failed SQL statement.
#[derive(Debug, Deserialize)]
pub struct StatusError {
    pub error_code: Option<String>,
    pub message: Option<String>,
}

/// Result manifest containing schema and row count metadata.
#[derive(Debug, Clone, Deserialize)]
pub struct Manifest {
    pub schema: Option<ManifestSchema>,
    pub total_row_count: Option<u64>,
}

/// Column schema from a statement result manifest.
#[derive(Debug, Clone, Deserialize)]
pub struct ManifestSchema {
    #[serde(default)]
    pub columns: Vec<ColumnSchema>,
}

/// A single column's name, type, and ordinal position in the result set.
#[derive(Debug, Clone, Deserialize)]
pub struct ColumnSchema {
    pub name: String,
    pub type_name: String,
    pub position: u32,
}

/// Inline result data from a SQL statement (JSON_ARRAY format).
#[derive(Debug, Clone, Deserialize)]
pub struct ResultData {
    pub data_array: Option<Vec<Vec<serde_json::Value>>>,
}

/// The result of executing a SQL statement.
#[derive(Debug)]
pub struct QueryResult {
    pub statement_id: String,
    pub columns: Vec<ColumnSchema>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub total_row_count: Option<u64>,
}

impl DatabricksConnector {
    /// Creates a new connector with the given configuration and auth provider.
    pub fn new(config: ConnectorConfig, auth: Auth) -> Self {
        let retry_budget =
            rocky_core::retry_budget::RetryBudget::from_config(config.retry.max_retries_per_run);
        DatabricksConnector {
            config,
            auth,
            client: reqwest::Client::builder()
                .tcp_nodelay(true)
                .pool_max_idle_per_host(32)
                .pool_idle_timeout(std::time::Duration::from_secs(300))
                .timeout(std::time::Duration::from_secs(120))
                .build()
                .unwrap_or_else(|_| reqwest::Client::new()),
            consecutive_failures: std::sync::Arc::new(AtomicU32::new(0)),
            retry_budget,
            #[cfg(any(test, feature = "test-support"))]
            base_url_override: None,
        }
    }

    /// Override the run-level [`RetryBudget`](rocky_core::retry_budget::RetryBudget).
    /// Used by `rocky run` when multiple adapters should share one budget; not
    /// required for single-adapter setups.
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

    /// Returns the base URL for API calls.
    fn api_base_url(&self) -> String {
        #[cfg(any(test, feature = "test-support"))]
        if let Some(url) = &self.base_url_override {
            return url.clone();
        }
        format!("https://{}", self.config.host)
    }

    /// Executes a SQL statement and waits for results.
    pub async fn execute_sql(&self, sql: &str) -> Result<QueryResult, ConnectorError> {
        let query_start = std::time::Instant::now();
        let response = self.submit_and_wait(sql).await?;
        rocky_observe::metrics::METRICS
            .record_query_duration_ms(query_start.elapsed().as_millis() as u64);
        rocky_observe::metrics::METRICS.inc_statements_executed();

        let total_row_count = response.manifest.as_ref().and_then(|m| m.total_row_count);

        let columns = response
            .manifest
            .and_then(|m| m.schema)
            .map(|s| s.columns)
            .unwrap_or_default();

        let rows = response
            .result
            .and_then(|r| r.data_array)
            .unwrap_or_default();

        Ok(QueryResult {
            statement_id: response.statement_id,
            columns,
            rows,
            total_row_count,
        })
    }

    /// Executes a SQL statement without caring about the result data.
    pub async fn execute_statement(&self, sql: &str) -> Result<String, ConnectorError> {
        let response = self.submit_and_wait(sql).await?;
        Ok(response.statement_id)
    }

    async fn submit_and_wait(&self, sql: &str) -> Result<StatementResponse, ConnectorError> {
        let retry = &self.config.retry;

        // Circuit breaker: fail fast if too many consecutive transient failures
        let cb_threshold = retry.circuit_breaker_threshold;
        if cb_threshold > 0 {
            let failures = self.consecutive_failures.load(Ordering::Relaxed);
            if failures >= cb_threshold {
                return Err(ConnectorError::CircuitBreakerOpen {
                    consecutive_failures: failures,
                });
            }
        }

        for attempt in 0..=retry.max_retries {
            match self.submit_and_wait_once(sql).await {
                Ok(resp) => {
                    // Reset circuit breaker on success
                    self.consecutive_failures.store(0, Ordering::Relaxed);
                    if attempt > 0 {
                        rocky_observe::metrics::METRICS.inc_retries_succeeded();
                    }
                    return Ok(resp);
                }
                Err(err) => {
                    if is_transient(&err) {
                        self.consecutive_failures.fetch_add(1, Ordering::Relaxed);
                    }
                    if attempt < retry.max_retries && is_transient(&err) {
                        // Run-level retry budget (§P2.7). If exhausted, abort
                        // remaining retries so one bad statement can't drain
                        // the adapter's rate-limit quota for the rest of the
                        // run. Unbounded budgets always consume successfully.
                        if !self.retry_budget.try_consume() {
                            let limit = self.retry_budget.total().unwrap_or(0);
                            warn!(
                                attempt = attempt + 1,
                                max_retries = retry.max_retries,
                                budget_limit = limit,
                                error = %err,
                                "retry budget exhausted for this run; aborting further retries",
                            );
                            return Err(ConnectorError::RetryBudgetExhausted { limit });
                        }
                        // §P2.8 emit site: retry-about-to-fire with structured
                        // attempt / classification so event-bus subscribers
                        // (Dagster, dashboards) can tell "retry 2/5" from
                        // terminal failure without string-matching.
                        global_event_bus().emit(
                            PipelineEvent::new("statement_retry")
                                .with_error(err.to_string())
                                .with_attempt(attempt + 1, retry.max_retries)
                                .with_error_class(classify_error(&err)),
                        );
                        rocky_observe::metrics::METRICS.inc_retries_attempted();
                        // 401/403 typically mean the OAuth token expired
                        // between cache-mint and server-use. Databricks
                        // returns 403 "Invalid Token" from some SQL
                        // endpoints where 401 would be more idiomatic.
                        // Drop the cache so the next attempt triggers a
                        // fresh token exchange; genuine bad credentials
                        // re-fail and exhaust retries.
                        if matches!(
                            &err,
                            ConnectorError::ApiError { status: 401 | 403, .. }
                        ) {
                            self.auth.invalidate_cache().await;
                        }
                        let backoff_ms = compute_backoff(retry, attempt);
                        warn!(
                            attempt = attempt + 1,
                            max_retries = retry.max_retries,
                            backoff_ms = backoff_ms,
                            error = %err,
                            "transient error, retrying"
                        );
                        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                        continue;
                    }
                    return Err(err);
                }
            }
        }

        unreachable!("retry loop should always return")
    }

    async fn submit_and_wait_once(&self, sql: &str) -> Result<StatementResponse, ConnectorError> {
        let token = self.auth.get_token().await?;
        let url = format!("{}/api/2.0/sql/statements", self.api_base_url());

        debug!(sql = sql, "submitting SQL statement");

        let body = SubmitRequest {
            warehouse_id: self.config.warehouse_id.clone(),
            statement: sql.to_string(),
            wait_timeout: "30s".to_string(),
            disposition: "INLINE".to_string(),
            format: "JSON_ARRAY".to_string(),
        };

        let resp = self
            .client
            .post(&url)
            .bearer_auth(&token)
            .json(&body)
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status().as_u16();
            let body = resp.text().await.unwrap_or_default();
            return Err(ConnectorError::ApiError { status, body });
        }

        let mut response: StatementResponse = resp.json().await?;

        // If the statement completed inline, return immediately
        if is_terminal(&response.status.state) {
            return check_terminal_state(response);
        }

        // Poll until complete
        let poll_url = format!(
            "{}/api/2.0/sql/statements/{}",
            self.api_base_url(),
            response.statement_id
        );
        let deadline = tokio::time::Instant::now() + self.config.timeout;
        let mut step = 0;

        loop {
            if tokio::time::Instant::now() > deadline {
                return Err(ConnectorError::Timeout {
                    id: response.statement_id,
                    seconds: self.config.timeout.as_secs(),
                });
            }

            let delay = poll_delay(step);
            tokio::time::sleep(delay).await;
            step += 1;

            debug!(
                statement_id = response.statement_id,
                state = response.status.state,
                "polling statement"
            );

            let token = self.auth.get_token().await?;
            let poll_resp = self
                .client
                .get(&poll_url)
                .bearer_auth(&token)
                .send()
                .await?;

            if !poll_resp.status().is_success() {
                let status = poll_resp.status().as_u16();
                let body = poll_resp.text().await.unwrap_or_default();
                return Err(ConnectorError::ApiError { status, body });
            }

            response = poll_resp.json().await?;

            if is_terminal(&response.status.state) {
                return check_terminal_state(response);
            }
        }
    }

    /// Cancels a running statement.
    pub async fn cancel_statement(&self, statement_id: &str) -> Result<(), ConnectorError> {
        let token = self.auth.get_token().await?;
        let url = format!(
            "{}/api/2.0/sql/statements/{}/cancel",
            self.api_base_url(),
            statement_id
        );

        self.client
            .post(&url)
            .bearer_auth(&token)
            .send()
            .await?
            .error_for_status()?;

        Ok(())
    }
}

fn is_terminal(state: &str) -> bool {
    matches!(state, "SUCCEEDED" | "FAILED" | "CANCELED" | "CLOSED")
}

fn check_terminal_state(response: StatementResponse) -> Result<StatementResponse, ConnectorError> {
    match response.status.state.as_str() {
        "SUCCEEDED" => Ok(response),
        "FAILED" => {
            let message = response
                .status
                .error
                .as_ref()
                .and_then(|e| e.message.as_deref())
                .unwrap_or("unknown error")
                .to_string();
            warn!(
                statement_id = response.statement_id,
                error = message,
                "statement failed"
            );
            Err(ConnectorError::StatementFailed {
                id: response.statement_id,
                message,
            })
        }
        "CANCELED" => Err(ConnectorError::Canceled {
            id: response.statement_id,
        }),
        state => Err(ConnectorError::UnexpectedState {
            id: response.statement_id,
            state: state.to_string(),
        }),
    }
}

/// Classifies whether a connector error is transient and worth retrying.
///
/// Transient errors include:
/// - HTTP 429 (Too Many Requests), 502/503/504 (server errors)
/// - Network connection or timeout errors
/// - Databricks-specific: warehouse starting, rate limits, temporary unavailability
/// - Statement execution timeouts (may succeed on retry with a warmed-up warehouse)
fn is_transient(err: &ConnectorError) -> bool {
    match err {
        // 401 and 403 are transient-on-first-retry — the connector drops
        // the OAuth cache before retrying, so a server-expired token is
        // replaced with a fresh exchange. Bad credentials re-fail on
        // every attempt. Databricks's SQL Statement Execution API
        // returns 403 "Invalid Token" where 401 would be more idiomatic;
        // both paths must flow through the token-refresh branch or
        // long-running operations die at the 1-hour OAuth TTL boundary.
        ConnectorError::ApiError { status, .. } => {
            matches!(status, 401 | 403 | 429 | 502 | 503 | 504)
        }
        ConnectorError::Http(e) => e.is_connect() || e.is_timeout(),
        ConnectorError::StatementFailed { message, .. } => {
            let msg = message.to_uppercase();
            msg.contains("TEMPORARILY_UNAVAILABLE")
                || msg.contains("UC_REQUEST_LIMIT_EXCEEDED")
                || msg.contains("DEADLINE_EXCEEDED")
                || msg.contains("WAREHOUSE IS STARTING")
                || msg.contains("INVALID OPERATIONHANDLE")
                || msg.contains("CONNECTION RESET")
                || msg.contains("RESOURCE_DOES_NOT_EXIST")
        }
        ConnectorError::Timeout { .. } => true,
        ConnectorError::Auth(_)
        | ConnectorError::Canceled { .. }
        | ConnectorError::UnexpectedState { .. }
        | ConnectorError::CircuitBreakerOpen { .. }
        | ConnectorError::RetryBudgetExhausted { .. } => false,
    }
}

/// Maps a `ConnectorError` to the structured `ErrorClass` carried on
/// `PipelineEvent` — §P2.8. Feeds the event-bus emission from the retry
/// loop so downstream observers (metrics, Dagster) can distinguish
/// transient from terminal without string-matching.
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
        ConnectorError::Canceled { .. } | ConnectorError::UnexpectedState { .. } => {
            ErrorClass::Permanent
        }
    }
}

/// Fixed polling delays (ms) for the first 5 statement status checks.
const POLL_DELAY_STEPS_MS: [u64; 5] = [100, 200, 500, 1000, 2000];
/// Maximum polling delay after exponential growth.
const MAX_POLL_DELAY_MS: u64 = 5000;

/// Computes the polling delay for statement status checks.
///
/// Uses fixed steps for the first 5 polls, then exponential growth capped at [`MAX_POLL_DELAY_MS`].
fn poll_delay(attempt: usize) -> Duration {
    let delay_ms = if attempt < POLL_DELAY_STEPS_MS.len() {
        POLL_DELAY_STEPS_MS[attempt]
    } else {
        let extra = attempt - POLL_DELAY_STEPS_MS.len();
        (2000 * (1 << extra.min(3))).min(MAX_POLL_DELAY_MS)
    };
    Duration::from_millis(delay_ms)
}

/// Computes backoff duration with exponential growth, capped at max, with optional jitter.
fn compute_backoff(cfg: &RetryConfig, attempt: u32) -> u64 {
    let base = (cfg.initial_backoff_ms as f64) * cfg.backoff_multiplier.powi(attempt as i32);
    let capped = base.min(cfg.max_backoff_ms as f64) as u64;

    if cfg.jitter {
        // Use subsecond nanos as cheap jitter source (no rand dependency needed)
        let jitter_range = capped / 4; // +-25% jitter
        let nanos = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos() as u64;
        let jitter = nanos % (jitter_range.max(1));
        capped
            .saturating_sub(jitter_range / 2)
            .saturating_add(jitter)
    } else {
        capped
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_warehouse_id_from_http_path() {
        assert_eq!(
            ConnectorConfig::warehouse_id_from_http_path("/sql/1.0/warehouses/fd659821e786e7ad"),
            Some("fd659821e786e7ad".to_string())
        );
    }

    #[test]
    fn test_warehouse_id_from_simple_path() {
        assert_eq!(
            ConnectorConfig::warehouse_id_from_http_path("abc123"),
            Some("abc123".to_string())
        );
    }

    #[test]
    fn test_is_terminal_states() {
        assert!(is_terminal("SUCCEEDED"));
        assert!(is_terminal("FAILED"));
        assert!(is_terminal("CANCELED"));
        assert!(is_terminal("CLOSED"));
        assert!(!is_terminal("PENDING"));
        assert!(!is_terminal("RUNNING"));
    }

    #[test]
    fn test_check_succeeded() {
        let resp = StatementResponse {
            statement_id: "id-1".into(),
            status: StatementStatus {
                state: "SUCCEEDED".into(),
                error: None,
            },
            manifest: None,
            result: None,
        };
        assert!(check_terminal_state(resp).is_ok());
    }

    #[test]
    fn test_check_failed() {
        let resp = StatementResponse {
            statement_id: "id-1".into(),
            status: StatementStatus {
                state: "FAILED".into(),
                error: Some(StatusError {
                    error_code: Some("PARSE_ERROR".into()),
                    message: Some("syntax error".into()),
                }),
            },
            manifest: None,
            result: None,
        };
        let err = check_terminal_state(resp).unwrap_err();
        assert!(matches!(err, ConnectorError::StatementFailed { .. }));
    }

    #[test]
    fn test_check_canceled() {
        let resp = StatementResponse {
            statement_id: "id-1".into(),
            status: StatementStatus {
                state: "CANCELED".into(),
                error: None,
            },
            manifest: None,
            result: None,
        };
        assert!(matches!(
            check_terminal_state(resp),
            Err(ConnectorError::Canceled { .. })
        ));
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
        // 401 is now treated as transient so the retry loop can drop the
        // OAuth token cache and re-exchange. Bad credentials re-fail and
        // exhaust retries quickly.
        let err = ConnectorError::ApiError {
            status: 401,
            body: "unauthorized".into(),
        };
        assert!(is_transient(&err));
    }

    #[test]
    fn test_transient_http_403_invalid_token() {
        // Databricks's SQL Statement Execution API returns 403 "Invalid
        // Token" where 401 would be more idiomatic. Without this in the
        // transient set, the connector never drops the expired OAuth
        // token and every subsequent call fails for the rest of the run
        // (seen on a dedup sweep that crossed the 1-hour OAuth TTL
        // boundary mid-execution).
        let err = ConnectorError::ApiError {
            status: 403,
            body: "Invalid Token".into(),
        };
        assert!(is_transient(&err));
    }

    #[test]
    fn test_transient_statement_rate_limit() {
        let err = ConnectorError::StatementFailed {
            id: "id-1".into(),
            message: "UC_REQUEST_LIMIT_EXCEEDED: rate limit".into(),
        };
        assert!(is_transient(&err));
    }

    #[test]
    fn test_transient_warehouse_starting() {
        let err = ConnectorError::StatementFailed {
            id: "id-1".into(),
            message: "Warehouse is starting up, please retry".into(),
        };
        assert!(is_transient(&err));
    }

    #[test]
    fn test_not_transient_syntax_error() {
        let err = ConnectorError::StatementFailed {
            id: "id-1".into(),
            message: "PARSE_ERROR: syntax error at position 5".into(),
        };
        assert!(!is_transient(&err));
    }

    #[test]
    fn test_transient_timeout() {
        let err = ConnectorError::Timeout {
            id: "id-1".into(),
            seconds: 120,
        };
        assert!(is_transient(&err));
    }

    #[test]
    fn test_not_transient_canceled() {
        let err = ConnectorError::Canceled { id: "id-1".into() };
        assert!(!is_transient(&err));
    }

    #[test]
    fn test_backoff_exponential_no_jitter() {
        let cfg = RetryConfig {
            max_retries: 3,
            initial_backoff_ms: 1000,
            max_backoff_ms: 30000,
            backoff_multiplier: 2.0,
            jitter: false,
            ..Default::default()
        };
        assert_eq!(compute_backoff(&cfg, 0), 1000); // 1000 * 2^0
        assert_eq!(compute_backoff(&cfg, 1), 2000); // 1000 * 2^1
        assert_eq!(compute_backoff(&cfg, 2), 4000); // 1000 * 2^2
    }

    #[test]
    fn test_backoff_capped() {
        let cfg = RetryConfig {
            max_retries: 5,
            initial_backoff_ms: 1000,
            max_backoff_ms: 5000,
            backoff_multiplier: 2.0,
            jitter: false,
            ..Default::default()
        };
        assert_eq!(compute_backoff(&cfg, 3), 5000); // 1000 * 2^3 = 8000, capped at 5000
        assert_eq!(compute_backoff(&cfg, 4), 5000); // still capped
    }

    #[test]
    fn test_backoff_with_jitter_in_range() {
        let cfg = RetryConfig {
            max_retries: 3,
            initial_backoff_ms: 1000,
            max_backoff_ms: 30000,
            backoff_multiplier: 2.0,
            jitter: true,
            ..Default::default()
        };
        // With jitter, result should be within +-25% of base (1000)
        let result = compute_backoff(&cfg, 0);
        assert!(result >= 750, "backoff {result} should be >= 750");
        assert!(result <= 1250, "backoff {result} should be <= 1250");
    }

    #[test]
    fn test_circuit_breaker_not_transient() {
        let err = ConnectorError::CircuitBreakerOpen {
            consecutive_failures: 5,
        };
        assert!(!is_transient(&err));
    }
}
