//! Snowflake SQL REST API v2 connector.
//!
//! Executes SQL statements against Snowflake using the SQL REST API:
//! - Submit: `POST https://{account}.snowflakecomputing.com/api/v2/statements`
//! - Poll: `GET https://{account}.snowflakecomputing.com/api/v2/statements/{handle}`
//! - Cancel: `POST https://{account}.snowflakecomputing.com/api/v2/statements/{handle}/cancel`
//!
//! Supports retry with exponential backoff for transient failures.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, SystemTime};

use reqwest::Client;
use rocky_core::config::RetryConfig;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{debug, warn};

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
    /// Consecutive transient failures (shared across clones for circuit breaker).
    consecutive_failures: Arc<AtomicU32>,
    /// Override for the base URL (used by tests to point at wiremock).
    #[cfg(any(test, feature = "test-support"))]
    base_url_override: Option<String>,
}

impl SnowflakeConnector {
    /// Creates a new connector with the given configuration and auth provider.
    pub fn new(config: ConnectorConfig, auth: Auth) -> Self {
        SnowflakeConnector {
            config,
            auth,
            client: Client::builder()
                .tcp_nodelay(true)
                .pool_max_idle_per_host(32)
                .pool_idle_timeout(std::time::Duration::from_secs(300))
                .build()
                .unwrap_or_else(|_| Client::new()),
            consecutive_failures: Arc::new(AtomicU32::new(0)),
            #[cfg(any(test, feature = "test-support"))]
            base_url_override: None,
        }
    }

    /// Creates a connector that points at a custom base URL (for testing with wiremock).
    #[cfg(any(test, feature = "test-support"))]
    #[must_use]
    pub fn with_base_url(mut self, base_url: String) -> Self {
        self.base_url_override = Some(base_url);
        self
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
                    self.consecutive_failures.store(0, Ordering::Relaxed);
                    return Ok(resp);
                }
                Err(err) => {
                    if is_transient(&err) {
                        self.consecutive_failures.fetch_add(1, Ordering::Relaxed);
                    }
                    if attempt < retry.max_retries && is_transient(&err) {
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

        let resp = self
            .client
            .post(&url)
            .bearer_auth(&token)
            .header("Content-Type", "application/json")
            .header("Accept", "application/json")
            .header("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT")
            .json(&body)
            .send()
            .await?;

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
            let poll_resp = self
                .client
                .get(&poll_url)
                .bearer_auth(&token)
                .header("Accept", "application/json")
                .send()
                .await?;

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

        self.client
            .post(&url)
            .bearer_auth(&token)
            .header("Accept", "application/json")
            .send()
            .await?
            .error_for_status()
            .map_err(ConnectorError::Http)?;

        Ok(())
    }
}

/// Snowflake SQL API status codes:
/// - "00000" = success
/// - "333334" = async, still executing
/// - Anything else starting with a non-zero digit = error
fn is_terminal_code(code: &str) -> bool {
    // Async-in-progress is the only non-terminal state
    code != "333334" && !code.is_empty()
}

fn check_terminal(response: StatementResponse) -> Result<StatementResponse, ConnectorError> {
    // "00000" is the SQL success SQLSTATE
    if response.code == "00000" {
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
        ConnectorError::Auth(_) | ConnectorError::CircuitBreakerOpen { .. } => false,
    }
}

/// Computes backoff duration with exponential growth, capped at max, with optional jitter.
fn compute_backoff(cfg: &RetryConfig, attempt: u32) -> u64 {
    let base = (cfg.initial_backoff_ms as f64) * cfg.backoff_multiplier.powi(attempt as i32);
    let capped = base.min(cfg.max_backoff_ms as f64) as u64;

    if cfg.jitter {
        let jitter_range = capped / 4;
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
        assert_eq!(compute_backoff(&cfg, 0), 1000);
        assert_eq!(compute_backoff(&cfg, 1), 2000);
        assert_eq!(compute_backoff(&cfg, 2), 4000);
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
        assert_eq!(compute_backoff(&cfg, 3), 5000);
        assert_eq!(compute_backoff(&cfg, 4), 5000);
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
        let result = compute_backoff(&cfg, 0);
        assert!(result >= 750, "backoff {result} should be >= 750");
        assert!(result <= 1250, "backoff {result} should be <= 1250");
    }
}
