use rocky_core::circuit_breaker::{CircuitBreaker, TransitionOutcome};
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use futures::stream::{self, StreamExt, TryStreamExt};
use reqwest::Client;
use rocky_core::config::RetryConfig;
use rocky_core::retry::compute_backoff;
use rocky_observe::events::{
    ErrorClass, PipelineEvent, global_event_bus, record_span_event, set_current_span_error,
};
use rocky_observe::span_attrs;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{Instrument, Span, debug, field, info_span, warn};

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
    CircuitBreakerOpen {
        consecutive_failures: u32,
        /// Initial half-open recovery timeout the underlying
        /// [`CircuitBreaker`] honours before letting a trial request
        /// through, in whole seconds. Populated from
        /// [`CircuitBreaker::recovery_timeout`] when configured (i.e.
        /// `retry.circuit_breaker_recovery_timeout_secs` is `Some`);
        /// `None` for manual-reset-only breakers. Surfaces on
        /// `TableErrorOutput.cooldown_seconds` so orchestrators can
        /// derive a `retry_after` hint without re-parsing config —
        /// mirrors the Fivetran source-adapter shape.
        cooldown_seconds: Option<u64>,
    },

    #[error("run-level retry budget exhausted (limit {limit}); aborting remaining retries")]
    RetryBudgetExhausted { limit: u32 },

    /// The statement returned no Arrow chunks. Surfaces from
    /// [`DatabricksConnector::execute_sql_arrow`] when the warehouse
    /// produced no result payload (e.g. DDL routed through the Arrow
    /// path) — the trait method `fetch_arrow_batch` is documented as
    /// returning a `RecordBatch`, so we surface this as an error
    /// rather than synthesizing an empty schema.
    #[error("statement {id} returned no Arrow chunks")]
    NoArrowChunks { id: String },

    /// Arrow IPC stream decode failure on a chunk. Wraps the
    /// underlying `arrow::error::ArrowError` message.
    #[error("Arrow IPC decode error: {0}")]
    Arrow(String),
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
    /// Shared circuit breaker (clones share state via `Arc`). See
    /// `rocky_core::circuit_breaker::CircuitBreaker` for the timed
    /// half-open recovery semantics.
    circuit_breaker: std::sync::Arc<CircuitBreaker>,
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

/// Result-payload shape requested from the Statement Execution API.
///
/// Databricks pairs `disposition` × `format` rigidly: `INLINE` only
/// supports `JSON_ARRAY`, and `ARROW_STREAM` is only accepted with
/// `EXTERNAL_LINKS`. We expose the two paths the connector uses.
#[derive(Debug, Clone, Copy)]
enum ResultFormat {
    /// `disposition=INLINE, format=JSON_ARRAY` — the legacy path used by
    /// every non-Arrow connector entry point. Result rows arrive in the
    /// initial response body as a `data_array` of JSON cells.
    InlineJson,
    /// `disposition=EXTERNAL_LINKS, format=ARROW_STREAM` — used by
    /// [`DatabricksConnector::execute_sql_arrow`]. The response carries
    /// pre-signed URLs to chunked Arrow IPC stream files in cloud
    /// storage (S3 / ADLS / GCS, depending on the workspace cloud).
    ExternalArrow,
}

impl ResultFormat {
    fn disposition(self) -> &'static str {
        match self {
            Self::InlineJson => "INLINE",
            Self::ExternalArrow => "EXTERNAL_LINKS",
        }
    }

    fn format(self) -> &'static str {
        match self {
            Self::InlineJson => "JSON_ARRAY",
            Self::ExternalArrow => "ARROW_STREAM",
        }
    }
}

/// A pre-signed external link to one Arrow IPC stream chunk.
///
/// Databricks returns these on `disposition=EXTERNAL_LINKS` responses;
/// the URL is a cloud-storage pre-signed GET that must NOT carry the
/// Databricks Bearer token (the storage layer rejects it).
#[derive(Debug, Clone, Deserialize)]
pub struct ExternalLink {
    pub chunk_index: u64,
    pub row_offset: Option<u64>,
    pub row_count: Option<u64>,
    pub byte_count: Option<u64>,
    pub external_link: String,
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

/// Result manifest containing schema and row / byte count metadata.
#[derive(Debug, Clone, Deserialize)]
pub struct Manifest {
    pub schema: Option<ManifestSchema>,
    pub total_row_count: Option<u64>,
    /// Total bytes in the statement result payload, aggregated across all
    /// chunks. Populated for SUCCEEDED `SELECT`s that return data;
    /// Databricks omits the field (and we see `None`) for DDL / DML
    /// where the response has no result payload. Surfaced into
    /// [`rocky_core::traits::ExecutionStats::bytes_scanned`] — see
    /// [`stats_from_response`] for the semantic-impurity note.
    pub total_byte_count: Option<u64>,
    /// Total chunk count across the result set. Populated on
    /// `disposition=EXTERNAL_LINKS` responses; `None` on the inline
    /// JSON path where chunking does not apply.
    #[serde(default)]
    pub total_chunk_count: Option<u64>,
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

/// Inline result data from a SQL statement (JSON_ARRAY format) or the
/// initial batch of pre-signed external-link descriptors
/// (`ARROW_STREAM` / `EXTERNAL_LINKS`). Databricks populates exactly
/// one of the two fields per response.
#[derive(Debug, Clone, Deserialize)]
pub struct ResultData {
    pub data_array: Option<Vec<Vec<serde_json::Value>>>,
    /// Initial batch of pre-signed URLs returned alongside the response
    /// when `disposition=EXTERNAL_LINKS`. Holds at most the chunks
    /// Databricks chose to inline in the submit/poll reply; remaining
    /// chunks are pulled via `GET /api/2.0/sql/statements/{id}/result/chunks/{i}`.
    #[serde(default)]
    pub external_links: Option<Vec<ExternalLink>>,
}

/// Response from `GET /api/2.0/sql/statements/{id}/result/chunks/{chunk_index}`.
///
/// Used to walk the chunk listing forward when the initial submit/poll
/// reply only returned a subset of `manifest.total_chunk_count` links.
#[derive(Debug, Deserialize)]
struct ChunkResponse {
    #[serde(default)]
    external_links: Vec<ExternalLink>,
}

/// The result of executing a SQL statement.
#[derive(Debug)]
pub struct QueryResult {
    pub statement_id: String,
    pub columns: Vec<ColumnSchema>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub total_row_count: Option<u64>,
}

/// Table-level byte statistics returned by `DESCRIBE DETAIL`.
///
/// `DESCRIBE DETAIL` always populates `sizeInBytes` for Delta tables;
/// row count is omitted here because Databricks does not expose it without
/// a prior `ANALYZE TABLE`.
#[derive(Debug, Clone)]
pub struct DescribeDetailStats {
    /// Total size of all data files in the current Delta snapshot, in bytes.
    /// `None` when the value was absent or NULL in the `DESCRIBE DETAIL` result.
    pub size_bytes: Option<u64>,
}

impl DatabricksConnector {
    /// Creates a new connector with the given configuration and auth provider.
    pub fn new(config: ConnectorConfig, auth: Auth) -> Self {
        let retry_budget =
            rocky_core::retry_budget::RetryBudget::from_config(config.retry.max_retries_per_run);
        let circuit_breaker = std::sync::Arc::new(config.retry.build_circuit_breaker());
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
            circuit_breaker,
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

    /// Executes a SQL statement and returns the warehouse-reported
    /// [`ExecutionStats`] for it — the stats-aware counterpart to
    /// [`Self::execute_statement`]. Feeds
    /// [`WarehouseAdapter::execute_statement_with_stats`] on
    /// [`crate::adapter::DatabricksWarehouseAdapter`].
    pub async fn execute_statement_with_stats(
        &self,
        sql: &str,
    ) -> Result<rocky_core::traits::ExecutionStats, ConnectorError> {
        let response = self.submit_and_wait(sql).await?;
        Ok(stats_from_response(&response))
    }

    /// Execute `sql` and return a single workspace
    /// [`arrow::record_batch::RecordBatch`] for the full result set.
    ///
    /// Submits via `disposition=EXTERNAL_LINKS, format=ARROW_STREAM` —
    /// Databricks rejects `disposition=INLINE` paired with
    /// `format=ARROW_STREAM`, so EXTERNAL_LINKS is the only Arrow path.
    /// The connector:
    ///
    /// 1. Submits the statement through the existing breaker + retry +
    ///    span-attribute path (`submit_and_wait_with_format`).
    /// 2. Walks `manifest.total_chunk_count` forward, paging through
    ///    `GET /api/2.0/sql/statements/{id}/result/chunks/{i}` whenever
    ///    Databricks did not inline every link in the initial reply.
    /// 3. Fetches every chunk URL concurrently (cap: 4) — the URLs are
    ///    pre-signed cloud-storage GETs that must NOT carry the
    ///    Databricks Bearer token (the storage layer rejects it), so
    ///    each chunk is fetched on the bare `reqwest::Client` without
    ///    `.bearer_auth(...)`.
    /// 4. Decodes each chunk via `arrow::ipc::reader::StreamReader` and
    ///    concatenates the per-chunk `RecordBatch` slices into a single
    ///    `RecordBatch` via `arrow::compute::concat_batches`.
    ///
    /// Backs [`crate::adapter::DatabricksWarehouseAdapter::fetch_arrow_batch`].
    ///
    /// # Errors
    ///
    /// - [`ConnectorError::NoArrowChunks`] when the statement succeeds
    ///   but the warehouse returns no chunks (e.g. DDL accidentally
    ///   routed through the Arrow path).
    /// - [`ConnectorError::Arrow`] for IPC stream decode / concat
    ///   failures.
    /// - All transport / API / auth / retry-budget errors that the
    ///   underlying submit-and-wait path can surface.
    pub async fn execute_sql_arrow(&self, sql: &str) -> Result<RecordBatch, ConnectorError> {
        let query_start = std::time::Instant::now();
        let response = self
            .submit_and_wait_with_format(sql, ResultFormat::ExternalArrow)
            .await?;
        rocky_observe::metrics::METRICS
            .record_query_duration_ms(query_start.elapsed().as_millis() as u64);
        rocky_observe::metrics::METRICS.inc_statements_executed();

        let statement_id = response.statement_id.clone();
        let total_chunk_count = response
            .manifest
            .as_ref()
            .and_then(|m| m.total_chunk_count)
            .unwrap_or(0);

        // Gather every pre-signed URL. The submit/poll reply inlines the
        // first batch on `result.external_links`; the rest, if any, are
        // pulled via `GET .../result/chunks/{index}` until we've covered
        // `manifest.total_chunk_count`.
        let mut links: Vec<ExternalLink> = response
            .result
            .and_then(|r| r.external_links)
            .unwrap_or_default();

        if total_chunk_count == 0 || links.is_empty() {
            return Err(ConnectorError::NoArrowChunks { id: statement_id });
        }

        let known_max = links.iter().map(|l| l.chunk_index).max().unwrap_or(0);
        let mut next_chunk = known_max + 1;
        while (links.len() as u64) < total_chunk_count {
            let extra = self
                .fetch_result_chunk_links(&statement_id, next_chunk)
                .await?;
            if extra.is_empty() {
                // Server gave us no forward progress; bail to avoid
                // looping. Mirrors the manifest-vs-links sanity check
                // the Databricks SDK applies on this path.
                return Err(ConnectorError::Arrow(format!(
                    "statement {statement_id}: expected {total_chunk_count} chunks, \
                     got {got} and the chunks endpoint returned no further links",
                    got = links.len(),
                )));
            }
            next_chunk = extra
                .iter()
                .map(|l| l.chunk_index)
                .max()
                .unwrap_or(next_chunk)
                + 1;
            links.extend(extra);
        }

        // Fetch every chunk in parallel (cap concurrency at 4 to avoid
        // hammering the storage layer or the local socket pool).
        let client = self.client.clone();
        let chunk_bytes: Vec<bytes::Bytes> = stream::iter(links)
            .map(|link| {
                let client = client.clone();
                async move { fetch_external_chunk(&client, &link).await }
            })
            .buffered(4)
            .try_collect()
            .await?;

        decode_arrow_chunks(&statement_id, chunk_bytes)
    }

    /// Page through `GET /api/2.0/sql/statements/{id}/result/chunks/{chunk_index}`
    /// to retrieve pre-signed URLs that Databricks did not inline on the
    /// initial submit/poll reply. This call IS authenticated against
    /// Databricks (unlike the chunk URLs themselves, which hit cloud
    /// storage). No retry wrapping here — the parent `submit_and_wait`
    /// already protected the submit/poll cycle.
    async fn fetch_result_chunk_links(
        &self,
        statement_id: &str,
        chunk_index: u64,
    ) -> Result<Vec<ExternalLink>, ConnectorError> {
        let token = self.auth.get_token().await?;
        let url = format!(
            "{}/api/2.0/sql/statements/{statement_id}/result/chunks/{chunk_index}",
            self.api_base_url(),
        );
        let resp = self.client.get(&url).bearer_auth(&token).send().await?;
        if !resp.status().is_success() {
            let status = resp.status().as_u16();
            let body = resp.text().await.unwrap_or_default();
            return Err(ConnectorError::ApiError { status, body });
        }
        let body: ChunkResponse = resp.json().await?;
        Ok(body.external_links)
    }

    async fn submit_and_wait(&self, sql: &str) -> Result<StatementResponse, ConnectorError> {
        self.submit_and_wait_with_format(sql, ResultFormat::InlineJson)
            .await
    }

    /// Run the submit/poll/retry path with a caller-chosen disposition × format.
    ///
    /// All existing JSON-row callers stay on `ResultFormat::InlineJson`
    /// via [`Self::submit_and_wait`]; the Arrow path uses
    /// `ResultFormat::ExternalArrow` to request pre-signed chunk URLs.
    async fn submit_and_wait_with_format(
        &self,
        sql: &str,
        result_format: ResultFormat,
    ) -> Result<StatementResponse, ConnectorError> {
        // Span attribute schema unification: every adapter wraps its
        // retry-loop in a `statement.execute` span carrying the
        // canonical `rocky.*` keys so OTel collectors see consistent
        // attributes across Databricks / Snowflake / BigQuery. The
        // bespoke `adapter` / `statement.kind` names emit alongside
        // for one release so existing dashboards see no breakage.
        // Follow-up PRs drop the aliases. See
        // `rocky_observe::span_attrs`.
        //
        // `rocky.warehouse.query_id` / `bytes_scanned` /
        // `retry.attempt` start empty; success path records them
        // once the warehouse manifest lands.
        let span = info_span!(
            "statement.execute",
            adapter = "databricks",
            statement.kind = classify_statement_kind(sql),
            "rocky.adapter.name" = "databricks",
            "rocky.statement.kind" = classify_statement_kind(sql),
            "rocky.warehouse.name" = %self.config.warehouse_id,
            "rocky.warehouse.query_id" = field::Empty,
            "rocky.warehouse.bytes_scanned" = field::Empty,
            "rocky.retry.attempt" = field::Empty,
        );
        self.submit_and_wait_inner(sql, result_format)
            .instrument(span)
            .await
    }

    async fn submit_and_wait_inner(
        &self,
        sql: &str,
        result_format: ResultFormat,
    ) -> Result<StatementResponse, ConnectorError> {
        let retry = &self.config.retry;

        // Circuit breaker: fail fast if Open. When timed half-open
        // recovery is configured, `check()` transitions Open → HalfOpen
        // after `circuit_breaker_recovery_timeout_secs` and lets one
        // trial request through — success below closes the breaker,
        // failure re-opens.
        if let Err(e) = self.circuit_breaker.check() {
            return Err(ConnectorError::CircuitBreakerOpen {
                consecutive_failures: e.consecutive_failures,
                cooldown_seconds: self.circuit_breaker.recovery_timeout().map(|d| d.as_secs()),
            });
        }

        for attempt in 0..=retry.max_retries {
            match self.submit_and_wait_once(sql, result_format).await {
                Ok(resp) => {
                    if self.circuit_breaker.record_success() == TransitionOutcome::Recovered {
                        let evt = PipelineEvent::new("circuit_breaker_recovered")
                            .with_target("databricks");
                        record_span_event(&evt);
                        global_event_bus().emit(evt);
                    }
                    if attempt > 0 {
                        rocky_observe::metrics::METRICS.inc_retries_succeeded();
                    }
                    // Enrich the active `statement.execute` span with
                    // the warehouse-side query ID + bytes manifest +
                    // final attempt count now that Databricks has
                    // returned them.
                    let span = Span::current();
                    span.record(span_attrs::WAREHOUSE_QUERY_ID, resp.statement_id.as_str());
                    if let Some(bytes) = resp.manifest.as_ref().and_then(|m| m.total_byte_count) {
                        span.record(span_attrs::WAREHOUSE_BYTES_SCANNED, bytes);
                    }
                    span.record(span_attrs::RETRY_ATTEMPT, u64::from(attempt + 1));
                    return Ok(resp);
                }
                Err(err) => {
                    if is_transient(&err)
                        && self.circuit_breaker.record_failure(&err.to_string())
                            == TransitionOutcome::Tripped
                    {
                        let evt = PipelineEvent::new("circuit_breaker_tripped")
                            .with_target("databricks")
                            .with_error(err.to_string())
                            .with_error_class(classify_error(&err));
                        record_span_event(&evt);
                        global_event_bus().emit(evt);
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
                            // Mark the active OTel span as Error so trace
                            // viewers surface the run-level retry-budget
                            // exhaustion without dashboard-side log
                            // string-matching. No-op when `otel` is off.
                            set_current_span_error(format!(
                                "retry budget exhausted (limit {limit})"
                            ));
                            return Err(ConnectorError::RetryBudgetExhausted { limit });
                        }
                        // §P2.8 emit site: retry-about-to-fire with structured
                        // attempt / classification so event-bus subscribers
                        // (Dagster, dashboards) can tell "retry 2/5" from
                        // terminal failure without string-matching.
                        let evt = PipelineEvent::new("statement_retry")
                            .with_error(err.to_string())
                            .with_attempt(attempt + 1, retry.max_retries)
                            .with_error_class(classify_error(&err));
                        record_span_event(&evt);
                        global_event_bus().emit(evt);
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
                            ConnectorError::ApiError {
                                status: 401 | 403,
                                ..
                            }
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
                    // Retry exhausted (or non-transient error) — terminal
                    // failure. Mark the active OTel span as Error.
                    set_current_span_error(err.to_string());
                    return Err(err);
                }
            }
        }

        unreachable!("retry loop should always return")
    }

    async fn submit_and_wait_once(
        &self,
        sql: &str,
        result_format: ResultFormat,
    ) -> Result<StatementResponse, ConnectorError> {
        let token = self.auth.get_token().await?;
        let url = format!("{}/api/2.0/sql/statements", self.api_base_url());

        debug!(sql = sql, "submitting SQL statement");

        let body = SubmitRequest {
            warehouse_id: self.config.warehouse_id.clone(),
            statement: sql.to_string(),
            wait_timeout: "30s".to_string(),
            disposition: result_format.disposition().to_string(),
            format: result_format.format().to_string(),
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

    /// Fetch table-level byte statistics using `DESCRIBE DETAIL`.
    ///
    /// Issues `DESCRIBE DETAIL <catalog>.<schema>.<table>` against the
    /// Databricks SQL warehouse and parses the `sizeInBytes` column from
    /// the response.  Row count is intentionally not populated here —
    /// Databricks does not expose `numRows` without a prior `ANALYZE TABLE`
    /// (which itself can be expensive).
    ///
    /// Returns `Ok(Some(...))` when the command succeeds and `sizeInBytes`
    /// is present in the result.  Returns `Ok(None)` when the table is not
    /// found or the column is absent.  Propagates [`ConnectorError`] on
    /// network or API failures.
    ///
    /// # Architecture note
    ///
    /// `UnityCatalogClient::table_stats` is REST-only by design — placing
    /// a SQL dependency inside that client would break the separation between
    /// the catalog REST layer and the SQL execution layer.  This method
    /// lives on `DatabricksConnector` (the SQL executor) instead, so callers
    /// that want real stats for a Databricks-hosted table can call this
    /// directly as a fallback when the REST path returns
    /// [`rocky_catalog_core::CatalogError::UnsupportedOperation`].
    pub async fn describe_detail_stats(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Result<Option<DescribeDetailStats>, ConnectorError> {
        let fqn = format!(
            "`{catalog}`.`{schema}`.`{table}`",
            catalog = catalog,
            schema = schema,
            table = table,
        );
        let sql = format!("DESCRIBE DETAIL {fqn}");

        let result = match self.execute_sql(&sql).await {
            Ok(r) => r,
            Err(ConnectorError::StatementFailed { message, .. })
                if message.to_uppercase().contains("TABLE_OR_VIEW_NOT_FOUND")
                    || message.to_uppercase().contains("DELTA_TABLE_NOT_FOUND")
                    || message.to_uppercase().contains("DOES NOT EXIST")
                    || message.to_uppercase().contains("NOT FOUND") =>
            {
                return Ok(None);
            }
            Err(e) => return Err(e),
        };

        // Find the column index for `sizeInBytes` in the response schema.
        let size_bytes_idx = result
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case("sizeInBytes"));

        let size_bytes = match (size_bytes_idx, result.rows.first()) {
            (Some(idx), Some(row)) => row.get(idx).and_then(|v| {
                if v.is_null() {
                    None
                } else if let Some(n) = v.as_u64() {
                    Some(n)
                } else {
                    v.as_str().and_then(|s| s.parse::<u64>().ok())
                }
            }),
            _ => None,
        };

        Ok(Some(DescribeDetailStats { size_bytes }))
    }

    /// Fetch an opaque change-marker for a table via `DESCRIBE DETAIL`.
    ///
    /// Combines `lastModified` (advances on every Delta commit / data write)
    /// with `numFiles` and `sizeInBytes` so any write moves the marker even in
    /// the (vanishingly rare) event of two commits sharing a millisecond
    /// `lastModified`. The replication runner's `prune_unchanged` pruning
    /// compares this for equality against the value recorded at the last
    /// successful copy.
    ///
    /// Returns `Ok(None)` when the table is absent, or when the response has no
    /// `lastModified` (a non-Delta table or a view) — the runner then always
    /// copies rather than risking a false skip. Propagates [`ConnectorError`]
    /// on network / API failures.
    pub async fn describe_detail_marker(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Result<Option<String>, ConnectorError> {
        let fqn = format!("`{catalog}`.`{schema}`.`{table}`");
        let sql = format!("DESCRIBE DETAIL {fqn}");

        let result = match self.execute_sql(&sql).await {
            Ok(r) => r,
            Err(ConnectorError::StatementFailed { message, .. })
                if message.to_uppercase().contains("TABLE_OR_VIEW_NOT_FOUND")
                    || message.to_uppercase().contains("DELTA_TABLE_NOT_FOUND")
                    || message.to_uppercase().contains("DOES NOT EXIST")
                    || message.to_uppercase().contains("NOT FOUND") =>
            {
                return Ok(None);
            }
            Err(e) => return Err(e),
        };

        let Some(row) = result.rows.first() else {
            return Ok(None);
        };
        let cell = |name: &str| -> Option<String> {
            let idx = result
                .columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(name))?;
            let v = row.get(idx)?;
            if v.is_null() {
                return None;
            }
            if let Some(s) = v.as_str() {
                return Some(s.to_string());
            }
            if let Some(n) = v.as_u64() {
                return Some(n.to_string());
            }
            if let Some(n) = v.as_i64() {
                return Some(n.to_string());
            }
            Some(v.to_string())
        };

        // `lastModified` is the primary signal; its absence means the object
        // has no Delta commit timestamp (view / non-Delta) — no reliable
        // marker, so fall back to "always copy".
        let Some(last_modified) = cell("lastModified") else {
            return Ok(None);
        };
        let marker = format!(
            "{last_modified}|{}|{}",
            cell("numFiles").unwrap_or_default(),
            cell("sizeInBytes").unwrap_or_default(),
        );
        Ok(Some(marker))
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

    /// Upload bytes to a Unity Catalog Volume path via the Files API.
    ///
    /// Issues `PUT /api/2.0/fs/files/<volume_path>?overwrite=true` with the
    /// file contents as the raw request body. `volume_path` is an absolute
    /// Volume path (`/Volumes/<catalog>/<schema>/<volume>/<file>`); the loader
    /// builds it from validated identifiers via
    /// [`crate::volume::StagingVolume`], so it never carries an unvalidated
    /// component.
    ///
    /// Like [`Self::cancel_statement`], this is a direct authenticated request
    /// — it does **not** flow through the SQL submit/poll/retry path or the
    /// circuit breaker (those are SQL-statement concerns). `overwrite=true`
    /// guards against the astronomically-unlikely case of a UUID-name
    /// collision on the shared volume.
    ///
    /// # Errors
    ///
    /// - [`ConnectorError::Auth`] when the token mint fails.
    /// - [`ConnectorError::Http`] on transport failure.
    /// - [`ConnectorError::ApiError`] on any non-2xx response (e.g. 404 when
    ///   the volume doesn't exist, 403 on missing write privilege).
    pub async fn upload_file(
        &self,
        volume_path: &str,
        contents: Vec<u8>,
    ) -> Result<(), ConnectorError> {
        let token = self.auth.get_token().await?;
        let url = format!(
            "{}/api/2.0/fs/files{}",
            self.api_base_url(),
            encode_volume_path(volume_path),
        );

        debug!(
            volume_path = volume_path,
            bytes = contents.len(),
            "Files API upload"
        );

        let resp = self
            .client
            .put(&url)
            .bearer_auth(&token)
            .query(&[("overwrite", "true")])
            .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
            .body(contents)
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status().as_u16();
            let body = resp.text().await.unwrap_or_default();
            return Err(ConnectorError::ApiError { status, body });
        }
        Ok(())
    }

    /// Delete a file from a Unity Catalog Volume path via the Files API.
    ///
    /// Issues `DELETE /api/2.0/fs/files/<volume_path>`. Used to clean up a
    /// staged file after `COPY INTO` (success or failure). `volume_path` is the
    /// same validated absolute path passed to [`Self::upload_file`].
    ///
    /// A `404 Not Found` is treated as success (the file is already gone — the
    /// post-condition "file no longer staged" holds), so a double-cleanup or a
    /// cleanup after a failed upload is a no-op rather than an error.
    ///
    /// # Errors
    ///
    /// - [`ConnectorError::Auth`] when the token mint fails.
    /// - [`ConnectorError::Http`] on transport failure.
    /// - [`ConnectorError::ApiError`] on any non-2xx response other than 404.
    pub async fn delete_file(&self, volume_path: &str) -> Result<(), ConnectorError> {
        let token = self.auth.get_token().await?;
        let url = format!(
            "{}/api/2.0/fs/files{}",
            self.api_base_url(),
            encode_volume_path(volume_path),
        );

        debug!(volume_path = volume_path, "Files API delete");

        let resp = self.client.delete(&url).bearer_auth(&token).send().await?;

        let status = resp.status();
        if status.is_success() || status.as_u16() == 404 {
            return Ok(());
        }
        let body = resp.text().await.unwrap_or_default();
        Err(ConnectorError::ApiError {
            status: status.as_u16(),
            body,
        })
    }
}

/// Percent-encode each path segment of an absolute Volume path while keeping
/// the `/` separators literal.
///
/// The loader only ever passes paths whose components were validated to
/// `[A-Za-z0-9._-]` (none of which require encoding), but encoding defensively
/// means a future caller can't smuggle a reserved character into the REST URL.
/// Splitting on `/` and re-joining preserves the path structure that
/// `format!("{base}/api/2.0/fs/files{path}")` depends on.
fn encode_volume_path(path: &str) -> String {
    path.split('/')
        .map(|seg| percent_encoding::utf8_percent_encode(seg, FILES_PATH_SEGMENT).to_string())
        .collect::<Vec<_>>()
        .join("/")
}

/// Byte set for percent-encoding one Files-API path segment: RFC 3986
/// unreserved characters (`-`, `.`, `_`, `~`, alphanumerics) pass through;
/// everything else (including `/`) is encoded. The separator `/` is handled by
/// [`encode_volume_path`] splitting before this is applied per-segment.
const FILES_PATH_SEGMENT: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

/// Fetch one chunk's Arrow IPC stream bytes from its pre-signed
/// external URL. The URL is a cloud-storage pre-signed GET (S3 /
/// Azure-blob / GCS, per the workspace cloud); it must NOT carry the
/// Databricks Bearer token — the storage layer either ignores it
/// (S3) or rejects the signature (Azure / GCS).
async fn fetch_external_chunk(
    client: &Client,
    link: &ExternalLink,
) -> Result<bytes::Bytes, ConnectorError> {
    let resp = client.get(&link.external_link).send().await?;
    if !resp.status().is_success() {
        let status = resp.status().as_u16();
        let body = resp.text().await.unwrap_or_default();
        return Err(ConnectorError::ApiError { status, body });
    }
    Ok(resp.bytes().await?)
}

/// Decode a vector of Arrow IPC stream chunks (each one is a full
/// self-describing stream starting with its own schema message) into a
/// single concatenated [`RecordBatch`].
///
/// Each Databricks chunk is independently `StreamReader`-decodable —
/// the schema is repeated at the head of every chunk, so we don't need
/// to stitch chunk bodies together at the byte level. We collect every
/// chunk's batches, then `concat_batches` against the first chunk's
/// schema. An empty input is treated as `NoArrowChunks` (defensive —
/// the caller already enforced this on the link list).
fn decode_arrow_chunks(
    statement_id: &str,
    chunks: Vec<bytes::Bytes>,
) -> Result<RecordBatch, ConnectorError> {
    use arrow::ipc::reader::StreamReader;

    if chunks.is_empty() {
        return Err(ConnectorError::NoArrowChunks {
            id: statement_id.to_string(),
        });
    }

    let mut all_batches: Vec<RecordBatch> = Vec::new();
    let mut schema: Option<arrow::datatypes::SchemaRef> = None;

    for (i, buf) in chunks.into_iter().enumerate() {
        let cursor = std::io::Cursor::new(buf);
        let reader = StreamReader::try_new(cursor, None).map_err(|e| {
            ConnectorError::Arrow(format!(
                "statement {statement_id}: chunk {i}: StreamReader::try_new: {e}"
            ))
        })?;
        if schema.is_none() {
            schema = Some(reader.schema());
        }
        for batch in reader {
            let batch = batch.map_err(|e| {
                ConnectorError::Arrow(format!(
                    "statement {statement_id}: chunk {i}: stream batch: {e}"
                ))
            })?;
            all_batches.push(batch);
        }
    }

    let schema = schema.expect("schema set on first chunk");
    if all_batches.is_empty() {
        // Arrow-stream-with-zero-batches happens for 0-row results;
        // synthesize an empty batch with the schema we read off the
        // stream header rather than erroring (`SELECT * FROM t WHERE
        // false` should round-trip with the right schema and 0 rows).
        return Ok(RecordBatch::new_empty(schema));
    }
    if all_batches.len() == 1 {
        return Ok(all_batches.into_iter().next().expect("len==1"));
    }
    arrow::compute::concat_batches(&schema, all_batches.iter())
        .map_err(|e| ConnectorError::Arrow(format!("statement {statement_id}: concat: {e}")))
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
        | ConnectorError::RetryBudgetExhausted { .. }
        | ConnectorError::NoArrowChunks { .. }
        | ConnectorError::Arrow(_) => false,
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
        ConnectorError::Canceled { .. }
        | ConnectorError::UnexpectedState { .. }
        | ConnectorError::NoArrowChunks { .. }
        | ConnectorError::Arrow(_) => ErrorClass::Permanent,
    }
}

/// Extract [`rocky_core::traits::ExecutionStats`] from a successful
/// [`StatementResponse`].
///
/// Maps `response.manifest.total_byte_count` (the aggregate size of the
/// statement result payload across all chunks, per Databricks's
/// Statement Execution REST contract) into
/// [`ExecutionStats::bytes_scanned`]. Databricks is priced in
/// DBU-hours, not bytes, so this figure isn't a cost driver the way
/// BigQuery's `totalBytesBilled` is — it's the byte count Databricks
/// natively reports for the statement, surfaced unchanged so callers
/// can observe it. The `bytes_scanned` slot name is retained (rather
/// than introducing a Databricks-specific field) to keep the
/// [`crate::adapter::DatabricksWarehouseAdapter`] cost path free of
/// adapter-specific branching — see the matching BigQuery comment in
/// `rocky_bigquery::connector::stats_from_response`.
///
/// Returns all-`None` when Databricks omits the manifest (e.g. on
/// DDL / DML that produces no result payload) or when the manifest is
/// present but `total_byte_count` isn't set. `bytes_written` and
/// `rows_affected` are always `None` — Databricks doesn't expose
/// bytes-written on the Statement Execution response, and
/// `total_row_count` on the manifest is the result-row count, not the
/// rows-affected figure for DML.
fn stats_from_response(response: &StatementResponse) -> rocky_core::traits::ExecutionStats {
    let bytes_scanned = response.manifest.as_ref().and_then(|m| m.total_byte_count);
    rocky_core::traits::ExecutionStats {
        bytes_scanned,
        bytes_written: None,
        rows_affected: None,
        // Databricks surfaces a statement identifier on the response;
        // wiring it is a follow-up wave (deferred along with the
        // `bytes_written` / `rows_affected` fields).
        job_id: None,
    }
}

/// Fixed polling delays (ms) for the first 5 statement status checks.
const POLL_DELAY_STEPS_MS: [u64; 5] = [100, 200, 500, 1000, 2000];
/// Maximum polling delay after exponential growth.
const MAX_POLL_DELAY_MS: u64 = 5000;

/// Best-effort SQL-keyword classifier for the
/// `rocky.statement.kind` span attribute (and its legacy
/// `statement.kind` alias). Mirrors the matcher in the Snowflake
/// connector so the span-attribute taxonomy is uniform across
/// adapters — a dashboard filtering on
/// `rocky.statement.kind = "ddl"` returns the same shape of work
/// regardless of which warehouse executed it.
///
/// Returns one of `"query"`, `"dml"`, `"ddl"`, `"other"`.
fn classify_statement_kind(sql: &str) -> &'static str {
    let sql = strip_leading_sql_comments_and_whitespace(sql);
    let keyword = sql
        .split(|ch: char| !ch.is_ascii_alphabetic())
        .next()
        .unwrap_or("");

    match keyword.to_ascii_uppercase().as_str() {
        "SELECT" | "WITH" | "SHOW" | "DESCRIBE" | "EXPLAIN" => "query",
        "INSERT" | "UPDATE" | "DELETE" | "MERGE" | "COPY" | "TRUNCATE" => "dml",
        "CREATE" | "ALTER" | "DROP" | "GRANT" | "REVOKE" => "ddl",
        _ => "other",
    }
}

fn strip_leading_sql_comments_and_whitespace(mut sql: &str) -> &str {
    loop {
        let trimmed = sql.trim_start();
        let Some(comment_body) = trimmed.strip_prefix("--") else {
            return trimmed;
        };

        sql = match comment_body.find('\n') {
            Some(line_end) => &comment_body[line_end + 1..],
            None => "",
        };
    }
}

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
    fn test_circuit_breaker_not_transient() {
        let err = ConnectorError::CircuitBreakerOpen {
            consecutive_failures: 5,
            cooldown_seconds: None,
        };
        assert!(!is_transient(&err));
    }

    #[test]
    fn manifest_deserializes_total_byte_count() {
        // Happy path: SUCCEEDED SELECT with a result payload — the
        // manifest carries `total_byte_count` alongside
        // `total_row_count`.
        let json = r#"{
            "statement_id": "id-1",
            "status": {"state": "SUCCEEDED"},
            "manifest": {
                "schema": {
                    "columns": [
                        {"name": "col1", "type_name": "STRING", "position": 0}
                    ]
                },
                "total_row_count": 10,
                "total_byte_count": 2843848
            }
        }"#;
        let resp: StatementResponse = serde_json::from_str(json).unwrap();
        let manifest = resp.manifest.unwrap();
        assert_eq!(manifest.total_byte_count, Some(2_843_848));
        assert_eq!(manifest.total_row_count, Some(10));
    }

    #[test]
    fn stats_from_response_populates_bytes_scanned() {
        // Happy path into `stats_from_response`: `manifest.total_byte_count`
        // flows into `ExecutionStats.bytes_scanned`. `bytes_written`
        // and `rows_affected` stay `None` (Databricks doesn't expose
        // them on the Statement Execution response).
        let json = r#"{
            "statement_id": "id-1",
            "status": {"state": "SUCCEEDED"},
            "manifest": {
                "total_row_count": 10,
                "total_byte_count": 2843848
            }
        }"#;
        let resp: StatementResponse = serde_json::from_str(json).unwrap();
        let stats = stats_from_response(&resp);
        assert_eq!(stats.bytes_scanned, Some(2_843_848));
        assert_eq!(stats.bytes_written, None);
        assert_eq!(stats.rows_affected, None);
    }

    #[test]
    fn stats_from_response_without_manifest_is_all_none() {
        // DDL / DML case: Databricks omits the manifest when there's
        // no result payload. `stats_from_response` must degrade to
        // all-`None` rather than erroring, mirroring BigQuery's
        // `statistics-absent` case.
        let json = r#"{
            "statement_id": "id-1",
            "status": {"state": "SUCCEEDED"}
        }"#;
        let resp: StatementResponse = serde_json::from_str(json).unwrap();
        assert!(resp.manifest.is_none());
        let stats = stats_from_response(&resp);
        assert_eq!(stats.bytes_scanned, None);
        assert_eq!(stats.bytes_written, None);
        assert_eq!(stats.rows_affected, None);
    }

    #[test]
    fn stats_from_response_manifest_without_total_byte_count_is_none() {
        // Defensive: manifest is present (we have `total_row_count`)
        // but Databricks didn't include `total_byte_count` — the
        // helper should return `None` for bytes rather than
        // inventing a value.
        let json = r#"{
            "statement_id": "id-1",
            "status": {"state": "SUCCEEDED"},
            "manifest": {
                "total_row_count": 0
            }
        }"#;
        let resp: StatementResponse = serde_json::from_str(json).unwrap();
        let manifest = resp.manifest.as_ref().unwrap();
        assert_eq!(manifest.total_byte_count, None);
        let stats = stats_from_response(&resp);
        assert_eq!(stats.bytes_scanned, None);
    }

    // ---- describe_detail_stats parsing helpers ----

    /// Build a `QueryResult` that looks like a `DESCRIBE DETAIL` response
    /// — one row, columns include `sizeInBytes`.
    fn make_describe_detail_result(size_bytes_value: serde_json::Value) -> QueryResult {
        QueryResult {
            statement_id: "test".to_string(),
            columns: vec![
                ColumnSchema {
                    name: "format".to_string(),
                    type_name: "STRING".to_string(),
                    position: 0,
                },
                ColumnSchema {
                    name: "sizeInBytes".to_string(),
                    type_name: "LONG".to_string(),
                    position: 1,
                },
                ColumnSchema {
                    name: "numFiles".to_string(),
                    type_name: "LONG".to_string(),
                    position: 2,
                },
            ],
            rows: vec![vec![
                serde_json::Value::String("delta".to_string()),
                size_bytes_value,
                serde_json::Value::Number(serde_json::Number::from(3u64)),
            ]],
            total_row_count: Some(1),
        }
    }

    #[test]
    fn describe_detail_parses_size_bytes_integer() {
        let result = make_describe_detail_result(serde_json::Value::Number(
            serde_json::Number::from(12_345_678u64),
        ));
        let size_bytes_idx = result
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case("sizeInBytes"));
        let size = match (size_bytes_idx, result.rows.first()) {
            (Some(idx), Some(row)) => row.get(idx).and_then(|v| {
                if v.is_null() {
                    None
                } else if let Some(n) = v.as_u64() {
                    Some(n)
                } else {
                    v.as_str().and_then(|s| s.parse::<u64>().ok())
                }
            }),
            _ => None,
        };
        assert_eq!(size, Some(12_345_678u64));
    }

    #[test]
    fn describe_detail_parses_size_bytes_string() {
        // Some Databricks response variants encode numbers as strings.
        let result = make_describe_detail_result(serde_json::Value::String("98765".to_string()));
        let size_bytes_idx = result
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case("sizeInBytes"));
        let size = match (size_bytes_idx, result.rows.first()) {
            (Some(idx), Some(row)) => row.get(idx).and_then(|v| {
                if v.is_null() {
                    None
                } else if let Some(n) = v.as_u64() {
                    Some(n)
                } else {
                    v.as_str().and_then(|s| s.parse::<u64>().ok())
                }
            }),
            _ => None,
        };
        assert_eq!(size, Some(98_765u64));
    }

    #[test]
    fn describe_detail_returns_none_for_null_size() {
        let result = make_describe_detail_result(serde_json::Value::Null);
        let size_bytes_idx = result
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case("sizeInBytes"));
        let size = match (size_bytes_idx, result.rows.first()) {
            (Some(idx), Some(row)) => row.get(idx).and_then(|v| {
                if v.is_null() {
                    None
                } else if let Some(n) = v.as_u64() {
                    Some(n)
                } else {
                    v.as_str().and_then(|s| s.parse::<u64>().ok())
                }
            }),
            _ => None,
        };
        assert_eq!(size, None);
    }

    #[test]
    fn describe_detail_no_size_bytes_column_returns_none() {
        let result = QueryResult {
            statement_id: "test".to_string(),
            columns: vec![ColumnSchema {
                name: "format".to_string(),
                type_name: "STRING".to_string(),
                position: 0,
            }],
            rows: vec![vec![serde_json::Value::String("delta".to_string())]],
            total_row_count: Some(1),
        };
        let size_bytes_idx = result
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case("sizeInBytes"));
        assert!(size_bytes_idx.is_none());
        let size = match (size_bytes_idx, result.rows.first()) {
            (Some(idx), Some(row)) => row.get(idx).and_then(|v| {
                if v.is_null() {
                    None
                } else if let Some(n) = v.as_u64() {
                    Some(n)
                } else {
                    v.as_str().and_then(|s| s.parse::<u64>().ok())
                }
            }),
            _ => None,
        };
        assert_eq!(size, None);
    }

    // ---- Arrow IPC stream decode + result-format wiring ----

    #[test]
    fn result_format_disposition_and_format_pairs() {
        // Encode the disposition × format mapping the Databricks Statement
        // Execution API enforces: `INLINE`+`JSON_ARRAY` and `EXTERNAL_LINKS`+
        // `ARROW_STREAM`. Any drift here flips a request to a server-side
        // `INVALID_PARAMETER_VALUE` we'd only see on the live test.
        assert_eq!(ResultFormat::InlineJson.disposition(), "INLINE");
        assert_eq!(ResultFormat::InlineJson.format(), "JSON_ARRAY");
        assert_eq!(ResultFormat::ExternalArrow.disposition(), "EXTERNAL_LINKS");
        assert_eq!(ResultFormat::ExternalArrow.format(), "ARROW_STREAM");
    }

    #[test]
    fn manifest_with_external_links_deserializes_chunks() {
        // Shape captured from the live sandbox — confirms we walk the
        // initial reply's `result.external_links` + `manifest.chunks`
        // without serde drift.
        let json = r#"{
            "statement_id": "id-arrow",
            "status": {"state": "SUCCEEDED"},
            "manifest": {
                "format": "ARROW_STREAM",
                "total_row_count": 1,
                "total_byte_count": 456,
                "total_chunk_count": 1
            },
            "result": {
                "external_links": [
                    {
                        "chunk_index": 0,
                        "row_offset": 0,
                        "row_count": 1,
                        "byte_count": 456,
                        "external_link": "https://example.test/chunk0"
                    }
                ]
            }
        }"#;
        let resp: StatementResponse = serde_json::from_str(json).unwrap();
        let manifest = resp.manifest.unwrap();
        assert_eq!(manifest.total_chunk_count, Some(1));
        let links = resp.result.unwrap().external_links.unwrap();
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].chunk_index, 0);
        assert_eq!(links[0].external_link, "https://example.test/chunk0");
    }

    /// Round-trip a known Arrow batch through the IPC stream writer and
    /// back through `decode_arrow_chunks`. Single-chunk fast path —
    /// the multi-chunk `concat_batches` path is exercised by the
    /// next test.
    #[test]
    fn decode_arrow_chunks_round_trip_single_chunk() {
        use arrow::array::{Int32Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::ipc::writer::StreamWriter;
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
            Field::new("n", DataType::Int32, false),
            Field::new("s", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["foo"])),
            ],
        )
        .unwrap();

        let mut buf: Vec<u8> = Vec::new();
        {
            let mut w = StreamWriter::try_new(&mut buf, &schema).unwrap();
            w.write(&batch).unwrap();
            w.finish().unwrap();
        }

        let decoded = decode_arrow_chunks("test", vec![bytes::Bytes::from(buf)]).unwrap();
        assert_eq!(decoded.num_rows(), 1);
        assert_eq!(decoded.schema().fields().len(), 2);
        assert_eq!(decoded.schema().field(0).name(), "n");
        assert_eq!(decoded.schema().field(1).name(), "s");
    }

    #[test]
    fn decode_arrow_chunks_concatenates_multiple_chunks() {
        // Two chunks, each a self-describing IPC stream (schema is
        // repeated at the head) — `decode_arrow_chunks` should
        // concatenate them into a single `RecordBatch`. Mirrors the
        // Databricks `EXTERNAL_LINKS` shape where every chunk URL
        // resolves to an independently-decodable stream.
        use arrow::array::Int32Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::ipc::writer::StreamWriter;
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int32, false)]));
        let make_chunk = |vals: Vec<i32>| -> bytes::Bytes {
            let batch =
                RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vals)) as _])
                    .unwrap();
            let mut buf: Vec<u8> = Vec::new();
            {
                let mut w = StreamWriter::try_new(&mut buf, &schema).unwrap();
                w.write(&batch).unwrap();
                w.finish().unwrap();
            }
            bytes::Bytes::from(buf)
        };

        let chunks = vec![make_chunk(vec![1, 2]), make_chunk(vec![3, 4, 5])];
        let decoded = decode_arrow_chunks("test", chunks).unwrap();
        assert_eq!(decoded.num_rows(), 5);
        let n = decoded
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(
            (0..n.len()).map(|i| n.value(i)).collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5]
        );
    }

    #[test]
    fn decode_arrow_chunks_empty_input_returns_no_arrow_chunks() {
        let err = decode_arrow_chunks("id-empty", vec![]).unwrap_err();
        assert!(
            matches!(err, ConnectorError::NoArrowChunks { ref id } if id == "id-empty"),
            "got: {err:?}",
        );
    }

    #[test]
    fn arrow_and_no_arrow_chunks_errors_are_not_transient() {
        // Both Arrow-path-specific errors are terminal — the retry
        // loop must not loop on them.
        let arrow_err = ConnectorError::Arrow("schema mismatch".into());
        assert!(!is_transient(&arrow_err));
        assert!(matches!(classify_error(&arrow_err), ErrorClass::Permanent));

        let no_chunks_err = ConnectorError::NoArrowChunks { id: "id-x".into() };
        assert!(!is_transient(&no_chunks_err));
        assert!(matches!(
            classify_error(&no_chunks_err),
            ErrorClass::Permanent
        ));
    }
}
