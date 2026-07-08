//! BigQuery REST API connector — jobs.query + jobs.getQueryResults.

use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::time::Instant;
use tracing::{Instrument, Span, debug, field, info_span, warn};

use rocky_core::config::RetryConfig;
use rocky_core::failure_class::{FailureClass, TransientKind};
use rocky_core::retry::compute_backoff;
use rocky_core::retry_budget::RetryBudget;
use rocky_core::traits::{
    AdapterError, AdapterResult, ChunkChecksum, ExecutionStats, PkRange, QueryResult, SqlDialect,
    WarehouseAdapter,
};
use rocky_ir::{ColumnInfo, TableRef};
use rocky_observe::span_attrs;
use rocky_sql::validation::{validate_gcp_project_id, validate_identifier};

use crate::auth::BigQueryAuth;
use crate::dialect::BigQueryDialect;
use crate::storage_read::{StorageReadError, StorageTableRef, fetch_arrow_record_batch};

#[derive(Debug, Error)]
pub enum BigQueryError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("BigQuery API error: {message} (status: {status})")]
    ApiError { status: String, message: String },

    /// A query job that BigQuery accepted (HTTP 200, `jobComplete=true`)
    /// but which then *failed at the job level* — the response carries a
    /// top-level `errors[]` array (an `ErrorProto`) and no result rows.
    ///
    /// Before this variant existed, `run_query` returned `Ok` on any
    /// `jobComplete=true` response, so an async job failure was silently
    /// swallowed and the caller saw an empty result set instead of an
    /// error. This mirrors how the Databricks adapter maps a
    /// `state == FAILED` statement to `ConnectorError::StatementFailed`
    /// and the Snowflake adapter maps a non-success `statusCode`. It is
    /// classified **non-transient** — a rejected query won't pass on
    /// retry.
    #[error("BigQuery job failed: {message} (reason: {reason})")]
    JobError { reason: String, message: String },

    /// A **load** job (`jobs.insert` → polled via `jobs.get`) that reached
    /// `status.state == "DONE"` carrying a `status.errorResult`.
    ///
    /// Unlike the query path — where an async failure surfaces as a
    /// *top-level* `errors[]` array on the `jobs.query` /
    /// `jobs.getQueryResults` response (see [`Self::JobError`]) — a load
    /// job's terminal failure lives under the `Job` resource's
    /// `status.errorResult` (an `ErrorProto`), with the full list under
    /// `status.errors[]`. `jobs.insert` has no synchronous result
    /// endpoint, so this is the only place a load failure can appear.
    /// Classified **non-transient**: a malformed file or a schema
    /// mismatch won't pass on retry. `message` aggregates the per-row /
    /// per-file reasons from `status.errors[]` when present so the
    /// operator sees *why* the load failed, not just "load job failed".
    #[error("BigQuery load job failed: {message} (reason: {reason})")]
    LoadJobError { reason: String, message: String },

    #[error("authentication error: {0}")]
    Auth(#[from] crate::auth::AuthError),

    #[error("query timed out after {timeout_secs}s")]
    Timeout { timeout_secs: u64 },

    /// The run-level retry budget was exhausted before this statement's
    /// transient error could be retried. Mirrors the sibling adapters'
    /// `RetryBudgetExhausted` so one rate-limited statement can't drain
    /// the whole run's quota and the failure stays attributable.
    #[error("run-level retry budget exhausted (limit {limit}); aborting remaining retries")]
    RetryBudgetExhausted { limit: u32 },

    #[error("Storage Read API error: {0}")]
    StorageRead(#[from] StorageReadError),
}

/// BigQuery warehouse adapter.
pub struct BigQueryAdapter {
    client: reqwest::Client,
    auth: BigQueryAuth,
    project_id: String,
    location: String,
    dialect: BigQueryDialect,
    timeout_secs: u64,
    /// Retry policy for transient REST failures (429 / 502 / 503 / 504 +
    /// connection/timeout). Mirrors the Snowflake and Databricks adapters,
    /// which both carry a [`RetryConfig`] and retry transient errors with
    /// exponential backoff. Defaults to [`RetryConfig::default`]
    /// (`max_retries = 3`) so the bare `new()` constructor gains retry
    /// without a signature change; the registry threads the pipeline's
    /// `[retry]` config in via [`BigQueryAdapter::with_retry`].
    retry: RetryConfig,
    /// Shared run-level retry budget. Unbounded by default — set via
    /// [`BigQueryAdapter::with_retry_budget`] from the top-level
    /// `[retry] max_retries_per_run` so one rate-limited statement can't
    /// drain the whole run's quota. Mirrors the sibling adapters' §P2.7
    /// budget.
    retry_budget: RetryBudget,
    /// Override for the REST API base URL (used by tests to point at a
    /// wiremock server instead of `bigquery.googleapis.com`). Always
    /// `None` in production builds — the field only exists under `test`
    /// or the `test-support` feature, so the production binary is byte
    /// identical.
    #[cfg(any(test, feature = "test-support"))]
    base_url_override: Option<String>,
}

impl std::fmt::Debug for BigQueryAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BigQueryAdapter")
            .field("project_id", &self.project_id)
            .field("location", &self.location)
            .finish()
    }
}

/// Default query timeout in seconds.
const DEFAULT_TIMEOUT_SECS: u64 = 300;
/// Maximum number of rows returned per query page.
const MAX_RESULTS_PER_PAGE: u32 = 10_000;

/// Fixed polling delays (ms) for the first 5 `jobs.getQueryResults` checks.
/// Matches the Databricks connector's ladder so a long BigQuery query has
/// the same observable cadence. After step 5, exponential growth capped at
/// [`MAX_POLL_DELAY_MS`].
const POLL_DELAY_STEPS_MS: [u64; 5] = [100, 200, 500, 1000, 2000];
/// Maximum polling delay after exponential growth.
const MAX_POLL_DELAY_MS: u64 = 5000;

/// Build the production reqwest client for talking to
/// `bigquery.googleapis.com`. Extracted so `new()` and (eventually) any
/// other constructor share one definition; the test seam
/// (`with_base_url`) builds its own h2-less client instead.
fn build_production_client() -> reqwest::Client {
    reqwest::Client::builder()
        .tcp_nodelay(true)
        // Keep connections alive on the wire so GCP load balancers
        // don't silently sever idle sockets mid-query. Matches the
        // cadence reqwest uses internally for HTTP/2 ping frames.
        .tcp_keepalive(Some(std::time::Duration::from_secs(30)))
        .pool_max_idle_per_host(32)
        .pool_idle_timeout(std::time::Duration::from_secs(300))
        // Separate HTTP request timeout from the BigQuery query
        // timeout (which is encoded in the request body as
        // `timeout_ms`). Post-P2.3 we poll `jobs.getQueryResults` for
        // anything longer; 120 s bounds individual HTTP roundtrips.
        // Matches the Databricks adapter.
        .timeout(std::time::Duration::from_secs(120))
        .connect_timeout(std::time::Duration::from_secs(10))
        // GCP endpoints (bigquery.googleapis.com, oauth2.googleapis.com)
        // support HTTP/2 natively; skipping the Upgrade dance shaves
        // one RTT off the first request. Safe only because this
        // adapter never hits HTTP/1-only endpoints (metadata server,
        // corporate proxies, etc. — see auth.rs which only talks to
        // oauth2.googleapis.com).
        .http2_prior_knowledge()
        .build()
        .unwrap_or_else(|_| reqwest::Client::new())
}

fn poll_delay(attempt: usize) -> Duration {
    let delay_ms = if attempt < POLL_DELAY_STEPS_MS.len() {
        POLL_DELAY_STEPS_MS[attempt]
    } else {
        let extra = attempt - POLL_DELAY_STEPS_MS.len();
        (2000u64 * (1u64 << extra.min(3))).min(MAX_POLL_DELAY_MS)
    };
    Duration::from_millis(delay_ms)
}

impl BigQueryAdapter {
    /// Create a new BigQuery adapter.
    pub fn new(
        project_id: impl Into<String>,
        location: impl Into<String>,
        auth: BigQueryAuth,
    ) -> Self {
        Self {
            client: build_production_client(),
            auth,
            project_id: project_id.into(),
            location: location.into(),
            dialect: BigQueryDialect,
            timeout_secs: DEFAULT_TIMEOUT_SECS,
            retry: RetryConfig::default(),
            retry_budget: RetryBudget::unbounded(),
            #[cfg(any(test, feature = "test-support"))]
            base_url_override: None,
        }
    }

    /// Set the retry policy for transient REST failures. The registry
    /// threads the pipeline's `[retry]` config in here so a BigQuery
    /// adapter honours the same `max_retries` / backoff knobs as the
    /// Snowflake and Databricks adapters.
    #[must_use]
    pub fn with_retry(mut self, retry: RetryConfig) -> Self {
        self.retry = retry;
        self
    }

    /// Override the run-level [`RetryBudget`]. The registry passes the
    /// shared cross-adapter budget built from the top-level `[retry]
    /// max_retries_per_run` so all adapters in a run draw from one quota.
    #[must_use]
    pub fn with_retry_budget(mut self, budget: RetryBudget) -> Self {
        self.retry_budget = budget;
        self
    }

    /// Point this adapter at a custom REST API base URL (for testing with
    /// wiremock). Rebuilds the HTTP client **without**
    /// `http2_prior_knowledge` because wiremock serves HTTP/1.1; the
    /// production client negotiates HTTP/2 directly and so cannot connect
    /// to an HTTP/1.1 mock server. The base URL should have no trailing
    /// slash (e.g. `http://127.0.0.1:1234`). Production code never calls
    /// this — the method and its h2-less client only exist under `test`
    /// or the `test-support` feature.
    #[cfg(any(test, feature = "test-support"))]
    #[must_use]
    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.client = reqwest::Client::builder()
            .tcp_nodelay(true)
            .timeout(std::time::Duration::from_secs(120))
            .connect_timeout(std::time::Duration::from_secs(10))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        self.base_url_override = Some(base_url.into());
        self
    }

    /// Base URL for the BigQuery REST API. In `test` / `test-support`
    /// builds this returns the wiremock override when set; otherwise (and
    /// always in production) it returns the real `bigquery.googleapis.com`
    /// host. Every REST URL the connector builds routes through here so a
    /// stray call can never leak to Google during a mocked test.
    fn base_url(&self) -> &str {
        #[cfg(any(test, feature = "test-support"))]
        if let Some(url) = &self.base_url_override {
            return url;
        }
        "https://bigquery.googleapis.com"
    }

    /// Set the query timeout.
    #[must_use]
    pub fn with_timeout(mut self, secs: u64) -> Self {
        self.timeout_secs = secs;
        self
    }

    /// GCP project ID this adapter is bound to. Exposed so adapters that
    /// share state with this one (e.g. `BigQueryDiscoveryAdapter`) can
    /// build region-qualified `INFORMATION_SCHEMA` queries without the
    /// caller having to thread the project ID through twice.
    pub fn project_id(&self) -> &str {
        &self.project_id
    }

    /// Dataset location ("EU", "US", "us-east1", …). Used by
    /// `BigQueryDiscoveryAdapter` to build region-scoped
    /// `INFORMATION_SCHEMA.SCHEMATA` queries — BigQuery's
    /// region-unqualified form returns rows only for datasets in the
    /// region the query is *executed* in, so cross-region projects need
    /// the explicit `region-<location>` qualifier.
    pub fn location(&self) -> &str {
        &self.location
    }

    /// Execute a query via the BigQuery REST API.
    ///
    /// Wrapped in a `statement.execute` span so every BigQuery API call —
    /// `execute_statement`, `execute_statement_with_stats`, `execute_query`,
    /// `describe_table`, `list_tables`, etc. — appears under a consistent,
    /// adapter-tagged child span when traced. Mirrors the OTel
    /// `<verb>.<resource>` shape used by the `materialize.table` parent.
    async fn run_query(&self, sql: &str) -> Result<BigQueryResponse, BigQueryError> {
        // `statement.kind = "query"` is a best-effort default — BigQuery
        // routes DDL, DML, and SELECT through the same `jobs.query`
        // endpoint and the wire request doesn't surface the parsed
        // statement kind. TODO: classify via `rocky-sql` if downstream
        // consumers need to filter ddl/dml from query traffic.
        //
        // Span attribute schema unification: emit the canonical
        // `rocky.*` keys alongside the bespoke `adapter` /
        // `statement.kind` names for one release so existing
        // dashboards see no breakage. Follow-up PRs drop the bespoke
        // aliases. `rocky.warehouse.query_id` starts empty and is
        // filled by `Span::record` once BigQuery returns the
        // `JobReference.job_id`. See `rocky_observe::span_attrs`.
        let span = info_span!(
            "statement.execute",
            adapter = "bigquery",
            statement.kind = "query",
            "rocky.adapter.name" = "bigquery",
            "rocky.statement.kind" = "query",
            "rocky.warehouse.name" = %self.project_id,
            "rocky.warehouse.query_id" = field::Empty,
            "rocky.warehouse.bytes_scanned" = field::Empty,
            "rocky.retry.attempt" = field::Empty,
        );
        self.run_query_with_retry(sql).instrument(span).await
    }

    /// Submit `sql` with bounded retry-with-backoff for transient REST
    /// failures (429 / 502 / 503 / 504 + connection/timeout), mirroring the
    /// Snowflake and Databricks adapters. Non-transient failures (auth
    /// 401/403, a 400 syntax rejection, a job-level `errors[]`, or a poll
    /// timeout) return on the first attempt — retrying them only burns
    /// quota. The run-level [`RetryBudget`] caps total retries so one
    /// rate-limited statement can't drain the whole run's quota.
    async fn run_query_with_retry(&self, sql: &str) -> Result<BigQueryResponse, BigQueryError> {
        for attempt in 0..=self.retry.max_retries {
            match self.run_query_once(sql).await {
                Ok(resp) => {
                    if attempt > 0 {
                        rocky_observe::metrics::METRICS.inc_retries_succeeded();
                    }
                    Span::current().record(span_attrs::RETRY_ATTEMPT, u64::from(attempt + 1));
                    return Ok(resp);
                }
                Err(err) => {
                    if attempt < self.retry.max_retries && is_transient(&err) {
                        // Run-level retry budget: if exhausted, abort
                        // remaining retries so one rate-limited statement
                        // can't drain the run's quota. Unbounded budgets
                        // always consume successfully.
                        if !self.retry_budget.try_consume() {
                            let limit = self.retry_budget.total().unwrap_or(0);
                            warn!(
                                attempt = attempt + 1,
                                max_retries = self.retry.max_retries,
                                budget_limit = limit,
                                error = %err,
                                "retry budget exhausted for this run; aborting further retries",
                            );
                            return Err(BigQueryError::RetryBudgetExhausted { limit });
                        }
                        rocky_observe::metrics::METRICS.inc_retries_attempted();
                        let backoff_ms = compute_backoff(&self.retry, attempt);
                        warn!(
                            attempt = attempt + 1,
                            max_retries = self.retry.max_retries,
                            backoff_ms = backoff_ms,
                            error = %err,
                            "transient BigQuery error, retrying"
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

    /// One BigQuery `jobs.query` submission + poll, with no retry. The
    /// retry/backoff loop lives in [`Self::run_query_with_retry`].
    async fn run_query_once(&self, sql: &str) -> Result<BigQueryResponse, BigQueryError> {
        let token = self.auth.get_token(&self.client).await?;
        let url = format!(
            "{}/bigquery/v2/projects/{}/queries",
            self.base_url(),
            self.project_id
        );

        let request = QueryRequest {
            query: sql.to_string(),
            use_legacy_sql: false,
            location: self.location.clone(),
            timeout_ms: self.timeout_secs * 1000,
            max_results: MAX_RESULTS_PER_PAGE,
        };

        debug!(sql = sql, project = %self.project_id, "executing BigQuery query");

        let resp = self
            .client
            .post(&url)
            .bearer_auth(&token)
            .json(&request)
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status().to_string();
            let body = resp.text().await.unwrap_or_default();
            return Err(BigQueryError::ApiError {
                status,
                message: body,
            });
        }

        let response: BigQueryResponse = resp.json().await?;

        if response.job_complete {
            // Surface an async job-level failure as an error instead of
            // swallowing it as an empty result set. BigQuery accepts the
            // job (HTTP 200, `jobComplete=true`) but reports the failure
            // via a top-level `errors[]` array with no rows.
            response.check_job_error()?;
            record_warehouse_attrs_on_current_span(&response);
            return Ok(response);
        }

        // Async job: BigQuery deferred the result. Poll jobs.getQueryResults
        // until it reports `job_complete=true` or `timeout_secs` elapses.
        // Without this, the previous behaviour silently returned an empty
        // rows array for any query slower than BigQuery's sync window.
        let job_ref = response
            .job_reference
            .ok_or_else(|| BigQueryError::ApiError {
                status: "missing jobReference".into(),
                message: "BigQuery returned job_complete=false with no job_reference; cannot poll"
                    .into(),
            })?;
        self.poll_query_results(&job_ref).await
    }

    /// Poll `jobs.getQueryResults` until the job finishes or the configured
    /// timeout elapses.
    ///
    /// Mirrors the polling shape used by the Databricks connector's
    /// statement-execution loop (same fixed ladder → exponential cap). The
    /// endpoint accepts an optional `timeoutMs` query param that lets
    /// BigQuery hold the connection open for up to 60 s per poll, which
    /// keeps attempt count low on fast jobs without burning API quota.
    async fn poll_query_results(
        &self,
        job_ref: &JobReference,
    ) -> Result<BigQueryResponse, BigQueryError> {
        let deadline = Instant::now() + Duration::from_secs(self.timeout_secs);
        let token = self.auth.get_token(&self.client).await?;
        let url = format!(
            "{}/bigquery/v2/projects/{}/queries/{}",
            self.base_url(),
            self.project_id,
            job_ref.job_id
        );

        for attempt in 0..usize::MAX {
            if Instant::now() >= deadline {
                return Err(BigQueryError::Timeout {
                    timeout_secs: self.timeout_secs,
                });
            }

            tokio::time::sleep(poll_delay(attempt)).await;

            // Ask BigQuery to wait up to 10 s on its side before returning —
            // cheap early-exit if the job finishes while we're blocked,
            // bounded so we can re-check the deadline quickly on slow jobs.
            let resp = self
                .client
                .get(&url)
                .bearer_auth(&token)
                .query(&[
                    ("location", self.location.as_str()),
                    ("maxResults", "10000"),
                    ("timeoutMs", "10000"),
                ])
                .send()
                .await?;

            if !resp.status().is_success() {
                let status = resp.status().to_string();
                let body = resp.text().await.unwrap_or_default();
                return Err(BigQueryError::ApiError {
                    status,
                    message: body,
                });
            }

            let response: BigQueryResponse = resp.json().await?;
            if response.job_complete {
                // Same async-failure guard as the sync path: a job that
                // finished with a top-level `errors[]` is surfaced as a
                // `JobError`, not swallowed as an empty result set.
                response.check_job_error()?;
                debug!(
                    job_id = %job_ref.job_id,
                    attempts = attempt + 1,
                    "BigQuery query completed"
                );
                record_warehouse_attrs_on_current_span(&response);
                return Ok(response);
            }
        }

        Err(BigQueryError::Timeout {
            timeout_secs: self.timeout_secs,
        })
    }

    /// Fetch the full `Job` resource for a completed job ID and return
    /// its `configuration.query.destinationTable` reference.
    ///
    /// Every non-DDL `jobs.query` job lands its result rows in an
    /// anonymous temporary table BigQuery creates under
    /// `<project>:_<hash>.<table>`. The reference comes back on
    /// `jobs.get` under `configuration.query.destinationTable`; we use
    /// it as the input to the Storage Read API's `CreateReadSession`
    /// call for the Arrow path.
    ///
    /// Anonymous result tables auto-expire after ~24h, so there's no
    /// cleanup tax on the caller.
    async fn fetch_destination_table(
        &self,
        job_ref: &JobReference,
    ) -> Result<JobDestinationTable, BigQueryError> {
        let token = self.auth.get_token(&self.client).await?;
        let url = format!(
            "{}/bigquery/v2/projects/{}/jobs/{}",
            self.base_url(),
            self.project_id,
            job_ref.job_id
        );

        let resp = self
            .client
            .get(&url)
            .bearer_auth(&token)
            .query(&[("location", self.location.as_str())])
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status().to_string();
            let body = resp.text().await.unwrap_or_default();
            return Err(BigQueryError::ApiError {
                status,
                message: body,
            });
        }

        #[derive(Deserialize)]
        struct JobGetResponse {
            configuration: Option<JobConfiguration>,
        }
        #[derive(Deserialize)]
        struct JobConfiguration {
            query: Option<JobQueryConfig>,
        }
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct JobQueryConfig {
            destination_table: Option<JobDestinationTable>,
        }

        let job: JobGetResponse = resp.json().await?;
        job.configuration
            .and_then(|c| c.query)
            .and_then(|q| q.destination_table)
            .ok_or_else(|| BigQueryError::ApiError {
                status: "missing destinationTable".into(),
                message: format!(
                    "jobs.get response for job '{}' had no configuration.query.destinationTable; \
                     the query may be DDL-only or the job may not be a query job",
                    job_ref.job_id
                ),
            })
    }

    /// Execute `sql` and return its result as a single workspace
    /// `arrow 58` `RecordBatch` via the BigQuery Storage Read API.
    ///
    /// Two-step:
    ///   1. `jobs.query` runs the SQL synchronously / async (existing
    ///      polling-loop path). The response carries the `jobReference`
    ///      we need to look up the anonymous destination table.
    ///   2. `jobs.get` resolves
    ///      `configuration.query.destinationTable` → `(project,
    ///      dataset, table)`. We pass that handle to
    ///      `storage_read::fetch_arrow_record_batch`, which opens a
    ///      one-stream Arrow read session and streams Arrow IPC bytes
    ///      back over gRPC.
    ///
    /// The Storage Read API uses the same OAuth scope as the REST API
    /// (`https://www.googleapis.com/auth/bigquery`), so the existing
    /// `BigQueryAuth` cache covers both transports.
    async fn fetch_arrow_via_storage_read(
        &self,
        sql: &str,
    ) -> Result<arrow::record_batch::RecordBatch, BigQueryError> {
        let response = self.run_query(sql).await?;
        let job_ref = response
            .job_reference
            .as_ref()
            .ok_or_else(|| BigQueryError::ApiError {
                status: "missing jobReference".into(),
                message:
                    "jobs.query response had no jobReference; cannot resolve destination table"
                        .into(),
            })?;
        let dest = self.fetch_destination_table(job_ref).await?;
        let table_ref = StorageTableRef {
            project: &dest.project_id,
            dataset: &dest.dataset_id,
            table: &dest.table_id,
        };
        let batch =
            fetch_arrow_record_batch(&self.auth, &self.client, &self.project_id, &table_ref, &[])
                .await?;
        Ok(batch)
    }

    /// Fetch the full `Job` resource for a completed job ID and return
    /// just its `statistics` block.
    ///
    /// The synchronous `jobs.query` and `jobs.getQueryResults` endpoints
    /// only surface `totalBytesProcessed` at the top level of the
    /// response; the `statistics.query.totalBytesBilled` figure (which
    /// applies the 10 MB per-query minimum-bill floor and is what the
    /// BigQuery console displays) is exclusive to `jobs.get`. Calling
    /// this method after `run_query` succeeds enriches the existing
    /// response so [`stats_from_response`] picks the billed figure
    /// rather than falling back to processed bytes.
    ///
    /// One extra HTTP roundtrip per query. `jobs.get` is free and
    /// returns in tens of milliseconds for a fresh job, so the trade-off
    /// is a small latency tax for accurate cost numbers.
    async fn fetch_job_statistics(
        &self,
        job_ref: &JobReference,
    ) -> Result<BigQueryStatistics, BigQueryError> {
        let token = self.auth.get_token(&self.client).await?;
        let url = format!(
            "{}/bigquery/v2/projects/{}/jobs/{}",
            self.base_url(),
            self.project_id,
            job_ref.job_id
        );

        let resp = self
            .client
            .get(&url)
            .bearer_auth(&token)
            .query(&[("location", self.location.as_str())])
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status().to_string();
            let body = resp.text().await.unwrap_or_default();
            return Err(BigQueryError::ApiError {
                status,
                message: body,
            });
        }

        // `jobs.get` returns the full Job resource; we only care about
        // `statistics`. A minimal local struct keeps the deserializer
        // tolerant of fields we don't read.
        #[derive(Deserialize)]
        struct JobGetResponse {
            statistics: Option<BigQueryStatistics>,
        }

        let job: JobGetResponse = resp.json().await?;
        job.statistics.ok_or_else(|| BigQueryError::ApiError {
            status: "missing statistics".into(),
            message: format!(
                "jobs.get response for job '{}' had no statistics block",
                job_ref.job_id
            ),
        })
    }

    /// Run a native BigQuery **load job** via `jobs.insert` and poll it to
    /// completion.
    ///
    /// This is the bulk-ingest primitive behind the BigQuery `load`
    /// pipeline. Unlike the query path (`jobs.query`, which can return
    /// inline), a load job is **always asynchronous**: `jobs.insert`
    /// accepts the configuration and returns a `Job` resource in
    /// `PENDING`/`RUNNING`; the caller polls `jobs.get` until
    /// `status.state == "DONE"`.
    ///
    /// # Source URIs
    ///
    /// `spec.source_uris` are `gs://bucket/object` paths (BigQuery load
    /// jobs read only from Google Cloud Storage — local files are not a
    /// load-job input, which is why the loader keeps an INSERT fallback
    /// for `LocalFile`). Wildcards (`gs://bucket/prefix*`) are honoured by
    /// BigQuery natively.
    ///
    /// # Errors
    ///
    /// - [`BigQueryError::ApiError`] — a non-2xx from `jobs.insert` /
    ///   `jobs.get` (auth, malformed config, quota).
    /// - [`BigQueryError::LoadJobError`] — the job reached `DONE` with a
    ///   `status.errorResult`. The message aggregates `status.errors[]`
    ///   so per-row / per-file reasons are visible.
    /// - [`BigQueryError::Timeout`] — the job didn't finish within the
    ///   adapter's configured timeout.
    pub async fn load_via_job(&self, spec: &LoadJobSpec) -> Result<LoadJobOutcome, BigQueryError> {
        let token = self.auth.get_token(&self.client).await?;
        let url = format!(
            "{}/bigquery/v2/projects/{}/jobs",
            self.base_url(),
            self.project_id
        );

        let request =
            JobInsertRequest::for_load(self.project_id.clone(), self.location.clone(), spec);

        debug!(
            uris = ?spec.source_uris,
            format = %spec.source_format.as_api_str(),
            write_disposition = %spec.write_disposition.as_api_str(),
            "submitting BigQuery load job"
        );

        let resp = self
            .client
            .post(&url)
            .bearer_auth(&token)
            .json(&request)
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status().to_string();
            let body = resp.text().await.unwrap_or_default();
            return Err(BigQueryError::ApiError {
                status,
                message: body,
            });
        }

        // `jobs.insert` returns the freshly-created `Job` resource. It may
        // already be `DONE` (a synchronous rejection, or a tiny load) or
        // still `PENDING`/`RUNNING`.
        let inserted: JobResource = resp.json().await?;
        let job_ref = inserted
            .job_reference
            .ok_or_else(|| BigQueryError::ApiError {
                status: "missing jobReference".into(),
                message: "jobs.insert response had no jobReference; cannot poll the load job"
                    .into(),
            })?;

        // Surface an inline failure immediately (the insert response can
        // carry `status.errorResult` for a job rejected synchronously).
        Self::check_load_status_error(inserted.status.as_ref())?;

        // For rows-loaded, always resolve via `jobs.get`: the
        // `statistics.load` block (and `outputRows` in particular) is only
        // reliably populated on the `Job` resource returned by `jobs.get`,
        // not necessarily on the `jobs.insert` response — even when that
        // response already reports `state == DONE`. Reading stats from the
        // insert body would risk reporting `rows_loaded = 0` on an inline
        // DONE that omitted the stats. `poll_load_job`'s first GET returns
        // immediately for an already-DONE job, so this is at most one extra
        // (cheap) roundtrip on the rare inline-DONE path.
        self.poll_load_job(&job_ref).await
    }

    /// Poll `jobs.get` for a load job until `status.state == "DONE"` or the
    /// configured timeout elapses. Reuses the same fixed-ladder →
    /// exponential-cap [`poll_delay`] cadence as the query poll loop.
    async fn poll_load_job(&self, job_ref: &JobReference) -> Result<LoadJobOutcome, BigQueryError> {
        let deadline = Instant::now() + Duration::from_secs(self.timeout_secs);
        let token = self.auth.get_token(&self.client).await?;
        let url = format!(
            "{}/bigquery/v2/projects/{}/jobs/{}",
            self.base_url(),
            self.project_id,
            job_ref.job_id
        );

        for attempt in 0..usize::MAX {
            if Instant::now() >= deadline {
                return Err(BigQueryError::Timeout {
                    timeout_secs: self.timeout_secs,
                });
            }

            tokio::time::sleep(poll_delay(attempt)).await;

            let resp = self
                .client
                .get(&url)
                .bearer_auth(&token)
                .query(&[("location", self.location.as_str())])
                .send()
                .await?;

            if !resp.status().is_success() {
                let status = resp.status().to_string();
                let body = resp.text().await.unwrap_or_default();
                return Err(BigQueryError::ApiError {
                    status,
                    message: body,
                });
            }

            let job: JobResource = resp.json().await?;
            if let Some(outcome) = Self::load_outcome_if_done(&job.status, &job.statistics)? {
                debug!(
                    job_id = %job_ref.job_id,
                    attempts = attempt + 1,
                    rows = outcome.rows_loaded,
                    "BigQuery load job completed"
                );
                return Ok(outcome);
            }
        }

        Err(BigQueryError::Timeout {
            timeout_secs: self.timeout_secs,
        })
    }

    /// Map a load job's `status.errorResult` (set once the job is `DONE`
    /// and failed) to a [`BigQueryError::LoadJobError`]. No-op when the
    /// status is absent, still running, or DONE without an error.
    ///
    /// A load job's terminal failure lives under `status.errorResult`
    /// (an `ErrorProto`) — **not** the top-level `errors[]` the query path
    /// uses — which is why this can't reuse
    /// [`BigQueryResponse::check_job_error`]. Called both on the
    /// `jobs.insert` response (synchronous rejections) and during the
    /// `jobs.get` poll loop. No-op when `status` is absent or carries no
    /// `errorResult`.
    fn check_load_status_error(status: Option<&JobStatus>) -> Result<(), BigQueryError> {
        let Some(status) = status else {
            return Ok(());
        };
        let Some(err) = &status.error_result else {
            return Ok(());
        };
        let reason = err.reason.clone().unwrap_or_else(|| "unknown".into());
        // Aggregate per-row / per-file reasons from `errors[]` when present
        // so the operator sees the underlying cause, not just the summary
        // `errorResult`. Fall back to `errorResult.message`.
        let detail: Vec<String> = status
            .errors
            .iter()
            .flatten()
            .filter_map(|e| e.message.clone())
            .collect();
        let message = if detail.is_empty() {
            err.message.clone().unwrap_or_default()
        } else {
            detail.join("; ")
        };
        Err(BigQueryError::LoadJobError { reason, message })
    }

    /// If a load job's `status.state == "DONE"`, map it to either a
    /// [`LoadJobOutcome`] (success — rows-loaded from
    /// `statistics.load.outputRows`) or a
    /// [`BigQueryError::LoadJobError`]. Returns `Ok(None)` while the job is
    /// still `PENDING`/`RUNNING` so the poll loop keeps going.
    fn load_outcome_if_done(
        status: &Option<JobStatus>,
        statistics: &Option<BigQueryStatistics>,
    ) -> Result<Option<LoadJobOutcome>, BigQueryError> {
        let Some(status) = status else {
            // No status block yet → job hasn't reached a reportable state.
            return Ok(None);
        };
        if status.state.as_deref() != Some("DONE") {
            return Ok(None);
        }

        Self::check_load_status_error(Some(status))?;

        let load_stats = statistics.as_ref().and_then(|s| s.load.as_ref());
        let rows_loaded = load_stats
            .and_then(BigQueryLoadStatistics::output_rows_u64)
            .unwrap_or(0);
        let input_bytes = load_stats.and_then(BigQueryLoadStatistics::input_file_bytes_u64);
        Ok(Some(LoadJobOutcome {
            rows_loaded,
            input_file_bytes: input_bytes,
        }))
    }
}

#[async_trait]
impl WarehouseAdapter for BigQueryAdapter {
    fn dialect(&self) -> &dyn SqlDialect {
        &self.dialect
    }

    async fn execute_statement(&self, sql: &str) -> AdapterResult<()> {
        self.run_query(sql)
            .await
            .map(|_| ())
            .map_err(AdapterError::new)
    }

    fn classify_failure(&self, err: &AdapterError) -> FailureClass {
        classify_bigquery_failure(err)
    }

    async fn execute_statement_with_stats(&self, sql: &str) -> AdapterResult<ExecutionStats> {
        let mut response = self.run_query(sql).await.map_err(AdapterError::new)?;

        // Enrich with `jobs.get` statistics so cost reporting reflects
        // `totalBytesBilled` (10 MB minimum-bill floor applied) instead
        // of the bare `totalBytesProcessed` `jobs.query` returns.
        // `run_query`'s response shape never carries `statistics` on the
        // sync path, so the `is_none()` check is currently always true
        // when a job ran — but keeping it defensive against future
        // codepaths that might pre-populate the field.
        if response.statistics.is_none()
            && let Some(ref job_ref) = response.job_reference
        {
            match self.fetch_job_statistics(job_ref).await {
                Ok(stats) => response.statistics = Some(stats),
                Err(e) => {
                    // Best-effort: fall back to top-level
                    // `total_bytes_processed`. Logged so a future
                    // "cost numbers look low" debug session has the
                    // failure reason recorded.
                    debug!(
                        job_id = %job_ref.job_id,
                        error = %e,
                        "jobs.get failed; falling back to totalBytesProcessed",
                    );
                }
            }
        }

        Ok(stats_from_response(&response))
    }

    async fn execute_query(&self, sql: &str) -> AdapterResult<QueryResult> {
        let response = self.run_query(sql).await.map_err(AdapterError::new)?;

        let columns: Vec<String> = response
            .schema
            .as_ref()
            .map(|s| s.fields.iter().map(|f| f.name.clone()).collect())
            .unwrap_or_default();

        let rows: Vec<Vec<serde_json::Value>> = response
            .rows
            .unwrap_or_default()
            .iter()
            .map(|row| {
                row.f
                    .iter()
                    .map(|cell| cell.v.clone().unwrap_or(serde_json::Value::Null))
                    .collect()
            })
            .collect();

        Ok(QueryResult { columns, rows })
    }

    /// Arrow path via the BigQuery Storage Read API (gRPC). Overrides
    /// the default `Err(...)` impl on `WarehouseAdapter`. See
    /// [`BigQueryAdapter::fetch_arrow_via_storage_read`] for the
    /// two-hop flow (`jobs.query` → `jobs.get` for destination table →
    /// `CreateReadSession` + `ReadRows` over gRPC).
    async fn fetch_arrow_batch(
        &self,
        sql: &str,
    ) -> AdapterResult<arrow::record_batch::RecordBatch> {
        self.fetch_arrow_via_storage_read(sql)
            .await
            .map_err(AdapterError::new)
    }

    async fn describe_table(&self, table: &TableRef) -> AdapterResult<Vec<ColumnInfo>> {
        // validate_identifier rejects `'` and other non-alphanumerics, so the
        // `table_name = '...'` literal cannot be broken out of by `table.table`.
        // The catalog (= GCP project) needs the looser project-ID validator
        // because GCP allows hyphens in project IDs (e.g. `my-project-id`).
        validate_gcp_project_id(&table.catalog).map_err(AdapterError::new)?;
        validate_identifier(&table.schema).map_err(AdapterError::new)?;
        validate_identifier(&table.table).map_err(AdapterError::new)?;
        let sql = format!(
            "SELECT column_name, data_type, is_nullable \
             FROM `{}`.`{}`.INFORMATION_SCHEMA.COLUMNS \
             WHERE table_name = '{}'",
            table.catalog, table.schema, table.table
        );

        let result = self.execute_query(&sql).await?;
        let columns: Vec<ColumnInfo> = result
            .rows
            .iter()
            .filter_map(|row| {
                if row.len() >= 3 {
                    Some(ColumnInfo {
                        name: row[0].as_str().unwrap_or("").to_string(),
                        data_type: row[1].as_str().unwrap_or("").to_string(),
                        nullable: row[2].as_str().unwrap_or("NO") == "YES",
                    })
                } else {
                    None
                }
            })
            .collect();

        // INFORMATION_SCHEMA.COLUMNS returns zero rows for a missing table
        // rather than raising — so an empty result here means the table
        // doesn't exist. Surface that as an error so callers using
        // `describe_table().is_ok()` to probe existence (e.g. the
        // time-interval bootstrap path) get the right answer.
        if columns.is_empty() {
            return Err(AdapterError::msg(format!(
                "table `{}`.`{}`.`{}` not found",
                table.catalog, table.schema, table.table
            )));
        }

        Ok(columns)
    }

    async fn list_tables(&self, catalog: &str, schema: &str) -> AdapterResult<Vec<String>> {
        // BigQuery: catalog = project (allows hyphens), schema = dataset.
        validate_gcp_project_id(catalog).map_err(AdapterError::new)?;
        validate_identifier(schema).map_err(AdapterError::new)?;
        let sql =
            format!("SELECT table_name FROM `{catalog}`.`{schema}`.INFORMATION_SCHEMA.TABLES");
        let result = self.execute_query(&sql).await?;
        let tables = result
            .rows
            .iter()
            .filter_map(|row| row.first().and_then(|v| v.as_str()).map(str::to_lowercase))
            .collect();
        Ok(tables)
    }

    /// Override the trait default: emit BigQuery's native
    /// `CREATE OR REPLACE TABLE ... COPY` instead of CTAS. The branch
    /// table lands in `<source.catalog>.<branch_schema>.<source.table>` —
    /// same project as the source, since BigQuery's `COPY` primitive is
    /// scoped to a single project (cross-region multi-region datasets
    /// are fine, but cross-project is not).
    ///
    /// `COPY` is a metadata-only operation: the new table references the
    /// same physical storage as the source until either side mutates,
    /// so the per-PR branch is effectively zero-cost at create time.
    /// Strictly dominates the default CTAS implementation, which would
    /// re-scan the source bytes.
    /// BigQuery override of the checksum-bisection chunk-checksum query.
    ///
    /// The kernel default in `rocky-core` uses double-quoted identifiers
    /// (`"id"`), which BigQuery treats as **string literals** rather than
    /// column references — running the default impl on BQ would silently
    /// hash the literal text `"id"` for every row instead of the column.
    /// This override quotes columns with backticks, casts via BigQuery's
    /// `FLOAT64` / `INT64` (the canonical names — `DOUBLE` / `BIGINT`
    /// aliases work in casts but emitting the canonical names matches
    /// what BQ logs and keeps the audit trail clean), and composes the
    /// per-row hash with `BIT_XOR(<dialect.row_hash_expr>)`.
    ///
    /// The `LEAST(K-1, FLOOR(...))` clamp matches the kernel default's
    /// behavior — `split_int_range` lets the last chunk absorb the
    /// integer-step truncation remainder, so the SQL bucketing has to
    /// pin the highest pk values back into chunk K-1 instead of
    /// overflowing to K.
    async fn checksum_chunks(
        &self,
        table: &TableRef,
        pk_column: &str,
        value_columns: &[String],
        pk_ranges: &[PkRange],
    ) -> AdapterResult<Vec<ChunkChecksum>> {
        if pk_ranges.is_empty() {
            return Ok(Vec::new());
        }
        let (lo_min, hi_max, k) = bigquery_uniform_int_window(pk_ranges)?;
        let step = (hi_max - lo_min) / (k as i128);
        if step <= 0 {
            return Err(AdapterError::msg(
                "BigQuery checksum_chunks requires a positive integer step \
                 across the chunk window (lo == hi or hi < lo)",
            ));
        }

        validate_identifier(pk_column).map_err(AdapterError::new)?;
        for col in value_columns {
            validate_identifier(col).map_err(AdapterError::new)?;
        }

        let table_ref =
            self.dialect
                .format_table_ref(&table.catalog, &table.schema, &table.table)?;
        let row_hash = self.dialect.row_hash_expr(value_columns)?;
        let last_id = k - 1;

        let sql = format!(
            "SELECT chunk_id, COUNT(*) AS row_count, BIT_XOR({row_hash}) AS chk \
             FROM ( \
                 SELECT *, \
                        LEAST( \
                            CAST(FLOOR((CAST(`{pk_column}` AS FLOAT64) - {lo}) / {step}) AS INT64), \
                            {last_id} \
                        ) AS chunk_id \
                 FROM {table_ref} \
                 WHERE `{pk_column}` IS NOT NULL \
                   AND `{pk_column}` >= {lo} \
                   AND `{pk_column}` < {hi} \
             ) AS chunked \
             GROUP BY chunk_id \
             ORDER BY chunk_id",
            lo = lo_min,
            hi = hi_max,
        );

        let result = self.execute_query(&sql).await?;
        parse_bigquery_chunk_checksums(&result, k)
    }

    async fn clone_table_for_branch(
        &self,
        source: &TableRef,
        branch_schema: &str,
    ) -> AdapterResult<()> {
        // GCP project IDs allow hyphens (`my-project-1`), so the catalog
        // component takes the looser project-ID validator. Datasets and
        // tables stay on the strict `[A-Za-z0-9_]+` rule.
        validate_gcp_project_id(&source.catalog).map_err(AdapterError::new)?;
        validate_identifier(&source.schema).map_err(AdapterError::new)?;
        validate_identifier(&source.table).map_err(AdapterError::new)?;
        validate_identifier(branch_schema).map_err(AdapterError::new)?;

        let project = &source.catalog;
        let src_dataset = &source.schema;
        let table = &source.table;
        let sql = format!(
            "CREATE OR REPLACE TABLE `{project}`.`{branch_schema}`.`{table}` \
             COPY `{project}`.`{src_dataset}`.`{table}`"
        );
        self.execute_statement(&sql).await
    }
}

/// Verify the bisection runner passed K contiguous IntRange chunks and
/// return `(lo_min, hi_max, K)` for SQL interpolation.
///
/// The contract is the same as the kernel default's
/// `uniform_int_range_window`: composite + hash-bucket strategies are
/// out of scope for the override and surface as an explicit error.
fn bigquery_uniform_int_window(pk_ranges: &[PkRange]) -> AdapterResult<(i128, i128, u32)> {
    let mut lo_min: Option<i128> = None;
    let mut hi_max: Option<i128> = None;
    for range in pk_ranges {
        match range {
            PkRange::IntRange { lo, hi } => {
                lo_min = Some(lo_min.map_or(*lo, |x| x.min(*lo)));
                hi_max = Some(hi_max.map_or(*hi, |x| x.max(*hi)));
            }
            PkRange::Composite { .. } | PkRange::HashBucket { .. } => {
                return Err(AdapterError::msg(
                    "BigQuery checksum_chunks supports IntRange only; \
                     composite and hash-bucket strategies require follow-up changes",
                ));
            }
        }
    }
    let lo = lo_min.expect("non-empty pk_ranges checked by caller");
    let hi = hi_max.expect("non-empty pk_ranges checked by caller");
    let k = u32::try_from(pk_ranges.len())
        .map_err(|_| AdapterError::msg("checksum_chunks accepts up to u32::MAX chunks per call"))?;
    Ok((lo, hi, k))
}

/// Parse a BigQuery `(chunk_id, row_count, chk)` result set into
/// [`ChunkChecksum`] rows. BigQuery returns every column as a string
/// over the REST API, so the parser stays string-first.
fn parse_bigquery_chunk_checksums(
    result: &QueryResult,
    k: u32,
) -> AdapterResult<Vec<ChunkChecksum>> {
    let mut out = Vec::with_capacity(result.rows.len());
    for row in &result.rows {
        if row.len() < 3 {
            return Err(AdapterError::msg(
                "checksum_chunks query returned a row with fewer than 3 columns",
            ));
        }
        let chunk_id_raw = parse_bq_i128(&row[0])?;
        if chunk_id_raw < 0 || chunk_id_raw >= i128::from(k) {
            return Err(AdapterError::msg(format!(
                "checksum_chunks returned chunk_id={chunk_id_raw}, outside [0, {k})"
            )));
        }
        let row_count_raw = parse_bq_i128(&row[1])?;
        let row_count = u64::try_from(row_count_raw).map_err(|_| {
            AdapterError::msg(format!(
                "checksum_chunks returned row_count={row_count_raw}, expected non-negative u64"
            ))
        })?;
        let checksum = parse_bq_i128(&row[2])? as u128;
        out.push(ChunkChecksum {
            chunk_id: chunk_id_raw as u32,
            row_count,
            checksum,
        });
    }
    Ok(out)
}

fn parse_bq_i128(v: &serde_json::Value) -> AdapterResult<i128> {
    if let Some(s) = v.as_str() {
        return s.parse::<i128>().map_err(|e| {
            AdapterError::msg(format!(
                "failed to parse {s:?} as i128 from BigQuery result: {e}"
            ))
        });
    }
    if let Some(n) = v.as_i64() {
        return Ok(n.into());
    }
    if let Some(n) = v.as_u64() {
        return Ok(n.into());
    }
    Err(AdapterError::msg(format!(
        "BigQuery checksum_chunks result column had unexpected JSON shape: {v:?}"
    )))
}

/// Extract [`ExecutionStats`] from a successful [`BigQueryResponse`].
///
/// # The "billed-in-scanned" slot
///
/// The field is called `bytes_scanned` but BigQuery emits **two** byte
/// counts per query job: `totalBytesProcessed` (what the engine read)
/// and `totalBytesBilled` (what the customer pays for — strictly `>=`
/// processed due to the 10 MB per-query minimum floor). This function
/// stores **billed** bytes in [`ExecutionStats::bytes_scanned`]
/// because [`rocky_core::cost::compute_observed_cost_usd`] multiplies
/// that field by the per-TB rate to produce the dollar figure
/// displayed in `rocky cost`. Using `processed` here would
/// under-report the cost for sub-10 MB queries. The semantic impurity
/// (field name says "scanned" but holds "billed") is accepted so that
/// every downstream consumer can keep treating `bytes_scanned` as the
/// single cost driver without BigQuery-specific branching.
///
/// Resolution order:
///
/// 1. `statistics.query.totalBytesBilled` — the cost-accurate figure
///    (with 10 MB minimum applied), only present on `jobs.get`
///    responses.
/// 2. Top-level `totalBytesProcessed` — surfaced by `jobs.query` /
///    `jobs.getQueryResults`, which is the path the runtime actually
///    takes today. Off by the 10 MB floor on sub-10 MB queries; the
///    cost calc therefore under-reports those slightly until a
///    follow-up `jobs.get` is wired in.
/// 3. `None` — no bytes information available (DDL responses BigQuery
///    omits both fields on, or fields unparseable as `u64`).
///
/// `bytes_written` is always `None` — BigQuery query jobs don't expose
/// a bytes-written figure naturally.
fn stats_from_response(response: &BigQueryResponse) -> ExecutionStats {
    let bytes_scanned = response
        .statistics
        .as_ref()
        .and_then(|s| s.query.as_ref())
        .and_then(BigQueryQueryStatistics::total_bytes_billed_u64)
        .or_else(|| {
            response
                .total_bytes_processed
                .as_deref()
                .and_then(|s| s.parse::<u64>().ok())
        });
    let job_id = response.job_reference.as_ref().map(|jr| jr.job_id.clone());
    ExecutionStats {
        bytes_scanned,
        bytes_written: None,
        rows_affected: None,
        job_id,
    }
}

/// Record the canonical warehouse-side attributes on the current
/// `statement.execute` span once a `jobs.query` /
/// `jobs.getQueryResults` response has resolved.
///
/// Mirrors the resolution order of [`stats_from_response`] so the
/// span attribute and the [`ExecutionStats`] returned to the runner
/// agree on which figure represents "bytes scanned" — under the OTel
/// `rocky.warehouse.bytes_scanned` semantic this is the billed
/// (cost-relevant) figure when available, otherwise the processed
/// fallback.
fn record_warehouse_attrs_on_current_span(response: &BigQueryResponse) {
    let span = Span::current();
    if let Some(job_ref) = &response.job_reference {
        span.record(span_attrs::WAREHOUSE_QUERY_ID, job_ref.job_id.as_str());
    }
    if let Some(bytes) = response
        .statistics
        .as_ref()
        .and_then(|s| s.query.as_ref())
        .and_then(BigQueryQueryStatistics::total_bytes_billed_u64)
        .or_else(|| {
            response
                .total_bytes_processed
                .as_deref()
                .and_then(|s| s.parse::<u64>().ok())
        })
    {
        span.record(span_attrs::WAREHOUSE_BYTES_SCANNED, bytes);
    }
}

// -- BigQuery load-job public API --

/// Source file format for a BigQuery load job. Maps to the
/// `configuration.load.sourceFormat` enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BigQuerySourceFormat {
    /// `CSV` — header rows / delimiter controlled via [`LoadJobSpec`].
    Csv,
    /// `PARQUET` — self-describing; schema autodetect is implicit.
    Parquet,
    /// `NEWLINE_DELIMITED_JSON` (JSONL / NDJSON).
    NewlineDelimitedJson,
}

impl BigQuerySourceFormat {
    /// The exact `sourceFormat` string BigQuery's load config expects.
    pub fn as_api_str(self) -> &'static str {
        match self {
            Self::Csv => "CSV",
            Self::Parquet => "PARQUET",
            Self::NewlineDelimitedJson => "NEWLINE_DELIMITED_JSON",
        }
    }
}

/// `configuration.load.writeDisposition` — what to do with existing rows
/// in the destination table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteDisposition {
    /// `WRITE_TRUNCATE` — overwrite the table (truncate then load).
    Truncate,
    /// `WRITE_APPEND` — append to existing rows.
    Append,
}

impl WriteDisposition {
    /// The exact `writeDisposition` string BigQuery expects.
    pub fn as_api_str(self) -> &'static str {
        match self {
            Self::Truncate => "WRITE_TRUNCATE",
            Self::Append => "WRITE_APPEND",
        }
    }
}

/// `configuration.load.createDisposition` — whether to create the table
/// when it doesn't exist.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoadCreateDisposition {
    /// `CREATE_IF_NEEDED` — create the table if it doesn't exist
    /// (autodetect or explicit schema).
    IfNeeded,
    /// `CREATE_NEVER` — fail if the table doesn't already exist.
    Never,
}

impl LoadCreateDisposition {
    /// The exact `createDisposition` string BigQuery expects.
    pub fn as_api_str(self) -> &'static str {
        match self {
            Self::IfNeeded => "CREATE_IF_NEEDED",
            Self::Never => "CREATE_NEVER",
        }
    }
}

/// A fully-resolved BigQuery load-job configuration, consumed by
/// [`BigQueryAdapter::load_via_job`].
///
/// The caller (the [`crate::BigQueryLoaderAdapter`]) translates the
/// generic [`rocky_adapter_sdk::LoadOptions`] into this BigQuery-specific
/// shape so the connector stays free of SDK types.
#[derive(Debug, Clone)]
pub struct LoadJobSpec {
    /// GCP project that owns the destination table. Empty means "use the
    /// adapter's project".
    pub destination_project: String,
    /// Destination dataset (BigQuery "schema").
    pub destination_dataset: String,
    /// Destination table name.
    pub destination_table: String,
    /// `gs://bucket/object` source URIs (wildcards allowed).
    pub source_uris: Vec<String>,
    /// File format of the source data.
    pub source_format: BigQuerySourceFormat,
    /// Overwrite vs append.
    pub write_disposition: WriteDisposition,
    /// Create-if-needed vs never.
    pub create_disposition: LoadCreateDisposition,
    /// Let BigQuery infer the schema from the data. Set for CSV/JSONL when
    /// no explicit schema is supplied; implicit/harmless for Parquet.
    pub autodetect: bool,
    /// CSV only: number of leading header rows to skip. `None` for
    /// non-CSV formats (the field is omitted from the request).
    pub skip_leading_rows: Option<u64>,
    /// CSV only: field delimiter. `None` for non-CSV formats.
    pub field_delimiter: Option<String>,
}

/// Outcome of a completed BigQuery load job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadJobOutcome {
    /// Rows written into the destination table
    /// (`statistics.load.outputRows`).
    pub rows_loaded: u64,
    /// Bytes read from the source URIs (`statistics.load.inputFileBytes`),
    /// when BigQuery reported it.
    pub input_file_bytes: Option<u64>,
}

// -- BigQuery REST API types --

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct QueryRequest {
    query: String,
    use_legacy_sql: bool,
    location: String,
    timeout_ms: u64,
    max_results: u32,
}

// -- load-job (`jobs.insert`) wire request --

/// `jobs.insert` request body: a `Job` resource carrying only a
/// `configuration.load` block and a `jobReference.location`.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct JobInsertRequest {
    job_reference: JobReferenceRequest,
    configuration: JobConfigurationRequest,
}

impl JobInsertRequest {
    /// Build the request from a [`LoadJobSpec`], pinning the job to the
    /// adapter's `project`/`location` and translating the typed
    /// dispositions to BigQuery's enum strings. An empty
    /// `destination_project` falls back to the adapter's project so the
    /// destinationTable never carries an empty `projectId`.
    fn for_load(project: String, location: String, spec: &LoadJobSpec) -> Self {
        let destination_project = if spec.destination_project.is_empty() {
            project.clone()
        } else {
            spec.destination_project.clone()
        };
        JobInsertRequest {
            job_reference: JobReferenceRequest {
                project_id: project,
                location,
            },
            configuration: JobConfigurationRequest {
                load: JobConfigurationLoad {
                    source_uris: spec.source_uris.clone(),
                    source_format: spec.source_format.as_api_str().to_string(),
                    write_disposition: spec.write_disposition.as_api_str().to_string(),
                    create_disposition: spec.create_disposition.as_api_str().to_string(),
                    autodetect: spec.autodetect,
                    destination_table: DestinationTableRef {
                        project_id: destination_project,
                        dataset_id: spec.destination_dataset.clone(),
                        table_id: spec.destination_table.clone(),
                    },
                    skip_leading_rows: spec.skip_leading_rows,
                    field_delimiter: spec.field_delimiter.clone(),
                },
            },
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct JobReferenceRequest {
    project_id: String,
    location: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct JobConfigurationRequest {
    load: JobConfigurationLoad,
}

/// `configuration.load` — the load-job knobs. Optional CSV fields are
/// omitted entirely (not sent as `null`) for non-CSV formats so the
/// request mirrors what a hand-written BigQuery load config looks like.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct JobConfigurationLoad {
    source_uris: Vec<String>,
    source_format: String,
    write_disposition: String,
    create_disposition: String,
    autodetect: bool,
    destination_table: DestinationTableRef,
    #[serde(skip_serializing_if = "Option::is_none")]
    skip_leading_rows: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    field_delimiter: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct DestinationTableRef {
    project_id: String,
    dataset_id: String,
    table_id: String,
}

// -- load-job (`jobs.insert` / `jobs.get`) wire response --

/// A BigQuery `Job` resource as returned by `jobs.insert` and `jobs.get`.
/// Only the slices the load path reads are modelled; everything else is
/// ignored by serde.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JobResource {
    #[serde(default)]
    job_reference: Option<JobReference>,
    #[serde(default)]
    status: Option<JobStatus>,
    #[serde(default)]
    statistics: Option<BigQueryStatistics>,
}

/// `Job.status`. `state` is `PENDING` / `RUNNING` / `DONE`; a terminal
/// failure carries `errorResult` (the summary `ErrorProto`) plus the full
/// `errors[]` list.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JobStatus {
    #[serde(default)]
    state: Option<String>,
    #[serde(default)]
    error_result: Option<JobErrorProto>,
    #[serde(default)]
    errors: Option<Vec<JobErrorProto>>,
}

/// An `ErrorProto` under `Job.status`. Distinct from the query path's
/// top-level [`ErrorProto`] only in where it lives on the wire; the shape
/// (`reason` + `message`) is the same subset.
#[derive(Debug, Deserialize)]
struct JobErrorProto {
    #[serde(default)]
    reason: Option<String>,
    #[serde(default)]
    message: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BigQueryResponse {
    #[serde(default)]
    job_complete: bool,
    job_reference: Option<JobReference>,
    schema: Option<TableSchema>,
    rows: Option<Vec<TableRow>>,
    #[allow(dead_code)]
    #[serde(default)]
    total_rows: Option<String>,
    /// Top-level `totalBytesProcessed` from the `jobs.query` /
    /// `jobs.getQueryResults` response shape (decimal-encoded int64).
    /// Set on every successful query job. **Note:** the synchronous
    /// `jobs.query` endpoint does *not* surface a `statistics` block
    /// (that's exclusive to `jobs.get`), so this top-level field is
    /// the only bytes-figure we can read without a follow-up API call.
    /// `stats_from_response` falls back to this when `statistics` is
    /// absent.
    #[serde(default)]
    total_bytes_processed: Option<String>,
    /// Query-job statistics. Populated for `jobs.get` responses; the
    /// synchronous `jobs.query` and `jobs.getQueryResults` endpoints
    /// omit this entire block, so reading bytes from here alone leaves
    /// every sync query with `bytes_scanned: None` (PR #326). The
    /// top-level `total_bytes_processed` covers the sync path; this
    /// stays `Option` for forward compatibility with `jobs.get` paths.
    #[serde(default)]
    statistics: Option<BigQueryStatistics>,
    /// Top-level `errors[]` array on the `jobs.query` `QueryResponse` /
    /// `jobs.getQueryResults` `GetQueryResultsResponse`. When a query job
    /// BigQuery *accepted* (HTTP 200, `jobComplete=true`) then *failed*,
    /// the failure surfaces here as an `ErrorProto` array — there is no
    /// top-level `status` block on these endpoints (that nesting belongs
    /// to the `jobs.get` `Job` resource). Before this field existed, an
    /// accepted-but-failed job was silently returned as `Ok` with an
    /// empty result set. `check_job_error` maps a non-empty `errors[]` to
    /// [`BigQueryError::JobError`].
    ///
    /// Note: BigQuery documents that `errors[]` can also carry *warnings*
    /// on a successful job, in which case result rows are still present.
    /// `check_job_error` keys on the first `ErrorProto` regardless; in
    /// practice the synchronous query endpoint returns warnings rarely and
    /// a true failure always carries no rows. Sync rejections at job
    /// creation arrive as a non-2xx Google API `error` envelope instead
    /// and are handled by the `!resp.status().is_success()` branch.
    #[serde(default)]
    errors: Option<Vec<ErrorProto>>,
}

impl BigQueryResponse {
    /// Map a top-level `errors[]` on a completed job to a
    /// [`BigQueryError::JobError`], so an async job failure surfaces as an
    /// error instead of an empty result set. No-op when `errors` is absent
    /// or empty (the common success case).
    fn check_job_error(&self) -> Result<(), BigQueryError> {
        let Some(first) = self.errors.as_ref().and_then(|e| e.first()) else {
            return Ok(());
        };
        Err(BigQueryError::JobError {
            reason: first.reason.clone().unwrap_or_else(|| "unknown".into()),
            message: first.message.clone().unwrap_or_default(),
        })
    }
}

/// A single BigQuery `ErrorProto` entry from a job-level `errors[]` array.
/// Only `reason` + `message` are read; the other documented fields
/// (`location`, `locationType`, `debugInfo`) are ignored.
#[derive(Debug, Deserialize)]
struct ErrorProto {
    #[serde(default)]
    reason: Option<String>,
    #[serde(default)]
    message: Option<String>,
}

/// Classifies whether a [`BigQueryError`] is transient and worth retrying.
///
/// Transient (retried with backoff):
/// - HTTP 429 (Too Many Requests), 502/503/504 (server errors)
/// - Network connection or HTTP-request timeout errors
///
/// **Not** transient (returned on the first attempt):
/// - Auth failures (401/403) and a 400 query rejection — retrying a bad
///   token or a syntax error only burns quota. (Unlike the Databricks
///   adapter, this connector does not refresh a cached token on 401/403;
///   `BigQueryAuth::ServiceAccount` mints fresh JWTs per-exchange and a
///   true bad-credential 401 would re-fail every attempt anyway.)
/// - A job-level [`BigQueryError::JobError`] — a rejected query won't pass
///   on retry.
/// - A poll-loop [`BigQueryError::Timeout`] — the job already ran to the
///   deadline; re-submitting it is a fresh statement's concern.
pub(crate) fn is_transient(err: &BigQueryError) -> bool {
    match err {
        BigQueryError::ApiError { status, .. } => {
            // `status` is the reqwest `StatusCode` `Display` form, e.g.
            // "429 Too Many Requests". Match on the leading code.
            let code = status
                .split_whitespace()
                .next()
                .and_then(|c| c.parse::<u16>().ok());
            matches!(code, Some(429 | 502 | 503 | 504))
        }
        BigQueryError::Http(e) => e.is_connect() || e.is_timeout(),
        BigQueryError::JobError { .. }
        | BigQueryError::LoadJobError { .. }
        | BigQueryError::Auth(_)
        | BigQueryError::Timeout { .. }
        | BigQueryError::RetryBudgetExhausted { .. }
        | BigQueryError::StorageRead(_) => false,
    }
}

/// Classify a BigQuery [`AdapterError`] for the run loop's classified-retry
/// layer, reusing the connector's own [`is_transient`] judgement so the run
/// loop and the connector agree by construction.
///
/// Downcasts to a [`BigQueryError`]: transient ⇒ [`FailureClass::Transient`]
/// with a kind read from the HTTP shape; not transient (a job/type error, auth,
/// budget exhaustion) ⇒ [`FailureClass::Permanent`]; not a [`BigQueryError`] ⇒
/// [`FailureClass::Unknown`] (fail closed).
#[must_use]
pub(crate) fn classify_bigquery_failure(err: &AdapterError) -> FailureClass {
    let Some(bq_err) = err.inner().downcast_ref::<BigQueryError>() else {
        return FailureClass::Unknown;
    };
    if !is_transient(bq_err) {
        return FailureClass::Permanent;
    }
    let kind = match bq_err {
        BigQueryError::ApiError { status, .. } => {
            let code = status
                .split_whitespace()
                .next()
                .and_then(|c| c.parse::<u16>().ok());
            match code {
                Some(429) => TransientKind::RateLimit,
                _ => TransientKind::Network,
            }
        }
        BigQueryError::Http(e) if e.is_timeout() => TransientKind::Timeout,
        BigQueryError::Http(_) => TransientKind::Network,
        _ => TransientKind::Other,
    };
    FailureClass::Transient(kind)
}

/// Top-level `statistics` block on a BigQuery job response. Only the
/// `query` slice is currently parsed; other nested shapes (load, extract,
/// copy) are ignored.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BigQueryStatistics {
    #[serde(default)]
    query: Option<BigQueryQueryStatistics>,
    /// `statistics.load` slice — populated on a load job's `jobs.get`
    /// response. Carries `outputRows` (rows landed in the target) and
    /// `inputFileBytes` (bytes read from the source URIs). Both are
    /// decimal-string int64s, parsed best-effort.
    #[serde(default)]
    load: Option<BigQueryLoadStatistics>,
}

/// `statistics.load` subset for a BigQuery load job.
///
/// BigQuery encodes int64 figures as decimal strings (JSON has no native
/// 64-bit int), so both fields are `Option<String>` parsed to `u64`.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BigQueryLoadStatistics {
    /// Number of rows the load job wrote into the destination table.
    #[serde(default)]
    output_rows: Option<String>,
    /// Bytes read from the source URIs. Used as `LoadResult.bytes_read`
    /// on the cloud-URI path, where the caller has no local file size.
    #[serde(default)]
    input_file_bytes: Option<String>,
}

impl BigQueryLoadStatistics {
    /// Parse `outputRows` to `u64`. Missing / unparseable → `None`.
    fn output_rows_u64(&self) -> Option<u64> {
        self.output_rows
            .as_deref()
            .and_then(|s| s.parse::<u64>().ok())
    }

    /// Parse `inputFileBytes` to `u64`. Missing / unparseable → `None`.
    fn input_file_bytes_u64(&self) -> Option<u64> {
        self.input_file_bytes
            .as_deref()
            .and_then(|s| s.parse::<u64>().ok())
    }
}

/// `statistics.query` subset we care about today.
///
/// BigQuery returns byte counts as decimal strings (JSON doesn't have a
/// native 64-bit integer; GCP APIs encode `int64` fields as strings), so
/// both fields are `Option<String>` and parsed to `u64` via
/// [`BigQueryQueryStatistics::total_bytes_billed_u64`].
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BigQueryQueryStatistics {
    /// Bytes BigQuery read to produce the result. Strictly `>=`
    /// `total_bytes_billed` because the billable figure has the 10 MB
    /// minimum floor applied.
    #[allow(dead_code)]
    #[serde(default)]
    total_bytes_processed: Option<String>,
    /// Bytes the user is actually billed for. This is the figure
    /// threaded into [`ExecutionStats::bytes_scanned`] because
    /// [`rocky_core::cost::compute_observed_cost_usd`] multiplies it by
    /// the per-TB rate to produce the billed cost. Storing "billed" in
    /// a field named "scanned" is a deliberate impurity — the cost
    /// formula wants billed, and introducing a separate "billed" field
    /// would force every downstream consumer to special-case BigQuery.
    #[serde(default)]
    total_bytes_billed: Option<String>,
}

impl BigQueryQueryStatistics {
    /// Parse `totalBytesBilled` to `u64`. Invalid / overflowing / missing
    /// values all return `None` so the stats remain best-effort and
    /// never fail a run.
    fn total_bytes_billed_u64(&self) -> Option<u64> {
        self.total_bytes_billed
            .as_deref()
            .and_then(|s| s.parse::<u64>().ok())
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JobReference {
    #[allow(dead_code)]
    project_id: String,
    job_id: String,
}

/// `configuration.query.destinationTable` shape from `jobs.get`.
///
/// For a non-DDL `jobs.query` job, BigQuery silently materializes the
/// result rows in an anonymous table named under `<project>:_<hash>`
/// and returns that table reference here. The Storage Read API's
/// `CreateReadSession.read_session.table` accepts the same
/// `projects/<project>/datasets/<dataset>/tables/<table>` form built
/// from these three fields.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JobDestinationTable {
    project_id: String,
    dataset_id: String,
    table_id: String,
}

#[derive(Debug, Deserialize)]
struct TableSchema {
    fields: Vec<TableFieldSchema>,
}

#[derive(Debug, Deserialize)]
struct TableFieldSchema {
    name: String,
    #[serde(rename = "type")]
    #[allow(dead_code)]
    field_type: String,
    #[allow(dead_code)]
    #[serde(default)]
    mode: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TableRow {
    f: Vec<TableCell>,
}

#[derive(Debug, Deserialize)]
struct TableCell {
    v: Option<serde_json::Value>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_request_serialization() {
        let req = QueryRequest {
            query: "SELECT 1".to_string(),
            use_legacy_sql: false,
            location: "US".to_string(),
            timeout_ms: 30000,
            max_results: MAX_RESULTS_PER_PAGE,
        };
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("useLegacySql"));
        assert!(json.contains("timeoutMs"));
    }

    #[test]
    fn test_response_deserialization() {
        let json = r#"{
            "jobComplete": true,
            "schema": {
                "fields": [
                    {"name": "id", "type": "INTEGER"},
                    {"name": "name", "type": "STRING"}
                ]
            },
            "rows": [
                {"f": [{"v": "1"}, {"v": "test"}]}
            ],
            "totalRows": "1"
        }"#;
        let resp: BigQueryResponse = serde_json::from_str(json).unwrap();
        assert!(resp.job_complete);
        assert_eq!(resp.schema.unwrap().fields.len(), 2);
        assert_eq!(resp.rows.unwrap().len(), 1);
    }

    #[test]
    fn test_response_deserialization_minimal() {
        let json = r#"{"jobComplete": true}"#;
        let resp: BigQueryResponse = serde_json::from_str(json).unwrap();
        assert!(resp.job_complete);
        assert!(resp.schema.is_none());
        assert!(resp.rows.is_none());
    }

    #[test]
    fn poll_delay_follows_fixed_ladder_then_exponential_cap() {
        // First 5 polls: fixed ladder.
        assert_eq!(poll_delay(0), Duration::from_millis(100));
        assert_eq!(poll_delay(1), Duration::from_millis(200));
        assert_eq!(poll_delay(2), Duration::from_millis(500));
        assert_eq!(poll_delay(3), Duration::from_millis(1000));
        assert_eq!(poll_delay(4), Duration::from_millis(2000));

        // Past the ladder: exponential growth capped at MAX_POLL_DELAY_MS.
        // step 0 → 2000*1 = 2000, step 1 → 4000, step 2 → 8000 → capped at 5000.
        assert_eq!(poll_delay(5), Duration::from_millis(2000));
        assert_eq!(poll_delay(6), Duration::from_millis(4000));
        assert_eq!(poll_delay(7), Duration::from_millis(MAX_POLL_DELAY_MS));
        assert_eq!(poll_delay(100), Duration::from_millis(MAX_POLL_DELAY_MS));
    }

    #[test]
    fn response_with_job_complete_false_still_deserializes() {
        // Regression: the polling loop's `Err(missing jobReference)` branch
        // only triggers when BigQuery returns `jobComplete=false` with no
        // `jobReference`, which is the precondition we need to surface.
        let json = r#"{"jobComplete": false}"#;
        let resp: BigQueryResponse = serde_json::from_str(json).unwrap();
        assert!(!resp.job_complete);
        assert!(resp.job_reference.is_none());
    }

    #[test]
    fn response_with_job_complete_false_and_job_reference_deserializes() {
        // Happy path into the polling loop: async job, reference present.
        let json = r#"{
            "jobComplete": false,
            "jobReference": {"projectId": "p", "jobId": "job_abc"}
        }"#;
        let resp: BigQueryResponse = serde_json::from_str(json).unwrap();
        assert!(!resp.job_complete);
        let job_ref = resp.job_reference.unwrap();
        assert_eq!(job_ref.job_id, "job_abc");
    }

    #[test]
    fn statistics_deserializes_when_present() {
        // Happy path: completed query job with totalBytesBilled.
        // BigQuery always encodes int64 as decimal strings — parser
        // must handle the string form.
        let json = r#"{
            "jobComplete": true,
            "statistics": {
                "query": {
                    "totalBytesProcessed": "12345678",
                    "totalBytesBilled": "10485760"
                }
            }
        }"#;
        let resp: BigQueryResponse = serde_json::from_str(json).unwrap();
        let stats = stats_from_response(&resp);
        assert_eq!(stats.bytes_scanned, Some(10_485_760));
        assert_eq!(stats.bytes_written, None);
        assert_eq!(stats.rows_affected, None);
    }

    #[test]
    fn statistics_absent_yields_all_none() {
        // BigQuery omits `statistics` for some DDL responses — must not
        // fail to deserialize, and stats should be all-None.
        let json = r#"{"jobComplete": true}"#;
        let resp: BigQueryResponse = serde_json::from_str(json).unwrap();
        assert!(resp.statistics.is_none());
        let stats = stats_from_response(&resp);
        assert_eq!(stats.bytes_scanned, None);
        assert_eq!(stats.bytes_written, None);
    }

    #[test]
    fn statistics_query_absent_yields_none() {
        // The outer `statistics` block is present but the `query` slice
        // isn't — shouldn't blow up.
        let json = r#"{
            "jobComplete": true,
            "statistics": {}
        }"#;
        let resp: BigQueryResponse = serde_json::from_str(json).unwrap();
        let stats = stats_from_response(&resp);
        assert_eq!(stats.bytes_scanned, None);
    }

    #[test]
    fn statistics_total_bytes_billed_absent_yields_none() {
        // `statistics.query` is present but only `totalBytesProcessed`
        // is set — the billed-field parse returns None.
        let json = r#"{
            "jobComplete": true,
            "statistics": {
                "query": {
                    "totalBytesProcessed": "12345678"
                }
            }
        }"#;
        let resp: BigQueryResponse = serde_json::from_str(json).unwrap();
        let stats = stats_from_response(&resp);
        assert_eq!(stats.bytes_scanned, None);
    }

    #[test]
    fn statistics_unparseable_bytes_yields_none() {
        // Defensive: if BigQuery ever returns a non-integer (never
        // observed but the REST contract is a string), parsing must
        // fail softly rather than erroring the whole run.
        let json = r#"{
            "jobComplete": true,
            "statistics": {
                "query": {
                    "totalBytesBilled": "not-a-number"
                }
            }
        }"#;
        let resp: BigQueryResponse = serde_json::from_str(json).unwrap();
        let stats = stats_from_response(&resp);
        assert_eq!(stats.bytes_scanned, None);
    }

    #[test]
    fn test_response_deserialization_null_cell() {
        let json = r#"{
            "jobComplete": true,
            "rows": [
                {"f": [{"v": null}, {"v": "test"}]}
            ]
        }"#;
        let resp: BigQueryResponse = serde_json::from_str(json).unwrap();
        let rows = resp.rows.unwrap();
        assert!(rows[0].f[0].v.is_none());
        assert_eq!(rows[0].f[1].v.as_ref().unwrap(), "test");
    }

    #[test]
    fn jobs_query_top_level_total_bytes_processed_is_used() {
        // The synchronous `jobs.query` / `jobs.getQueryResults` endpoints
        // surface `totalBytesProcessed` at the top level of the response
        // and omit the `statistics` block entirely. Before PR #326 the
        // connector only read from `statistics.query.totalBytesBilled`,
        // so every sync query returned `bytes_scanned: None` and broke
        // cost attribution end-to-end.
        let json = r#"{
            "jobComplete": true,
            "totalBytesProcessed": "10485760"
        }"#;
        let resp: BigQueryResponse = serde_json::from_str(json).unwrap();
        let stats = stats_from_response(&resp);
        assert_eq!(stats.bytes_scanned, Some(10_485_760));
    }

    #[test]
    fn jobs_get_statistics_block_takes_precedence_over_top_level() {
        // When both shapes are present (e.g., a future code path that
        // also fetches `jobs.get`), the more accurate
        // `statistics.query.totalBytesBilled` wins because it includes
        // the 10 MB minimum-bill floor that `totalBytesProcessed`
        // doesn't account for.
        let json = r#"{
            "jobComplete": true,
            "totalBytesProcessed": "5242880",
            "statistics": {
                "query": {
                    "totalBytesBilled": "10485760"
                }
            }
        }"#;
        let resp: BigQueryResponse = serde_json::from_str(json).unwrap();
        let stats = stats_from_response(&resp);
        assert_eq!(stats.bytes_scanned, Some(10_485_760));
    }

    #[test]
    fn job_errors_array_real_shape_deserializes_and_maps_to_job_error() {
        // The real top-level `errors[]` ErrorProto shape captured from a
        // live BigQuery sandbox probe — `{message, reason, domain,
        // location, locationType}` at the response top level. (The live
        // `jobs.query` runtime-failure probe returned this wrapped in a
        // 400 `error.errors[]` envelope; the same ErrorProto is the
        // top-level `errors[]` on a `jobComplete=true` async-failure
        // response.) `check_job_error` maps the first entry to a
        // `JobError` carrying `reason` + `message`.
        let json = r#"{
            "jobComplete": true,
            "jobReference": {"projectId": "p", "jobId": "job-x"},
            "errors": [
                {
                    "message": "division by zero: 1 / 0",
                    "domain": "global",
                    "reason": "invalidQuery",
                    "location": "q",
                    "locationType": "parameter"
                }
            ]
        }"#;
        let resp: BigQueryResponse = serde_json::from_str(json).unwrap();
        let err = resp
            .check_job_error()
            .expect_err("a top-level errors[] must map to a JobError");
        match err {
            BigQueryError::JobError { reason, message } => {
                assert_eq!(reason, "invalidQuery");
                assert_eq!(message, "division by zero: 1 / 0");
            }
            other => panic!("expected JobError, got: {other:?}"),
        }
    }

    #[test]
    fn check_job_error_is_noop_without_errors() {
        // A normal completed response (no `errors` field) — the happy
        // path captured live had no `errors` key at all. Must stay Ok so
        // empty-result / DDL responses aren't mistaken for failures.
        let json = r#"{
            "jobComplete": true,
            "jobReference": {"projectId": "p", "jobId": "job-ok"},
            "rows": [{"f": [{"v": "1"}]}]
        }"#;
        let resp: BigQueryResponse = serde_json::from_str(json).unwrap();
        assert!(resp.errors.is_none());
        assert!(resp.check_job_error().is_ok());
    }

    #[test]
    fn check_job_error_is_noop_for_empty_errors_array() {
        // Defensive: an empty `errors[]` array (not `null`) must not be
        // treated as a failure.
        let json = r#"{
            "jobComplete": true,
            "errors": []
        }"#;
        let resp: BigQueryResponse = serde_json::from_str(json).unwrap();
        assert!(resp.check_job_error().is_ok());
    }

    #[test]
    fn is_transient_classifies_retryable_vs_terminal() {
        // Retryable: 429 / 502 / 503 / 504 (status is the reqwest
        // StatusCode Display form, e.g. "429 Too Many Requests").
        for code in ["429 Too Many Requests", "502 Bad Gateway", "503", "504"] {
            assert!(
                is_transient(&BigQueryError::ApiError {
                    status: code.into(),
                    message: "x".into(),
                }),
                "{code} should be transient"
            );
        }
        // Terminal: auth 401/403 and a 400 rejection are NOT retried.
        for code in ["400 Bad Request", "401 Unauthorized", "403 Forbidden"] {
            assert!(
                !is_transient(&BigQueryError::ApiError {
                    status: code.into(),
                    message: "x".into(),
                }),
                "{code} should be terminal"
            );
        }
        // Terminal: a job-level rejection and a poll timeout don't pass
        // on retry.
        assert!(!is_transient(&BigQueryError::JobError {
            reason: "invalidQuery".into(),
            message: "bad".into(),
        }));
        assert!(!is_transient(&BigQueryError::Timeout { timeout_secs: 1 }));
        assert!(!is_transient(&BigQueryError::RetryBudgetExhausted {
            limit: 1
        }));
    }

    /// The run-loop classifier hoists `is_transient`: a 429 becomes a
    /// rate-limited Transient, a 5xx a network Transient, a job/type error
    /// Permanent, and a non-`BigQueryError` boxed error `Unknown`. A permanent
    /// error is never marked retryable (the dangerous direction).
    #[test]
    fn classify_bigquery_failure_maps_three_ways() {
        let rate_limited = classify_bigquery_failure(&AdapterError::new(BigQueryError::ApiError {
            status: "429 Too Many Requests".into(),
            message: "slow down".into(),
        }));
        assert_eq!(
            rate_limited,
            FailureClass::Transient(TransientKind::RateLimit)
        );

        let unavailable = classify_bigquery_failure(&AdapterError::new(BigQueryError::ApiError {
            status: "503 Service Unavailable".into(),
            message: "x".into(),
        }));
        assert_eq!(unavailable, FailureClass::Transient(TransientKind::Network));

        let job_err = classify_bigquery_failure(&AdapterError::new(BigQueryError::JobError {
            reason: "invalidQuery".into(),
            message: "bad column".into(),
        }));
        assert_eq!(job_err, FailureClass::Permanent);
        assert!(!job_err.is_retryable());

        let budget =
            classify_bigquery_failure(&AdapterError::new(BigQueryError::RetryBudgetExhausted {
                limit: 1,
            }));
        assert_eq!(budget, FailureClass::Permanent);

        let unknown = classify_bigquery_failure(&AdapterError::msg("not a bigquery error"));
        assert_eq!(unknown, FailureClass::Unknown);
    }
}
