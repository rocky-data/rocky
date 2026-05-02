//! BigQuery REST API connector — jobs.query + jobs.getQueryResults.

use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::time::Instant;
use tracing::{Instrument, debug, info_span};

use rocky_core::ir::{ColumnInfo, TableRef};
use rocky_core::traits::{
    AdapterError, AdapterResult, ChunkChecksum, ExecutionStats, PkRange, QueryResult, SqlDialect,
    WarehouseAdapter,
};
use rocky_sql::validation::{validate_gcp_project_id, validate_identifier};

use crate::auth::BigQueryAuth;
use crate::dialect::BigQueryDialect;

#[derive(Debug, Error)]
pub enum BigQueryError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("BigQuery API error: {message} (status: {status})")]
    ApiError { status: String, message: String },

    #[error("authentication error: {0}")]
    Auth(#[from] crate::auth::AuthError),

    #[error("query timed out after {timeout_secs}s")]
    Timeout { timeout_secs: u64 },
}

/// BigQuery warehouse adapter.
pub struct BigQueryAdapter {
    client: reqwest::Client,
    auth: BigQueryAuth,
    project_id: String,
    location: String,
    dialect: BigQueryDialect,
    timeout_secs: u64,
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
            client: reqwest::Client::builder()
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
                .unwrap_or_else(|_| reqwest::Client::new()),
            auth,
            project_id: project_id.into(),
            location: location.into(),
            dialect: BigQueryDialect,
            timeout_secs: DEFAULT_TIMEOUT_SECS,
        }
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
        let span = info_span!(
            "statement.execute",
            adapter = "bigquery",
            statement.kind = "query",
        );
        async move {
            let token = self.auth.get_token(&self.client).await?;
            let url = format!(
                "https://bigquery.googleapis.com/bigquery/v2/projects/{}/queries",
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
                    message:
                        "BigQuery returned job_complete=false with no job_reference; cannot poll"
                            .into(),
                })?;
            self.poll_query_results(&job_ref).await
        }
        .instrument(span)
        .await
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
            "https://bigquery.googleapis.com/bigquery/v2/projects/{}/queries/{}",
            self.project_id, job_ref.job_id
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
                debug!(
                    job_id = %job_ref.job_id,
                    attempts = attempt + 1,
                    "BigQuery query completed"
                );
                return Ok(response);
            }
        }

        Err(BigQueryError::Timeout {
            timeout_secs: self.timeout_secs,
        })
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
            "https://bigquery.googleapis.com/bigquery/v2/projects/{}/jobs/{}",
            self.project_id, job_ref.job_id
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

    async fn execute_statement_with_stats(&self, sql: &str) -> AdapterResult<ExecutionStats> {
        let mut response = self.run_query(sql).await.map_err(AdapterError::new)?;

        // Enrich with `jobs.get` statistics so cost reporting reflects
        // `totalBytesBilled` (10 MB minimum-bill floor applied) instead
        // of the bare `totalBytesProcessed` `jobs.query` returns.
        // `run_query`'s response shape never carries `statistics` on the
        // sync path, so the `is_none()` check is currently always true
        // when a job ran — but keeping it defensive against future
        // codepaths that might pre-populate the field.
        if response.statistics.is_none() {
            if let Some(ref job_ref) = response.job_reference {
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
}

/// Top-level `statistics` block on a BigQuery job response. Only the
/// `query` slice is currently parsed; other nested shapes (load, extract,
/// copy) are ignored.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BigQueryStatistics {
    #[serde(default)]
    query: Option<BigQueryQueryStatistics>,
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
}
