//! BigQuery REST API connector — jobs.query + jobs.getQueryResults.

use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::time::Instant;
use tracing::{debug, warn};

use rocky_core::ir::{ColumnInfo, TableRef};
use rocky_core::traits::{AdapterError, AdapterResult, QueryResult, SqlDialect, WarehouseAdapter};

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
    ///
    /// Logs a one-time warning that the BigQuery adapter is experimental.
    pub fn new(
        project_id: impl Into<String>,
        location: impl Into<String>,
        auth: BigQueryAuth,
    ) -> Self {
        warn!(
            "BigQuery adapter is experimental. \
             Some features may be incomplete or behave differently from Databricks/Snowflake."
        );
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

    /// Execute a query via the BigQuery REST API.
    async fn run_query(&self, sql: &str) -> Result<BigQueryResponse, BigQueryError> {
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
}

#[async_trait]
impl WarehouseAdapter for BigQueryAdapter {
    fn is_experimental(&self) -> bool {
        true
    }

    fn dialect(&self) -> &dyn SqlDialect {
        &self.dialect
    }

    async fn execute_statement(&self, sql: &str) -> AdapterResult<()> {
        self.run_query(sql)
            .await
            .map(|_| ())
            .map_err(AdapterError::new)
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

        Ok(columns)
    }

    async fn list_tables(&self, catalog: &str, schema: &str) -> AdapterResult<Vec<String>> {
        // BigQuery: catalog = project, schema = dataset
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
}
