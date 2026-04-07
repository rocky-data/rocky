//! BigQuery REST API connector — jobs.query + jobs.getQueryResults.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;
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
                .pool_max_idle_per_host(32)
                .pool_idle_timeout(std::time::Duration::from_secs(300))
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

        // Handle job not complete — poll for results
        if !response.job_complete {
            let job_id = response
                .job_reference
                .as_ref()
                .map(|r| r.job_id.as_str())
                .unwrap_or("unknown");
            warn!(
                job_id = job_id,
                "BigQuery query not complete — polling not yet implemented"
            );
        }

        Ok(response)
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
            .filter_map(|row| {
                row.first()
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_lowercase())
            })
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
