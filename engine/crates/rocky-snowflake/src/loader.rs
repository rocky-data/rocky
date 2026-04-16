//! Snowflake [`LoaderAdapter`] implementation — stage + COPY INTO.
//!
//! Snowflake's bulk-load model: files live in a *stage*, then `COPY INTO` reads
//! from that stage into the target table. Two paths:
//!
//! - **Local file:** `CREATE TEMPORARY STAGE → PUT file://... @stg → COPY INTO → DROP`
//! - **Cloud URI:** `CREATE TEMPORARY STAGE ... URL = 's3://...' → COPY INTO → DROP`
//!
//! Temporary stages auto-expire at session end, so a failed load still cleans
//! up. The explicit `DROP STAGE IF EXISTS` after each load keeps long-running
//! sessions tidy.

use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use tracing::{debug, info, warn};

use rocky_adapter_sdk::{
    AdapterError, AdapterResult, FileFormat, LoadOptions, LoadResult, LoadSource, LoaderAdapter,
    TableRef,
};

use crate::connector::{QueryResult, SnowflakeConnector};
use crate::stage::{
    copy_into_sql, create_external_stage_sql, create_temporary_stage_sql, drop_stage_sql,
    generate_stage_name, put_file_sql,
};

/// Snowflake loader adapter using temporary stages + COPY INTO.
pub struct SnowflakeLoaderAdapter {
    connector: Arc<SnowflakeConnector>,
}

impl SnowflakeLoaderAdapter {
    /// Create a loader wrapping the given `SnowflakeConnector`.
    pub fn new(connector: Arc<SnowflakeConnector>) -> Self {
        Self { connector }
    }

    /// Execute a statement, mapping connector errors into the adapter's
    /// error type. Used when the caller doesn't need the result rows.
    async fn exec(&self, sql: &str) -> AdapterResult<()> {
        self.connector
            .execute_statement(sql)
            .await
            .map(|_| ())
            .map_err(|e| AdapterError::msg(e.to_string()))
    }

    /// Execute a statement and return the full result set. Used by COPY INTO
    /// so we can parse per-file `rows_loaded` from the response.
    async fn exec_with_rows(&self, sql: &str) -> AdapterResult<QueryResult> {
        self.connector
            .execute_sql(sql)
            .await
            .map_err(|e| AdapterError::msg(e.to_string()))
    }
}

impl std::fmt::Debug for SnowflakeLoaderAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnowflakeLoaderAdapter").finish()
    }
}

/// Resolve the file format from explicit option or the source's extension.
fn resolve_format(source: &LoadSource, options: &LoadOptions) -> AdapterResult<FileFormat> {
    if let Some(fmt) = options.format {
        return Ok(fmt);
    }
    let ext = source.extension().unwrap_or_default();
    FileFormat::from_extension(ext).ok_or_else(|| {
        AdapterError::msg(format!(
            "cannot detect file format from extension '{ext}'; set options.format explicitly"
        ))
    })
}

/// Render a Snowflake target as `db.schema.table` (Snowflake uses the same
/// 3-part form as Databricks for fully-qualified references).
fn format_target(target: &TableRef) -> String {
    if target.catalog.is_empty() {
        format!("{}.{}", target.schema, target.table)
    } else {
        format!("{}.{}.{}", target.catalog, target.schema, target.table)
    }
}

#[async_trait]
impl LoaderAdapter for SnowflakeLoaderAdapter {
    async fn load(
        &self,
        source: &LoadSource,
        target: &TableRef,
        options: &LoadOptions,
    ) -> AdapterResult<LoadResult> {
        let start = Instant::now();
        let format = resolve_format(source, options)?;
        let target_ref = format_target(target);
        let stage = generate_stage_name();

        // `truncate_first` → DELETE FROM before COPY INTO. Snowflake supports
        // TRUNCATE TABLE but DELETE is more portable and plays nicer with
        // tables that have transient dependents.
        if options.truncate_first {
            let sql = format!("DELETE FROM {target_ref}");
            debug!(sql = %sql, "truncating target before COPY INTO");
            self.exec(&sql).await?;
        }

        // Build and run the appropriate sequence for the source type.
        let (rows_loaded, file_size) = match source {
            LoadSource::CloudUri(uri) => {
                // External stage path: one CREATE, one COPY, one DROP.
                let create_sql = create_external_stage_sql(&stage, uri, format, options);
                debug!(sql = %create_sql, "creating external stage");
                self.exec(&create_sql).await?;

                let copy_sql = copy_into_sql(&target_ref, &stage);
                info!(sql = %copy_sql, "snowflake COPY INTO (external stage)");
                let copy_result = self.exec_with_rows(&copy_sql).await;

                // Drop stage regardless of COPY result to tidy up.
                let drop_sql = drop_stage_sql(&stage);
                if let Err(e) = self.exec(&drop_sql).await {
                    warn!(error = %e, stage = %stage, "failed to drop stage (non-fatal)");
                }

                let rows = rows_from_copy_result(&copy_result?);
                (rows, 0u64) // No local byte count for cloud sources.
            }
            LoadSource::LocalFile(local_path) => {
                // Internal stage path: CREATE, PUT, COPY, DROP.
                let create_sql = create_temporary_stage_sql(&stage, format, options);
                debug!(sql = %create_sql, "creating internal stage");
                self.exec(&create_sql).await?;

                let local_str = local_path_str(local_path)?;
                let put_sql = put_file_sql(&local_str, &stage);
                info!(sql = %put_sql, "snowflake PUT");
                let put_result = self.exec(&put_sql).await;

                // If PUT fails, still try to clean up the stage.
                if put_result.is_err() {
                    let drop_sql = drop_stage_sql(&stage);
                    if let Err(e) = self.exec(&drop_sql).await {
                        warn!(error = %e, stage = %stage, "failed to drop stage after PUT failure");
                    }
                    put_result?;
                }

                let copy_sql = copy_into_sql(&target_ref, &stage);
                info!(sql = %copy_sql, "snowflake COPY INTO (internal stage)");
                let copy_result = self.exec_with_rows(&copy_sql).await;

                let drop_sql = drop_stage_sql(&stage);
                if let Err(e) = self.exec(&drop_sql).await {
                    warn!(error = %e, stage = %stage, "failed to drop stage (non-fatal)");
                }

                let rows = rows_from_copy_result(&copy_result?);
                let bytes = std::fs::metadata(local_path)
                    .map(|m| m.len())
                    .unwrap_or_default();
                (rows, bytes)
            }
        };

        Ok(LoadResult {
            rows_loaded,
            bytes_read: file_size,
            duration_ms: start.elapsed().as_millis() as u64,
        })
    }

    fn supported_formats(&self) -> Vec<FileFormat> {
        vec![FileFormat::Csv, FileFormat::Parquet, FileFormat::JsonLines]
    }
}

/// Extract the total `rows_loaded` from a COPY INTO response, warning when
/// the column is missing so unexpected API shapes are visible in logs.
///
/// Returns `0` (rather than failing the load) when the column isn't present.
fn rows_from_copy_result(result: &QueryResult) -> u64 {
    match sum_rows_loaded(result) {
        Some(n) => n,
        None => {
            warn!(
                statement_handle = %result.statement_handle,
                "snowflake COPY INTO response missing `rows_loaded` column; \
                 reporting 0 rows loaded"
            );
            0
        }
    }
}

/// Sum the `rows_loaded` column across every row of a COPY INTO result.
///
/// Snowflake's COPY INTO returns one row per source file loaded with
/// columns including `FILE`, `STATUS`, `ROWS_PARSED`, `ROWS_LOADED`,
/// `ERRORS_SEEN`, etc. Summing `rows_loaded` across files gives the total
/// rows appended to the target table for this COPY INTO call.
///
/// Returns `None` when the column is missing from the response schema —
/// callers should default to 0 so unexpected API shapes don't fail the
/// load.
fn sum_rows_loaded(result: &QueryResult) -> Option<u64> {
    let idx = result
        .columns
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case("rows_loaded"))?;
    let mut total: u64 = 0;
    for row in &result.rows {
        let Some(cell) = row.get(idx) else {
            continue;
        };
        let parsed = match cell {
            serde_json::Value::Number(n) => n
                .as_u64()
                .or_else(|| n.as_i64().and_then(|i| u64::try_from(i).ok())),
            serde_json::Value::String(s) => s.parse().ok(),
            _ => None,
        };
        if let Some(v) = parsed {
            total = total.saturating_add(v);
        }
    }
    Some(total)
}

/// Convert a local path to a string for SQL interpolation, rejecting paths
/// that contain single quotes (which would break the `file://` URI literal).
fn local_path_str(path: &std::path::Path) -> AdapterResult<String> {
    let s = path.display().to_string();
    if s.contains('\'') {
        return Err(AdapterError::msg(format!(
            "local path '{s}' contains a single quote, which is not supported \
             in Snowflake PUT statements"
        )));
    }
    Ok(s)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_format_target_fully_qualified() {
        let t = TableRef {
            catalog: "DB".into(),
            schema: "RAW".into(),
            table: "ORDERS".into(),
        };
        assert_eq!(format_target(&t), "DB.RAW.ORDERS");
    }

    #[test]
    fn test_format_target_no_catalog() {
        let t = TableRef {
            catalog: String::new(),
            schema: "RAW".into(),
            table: "ORDERS".into(),
        };
        assert_eq!(format_target(&t), "RAW.ORDERS");
    }

    #[test]
    fn test_resolve_format_from_extension() {
        let src = LoadSource::LocalFile(PathBuf::from("/tmp/data.parquet"));
        assert_eq!(
            resolve_format(&src, &LoadOptions::default()).unwrap(),
            FileFormat::Parquet
        );
    }

    #[test]
    fn test_resolve_format_cloud_uri() {
        let src = LoadSource::CloudUri("s3://bucket/data.csv".into());
        assert_eq!(
            resolve_format(&src, &LoadOptions::default()).unwrap(),
            FileFormat::Csv
        );
    }

    #[test]
    fn test_local_path_str_rejects_quote() {
        let bad = PathBuf::from("/tmp/it's.csv");
        assert!(local_path_str(&bad).is_err());
    }

    #[test]
    fn test_local_path_str_accepts_normal() {
        let p = PathBuf::from("/tmp/data.csv");
        assert_eq!(local_path_str(&p).unwrap(), "/tmp/data.csv");
    }

    fn copy_into_result(columns: &[&str], rows: Vec<Vec<serde_json::Value>>) -> QueryResult {
        use crate::connector::ColumnMetaData;
        let columns = columns
            .iter()
            .map(|n| ColumnMetaData {
                name: (*n).to_string(),
                type_name: Some("FIXED".into()),
                nullable: Some(true),
            })
            .collect();
        let row_count = rows.len() as u64;
        QueryResult {
            statement_handle: "sf-copy".into(),
            columns,
            rows,
            total_row_count: Some(row_count),
        }
    }

    #[test]
    fn test_sum_rows_loaded_single_file() {
        let qr = copy_into_result(
            &[
                "file",
                "status",
                "rows_parsed",
                "rows_loaded",
                "errors_seen",
            ],
            vec![vec![
                serde_json::json!("users_1.csv"),
                serde_json::json!("LOADED"),
                serde_json::json!(100),
                serde_json::json!(100),
                serde_json::json!(0),
            ]],
        );
        assert_eq!(sum_rows_loaded(&qr), Some(100));
    }

    #[test]
    fn test_sum_rows_loaded_multiple_files() {
        let qr = copy_into_result(
            &["file", "rows_loaded"],
            vec![
                vec![serde_json::json!("a.csv"), serde_json::json!(10)],
                vec![serde_json::json!("b.csv"), serde_json::json!(20)],
                vec![serde_json::json!("c.csv"), serde_json::json!(30)],
            ],
        );
        assert_eq!(sum_rows_loaded(&qr), Some(60));
    }

    #[test]
    fn test_sum_rows_loaded_string_values() {
        // Snowflake's REST API returns numeric columns as JSON strings.
        let qr = copy_into_result(
            &["rows_loaded"],
            vec![vec![serde_json::json!("50")], vec![serde_json::json!("75")]],
        );
        assert_eq!(sum_rows_loaded(&qr), Some(125));
    }

    #[test]
    fn test_sum_rows_loaded_case_insensitive() {
        let qr = copy_into_result(&["ROWS_LOADED"], vec![vec![serde_json::json!(42)]]);
        assert_eq!(sum_rows_loaded(&qr), Some(42));
    }

    #[test]
    fn test_sum_rows_loaded_missing_column() {
        let qr = copy_into_result(
            &["file", "status"],
            vec![vec![
                serde_json::json!("users.csv"),
                serde_json::json!("LOADED"),
            ]],
        );
        assert_eq!(sum_rows_loaded(&qr), None);
    }

    #[test]
    fn test_sum_rows_loaded_empty_result() {
        let qr = copy_into_result(&["rows_loaded"], vec![]);
        assert_eq!(sum_rows_loaded(&qr), Some(0));
    }
}
