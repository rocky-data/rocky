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

use crate::connector::SnowflakeConnector;
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
    /// error type.
    async fn exec(&self, sql: &str) -> AdapterResult<()> {
        self.connector
            .execute_statement(sql)
            .await
            .map(|_| ())
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
        let file_size = match source {
            LoadSource::CloudUri(uri) => {
                // External stage path: one CREATE, one COPY, one DROP.
                let create_sql = create_external_stage_sql(&stage, uri, format, options);
                debug!(sql = %create_sql, "creating external stage");
                self.exec(&create_sql).await?;

                let copy_sql = copy_into_sql(&target_ref, &stage);
                info!(sql = %copy_sql, "snowflake COPY INTO (external stage)");
                let copy_result = self.exec(&copy_sql).await;

                // Drop stage regardless of COPY result to tidy up.
                let drop_sql = drop_stage_sql(&stage);
                if let Err(e) = self.exec(&drop_sql).await {
                    warn!(error = %e, stage = %stage, "failed to drop stage (non-fatal)");
                }

                copy_result?;
                0u64 // No local byte count for cloud sources.
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
                let copy_result = self.exec(&copy_sql).await;

                let drop_sql = drop_stage_sql(&stage);
                if let Err(e) = self.exec(&drop_sql).await {
                    warn!(error = %e, stage = %stage, "failed to drop stage (non-fatal)");
                }

                copy_result?;

                std::fs::metadata(local_path)
                    .map(|m| m.len())
                    .unwrap_or_default()
            }
        };

        Ok(LoadResult {
            // Snowflake's COPY INTO returns row counts in the response payload,
            // but `execute_statement` here surfaces only success/failure. A
            // follow-up could parse counts from the connector's result row.
            rows_loaded: 0,
            bytes_read: file_size,
            duration_ms: start.elapsed().as_millis() as u64,
        })
    }

    fn supported_formats(&self) -> Vec<FileFormat> {
        vec![FileFormat::Csv, FileFormat::Parquet, FileFormat::JsonLines]
    }
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
}
