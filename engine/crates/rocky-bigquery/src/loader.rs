//! BigQuery [`LoaderAdapter`] implementation — INSERT-fallback path.
//!
//! BigQuery has no SQL `COPY INTO`. This adapter uses the shared
//! [`CsvBatchReader`][rocky_core::arrow_loader::CsvBatchReader] +
//! [`generate_batch_insert_sql`][rocky_core::arrow_loader::generate_batch_insert_sql]
//! utilities to stream a local CSV file into `INSERT INTO ... VALUES (...)`
//! batches, executed via the existing warehouse adapter's `execute_statement`.
//!
//! # Status: starter / dev-scale only
//!
//! INSERT-over-REST is ~3 orders of magnitude slower than native bulk load.
//! **Production-scale BigQuery loading requires the Storage Write API**
//! (gRPC streaming via `tonic`), which is deferred until the need arises.
//! This loader is appropriate for:
//! - Small reference datasets (seeds, lookup tables)
//! - Dev/CI smoke tests
//! - Proof-of-concept pipelines
//!
//! # Cloud URIs
//!
//! [`LoadSource::CloudUri`] is **not supported** — returns
//! `AdapterError::not_supported`. Cloud-storage loading will land with the
//! Storage Write API work.

use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use tracing::{debug, info};

use rocky_adapter_sdk::{
    AdapterError, AdapterResult, FileFormat, LoadOptions, LoadResult, LoadSource, LoaderAdapter,
    TableRef,
};
use rocky_core::arrow_loader::{CsvBatchReader, generate_batch_insert_sql};
use rocky_core::traits::WarehouseAdapter;

use crate::BigQueryAdapter;

/// BigQuery loader adapter — CSV-only, INSERT-over-REST fallback.
pub struct BigQueryLoaderAdapter {
    adapter: Arc<BigQueryAdapter>,
}

impl BigQueryLoaderAdapter {
    /// Create a loader wrapping the given `BigQueryAdapter`.
    pub fn new(adapter: Arc<BigQueryAdapter>) -> Self {
        Self { adapter }
    }
}

impl std::fmt::Debug for BigQueryLoaderAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BigQueryLoaderAdapter").finish()
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

/// Render a BigQuery target as `` `project`.`dataset`.`table` ``.
fn format_target(target: &TableRef) -> String {
    if target.catalog.is_empty() {
        format!("`{}`.`{}`", target.schema, target.table)
    } else {
        format!("`{}`.`{}`.`{}`", target.catalog, target.schema, target.table)
    }
}

#[async_trait]
impl LoaderAdapter for BigQueryLoaderAdapter {
    async fn load(
        &self,
        source: &LoadSource,
        target: &TableRef,
        options: &LoadOptions,
    ) -> AdapterResult<LoadResult> {
        let start = Instant::now();

        // Cloud URIs require the Storage Write API — not implemented yet.
        let local_path = match source {
            LoadSource::LocalFile(p) => p.clone(),
            LoadSource::CloudUri(uri) => {
                return Err(AdapterError::msg(format!(
                    "BigQuery CloudUri loading (got '{uri}') requires the Storage Write API \
                     which is not yet implemented; use a LocalFile source for now"
                )));
            }
        };

        let format = resolve_format(source, options)?;
        if format != FileFormat::Csv {
            return Err(AdapterError::msg(format!(
                "BigQuery loader currently supports only CSV (got {format}); \
                 Parquet/JSONL will land with the Storage Write API"
            )));
        }

        let target_ref = format_target(target);
        let file_size = std::fs::metadata(&local_path)
            .map(|m| m.len())
            .unwrap_or_default();

        // Optional: truncate target before load.
        if options.truncate_first {
            let sql = format!("TRUNCATE TABLE {target_ref}");
            debug!(sql = %sql, "truncating target before load");
            self.adapter
                .execute_statement(&sql)
                .await
                .map_err(|e| AdapterError::msg(e.to_string()))?;
        }

        let delim = options.csv_delimiter as u8;
        let reader = CsvBatchReader::with_delimiter(&local_path, options.batch_size, delim)
            .map_err(|e| AdapterError::msg(format!("failed to open CSV: {e}")))?;

        // If create_table is requested, infer column names from the header and
        // create a STRING-columns table. BigQuery inference is more involved
        // (would need a sample pass); strings are a safe, lossless fallback.
        if options.create_table {
            let columns = reader.column_names();
            if !columns.is_empty() {
                let col_defs: Vec<String> = columns
                    .iter()
                    .map(|c| format!("`{c}` STRING"))
                    .collect();
                let sql = format!(
                    "CREATE TABLE IF NOT EXISTS {target_ref} ({})",
                    col_defs.join(", ")
                );
                debug!(sql = %sql, "ensuring target table exists");
                self.adapter
                .execute_statement(&sql)
                .await
                .map_err(|e| AdapterError::msg(e.to_string()))?;
            }
        }

        let mut rows_loaded: u64 = 0;
        let dialect = self.adapter.dialect();

        for batch_res in reader {
            let batch =
                batch_res.map_err(|e| AdapterError::msg(format!("failed to read CSV batch: {e}")))?;
            let batch_row_count = batch.row_count as u64;
            let sql = generate_batch_insert_sql(&batch, &target_ref, dialect)
                .map_err(|e| AdapterError::msg(format!("failed to build INSERT SQL: {e}")))?;
            debug!(rows = batch_row_count, "inserting CSV batch");
            self.adapter
                .execute_statement(&sql)
                .await
                .map_err(|e| AdapterError::msg(e.to_string()))?;
            rows_loaded += batch_row_count;
        }

        info!(
            file = %local_path.display(),
            rows = rows_loaded,
            bytes = file_size,
            "bigquery load complete"
        );

        Ok(LoadResult {
            rows_loaded,
            bytes_read: file_size,
            duration_ms: start.elapsed().as_millis() as u64,
        })
    }

    fn supported_formats(&self) -> Vec<FileFormat> {
        // Parquet/JSONL need the Storage Write API; only CSV for now.
        vec![FileFormat::Csv]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_target_fully_qualified() {
        let t = TableRef {
            catalog: "proj".into(),
            schema: "ds".into(),
            table: "tbl".into(),
        };
        assert_eq!(format_target(&t), "`proj`.`ds`.`tbl`");
    }

    #[test]
    fn test_format_target_no_catalog() {
        let t = TableRef {
            catalog: String::new(),
            schema: "ds".into(),
            table: "tbl".into(),
        };
        assert_eq!(format_target(&t), "`ds`.`tbl`");
    }

    #[test]
    fn test_resolve_format_from_extension() {
        let src = LoadSource::LocalFile(std::path::PathBuf::from("/tmp/data.csv"));
        assert_eq!(
            resolve_format(&src, &LoadOptions::default()).unwrap(),
            FileFormat::Csv
        );
    }

    #[test]
    fn test_resolve_format_explicit_wins() {
        let src = LoadSource::LocalFile(std::path::PathBuf::from("/tmp/data.csv"));
        let opts = LoadOptions {
            format: Some(FileFormat::Parquet),
            ..Default::default()
        };
        assert_eq!(resolve_format(&src, &opts).unwrap(), FileFormat::Parquet);
    }
}
