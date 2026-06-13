//! BigQuery [`LoaderAdapter`] implementation.
//!
//! Two ingest paths, dispatched on the [`LoadSource`] variant:
//!
//! - **Cloud URIs (`gs://…`) → native BigQuery LOAD JOB.** Submits a
//!   `jobs.insert` load configuration via
//!   [`BigQueryAdapter::load_via_job`][crate::BigQueryAdapter::load_via_job],
//!   polls it to completion, and reads rows-loaded from the job
//!   statistics. This is the production-scale path and supports **CSV,
//!   Parquet, and newline-delimited JSON** — the load job reads the files
//!   directly from Google Cloud Storage, so there's no per-row REST tax.
//!   `writeDisposition`, `createDisposition`, schema autodetect, and the
//!   CSV header/delimiter options all map onto the load config.
//!
//! - **Local files → `INSERT INTO … VALUES` fallback (CSV only).**
//!   BigQuery load jobs read only from GCS, so a `LoadSource::LocalFile`
//!   can't feed one without first uploading to a bucket. Rather than take
//!   a hard dependency on a GCS client, local files keep the original
//!   batched-INSERT path (via the shared
//!   [`CsvBatchReader`][rocky_core::arrow_loader::CsvBatchReader]). It is
//!   ~3 orders of magnitude slower than a native load and appropriate for
//!   small reference datasets, dev/CI smoke tests, and POCs. Parquet /
//!   JSONL local files are rejected with a message pointing at the
//!   `gs://` path.
//!
//! # Future work
//!
//! A `jobs.insert` *multipart* upload can stream a local file into a load
//! job without a GCS round-trip; it's deferred (MIME-body complexity, and
//! the local path is dev-scale by definition). Local→GCS upload + load is
//! the other option, gated on adding a GCS client dependency.

use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use tracing::{debug, info};

use rocky_adapter_sdk::{
    AdapterError, AdapterResult, FileFormat, LoadOptions, LoadResult, LoadSource, LoaderAdapter,
    TableRef,
};
use rocky_core::arrow_loader::{
    CsvBatchReader, RowBatch, generate_batch_insert_sql, infer_column_types,
};
use rocky_core::traits::WarehouseAdapter;
use rocky_sql::validation::{validate_gcp_project_id, validate_identifier};

use crate::BigQueryAdapter;
use crate::connector::{
    BigQuerySourceFormat, LoadCreateDisposition, LoadJobSpec, WriteDisposition,
};

/// BigQuery loader adapter.
///
/// Cloud URIs use native LOAD JOBS; local files fall back to batched
/// `INSERT INTO … VALUES`. See the module docs.
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

/// Render a BigQuery target as `` `project`.`dataset`.`table` `` for the
/// SQL (INSERT-fallback) path. Mirrors how the connector formats refs.
///
/// Every segment is validated before interpolation — the dataset and table as
/// SQL identifiers, the project (catalog) as a GCP project id — so a malicious
/// target (e.g. a discovered filename stem containing a backtick) can never
/// break out of the identifier. Backtick-wrapping alone does not escape an
/// embedded backtick, so validation is the guard, matching the Databricks and
/// Snowflake loaders.
fn format_target(target: &TableRef) -> AdapterResult<String> {
    validate_identifier(&target.schema).map_err(AdapterError::new)?;
    validate_identifier(&target.table).map_err(AdapterError::new)?;
    if target.catalog.is_empty() {
        Ok(format!("`{}`.`{}`", target.schema, target.table))
    } else {
        validate_gcp_project_id(&target.catalog).map_err(AdapterError::new)?;
        Ok(format!(
            "`{}`.`{}`.`{}`",
            target.catalog, target.schema, target.table
        ))
    }
}

/// Map a generic inferred SQL type name (as produced by
/// [`infer_column_types`]) to its canonical BigQuery type name.
///
/// [`infer_column_types`] emits the DuckDB/generic family
/// (`BOOLEAN` / `BIGINT` / `DOUBLE` / `TIMESTAMP` / `STRING`); BigQuery's
/// canonical names are `BOOL` / `INT64` / `FLOAT64` / `TIMESTAMP` /
/// `STRING` (the names BQ itself logs — see the `FLOAT64` / `INT64`
/// rationale in `connector.rs`). Anything unrecognized falls back to
/// `STRING`, which is lossless for the CSV INSERT path.
fn bq_column_type(generic_type: &str) -> &'static str {
    match generic_type.to_ascii_uppercase().as_str() {
        "BOOLEAN" | "BOOL" => "BOOL",
        "BIGINT" | "INTEGER" | "INT" | "INT64" => "INT64",
        "DOUBLE" | "FLOAT" | "FLOAT64" => "FLOAT64",
        "TIMESTAMP" => "TIMESTAMP",
        _ => "STRING",
    }
}

/// Build a `CREATE TABLE IF NOT EXISTS {target_ref} (`col` TYPE, …)`
/// statement with column types inferred from the CSV batches.
///
/// The column list is driven by the CSV header (`column_names`) so a
/// header-only file (zero data rows, hence no inferred types) still
/// produces every column — those default to `STRING`. Returns `None`
/// when there are no columns at all.
fn build_create_table_sql(
    target_ref: &str,
    column_names: &[String],
    batches: &[RowBatch],
) -> AdapterResult<Option<String>> {
    if column_names.is_empty() {
        return Ok(None);
    }

    // Inferred types keyed by column name; absent columns (e.g. a
    // header-only CSV) fall back to STRING below.
    let inferred: std::collections::HashMap<String, String> = infer_column_types(batches)
        .into_iter()
        .map(|c| (c.name, c.data_type))
        .collect();

    // Validate every header-derived column name before backtick-wrapping it.
    // CSV headers are file content (the most plausibly untrusted input on this
    // path), and a backtick in a header would otherwise break out of the
    // identifier into arbitrary DDL — the Databricks and Snowflake CSV loaders
    // validate here too.
    let col_defs: Vec<String> = column_names
        .iter()
        .map(|name| {
            validate_identifier(name).map_err(AdapterError::new)?;
            let generic = inferred.get(name).map_or("STRING", String::as_str);
            Ok(format!("`{name}` {}", bq_column_type(generic)))
        })
        .collect::<AdapterResult<Vec<String>>>()?;

    Ok(Some(format!(
        "CREATE TABLE IF NOT EXISTS {target_ref} ({})",
        col_defs.join(", ")
    )))
}

/// Map the SDK [`FileFormat`] to the BigQuery load-job source format.
fn source_format(format: FileFormat) -> BigQuerySourceFormat {
    match format {
        FileFormat::Csv => BigQuerySourceFormat::Csv,
        FileFormat::Parquet => BigQuerySourceFormat::Parquet,
        FileFormat::JsonLines => BigQuerySourceFormat::NewlineDelimitedJson,
    }
}

/// Build a [`LoadJobSpec`] from the generic SDK options for the cloud path.
///
/// - `writeDisposition` = `WRITE_TRUNCATE` when `truncate_first`, else
///   `WRITE_APPEND`.
/// - `createDisposition` = `CREATE_IF_NEEDED` when `create_table`, else
///   `CREATE_NEVER`.
/// - CSV-only knobs (`skipLeadingRows`, `fieldDelimiter`) are threaded
///   from [`LoadOptions`] and left `None` for Parquet / JSONL.
/// - `autodetect` is on for CSV/JSONL (no explicit schema is available
///   here); harmless for Parquet, which is self-describing.
fn build_load_spec(
    uri: &str,
    target: &TableRef,
    format: FileFormat,
    options: &LoadOptions,
) -> LoadJobSpec {
    let write_disposition = if options.truncate_first {
        WriteDisposition::Truncate
    } else {
        WriteDisposition::Append
    };
    let create_disposition = if options.create_table {
        LoadCreateDisposition::IfNeeded
    } else {
        LoadCreateDisposition::Never
    };

    let (skip_leading_rows, field_delimiter) = if format == FileFormat::Csv {
        // BigQuery's `skipLeadingRows` counts header lines to skip; map
        // the boolean `csv_has_header` to 1/0. The delimiter is a single
        // character on the SDK side.
        let skip = u64::from(options.csv_has_header);
        (Some(skip), Some(options.csv_delimiter.to_string()))
    } else {
        (None, None)
    };

    LoadJobSpec {
        destination_project: target.catalog.clone(),
        destination_dataset: target.schema.clone(),
        destination_table: target.table.clone(),
        source_uris: vec![uri.to_string()],
        source_format: source_format(format),
        write_disposition,
        create_disposition,
        autodetect: true,
        skip_leading_rows,
        field_delimiter,
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
        let format = resolve_format(source, options)?;

        match source {
            // --- Native LOAD JOB path: gs:// → jobs.insert ---
            LoadSource::CloudUri(uri) => {
                if !uri.starts_with("gs://") {
                    return Err(AdapterError::msg(format!(
                        "BigQuery load jobs read only from Google Cloud Storage; \
                         got non-gs:// URI '{uri}'"
                    )));
                }
                let spec = build_load_spec(uri, target, format, options);
                debug!(
                    uri = %uri,
                    format = %format,
                    truncate = options.truncate_first,
                    "submitting BigQuery load job for cloud URI"
                );
                let outcome = self
                    .adapter
                    .load_via_job(&spec)
                    .await
                    .map_err(AdapterError::new)?;

                info!(
                    uri = %uri,
                    rows = outcome.rows_loaded,
                    bytes = outcome.input_file_bytes.unwrap_or_default(),
                    "bigquery load job complete"
                );
                Ok(LoadResult {
                    rows_loaded: outcome.rows_loaded,
                    bytes_read: outcome.input_file_bytes.unwrap_or_default(),
                    duration_ms: start.elapsed().as_millis() as u64,
                })
            }

            // --- INSERT fallback path: local file (CSV only) ---
            LoadSource::LocalFile(local_path) => {
                if format != FileFormat::Csv {
                    return Err(AdapterError::msg(format!(
                        "BigQuery local-file loading supports only CSV (got {format}); \
                         stage the file in Google Cloud Storage and load via a gs:// URI \
                         for Parquet/JSONL (native load jobs)"
                    )));
                }

                let target_ref = format_target(target)?;
                let file_size = std::fs::metadata(local_path)
                    .map(|m| m.len())
                    .unwrap_or_default();

                if options.truncate_first {
                    let sql = format!("TRUNCATE TABLE {target_ref}");
                    debug!(sql = %sql, "truncating target before load");
                    self.adapter
                        .execute_statement(&sql)
                        .await
                        .map_err(|e| AdapterError::msg(e.to_string()))?;
                }

                let delim = options.csv_delimiter as u8;
                let reader = CsvBatchReader::with_delimiter(local_path, options.batch_size, delim)
                    .map_err(|e| AdapterError::msg(format!("failed to open CSV: {e}")))?;

                // Read all batches up front with the caller's delimiter
                // (re-reading via the comma-only `read_csv_batches` would
                // misparse a TSV as one column). The whole file is buffered:
                // this INSERT path is dev-scale by design (see module docs),
                // and type inference needs the rows anyway. The same batches
                // then drive the INSERT loop, so the file is read exactly
                // once.
                let column_names = reader.column_names().to_vec();
                let batches: Vec<RowBatch> = reader
                    .collect::<Result<_, _>>()
                    .map_err(|e| AdapterError::msg(format!("failed to read CSV batch: {e}")))?;

                // If create_table is requested, infer column TYPES from the
                // sampled rows and create the table with those types (mapped
                // to BigQuery's canonical type names). All-STRING would defeat
                // the typed contract-gate (`rocky load` with a `[contract]`),
                // which a header-derived STRING column always fails against a
                // declared `id INT64`. Idempotent via IF NOT EXISTS — no
                // DESCRIBE / existence round-trip.
                if options.create_table
                    && let Some(sql) = build_create_table_sql(&target_ref, &column_names, &batches)?
                {
                    debug!(sql = %sql, "ensuring target table exists with inferred types");
                    self.adapter
                        .execute_statement(&sql)
                        .await
                        .map_err(|e| AdapterError::msg(e.to_string()))?;
                }

                let mut rows_loaded: u64 = 0;
                let dialect = self.adapter.dialect();

                for batch in &batches {
                    let batch_row_count = batch.row_count as u64;
                    let sql =
                        generate_batch_insert_sql(batch, &target_ref, dialect).map_err(|e| {
                            AdapterError::msg(format!("failed to build INSERT SQL: {e}"))
                        })?;
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
                    "bigquery local-file load complete (INSERT fallback)"
                );

                Ok(LoadResult {
                    rows_loaded,
                    bytes_read: file_size,
                    duration_ms: start.elapsed().as_millis() as u64,
                })
            }
        }
    }

    fn supported_formats(&self) -> Vec<FileFormat> {
        // Cloud-URI loads support all three via native load jobs. The
        // local-file INSERT fallback is CSV-only, enforced at `load` time
        // with an explicit error pointing at the gs:// path.
        vec![FileFormat::Csv, FileFormat::Parquet, FileFormat::JsonLines]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_target_fully_qualified() {
        let t = TableRef {
            catalog: "my-proj".into(),
            schema: "ds".into(),
            table: "tbl".into(),
        };
        assert_eq!(format_target(&t).unwrap(), "`my-proj`.`ds`.`tbl`");
    }

    #[test]
    fn test_format_target_no_catalog() {
        let t = TableRef {
            catalog: String::new(),
            schema: "ds".into(),
            table: "tbl".into(),
        };
        assert_eq!(format_target(&t).unwrap(), "`ds`.`tbl`");
    }

    #[test]
    fn format_target_rejects_backtick_injection() {
        // A discovered filename stem like `t`); DROP TABLE x; --` must not
        // break out of the backtick-quoted identifier.
        let t = TableRef {
            catalog: String::new(),
            schema: "ds".into(),
            table: "t`); DROP TABLE x; --".into(),
        };
        assert!(format_target(&t).is_err());
    }

    #[test]
    fn format_target_rejects_bad_project() {
        let t = TableRef {
            catalog: "proj`.`evil".into(),
            schema: "ds".into(),
            table: "tbl".into(),
        };
        assert!(format_target(&t).is_err());
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

    #[test]
    fn source_format_maps_all_three() {
        assert_eq!(source_format(FileFormat::Csv), BigQuerySourceFormat::Csv);
        assert_eq!(
            source_format(FileFormat::Parquet),
            BigQuerySourceFormat::Parquet
        );
        assert_eq!(
            source_format(FileFormat::JsonLines),
            BigQuerySourceFormat::NewlineDelimitedJson
        );
    }

    #[test]
    fn build_load_spec_truncate_maps_to_write_truncate() {
        let target = TableRef {
            catalog: "proj".into(),
            schema: "ds".into(),
            table: "tbl".into(),
        };
        let opts = LoadOptions {
            truncate_first: true,
            create_table: true,
            ..Default::default()
        };
        let spec = build_load_spec("gs://b/o.csv", &target, FileFormat::Csv, &opts);
        assert_eq!(spec.write_disposition, WriteDisposition::Truncate);
        assert_eq!(spec.create_disposition, LoadCreateDisposition::IfNeeded);
        assert_eq!(spec.source_uris, vec!["gs://b/o.csv".to_string()]);
        assert_eq!(spec.destination_project, "proj");
        assert_eq!(spec.destination_dataset, "ds");
        assert_eq!(spec.destination_table, "tbl");
        // CSV → header skip + delimiter present.
        assert_eq!(spec.skip_leading_rows, Some(1));
        assert_eq!(spec.field_delimiter.as_deref(), Some(","));
    }

    #[test]
    fn build_load_spec_append_and_create_never() {
        let target = TableRef {
            catalog: String::new(),
            schema: "ds".into(),
            table: "tbl".into(),
        };
        let opts = LoadOptions {
            truncate_first: false,
            create_table: false,
            ..Default::default()
        };
        let spec = build_load_spec("gs://b/o.parquet", &target, FileFormat::Parquet, &opts);
        assert_eq!(spec.write_disposition, WriteDisposition::Append);
        assert_eq!(spec.create_disposition, LoadCreateDisposition::Never);
        // Parquet → no CSV knobs.
        assert_eq!(spec.skip_leading_rows, None);
        assert_eq!(spec.field_delimiter, None);
        assert_eq!(spec.source_format, BigQuerySourceFormat::Parquet);
        // Empty catalog is preserved here; the connector backfills the
        // adapter's project when building the wire request.
        assert_eq!(spec.destination_project, "");
    }

    #[test]
    fn build_load_spec_no_header_skips_zero_rows() {
        let target = TableRef {
            catalog: "proj".into(),
            schema: "ds".into(),
            table: "tbl".into(),
        };
        let opts = LoadOptions {
            csv_has_header: false,
            csv_delimiter: '\t',
            ..Default::default()
        };
        let spec = build_load_spec("gs://b/o.csv", &target, FileFormat::Csv, &opts);
        assert_eq!(spec.skip_leading_rows, Some(0));
        assert_eq!(spec.field_delimiter.as_deref(), Some("\t"));
    }

    #[test]
    fn bq_column_type_maps_generic_to_canonical_bq_names() {
        // The generic family infer_column_types emits.
        assert_eq!(bq_column_type("BOOLEAN"), "BOOL");
        assert_eq!(bq_column_type("BIGINT"), "INT64");
        assert_eq!(bq_column_type("DOUBLE"), "FLOAT64");
        assert_eq!(bq_column_type("TIMESTAMP"), "TIMESTAMP");
        assert_eq!(bq_column_type("STRING"), "STRING");
        // Case-insensitive + canonical-name passthrough.
        assert_eq!(bq_column_type("bigint"), "INT64");
        assert_eq!(bq_column_type("INT64"), "INT64");
        assert_eq!(bq_column_type("FLOAT64"), "FLOAT64");
        // Unknown → STRING (lossless fallback for the INSERT path).
        assert_eq!(bq_column_type("DECIMAL(10,2)"), "STRING");
        assert_eq!(bq_column_type(""), "STRING");
    }

    #[test]
    fn build_create_table_sql_uses_inferred_types_not_all_string() {
        let batches = vec![RowBatch {
            column_names: vec![
                "id".into(),
                "amount".into(),
                "name".into(),
                "active".into(),
                "created_at".into(),
            ],
            rows: vec![
                vec![
                    "1".into(),
                    "9.99".into(),
                    "alice".into(),
                    "true".into(),
                    "2024-01-15".into(),
                ],
                vec![
                    "2".into(),
                    "8.50".into(),
                    "bob".into(),
                    "false".into(),
                    "2024-02-20".into(),
                ],
            ],
            row_count: 2,
        }];
        let cols = batches[0].column_names.clone();
        let sql = build_create_table_sql("`p`.`d`.`t`", &cols, &batches)
            .unwrap()
            .unwrap();
        assert_eq!(
            sql,
            "CREATE TABLE IF NOT EXISTS `p`.`d`.`t` \
             (`id` INT64, `amount` FLOAT64, `name` STRING, `active` BOOL, `created_at` TIMESTAMP)"
        );
        // The whole point: id is NOT all-STRING.
        assert!(sql.contains("`id` INT64"));
        assert!(!sql.contains("`id` STRING"));
        // A date-looking column infers to TIMESTAMP (kept TIMESTAMP, not
        // downgraded to STRING — STRING would re-fail a `created_at TIMESTAMP`
        // contract). The INSERT path feeds rocky-core's `format_cell` quoted
        // string literal `'2024-01-15'` into this column, relying on BigQuery
        // coercing a string literal to TIMESTAMP — exercised by the live
        // `load_local_csv_creates_inferred_types` test.
        assert!(sql.contains("`created_at` TIMESTAMP"));
    }

    #[test]
    fn build_create_table_sql_header_only_defaults_to_string() {
        // A CSV with a header but no data rows: infer_column_types returns
        // nothing, so every column falls back to STRING — but all columns
        // are still present (no regression from the old header-derived path).
        let cols = vec!["id".to_string(), "name".to_string()];
        let sql = build_create_table_sql("`p`.`d`.`t`", &cols, &[])
            .unwrap()
            .unwrap();
        assert_eq!(
            sql,
            "CREATE TABLE IF NOT EXISTS `p`.`d`.`t` (`id` STRING, `name` STRING)"
        );
    }

    #[test]
    fn build_create_table_sql_no_columns_is_none() {
        assert!(
            build_create_table_sql("`p`.`d`.`t`", &[], &[])
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn build_create_table_sql_rejects_backtick_header() {
        // A CSV header column containing a backtick would otherwise break out
        // of the `col` identifier into arbitrary DDL.
        let cols = vec!["id".to_string(), "x` STRING); DROP TABLE y; --".to_string()];
        assert!(build_create_table_sql("`p`.`d`.`t`", &cols, &[]).is_err());
    }

    #[test]
    fn build_load_spec_jsonl_has_no_csv_knobs() {
        let target = TableRef {
            catalog: "proj".into(),
            schema: "ds".into(),
            table: "tbl".into(),
        };
        let spec = build_load_spec(
            "gs://b/o.jsonl",
            &target,
            FileFormat::JsonLines,
            &LoadOptions::default(),
        );
        assert_eq!(
            spec.source_format,
            BigQuerySourceFormat::NewlineDelimitedJson
        );
        assert_eq!(spec.skip_leading_rows, None);
        assert_eq!(spec.field_delimiter, None);
    }
}
