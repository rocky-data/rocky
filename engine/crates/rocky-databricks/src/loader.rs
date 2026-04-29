//! Databricks [`LoaderAdapter`] implementation — COPY INTO.
//!
//! Databricks' SQL bulk-load primitive is:
//!
//! ```sql
//! COPY INTO catalog.schema.table
//! FROM 's3://bucket/path'
//! FILEFORMAT = CSV
//! FORMAT_OPTIONS ('header' = 'true', 'delimiter' = ',')
//! COPY_OPTIONS ('mergeSchema' = 'true');
//! ```
//!
//! The source file must already live in cloud storage (S3, ADLS, etc.).
//! Databricks reads the URI directly — Rocky does **not** upload files.
//! [`LoadSource::LocalFile`] is rejected with a clear error message.

use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use tracing::{debug, info, warn};

use rocky_adapter_sdk::{
    AdapterError, AdapterResult, FileFormat, LoadOptions, LoadResult, LoadSource, LoaderAdapter,
    TableRef,
};
use rocky_sql::validation::validate_identifier;

use crate::connector::{DatabricksConnector, QueryResult};

/// Databricks loader adapter using COPY INTO.
pub struct DatabricksLoaderAdapter {
    connector: Arc<DatabricksConnector>,
}

impl DatabricksLoaderAdapter {
    /// Create a loader wrapping the given `DatabricksConnector`.
    pub fn new(connector: Arc<DatabricksConnector>) -> Self {
        Self { connector }
    }
}

impl std::fmt::Debug for DatabricksLoaderAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatabricksLoaderAdapter").finish()
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

/// Render a Databricks target as `catalog.schema.table` after validating each
/// identifier. Rejects anything that isn't `[A-Za-z0-9_]+` so the result is
/// always safe to interpolate into SQL.
fn format_target(target: &TableRef) -> AdapterResult<String> {
    validate_identifier(&target.schema).map_err(AdapterError::new)?;
    validate_identifier(&target.table).map_err(AdapterError::new)?;
    if target.catalog.is_empty() {
        Ok(format!("{}.{}", target.schema, target.table))
    } else {
        validate_identifier(&target.catalog).map_err(AdapterError::new)?;
        Ok(format!(
            "{}.{}.{}",
            target.catalog, target.schema, target.table
        ))
    }
}

/// Render the FILEFORMAT clause for a given [`FileFormat`].
fn file_format_clause(format: FileFormat) -> &'static str {
    match format {
        FileFormat::Csv => "CSV",
        FileFormat::Parquet => "PARQUET",
        FileFormat::JsonLines => "JSON",
    }
}

/// Render the FORMAT_OPTIONS clause (only CSV has tunables Databricks cares about).
fn format_options_clause(
    format: FileFormat,
    options: &LoadOptions,
) -> AdapterResult<Option<String>> {
    match format {
        FileFormat::Csv => {
            let header = if options.csv_has_header {
                "true"
            } else {
                "false"
            };
            let delim = validate_csv_delimiter(options.csv_delimiter)?;
            Ok(Some(format!(
                "FORMAT_OPTIONS ('header' = '{header}', 'delimiter' = '{delim}')"
            )))
        }
        FileFormat::Parquet | FileFormat::JsonLines => Ok(None),
    }
}

/// Reject CSV delimiters that would break out of the `'...'` SQL string literal.
/// Allows printable ASCII plus tab; rejects quotes, backslash, and newlines.
fn validate_csv_delimiter(c: char) -> AdapterResult<char> {
    let ok = c == '\t' || (c.is_ascii() && !c.is_ascii_control() && c != '\'' && c != '\\');
    if ok {
        Ok(c)
    } else {
        Err(AdapterError::msg(format!(
            "invalid CSV delimiter {c:?}: must be a printable ASCII char (or tab), not a quote, backslash, or control character"
        )))
    }
}

/// Reject cloud URIs that would break out of the `'<uri>'` SQL string literal.
/// Rocky never escapes embedded quotes — instead we refuse to build the SQL.
fn validate_cloud_uri(uri: &str) -> AdapterResult<&str> {
    if uri.is_empty() {
        return Err(AdapterError::msg("cloud URI cannot be empty"));
    }
    if uri.contains(['\'', '\n', '\r', '\\']) {
        return Err(AdapterError::msg(format!(
            "invalid cloud URI {uri:?}: must not contain quotes, backslashes, or newlines"
        )));
    }
    Ok(uri)
}

/// Extract the `num_affected_rows` count from a COPY INTO response.
///
/// Databricks' COPY INTO returns a single-row result set whose schema
/// exposes per-operation counters (`num_affected_rows`, `num_inserted_rows`,
/// `num_skipped_corrupt_files`, …). We read `num_affected_rows` since it
/// represents the rows written by this COPY INTO call (matches COPY INTO
/// semantics for append-only + merge-schema loads).
///
/// Returns `None` when the column is missing, the result is empty, or the
/// value isn't a non-negative integer — the caller should default to 0 in
/// that case rather than fail the load.
fn extract_num_affected_rows(result: &QueryResult) -> Option<u64> {
    let idx = result
        .columns
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case("num_affected_rows"))?;
    let first_row = result.rows.first()?;
    let cell = first_row.get(idx)?;
    match cell {
        serde_json::Value::Number(n) => n
            .as_u64()
            .or_else(|| n.as_i64().and_then(|i| u64::try_from(i).ok())),
        serde_json::Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

/// Build the full `COPY INTO ... FROM '<uri>'` SQL statement.
///
/// The caller must pass a validated `target_ref` (use [`format_target`]) and a
/// validated `uri` (use [`validate_cloud_uri`]) so the literal-context
/// interpolation can't be broken out of.
fn build_copy_into_sql(
    uri: &str,
    target_ref: &str,
    format: FileFormat,
    options: &LoadOptions,
) -> AdapterResult<String> {
    let fileformat = file_format_clause(format);
    let format_opts = format_options_clause(format, options)?
        .map(|s| format!(" {s}"))
        .unwrap_or_default();
    // mergeSchema lets Databricks evolve the target schema when new columns
    // appear — matches DuckDB's create_table-from-file semantics.
    let copy_opts = if options.create_table {
        " COPY_OPTIONS ('mergeSchema' = 'true')"
    } else {
        ""
    };
    Ok(format!(
        "COPY INTO {target_ref} FROM '{uri}' FILEFORMAT = {fileformat}{format_opts}{copy_opts}"
    ))
}

#[async_trait]
impl LoaderAdapter for DatabricksLoaderAdapter {
    async fn load(
        &self,
        source: &LoadSource,
        target: &TableRef,
        options: &LoadOptions,
    ) -> AdapterResult<LoadResult> {
        let start = Instant::now();

        let uri = match source {
            LoadSource::CloudUri(uri) => uri.clone(),
            LoadSource::LocalFile(p) => {
                return Err(AdapterError::msg(format!(
                    "Databricks requires files in cloud storage (S3/ADLS/GCS); \
                     got local path '{}'. Upload the file first, then point \
                     `source_dir` at the cloud URI (e.g. s3://bucket/prefix/).",
                    p.display()
                )));
            }
        };

        let format = resolve_format(source, options)?;
        let target_ref = format_target(target)?;
        let uri = validate_cloud_uri(&uri)?.to_string();

        // `truncate_first` maps to a DELETE prior to COPY INTO. Databricks
        // supports DELETE FROM on managed tables.
        if options.truncate_first {
            let sql = format!("DELETE FROM {target_ref}");
            debug!(sql = %sql, "truncating target before COPY INTO");
            self.connector
                .execute_statement(&sql)
                .await
                .map_err(|e| AdapterError::msg(e.to_string()))?;
        }

        let sql = build_copy_into_sql(&uri, &target_ref, format, options)?;
        info!(sql = %sql, "databricks COPY INTO");
        let copy_result = self
            .connector
            .execute_sql(&sql)
            .await
            .map_err(|e| AdapterError::msg(e.to_string()))?;

        // Databricks' COPY INTO returns a single-row result set whose
        // schema includes `num_affected_rows` (the count of rows written
        // into the target table for this call). Parse it from the result;
        // if the column isn't present we fall back to 0 rather than fail,
        // so older/non-standard API shapes still return a usable load.
        let rows_loaded = match extract_num_affected_rows(&copy_result) {
            Some(n) => n,
            None => {
                warn!(
                    statement_id = %copy_result.statement_id,
                    "databricks COPY INTO response missing `num_affected_rows` \
                     column; reporting 0 rows loaded"
                );
                0
            }
        };

        Ok(LoadResult {
            rows_loaded,
            bytes_read: 0, // Cloud-URI sources: no local byte count.
            duration_ms: start.elapsed().as_millis() as u64,
        })
    }

    fn supported_formats(&self) -> Vec<FileFormat> {
        vec![FileFormat::Csv, FileFormat::Parquet, FileFormat::JsonLines]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_format_clause() {
        assert_eq!(file_format_clause(FileFormat::Csv), "CSV");
        assert_eq!(file_format_clause(FileFormat::Parquet), "PARQUET");
        assert_eq!(file_format_clause(FileFormat::JsonLines), "JSON");
    }

    #[test]
    fn test_format_options_csv_default() {
        let opts = LoadOptions::default();
        let clause = format_options_clause(FileFormat::Csv, &opts)
            .unwrap()
            .unwrap();
        assert!(clause.contains("'header' = 'true'"));
        assert!(clause.contains("'delimiter' = ','"));
    }

    #[test]
    fn test_format_options_csv_custom_delimiter() {
        let opts = LoadOptions {
            csv_delimiter: '\t',
            csv_has_header: false,
            ..Default::default()
        };
        let clause = format_options_clause(FileFormat::Csv, &opts)
            .unwrap()
            .unwrap();
        assert!(clause.contains("'header' = 'false'"));
        assert!(clause.contains("'delimiter' = '\t'"));
    }

    #[test]
    fn test_format_options_parquet_none() {
        assert!(
            format_options_clause(FileFormat::Parquet, &LoadOptions::default())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn test_format_options_csv_rejects_quote_delimiter() {
        let opts = LoadOptions {
            csv_delimiter: '\'',
            ..Default::default()
        };
        assert!(format_options_clause(FileFormat::Csv, &opts).is_err());
    }

    #[test]
    fn test_format_target_fully_qualified() {
        let t = TableRef {
            catalog: "main".into(),
            schema: "raw".into(),
            table: "orders".into(),
        };
        assert_eq!(format_target(&t).unwrap(), "main.raw.orders");
    }

    #[test]
    fn test_format_target_no_catalog() {
        let t = TableRef {
            catalog: String::new(),
            schema: "raw".into(),
            table: "orders".into(),
        };
        assert_eq!(format_target(&t).unwrap(), "raw.orders");
    }

    #[test]
    fn test_format_target_rejects_injection() {
        let t = TableRef {
            catalog: "main; DROP TABLE x".into(),
            schema: "raw".into(),
            table: "orders".into(),
        };
        assert!(format_target(&t).is_err());
    }

    #[test]
    fn test_validate_cloud_uri_rejects_quote() {
        assert!(validate_cloud_uri("s3://bucket/key").is_ok());
        assert!(validate_cloud_uri("s3://bucket/key'; DROP --").is_err());
        assert!(validate_cloud_uri("s3://bucket/\nfoo").is_err());
        assert!(validate_cloud_uri("").is_err());
    }

    #[test]
    fn test_build_copy_into_csv() {
        let opts = LoadOptions::default();
        let sql = build_copy_into_sql(
            "s3://bucket/path/file.csv",
            "main.raw.orders",
            FileFormat::Csv,
            &opts,
        )
        .unwrap();
        assert!(sql.starts_with("COPY INTO main.raw.orders"));
        assert!(sql.contains("FROM 's3://bucket/path/file.csv'"));
        assert!(sql.contains("FILEFORMAT = CSV"));
        assert!(sql.contains("'header' = 'true'"));
        assert!(sql.contains("mergeSchema"));
    }

    #[test]
    fn test_build_copy_into_parquet() {
        let opts = LoadOptions {
            create_table: false,
            ..Default::default()
        };
        let sql = build_copy_into_sql(
            "s3://bucket/data.parquet",
            "main.raw.orders",
            FileFormat::Parquet,
            &opts,
        )
        .unwrap();
        assert!(sql.contains("FILEFORMAT = PARQUET"));
        assert!(!sql.contains("mergeSchema"));
        assert!(!sql.contains("FORMAT_OPTIONS"));
    }

    #[test]
    fn test_build_copy_into_json() {
        let opts = LoadOptions::default();
        let sql = build_copy_into_sql(
            "abfss://container@acct/data.jsonl",
            "main.raw.orders",
            FileFormat::JsonLines,
            &opts,
        )
        .unwrap();
        assert!(sql.contains("FILEFORMAT = JSON"));
        assert!(sql.contains("abfss://container@acct/data.jsonl"));
    }

    #[test]
    fn test_resolve_format_cloud_uri() {
        let src = LoadSource::CloudUri("s3://bucket/data.parquet".into());
        assert_eq!(
            resolve_format(&src, &LoadOptions::default()).unwrap(),
            FileFormat::Parquet
        );
    }

    fn copy_into_result(column: &str, value: serde_json::Value) -> QueryResult {
        use crate::connector::ColumnSchema;
        QueryResult {
            statement_id: "stmt-copy".into(),
            columns: vec![ColumnSchema {
                name: column.into(),
                type_name: "LONG".into(),
                position: 0,
            }],
            rows: vec![vec![value]],
            total_row_count: Some(1),
        }
    }

    #[test]
    fn test_extract_num_affected_rows_number() {
        let qr = copy_into_result("num_affected_rows", serde_json::json!(1234));
        assert_eq!(extract_num_affected_rows(&qr), Some(1234));
    }

    #[test]
    fn test_extract_num_affected_rows_string() {
        // Databricks sometimes returns numeric columns as JSON strings in
        // JSON_ARRAY disposition (SQL LONGs don't fit in JSON numbers).
        let qr = copy_into_result("num_affected_rows", serde_json::json!("4242"));
        assert_eq!(extract_num_affected_rows(&qr), Some(4242));
    }

    #[test]
    fn test_extract_num_affected_rows_case_insensitive() {
        let qr = copy_into_result("NUM_AFFECTED_ROWS", serde_json::json!(7));
        assert_eq!(extract_num_affected_rows(&qr), Some(7));
    }

    #[test]
    fn test_extract_num_affected_rows_missing_column() {
        let qr = copy_into_result("num_inserted_rows", serde_json::json!(5));
        assert_eq!(extract_num_affected_rows(&qr), None);
    }

    #[test]
    fn test_extract_num_affected_rows_empty_rows() {
        let mut qr = copy_into_result("num_affected_rows", serde_json::json!(9));
        qr.rows.clear();
        assert_eq!(extract_num_affected_rows(&qr), None);
    }
}
