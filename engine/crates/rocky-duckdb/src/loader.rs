//! DuckDB [`LoaderAdapter`] implementation.
//!
//! Leverages DuckDB's native bulk-load capabilities instead of row-by-row
//! INSERT:
//!
//! - **CSV:** `COPY <target> FROM '<path>' (HEADER, DELIMITER '<d>')`
//! - **Parquet:** `COPY <target> FROM '<path>' (FORMAT PARQUET)`
//! - **JSONL:** `CREATE TABLE <target> AS SELECT * FROM read_json_auto('<path>')`
//!
//! Cloud URIs (`s3://`, `gs://`) are passed through verbatim — DuckDB's httpfs
//! extension handles them natively when loaded at runtime.
//!
//! All DuckDB calls are synchronous. The adapter wraps them in
//! [`tokio::task::spawn_blocking`] to avoid blocking the async runtime.

use std::sync::{Arc, Mutex};
use std::time::Instant;

use async_trait::async_trait;
use tracing::debug;

use rocky_adapter_sdk::{
    AdapterError, AdapterResult, FileFormat, LoadOptions, LoadResult, LoadSource, LoaderAdapter,
    TableRef,
};

use crate::DuckDbConnector;

/// DuckDB loader adapter using native COPY and `read_json_auto` commands.
///
/// Shares the same `Arc<Mutex<DuckDbConnector>>` as
/// [`crate::adapter::DuckDbWarehouseAdapter`], so a single DuckDB database
/// can serve both query execution and file loading.
pub struct DuckDbLoaderAdapter {
    connector: Arc<Mutex<DuckDbConnector>>,
}

impl DuckDbLoaderAdapter {
    /// Create a loader from a shared connector.
    pub fn new(connector: Arc<Mutex<DuckDbConnector>>) -> Self {
        Self { connector }
    }

    /// Create a loader backed by a fresh in-memory DuckDB instance.
    pub fn in_memory() -> Result<Self, crate::DuckDbError> {
        let connector = DuckDbConnector::in_memory()?;
        Ok(Self {
            connector: Arc::new(Mutex::new(connector)),
        })
    }

    /// Returns a clone of the underlying connector handle for sharing.
    pub fn shared_connector(&self) -> Arc<Mutex<DuckDbConnector>> {
        Arc::clone(&self.connector)
    }
}

/// Resolve the file format, preferring the explicit option then falling back
/// to extension-based detection.
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

/// Build a two-part DuckDB table reference (`schema.table`), falling back to
/// just the table name when schema is empty.
fn format_target(target: &TableRef) -> String {
    if target.schema.is_empty() {
        target.table.clone()
    } else {
        format!("{}.{}", target.schema, target.table)
    }
}

/// Render a [`LoadSource`] as a SQL path string suitable for DuckDB COPY/read
/// functions. Local paths use `Path::display()`; cloud URIs pass through.
fn source_path_str(source: &LoadSource) -> String {
    match source {
        LoadSource::LocalFile(p) => p.display().to_string(),
        LoadSource::CloudUri(uri) => uri.clone(),
    }
}

/// Build the COPY/read SQL for loading a source into a target table.
///
/// The caller is responsible for handling `create_table` and `truncate_first`
/// options separately — this function only produces the load statement itself.
fn load_sql(
    source: &LoadSource,
    target_ref: &str,
    format: FileFormat,
    options: &LoadOptions,
) -> String {
    let path_str = source_path_str(source);
    match format {
        FileFormat::Csv => {
            let header = if options.csv_has_header {
                "HEADER"
            } else {
                "HEADER false"
            };
            let delim = options.csv_delimiter;
            format!("COPY {target_ref} FROM '{path_str}' (DELIMITER '{delim}', {header})")
        }
        FileFormat::Parquet => {
            format!("COPY {target_ref} FROM '{path_str}' (FORMAT PARQUET)")
        }
        FileFormat::JsonLines => {
            // COPY ... FROM does not support JSONL; use read_json_auto which
            // handles newline-delimited JSON natively.
            format!("INSERT INTO {target_ref} SELECT * FROM read_json_auto('{path_str}')")
        }
    }
}

/// Build a `CREATE TABLE ... AS SELECT` statement that infers the schema from
/// the source itself, used when `create_table` is true.
fn create_table_sql(source: &LoadSource, target_ref: &str, format: FileFormat) -> String {
    let path_str = source_path_str(source);
    match format {
        FileFormat::Csv => {
            format!("CREATE TABLE {target_ref} AS SELECT * FROM read_csv_auto('{path_str}')")
        }
        FileFormat::Parquet => {
            format!("CREATE TABLE {target_ref} AS SELECT * FROM read_parquet('{path_str}')")
        }
        FileFormat::JsonLines => {
            format!("CREATE TABLE {target_ref} AS SELECT * FROM read_json_auto('{path_str}')")
        }
    }
}

#[async_trait]
impl LoaderAdapter for DuckDbLoaderAdapter {
    async fn load(
        &self,
        source: &LoadSource,
        target: &TableRef,
        options: &LoadOptions,
    ) -> AdapterResult<LoadResult> {
        let format = resolve_format(source, options)?;
        let target_ref = format_target(target);
        // Only local files have filesystem metadata; cloud sources report 0.
        let file_size = match source {
            LoadSource::LocalFile(p) => std::fs::metadata(p).map(|m| m.len()).unwrap_or_default(),
            LoadSource::CloudUri(_) => 0,
        };

        let source = source.clone();
        let connector = Arc::clone(&self.connector);
        let create_table = options.create_table;
        let truncate_first = options.truncate_first;
        let options = options.clone();

        // DuckDB operations are synchronous; run on the blocking thread pool.
        tokio::task::spawn_blocking(move || {
            let start = Instant::now();

            let conn = connector
                .lock()
                .map_err(|e| AdapterError::msg(format!("mutex poisoned: {e}")))?;

            // Check whether the target table already exists.
            let table_exists = conn
                .execute_sql(&format!(
                    "SELECT 1 FROM information_schema.tables WHERE table_name = '{}'",
                    target_ref.split('.').next_back().unwrap_or(&target_ref)
                ))
                .map(|r| !r.rows.is_empty())
                .unwrap_or(false);

            if !table_exists && create_table {
                // Let DuckDB infer the schema from the source.
                let sql = create_table_sql(&source, &target_ref, format);
                debug!(sql = %sql, "creating table from source");
                conn.execute_statement(&sql).map_err(AdapterError::new)?;
            } else {
                // Table exists (or we were told not to create it).
                if truncate_first {
                    let sql = format!("DELETE FROM {target_ref}");
                    debug!(sql = %sql, "truncating target before load");
                    conn.execute_statement(&sql).map_err(AdapterError::new)?;
                }

                let sql = load_sql(&source, &target_ref, format, &options);
                debug!(sql = %sql, "loading source into existing table");
                conn.execute_statement(&sql).map_err(AdapterError::new)?;
            }

            // Count rows in the target to report how many were loaded.
            let count_result = conn
                .execute_sql(&format!("SELECT COUNT(*) AS cnt FROM {target_ref}"))
                .map_err(AdapterError::new)?;
            let rows_loaded: u64 = count_result
                .rows
                .first()
                .and_then(|r| r.first())
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);

            Ok(LoadResult {
                rows_loaded,
                bytes_read: file_size,
                duration_ms: start.elapsed().as_millis() as u64,
            })
        })
        .await
        .map_err(|e| AdapterError::msg(format!("spawn_blocking join error: {e}")))?
    }

    fn supported_formats(&self) -> Vec<FileFormat> {
        vec![FileFormat::Csv, FileFormat::Parquet, FileFormat::JsonLines]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::path::{Path, PathBuf};
    use tempfile::NamedTempFile;

    /// Helper: create an in-memory loader and its shared connector.
    fn loader() -> DuckDbLoaderAdapter {
        DuckDbLoaderAdapter::in_memory().expect("in-memory DuckDB should succeed")
    }

    fn csv_file(content: &str) -> NamedTempFile {
        let mut f = NamedTempFile::with_suffix(".csv").unwrap();
        f.write_all(content.as_bytes()).unwrap();
        f.flush().unwrap();
        f
    }

    fn target(table: &str) -> TableRef {
        TableRef {
            catalog: String::new(),
            schema: "main".into(),
            table: table.into(),
        }
    }

    fn local(path: &Path) -> LoadSource {
        LoadSource::LocalFile(path.to_path_buf())
    }

    // ------------------------------------------------------------------
    // CSV
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn test_load_csv_creates_table() {
        let adapter = loader();
        let file = csv_file("id,name\n1,Alice\n2,Bob\n3,Carol\n");

        let result = adapter
            .load(&local(file.path()), &target("people"), &LoadOptions::default())
            .await
            .unwrap();

        assert_eq!(result.rows_loaded, 3);
        assert!(result.bytes_read > 0);

        // Verify data is actually in the table.
        let conn = adapter.shared_connector();
        let guard = conn.lock().unwrap();
        let qr = guard
            .execute_sql("SELECT name FROM main.people ORDER BY id")
            .unwrap();
        assert_eq!(qr.rows.len(), 3);
        assert_eq!(qr.rows[0][0], "Alice");
    }

    #[tokio::test]
    async fn test_load_csv_into_existing_table() {
        let adapter = loader();
        {
            let conn = adapter.shared_connector();
            let guard = conn.lock().unwrap();
            guard
                .execute_statement("CREATE TABLE main.items (id INTEGER, val VARCHAR)")
                .unwrap();
            guard
                .execute_statement("INSERT INTO main.items VALUES (99, 'existing')")
                .unwrap();
        }

        let file = csv_file("id,val\n1,alpha\n2,beta\n");
        let opts = LoadOptions {
            create_table: false,
            ..Default::default()
        };
        let result = adapter
            .load(&local(file.path()), &target("items"), &opts)
            .await
            .unwrap();

        // 1 existing + 2 new = 3 total
        assert_eq!(result.rows_loaded, 3);
    }

    #[tokio::test]
    async fn test_load_csv_truncate_first() {
        let adapter = loader();
        {
            let conn = adapter.shared_connector();
            let guard = conn.lock().unwrap();
            guard
                .execute_statement("CREATE TABLE main.t (id INTEGER, name VARCHAR)")
                .unwrap();
            guard
                .execute_statement("INSERT INTO main.t VALUES (99, 'old')")
                .unwrap();
        }

        let file = csv_file("id,name\n1,new\n");
        let opts = LoadOptions {
            create_table: false,
            truncate_first: true,
            ..Default::default()
        };
        let result = adapter
            .load(&local(file.path()), &target("t"), &opts)
            .await
            .unwrap();

        // Truncated old row, loaded 1 new
        assert_eq!(result.rows_loaded, 1);
    }

    #[tokio::test]
    async fn test_load_csv_custom_delimiter() {
        let adapter = loader();
        let mut f = NamedTempFile::with_suffix(".csv").unwrap();
        f.write_all(b"id\tname\n1\tAlice\n2\tBob\n").unwrap();
        f.flush().unwrap();

        let opts = LoadOptions {
            csv_delimiter: '\t',
            ..Default::default()
        };
        let result = adapter
            .load(&local(f.path()), &target("tab_data"), &opts)
            .await
            .unwrap();

        assert_eq!(result.rows_loaded, 2);
    }

    // ------------------------------------------------------------------
    // JSONL
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn test_load_jsonl_creates_table() {
        let adapter = loader();
        let mut f = NamedTempFile::with_suffix(".jsonl").unwrap();
        f.write_all(b"{\"id\":1,\"color\":\"red\"}\n{\"id\":2,\"color\":\"blue\"}\n")
            .unwrap();
        f.flush().unwrap();

        let result = adapter
            .load(&local(f.path()), &target("colors"), &LoadOptions::default())
            .await
            .unwrap();

        assert_eq!(result.rows_loaded, 2);
    }

    // ------------------------------------------------------------------
    // Format detection
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn test_format_detection_unknown_extension() {
        let adapter = loader();
        let f = NamedTempFile::with_suffix(".xlsx").unwrap();
        let result = adapter
            .load(&local(f.path()), &target("x"), &LoadOptions::default())
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("cannot detect file format"));
    }

    #[tokio::test]
    async fn test_explicit_format_overrides_extension() {
        let adapter = loader();
        // Write CSV content but with a .txt extension.
        let mut f = NamedTempFile::with_suffix(".txt").unwrap();
        f.write_all(b"id,name\n1,Alice\n").unwrap();
        f.flush().unwrap();

        let opts = LoadOptions {
            format: Some(FileFormat::Csv),
            ..Default::default()
        };
        let result = adapter
            .load(&local(f.path()), &target("txt_as_csv"), &opts)
            .await
            .unwrap();

        assert_eq!(result.rows_loaded, 1);
    }

    // ------------------------------------------------------------------
    // supported_formats
    // ------------------------------------------------------------------

    #[test]
    fn test_supported_formats() {
        let adapter = loader();
        let fmts = adapter.supported_formats();
        assert!(fmts.contains(&FileFormat::Csv));
        assert!(fmts.contains(&FileFormat::Parquet));
        assert!(fmts.contains(&FileFormat::JsonLines));
    }

    // ------------------------------------------------------------------
    // Helpers (unit)
    // ------------------------------------------------------------------

    #[test]
    fn test_resolve_format_csv() {
        let src = LoadSource::LocalFile(PathBuf::from("/tmp/data.csv"));
        assert_eq!(
            resolve_format(&src, &LoadOptions::default()).unwrap(),
            FileFormat::Csv
        );
    }

    #[test]
    fn test_resolve_format_parquet() {
        let src = LoadSource::LocalFile(PathBuf::from("/tmp/data.parquet"));
        assert_eq!(
            resolve_format(&src, &LoadOptions::default()).unwrap(),
            FileFormat::Parquet
        );
    }

    #[test]
    fn test_resolve_format_explicit_wins() {
        let src = LoadSource::LocalFile(PathBuf::from("/tmp/data.csv"));
        let opts = LoadOptions {
            format: Some(FileFormat::JsonLines),
            ..Default::default()
        };
        assert_eq!(resolve_format(&src, &opts).unwrap(), FileFormat::JsonLines);
    }

    #[test]
    fn test_resolve_format_cloud_uri() {
        let src = LoadSource::CloudUri("s3://bucket/data.parquet".into());
        assert_eq!(
            resolve_format(&src, &LoadOptions::default()).unwrap(),
            FileFormat::Parquet
        );
    }

    #[test]
    fn test_format_target_with_schema() {
        let t = TableRef {
            catalog: String::new(),
            schema: "main".into(),
            table: "tbl".into(),
        };
        assert_eq!(format_target(&t), "main.tbl");
    }

    #[test]
    fn test_format_target_no_schema() {
        let t = TableRef {
            catalog: String::new(),
            schema: String::new(),
            table: "tbl".into(),
        };
        assert_eq!(format_target(&t), "tbl");
    }

    #[test]
    fn test_load_sql_csv() {
        let src = LoadSource::LocalFile(PathBuf::from("/tmp/data.csv"));
        let sql = load_sql(&src, "main.t", FileFormat::Csv, &LoadOptions::default());
        assert!(sql.contains("COPY main.t FROM '/tmp/data.csv'"));
        assert!(sql.contains("HEADER"));
        assert!(sql.contains("DELIMITER ','"));
    }

    #[test]
    fn test_load_sql_parquet() {
        let src = LoadSource::LocalFile(PathBuf::from("/tmp/data.parquet"));
        let sql = load_sql(
            &src,
            "main.t",
            FileFormat::Parquet,
            &LoadOptions::default(),
        );
        assert_eq!(sql, "COPY main.t FROM '/tmp/data.parquet' (FORMAT PARQUET)");
    }

    #[test]
    fn test_load_sql_jsonl() {
        let src = LoadSource::LocalFile(PathBuf::from("/tmp/data.jsonl"));
        let sql = load_sql(
            &src,
            "main.t",
            FileFormat::JsonLines,
            &LoadOptions::default(),
        );
        assert!(sql.contains("read_json_auto"));
        assert!(sql.contains("INSERT INTO main.t"));
    }

    #[test]
    fn test_load_sql_cloud_uri() {
        let src = LoadSource::CloudUri("s3://bucket/data.parquet".into());
        let sql = load_sql(
            &src,
            "main.t",
            FileFormat::Parquet,
            &LoadOptions::default(),
        );
        assert_eq!(
            sql,
            "COPY main.t FROM 's3://bucket/data.parquet' (FORMAT PARQUET)"
        );
    }

    #[test]
    fn test_create_table_sql_csv() {
        let src = LoadSource::LocalFile(PathBuf::from("/tmp/d.csv"));
        let sql = create_table_sql(&src, "main.t", FileFormat::Csv);
        assert!(sql.contains("CREATE TABLE main.t AS SELECT * FROM read_csv_auto"));
    }

    #[test]
    fn test_create_table_sql_parquet() {
        let src = LoadSource::LocalFile(PathBuf::from("/tmp/d.parquet"));
        let sql = create_table_sql(&src, "main.t", FileFormat::Parquet);
        assert!(sql.contains("read_parquet"));
    }

    #[test]
    fn test_create_table_sql_jsonl() {
        let src = LoadSource::LocalFile(PathBuf::from("/tmp/d.jsonl"));
        let sql = create_table_sql(&src, "main.t", FileFormat::JsonLines);
        assert!(sql.contains("read_json_auto"));
    }

    #[test]
    fn test_source_path_str_local() {
        let src = LoadSource::LocalFile(PathBuf::from("/tmp/data.csv"));
        assert_eq!(source_path_str(&src), "/tmp/data.csv");
    }

    #[test]
    fn test_source_path_str_cloud() {
        let src = LoadSource::CloudUri("s3://bucket/data.csv".into());
        assert_eq!(source_path_str(&src), "s3://bucket/data.csv");
    }
}
