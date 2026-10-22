//! DuckDB [`LoaderAdapter`] implementation.
//!
//! Leverages DuckDB's native bulk-load capabilities instead of row-by-row
//! INSERT:
//!
//! - **CSV with a header:** `INSERT INTO <target> BY NAME SELECT * FROM read_csv(...)`
//! - **CSV without a header:** `COPY <target> FROM '<path>' (HEADER false, DELIMITER '<d>')`
//! - **Parquet:** `INSERT INTO <target> BY NAME SELECT * FROM read_parquet('<path>')`
//! - **JSONL:** `INSERT INTO <target> BY NAME SELECT * FROM read_json_auto('<path>')`
//!
//! Into an existing table, every format that carries column names binds by
//! name, not by position (#2466). A plain `COPY` or `INSERT ... SELECT *`
//! maps by position, so a file whose columns are in a different order than
//! the table puts same-typed values in the wrong columns. A table column the
//! file lacks gets its default, or `NULL`. A file column the table lacks is
//! an error that names the column. A CSV without a header has no names, so it
//! stays positional.
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
use rocky_sql::validation::validate_identifier;

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
///
/// Both identifiers are validated before interpolation. The target table is
/// derived from `--table`, config, or a discovered filename stem, so a file
/// named e.g. `evil; DROP TABLE x; --.csv` must not be able to drive arbitrary
/// SQL. Matches the validation the Databricks and Snowflake loaders apply.
fn format_target(target: &TableRef) -> AdapterResult<String> {
    validate_identifier(&target.table).map_err(AdapterError::new)?;
    if target.schema.is_empty() {
        Ok(target.table.clone())
    } else {
        validate_identifier(&target.schema).map_err(AdapterError::new)?;
        Ok(format!("{}.{}", target.schema, target.table))
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

/// Reject a `FROM '<path>'` literal that would break out of the surrounding
/// `'...'` SQL string literal. Used for both local paths and cloud URIs.
/// Rocky never escapes embedded quotes — instead we refuse to build the SQL,
/// matching the Databricks and Snowflake loaders.
fn validate_sql_path_literal(path: &str) -> AdapterResult<&str> {
    if path.is_empty() {
        return Err(AdapterError::msg("load source path cannot be empty"));
    }
    if path.contains(['\'', '\n', '\r', '\\']) {
        return Err(AdapterError::msg(format!(
            "invalid load source path {path:?}: must not contain quotes, backslashes, or newlines"
        )));
    }
    Ok(path)
}

/// Reject CSV delimiters that would break out of the `'...'` SQL string
/// literal. Allows printable ASCII plus tab; rejects quotes, backslash, and
/// control characters. Matches the Databricks and Snowflake loaders.
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

/// Build the COPY/read SQL for loading a source into a target table.
///
/// The caller is responsible for handling `create_table` and `truncate_first`
/// options separately — this function only produces the load statement itself.
fn load_sql(
    source: &LoadSource,
    target_ref: &str,
    format: FileFormat,
    options: &LoadOptions,
) -> AdapterResult<String> {
    let path_str = source_path_str(source);
    let path_str = validate_sql_path_literal(&path_str)?;
    match format {
        FileFormat::Csv => {
            let header = if options.csv_has_header {
                "HEADER"
            } else {
                "HEADER false"
            };
            let delim = validate_csv_delimiter(options.csv_delimiter)?;
            if options.csv_has_header {
                // Header names exist, so bind by name. `all_varchar` keeps
                // DuckDB from guessing types (a code like `007` must not turn
                // into `7`); the insert casts each value to the target
                // column's type, as COPY did.
                Ok(format!(
                    "INSERT INTO {target_ref} BY NAME SELECT * FROM \
                     read_csv('{path_str}', header = true, delim = '{delim}', all_varchar = true)"
                ))
            } else {
                // No header means no column names to bind by. This load stays
                // positional: file column N goes to table column N.
                Ok(format!(
                    "COPY {target_ref} FROM '{path_str}' (DELIMITER '{delim}', {header})"
                ))
            }
        }
        FileFormat::Parquet => Ok(format!(
            "INSERT INTO {target_ref} BY NAME SELECT * FROM read_parquet('{path_str}')"
        )),
        FileFormat::JsonLines => {
            // COPY ... FROM does not support JSONL; use read_json_auto which
            // handles newline-delimited JSON natively. BY NAME: key order in
            // JSONL is free, so a positional insert swaps same-typed columns.
            Ok(format!(
                "INSERT INTO {target_ref} BY NAME SELECT * FROM read_json_auto('{path_str}')"
            ))
        }
    }
}

/// Build a `CREATE TABLE ... AS SELECT` statement that infers the schema from
/// the source itself, used when `create_table` is true.
fn create_table_sql(
    source: &LoadSource,
    target_ref: &str,
    format: FileFormat,
) -> AdapterResult<String> {
    let path_str = source_path_str(source);
    let path_str = validate_sql_path_literal(&path_str)?;
    match format {
        FileFormat::Csv => Ok(format!(
            "CREATE TABLE {target_ref} AS SELECT * FROM read_csv_auto('{path_str}')"
        )),
        FileFormat::Parquet => Ok(format!(
            "CREATE TABLE {target_ref} AS SELECT * FROM read_parquet('{path_str}')"
        )),
        FileFormat::JsonLines => Ok(format!(
            "CREATE TABLE {target_ref} AS SELECT * FROM read_json_auto('{path_str}')"
        )),
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
        let target_ref = format_target(target)?;
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
                let sql = create_table_sql(&source, &target_ref, format)?;
                debug!(sql = %sql, "creating table from source");
                conn.execute_statement(&sql).map_err(AdapterError::new)?;
            } else {
                // Table exists (or we were told not to create it).
                //
                // Truncate + load is ONE transaction. As two autocommitted
                // statements, anything that stopped the process between them
                // left the target emptied and unloaded: a crash, a SIGKILL, or
                // `rocky`'s bounded runtime shutdown, which abandons a blocking
                // statement after `RUNTIME_SHUTDOWN_GRACE` (#1590). Uncommitted
                // work is rolled back on the next open, so the target survives
                // intact instead.
                //
                // The `create_table` branch above needs no wrapper: `CREATE
                // TABLE ... AS SELECT` is already a single statement.
                let load = load_sql(&source, &target_ref, format, &options)?;
                let sql = if truncate_first {
                    format!("BEGIN TRANSACTION; DELETE FROM {target_ref}; {load}; COMMIT")
                } else {
                    load
                };
                debug!(sql = %sql, "loading source into existing table");
                if let Err(e) = conn.execute_statement(&sql) {
                    // A DuckDB transaction that hit an error stays open and
                    // poisons every later statement on this connection until it
                    // is rolled back — including the row count below and every
                    // subsequent load through the same shared connector.
                    if truncate_first {
                        let _ = conn.execute_statement("ROLLBACK");
                    }
                    return Err(AdapterError::new(e));
                }
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
            .load(
                &local(file.path()),
                &target("people"),
                &LoadOptions::default(),
            )
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

    /// Truncate + load is one transaction, so a failed load leaves the target
    /// as it was rather than emptied.
    ///
    /// The two used to be separate autocommitted statements. That made the
    /// window between them a data-loss window for anything that stopped the
    /// process there — a crash, a SIGKILL, or `rocky`'s bounded runtime
    /// shutdown, which abandons a blocking statement after
    /// `RUNTIME_SHUTDOWN_GRACE` (#1590). A failed load is the in-process way to
    /// exhibit the same window: without the transaction the `DELETE` has
    /// already committed by the time the load fails.
    #[tokio::test]
    async fn test_load_csv_truncate_rolls_back_when_the_load_fails() {
        let adapter = loader();
        {
            let conn = adapter.shared_connector();
            let guard = conn.lock().unwrap();
            guard
                .execute_statement(
                    "CREATE TABLE main.keep (id INTEGER, name VARCHAR); \
                     INSERT INTO main.keep VALUES (1, 'Alice'), (2, 'Bob')",
                )
                .unwrap();
        }

        // Three columns into a two-column table: the COPY fails, the DELETE
        // that preceded it must not survive.
        let file = csv_file("id,name,extra\n9,Zoe,x\n");
        let err = adapter
            .load(
                &local(file.path()),
                &target("keep"),
                &LoadOptions {
                    create_table: false,
                    truncate_first: true,
                    ..LoadOptions::default()
                },
            )
            .await;
        assert!(err.is_err(), "the load was expected to fail; got {err:?}");

        let conn = adapter.shared_connector();
        let guard = conn.lock().unwrap();
        let qr = guard
            .execute_sql("SELECT name FROM main.keep ORDER BY id")
            .unwrap();
        assert_eq!(
            qr.rows.len(),
            2,
            "a failed load must leave the target's rows in place, not truncated"
        );

        // The connection is still usable — an unrolled-back aborted
        // transaction would poison every later statement on it.
        guard
            .execute_sql("SELECT 1")
            .expect("the shared connection must survive a failed load");
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
    // Column binding into an existing table (#2466)
    // ------------------------------------------------------------------

    fn no_create() -> LoadOptions {
        LoadOptions {
            create_table: false,
            ..Default::default()
        }
    }

    fn exec(adapter: &DuckDbLoaderAdapter, sql: &str) {
        let conn = adapter.shared_connector();
        let guard = conn.lock().unwrap();
        guard.execute_statement(sql).unwrap();
    }

    /// Result rows as strings. SQL NULL shows as `null`.
    fn rows(adapter: &DuckDbLoaderAdapter, sql: &str) -> Vec<Vec<String>> {
        let conn = adapter.shared_connector();
        let guard = conn.lock().unwrap();
        let qr = guard.execute_sql(sql).unwrap();
        qr.rows
            .iter()
            .map(|r| {
                r.iter()
                    .map(|v| {
                        v.as_str()
                            .map(str::to_owned)
                            .unwrap_or_else(|| v.to_string())
                    })
                    .collect()
            })
            .collect()
    }

    fn jsonl_file(content: &str) -> NamedTempFile {
        let mut f = NamedTempFile::with_suffix(".jsonl").unwrap();
        f.write_all(content.as_bytes()).unwrap();
        f.flush().unwrap();
        f
    }

    #[tokio::test]
    async fn jsonl_into_existing_table_binds_by_name() {
        let adapter = loader();
        exec(&adapter, "CREATE TABLE main.pair (k VARCHAR, v VARCHAR)");
        // Keys come in the opposite order to the table's columns.
        let f =
            jsonl_file("{\"v\":\"value1\",\"k\":\"key1\"}\n{\"v\":\"value2\",\"k\":\"key2\"}\n");
        adapter
            .load(&local(f.path()), &target("pair"), &no_create())
            .await
            .unwrap();
        assert_eq!(
            rows(&adapter, "SELECT k, v FROM main.pair ORDER BY k"),
            vec![vec!["key1", "value1"], vec!["key2", "value2"]]
        );
    }

    #[tokio::test]
    async fn jsonl_missing_target_column_gets_default_or_null() {
        let adapter = loader();
        exec(
            &adapter,
            "CREATE TABLE main.defs (a VARCHAR, b VARCHAR, c INTEGER DEFAULT 5)",
        );
        let f = jsonl_file("{\"a\":\"A1\"}\n");
        adapter
            .load(&local(f.path()), &target("defs"), &no_create())
            .await
            .unwrap();
        assert_eq!(
            rows(&adapter, "SELECT a, b, c FROM main.defs"),
            vec![vec!["A1", "null", "5"]]
        );
    }

    #[tokio::test]
    async fn jsonl_extra_file_column_is_an_error_naming_the_column() {
        let adapter = loader();
        exec(&adapter, "CREATE TABLE main.narrow (a VARCHAR)");
        let f = jsonl_file("{\"a\":\"A1\",\"stray\":\"x\"}\n");
        let err = adapter
            .load(&local(f.path()), &target("narrow"), &no_create())
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("stray"), "error should name the column: {err}");
        assert!(rows(&adapter, "SELECT * FROM main.narrow").is_empty());
    }

    #[tokio::test]
    async fn csv_with_header_into_existing_table_binds_by_name() {
        let adapter = loader();
        exec(&adapter, "CREATE TABLE main.pair (k VARCHAR, v VARCHAR)");
        // Header order is the reverse of the table's. `007` must stay text.
        let f = csv_file("v,k\nvalue1,007\nvalue2,key2\n");
        adapter
            .load(&local(f.path()), &target("pair"), &no_create())
            .await
            .unwrap();
        assert_eq!(
            rows(&adapter, "SELECT k, v FROM main.pair ORDER BY v"),
            vec![vec!["007", "value1"], vec!["key2", "value2"]]
        );
    }

    #[tokio::test]
    async fn csv_with_header_casts_to_target_types_and_fills_missing_columns() {
        let adapter = loader();
        exec(
            &adapter,
            "CREATE TABLE main.typed (d DATE, n INTEGER, note VARCHAR, c INTEGER DEFAULT 5)",
        );
        let f = csv_file("n,d\n1,2024-01-02\n");
        adapter
            .load(&local(f.path()), &target("typed"), &no_create())
            .await
            .unwrap();
        assert_eq!(
            rows(
                &adapter,
                "SELECT CAST(d AS VARCHAR), n, note, c FROM main.typed"
            ),
            vec![vec!["2024-01-02", "1", "null", "5"]]
        );
    }

    #[tokio::test]
    async fn csv_extra_file_column_is_an_error_naming_the_column() {
        let adapter = loader();
        exec(&adapter, "CREATE TABLE main.narrow (a VARCHAR)");
        let f = csv_file("a,stray\nA1,x\n");
        let err = adapter
            .load(&local(f.path()), &target("narrow"), &no_create())
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("stray"), "error should name the column: {err}");
    }

    #[tokio::test]
    async fn csv_without_header_stays_positional() {
        let adapter = loader();
        exec(&adapter, "CREATE TABLE main.pair (k VARCHAR, v VARCHAR)");
        let f = csv_file("first,second\n");
        let opts = LoadOptions {
            create_table: false,
            csv_has_header: false,
            ..Default::default()
        };
        adapter
            .load(&local(f.path()), &target("pair"), &opts)
            .await
            .unwrap();
        assert_eq!(
            rows(&adapter, "SELECT k, v FROM main.pair"),
            vec![vec!["first", "second"]]
        );
    }

    fn parquet_file(adapter: &DuckDbLoaderAdapter, select: &str) -> NamedTempFile {
        let f = NamedTempFile::with_suffix(".parquet").unwrap();
        exec(
            adapter,
            &format!(
                "COPY ({select}) TO '{}' (FORMAT PARQUET)",
                f.path().display()
            ),
        );
        f
    }

    #[tokio::test]
    async fn parquet_into_existing_table_binds_by_name() {
        let adapter = loader();
        exec(&adapter, "CREATE TABLE main.pair (k VARCHAR, v VARCHAR)");
        let f = parquet_file(&adapter, "SELECT 'value1' AS v, 'key1' AS k");
        adapter
            .load(&local(f.path()), &target("pair"), &no_create())
            .await
            .unwrap();
        assert_eq!(
            rows(&adapter, "SELECT k, v FROM main.pair"),
            vec![vec!["key1", "value1"]]
        );
    }

    #[tokio::test]
    async fn parquet_extra_file_column_is_an_error_naming_the_column() {
        let adapter = loader();
        exec(&adapter, "CREATE TABLE main.narrow (a VARCHAR)");
        let f = parquet_file(&adapter, "SELECT 'A1' AS a, 'x' AS stray");
        let err = adapter
            .load(&local(f.path()), &target("narrow"), &no_create())
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("stray"), "error should name the column: {err}");
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
        assert_eq!(format_target(&t).unwrap(), "main.tbl");
    }

    #[test]
    fn test_format_target_no_schema() {
        let t = TableRef {
            catalog: String::new(),
            schema: String::new(),
            table: "tbl".into(),
        };
        assert_eq!(format_target(&t).unwrap(), "tbl");
    }

    #[test]
    fn format_target_rejects_injecting_table_name() {
        // A discovered filename stem like `evil; DROP TABLE x; --` must error,
        // not flow into the table reference.
        let t = TableRef {
            catalog: String::new(),
            schema: "main".into(),
            table: "evil; DROP TABLE x; --".into(),
        };
        assert!(format_target(&t).is_err());
    }

    #[test]
    fn format_target_rejects_injecting_schema_name() {
        let t = TableRef {
            catalog: String::new(),
            schema: "main; DROP SCHEMA s".into(),
            table: "tbl".into(),
        };
        assert!(format_target(&t).is_err());
    }

    #[test]
    fn test_load_sql_csv() {
        let src = LoadSource::LocalFile(PathBuf::from("/tmp/data.csv"));
        let sql = load_sql(&src, "main.t", FileFormat::Csv, &LoadOptions::default()).unwrap();
        assert!(
            sql.starts_with("INSERT INTO main.t BY NAME SELECT * FROM read_csv('/tmp/data.csv'")
        );
        assert!(sql.contains("header = true"));
        assert!(sql.contains("delim = ','"));
    }

    #[test]
    fn test_load_sql_csv_without_header_stays_positional() {
        let src = LoadSource::LocalFile(PathBuf::from("/tmp/data.csv"));
        let opts = LoadOptions {
            csv_has_header: false,
            ..Default::default()
        };
        let sql = load_sql(&src, "main.t", FileFormat::Csv, &opts).unwrap();
        assert_eq!(
            sql,
            "COPY main.t FROM '/tmp/data.csv' (DELIMITER ',', HEADER false)"
        );
    }

    #[test]
    fn test_load_sql_parquet() {
        let src = LoadSource::LocalFile(PathBuf::from("/tmp/data.parquet"));
        let sql = load_sql(&src, "main.t", FileFormat::Parquet, &LoadOptions::default()).unwrap();
        assert_eq!(
            sql,
            "INSERT INTO main.t BY NAME SELECT * FROM read_parquet('/tmp/data.parquet')"
        );
    }

    #[test]
    fn test_load_sql_jsonl() {
        let src = LoadSource::LocalFile(PathBuf::from("/tmp/data.jsonl"));
        let sql = load_sql(
            &src,
            "main.t",
            FileFormat::JsonLines,
            &LoadOptions::default(),
        )
        .unwrap();
        assert!(sql.contains("read_json_auto"));
        assert!(sql.contains("INSERT INTO main.t BY NAME SELECT"));
    }

    #[test]
    fn test_load_sql_cloud_uri() {
        let src = LoadSource::CloudUri("s3://bucket/data.parquet".into());
        let sql = load_sql(&src, "main.t", FileFormat::Parquet, &LoadOptions::default()).unwrap();
        assert_eq!(
            sql,
            "INSERT INTO main.t BY NAME SELECT * FROM read_parquet('s3://bucket/data.parquet')"
        );
    }

    #[test]
    fn load_sql_rejects_quote_bearing_path() {
        // A path containing a single quote would break out of the '...' literal.
        let src = LoadSource::LocalFile(PathBuf::from("/tmp/a'); DROP TABLE x; --.csv"));
        assert!(load_sql(&src, "main.t", FileFormat::Csv, &LoadOptions::default()).is_err());
    }

    #[test]
    fn load_sql_rejects_quote_bearing_delimiter() {
        let src = LoadSource::LocalFile(PathBuf::from("/tmp/data.csv"));
        let opts = LoadOptions {
            csv_delimiter: '\'',
            ..Default::default()
        };
        assert!(load_sql(&src, "main.t", FileFormat::Csv, &opts).is_err());
    }

    #[test]
    fn test_create_table_sql_csv() {
        let src = LoadSource::LocalFile(PathBuf::from("/tmp/d.csv"));
        let sql = create_table_sql(&src, "main.t", FileFormat::Csv).unwrap();
        assert!(sql.contains("CREATE TABLE main.t AS SELECT * FROM read_csv_auto"));
    }

    #[test]
    fn test_create_table_sql_parquet() {
        let src = LoadSource::LocalFile(PathBuf::from("/tmp/d.parquet"));
        let sql = create_table_sql(&src, "main.t", FileFormat::Parquet).unwrap();
        assert!(sql.contains("read_parquet"));
    }

    #[test]
    fn test_create_table_sql_jsonl() {
        let src = LoadSource::LocalFile(PathBuf::from("/tmp/d.jsonl"));
        let sql = create_table_sql(&src, "main.t", FileFormat::JsonLines).unwrap();
        assert!(sql.contains("read_json_auto"));
    }

    #[test]
    fn create_table_sql_rejects_quote_bearing_path() {
        let src = LoadSource::LocalFile(PathBuf::from("/tmp/a'.csv"));
        assert!(create_table_sql(&src, "main.t", FileFormat::Csv).is_err());
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
