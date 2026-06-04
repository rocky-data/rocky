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
//!
//! ## Creating the target from a local CSV
//!
//! `COPY INTO` requires the target table to already exist. When loading a
//! **local CSV** with [`LoadOptions::create_table`] set, the loader first
//! infers column types from the file (via
//! [`rocky_core::seeds::infer_schema_from_csv`]), maps them to Snowflake type
//! names, and emits `CREATE TABLE IF NOT EXISTS` *before* the stage + PUT +
//! COPY sequence. This unblocks loading a CSV into a fresh table — notably the
//! typed contract-gate, which loads into a per-load staging table that doesn't
//! exist yet.
//!
//! Scope of the create-from-file path:
//! - **Local CSV only.** Type inference reads the local file, so it can't run
//!   against a cloud URI. Auto-creating from `s3://` would need Snowflake's
//!   server-side `INFER_SCHEMA` table function — left as a follow-up.
//! - **Comma-delimited.** `infer_schema_from_csv` reads with a comma
//!   delimiter; a non-comma local CSV with `create_table` won't auto-create
//!   the right columns. `CREATE TABLE IF NOT EXISTS` keeps an existing table
//!   safe regardless. A delimiter-aware inference is a follow-up.
//! - **Local Parquet / JSONL fall through untouched** (no inference, no
//!   create) — they only load into a pre-existing table today, and silently
//!   skipping the create preserves that behavior rather than failing the load.

use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use tracing::{debug, info, warn};

use rocky_adapter_sdk::{
    AdapterError, AdapterResult, FileFormat, LoadOptions, LoadResult, LoadSource, LoaderAdapter,
    TableRef,
};
use rocky_core::sql_gen::validate_sql_type;
use rocky_sql::validation::validate_identifier;

use crate::connector::{QueryResult, SnowflakeConnector};
use crate::dialect::SnowflakeSqlDialect;
use crate::stage::{
    copy_into_sql, create_external_stage_sql, create_temporary_stage_sql, drop_stage_sql,
    generate_stage_name, put_file_sql,
};

use rocky_core::traits::SqlDialect;

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
/// 3-part form as Databricks for fully-qualified references). Each segment is
/// validated as a SQL identifier so the result is safe to interpolate.
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

/// Map a [`rocky_core::seeds`]-inferred generic SQL type to its Snowflake
/// equivalent.
///
/// `infer_schema_from_csv` emits a small, fixed set of generic type names
/// (`BOOLEAN`, `BIGINT`, `DOUBLE`, `TIMESTAMP`, `STRING`). Snowflake accepts
/// `BOOLEAN` and `TIMESTAMP` verbatim, but the canonical names that round-trip
/// through `DESCRIBE TABLE` (and the dialect's safe-widening parsers) are
/// `NUMBER(38,0)`, `FLOAT`, `TIMESTAMP_NTZ`, and `VARCHAR`. Mapping to those
/// keeps a later schema-drift comparison stable.
///
/// Any name not in the known set is passed through unchanged — it's then
/// validated by [`validate_sql_type`] before interpolation, so an unexpected
/// value can't smuggle SQL into the DDL.
fn map_inferred_type_to_snowflake(generic: &str) -> &str {
    match generic {
        "BIGINT" => "NUMBER(38,0)",
        "DOUBLE" => "FLOAT",
        "TIMESTAMP" => "TIMESTAMP_NTZ",
        // `infer_schema_from_csv` emits the generic `STRING`; Snowflake's
        // canonical text type is `VARCHAR` (its `STRING` alias also works,
        // but `DESCRIBE` reports `VARCHAR`).
        "STRING" => "VARCHAR",
        // `BOOLEAN` and any already-Snowflake / overridden type pass through.
        other => other,
    }
}

/// Build a `CREATE TABLE IF NOT EXISTS <target> (<typed cols>)` statement from
/// CSV-inferred columns.
///
/// Each column name is validated as a SQL identifier and double-quoted (the
/// adapter's lowercase-preserving convention — matching `format_table_ref`,
/// `merge_into`, and the COPY column resolution). Each mapped Snowflake type is
/// validated by [`validate_sql_type`] before interpolation. `IF NOT EXISTS`
/// makes the statement idempotent and avoids any existence round-trip
/// (a `DESCRIBE`/exists probe is deliberately *not* issued).
fn create_table_from_csv_sql(
    target_ref: &str,
    columns: &[rocky_core::seeds::ColumnDef],
) -> AdapterResult<String> {
    if columns.is_empty() {
        return Err(AdapterError::msg(
            "cannot create table from CSV with no columns",
        ));
    }
    let dialect = SnowflakeSqlDialect;
    let mut col_defs = Vec::with_capacity(columns.len());
    for col in columns {
        validate_identifier(&col.name).map_err(AdapterError::new)?;
        let sf_type = map_inferred_type_to_snowflake(&col.data_type);
        validate_sql_type(sf_type).map_err(AdapterError::new)?;
        let col_q = dialect.quote_identifier(&col.name);
        col_defs.push(format!("{col_q} {sf_type}"));
    }
    Ok(format!(
        "CREATE TABLE IF NOT EXISTS {target_ref} ({})",
        col_defs.join(", ")
    ))
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
        let target_ref = format_target(target)?;
        let stage = generate_stage_name();

        // `create_table` for a LOCAL CSV: COPY INTO needs an existing table, so
        // infer column types from the file and `CREATE TABLE IF NOT EXISTS`
        // before the stage + PUT + COPY sequence. Runs ahead of the truncate
        // block so a `create_table + truncate_first` combo against a missing
        // table creates first (the DELETE would otherwise fail on a
        // non-existent table). Local Parquet/JSONL and cloud URIs fall through
        // untouched — see the module docs for the create-from-file scope.
        if options.create_table
            && format == FileFormat::Csv
            && let LoadSource::LocalFile(local_path) = source
        {
            let columns = rocky_core::seeds::infer_schema_from_csv(local_path)
                .map_err(|e| AdapterError::msg(format!("failed to infer CSV schema: {e}")))?;
            let create_sql = create_table_from_csv_sql(&target_ref, &columns)?;
            debug!(sql = %create_sql, "ensuring target table exists (create from CSV)");
            self.exec(&create_sql).await?;
        }

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
                let create_sql = create_external_stage_sql(&stage, uri, format, options)?;
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
                let create_sql = create_temporary_stage_sql(&stage, format, options)?;
                debug!(sql = %create_sql, "creating internal stage");
                self.exec(&create_sql).await?;

                let local_str = local_path_str(local_path)?;
                let put_sql = put_file_sql(&local_str, &stage)?;
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
        assert_eq!(format_target(&t).unwrap(), "DB.RAW.ORDERS");
    }

    #[test]
    fn test_format_target_no_catalog() {
        let t = TableRef {
            catalog: String::new(),
            schema: "RAW".into(),
            table: "ORDERS".into(),
        };
        assert_eq!(format_target(&t).unwrap(), "RAW.ORDERS");
    }

    #[test]
    fn test_format_target_rejects_injection() {
        let t = TableRef {
            catalog: "DB".into(),
            schema: "RAW; DROP TABLE x; --".into(),
            table: "ORDERS".into(),
        };
        assert!(format_target(&t).is_err());
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

    use rocky_core::seeds::ColumnDef;

    fn col(name: &str, data_type: &str) -> ColumnDef {
        ColumnDef {
            name: name.into(),
            data_type: data_type.into(),
        }
    }

    #[test]
    fn test_map_inferred_type_to_snowflake_known_types() {
        // The generic names `infer_schema_from_csv` emits map to Snowflake's
        // canonical `DESCRIBE`-round-tripping names.
        assert_eq!(map_inferred_type_to_snowflake("BIGINT"), "NUMBER(38,0)");
        assert_eq!(map_inferred_type_to_snowflake("DOUBLE"), "FLOAT");
        assert_eq!(map_inferred_type_to_snowflake("TIMESTAMP"), "TIMESTAMP_NTZ");
        assert_eq!(map_inferred_type_to_snowflake("STRING"), "VARCHAR");
        // BOOLEAN is accepted verbatim by Snowflake.
        assert_eq!(map_inferred_type_to_snowflake("BOOLEAN"), "BOOLEAN");
    }

    #[test]
    fn test_map_inferred_type_passthrough_for_unknown() {
        // Already-Snowflake / overridden types pass through unchanged; the
        // caller still runs them through `validate_sql_type`.
        assert_eq!(
            map_inferred_type_to_snowflake("NUMBER(10,2)"),
            "NUMBER(10,2)"
        );
        assert_eq!(
            map_inferred_type_to_snowflake("VARCHAR(255)"),
            "VARCHAR(255)"
        );
    }

    #[test]
    fn test_create_table_from_csv_sql_quotes_and_maps_types() {
        let cols = vec![
            col("id", "BIGINT"),
            col("name", "STRING"),
            col("amount", "DOUBLE"),
            col("active", "BOOLEAN"),
            col("created_at", "TIMESTAMP"),
        ];
        let sql = create_table_from_csv_sql("DB.RAW.ORDERS", &cols).unwrap();
        assert_eq!(
            sql,
            "CREATE TABLE IF NOT EXISTS DB.RAW.ORDERS \
             (\"id\" NUMBER(38,0), \"name\" VARCHAR, \"amount\" FLOAT, \
             \"active\" BOOLEAN, \"created_at\" TIMESTAMP_NTZ)"
        );
    }

    #[test]
    fn test_create_table_from_csv_sql_is_idempotent() {
        let cols = vec![col("id", "BIGINT")];
        let sql = create_table_from_csv_sql("T", &cols).unwrap();
        // `IF NOT EXISTS` keeps the create safe when the table already exists —
        // no DESCRIBE / existence round-trip is issued (the #826 lesson).
        assert!(sql.starts_with("CREATE TABLE IF NOT EXISTS T ("));
    }

    #[test]
    fn test_create_table_from_csv_sql_rejects_empty_columns() {
        assert!(create_table_from_csv_sql("T", &[]).is_err());
    }

    #[test]
    fn test_create_table_from_csv_sql_rejects_bad_column_identifier() {
        // A header carrying SQL must be refused at identifier validation —
        // never interpolated into the DDL.
        let cols = vec![col("bad; DROP TABLE x; --", "BIGINT")];
        assert!(create_table_from_csv_sql("T", &cols).is_err());
    }

    #[test]
    fn test_create_table_from_csv_sql_rejects_bad_type() {
        // A passthrough type carrying SQL must be refused by validate_sql_type.
        let cols = vec![col("id", "BIGINT; DROP TABLE x")];
        assert!(create_table_from_csv_sql("T", &cols).is_err());
    }
}
