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
//! `COPY INTO` reads a file the warehouse can already see. Two paths:
//!
//! - **Cloud URI** ([`LoadSource::CloudUri`]) — `s3://…`, `abfss://…`, etc.
//!   Databricks reads the URI directly; Rocky runs a single `COPY INTO`.
//! - **Local file** ([`LoadSource::LocalFile`]) — Databricks can't read the
//!   local filesystem, so Rocky stages the file to a Unity Catalog Volume
//!   first (mirroring the Snowflake `PUT → COPY INTO` model):
//!   1. `CREATE VOLUME IF NOT EXISTS <catalog>.<schema>.<volume>` (idempotent),
//!   2. upload the file to `/Volumes/.../rocky_load_<uuid>.<ext>` via the
//!      [Files API],
//!   3. `COPY INTO ... FROM '/Volumes/.../<file>'`,
//!   4. delete the staged **file** (success or failure); the volume is left
//!      in place as a reusable governed container.
//!
//! The volume lands in the **target table's** catalog + schema, with a
//! configurable volume name (default [`crate::volume::DEFAULT_STAGING_VOLUME`]).
//!
//! [Files API]: https://docs.databricks.com/api/workspace/files

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
use crate::volume::{
    DEFAULT_STAGING_VOLUME, StagingVolume, create_volume_sql, generate_staged_file_name,
};

/// Databricks loader adapter using COPY INTO.
///
/// Local files are staged to a Unity Catalog Volume in the target table's
/// catalog + schema before the COPY INTO; the volume name defaults to
/// [`crate::volume::DEFAULT_STAGING_VOLUME`] and is configurable via
/// [`Self::with_staging_volume`].
pub struct DatabricksLoaderAdapter {
    connector: Arc<DatabricksConnector>,
    /// Unity Catalog Volume name used to stage local files, created on demand
    /// in the target table's catalog + schema. Cloud-URI loads never touch it.
    staging_volume: String,
}

impl DatabricksLoaderAdapter {
    /// Create a loader wrapping the given `DatabricksConnector`, staging local
    /// files to the default volume ([`crate::volume::DEFAULT_STAGING_VOLUME`]).
    pub fn new(connector: Arc<DatabricksConnector>) -> Self {
        Self {
            connector,
            staging_volume: DEFAULT_STAGING_VOLUME.to_string(),
        }
    }

    /// Override the Unity Catalog Volume name used to stage local files.
    ///
    /// The volume is created on demand (`CREATE VOLUME IF NOT EXISTS`) in the
    /// target table's catalog + schema. The name is validated as a SQL
    /// identifier when a local file is actually staged — an invalid name only
    /// surfaces on a local-file load, never on a cloud-URI load.
    #[must_use]
    pub fn with_staging_volume(mut self, volume: impl Into<String>) -> Self {
        self.staging_volume = volume.into();
        self
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

/// Reject a `FROM '<path>'` literal that would break out of the surrounding
/// `'...'` SQL string literal. Used for both cloud URIs (`s3://…`) and staged
/// Volume paths (`/Volumes/…`). Rocky never escapes embedded quotes — instead
/// we refuse to build the SQL.
fn validate_sql_path_literal(path: &str) -> AdapterResult<&str> {
    if path.is_empty() {
        return Err(AdapterError::msg("COPY INTO source path cannot be empty"));
    }
    if path.contains(['\'', '\n', '\r', '\\']) {
        return Err(AdapterError::msg(format!(
            "invalid COPY INTO source path {path:?}: must not contain quotes, backslashes, or newlines"
        )));
    }
    Ok(path)
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

/// Build the full `COPY INTO ... FROM '<path>'` SQL statement.
///
/// `path` is either a cloud URI (`s3://…`) or a staged Volume path
/// (`/Volumes/…`) — the FROM clause and FORMAT_OPTIONS are byte-identical
/// either way, so both source kinds share this builder. The caller must pass a
/// validated `target_ref` (use [`format_target`]) and a validated `path` (use
/// [`validate_sql_path_literal`]) so the literal-context interpolation can't be
/// broken out of.
fn build_copy_into_sql(
    path: &str,
    target_ref: &str,
    format: FileFormat,
    options: &LoadOptions,
) -> AdapterResult<String> {
    build_copy_into_sql_from(&format!("'{path}'"), target_ref, format, options)
}

/// Build `COPY INTO … FROM <from_clause> …` where `from_clause` is the full
/// FROM expression — either a quoted path (`'/Volumes/…'`) or a typed-cast
/// subquery (`(SELECT CAST(…) FROM '…')`). All path + identifier validation
/// happens in the caller before the clause is assembled.
fn build_copy_into_sql_from(
    from_clause: &str,
    target_ref: &str,
    format: FileFormat,
    options: &LoadOptions,
) -> AdapterResult<String> {
    let fileformat = file_format_clause(format);
    let format_opts = format_options_clause(format, options)?
        .map(|s| format!(" {s}"))
        .unwrap_or_default();
    // mergeSchema lets Databricks evolve the target schema when new columns
    // appear. CSV loads route through a typed-cast subquery (see
    // `build_csv_cast_source`), so the loaded schema already matches the
    // target and the merge is a no-op rather than a string-vs-typed conflict.
    let copy_opts = if options.create_table {
        " COPY_OPTIONS ('mergeSchema' = 'true')"
    } else {
        ""
    };
    Ok(format!(
        "COPY INTO {target_ref} FROM {from_clause} FILEFORMAT = {fileformat}{format_opts}{copy_opts}"
    ))
}

/// Build a typed `(SELECT CAST(`col` AS <type>) … FROM '<path>')` source for a
/// CSV `COPY INTO`. Databricks reads CSV columns as strings; casting each to
/// the target's declared type lands real typed values and avoids
/// `DELTA_FAILED_TO_MERGE_FIELDS` when a target column isn't STRING. `path`
/// must already be validated via [`validate_sql_path_literal`]; `columns` are
/// the target's `(name, declared_type)` pairs from
/// [`DatabricksLoaderAdapter::describe_target_columns`].
fn build_csv_cast_source(path: &str, columns: &[(String, String)]) -> AdapterResult<String> {
    let mut casts = Vec::with_capacity(columns.len());
    for (name, dtype) in columns {
        validate_identifier(name).map_err(AdapterError::new)?;
        let ty = validate_sql_type(dtype)?;
        casts.push(format!("CAST(`{name}` AS {ty}) AS `{name}`"));
    }
    Ok(format!("(SELECT {} FROM '{path}')", casts.join(", ")))
}

/// Reject a column type string that could break out of the `CAST(… AS <type>)`
/// context. DESCRIBE-sourced types are trusted, but we still constrain them to
/// `[A-Za-z0-9_(), <>]` (covers `decimal(10,2)`, `array<int>`,
/// `map<string,int>`) so an unexpected warehouse response can't inject SQL.
fn validate_sql_type(ty: &str) -> AdapterResult<&str> {
    let t = ty.trim();
    if t.is_empty() {
        return Err(AdapterError::msg("empty column type from DESCRIBE TABLE"));
    }
    let safe = t
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '(' | ')' | ',' | ' ' | '<' | '>'));
    if safe {
        Ok(t)
    } else {
        Err(AdapterError::msg(format!(
            "unsafe column type {ty:?} from DESCRIBE TABLE"
        )))
    }
}

/// Build `CREATE TABLE <target> AS SELECT * FROM read_files('<path>', …)`, used
/// when `create_table` is set and the target doesn't exist (COPY INTO can't
/// create a table). `read_files` infers the schema from the file — mirroring
/// DuckDB's `read_csv_auto` / `read_parquet` — so CSV columns get inferred
/// types rather than all-strings. `path` must already be validated via
/// [`validate_sql_path_literal`].
fn build_create_table_from_files_sql(
    target_ref: &str,
    path: &str,
    format: FileFormat,
    options: &LoadOptions,
) -> AdapterResult<String> {
    let fmt = read_files_format(format);
    let csv_opts = if format == FileFormat::Csv {
        let header = if options.csv_has_header {
            "true"
        } else {
            "false"
        };
        let delim = validate_csv_delimiter(options.csv_delimiter)?;
        format!(", header => {header}, sep => '{delim}'")
    } else {
        String::new()
    };
    Ok(format!(
        "CREATE TABLE {target_ref} AS SELECT * FROM read_files('{path}', format => '{fmt}'{csv_opts})"
    ))
}

/// `read_files` format token for a [`FileFormat`].
fn read_files_format(format: FileFormat) -> &'static str {
    match format {
        FileFormat::Csv => "csv",
        FileFormat::Parquet => "parquet",
        FileFormat::JsonLines => "json",
    }
}

impl DatabricksLoaderAdapter {
    /// Execute `DELETE FROM <target>` when `truncate_first` is set. Databricks
    /// supports DELETE FROM on managed tables.
    async fn maybe_truncate(&self, target_ref: &str, options: &LoadOptions) -> AdapterResult<()> {
        if options.truncate_first {
            let sql = format!("DELETE FROM {target_ref}");
            debug!(sql = %sql, "truncating target before COPY INTO");
            self.connector
                .execute_statement(&sql)
                .await
                .map_err(|e| AdapterError::msg(e.to_string()))?;
        }
        Ok(())
    }

    /// Fetch the target table's declared `(name, type)` columns via
    /// `DESCRIBE TABLE`, so a CSV load can CAST each column to its declared
    /// type. Returns an empty `Vec` when the table can't be described (e.g. it
    /// doesn't exist yet) — the caller then falls back to a plain path load.
    async fn describe_target_columns(&self, target_ref: &str) -> Vec<(String, String)> {
        let sql = format!("DESCRIBE TABLE {target_ref}");
        let result = match self.connector.execute_sql(&sql).await {
            Ok(r) => r,
            Err(e) => {
                debug!(error = %e, target = %target_ref, "DESCRIBE TABLE failed; CSV load falls back to a plain COPY INTO");
                return Vec::new();
            }
        };
        let name_idx = result
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case("col_name"))
            .unwrap_or(0);
        let type_idx = result
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case("data_type"))
            .unwrap_or(1);
        let mut cols = Vec::new();
        for row in &result.rows {
            let name = row
                .get(name_idx)
                .and_then(serde_json::Value::as_str)
                .unwrap_or("")
                .trim()
                .to_string();
            // DESCRIBE TABLE lists data columns first, then a blank row and
            // `# Partition Information` / `# Detailed Table Information`
            // sections — stop at the first non-column row.
            if name.is_empty() || name.starts_with('#') {
                break;
            }
            let dtype = row
                .get(type_idx)
                .and_then(serde_json::Value::as_str)
                .unwrap_or("")
                .trim()
                .to_string();
            if !dtype.is_empty() {
                cols.push((name, dtype));
            }
        }
        cols
    }

    /// Run a `COPY INTO` and return the rows-loaded count.
    ///
    /// `path` must already be validated via [`validate_sql_path_literal`]. For
    /// CSV into an existing table, each column is cast to the target's declared
    /// type (Databricks reads CSV as strings, so a non-STRING target column
    /// would otherwise fail `DELTA_FAILED_TO_MERGE_FIELDS`); Parquet/JSON carry
    /// their own types, and a missing target is created from its inferred schema
    /// (`read_files`) when `create_table` is set. Databricks' COPY INTO returns a single-row result
    /// whose schema includes `num_affected_rows`; if it's absent we fall back
    /// to 0 rather than fail.
    async fn run_copy_into(
        &self,
        path: &str,
        target_ref: &str,
        format: FileFormat,
        options: &LoadOptions,
    ) -> AdapterResult<u64> {
        // Existence check (also yields the declared columns for the CSV cast
        // path); an empty Vec means the table doesn't exist.
        let columns = self.describe_target_columns(target_ref).await;

        if columns.is_empty() {
            // COPY INTO can't create a table. Either create it from the file's
            // inferred schema (read_files), or fail clearly when create_table
            // is off.
            if !options.create_table {
                return Err(AdapterError::msg(format!(
                    "target table {target_ref} does not exist and create_table is disabled; \
                     create the table first or enable create_table"
                )));
            }
            let sql = build_create_table_from_files_sql(target_ref, path, format, options)?;
            info!(sql = %sql, "databricks CREATE TABLE AS read_files");
            self.connector
                .execute_sql(&sql)
                .await
                .map_err(|e| AdapterError::msg(e.to_string()))?;
            // CTAS doesn't return a load count — read it back.
            return self.count_table_rows(target_ref).await;
        }

        // Target exists → COPY INTO. For CSV, cast each column to its declared
        // type so string values land typed.
        let sql = if format == FileFormat::Csv {
            let from = build_csv_cast_source(path, &columns)?;
            build_copy_into_sql_from(&from, target_ref, format, options)?
        } else {
            build_copy_into_sql(path, target_ref, format, options)?
        };
        info!(sql = %sql, "databricks COPY INTO");
        let copy_result = self
            .connector
            .execute_sql(&sql)
            .await
            .map_err(|e| AdapterError::msg(e.to_string()))?;

        Ok(rows_from_copy_result(&copy_result))
    }

    /// Count rows in a table — reports rows-loaded after a CTAS create-load,
    /// which (unlike COPY INTO) doesn't return a `num_affected_rows` counter.
    async fn count_table_rows(&self, target_ref: &str) -> AdapterResult<u64> {
        let sql = format!("SELECT COUNT(*) AS n FROM {target_ref}");
        let result = self
            .connector
            .execute_sql(&sql)
            .await
            .map_err(|e| AdapterError::msg(e.to_string()))?;
        Ok(result
            .rows
            .first()
            .and_then(|row| row.first())
            .and_then(|v| {
                v.as_u64()
                    .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
            })
            .unwrap_or(0))
    }

    /// Stage a local file to a Unity Catalog Volume, COPY INTO from it, then
    /// clean up the staged file.
    ///
    /// Sequence (mirrors the Snowflake `PUT → COPY INTO → DROP` ordering):
    /// 1. `CREATE VOLUME IF NOT EXISTS` in the target's catalog + schema,
    /// 2. read the file + upload it to `/Volumes/.../rocky_load_<uuid>.<ext>`,
    /// 3. `COPY INTO ... FROM` the staged path,
    /// 4. delete the staged file — on upload failure, COPY failure, or success.
    ///
    /// Returns `(rows_loaded, bytes_read)`. The volume is never dropped (it's a
    /// reusable governed container shared across loads).
    async fn load_local_file(
        &self,
        local_path: &std::path::Path,
        target: &TableRef,
        target_ref: &str,
        format: FileFormat,
        options: &LoadOptions,
    ) -> AdapterResult<(u64, u64)> {
        // Stage into the *target table's* catalog + schema. `format_target`
        // already validated these identifiers, but `StagingVolume::new`
        // re-validates (including the configured volume name) so the rendered
        // DDL + path are provably safe.
        let volume = StagingVolume::new(&target.catalog, &target.schema, &self.staging_volume)?;

        // Auto-provision the volume (idempotent). Databricks has no
        // temporary-volume concept, so without this the Files API PUT 404s.
        let create_sql = create_volume_sql(&volume);
        debug!(sql = %create_sql, "creating staging volume");
        self.connector
            .execute_statement(&create_sql)
            .await
            .map_err(|e| AdapterError::msg(e.to_string()))?;

        // Read the local file before uploading so a read error fails fast,
        // before we've created anything to clean up on the volume. Use the
        // async fs API so a large staging file (or a slow/networked disk)
        // doesn't block the tokio worker while it's read into memory.
        let contents = tokio::fs::read(local_path).await.map_err(|e| {
            AdapterError::msg(format!(
                "failed to read local file '{}': {e}",
                local_path.display()
            ))
        })?;
        let bytes_read = contents.len() as u64;

        let file_name = generate_staged_file_name(format.extension());
        let staged_path = volume.file_path(&file_name)?;
        // Defensive: the staged path is built from validated components, but
        // the COPY INTO literal check is the same guard the cloud path uses.
        let staged_path = validate_sql_path_literal(&staged_path)?.to_string();

        info!(volume_path = %staged_path, "staging local file to UC Volume");
        if let Err(e) = self.connector.upload_file(&staged_path, contents).await {
            // Upload failed — best-effort clean up anything partially written.
            self.cleanup_staged_file(&staged_path).await;
            return Err(AdapterError::msg(format!(
                "Files API upload to '{staged_path}' failed: {e}"
            )));
        }

        // COPY INTO from the staged path; clean up the file regardless of
        // the COPY result.
        let copy_result = self
            .run_copy_into(&staged_path, target_ref, format, options)
            .await;
        self.cleanup_staged_file(&staged_path).await;

        let rows_loaded = copy_result?;
        Ok((rows_loaded, bytes_read))
    }

    /// Best-effort deletion of a staged file. Logs (does not fail the load) on
    /// error — the volume's lifecycle policy and the unique UUID name mean a
    /// leaked file is harmless, and we never want cleanup to mask the real
    /// load result.
    async fn cleanup_staged_file(&self, staged_path: &str) {
        if let Err(e) = self.connector.delete_file(staged_path).await {
            warn!(error = %e, volume_path = %staged_path, "failed to delete staged file (non-fatal)");
        }
    }
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

        let format = resolve_format(source, options)?;
        let target_ref = format_target(target)?;

        self.maybe_truncate(&target_ref, options).await?;

        let (rows_loaded, bytes_read) = match source {
            LoadSource::CloudUri(uri) => {
                let uri = validate_sql_path_literal(uri)?.to_string();
                let rows = self
                    .run_copy_into(&uri, &target_ref, format, options)
                    .await?;
                (rows, 0) // Cloud-URI sources: no local byte count.
            }
            LoadSource::LocalFile(local_path) => {
                self.load_local_file(local_path, target, &target_ref, format, options)
                    .await?
            }
        };

        Ok(LoadResult {
            rows_loaded,
            bytes_read,
            duration_ms: start.elapsed().as_millis() as u64,
        })
    }

    fn supported_formats(&self) -> Vec<FileFormat> {
        vec![FileFormat::Csv, FileFormat::Parquet, FileFormat::JsonLines]
    }
}

/// Extract the rows-loaded count from a COPY INTO response, warning (rather
/// than failing the load) when `num_affected_rows` is absent.
fn rows_from_copy_result(copy_result: &QueryResult) -> u64 {
    match extract_num_affected_rows(copy_result) {
        Some(n) => n,
        None => {
            warn!(
                statement_id = %copy_result.statement_id,
                "databricks COPY INTO response missing `num_affected_rows` \
                 column; reporting 0 rows loaded"
            );
            0
        }
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
    fn test_validate_sql_path_literal_rejects_quote() {
        assert!(validate_sql_path_literal("s3://bucket/key").is_ok());
        assert!(validate_sql_path_literal("/Volumes/main/raw/vol/rocky_load_abc.csv").is_ok());
        assert!(validate_sql_path_literal("s3://bucket/key'; DROP --").is_err());
        assert!(validate_sql_path_literal("s3://bucket/\nfoo").is_err());
        assert!(validate_sql_path_literal("").is_err());
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

    #[test]
    fn test_rows_from_copy_result_present() {
        let qr = copy_into_result("num_affected_rows", serde_json::json!(55));
        assert_eq!(rows_from_copy_result(&qr), 55);
    }

    #[test]
    fn test_rows_from_copy_result_missing_defaults_zero() {
        let qr = copy_into_result("some_other_column", serde_json::json!(99));
        assert_eq!(rows_from_copy_result(&qr), 0);
    }

    #[test]
    fn test_build_copy_into_from_volume_path() {
        // The Volume-staged path shares the cloud-URI builder, so FORMAT_OPTIONS
        // are byte-identical — only the FROM literal differs.
        let opts = LoadOptions::default();
        let sql = build_copy_into_sql(
            "/Volumes/main/raw/rocky_staging/rocky_load_abc.csv",
            "main.raw.orders",
            FileFormat::Csv,
            &opts,
        )
        .unwrap();
        assert!(sql.contains("FROM '/Volumes/main/raw/rocky_staging/rocky_load_abc.csv'"));
        assert!(sql.contains("FILEFORMAT = CSV"));
        assert!(sql.contains("'header' = 'true'"));
        assert!(sql.contains("'delimiter' = ','"));
        assert!(sql.contains("mergeSchema"));
    }

    #[test]
    fn test_build_csv_cast_source() {
        let cols = vec![
            ("id".to_string(), "BIGINT".to_string()),
            ("name".to_string(), "STRING".to_string()),
        ];
        let src = build_csv_cast_source("/Volumes/c/s/v/f.csv", &cols).unwrap();
        assert_eq!(
            src,
            "(SELECT CAST(`id` AS BIGINT) AS `id`, CAST(`name` AS STRING) AS `name` FROM '/Volumes/c/s/v/f.csv')"
        );
    }

    #[test]
    fn test_build_create_table_from_files_csv() {
        let opts = LoadOptions::default();
        let sql = build_create_table_from_files_sql(
            "main.raw.orders",
            "/Volumes/c/s/v/f.csv",
            FileFormat::Csv,
            &opts,
        )
        .unwrap();
        assert_eq!(
            sql,
            "CREATE TABLE main.raw.orders AS SELECT * FROM read_files('/Volumes/c/s/v/f.csv', format => 'csv', header => true, sep => ',')"
        );
    }

    #[test]
    fn test_build_create_table_from_files_parquet() {
        let opts = LoadOptions::default();
        let sql = build_create_table_from_files_sql(
            "main.raw.orders",
            "s3://b/d.parquet",
            FileFormat::Parquet,
            &opts,
        )
        .unwrap();
        assert_eq!(
            sql,
            "CREATE TABLE main.raw.orders AS SELECT * FROM read_files('s3://b/d.parquet', format => 'parquet')"
        );
    }

    #[test]
    fn test_build_csv_cast_source_rejects_bad_identifier() {
        let cols = vec![("id`; DROP".to_string(), "BIGINT".to_string())];
        assert!(build_csv_cast_source("/p.csv", &cols).is_err());
    }

    #[test]
    fn test_validate_sql_type() {
        assert_eq!(validate_sql_type("BIGINT").unwrap(), "BIGINT");
        assert_eq!(validate_sql_type("decimal(10,2)").unwrap(), "decimal(10,2)");
        assert_eq!(validate_sql_type("array<int>").unwrap(), "array<int>");
        assert!(validate_sql_type("string'; DROP").is_err());
        assert!(validate_sql_type("").is_err());
    }

    #[test]
    fn test_copy_into_csv_cast_subquery_shape() {
        let cols = vec![
            ("id".to_string(), "BIGINT".to_string()),
            ("name".to_string(), "STRING".to_string()),
        ];
        let from = build_csv_cast_source("/Volumes/c/s/v/f.csv", &cols).unwrap();
        let sql = build_copy_into_sql_from(
            &from,
            "main.raw.orders",
            FileFormat::Csv,
            &LoadOptions::default(),
        )
        .unwrap();
        assert!(sql.starts_with("COPY INTO main.raw.orders FROM (SELECT CAST(`id` AS BIGINT)"));
        assert!(sql.contains("FILEFORMAT = CSV"));
        assert!(sql.contains("'header' = 'true'"));
    }

    #[test]
    fn test_loader_default_staging_volume() {
        // `new` uses the default; `with_staging_volume` overrides it. We don't
        // need a connector to inspect the field — construct via a dummy Arc.
        // (Build a connector pointing at an unused host; never executed.)
        use crate::auth::{Auth, AuthConfig};
        use crate::connector::ConnectorConfig;
        use std::time::Duration;

        let auth = Auth::from_config(AuthConfig {
            host: "unused.databricks.com".into(),
            token: Some("t".into()),
            client_id: None,
            client_secret: None,
        })
        .unwrap();
        let connector = Arc::new(DatabricksConnector::new(
            ConnectorConfig {
                host: "unused.databricks.com".into(),
                warehouse_id: "w".into(),
                timeout: Duration::from_secs(1),
                retry: Default::default(),
            },
            auth,
        ));

        let default_loader = DatabricksLoaderAdapter::new(Arc::clone(&connector));
        assert_eq!(default_loader.staging_volume, DEFAULT_STAGING_VOLUME);

        let custom = DatabricksLoaderAdapter::new(connector).with_staging_volume("hc_stage");
        assert_eq!(custom.staging_volume, "hc_stage");
    }
}
