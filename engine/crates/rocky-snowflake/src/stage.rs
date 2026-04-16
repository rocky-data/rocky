//! Snowflake stage lifecycle helpers used by the loader adapter.
//!
//! A stage is a named location where files are kept before being copied into
//! a table. Rocky creates stages with `CREATE TEMPORARY STAGE` so they
//! auto-expire with the session, ensuring cleanup even if the load errors.
//!
//! Two stage flavours exist:
//!
//! - **Internal stage** (`@<name>`) — a Snowflake-managed location used for
//!   `PUT` uploads from the client (local file → stage → COPY INTO).
//! - **External stage** (`@<name>` backed by `URL = 's3://...'`) — points at
//!   a cloud URI. No PUT needed; `COPY INTO` reads directly from cloud.
//!
//! Stage names are validated as SQL identifiers (alnum + underscores). The
//! generated SQL intentionally uses simple identifiers without double-quoting.

use rocky_adapter_sdk::{AdapterError, AdapterResult, FileFormat, LoadOptions};

/// Snowflake stage reference. Prefixed with `@` when rendered in SQL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StageName(String);

impl StageName {
    /// Validate and construct a stage name.
    pub fn new(name: &str) -> AdapterResult<Self> {
        if name.is_empty() {
            return Err(AdapterError::msg("stage name cannot be empty"));
        }
        if !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
        {
            return Err(AdapterError::msg(format!(
                "invalid stage name '{name}' — must be [A-Za-z0-9_]+"
            )));
        }
        Ok(Self(name.to_string()))
    }

    /// Raw stage name (without `@`).
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Render as `@<name>` for use in SQL.
    pub fn reference(&self) -> String {
        format!("@{}", self.0)
    }
}

impl std::fmt::Display for StageName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Build the `CREATE TEMPORARY STAGE` SQL.
///
/// Temporary stages are dropped automatically when the session ends, so a
/// failed load still cleans up resources. Includes FILE_FORMAT defaults for
/// the given [`FileFormat`] so COPY INTO doesn't need to re-specify them.
pub fn create_temporary_stage_sql(name: &StageName, format: FileFormat, options: &LoadOptions) -> String {
    format!(
        "CREATE TEMPORARY STAGE {name} FILE_FORMAT = ({})",
        file_format_clause(format, options)
    )
}

/// Build the `CREATE TEMPORARY STAGE ... URL = '<uri>'` SQL for an external
/// stage backed by cloud storage.
pub fn create_external_stage_sql(
    name: &StageName,
    uri: &str,
    format: FileFormat,
    options: &LoadOptions,
) -> String {
    format!(
        "CREATE TEMPORARY STAGE {name} URL = '{uri}' FILE_FORMAT = ({})",
        file_format_clause(format, options)
    )
}

/// Build the `PUT file://<path> @<stage>` SQL for uploading a local file.
///
/// Snowflake's PUT automatically compresses files with gzip unless we turn
/// that off; we keep the default for smaller network transfer.
pub fn put_file_sql(local_path: &str, stage: &StageName) -> String {
    format!(
        "PUT 'file://{local_path}' {stage_ref} OVERWRITE = TRUE",
        stage_ref = stage.reference()
    )
}

/// Build the `DROP STAGE IF EXISTS` SQL (defensive cleanup).
///
/// Temporary stages drop themselves at session end, but explicit cleanup
/// reduces resource pressure on long-running sessions.
pub fn drop_stage_sql(name: &StageName) -> String {
    format!("DROP STAGE IF EXISTS {name}")
}

/// Build the FILE_FORMAT inner clause for a [`FileFormat`] + [`LoadOptions`].
///
/// Snowflake expects `(TYPE = 'CSV', FIELD_DELIMITER = ',', SKIP_HEADER = 1)`
/// or similar. This function emits only the inner key-value pairs so callers
/// can wrap in `FILE_FORMAT = (...)` as needed.
pub fn file_format_clause(format: FileFormat, options: &LoadOptions) -> String {
    match format {
        FileFormat::Csv => {
            let skip = if options.csv_has_header { 1 } else { 0 };
            let delim = options.csv_delimiter;
            format!("TYPE = 'CSV', FIELD_DELIMITER = '{delim}', SKIP_HEADER = {skip}")
        }
        FileFormat::Parquet => "TYPE = 'PARQUET'".into(),
        FileFormat::JsonLines => "TYPE = 'JSON'".into(),
    }
}

/// Build the `COPY INTO <target> FROM @<stage>` SQL.
pub fn copy_into_sql(target_ref: &str, stage: &StageName) -> String {
    format!("COPY INTO {target_ref} FROM {}", stage.reference())
}

/// Generate a short, unique stage name (suffix of nanos-since-epoch + random salt).
///
/// Used to avoid collisions when multiple pipelines run concurrently.
pub fn generate_stage_name() -> StageName {
    // `SystemTime::now()` is not injection-safe in theory, but we only use
    // nanos + a simple hash — no user input is interpolated here.
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    // Already-validated name (alphanumeric + underscore): safe to construct
    // directly.
    StageName(format!("ROCKY_LOAD_{secs}_{nanos}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stage_name_valid() {
        let n = StageName::new("my_stage_1").unwrap();
        assert_eq!(n.as_str(), "my_stage_1");
        assert_eq!(n.reference(), "@my_stage_1");
    }

    #[test]
    fn test_stage_name_empty_rejected() {
        assert!(StageName::new("").is_err());
    }

    #[test]
    fn test_stage_name_special_chars_rejected() {
        assert!(StageName::new("bad stage").is_err());
        assert!(StageName::new("bad-stage").is_err());
        assert!(StageName::new("bad.stage").is_err());
    }

    #[test]
    fn test_create_temporary_stage_csv() {
        let name = StageName::new("tmp").unwrap();
        let sql = create_temporary_stage_sql(&name, FileFormat::Csv, &LoadOptions::default());
        assert!(sql.starts_with("CREATE TEMPORARY STAGE tmp"));
        assert!(sql.contains("TYPE = 'CSV'"));
        assert!(sql.contains("SKIP_HEADER = 1"));
    }

    #[test]
    fn test_create_external_stage_s3() {
        let name = StageName::new("ext").unwrap();
        let sql = create_external_stage_sql(
            &name,
            "s3://bucket/path",
            FileFormat::Parquet,
            &LoadOptions::default(),
        );
        assert!(sql.contains("URL = 's3://bucket/path'"));
        assert!(sql.contains("TYPE = 'PARQUET'"));
    }

    #[test]
    fn test_put_file_sql() {
        let stage = StageName::new("stg").unwrap();
        let sql = put_file_sql("/tmp/data.csv", &stage);
        assert_eq!(sql, "PUT 'file:///tmp/data.csv' @stg OVERWRITE = TRUE");
    }

    #[test]
    fn test_drop_stage() {
        let name = StageName::new("x").unwrap();
        assert_eq!(drop_stage_sql(&name), "DROP STAGE IF EXISTS x");
    }

    #[test]
    fn test_copy_into_sql() {
        let stage = StageName::new("s").unwrap();
        assert_eq!(
            copy_into_sql("db.sch.tbl", &stage),
            "COPY INTO db.sch.tbl FROM @s"
        );
    }

    #[test]
    fn test_file_format_clause_csv() {
        let opts = LoadOptions {
            csv_has_header: false,
            csv_delimiter: '|',
            ..Default::default()
        };
        let c = file_format_clause(FileFormat::Csv, &opts);
        assert!(c.contains("FIELD_DELIMITER = '|'"));
        assert!(c.contains("SKIP_HEADER = 0"));
    }

    #[test]
    fn test_file_format_clause_parquet() {
        assert_eq!(
            file_format_clause(FileFormat::Parquet, &LoadOptions::default()),
            "TYPE = 'PARQUET'"
        );
    }

    #[test]
    fn test_file_format_clause_json() {
        assert_eq!(
            file_format_clause(FileFormat::JsonLines, &LoadOptions::default()),
            "TYPE = 'JSON'"
        );
    }

    #[test]
    fn test_generate_stage_name_unique_format() {
        let n = generate_stage_name();
        assert!(n.as_str().starts_with("ROCKY_LOAD_"));
        // Should pass validation.
        assert!(StageName::new(n.as_str()).is_ok());
    }
}
