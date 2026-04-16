//! Stable adapter traits for Rocky's pluggable architecture.
//!
//! These traits define the public API contract between Rocky and warehouse adapters.
//! Once published at a given version, breaking changes require a minor version bump.
//!
//! Traits:
//! - [`WarehouseAdapter`] — execute SQL against a warehouse
//! - [`SqlDialect`] — generate warehouse-specific SQL syntax
//! - [`DiscoveryAdapter`] — discover available data sources
//! - [`GovernanceAdapter`] — manage catalog/schema lifecycle, tags, permissions
//! - [`BatchCheckAdapter`] — execute batched data quality checks
//! - [`TypeMapper`] — normalize warehouse type strings

use std::collections::BTreeMap;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Adapter methods return this to avoid the SDK depending on adapter error types.
pub type AdapterResult<T> = Result<T, AdapterError>;

/// Error from an adapter operation.
#[derive(Debug)]
pub struct AdapterError {
    inner: Box<dyn std::error::Error + Send + Sync>,
}

impl AdapterError {
    pub fn new(err: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self {
            inner: Box::new(err),
        }
    }

    pub fn msg(msg: impl Into<String>) -> Self {
        Self {
            inner: msg.into().into(),
        }
    }

    /// Create a "not supported" error for optional capabilities.
    pub fn not_supported(method: &str) -> Self {
        Self::msg(format!("{method} is not supported by this adapter"))
    }
}

impl std::fmt::Display for AdapterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner)
    }
}

impl std::error::Error for AdapterError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.inner.source()
    }
}

impl From<Box<dyn std::error::Error + Send + Sync>> for AdapterError {
    fn from(err: Box<dyn std::error::Error + Send + Sync>) -> Self {
        Self { inner: err }
    }
}

// ---------------------------------------------------------------------------
// Data types
// ---------------------------------------------------------------------------

/// Row-based query result from a warehouse.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
}

/// Three-part table reference.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TableRef {
    pub catalog: String,
    pub schema: String,
    pub table: String,
}

impl TableRef {
    /// Returns the fully-qualified three-part name (`catalog.schema.table`).
    pub fn full_name(&self) -> String {
        format!("{}.{}.{}", self.catalog, self.schema, self.table)
    }
}

impl std::fmt::Display for TableRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.catalog, self.schema, self.table)
    }
}

/// Column metadata from DESCRIBE TABLE.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

/// Which columns to select.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ColumnSelection {
    /// `SELECT *`
    All,
    /// `SELECT col1, col2, ...`
    Explicit(Vec<String>),
}

/// Extra columns to add during replication (e.g., `CAST(NULL AS STRING) AS _loaded_by`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataColumn {
    pub name: String,
    pub data_type: String,
    pub value: String,
}

/// A discovered connector from a source system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveredConnector {
    pub id: String,
    pub schema: String,
    pub source_type: String,
    pub last_sync_at: Option<DateTime<Utc>>,
    pub tables: Vec<DiscoveredTable>,
}

/// A discovered table within a connector.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveredTable {
    pub name: String,
    pub row_count: Option<u64>,
}

/// Target for a tag operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TagTarget {
    Catalog(String),
    Schema {
        catalog: String,
        schema: String,
    },
    Table {
        catalog: String,
        schema: String,
        table: String,
    },
}

/// A permission grant on a catalog or schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Grant {
    pub principal: String,
    pub permission: Permission,
    pub target: GrantTarget,
}

/// Permissions that Rocky manages.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Permission {
    Browse,
    UseCatalog,
    UseSchema,
    Select,
    Modify,
    Manage,
}

impl std::fmt::Display for Permission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Permission::Browse => write!(f, "BROWSE"),
            Permission::UseCatalog => write!(f, "USE CATALOG"),
            Permission::UseSchema => write!(f, "USE SCHEMA"),
            Permission::Select => write!(f, "SELECT"),
            Permission::Modify => write!(f, "MODIFY"),
            Permission::Manage => write!(f, "MANAGE"),
        }
    }
}

/// Target of a GRANT/REVOKE statement.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum GrantTarget {
    Catalog(String),
    Schema { catalog: String, schema: String },
}

/// Result of a batch row count query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowCountResult {
    pub table: TableRef,
    pub count: u64,
}

/// Result of a batch freshness query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FreshnessResult {
    pub table: TableRef,
    pub max_timestamp: Option<DateTime<Utc>>,
}

// ---------------------------------------------------------------------------
// Core trait: WarehouseAdapter
// ---------------------------------------------------------------------------

/// Execute SQL against a warehouse and provide dialect information.
///
/// This is the primary trait that all warehouse adapters must implement.
#[async_trait]
pub trait WarehouseAdapter: Send + Sync {
    /// Returns the SQL dialect for this warehouse.
    fn dialect(&self) -> &dyn SqlDialect;

    /// Execute a SQL statement (DDL/DML) without returning rows.
    async fn execute_statement(&self, sql: &str) -> AdapterResult<()>;

    /// Execute a SQL query and return rows.
    async fn execute_query(&self, sql: &str) -> AdapterResult<QueryResult>;

    /// Describe a table's columns (name, type, nullable).
    async fn describe_table(&self, table: &TableRef) -> AdapterResult<Vec<ColumnInfo>>;

    /// Check whether a table exists.
    async fn table_exists(&self, table: &TableRef) -> AdapterResult<bool>;

    /// Release any resources held by this adapter.
    async fn close(&self) -> AdapterResult<()>;
}

// ---------------------------------------------------------------------------
// SQL Dialect
// ---------------------------------------------------------------------------

/// Generate warehouse-specific SQL syntax.
///
/// Each warehouse adapter provides a dialect implementation. Rocky's SQL
/// generation functions use this trait instead of hardcoding SQL patterns.
pub trait SqlDialect: Send + Sync {
    /// Dialect name: "databricks", "snowflake", "duckdb", "bigquery", etc.
    fn name(&self) -> &str;

    /// Format a fully qualified table reference.
    /// Databricks: `catalog.schema.table` (three-part)
    /// DuckDB/Postgres: `schema.table` (two-part)
    fn format_table_ref(&self, catalog: &str, schema: &str, table: &str) -> AdapterResult<String>;

    /// CREATE TABLE AS SELECT (full refresh).
    fn create_table_as(&self, target: &str, select_sql: &str) -> String;

    /// INSERT INTO ... SELECT (incremental append).
    fn insert_into(&self, target: &str, select_sql: &str) -> String;

    /// MERGE INTO (upsert by key).
    fn merge_into(
        &self,
        target: &str,
        source_sql: &str,
        keys: &[String],
        update_cols: Option<&[String]>,
    ) -> AdapterResult<String>;

    /// SQL to describe a table's schema (column names and types).
    fn describe_table_sql(&self, table_ref: &str) -> String;

    /// DROP TABLE IF EXISTS.
    fn drop_table_sql(&self, table_ref: &str) -> String;

    /// CREATE CATALOG IF NOT EXISTS. Returns None if the warehouse
    /// doesn't support catalogs.
    fn create_catalog_sql(&self, name: &str) -> Option<AdapterResult<String>>;

    /// CREATE SCHEMA IF NOT EXISTS. Returns None if not supported.
    fn create_schema_sql(&self, catalog: &str, schema: &str) -> Option<AdapterResult<String>>;

    /// Expression to compute a row hash over the given columns.
    fn row_hash_expr(&self, columns: &[String]) -> String;

    /// TABLESAMPLE clause. Returns None if the warehouse doesn't support sampling.
    fn tablesample_clause(&self, percent: u32) -> Option<String>;

    /// SELECT clause with optional metadata columns appended.
    fn select_clause(
        &self,
        columns: &ColumnSelection,
        metadata: &[MetadataColumn],
    ) -> AdapterResult<String>;

    /// WHERE clause for incremental watermark filtering.
    fn watermark_where(&self, timestamp_col: &str, target_ref: &str) -> AdapterResult<String>;

    /// Generate one or more SQL statements that atomically replace the rows
    /// in `target` matching `partition_filter` with the result of `select_sql`.
    ///
    /// Used by the `time_interval` materialization strategy. The runtime
    /// executes the returned statements in order; on any failure mid-batch
    /// it issues `ROLLBACK`. Returns `Vec<String>` rather than `String`
    /// because some warehouse REST APIs (notably Snowflake) execute one
    /// statement per call.
    ///
    /// `partition_filter` is built by Rocky from a validated `time_column`
    /// and quoted timestamp literals — never user-supplied free text.
    ///
    /// Out-of-tree process adapters that don't yet support time_interval
    /// should return `AdapterError::msg("time_interval not supported")`
    /// rather than panic. The Rocky runtime surfaces the error with the
    /// adapter name in the diagnostic.
    fn insert_overwrite_partition(
        &self,
        target: &str,
        partition_filter: &str,
        select_sql: &str,
    ) -> AdapterResult<Vec<String>>;
}

// ---------------------------------------------------------------------------
// Discovery
// ---------------------------------------------------------------------------

/// Discover what schemas and tables are available from a source system.
///
/// This is a metadata-only operation -- it identifies what exists,
/// it does not extract or move data.
#[async_trait]
pub trait DiscoveryAdapter: Send + Sync {
    /// Discover connectors/schemas matching the given prefix.
    async fn discover(&self, schema_prefix: &str) -> AdapterResult<Vec<DiscoveredConnector>>;
}

// ---------------------------------------------------------------------------
// Governance
// ---------------------------------------------------------------------------

/// Manage catalog/schema lifecycle, tags, and permissions.
///
/// Not all warehouses support governance. This trait is optional.
#[async_trait]
pub trait GovernanceAdapter: Send + Sync {
    /// Apply tags to a catalog, schema, or table.
    async fn set_tags(
        &self,
        target: &TagTarget,
        tags: &BTreeMap<String, String>,
    ) -> AdapterResult<()>;

    /// Query current grants on a catalog or schema.
    async fn get_grants(&self, target: &GrantTarget) -> AdapterResult<Vec<Grant>>;

    /// Apply GRANT statements.
    async fn apply_grants(&self, grants: &[Grant]) -> AdapterResult<()>;

    /// Apply REVOKE statements.
    async fn revoke_grants(&self, grants: &[Grant]) -> AdapterResult<()>;
}

// ---------------------------------------------------------------------------
// Batch checks
// ---------------------------------------------------------------------------

/// Execute batched data quality checks.
///
/// If not implemented, Rocky falls back to sequential per-table checks.
#[async_trait]
pub trait BatchCheckAdapter: Send + Sync {
    /// Execute row count queries for multiple tables in a single batch.
    async fn batch_row_counts(&self, tables: &[TableRef]) -> AdapterResult<Vec<RowCountResult>>;

    /// Execute freshness queries for multiple tables in a single batch.
    async fn batch_freshness(
        &self,
        tables: &[TableRef],
        timestamp_col: &str,
    ) -> AdapterResult<Vec<FreshnessResult>>;
}

// ---------------------------------------------------------------------------
// Type mapping
// ---------------------------------------------------------------------------

/// Map between warehouse-specific type strings and Rocky's unified type system.
pub trait TypeMapper: Send + Sync {
    /// Normalize a warehouse-specific type string (e.g., "VARCHAR" -> "STRING").
    fn normalize_type(&self, warehouse_type: &str) -> String;

    /// Check if two warehouse type strings represent compatible types.
    fn types_compatible(&self, type_a: &str, type_b: &str) -> bool;
}

// ---------------------------------------------------------------------------
// File loading
// ---------------------------------------------------------------------------

/// Supported file formats for the load pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileFormat {
    /// Comma-separated values.
    Csv,
    /// Apache Parquet columnar format.
    Parquet,
    /// Newline-delimited JSON (one JSON object per line).
    JsonLines,
}

impl FileFormat {
    /// Returns the conventional file extension for this format (without leading dot).
    pub fn extension(&self) -> &'static str {
        match self {
            Self::Csv => "csv",
            Self::Parquet => "parquet",
            Self::JsonLines => "jsonl",
        }
    }

    /// Attempts to detect the format from a file extension.
    ///
    /// Returns `None` if the extension is not recognized.
    pub fn from_extension(ext: &str) -> Option<Self> {
        match ext.to_ascii_lowercase().as_str() {
            "csv" | "tsv" => Some(Self::Csv),
            "parquet" | "pq" => Some(Self::Parquet),
            "jsonl" | "ndjson" => Some(Self::JsonLines),
            _ => None,
        }
    }
}

impl std::fmt::Display for FileFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Csv => write!(f, "csv"),
            Self::Parquet => write!(f, "parquet"),
            Self::JsonLines => write!(f, "jsonlines"),
        }
    }
}

/// Options controlling how files are loaded into a target table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadOptions {
    /// Number of rows per INSERT batch. Default: 10,000.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Create the target table if it does not exist.
    #[serde(default = "default_true")]
    pub create_table: bool,

    /// Truncate the target table before loading.
    #[serde(default)]
    pub truncate_first: bool,

    /// Explicit file format. When `None`, the adapter should auto-detect
    /// from the file extension.
    #[serde(default)]
    pub format: Option<FileFormat>,

    /// CSV-specific: field delimiter character. Default: `,`.
    #[serde(default = "default_csv_delimiter")]
    pub csv_delimiter: char,

    /// CSV-specific: whether the first row is a header. Default: `true`.
    #[serde(default = "default_true")]
    pub csv_has_header: bool,
}

fn default_batch_size() -> usize {
    10_000
}

fn default_true() -> bool {
    true
}

fn default_csv_delimiter() -> char {
    ','
}

impl Default for LoadOptions {
    fn default() -> Self {
        Self {
            batch_size: default_batch_size(),
            create_table: true,
            truncate_first: false,
            format: None,
            csv_delimiter: default_csv_delimiter(),
            csv_has_header: true,
        }
    }
}

/// Result of a file load operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadResult {
    /// Number of rows successfully loaded.
    pub rows_loaded: u64,
    /// Number of bytes read from the source file.
    pub bytes_read: u64,
    /// Wall-clock duration of the load operation in milliseconds.
    pub duration_ms: u64,
}

/// Where data to be loaded resides.
///
/// Local files use [`LocalFile`](Self::LocalFile); cloud storage URIs
/// (e.g. `s3://bucket/path`, `gs://bucket/path`) use
/// [`CloudUri`](Self::CloudUri).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LoadSource {
    /// A file on the local filesystem.
    LocalFile(std::path::PathBuf),
    /// A URI pointing to a file in cloud storage (S3, GCS, ADLS).
    CloudUri(String),
}

impl LoadSource {
    /// Returns the file extension (without leading dot), if detectable.
    pub fn extension(&self) -> Option<&str> {
        match self {
            Self::LocalFile(p) => p.extension().and_then(|e| e.to_str()),
            Self::CloudUri(uri) => {
                let path = uri.rsplit('/').next().unwrap_or("");
                path.rsplit('.').next().filter(|e| !e.is_empty() && *e != path)
            }
        }
    }

    /// Returns a display-friendly string for logging and output.
    pub fn display_path(&self) -> String {
        match self {
            Self::LocalFile(p) => p.display().to_string(),
            Self::CloudUri(uri) => uri.clone(),
        }
    }
}

impl std::fmt::Display for LoadSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LocalFile(p) => write!(f, "{}", p.display()),
            Self::CloudUri(uri) => write!(f, "{uri}"),
        }
    }
}

/// Returns `true` if `s` starts with a recognized cloud storage URI scheme.
pub fn is_cloud_uri(s: &str) -> bool {
    s.starts_with("s3://")
        || s.starts_with("s3a://")
        || s.starts_with("gs://")
        || s.starts_with("az://")
        || s.starts_with("abfs://")
        || s.starts_with("abfss://")
}

/// Load data from files into a warehouse table.
///
/// This trait enables the `load` pipeline type, which ingests files
/// (CSV, Parquet, JSONL) into warehouse tables. Adapters that support file
/// loading implement this trait alongside [`WarehouseAdapter`].
///
/// Sources can be local files or cloud storage URIs — see [`LoadSource`].
#[async_trait]
pub trait LoaderAdapter: Send + Sync {
    /// Load a single source into a target table.
    ///
    /// The adapter should:
    /// 1. Detect or use the explicit format from `options.format`
    /// 2. Optionally create the target table if `options.create_table` is true
    /// 3. Optionally truncate the target table if `options.truncate_first` is true
    /// 4. Read the source and insert rows in batches of `options.batch_size`
    async fn load(
        &self,
        source: &LoadSource,
        target: &TableRef,
        options: &LoadOptions,
    ) -> AdapterResult<LoadResult>;

    /// Returns the file formats this adapter can load.
    fn supported_formats(&self) -> Vec<FileFormat>;
}

// ---------------------------------------------------------------------------
// Object safety checks
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // Verify that trait objects can be constructed (compile-time check).
    fn _assert_discovery_object_safe(_: &dyn DiscoveryAdapter) {}
    fn _assert_warehouse_object_safe(_: &dyn WarehouseAdapter) {}
    fn _assert_governance_object_safe(_: &dyn GovernanceAdapter) {}
    fn _assert_batch_check_object_safe(_: &dyn BatchCheckAdapter) {}
    fn _assert_dialect_object_safe(_: &dyn SqlDialect) {}
    fn _assert_loader_object_safe(_: &dyn LoaderAdapter) {}

    #[test]
    fn test_table_ref_display() {
        let r = TableRef {
            catalog: "cat".into(),
            schema: "sch".into(),
            table: "tbl".into(),
        };
        assert_eq!(r.to_string(), "cat.sch.tbl");
        assert_eq!(r.full_name(), "cat.sch.tbl");
    }

    #[test]
    fn test_adapter_error_msg() {
        let err = AdapterError::msg("something went wrong");
        assert_eq!(err.to_string(), "something went wrong");
    }

    #[test]
    fn test_adapter_error_not_supported() {
        let err = AdapterError::not_supported("set_tags");
        assert!(err.to_string().contains("not supported"));
    }

    #[test]
    fn test_permission_display() {
        assert_eq!(Permission::Browse.to_string(), "BROWSE");
        assert_eq!(Permission::UseCatalog.to_string(), "USE CATALOG");
        assert_eq!(Permission::UseSchema.to_string(), "USE SCHEMA");
        assert_eq!(Permission::Select.to_string(), "SELECT");
        assert_eq!(Permission::Modify.to_string(), "MODIFY");
        assert_eq!(Permission::Manage.to_string(), "MANAGE");
    }

    #[test]
    fn test_query_result_serialization() {
        let result = QueryResult {
            columns: vec!["id".into(), "name".into()],
            rows: vec![vec![
                serde_json::Value::Number(1.into()),
                serde_json::Value::String("test".into()),
            ]],
        };
        let json = serde_json::to_string(&result).unwrap();
        let deserialized: QueryResult = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.columns.len(), 2);
        assert_eq!(deserialized.rows.len(), 1);
    }

    // --- FileFormat tests ---

    #[test]
    fn test_file_format_extension() {
        assert_eq!(FileFormat::Csv.extension(), "csv");
        assert_eq!(FileFormat::Parquet.extension(), "parquet");
        assert_eq!(FileFormat::JsonLines.extension(), "jsonl");
    }

    #[test]
    fn test_file_format_from_extension() {
        assert_eq!(FileFormat::from_extension("csv"), Some(FileFormat::Csv));
        assert_eq!(FileFormat::from_extension("tsv"), Some(FileFormat::Csv));
        assert_eq!(FileFormat::from_extension("CSV"), Some(FileFormat::Csv));
        assert_eq!(
            FileFormat::from_extension("parquet"),
            Some(FileFormat::Parquet)
        );
        assert_eq!(FileFormat::from_extension("pq"), Some(FileFormat::Parquet));
        assert_eq!(
            FileFormat::from_extension("jsonl"),
            Some(FileFormat::JsonLines)
        );
        assert_eq!(
            FileFormat::from_extension("ndjson"),
            Some(FileFormat::JsonLines)
        );
        assert_eq!(FileFormat::from_extension("xlsx"), None);
        assert_eq!(FileFormat::from_extension(""), None);
    }

    #[test]
    fn test_file_format_display() {
        assert_eq!(FileFormat::Csv.to_string(), "csv");
        assert_eq!(FileFormat::Parquet.to_string(), "parquet");
        assert_eq!(FileFormat::JsonLines.to_string(), "jsonlines");
    }

    #[test]
    fn test_file_format_serialization_roundtrip() {
        let format = FileFormat::Parquet;
        let json = serde_json::to_string(&format).unwrap();
        assert_eq!(json, r#""parquet""#);
        let deserialized: FileFormat = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, FileFormat::Parquet);
    }

    #[test]
    fn test_load_options_defaults() {
        let opts = LoadOptions::default();
        assert_eq!(opts.batch_size, 10_000);
        assert!(opts.create_table);
        assert!(!opts.truncate_first);
        assert!(opts.format.is_none());
        assert_eq!(opts.csv_delimiter, ',');
        assert!(opts.csv_has_header);
    }

    #[test]
    fn test_load_options_serialization_roundtrip() {
        let opts = LoadOptions {
            batch_size: 5000,
            create_table: false,
            truncate_first: true,
            format: Some(FileFormat::Csv),
            csv_delimiter: '\t',
            csv_has_header: false,
        };
        let json = serde_json::to_string(&opts).unwrap();
        let deserialized: LoadOptions = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.batch_size, 5000);
        assert!(!deserialized.create_table);
        assert!(deserialized.truncate_first);
        assert_eq!(deserialized.format, Some(FileFormat::Csv));
        assert_eq!(deserialized.csv_delimiter, '\t');
        assert!(!deserialized.csv_has_header);
    }

    #[test]
    fn test_load_result_serialization() {
        let result = LoadResult {
            rows_loaded: 42_000,
            bytes_read: 1_024_000,
            duration_ms: 3500,
        };
        let json = serde_json::to_string(&result).unwrap();
        let deserialized: LoadResult = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.rows_loaded, 42_000);
        assert_eq!(deserialized.bytes_read, 1_024_000);
        assert_eq!(deserialized.duration_ms, 3500);
    }

    // --- LoadSource tests ---

    #[test]
    fn test_load_source_local_file_extension() {
        let src = LoadSource::LocalFile(std::path::PathBuf::from("/tmp/data.csv"));
        assert_eq!(src.extension(), Some("csv"));
    }

    #[test]
    fn test_load_source_cloud_uri_extension() {
        let src = LoadSource::CloudUri("s3://bucket/path/file.parquet".into());
        assert_eq!(src.extension(), Some("parquet"));
    }

    #[test]
    fn test_load_source_cloud_uri_no_extension() {
        let src = LoadSource::CloudUri("s3://bucket/path/file".into());
        assert_eq!(src.extension(), None);
    }

    #[test]
    fn test_load_source_display() {
        let local = LoadSource::LocalFile(std::path::PathBuf::from("/tmp/data.csv"));
        assert_eq!(local.to_string(), "/tmp/data.csv");

        let cloud = LoadSource::CloudUri("s3://bucket/data.csv".into());
        assert_eq!(cloud.to_string(), "s3://bucket/data.csv");
    }

    #[test]
    fn test_is_cloud_uri() {
        assert!(is_cloud_uri("s3://bucket/key"));
        assert!(is_cloud_uri("s3a://bucket/key"));
        assert!(is_cloud_uri("gs://bucket/key"));
        assert!(is_cloud_uri("az://container/blob"));
        assert!(is_cloud_uri("abfs://container@account/path"));
        assert!(is_cloud_uri("abfss://container@account/path"));
        assert!(!is_cloud_uri("/local/path"));
        assert!(!is_cloud_uri("relative/path"));
        assert!(!is_cloud_uri("http://example.com"));
    }
}
