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
//! - [`AuthProvider`] — render auth + mandatory headers for HTTP-based adapters
//!
//! # Method-surface parity with `rocky-core`
//!
//! The in-tree `rocky-core::traits::WarehouseAdapter` accumulates capabilities
//! that out-of-tree adapters also need (`ping`, `explain`, `list_tables`,
//! `execute_statement_with_stats`, etc.). This crate mirrors the additive
//! subset so SDK-only implementors have the same method surface available.
//! All mirrored methods have default impls — adapters opt in by overriding.
//!
//! Known non-additive divergences (defer to a follow-up — they require
//! cross-crate type or signature changes that this PR deliberately doesn't
//! land):
//!
//! - `clone_table_for_branch` (depends on `rocky-sql::validation` — pulling
//!   it into the SDK would expand the dep graph beyond the "thin contract"
//!   the SDK promises).
//! - `fetch_arrow_batch` (workspace `arrow` dep + active sibling rollout
//!   across in-tree adapters; the SDK trait will absorb it once that
//!   stabilizes).
//! - `SqlDialect::merge_into` signature mismatch (`&[Arc<str>]` +
//!   `ColumnSelection` in `rocky-core` vs `&[String]` + `Option<&[String]>`
//!   here). Aligning means changing one side; deferred so the sibling
//!   adapter rollout doesn't collide.
//! - `TableRef` / `ColumnInfo` / `Grant` / `MetadataColumn` are duplicated
//!   between the SDK and `rocky-ir`; the registry holds adapters as
//!   `Arc<dyn rocky_core::traits::WarehouseAdapter>` which is keyed off
//!   the `rocky-ir` types, so SDK-only adapter trait objects can't yet
//!   slot in. Unifying these types is the prerequisite for true
//!   process-adapter pluggability.

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

    /// Borrow the boxed inner error for downcasting.
    ///
    /// `AdapterError` deliberately stores the concrete adapter error
    /// type-erased so the SDK doesn't depend on any adapter crate. The
    /// `Error::source()` impl walks past `inner` (it returns
    /// `inner.source()`), so chain-walking consumers never see the
    /// concrete `ConnectorError` — they jump straight to whatever the
    /// `ConnectorError` carries underneath. This accessor lets callers
    /// who hold the boxed wrapper attempt a `downcast_ref::<T>()`
    /// against the wrapped error directly.
    pub fn inner(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
        self.inner.as_ref()
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

/// Result of an EXPLAIN or dry-run cost estimate for a SQL statement.
///
/// Returned by [`WarehouseAdapter::explain`]. Every field is optional
/// because not every warehouse reports every number — adapters populate
/// the slots their native EXPLAIN / dry-run path exposes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExplainResult {
    /// Estimated bytes that would be scanned.
    pub estimated_bytes_scanned: Option<u64>,
    /// Estimated number of rows processed.
    pub estimated_rows: Option<u64>,
    /// Estimated compute cost in the warehouse's native unit
    /// (e.g., DBU-seconds).
    pub estimated_compute_units: Option<f64>,
    /// Raw EXPLAIN output from the warehouse (for human inspection).
    pub raw_explain: String,
}

/// Statistics the warehouse reported for a single executed statement.
///
/// Returned by [`WarehouseAdapter::execute_statement_with_stats`]. Every
/// field is `Option<u64>` because not every adapter reports every number.
/// The host runtime threads populated fields up into
/// `MaterializationOutput.bytes_scanned` / `.bytes_written` so cost-aware
/// commands (`rocky cost`) can compute dollar figures without re-querying
/// the warehouse.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ExecutionStats {
    /// Adapter-reported bytes figure used for cost accounting.
    pub bytes_scanned: Option<u64>,
    /// Adapter-reported bytes-written figure (only populated by adapters
    /// that expose it natively).
    pub bytes_written: Option<u64>,
    /// Rows affected by the statement (inserts + updates + deletes).
    pub rows_affected: Option<u64>,
    /// Warehouse-specific job identifier for this statement, when one
    /// exists.
    pub job_id: Option<String>,
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

/// A source the discovery adapter attempted to fetch metadata for and failed.
///
/// Distinct from "removed upstream" — distinguishing the two is the contract
/// that lets downstream consumers safely diff one discover result against
/// the next without misinterpreting a transient fetch failure as a deletion
/// (FR-014).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailedSource {
    pub id: String,
    pub schema: String,
    pub source_type: String,
    pub error_class: FailedSourceErrorClass,
    pub message: String,
}

/// Coarse classification of why a discovery fetch failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailedSourceErrorClass {
    Transient,
    Timeout,
    RateLimit,
    Auth,
    Unknown,
}

/// Result of a discovery operation: successful connectors plus any sources
/// the adapter attempted to fetch metadata for and failed on.
///
/// Implementors of [`DiscoveryAdapter`] MUST surface per-source failures via
/// `failed` instead of silently dropping them — this is what lets downstream
/// consumers distinguish "fetch failed transiently" from "removed upstream"
/// when diffing snapshots.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryResult {
    pub connectors: Vec<DiscoveredConnector>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub failed: Vec<FailedSource>,
}

impl DiscoveryResult {
    /// Build a clean result with no failures — convenience for adapters
    /// whose discovery surface is single-shot (no per-source fan-out).
    pub fn ok(connectors: Vec<DiscoveredConnector>) -> Self {
        Self {
            connectors,
            failed: Vec::new(),
        }
    }
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
// Bisection diff
// ---------------------------------------------------------------------------

/// One contiguous range of primary-key values, identifying a chunk in a
/// checksum-bisection diff (see [`WarehouseAdapter::checksum_chunks`]).
///
/// Variants correspond one-to-one with [`SplitStrategy`]:
/// - `IntRange` for declared integer / numeric primary keys.
/// - `Composite` for multi-column primary keys; lo/hi are tuple
///   boundaries pre-computed by the caller.
/// - `HashBucket` for opaque-string / UUID primary keys; chunk identity
///   is `hash(pk) % modulus == residue`. Single-level by construction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PkRange {
    IntRange {
        lo: i128,
        hi: i128,
    },
    Composite {
        lo: Vec<serde_json::Value>,
        hi: Vec<serde_json::Value>,
    },
    HashBucket {
        modulus: u32,
        residue: u32,
    },
}

/// One chunk's checksum result returned from
/// [`WarehouseAdapter::checksum_chunks`].
///
/// Empty chunks (zero rows on this side) MAY be omitted by the
/// implementation; callers MUST index by `chunk_id`, not by vector
/// position. A `chunk_id` absent from the returned vector is equivalent
/// to `ChunkChecksum { chunk_id, row_count: 0, checksum: 0 }`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkChecksum {
    pub chunk_id: u32,
    pub row_count: u64,
    /// XOR-aggregated row hashes, widened to `u128` so the largest
    /// native adapter hash output (Snowflake `NUMBER(38,0)`) fits without
    /// truncation.
    pub checksum: u128,
}

/// How the bisection runner split the primary-key space into chunks.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SplitStrategy {
    IntRange,
    Composite,
    HashBucket,
    FirstColumn,
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

    /// Execute a SQL statement and return the warehouse-reported
    /// [`ExecutionStats`] (bytes scanned, bytes written, rows affected,
    /// job id).
    ///
    /// Default: delegates to [`Self::execute_statement`] and returns
    /// [`ExecutionStats::default`] (all `None`). Adapters with access to
    /// statement-level stats (BigQuery's `statistics.query.totalBytesBilled`,
    /// Databricks's `result.manifest.total_byte_count`, etc.) should
    /// override to populate the fields.
    async fn execute_statement_with_stats(&self, sql: &str) -> AdapterResult<ExecutionStats> {
        self.execute_statement(sql)
            .await
            .map(|()| ExecutionStats::default())
    }

    /// Cheap connectivity check. Exercises the auth path and verifies
    /// the warehouse is reachable without doing real work.
    ///
    /// Used by `rocky doctor --check auth`. Default: `SELECT 1`.
    /// Adapters with cheaper ping paths can override.
    async fn ping(&self) -> AdapterResult<()> {
        self.execute_statement("SELECT 1").await
    }

    /// Run EXPLAIN or a dry-run cost estimate for a SQL statement.
    ///
    /// Returns cost metadata (bytes scanned, rows, compute units) and
    /// the raw EXPLAIN output. Not all warehouses support this — the
    /// default returns an error.
    async fn explain(&self, _sql: &str) -> AdapterResult<ExplainResult> {
        Err(AdapterError::not_supported("explain"))
    }

    /// Whether this adapter is experimental.
    ///
    /// Experimental adapters may have incomplete feature coverage or
    /// behavioral differences compared to production-ready adapters.
    /// The runtime logs a warning at startup when an experimental
    /// adapter is selected.
    fn is_experimental(&self) -> bool {
        false
    }

    /// Compute warehouse name, if this adapter has one.
    ///
    /// Used by SQL generators that emit `WAREHOUSE = …` clauses
    /// (currently only Snowflake's `CREATE DYNAMIC TABLE`). Returns
    /// `None` for adapters without a compute-warehouse concept
    /// (Databricks routes to a configured SQL endpoint, BigQuery is
    /// serverless, DuckDB is in-process).
    fn warehouse_name(&self) -> Option<&str> {
        None
    }

    /// List table names in a given catalog + schema.
    ///
    /// Used by `rocky discover` to verify which tables actually exist
    /// in the source warehouse. Table names should be returned lowercase
    /// for case-insensitive matching.
    ///
    /// Default: returns an error indicating the adapter doesn't support
    /// listing. Callers should handle the error gracefully (include all
    /// discovered tables without filtering and log a warning).
    async fn list_tables(&self, _catalog: &str, _schema: &str) -> AdapterResult<Vec<String>> {
        Err(AdapterError::not_supported("list_tables"))
    }

    /// Compute checksums for a batch of chunks defined by primary-key
    /// ranges. Used by Rocky's checksum-bisection diff to compare two
    /// tables row-by-row without scanning either side end-to-end.
    ///
    /// **Returns one entry per non-empty chunk.** Chunks with zero rows
    /// on this side MAY be omitted; callers MUST index by
    /// [`ChunkChecksum::chunk_id`], not by vector position. A `chunk_id`
    /// absent from the returned vector is treated as
    /// `ChunkChecksum { chunk_id, row_count: 0, checksum: 0 }`.
    ///
    /// Default impl returns "not supported" — out-of-tree adapters must
    /// override to support `--algorithm=bisection`. Adapters compose the
    /// query around their native hash and bucketing primitives (DuckDB
    /// `hash()` + `width_bucket`, Spark `xxhash64`, Snowflake `HASH` +
    /// `BITXOR_AGG`, BigQuery `FARM_FINGERPRINT`); the in-tree
    /// equivalents live in `rocky-core` and ship with overrides per
    /// adapter.
    async fn checksum_chunks(
        &self,
        _table: &TableRef,
        _pk_column: &str,
        _value_columns: &[String],
        _pk_ranges: &[PkRange],
    ) -> AdapterResult<Vec<ChunkChecksum>> {
        Err(AdapterError::not_supported("checksum_chunks"))
    }

    /// Adapter-tuned override for the bisection leaf-row threshold (the
    /// `MIN_CHUNK_ROWS` knob). Below this row count the runner stops
    /// splitting and materializes the chunk for row-by-row diff.
    ///
    /// Defaults to `1000`, which lands break-even on in-process engines.
    /// Adapters with high per-query setup cost (Databricks, Snowflake,
    /// BigQuery) should override to a higher value.
    fn recommended_leaf_size(&self) -> u64 {
        1000
    }

    /// Adapter-tuned override for the row-count threshold above which
    /// UUID / hash-bucket-PK tables auto-fall-back from bisection to
    /// sampled. Defaults to `10_000_000`.
    fn recommended_uuid_threshold(&self) -> u64 {
        10_000_000
    }
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
    ///
    /// The watermark value is supplied as a literal `DateTime<Utc>` read
    /// from the host runtime's state store — the previous run's max source
    /// timestamp. The dialect formats it as a warehouse-native timestamp
    /// literal. `None` (first run or after `delete_watermark`) yields the
    /// `1970-01-01` sentinel so the whole source is scanned.
    fn watermark_where(
        &self,
        timestamp_col: &str,
        last_watermark: Option<&DateTime<Utc>>,
    ) -> AdapterResult<String>;

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
    ///
    /// Returns a [`DiscoveryResult`] with both successfully fetched
    /// `connectors` and any `failed` sources. Adapters MUST NOT silently
    /// drop a source on transient per-source failure — the distinction
    /// between "removed upstream" and "tried and failed" is the contract
    /// downstream consumers depend on (FR-014).
    async fn discover(&self, schema_prefix: &str) -> AdapterResult<DiscoveryResult>;
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
                path.rsplit('.')
                    .next()
                    .filter(|e| !e.is_empty() && *e != path)
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

    // ---------------------------------------------------------------
    // WarehouseAdapter additive-method defaults — these tests pin the
    // "out-of-tree adapters get rocky-core method parity for free"
    // contract. Each method has a sensible default so an adapter that
    // only implements the four required methods (dialect /
    // execute_statement / execute_query / describe_table / table_exists
    // / close) still composes with code that calls the additive set.
    // ---------------------------------------------------------------

    struct MinimalAdapter;
    struct StubDialect;

    impl SqlDialect for StubDialect {
        fn name(&self) -> &str {
            "stub"
        }
        fn format_table_ref(&self, _c: &str, _s: &str, _t: &str) -> AdapterResult<String> {
            Ok("stub".into())
        }
        fn create_table_as(&self, _t: &str, _s: &str) -> String {
            String::new()
        }
        fn insert_into(&self, _t: &str, _s: &str) -> String {
            String::new()
        }
        fn merge_into(
            &self,
            _t: &str,
            _s: &str,
            _k: &[String],
            _u: Option<&[String]>,
        ) -> AdapterResult<String> {
            Ok(String::new())
        }
        fn describe_table_sql(&self, _t: &str) -> String {
            String::new()
        }
        fn drop_table_sql(&self, _t: &str) -> String {
            String::new()
        }
        fn create_catalog_sql(&self, _n: &str) -> Option<AdapterResult<String>> {
            None
        }
        fn create_schema_sql(&self, _c: &str, _s: &str) -> Option<AdapterResult<String>> {
            None
        }
        fn row_hash_expr(&self, _c: &[String]) -> String {
            String::new()
        }
        fn tablesample_clause(&self, _p: u32) -> Option<String> {
            None
        }
        fn select_clause(
            &self,
            _c: &ColumnSelection,
            _m: &[MetadataColumn],
        ) -> AdapterResult<String> {
            Ok(String::new())
        }
        fn watermark_where(&self, _c: &str, _l: Option<&DateTime<Utc>>) -> AdapterResult<String> {
            Ok(String::new())
        }
        fn insert_overwrite_partition(
            &self,
            _t: &str,
            _f: &str,
            _s: &str,
        ) -> AdapterResult<Vec<String>> {
            Ok(vec![])
        }
    }

    #[async_trait]
    impl WarehouseAdapter for MinimalAdapter {
        fn dialect(&self) -> &dyn SqlDialect {
            &StubDialect
        }
        async fn execute_statement(&self, _sql: &str) -> AdapterResult<()> {
            Ok(())
        }
        async fn execute_query(&self, _sql: &str) -> AdapterResult<QueryResult> {
            Ok(QueryResult {
                columns: vec![],
                rows: vec![],
            })
        }
        async fn describe_table(&self, _t: &TableRef) -> AdapterResult<Vec<ColumnInfo>> {
            Ok(vec![])
        }
        async fn table_exists(&self, _t: &TableRef) -> AdapterResult<bool> {
            Ok(false)
        }
        async fn close(&self) -> AdapterResult<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn trait_default_execute_statement_with_stats_returns_default() {
        // Minimal adapters that don't override get an all-None stats
        // record — adapters opt in to populating fields by overriding.
        let stats = MinimalAdapter
            .execute_statement_with_stats("SELECT 1")
            .await
            .unwrap();
        assert!(stats.bytes_scanned.is_none());
        assert!(stats.bytes_written.is_none());
        assert!(stats.rows_affected.is_none());
        assert!(stats.job_id.is_none());
    }

    #[tokio::test]
    async fn trait_default_ping_delegates_to_select_one() {
        // The contract: `ping` exists with a sensible default so
        // `rocky doctor --check auth` works against every adapter
        // without per-adapter wiring.
        MinimalAdapter
            .ping()
            .await
            .expect("default ping is SELECT 1");
    }

    #[tokio::test]
    async fn trait_default_explain_errors() {
        // EXPLAIN is opt-in — adapters that don't override surface a
        // clear "not supported" rather than emitting bogus cost data.
        let err = MinimalAdapter.explain("SELECT 1").await.unwrap_err();
        assert!(err.to_string().contains("explain"));
    }

    #[tokio::test]
    async fn trait_default_list_tables_errors() {
        let err = MinimalAdapter.list_tables("c", "s").await.unwrap_err();
        assert!(err.to_string().contains("list_tables"));
    }

    #[test]
    fn trait_default_is_experimental_returns_false() {
        // Production-ready adapters get the right default automatically.
        assert!(!MinimalAdapter.is_experimental());
    }

    #[test]
    fn trait_default_warehouse_name_returns_none() {
        // Snowflake-style warehouses opt in by overriding; everything
        // else gets None — used by the dialect's `CREATE DYNAMIC TABLE`.
        assert!(MinimalAdapter.warehouse_name().is_none());
    }

    #[test]
    fn execution_stats_default_is_all_none() {
        let s = ExecutionStats::default();
        assert!(s.bytes_scanned.is_none());
        assert!(s.bytes_written.is_none());
        assert!(s.rows_affected.is_none());
        assert!(s.job_id.is_none());
    }

    #[test]
    fn explain_result_serializes_with_optional_fields() {
        // Mirrors the rocky-core shape so adapters porting an in-tree
        // EXPLAIN impl to an out-of-tree adapter don't need to reshape
        // the result type.
        let r = ExplainResult {
            estimated_bytes_scanned: Some(1024),
            estimated_rows: None,
            estimated_compute_units: Some(1.5),
            raw_explain: "PLAN".into(),
        };
        let json = serde_json::to_string(&r).unwrap();
        assert!(json.contains("\"estimated_bytes_scanned\":1024"));
        assert!(json.contains("\"raw_explain\":\"PLAN\""));
    }

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
