//! Adapter traits for Rocky's pluggable architecture.
//!
//! Rocky separates concerns through adapter traits:
//! - [`DiscoveryAdapter`] — finds what schemas/tables are available (metadata only)
//! - [`WarehouseAdapter`] — executes SQL against a warehouse
//! - [`SqlDialect`] — generates warehouse-specific SQL syntax
//! - [`GovernanceAdapter`] — manages catalog/schema lifecycle, tags, permissions, isolation
//! - [`BatchCheckAdapter`] — executes batched data quality checks
//!
//! rocky-core defines the traits; adapter crates (rocky-databricks, rocky-fivetran, etc.)
//! provide implementations.

use std::collections::BTreeMap;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::ir::{ColumnInfo, ColumnSelection, Grant, GrantTarget, MetadataColumn, TableRef};
use crate::source::DiscoveredConnector;

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Adapter methods return this to avoid rocky-core depending on adapter error types.
pub type AdapterResult<T> = Result<T, AdapterError>;

/// Boxed error from an adapter operation.
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
// Query result
// ---------------------------------------------------------------------------

/// Row-based query result from a warehouse.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
}

/// Result of an EXPLAIN or dry-run cost estimate for a SQL statement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExplainResult {
    /// Estimated bytes that would be scanned.
    pub estimated_bytes_scanned: Option<u64>,
    /// Estimated number of rows processed.
    pub estimated_rows: Option<u64>,
    /// Estimated compute cost in the warehouse's native unit (e.g., DBU-seconds).
    pub estimated_compute_units: Option<f64>,
    /// Raw EXPLAIN output from the warehouse (for human inspection).
    pub raw_explain: String,
}

// ---------------------------------------------------------------------------
// Discovery
// ---------------------------------------------------------------------------

/// Discovers what schemas and tables are available from a source system.
///
/// This is a metadata-only operation — it identifies what exists,
/// it does not extract or move data.
#[async_trait]
pub trait DiscoveryAdapter: Send + Sync {
    /// Discover connectors/schemas matching the given prefix.
    async fn discover(&self, schema_prefix: &str) -> AdapterResult<Vec<DiscoveredConnector>>;

    /// Cheap connectivity check for the discovery API.
    ///
    /// Used by `rocky doctor --check auth`. Default: calls `discover("")`
    /// (empty prefix) and discards the result. Override with a cheaper
    /// API call if available (e.g., Fivetran `GET /v1/users/me`).
    async fn ping(&self) -> AdapterResult<()> {
        self.discover("").await.map(|_| ())
    }
}

// ---------------------------------------------------------------------------
// Warehouse
// ---------------------------------------------------------------------------

/// Executes SQL against a warehouse and provides dialect information.
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
    ///
    /// Per-warehouse expectations:
    /// - **Databricks:** `EXPLAIN FORMATTED` or `EXPLAIN COST`.
    /// - **Snowflake:** `EXPLAIN USING JSON`.
    /// - **BigQuery:** `jobs.query(dryRun=true)`.
    /// - **DuckDB:** `EXPLAIN ANALYZE`.
    async fn explain(&self, _sql: &str) -> AdapterResult<ExplainResult> {
        Err(AdapterError::msg("explain not supported by this adapter"))
    }

    /// Whether this adapter is experimental.
    ///
    /// Experimental adapters may have incomplete feature coverage or
    /// behavioral differences compared to production-ready adapters
    /// (Databricks, Snowflake). The runtime logs a warning at startup
    /// when an experimental adapter is selected.
    fn is_experimental(&self) -> bool {
        false
    }

    /// List table names in a given catalog + schema.
    ///
    /// Used by `rocky discover` to verify which tables actually exist
    /// in the source warehouse (table-existence filtering). Table names
    /// are returned lowercase for case-insensitive matching.
    ///
    /// Default: returns an error indicating the adapter doesn't support
    /// listing. Callers handle this gracefully (include all discovered
    /// tables without filtering and log a warning).
    async fn list_tables(&self, _catalog: &str, _schema: &str) -> AdapterResult<Vec<String>> {
        Err(AdapterError::msg(
            "list_tables not supported by this adapter",
        ))
    }
}

// ---------------------------------------------------------------------------
// SQL Dialect
// ---------------------------------------------------------------------------

/// Generates warehouse-specific SQL syntax.
///
/// Each warehouse adapter provides a dialect implementation. rocky-core's
/// SQL generation functions use this trait instead of hardcoding SQL patterns.
pub trait SqlDialect: Send + Sync {
    /// Format a fully qualified table reference.
    /// Databricks: `catalog.schema.table` (three-part)
    /// DuckDB/Postgres: `schema.table` (two-part)
    fn format_table_ref(&self, catalog: &str, schema: &str, table: &str) -> AdapterResult<String>;

    /// CREATE TABLE AS SELECT (full refresh).
    /// Databricks: `CREATE OR REPLACE TABLE {target} AS {select}`
    fn create_table_as(&self, target: &str, select_sql: &str) -> String;

    /// INSERT INTO ... SELECT (incremental append).
    fn insert_into(&self, target: &str, select_sql: &str) -> String;

    /// MERGE INTO (upsert by key).
    fn merge_into(
        &self,
        target: &str,
        source_sql: &str,
        keys: &[String],
        update_cols: &ColumnSelection,
    ) -> AdapterResult<String>;

    /// SELECT clause with optional metadata columns appended.
    /// Returns the SELECT portion only (no FROM/WHERE).
    fn select_clause(
        &self,
        columns: &ColumnSelection,
        metadata: &[MetadataColumn],
    ) -> AdapterResult<String>;

    /// WHERE clause for incremental watermark filtering.
    /// Returns the full WHERE clause including the keyword.
    fn watermark_where(&self, timestamp_col: &str, target_ref: &str) -> AdapterResult<String>;

    /// SQL to describe a table's schema (column names and types).
    fn describe_table_sql(&self, table_ref: &str) -> String;

    /// DROP TABLE IF EXISTS.
    fn drop_table_sql(&self, table_ref: &str) -> String;

    /// CREATE CATALOG IF NOT EXISTS. Returns None if the warehouse
    /// doesn't support catalogs (e.g., DuckDB, Postgres).
    fn create_catalog_sql(&self, name: &str) -> Option<AdapterResult<String>>;

    /// CREATE SCHEMA IF NOT EXISTS.
    fn create_schema_sql(&self, catalog: &str, schema: &str) -> Option<AdapterResult<String>>;

    /// TABLESAMPLE clause for null rate checks.
    /// Returns None if the warehouse doesn't support sampling.
    /// Databricks: `TABLESAMPLE (10 PERCENT)`
    /// DuckDB: `USING SAMPLE 10 PERCENT (bernoulli)`
    fn tablesample_clause(&self, percent: u32) -> Option<String>;

    /// Generate one or more SQL statements that atomically replace the rows
    /// in `target` matching `partition_filter` with the result of `select_sql`.
    ///
    /// Used by the `time_interval` materialization strategy. The returned
    /// statements are executed in order by the runtime; the implementation is
    /// responsible for atomicity (e.g., wrapping DELETE + INSERT in a
    /// transaction). Returns `Vec<String>` rather than `String` because some
    /// warehouse REST APIs (notably Snowflake) execute one statement per
    /// call — keeping the decomposition explicit lets the runtime handle
    /// each statement and roll back on partial failure.
    ///
    /// Per-warehouse expectations:
    /// - **Databricks (Delta):** single `INSERT INTO ... REPLACE WHERE ...`.
    /// - **Snowflake:** four-statement `BEGIN; DELETE; INSERT; COMMIT;`.
    /// - **DuckDB:** same shape as Snowflake.
    ///
    /// `partition_filter` is built by Rocky from a validated `time_column`
    /// and quoted timestamp literals — never user-supplied free text. Even so,
    /// implementations should not concatenate `partition_filter` with any
    /// other untrusted strings.
    fn insert_overwrite_partition(
        &self,
        target: &str,
        partition_filter: &str,
        select_sql: &str,
    ) -> AdapterResult<Vec<String>>;

    /// DELETE FROM ... WHERE ... for delete+insert strategy.
    /// Default implementation uses ANSI SQL.
    fn delete_where(&self, target: &str, where_clause: &str) -> String {
        format!("DELETE FROM {target} WHERE {where_clause}")
    }
}

// ---------------------------------------------------------------------------
// Governance
// ---------------------------------------------------------------------------

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

/// Optional governance capabilities for warehouses that support
/// catalog/schema lifecycle, tagging, and permission management.
///
/// Not all warehouses support governance — this trait is queried
/// via `Option<&dyn GovernanceAdapter>` at runtime.
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

    /// Bind a catalog to a specific workspace (for workspace isolation).
    /// `binding_type` is the Databricks API string, e.g. `"BINDING_TYPE_READ_WRITE"`.
    /// Not all warehouses support this; default returns Ok.
    async fn bind_workspace(
        &self,
        catalog: &str,
        workspace_id: u64,
        binding_type: &str,
    ) -> AdapterResult<()>;

    /// Set catalog isolation mode.
    /// Not all warehouses support this; default returns Ok.
    async fn set_isolation(&self, catalog: &str, enabled: bool) -> AdapterResult<()>;
}

/// No-op governance adapter for warehouses that don't support catalog
/// lifecycle, tagging, or permission management.
///
/// All operations silently succeed (return `Ok(())` or empty results).
/// `rocky run` uses this when the target warehouse doesn't have a
/// governance implementation, ensuring governance config blocks are
/// processed without errors — the operations are simply skipped.
///
/// ```rust
/// use rocky_core::traits::NoopGovernanceAdapter;
/// let gov = NoopGovernanceAdapter;
/// // All trait methods return Ok/empty — no warehouse calls made.
/// ```
pub struct NoopGovernanceAdapter;

#[async_trait]
impl GovernanceAdapter for NoopGovernanceAdapter {
    async fn set_tags(
        &self,
        _target: &TagTarget,
        _tags: &BTreeMap<String, String>,
    ) -> AdapterResult<()> {
        Ok(())
    }

    async fn get_grants(&self, _target: &GrantTarget) -> AdapterResult<Vec<Grant>> {
        Ok(vec![])
    }

    async fn apply_grants(&self, _grants: &[Grant]) -> AdapterResult<()> {
        Ok(())
    }

    async fn revoke_grants(&self, _grants: &[Grant]) -> AdapterResult<()> {
        Ok(())
    }

    async fn bind_workspace(
        &self,
        _catalog: &str,
        _workspace_id: u64,
        _binding_type: &str,
    ) -> AdapterResult<()> {
        Ok(())
    }

    async fn set_isolation(&self, _catalog: &str, _enabled: bool) -> AdapterResult<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Type mapping
// ---------------------------------------------------------------------------

/// Maps between warehouse-specific type strings and Rocky's unified type system.
///
/// Each warehouse adapter provides a `TypeMapper` implementation. The compiler
/// uses it to convert warehouse column types to `RockyType` for type checking.
///
/// The `RockyType` enum is defined in `rocky-compiler/src/types.rs`. This trait
/// uses string-based type names to avoid rocky-core depending on rocky-compiler.
pub trait TypeMapper: Send + Sync {
    /// Parse a warehouse-specific type string into a structured representation.
    /// Returns the type name normalized to uppercase, with parsed precision/scale
    /// for DECIMAL types.
    ///
    /// Examples: "STRING" → "STRING", "DECIMAL(10,2)" → "DECIMAL(10,2)",
    ///           "VARCHAR" → "VARCHAR"
    fn normalize_type(&self, warehouse_type: &str) -> String;

    /// Check if two warehouse type strings represent compatible types.
    fn types_compatible(&self, type_a: &str, type_b: &str) -> bool;
}

// ---------------------------------------------------------------------------
// Batch checks
// ---------------------------------------------------------------------------

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

/// Optional batch check execution for warehouses that support
/// batched queries (e.g., UNION ALL aggregation).
///
/// If not implemented, rocky-core falls back to sequential per-table checks.
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

    /// Describe all tables in a schema in a single batched query.
    ///
    /// Replaces N per-table `DESCRIBE TABLE` calls with a single
    /// `information_schema.columns` query when the warehouse supports it.
    /// Returns column metadata keyed by table name (lowercase).
    ///
    /// Default: returns a `not supported` error so callers fall back to the
    /// per-table [`WarehouseAdapter::describe_table`] path.
    async fn batch_describe_schema(
        &self,
        _catalog: &str,
        _schema: &str,
    ) -> AdapterResult<std::collections::HashMap<String, Vec<ColumnInfo>>> {
        Err(AdapterError::msg(
            "batch_describe_schema not supported by this adapter",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Verify that trait objects can be constructed (compile-time check).
    fn _assert_discovery_object_safe(_: &dyn DiscoveryAdapter) {}
    fn _assert_warehouse_object_safe(_: &dyn WarehouseAdapter) {}
    fn _assert_governance_object_safe(_: &dyn GovernanceAdapter) {}
    fn _assert_batch_check_object_safe(_: &dyn BatchCheckAdapter) {}
    // SqlDialect is not async, but still needs to be object-safe.
    fn _assert_dialect_object_safe(_: &dyn SqlDialect) {}
}
