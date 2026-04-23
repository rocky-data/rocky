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

use schemars::JsonSchema;

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

/// Statistics the warehouse reported for a single executed statement.
///
/// Returned by [`WarehouseAdapter::execute_statement_with_stats`]. Every
/// field is `Option<u64>` because not every adapter reports every number:
/// DuckDB returns `None` everywhere, BigQuery populates `bytes_scanned`
/// from `statistics.query.totalBytesBilled`, Databricks / Snowflake
/// currently return `None` but may populate these in a future wave.
///
/// Threaded up into [`crate::state::ModelExecution`] and
/// `MaterializationOutput.bytes_scanned` / `.bytes_written` so
/// `rocky cost` can compute real dollar figures without re-querying the
/// warehouse.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct ExecutionStats {
    /// Adapter-reported bytes figure used for cost accounting. This is
    /// the *billing-relevant* number per adapter, not literal scan
    /// volume — so anyone comparing this to a warehouse console should
    /// know which column lines up.
    ///
    /// - **BigQuery:** `totalBytesBilled` (with the 10 MB per-query
    ///   minimum floor already applied) — matches the BigQuery
    ///   console's "Bytes billed" field, **not** "Bytes processed".
    ///   Stored in this field because
    ///   [`crate::cost::compute_observed_cost_usd`] multiplies it by
    ///   the per-TB rate to compute the billed cost.
    /// - **Databricks:** when populated, the byte count from the
    ///   statement-execution manifest (`total_byte_count`); `None`
    ///   today until the manifest plumbing lands.
    /// - **Snowflake:** `None` — deferred by design
    ///   (QUERY_HISTORY round-trip cost; Snowflake cost is
    ///   duration × DBU, not bytes-driven).
    /// - **DuckDB:** `None` — no billed-bytes concept.
    pub bytes_scanned: Option<u64>,
    /// Adapter-reported bytes-written figure. Only populated by
    /// adapters that report it natively:
    ///
    /// - **BigQuery:** `None` — query jobs don't surface a
    ///   bytes-written figure.
    /// - **Databricks / Snowflake:** `None` today; statement results
    ///   expose a bytes-written figure but it's not wired yet.
    /// - **DuckDB:** `None`.
    ///
    /// Reserved so a future wave can populate it without a schema
    /// break.
    pub bytes_written: Option<u64>,
    /// Rows affected by the statement (inserts + updates + deletes).
    /// Not populated yet — reserved so a follow-up wave can thread
    /// per-adapter row counts through without another trait-method
    /// addition.
    pub rows_affected: Option<u64>,
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

    /// Execute a SQL statement and return the warehouse-reported
    /// [`ExecutionStats`] (bytes scanned, bytes written, rows affected).
    ///
    /// Default: delegates to [`Self::execute_statement`] and returns
    /// [`ExecutionStats::default`] (all `None`). Adapters that have
    /// access to statement-level stats (BigQuery's
    /// `statistics.query.totalBytesBilled`, Databricks's
    /// `result.manifest.total_byte_count`, Snowflake's
    /// `statistics.queryLoad`) should override to populate the fields.
    ///
    /// Used by the materialize path in `rocky-cli` to populate
    /// `MaterializationOutput.bytes_scanned` / `.bytes_written` so
    /// `rocky cost` can derive a real dollar figure for bytes-priced
    /// warehouses (BigQuery on-demand).
    async fn execute_statement_with_stats(&self, sql: &str) -> AdapterResult<ExecutionStats> {
        self.execute_statement(sql)
            .await
            .map(|()| ExecutionStats::default())
    }

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
    ///
    /// Keys are `&[Arc<str>]` so the ir-level `Vec<Arc<str>>` on
    /// `MaterializationStrategy::Merge::unique_key` threads through without
    /// per-call allocation (§P4.2). Dialect implementations dereference
    /// (`&**key`) to get a `&str` for validation / formatting.
    fn merge_into(
        &self,
        target: &str,
        source_sql: &str,
        keys: &[std::sync::Arc<str>],
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

    /// SQL query that returns one row per user table in a catalog/schema,
    /// with `table_name` as the first column.
    ///
    /// Used by the quality pipeline when a `[[tables]]` entry omits
    /// `table` — every table in the schema is then checked.
    ///
    /// Default implementation uses the ANSI catalog-prefixed
    /// `information_schema` form (`{catalog}.information_schema.tables`
    /// filtered by `table_schema`), which works for Databricks Unity
    /// Catalog, Snowflake, and BigQuery. Warehouses without a
    /// catalog-prefixed information schema (e.g. DuckDB) must override.
    ///
    /// `catalog` and `schema` are validated as SQL identifiers before
    /// interpolation; callers should pass the already-validated values.
    fn list_tables_sql(&self, catalog: &str, schema: &str) -> AdapterResult<String> {
        rocky_sql::validation::validate_identifier(catalog).map_err(AdapterError::new)?;
        rocky_sql::validation::validate_identifier(schema).map_err(AdapterError::new)?;
        Ok(format!(
            "SELECT table_name FROM {catalog}.information_schema.tables \
             WHERE table_schema = '{schema}'"
        ))
    }

    /// Build a dialect-specific boolean SQL predicate that matches when
    /// `column` matches `pattern` (`REGEXP` / `RLIKE` / `REGEXP_LIKE` /
    /// `REGEXP_CONTAINS`, depending on the warehouse).
    ///
    /// Used by `TestType::RegexMatch` (Phase 4a). The default impl
    /// returns an error — adapters that support regex must override.
    /// `column` is already validated as a SQL identifier; `pattern` is
    /// already validated against the strict allowlist
    /// ([`crate::tests::validate_regex_pattern`]).
    fn regex_match_predicate(&self, _column: &str, _pattern: &str) -> AdapterResult<String> {
        Err(AdapterError::msg(
            "regex_match not supported by this dialect",
        ))
    }

    /// Build a dialect-specific SQL expression equivalent to
    /// `CURRENT_DATE - N days`. Used by `TestType::OlderThanNDays`
    /// (Phase 4b). The default impl uses the ANSI
    /// `CURRENT_DATE - INTERVAL 'N' DAY` form, which works for
    /// Databricks, Snowflake, and DuckDB. BigQuery overrides because
    /// it requires `DATE_SUB(CURRENT_DATE(), INTERVAL N DAY)`.
    fn date_minus_days_expr(&self, days: u32) -> AdapterResult<String> {
        Ok(format!("CURRENT_DATE - INTERVAL '{days}' DAY"))
    }

    /// SQL expression returning the current timestamp. Used by
    /// `TestType::NotInFuture` (Phase 4b).
    ///
    /// Default: `CURRENT_TIMESTAMP` (ANSI keyword, no parens). Works for
    /// Databricks, Snowflake, DuckDB. BigQuery overrides to
    /// `CURRENT_TIMESTAMP()` because it requires the function form.
    fn current_timestamp_expr(&self) -> &'static str {
        "CURRENT_TIMESTAMP"
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

/// Column-masking strategy applied to a classified column at materialization
/// time.
///
/// Rocky translates a classification tag (e.g., `pii`) into one of these
/// strategies via the `[mask]` / `[mask.<env>]` block in `rocky.toml`. The
/// adapter renders each strategy as a warehouse-native function:
/// Databricks uses `CREATE MASK ... RETURN <expr>` + `SET MASKING POLICY`;
/// other adapters default-unsupported until demand.
///
/// Serialized in lowercase to match the TOML spelling (`"hash"`, `"redact"`,
/// `"partial"`, `"none"`).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "lowercase")]
pub enum MaskStrategy {
    /// SHA-256 hex digest of the column value. Deterministic, one-way.
    Hash,
    /// Replace the column value with the literal string `'***'`.
    Redact,
    /// Keep the first and last two characters; replace the middle with `***`.
    /// Short values (<5 chars) are fully replaced with `'***'`.
    Partial,
    /// Explicit identity — no masking applied. Useful as a per-env override
    /// to "unmask" a column that defaults to masked at the workspace level.
    None,
}

impl MaskStrategy {
    /// Wire name used in `rocky.toml` and JSON schemas.
    pub fn as_str(self) -> &'static str {
        match self {
            MaskStrategy::Hash => "hash",
            MaskStrategy::Redact => "redact",
            MaskStrategy::Partial => "partial",
            MaskStrategy::None => "none",
        }
    }
}

impl std::fmt::Display for MaskStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A concrete masking policy resolved for a specific environment.
///
/// The pipeline config supplies a default `[mask]` block keyed by
/// classification tag; each `[mask.<env>]` block overrides individual
/// strategies. At apply time Rocky resolves classifications → strategies
/// for the active environment and passes the result to the adapter as a
/// `MaskingPolicy`.
///
/// `column_strategies` maps **column name → strategy** (not tag → strategy);
/// the translation from classification tag to strategy happens above the
/// adapter layer so the adapter only needs to know what to apply to each
/// column.
#[derive(Debug, Clone, Default)]
pub struct MaskingPolicy {
    /// Column name → resolved [`MaskStrategy`] for that column.
    pub column_strategies: BTreeMap<String, MaskStrategy>,
}

impl MaskingPolicy {
    /// Returns `true` when no columns need masking (either no classifications
    /// matched or every resolved strategy is [`MaskStrategy::None`]).
    pub fn is_empty(&self) -> bool {
        self.column_strategies.is_empty()
            || self
                .column_strategies
                .values()
                .all(|s| *s == MaskStrategy::None)
    }
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

    /// List current workspace bindings for a catalog.
    ///
    /// Returns the tuples `(workspace_id, binding_type)` currently applied
    /// to the catalog. Used by reconciliation to diff desired-vs-current
    /// bindings alongside grants.
    ///
    /// # Errors
    ///
    /// The trait default returns an `AdapterError` so adapters that don't
    /// support workspace bindings must explicitly override and declare their
    /// semantics (either "silently Ok with `vec![]`" or "surface the gap").
    /// `NoopGovernanceAdapter`, `SnowflakeGovernanceAdapter`, and
    /// `BigQueryGovernanceAdapter` override to return `Ok(vec![])`; the
    /// Databricks adapter implements the Unity Catalog workspace-bindings API.
    async fn list_workspace_bindings(&self, _catalog: &str) -> AdapterResult<Vec<(u64, String)>> {
        Err(AdapterError::msg(
            "list_workspace_bindings not supported by this adapter",
        ))
    }

    /// Remove a workspace binding from a catalog.
    ///
    /// Used by reconciliation to drop bindings that exist on the catalog
    /// but aren't in the desired state.
    ///
    /// # Errors
    ///
    /// The trait default returns an `AdapterError`; adapters that don't
    /// support workspace bindings override to return `Ok(())` (silent
    /// success, matching `bind_workspace`/`set_isolation` semantics).
    async fn remove_workspace_binding(
        &self,
        _catalog: &str,
        _workspace_id: u64,
    ) -> AdapterResult<()> {
        Err(AdapterError::msg(
            "remove_workspace_binding not supported by this adapter",
        ))
    }

    /// Apply classification tags to individual columns of a table.
    ///
    /// `column_tags` maps **column name → (tag-key → tag-value)**. The outer
    /// key is the column, the inner map is an arbitrary set of tag pairs —
    /// `set_tags`' column-scoped cousin. Column-level tagging is a
    /// Databricks-specific Unity Catalog capability; other adapters
    /// override to return an `AdapterError` signalling the gap so the
    /// mismatch is visible at plan/run time rather than silently skipped.
    ///
    /// Rocky calls this after writing a model's bytes so the tags land on
    /// the freshly-materialized table — matching the `set_tags` timing.
    ///
    /// # Errors
    ///
    /// The trait default returns an `AdapterError` to force adapters to
    /// declare explicit semantics. `NoopGovernanceAdapter` overrides to
    /// `Ok(())` so pipelines that declare classifications against a
    /// no-governance warehouse degrade gracefully; Snowflake / BigQuery
    /// surface the gap by returning an error.
    async fn apply_column_tags(
        &self,
        _table: &TableRef,
        _column_tags: &BTreeMap<String, BTreeMap<String, String>>,
    ) -> AdapterResult<()> {
        Err(AdapterError::msg(
            "apply_column_tags not supported by this adapter",
        ))
    }

    /// Apply a column-level masking policy to a table.
    ///
    /// `policy.column_strategies` maps column name → [`MaskStrategy`].
    /// `env` is the currently-active environment name (e.g., `"dev"`,
    /// `"prod"`), which Rocky uses to disambiguate per-env policy names
    /// at the warehouse layer. Adapters must:
    ///
    /// 1. Generate one masking function per distinct strategy (idempotent
    ///    `CREATE OR REPLACE` — two models with the same strategy share
    ///    a function).
    /// 2. Bind each column to its function via the warehouse's
    ///    column-masking primitive (Databricks: `ALTER TABLE ... ALTER
    ///    COLUMN ... SET MASK ...`).
    /// 3. Leave columns with [`MaskStrategy::None`] untouched (or clear
    ///    any existing mask, at the adapter's discretion).
    ///
    /// # Errors
    ///
    /// The trait default returns an `AdapterError`. Only Databricks
    /// implements masking in v1 — Snowflake / BigQuery default-unsupported
    /// until Rocky gains native demand for those dialects.
    async fn apply_masking_policy(
        &self,
        _table: &TableRef,
        _policy: &MaskingPolicy,
        _env: &str,
    ) -> AdapterResult<()> {
        Err(AdapterError::msg(
            "apply_masking_policy not supported by this adapter",
        ))
    }

    /// Reconcile a flattened role graph against the warehouse's native
    /// role/group system.
    ///
    /// `roles` is the post-[`crate::role_graph::flatten_role_graph`]
    /// map: each [`crate::ir::ResolvedRole`] carries its own permissions
    /// plus every transitive ancestor's, deduped and sorted. Adapters
    /// translate this into warehouse-specific primitives — on
    /// Databricks / Unity Catalog that means a group per role
    /// (convention: `rocky_role_<name>`) plus GRANT statements bound to
    /// the group; on Snowflake it would be `CREATE ROLE` + per-role
    /// GRANTs.
    ///
    /// # Errors
    ///
    /// The trait default returns an `AdapterError` to match the other
    /// optional capabilities (`apply_column_tags`,
    /// `apply_masking_policy`). [`NoopGovernanceAdapter`] overrides to
    /// `Ok(())` so pipelines with a `[role.*]` block against a
    /// no-governance warehouse degrade gracefully — the resolver itself
    /// still catches cycles at config-load time, the adapter just skips
    /// the no-op application.
    async fn reconcile_role_graph(
        &self,
        _roles: &BTreeMap<String, crate::ir::ResolvedRole>,
    ) -> AdapterResult<()> {
        Err(AdapterError::msg(
            "reconcile_role_graph not supported by this adapter",
        ))
    }
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

    async fn list_workspace_bindings(&self, _catalog: &str) -> AdapterResult<Vec<(u64, String)>> {
        Ok(vec![])
    }

    async fn remove_workspace_binding(
        &self,
        _catalog: &str,
        _workspace_id: u64,
    ) -> AdapterResult<()> {
        Ok(())
    }

    async fn apply_column_tags(
        &self,
        _table: &TableRef,
        _column_tags: &BTreeMap<String, BTreeMap<String, String>>,
    ) -> AdapterResult<()> {
        // No-op governance treats classification as optional metadata:
        // pipelines that declare classifications against a warehouse
        // without governance degrade gracefully. Surfacing the gap is the
        // compiler's job (W004), not this adapter's.
        Ok(())
    }

    async fn apply_masking_policy(
        &self,
        _table: &TableRef,
        _policy: &MaskingPolicy,
        _env: &str,
    ) -> AdapterResult<()> {
        // Same rationale as `apply_column_tags`.
        Ok(())
    }

    async fn reconcile_role_graph(
        &self,
        _roles: &BTreeMap<String, crate::ir::ResolvedRole>,
    ) -> AdapterResult<()> {
        // Same rationale as `apply_column_tags` / `apply_masking_policy`:
        // a no-governance warehouse silently accepts role-graph config so
        // pipelines downgrade gracefully. Cycle / unknown-parent errors
        // still surface at config-load time via flatten_role_graph.
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

    // Default workspace-binding primitives error out so adapters that don't
    // support the concept must override with explicit semantics.
    struct MinimalGovernance;

    #[async_trait]
    impl GovernanceAdapter for MinimalGovernance {
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

    #[tokio::test]
    async fn trait_default_list_workspace_bindings_errors() {
        let err = MinimalGovernance
            .list_workspace_bindings("cat")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("list_workspace_bindings"));
    }

    #[tokio::test]
    async fn trait_default_remove_workspace_binding_errors() {
        let err = MinimalGovernance
            .remove_workspace_binding("cat", 123)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("remove_workspace_binding"));
    }

    #[tokio::test]
    async fn noop_governance_overrides_workspace_bindings() {
        let noop = NoopGovernanceAdapter;
        assert!(
            noop.list_workspace_bindings("cat")
                .await
                .unwrap()
                .is_empty()
        );
        assert!(noop.remove_workspace_binding("cat", 123).await.is_ok());
    }

    // Default classification / masking primitives: same "must override"
    // contract as the workspace-binding primitives — adapters must declare
    // whether they support the capability.

    #[tokio::test]
    async fn trait_default_apply_column_tags_errors() {
        let t = TableRef {
            catalog: "c".into(),
            schema: "s".into(),
            table: "t".into(),
        };
        let err = MinimalGovernance
            .apply_column_tags(&t, &BTreeMap::new())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("apply_column_tags"));
    }

    #[tokio::test]
    async fn trait_default_apply_masking_policy_errors() {
        let t = TableRef {
            catalog: "c".into(),
            schema: "s".into(),
            table: "t".into(),
        };
        let err = MinimalGovernance
            .apply_masking_policy(&t, &MaskingPolicy::default(), "prod")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("apply_masking_policy"));
    }

    #[tokio::test]
    async fn noop_governance_accepts_classification_and_masking() {
        let noop = NoopGovernanceAdapter;
        let t = TableRef {
            catalog: "c".into(),
            schema: "s".into(),
            table: "t".into(),
        };
        assert!(noop.apply_column_tags(&t, &BTreeMap::new()).await.is_ok());
        assert!(
            noop.apply_masking_policy(&t, &MaskingPolicy::default(), "prod")
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn trait_default_reconcile_role_graph_errors() {
        let err = MinimalGovernance
            .reconcile_role_graph(&BTreeMap::new())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("reconcile_role_graph"));
    }

    #[tokio::test]
    async fn noop_governance_accepts_role_graph() {
        let noop = NoopGovernanceAdapter;
        assert!(noop.reconcile_role_graph(&BTreeMap::new()).await.is_ok());
    }

    #[test]
    fn masking_policy_is_empty() {
        let empty = MaskingPolicy::default();
        assert!(empty.is_empty());

        let mut all_none = MaskingPolicy::default();
        all_none
            .column_strategies
            .insert("email".into(), MaskStrategy::None);
        all_none
            .column_strategies
            .insert("phone".into(), MaskStrategy::None);
        assert!(
            all_none.is_empty(),
            "all-None policies are effectively empty"
        );

        let mut with_hash = MaskingPolicy::default();
        with_hash
            .column_strategies
            .insert("email".into(), MaskStrategy::Hash);
        assert!(!with_hash.is_empty());
    }

    #[test]
    fn mask_strategy_serde_roundtrip() {
        for (strat, wire) in [
            (MaskStrategy::Hash, "\"hash\""),
            (MaskStrategy::Redact, "\"redact\""),
            (MaskStrategy::Partial, "\"partial\""),
            (MaskStrategy::None, "\"none\""),
        ] {
            let s = serde_json::to_string(&strat).unwrap();
            assert_eq!(s, wire);
            let parsed: MaskStrategy = serde_json::from_str(wire).unwrap();
            assert_eq!(parsed, strat);
        }
    }
}
