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
use crate::retention::RetentionPolicy;
use crate::source::DiscoveryResult;

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
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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
    /// Warehouse-specific job identifier for this statement, when one
    /// exists. Threaded into `MaterializationOutput.job_ids` so callers
    /// can cross-check warehouse-side statistics against rocky's
    /// reported figures (e.g., comparing `bytes_scanned` to
    /// `bq show -j <id>`'s `totalBytesBilled`).
    ///
    /// - **BigQuery:** `jobReference.jobId` from the `jobs.query`
    ///   response.
    /// - **Databricks / Snowflake:** `None` today — both surface a
    ///   statement identifier in their REST responses; wiring is a
    ///   follow-up wave.
    /// - **DuckDB:** `None` — no job concept.
    pub job_id: Option<String>,
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
    ///
    /// Returns a [`DiscoveryResult`] with both successfully fetched
    /// `connectors` and any `failed` sources (transient API errors,
    /// rate-limit budget exhausted, auth blip). Adapters MUST NOT silently
    /// drop a source on transient per-source failure — the distinction
    /// between "removed upstream" and "tried and failed" is the contract
    /// downstream consumers depend on to avoid mistaking a fetch failure
    /// for a deletion (FR-014).
    async fn discover(&self, schema_prefix: &str) -> AdapterResult<DiscoveryResult>;

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
// Bisection diff
// ---------------------------------------------------------------------------

/// One contiguous range of primary-key values, identifying a chunk in a
/// checksum-bisection diff (see [`WarehouseAdapter::checksum_chunks`]).
///
/// Variants correspond one-to-one with [`SplitStrategy`]:
/// - `IntRange` for declared integer / numeric primary keys.
/// - `Composite` for multi-column primary keys; lo/hi are tuple boundaries.
///   Caller pre-computes boundaries (typically via a per-recursion-level
///   `NTILE` query on one side) so chunk identity stays stable across both
///   sides of the diff.
/// - `HashBucket` for opaque-string / UUID primary keys; chunk identity is
///   `hash(pk) % modulus == residue`. Single-level by construction —
///   recursion under hash bucketing isn't cost-bounded on unclustered
///   tables (every level requires a full re-scan to recompute the hash).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PkRange {
    /// `[lo, hi)` over a single integer/numeric primary-key column.
    IntRange { lo: i128, hi: i128 },
    /// `[lo, hi)` over a composite primary-key tuple. Boundaries are
    /// caller-provided as pre-computed `serde_json::Value` arrays so the
    /// adapter doesn't re-quantile per call.
    Composite {
        lo: Vec<serde_json::Value>,
        hi: Vec<serde_json::Value>,
    },
    /// `hash(pk) % modulus == residue` — single-level, no further recursion.
    HashBucket { modulus: u32, residue: u32 },
}

/// One chunk's checksum result returned from
/// [`WarehouseAdapter::checksum_chunks`].
///
/// Empty chunks (zero rows on this side) MAY be omitted by the
/// implementation; callers MUST index the result by `chunk_id`, not by
/// vector position. A `chunk_id` absent from the returned vector is
/// equivalent to `ChunkChecksum { chunk_id, row_count: 0, checksum: 0 }`.
///
/// This contract exists because `GROUP BY chunk_id` on every target
/// warehouse drops empty groups, and back-filling per-adapter is
/// error-prone. The trait places the alignment burden on the caller, which
/// already knows the full set of chunks it asked for.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkChecksum {
    /// Index into the `pk_ranges` slice the caller passed to
    /// [`WarehouseAdapter::checksum_chunks`].
    pub chunk_id: u32,
    /// Number of non-null primary-key rows that fell into this chunk.
    pub row_count: u64,
    /// XOR-aggregated row hashes, widened to `u128` so the largest native
    /// adapter hash output (Snowflake `NUMBER(38,0)`) fits without
    /// truncation. Adapters with smaller native widths (xxhash64,
    /// FARM_FINGERPRINT) zero-extend.
    pub checksum: u128,
}

/// How the bisection runner split the primary-key space into chunks.
///
/// Surfaced on the diff result (the `BisectionStats` in
/// `rocky_core::compare::bisection`) so downstream consumers know whether
/// chunk identity is a numeric range, a tuple boundary, a hash bucket, or
/// a non-unique-ordering fallback.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SplitStrategy {
    /// Declared integer / numeric primary key; arithmetic split.
    IntRange,
    /// Composite primary key; per-level NTILE boundaries on the base side.
    Composite,
    /// UUID / opaque string primary key; single-level hash bucketing.
    HashBucket,
    /// No declared primary key; fallback ordering by the first column.
    /// Bisection on this strategy is **not exhaustive** — multiple rows
    /// can share the same chunk identity and tie-breaks are warehouse-
    /// defined. Callers should treat the diff as best-effort and surface
    /// a coverage warning.
    FirstColumn,
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

    /// Materialize `source` into `branch_schema` under the same table name.
    ///
    /// Used by `rocky preview create` to populate the per-PR branch schema
    /// for every model not in the prune set, so the branch run only needs
    /// to re-execute the changed-and-downstream slice. The default impl
    /// is a portable `CREATE OR REPLACE TABLE "{branch}"."{table}" AS
    /// SELECT * FROM "{src_schema}"."{table}"` that works on DuckDB and
    /// any warehouse whose CTAS accepts the two-part `schema.table` form
    /// without a catalog prefix. Adapters with native zero-copy primitives
    /// (Databricks `SHALLOW CLONE`, Snowflake `CLONE`, BigQuery
    /// `CREATE TABLE ... COPY`) override to swap CTAS for the cheaper
    /// metadata-only operation.
    ///
    /// Lives on the in-tree `rocky-core` trait only — the SDK
    /// `WarehouseAdapter` does not yet expose a clone primitive, and
    /// `preview create` is already coupled to in-tree adapters via the
    /// registry.
    ///
    /// `branch_schema` and `source.{schema,table}` are validated as SQL
    /// identifiers before being interpolated into DDL; `source.catalog`
    /// is ignored by the default impl so it stays portable to DuckDB
    /// (which doesn't take a catalog prefix on CTAS in this shape).
    /// Adapters that override are responsible for their own validation
    /// and quoting.
    ///
    /// # Errors
    ///
    /// Surfaces any [`AdapterError`] from the underlying `execute_statement`
    /// — most commonly "source table does not exist in base schema",
    /// which `preview create` records as `copy_strategy = "failed"` so the
    /// rest of the copy set still runs.
    async fn clone_table_for_branch(
        &self,
        source: &TableRef,
        branch_schema: &str,
    ) -> AdapterResult<()> {
        rocky_sql::validation::validate_identifier(branch_schema).map_err(AdapterError::new)?;
        rocky_sql::validation::validate_identifier(&source.schema).map_err(AdapterError::new)?;
        rocky_sql::validation::validate_identifier(&source.table).map_err(AdapterError::new)?;
        let table = &source.table;
        let src_schema = &source.schema;
        let sql = format!(
            "CREATE OR REPLACE TABLE \"{branch_schema}\".\"{table}\" AS \
             SELECT * FROM \"{src_schema}\".\"{table}\""
        );
        self.execute_statement(&sql).await
    }

    /// Compute checksums for a batch of chunks defined by primary-key
    /// ranges. The bisection runner in
    /// [`crate::compare::bisection`] calls this once per recursion level
    /// per side; one call returns up to `K` chunks.
    ///
    /// **Returns one entry per non-empty chunk.** Chunks with zero rows on
    /// this side MAY be omitted by the implementation; callers MUST index
    /// the result by [`ChunkChecksum::chunk_id`], not by vector position.
    /// A `chunk_id` absent from the returned vector is equivalent to
    /// `ChunkChecksum { chunk_id, row_count: 0, checksum: 0 }`. This
    /// alignment burden lives on the caller because `GROUP BY chunk_id`
    /// drops empty groups on every target warehouse and back-filling
    /// per-adapter is error-prone.
    ///
    /// `value_columns` are the columns to hash for the chunk checksum
    /// (typically every non-`pk_column` column the caller cares about).
    /// `pk_column` identifies the bucketing column. The default impl
    /// supports a single integer/numeric column (`PkRange::IntRange`);
    /// composite and hash-bucket strategies require an adapter override
    /// that knows how to emit the corresponding bucketing SQL.
    ///
    /// **Integer-PK value range.** The default impl interpolates `lo`
    /// and `hi` as decimal literals into SQL, and the chunk-id
    /// arithmetic uses 64-bit floating-point. Tables whose PK exceeds
    /// the i64 range require an adapter override that handles wider
    /// integers natively.
    ///
    /// The default implementation uses [`SqlDialect::row_hash_expr`] +
    /// `BIT_XOR` to produce a portable query. Adapters with native
    /// hash + bucketing primitives (DuckDB `hash()` + `width_bucket`,
    /// BigQuery `FARM_FINGERPRINT`, Snowflake `HASH` + `BITXOR_AGG`,
    /// Databricks `xxhash64`) override for performance.
    async fn checksum_chunks(
        &self,
        table: &TableRef,
        pk_column: &str,
        value_columns: &[String],
        pk_ranges: &[PkRange],
    ) -> AdapterResult<Vec<ChunkChecksum>> {
        default_checksum_chunks(self, table, pk_column, value_columns, pk_ranges).await
    }

    /// Adapter-tuned override for the bisection leaf-row threshold (the
    /// `MIN_CHUNK_ROWS` knob). Below this row count, the bisection runner
    /// stops splitting and materializes the chunk for row-by-row diff.
    ///
    /// Defaults to `1000`, which lands break-even on DuckDB and similar
    /// in-process engines. Adapters with high per-query setup cost
    /// (Databricks, Snowflake, BigQuery) override to a higher value to
    /// avoid spending fixed roundtrip cost on small chunks.
    fn recommended_leaf_size(&self) -> u64 {
        1000
    }

    /// Adapter-tuned override for the row-count threshold above which
    /// UUID / hash-bucket-PK tables auto-fall-back from bisection to
    /// sampled (with a coverage warning).
    ///
    /// Defaults to `10_000_000` — chosen so the auto-fallback fires before
    /// a single mismatched bucket on `K=32` materializes more than ~300k
    /// rows per side. Adapters with cheap large-table scans (or with
    /// native hash-clustering guarantees) can lower this; the default is
    /// conservative.
    fn recommended_uuid_threshold(&self) -> u64 {
        10_000_000
    }
}

/// Default `checksum_chunks` implementation, factored out so it lives
/// outside the trait body and can call helpers without recursion-into-self
/// concerns. Adapter overrides bypass this entirely.
async fn default_checksum_chunks(
    adapter: &(impl WarehouseAdapter + ?Sized),
    table: &TableRef,
    pk_column: &str,
    value_columns: &[String],
    pk_ranges: &[PkRange],
) -> AdapterResult<Vec<ChunkChecksum>> {
    if pk_ranges.is_empty() {
        return Ok(Vec::new());
    }

    // Sub-step 1 supports IntRange only; composite + hash-bucket land in
    // follow-up phases. The error here is what the bisection runner
    // surfaces to the user when they request `--algorithm=bisection` on a
    // table whose PK isn't a single integer column on an adapter without
    // a native override.
    let (lo_min, hi_max, k) = uniform_int_range_window(pk_ranges)?;
    let step = (hi_max - lo_min) / (k as i128);
    if step <= 0 {
        return Err(AdapterError::msg(
            "default checksum_chunks requires a positive integer step \
             across the chunk window (lo == hi or hi < lo)",
        ));
    }

    rocky_sql::validation::validate_identifier(pk_column).map_err(AdapterError::new)?;
    for col in value_columns {
        rocky_sql::validation::validate_identifier(col).map_err(AdapterError::new)?;
    }

    let dialect = adapter.dialect();
    let table_ref = dialect.format_table_ref(&table.catalog, &table.schema, &table.table)?;
    let row_hash = dialect.row_hash_expr(value_columns)?;

    // FLOOR((pk - lo) / step) is the most-portable chunk-id construction —
    // works on Snowflake, Databricks Spark, BigQuery, and DuckDB without
    // dialect dispatch. The outer LEAST clamp handles the integer-step
    // truncation case: when `step * k < hi - lo`, the last chunk absorbs
    // the remainder (per `split_int_range`'s contract), so the SQL
    // bucketing must clamp the highest pk values back into chunk K-1
    // instead of overflowing to K. The outer SELECT pins ORDER BY for
    // stable serialization.
    let last_id = k - 1;
    let pk_quoted = dialect.quote_identifier(pk_column);
    let sql = format!(
        "SELECT chunk_id, COUNT(*) AS row_count, BIT_XOR({row_hash}) AS chk \
         FROM ( \
             SELECT *, \
                    LEAST( \
                        CAST(FLOOR((CAST({pk_quoted} AS DOUBLE) - {lo}) / {step}) AS BIGINT), \
                        {last_id} \
                    ) AS chunk_id \
             FROM {table_ref} \
             WHERE {pk_quoted} IS NOT NULL \
               AND {pk_quoted} >= {lo} \
               AND {pk_quoted} < {hi} \
         ) AS chunked \
         GROUP BY chunk_id \
         ORDER BY chunk_id",
        lo = lo_min,
        hi = hi_max,
    );

    let result = adapter.execute_query(&sql).await?;
    parse_chunk_checksums(&result, k)
}

/// Verifies the caller passed `K` contiguous, equal-width `IntRange`
/// chunks and returns `(lo_min, hi_max, K)` so the default impl can
/// derive the SQL `FLOOR((pk - lo) / step)` predicate.
fn uniform_int_range_window(pk_ranges: &[PkRange]) -> AdapterResult<(i128, i128, u32)> {
    let mut lo_min: Option<i128> = None;
    let mut hi_max: Option<i128> = None;
    for range in pk_ranges {
        match range {
            PkRange::IntRange { lo, hi } => {
                lo_min = Some(lo_min.map_or(*lo, |x| x.min(*lo)));
                hi_max = Some(hi_max.map_or(*hi, |x| x.max(*hi)));
            }
            PkRange::Composite { .. } | PkRange::HashBucket { .. } => {
                return Err(AdapterError::msg(
                    "default checksum_chunks supports IntRange only; \
                     composite and hash-bucket strategies require an \
                     adapter override",
                ));
            }
        }
    }
    let lo = lo_min.expect("non-empty pk_ranges checked above");
    let hi = hi_max.expect("non-empty pk_ranges checked above");
    let k = u32::try_from(pk_ranges.len())
        .map_err(|_| AdapterError::msg("checksum_chunks accepts up to u32::MAX chunks per call"))?;
    Ok((lo, hi, k))
}

/// Parse a `chunk_id, row_count, chk` result set into [`ChunkChecksum`]
/// rows. Empty chunks are absent from the input by SQL contract — the
/// caller back-fills via `chunk_id`.
fn parse_chunk_checksums(result: &QueryResult, k: u32) -> AdapterResult<Vec<ChunkChecksum>> {
    let mut out = Vec::with_capacity(result.rows.len());
    for row in &result.rows {
        if row.len() < 3 {
            return Err(AdapterError::msg(
                "checksum_chunks query returned a row with fewer than 3 \
                 columns; expected (chunk_id, row_count, chk)",
            ));
        }
        let chunk_id: u32 = parse_chunk_id(&row[0], k)?;
        let row_count: u64 = parse_u64(&row[1])?;
        let checksum: u128 = parse_checksum(&row[2])?;
        out.push(ChunkChecksum {
            chunk_id,
            row_count,
            checksum,
        });
    }
    Ok(out)
}

fn parse_chunk_id(v: &serde_json::Value, k: u32) -> AdapterResult<u32> {
    let raw = parse_i128(v)?;
    if raw < 0 || raw >= i128::from(k) {
        return Err(AdapterError::msg(format!(
            "checksum_chunks returned chunk_id={raw}, outside [0, {k})"
        )));
    }
    Ok(raw as u32)
}

fn parse_u64(v: &serde_json::Value) -> AdapterResult<u64> {
    let raw = parse_i128(v)?;
    u64::try_from(raw).map_err(|_| {
        AdapterError::msg(format!(
            "checksum_chunks returned row_count={raw}, expected non-negative u64"
        ))
    })
}

fn parse_checksum(v: &serde_json::Value) -> AdapterResult<u128> {
    let raw = parse_i128(v)?;
    Ok(raw as u128)
}

fn parse_i128(v: &serde_json::Value) -> AdapterResult<i128> {
    if let Some(s) = v.as_str() {
        return s.parse::<i128>().map_err(|e| {
            AdapterError::msg(format!(
                "failed to parse {s:?} as i128 from checksum_chunks result: {e}"
            ))
        });
    }
    if let Some(n) = v.as_i64() {
        return Ok(n.into());
    }
    if let Some(n) = v.as_u64() {
        return Ok(n.into());
    }
    Err(AdapterError::msg(format!(
        "checksum_chunks result column had unexpected JSON shape: {v:?}"
    )))
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
    /// Used by `TestType::RegexMatch`. The default impl returns an
    /// error — adapters that support regex must override.
    /// `column` is already validated as a SQL identifier; `pattern` is
    /// already validated against the strict allowlist
    /// ([`crate::tests::validate_regex_pattern`]).
    fn regex_match_predicate(&self, _column: &str, _pattern: &str) -> AdapterResult<String> {
        Err(AdapterError::msg(
            "regex_match not supported by this dialect",
        ))
    }

    /// Build a dialect-specific SQL expression equivalent to
    /// `CURRENT_DATE - N days`. Used by `TestType::OlderThanNDays`.
    /// The default impl uses the ANSI
    /// `CURRENT_DATE - INTERVAL 'N' DAY` form, which works for
    /// Databricks, Snowflake, and DuckDB. BigQuery overrides because
    /// it requires `DATE_SUB(CURRENT_DATE(), INTERVAL N DAY)`.
    fn date_minus_days_expr(&self, days: u32) -> AdapterResult<String> {
        Ok(format!("CURRENT_DATE - INTERVAL '{days}' DAY"))
    }

    /// SQL expression returning the current timestamp. Used by
    /// `TestType::NotInFuture`.
    ///
    /// Default: `CURRENT_TIMESTAMP` (ANSI keyword, no parens). Works for
    /// Databricks, Snowflake, DuckDB. BigQuery overrides to
    /// `CURRENT_TIMESTAMP()` because it requires the function form.
    fn current_timestamp_expr(&self) -> &'static str {
        "CURRENT_TIMESTAMP"
    }

    /// Quote a column / table identifier for safe interpolation into
    /// generated SQL. The caller has already validated `name` against
    /// the SQL-identifier allowlist; this method picks the dialect's
    /// quoting style.
    ///
    /// Default: `"name"` (DuckDB / Snowflake / Databricks accept double
    /// quotes for identifiers). BigQuery overrides to backticks because
    /// double-quoted strings in BigQuery are STRING literals, not
    /// identifiers — using the default on BQ produces SQL that
    /// type-checks the column name against the literal text rather than
    /// the column itself.
    ///
    /// Used by checksum-bisection's per-row fetch and the default
    /// `checksum_chunks` impl, where dialect-correct quoting is
    /// load-bearing for a query that interpolates the column name into
    /// numeric comparisons.
    fn quote_identifier(&self, name: &str) -> String {
        format!("\"{name}\"")
    }

    /// SQL expression that computes a single-row hash over `columns`,
    /// returning an integer wide enough to feed `BIT_XOR(...)` for
    /// chunk-checksum aggregation in
    /// [`WarehouseAdapter::checksum_chunks`].
    ///
    /// **No portable default exists** — the per-warehouse native hash
    /// functions all return integer types but the SQL surface differs
    /// (DuckDB `hash(...)`, Databricks `xxhash64(concat_ws(...))`,
    /// Snowflake `HASH(...)`, BigQuery
    /// `FARM_FINGERPRINT(TO_JSON_STRING(STRUCT(...)))`). The default impl
    /// returns an error so adapters that haven't overridden surface a
    /// helpful message at `--algorithm=bisection` time rather than
    /// emitting broken SQL.
    ///
    /// `columns` are quoted by the caller; the dialect must produce a SQL
    /// fragment safe to embed under `BIT_XOR(...)` and `GROUP BY
    /// chunk_id`.
    fn row_hash_expr(&self, _columns: &[String]) -> AdapterResult<String> {
        Err(AdapterError::msg(
            "row_hash_expr not supported by this dialect; required for \
             the checksum-bisection diff",
        ))
    }

    /// Returns `true` when changing a column's data type from
    /// `target_type` to `source_type` is safe enough to handle via
    /// `ALTER TABLE` instead of dropping and recreating the table.
    ///
    /// "Safe" means existing target values can be losslessly
    /// reinterpreted under the new type — typically a strict widening
    /// (`INT → BIGINT`) or a representation change with no value loss
    /// (`INT → STRING`). Each dialect's allowlist reflects its own
    /// type system; the default impl encodes Databricks/Spark
    /// conventions plus DECIMAL precision and VARCHAR length widening.
    /// Adapters with different type names (BigQuery `INT64` /
    /// `NUMERIC` / `FLOAT64` / `BIGNUMERIC`) override.
    fn is_safe_type_widening(&self, source_type: &str, target_type: &str) -> bool {
        crate::drift::default_is_safe_type_widening(source_type, target_type)
    }

    /// SQL to change a column's data type as part of drift evolution.
    ///
    /// Default emits the ANSI `ALTER TABLE x ALTER COLUMN y TYPE z`
    /// form (works for Databricks / Snowflake / DuckDB). BigQuery
    /// overrides — it requires `ALTER COLUMN y SET DATA TYPE z`.
    ///
    /// `column` is validated as a SQL identifier and `new_type` against
    /// the strict type allowlist
    /// ([`crate::sql_gen::validate_sql_type`]) before interpolation.
    /// `table_ref` should already be a fully formatted dialect-specific
    /// reference — the caller obtains it via `format_table_ref`.
    fn alter_column_type_sql(
        &self,
        table_ref: &str,
        column: &str,
        new_type: &str,
    ) -> AdapterResult<String> {
        rocky_sql::validation::validate_identifier(column).map_err(AdapterError::new)?;
        crate::sql_gen::validate_sql_type(new_type).map_err(AdapterError::new)?;
        Ok(format!(
            "ALTER TABLE {table_ref} ALTER COLUMN {column} TYPE {new_type}"
        ))
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
    /// role/group system, scoped to the given `catalogs`.
    ///
    /// `roles` is the post-[`crate::role_graph::flatten_role_graph`]
    /// map: each [`crate::ir::ResolvedRole`] carries its own permissions
    /// plus every transitive ancestor's, deduped and sorted. `catalogs`
    /// is the set of Unity Catalog / warehouse catalogs the current
    /// `rocky run` touched — the adapter emits per-catalog GRANT
    /// statements for each `(role, permission)` pair against every
    /// catalog in the slice. Group creation (e.g. Databricks SCIM
    /// `rocky_role_*`) is catalog-independent and fires once per role
    /// regardless of how many catalogs are in scope.
    ///
    /// An empty `catalogs` slice is a valid call: the adapter still
    /// creates groups so the warehouse is ready for later runs that
    /// *do* touch catalogs; it just skips GRANT emission.
    ///
    /// Adapters translate this into warehouse-specific primitives — on
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
        _catalogs: &[&str],
    ) -> AdapterResult<()> {
        Err(AdapterError::msg(
            "reconcile_role_graph not supported by this adapter",
        ))
    }

    /// Apply a data-retention policy to a table.
    ///
    /// Rocky calls this after a successful DAG run for every model whose
    /// sidecar declared `retention = "<N>[dy]"`. The resolved
    /// [`RetentionPolicy`] carries `duration_days: u32` — adapters turn
    /// that into warehouse-native retention DDL:
    ///
    /// - **Databricks (Delta):** pair of `TBLPROPERTIES` — both
    ///   `delta.logRetentionDuration` and
    ///   `delta.deletedFileRetentionDuration` updated to `'{N} days'`.
    /// - **Snowflake:** `ALTER TABLE ... SET
    ///   DATA_RETENTION_TIME_IN_DAYS = {N}` (Snowflake enforces its
    ///   edition-specific cap — 90 for Standard, 365 for Enterprise —
    ///   server-side; Rocky emits the DDL and surfaces any rejection).
    /// - **BigQuery / DuckDB:** not supported (no first-class
    ///   time-travel retention knob at the config level). The trait
    ///   default returns an error, which the runtime downgrades to a
    ///   `warn!` — the run is not aborted.
    ///
    /// # Errors
    ///
    /// The trait default returns an `AdapterError`. Adapters must declare
    /// explicit semantics: [`NoopGovernanceAdapter`] returns `Ok(())` so
    /// pipelines targeting a no-governance warehouse degrade gracefully;
    /// Databricks + Snowflake implement the DDL; BigQuery falls through
    /// to the default.
    async fn apply_retention_policy(
        &self,
        _table: &TableRef,
        _retention: &RetentionPolicy,
    ) -> AdapterResult<()> {
        Err(AdapterError::msg(
            "apply_retention_policy not supported by this adapter",
        ))
    }

    /// Read the warehouse-observed retention period for a table, in days.
    ///
    /// This is the read-side counterpart to [`Self::apply_retention_policy`]:
    /// `rocky retention-status --drift` calls it to compare the
    /// warehouse-side value against the declared `retention = "<N>[dy]"`
    /// sidecar. Adapters translate it to their warehouse's native probe:
    ///
    /// - **Databricks (Delta):** `SHOW TBLPROPERTIES ... (
    ///   'delta.deletedFileRetentionDuration')` and parse the `"interval N days"`
    ///   / `"N days"` value string.
    /// - **Snowflake:** `SHOW PARAMETERS LIKE 'DATA_RETENTION_TIME_IN_DAYS' IN TABLE ...`
    ///   and parse the integer value column.
    /// - **BigQuery / DuckDB:** not supported — the trait default returns
    ///   `Ok(None)`, which `retention-status --drift` surfaces as "no
    ///   warehouse value observed" rather than as an error.
    ///
    /// Returning `Ok(None)` is the declared way to signal "unsupported on
    /// this adapter" — it lets the CLI probe every model uniformly without
    /// having to branch on adapter kind first.
    ///
    /// # Errors
    ///
    /// Returns an [`AdapterError`] on network / auth / malformed-response
    /// failures. The trait default returns `Ok(None)` so callers can probe
    /// unsupported adapters safely.
    async fn read_retention_days(&self, _table: &TableRef) -> AdapterResult<Option<u32>> {
        Ok(None)
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
        _catalogs: &[&str],
    ) -> AdapterResult<()> {
        // Same rationale as `apply_column_tags` / `apply_masking_policy`:
        // a no-governance warehouse silently accepts role-graph config so
        // pipelines downgrade gracefully. Cycle / unknown-parent errors
        // still surface at config-load time via flatten_role_graph.
        Ok(())
    }

    async fn apply_retention_policy(
        &self,
        _table: &TableRef,
        _retention: &RetentionPolicy,
    ) -> AdapterResult<()> {
        // No-op governance treats retention as optional metadata: pipelines
        // targeting a no-governance warehouse (DuckDB, or any adapter
        // without a governance impl) degrade gracefully instead of aborting
        // a successful run.
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
/// The `RockyType` enum is defined in `rocky-core::types` (relocated from
/// `rocky-compiler::types` in Typed-IR Option B Phase 1). This trait uses
/// string-based type names so that adapter crates don't have to depend on the
/// typed representation directly.
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
            .reconcile_role_graph(&BTreeMap::new(), &[])
            .await
            .unwrap_err();
        assert!(err.to_string().contains("reconcile_role_graph"));
    }

    #[tokio::test]
    async fn noop_governance_accepts_role_graph() {
        let noop = NoopGovernanceAdapter;
        assert!(
            noop.reconcile_role_graph(&BTreeMap::new(), &[])
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn trait_default_apply_retention_policy_errors() {
        let t = TableRef {
            catalog: "c".into(),
            schema: "s".into(),
            table: "t".into(),
        };
        let err = MinimalGovernance
            .apply_retention_policy(&t, &RetentionPolicy { duration_days: 90 })
            .await
            .unwrap_err();
        assert!(err.to_string().contains("apply_retention_policy"));
    }

    #[tokio::test]
    async fn noop_governance_accepts_retention() {
        let noop = NoopGovernanceAdapter;
        let t = TableRef {
            catalog: "c".into(),
            schema: "s".into(),
            table: "t".into(),
        };
        assert!(
            noop.apply_retention_policy(&t, &RetentionPolicy { duration_days: 90 })
                .await
                .is_ok()
        );
    }

    // `read_retention_days` is the read-side probe used by
    // `rocky retention-status --drift`. Unlike the other governance
    // capabilities, the trait default is `Ok(None)` — an adapter that
    // doesn't implement it should degrade to "no observation" rather than
    // erroring, so the CLI can probe every model uniformly without
    // branching on adapter kind.
    #[tokio::test]
    async fn trait_default_read_retention_days_returns_none() {
        let t = TableRef {
            catalog: "c".into(),
            schema: "s".into(),
            table: "t".into(),
        };
        let got = MinimalGovernance.read_retention_days(&t).await.unwrap();
        assert!(
            got.is_none(),
            "trait default should return Ok(None), got {got:?}"
        );
    }

    #[tokio::test]
    async fn noop_governance_read_retention_days_returns_none() {
        let noop = NoopGovernanceAdapter;
        let t = TableRef {
            catalog: "c".into(),
            schema: "s".into(),
            table: "t".into(),
        };
        // Noop inherits the default (it doesn't override). Assert the
        // behaviour Noop consumers depend on: no warehouse observation
        // when no governance impl is wired.
        assert!(noop.read_retention_days(&t).await.unwrap().is_none());
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
