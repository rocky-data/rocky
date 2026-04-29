//! Rocky adapter skeleton — ClickHouse-shaped, copy-and-edit starter.
//!
//! This crate is a worked example of how to build a Rust-native warehouse
//! adapter against `rocky-adapter-sdk`. It is named after ClickHouse because
//! that is the most-asked-for community adapter, but the same shape works
//! for any SQL warehouse Rocky doesn't ship in-tree (Trino, Redshift,
//! StarRocks, Doris, MotherDuck, ...).
//!
//! # What you would change to make this real
//!
//! Three substitutions, in order:
//!
//! 1. Replace [`MockBackend`] with a real client. For ClickHouse that's
//!    `clickhouse::Client::default().with_url(host).with_database(db)`. The
//!    backend lives behind a small surface so you can wiremock it in
//!    tests instead of standing up a live warehouse for CI.
//! 2. Fill in the dialect specifics in [`SkeletonDialect`] — quoting,
//!    `MERGE` (or its absence — ClickHouse uses `ALTER TABLE ... UPDATE`),
//!    partition overwrite, type widening rules.
//! 3. Implement [`AdapterAuth`] (an example pattern, not part of the SDK)
//!    or your own auth helper for the credential flow your warehouse
//!    needs. Look at `engine/crates/rocky-databricks/src/auth.rs` for a
//!    PAT-then-OAuth pattern, or `rocky-snowflake/src/auth.rs` for
//!    multi-method priority.
//!
//! # What's intentionally out of scope
//!
//! - **Registry hookup.** Out-of-tree adapters are a forward-looking SDK
//!   contract today — a community contributor still needs to fork
//!   `rocky-data/rocky` and add the adapter to `engine/Cargo.toml` plus
//!   the CLI's adapter dispatch. The SDK pins the trait shape so that
//!   fork stays small and cleanly upstreamable. See the guide at
//!   `docs/src/content/docs/guides/adapter-sdk.md` for the current path.
//! - **Governance and discovery.** Both are optional traits in the SDK —
//!   skip them unless your warehouse exposes them. The skeleton declares
//!   `warehouse_only()` capabilities to keep the surface tight.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use rocky_adapter_sdk::{
    AdapterCapabilities, AdapterError, AdapterManifest, AdapterResult, ColumnInfo,
    ColumnSelection, MetadataColumn, QueryResult, SqlDialect, TableRef, WarehouseAdapter,
    SDK_VERSION,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Adapter-specific configuration deserialized from `[adapter.foo]` in
/// `rocky.toml`. Rocky validates this against [`AdapterManifest::config_schema`]
/// before constructing the adapter, so unknown fields surface as a config
/// error rather than a runtime panic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkeletonConfig {
    /// Warehouse host (e.g. `https://my-cluster.clickhouse.com:8443`).
    pub host: String,
    /// Default database / schema. ClickHouse calls this a "database";
    /// Rocky calls it a "schema".
    pub database: String,
    /// HTTP client timeout in seconds.
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,
}

fn default_timeout() -> u64 {
    30
}

/// Auth pattern for the skeleton. Rocky doesn't prescribe an auth trait —
/// each adapter wires its own — but every production adapter ends up with
/// a small enum like this. Keep environment-variable lookup out of `new`
/// so tests can construct the adapter without touching env state.
#[derive(Debug, Clone)]
pub enum AdapterAuth {
    /// Username + password. Read from `${VAR}` substitution at config-parse
    /// time so secrets never appear on the command line.
    BasicAuth { user: String, password: String },
    /// Bearer token (JWT, PAT, or service-account-issued).
    Token(String),
    /// No auth — only useful for embedded or local testing.
    None,
}

// ---------------------------------------------------------------------------
// Backend abstraction
// ---------------------------------------------------------------------------

/// Trait that hides the actual warehouse client behind a small surface.
///
/// In a real adapter, the production impl is `clickhouse::Client` (or
/// `reqwest::Client` for warehouses without a typed driver). The trait
/// indirection lets you ship a `MockBackend` for unit tests so CI doesn't
/// need a live cluster.
#[async_trait]
pub trait Backend: Send + Sync {
    async fn execute(&self, sql: &str) -> AdapterResult<()>;
    async fn query(&self, sql: &str) -> AdapterResult<QueryResult>;
    async fn describe(&self, table: &TableRef) -> AdapterResult<Vec<ColumnInfo>>;
    async fn exists(&self, table: &TableRef) -> AdapterResult<bool>;
}

/// In-memory backend used by the skeleton's tests and the `demo` example.
///
/// Records every executed statement and answers `describe`/`exists` from
/// a tiny in-memory catalog. Replace with a real client to ship a real
/// adapter — the rest of the file does not need to change.
#[derive(Default)]
pub struct MockBackend {
    inner: Mutex<MockState>,
}

#[derive(Default)]
struct MockState {
    /// Every SQL statement that has been sent — useful for asserting in
    /// tests that the dialect generated the SQL the test expected.
    pub log: Vec<String>,
    /// Tiny in-memory catalog: table -> column list.
    pub tables: HashMap<String, Vec<ColumnInfo>>,
}

impl MockBackend {
    pub fn new() -> Self {
        Self::default()
    }

    /// Pre-populate a table so `describe`/`exists` return something useful.
    pub async fn install_table(&self, table: TableRef, cols: Vec<ColumnInfo>) {
        let mut state = self.inner.lock().await;
        state.tables.insert(table.full_name(), cols);
    }

    /// Snapshot of every statement executed so far. Tests assert on this.
    pub async fn statement_log(&self) -> Vec<String> {
        self.inner.lock().await.log.clone()
    }
}

#[async_trait]
impl Backend for MockBackend {
    async fn execute(&self, sql: &str) -> AdapterResult<()> {
        self.inner.lock().await.log.push(sql.to_string());
        Ok(())
    }

    async fn query(&self, sql: &str) -> AdapterResult<QueryResult> {
        self.inner.lock().await.log.push(sql.to_string());
        // The mock returns a single-row, single-column result so the
        // skeleton example exercises the response path without needing a
        // SQL parser.
        Ok(QueryResult {
            columns: vec!["ok".into()],
            rows: vec![vec![serde_json::Value::Bool(true)]],
        })
    }

    async fn describe(&self, table: &TableRef) -> AdapterResult<Vec<ColumnInfo>> {
        let state = self.inner.lock().await;
        state
            .tables
            .get(&table.full_name())
            .cloned()
            .ok_or_else(|| AdapterError::msg(format!("table {table} not found")))
    }

    async fn exists(&self, table: &TableRef) -> AdapterResult<bool> {
        Ok(self.inner.lock().await.tables.contains_key(&table.full_name()))
    }
}

// ---------------------------------------------------------------------------
// SQL dialect
// ---------------------------------------------------------------------------

/// Skeleton dialect — ClickHouse-shaped: backtick quoting, two-part names
/// (no catalogs), `INSERT INTO ... SELECT` for both replace and append
/// because most ClickHouse engines don't have `MERGE`.
///
/// Override the methods that matter for your warehouse; the SDK's
/// conformance harness exercises the ones a basic adapter needs.
pub struct SkeletonDialect;

impl SqlDialect for SkeletonDialect {
    fn name(&self) -> &str {
        "skeleton"
    }

    fn format_table_ref(
        &self,
        _catalog: &str,
        schema: &str,
        table: &str,
    ) -> AdapterResult<String> {
        // ClickHouse has no catalogs. The SDK passes a `catalog` argument
        // anyway so the trait stays uniform — drop it on the floor and
        // build a two-part name. Note: we still validate identifiers,
        // because anything we splice into SQL must be allowlisted.
        validate_ident(schema)?;
        validate_ident(table)?;
        Ok(format!("`{schema}`.`{table}`"))
    }

    fn create_table_as(&self, target: &str, select_sql: &str) -> String {
        // ClickHouse needs an explicit ENGINE clause. Default to
        // `MergeTree() ORDER BY tuple()` so the skeleton compiles for
        // any select shape; a real adapter would derive the order key
        // from the contract or model sidecar.
        format!("CREATE TABLE {target} ENGINE = MergeTree() ORDER BY tuple() AS {select_sql}")
    }

    fn insert_into(&self, target: &str, select_sql: &str) -> String {
        format!("INSERT INTO {target} {select_sql}")
    }

    fn merge_into(
        &self,
        _target: &str,
        _source_sql: &str,
        _keys: &[String],
        _update_cols: Option<&[String]>,
    ) -> AdapterResult<String> {
        // ClickHouse has no MERGE. A real adapter typically synthesises
        // `ALTER TABLE ... UPDATE` + `INSERT INTO ... SELECT` and
        // returns them as a multi-statement string, or returns
        // `not_supported` and asks the user to use `incremental` instead.
        Err(AdapterError::not_supported("merge_into"))
    }

    fn describe_table_sql(&self, table_ref: &str) -> String {
        format!("DESCRIBE TABLE {table_ref}")
    }

    fn drop_table_sql(&self, table_ref: &str) -> String {
        format!("DROP TABLE IF EXISTS {table_ref}")
    }

    fn create_catalog_sql(&self, _name: &str) -> Option<AdapterResult<String>> {
        // No catalog concept. Returning None tells Rocky the warehouse
        // doesn't support it; `auto_create_catalogs = true` in
        // `rocky.toml` will then surface a clear error rather than
        // emit broken SQL.
        None
    }

    fn create_schema_sql(&self, _catalog: &str, schema: &str) -> Option<AdapterResult<String>> {
        match validate_ident(schema) {
            Ok(()) => Some(Ok(format!("CREATE DATABASE IF NOT EXISTS `{schema}`"))),
            Err(e) => Some(Err(e)),
        }
    }

    fn row_hash_expr(&self, columns: &[String]) -> String {
        // ClickHouse uses sipHash128 over a tuple. Real adapters often
        // want a stable cross-warehouse hash for change-detection; check
        // the existing rocky-bigquery / rocky-snowflake dialects for
        // the cross-warehouse pattern.
        let cols = columns.join(", ");
        format!("hex(sipHash128(({cols})))")
    }

    fn tablesample_clause(&self, percent: u32) -> Option<String> {
        Some(format!("SAMPLE {}", percent as f64 / 100.0))
    }

    fn select_clause(
        &self,
        columns: &ColumnSelection,
        metadata: &[MetadataColumn],
    ) -> AdapterResult<String> {
        let base = match columns {
            ColumnSelection::All => "*".to_string(),
            ColumnSelection::Explicit(cols) => {
                for c in cols {
                    validate_ident(c)?;
                }
                cols.iter()
                    .map(|c| format!("`{c}`"))
                    .collect::<Vec<_>>()
                    .join(", ")
            }
        };
        if metadata.is_empty() {
            return Ok(base);
        }
        let extras = metadata
            .iter()
            .map(|m| format!("CAST({} AS {}) AS `{}`", m.value, m.data_type, m.name))
            .collect::<Vec<_>>()
            .join(", ");
        Ok(format!("{base}, {extras}"))
    }

    fn watermark_where(&self, timestamp_col: &str, target_ref: &str) -> AdapterResult<String> {
        validate_ident(timestamp_col)?;
        // Standard incremental pattern: only pick up rows newer than the
        // current max in target. ClickHouse evaluates the subquery once.
        Ok(format!(
            "`{timestamp_col}` > (SELECT max(`{timestamp_col}`) FROM {target_ref})"
        ))
    }

    fn insert_overwrite_partition(
        &self,
        target: &str,
        partition_filter: &str,
        select_sql: &str,
    ) -> AdapterResult<Vec<String>> {
        // ClickHouse partition replace = ALTER TABLE ... DROP PARTITION
        // followed by INSERT. Two statements; the runtime executes them
        // in order and rolls back on partial failure.
        Ok(vec![
            format!("ALTER TABLE {target} DELETE WHERE {partition_filter}"),
            format!("INSERT INTO {target} {select_sql}"),
        ])
    }
}

/// Conservative SQL identifier check — the same shape rocky-sql uses.
/// Adapters MUST validate any identifier they splice into SQL.
fn validate_ident(ident: &str) -> AdapterResult<()> {
    if ident.is_empty() || ident.len() > 255 {
        return Err(AdapterError::msg(format!("invalid identifier: {ident:?}")));
    }
    if !ident.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(AdapterError::msg(format!(
            "identifier {ident:?} contains characters outside [A-Za-z0-9_]"
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Adapter
// ---------------------------------------------------------------------------

/// The adapter struct itself: a backend handle plus a dialect.
///
/// `Arc<dyn Backend>` lets the same backend be shared with discovery /
/// governance adapters in a fuller implementation. For warehouse-only
/// scope, a plain `Box<dyn Backend>` is enough.
pub struct SkeletonAdapter {
    backend: Arc<dyn Backend>,
    dialect: SkeletonDialect,
}

impl SkeletonAdapter {
    pub fn new(backend: Arc<dyn Backend>) -> Self {
        Self {
            backend,
            dialect: SkeletonDialect,
        }
    }

    /// Return the manifest Rocky uses to validate config and route
    /// optional features. Production adapters typically build this once
    /// at construction time and cache it.
    pub fn manifest() -> AdapterManifest {
        AdapterManifest {
            name: "skeleton".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            sdk_version: SDK_VERSION.into(),
            dialect: "skeleton".into(),
            capabilities: AdapterCapabilities {
                // Real ClickHouse adapters: warehouse + create_schema +
                // batch_checks. No merge, no governance.
                warehouse: true,
                discovery: false,
                governance: false,
                batch_checks: false,
                create_catalog: false,
                create_schema: true,
                merge: false,
                tablesample: true,
                file_load: false,
            },
            auth_methods: vec!["basic".into(), "token".into()],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "host": { "type": "string" },
                    "database": { "type": "string" },
                    "timeout_secs": { "type": "integer", "minimum": 1 }
                },
                "required": ["host", "database"]
            }),
        }
    }
}

#[async_trait]
impl WarehouseAdapter for SkeletonAdapter {
    fn dialect(&self) -> &dyn SqlDialect {
        &self.dialect
    }

    async fn execute_statement(&self, sql: &str) -> AdapterResult<()> {
        self.backend.execute(sql).await
    }

    async fn execute_query(&self, sql: &str) -> AdapterResult<QueryResult> {
        self.backend.query(sql).await
    }

    async fn describe_table(&self, table: &TableRef) -> AdapterResult<Vec<ColumnInfo>> {
        self.backend.describe(table).await
    }

    async fn table_exists(&self, table: &TableRef) -> AdapterResult<bool> {
        self.backend.exists(table).await
    }

    async fn close(&self) -> AdapterResult<()> {
        // Real adapter: drop pooled HTTP clients, flush metrics, etc.
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn t(schema: &str, table: &str) -> TableRef {
        TableRef {
            catalog: String::new(),
            schema: schema.into(),
            table: table.into(),
        }
    }

    #[tokio::test]
    async fn execute_statement_round_trips_to_backend() {
        let backend = Arc::new(MockBackend::new());
        let adapter = SkeletonAdapter::new(backend.clone());

        adapter
            .execute_statement("CREATE TABLE foo (id Int64) ENGINE=Memory")
            .await
            .unwrap();

        let log = backend.statement_log().await;
        assert_eq!(log.len(), 1);
        assert!(log[0].contains("CREATE TABLE foo"));
    }

    #[tokio::test]
    async fn describe_table_returns_installed_columns() {
        let backend = Arc::new(MockBackend::new());
        backend
            .install_table(
                t("default", "events"),
                vec![ColumnInfo {
                    name: "id".into(),
                    data_type: "Int64".into(),
                    nullable: false,
                }],
            )
            .await;
        let adapter = SkeletonAdapter::new(backend);

        let cols = adapter.describe_table(&t("default", "events")).await.unwrap();
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].name, "id");
    }

    #[tokio::test]
    async fn describe_table_errors_on_missing() {
        let backend = Arc::new(MockBackend::new());
        let adapter = SkeletonAdapter::new(backend);
        let err = adapter.describe_table(&t("default", "missing")).await.unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn dialect_quotes_two_part_names_and_drops_catalog() {
        let d = SkeletonDialect;
        let s = d.format_table_ref("ignored", "raw", "events").unwrap();
        assert_eq!(s, "`raw`.`events`");
    }

    #[test]
    fn dialect_rejects_injection_in_identifiers() {
        let d = SkeletonDialect;
        let err = d.format_table_ref("", "raw", "events; DROP TABLE x").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("invalid") || msg.contains("outside"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn dialect_emits_partition_overwrite_as_two_statements() {
        let d = SkeletonDialect;
        let stmts = d
            .insert_overwrite_partition(
                "`raw`.`events`",
                "`day` = '2026-01-01'",
                "SELECT * FROM staging",
            )
            .unwrap();
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].starts_with("ALTER TABLE"));
        assert!(stmts[1].starts_with("INSERT INTO"));
    }

    #[test]
    fn dialect_reports_no_merge_support() {
        let d = SkeletonDialect;
        let err = d.merge_into("t", "s", &["id".into()], None).unwrap_err();
        assert!(err.to_string().contains("not supported"));
    }

    #[test]
    fn manifest_targets_current_sdk_version() {
        let m = SkeletonAdapter::manifest();
        assert_eq!(m.sdk_version, SDK_VERSION);
        assert!(m.capabilities.warehouse);
        assert!(!m.capabilities.merge);
    }
}
