//! DuckDB warehouse adapter implementing [`WarehouseAdapter`].
//!
//! Wraps [`DuckDbConnector`] behind the generic warehouse interface,
//! enabling DuckDB to be used through the adapter registry alongside
//! Databricks and future warehouse adapters.
//!
//! The wrapped connector is held in `Arc<Mutex<>>` so that the same
//! database can also be used by [`crate::discovery::DuckDbDiscoveryAdapter`]
//! when DuckDB is registered as both warehouse and discovery for one pipeline.

use std::sync::{Arc, Mutex};

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use tokio::task::spawn_blocking;

use rocky_core::traits::{
    AdapterError, AdapterResult, ExplainResult, QueryResult, SqlDialect, WarehouseAdapter,
};
use rocky_ir::{ColumnInfo, TableRef};
use rocky_sql::validation;

use crate::DuckDbConnector;
use crate::dialect::DuckDbSqlDialect;

/// DuckDB warehouse adapter wrapping [`DuckDbConnector`] behind the
/// [`WarehouseAdapter`] trait.
///
/// Uses `Arc<Mutex<>>` so the same connector can be shared with a
/// [`crate::discovery::DuckDbDiscoveryAdapter`]. `duckdb::Connection` is
/// `Send` but not `Sync`, so the mutex is required.
///
/// The synchronous DuckDB C++ driver is CPU/IO-bound and would block a
/// tokio worker thread for the full duration of every query if called
/// inline. Each adapter method instead clones the `Arc<Mutex<…>>` into a
/// [`tokio::task::spawn_blocking`] closure that takes the lock and runs the
/// driver call on the blocking thread pool, keeping the async reactor free.
/// `Mutex<DuckDbConnector>` is `Send + Sync` (the inner `Connection` is
/// `Send`), so moving the handle into the closure is sound; the
/// `MutexGuard` itself never crosses an `.await`.
pub struct DuckDbWarehouseAdapter {
    connector: Arc<Mutex<DuckDbConnector>>,
    dialect: DuckDbSqlDialect,
    /// Test-only override that makes [`Self::supports_concurrent_execution`]
    /// report `true` so the CLI's intra-layer concurrent execution path can
    /// be exercised against real DuckDB storage in unit tests. Always `false`
    /// in production: the single `Mutex` connection just serializes work.
    concurrent_for_test: bool,
}

impl DuckDbWarehouseAdapter {
    /// Create an adapter wrapping an in-memory DuckDB database.
    pub fn in_memory() -> Result<Self, crate::DuckDbError> {
        let connector = DuckDbConnector::in_memory()?;
        Ok(Self {
            connector: Arc::new(Mutex::new(connector)),
            dialect: DuckDbSqlDialect,
            concurrent_for_test: false,
        })
    }

    /// Create an adapter wrapping a persistent DuckDB database file.
    pub fn open(path: &std::path::Path) -> Result<Self, crate::DuckDbError> {
        let connector = DuckDbConnector::open(path)?;
        Ok(Self {
            connector: Arc::new(Mutex::new(connector)),
            dialect: DuckDbSqlDialect,
            concurrent_for_test: false,
        })
    }

    /// Create an adapter from an existing connector.
    pub fn new(connector: DuckDbConnector) -> Self {
        Self {
            connector: Arc::new(Mutex::new(connector)),
            dialect: DuckDbSqlDialect,
            concurrent_for_test: false,
        }
    }

    /// Create an adapter from an existing shared connector.
    /// Used by the registry when registering DuckDB as both warehouse and
    /// discovery from a single `[adapter.NAME]` block.
    pub fn from_shared(connector: Arc<Mutex<DuckDbConnector>>) -> Self {
        Self {
            connector,
            dialect: DuckDbSqlDialect,
            concurrent_for_test: false,
        }
    }

    /// Test-only: flip [`Self::supports_concurrent_execution`] to `true`.
    ///
    /// Production DuckDB always runs serial (one blocking `Mutex`
    /// connection). This builder lets concurrency tests drive the CLI's
    /// intra-layer concurrent execution path against real DuckDB storage —
    /// the `Mutex` still serializes the actual driver calls, so it stays
    /// correct, but the concurrent control-flow (fan-out, barrier, ordered
    /// collect) is genuinely exercised.
    #[doc(hidden)]
    pub fn with_concurrent_for_test(mut self, concurrent: bool) -> Self {
        self.concurrent_for_test = concurrent;
        self
    }

    /// Returns a clone of the underlying connector handle for sharing
    /// with a [`crate::discovery::DuckDbDiscoveryAdapter`].
    pub fn shared_connector(&self) -> Arc<Mutex<DuckDbConnector>> {
        Arc::clone(&self.connector)
    }
}

#[async_trait]
impl WarehouseAdapter for DuckDbWarehouseAdapter {
    fn dialect(&self) -> &dyn SqlDialect {
        &self.dialect
    }

    async fn execute_statement(&self, sql: &str) -> AdapterResult<()> {
        let conn = Arc::clone(&self.connector);
        let sql = sql.to_string();
        spawn_blocking(move || {
            let conn = conn
                .lock()
                .map_err(|e| AdapterError::msg(format!("mutex poisoned: {e}")))?;
            conn.execute_statement(&sql).map_err(AdapterError::new)
        })
        .await
        .map_err(|e| join_error(&e))?
    }

    async fn execute_query(&self, sql: &str) -> AdapterResult<QueryResult> {
        let conn = Arc::clone(&self.connector);
        let sql = sql.to_string();
        spawn_blocking(move || {
            let conn = conn
                .lock()
                .map_err(|e| AdapterError::msg(format!("mutex poisoned: {e}")))?;
            let result = conn.execute_sql(&sql).map_err(AdapterError::new)?;
            Ok(QueryResult {
                columns: result.columns,
                rows: result.rows,
            })
        })
        .await
        .map_err(|e| join_error(&e))?
    }

    async fn fetch_arrow_batch(&self, sql: &str) -> AdapterResult<RecordBatch> {
        // `duckdb 1.10503` pins workspace `arrow 58`, so `query_arrow`
        // returns batches directly type-compatible with the trait's
        // `arrow::record_batch::RecordBatch`. No IPC bridge required —
        // the connector concatenates multi-batch result sets in place.
        let conn = Arc::clone(&self.connector);
        let sql = sql.to_string();
        spawn_blocking(move || {
            let conn = conn
                .lock()
                .map_err(|e| AdapterError::msg(format!("mutex poisoned: {e}")))?;
            conn.query_arrow_batch(&sql).map_err(AdapterError::new)
        })
        .await
        .map_err(|e| join_error(&e))?
    }

    async fn describe_table(&self, table: &TableRef) -> AdapterResult<Vec<ColumnInfo>> {
        let table_ref = if table.catalog.is_empty() {
            format!("{}.{}", table.schema, table.table)
        } else {
            format!("{}.{}.{}", table.catalog, table.schema, table.table)
        };

        let conn = Arc::clone(&self.connector);
        spawn_blocking(move || {
            let conn = conn
                .lock()
                .map_err(|e| AdapterError::msg(format!("mutex poisoned: {e}")))?;

            let result = conn
                .execute_sql(&format!("DESCRIBE {table_ref}"))
                .map_err(AdapterError::new)?;

            // DuckDB DESCRIBE returns: column_name, column_type, null, key, default, extra
            let columns = result
                .rows
                .iter()
                .filter_map(|row| {
                    let name = row.first()?.as_str()?.to_string();
                    let data_type = row.get(1)?.as_str()?.to_string();
                    let nullable = row
                        .get(2)
                        .and_then(|v| v.as_str())
                        .map(|v| v == "YES")
                        .unwrap_or(true);
                    Some(ColumnInfo {
                        name,
                        data_type,
                        nullable,
                    })
                })
                .collect();

            Ok(columns)
        })
        .await
        .map_err(|e| join_error(&e))?
    }

    async fn explain(&self, sql: &str) -> AdapterResult<ExplainResult> {
        let explain_sql = format!("EXPLAIN {sql}");
        let conn = Arc::clone(&self.connector);
        spawn_blocking(move || {
            let conn = conn
                .lock()
                .map_err(|e| AdapterError::msg(format!("mutex poisoned: {e}")))?;
            let result = conn.execute_sql(&explain_sql).map_err(AdapterError::new)?;

            // DuckDB EXPLAIN returns a two-column result: explain_key, explain_value.
            // Concatenate all values into the raw explain string.
            let raw_explain = result
                .rows
                .iter()
                .filter_map(|row| row.get(1).and_then(|v| v.as_str()))
                .collect::<Vec<_>>()
                .join("\n");

            Ok(ExplainResult {
                estimated_bytes_scanned: None,
                estimated_rows: None,
                estimated_compute_units: None,
                raw_explain,
            })
        })
        .await
        .map_err(|e| join_error(&e))?
    }

    fn supports_concurrent_execution(&self) -> bool {
        // Production DuckDB shares a single blocking `Arc<Mutex>` connection,
        // so concurrent execution just serializes through the mutex (and
        // would thrash the blocking pool). The CLI forces serial execution
        // for adapters that report `false`. The test-only override flips this
        // so the concurrent execution path can be exercised against real
        // DuckDB storage.
        self.concurrent_for_test
    }

    async fn list_tables(&self, _catalog: &str, schema: &str) -> AdapterResult<Vec<String>> {
        validation::validate_identifier(schema).map_err(AdapterError::new)?;
        let sql = format!(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema}'"
        );
        let conn = Arc::clone(&self.connector);
        spawn_blocking(move || {
            let conn = conn
                .lock()
                .map_err(|e| AdapterError::msg(format!("mutex poisoned: {e}")))?;
            let result = conn.execute_sql(&sql).map_err(AdapterError::new)?;
            let tables = result
                .rows
                .iter()
                .filter_map(|row| row.first().and_then(|v| v.as_str()).map(str::to_lowercase))
                .collect();
            Ok(tables)
        })
        .await
        .map_err(|e| join_error(&e))?
    }
}

/// Convert a [`tokio::task::JoinError`] (panic or cancellation of the
/// `spawn_blocking` task) into an [`AdapterError`]. A panic inside the
/// blocking closure surfaces here rather than aborting the worker.
fn join_error(e: &tokio::task::JoinError) -> AdapterError {
    AdapterError::msg(format!("duckdb blocking task failed: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_adapter_execute_statement() {
        let adapter = DuckDbWarehouseAdapter::in_memory().unwrap();
        adapter
            .execute_statement("CREATE TABLE t (id INTEGER, name VARCHAR)")
            .await
            .unwrap();
        adapter
            .execute_statement("INSERT INTO t VALUES (1, 'Alice')")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_adapter_execute_query() {
        let adapter = DuckDbWarehouseAdapter::in_memory().unwrap();
        adapter
            .execute_statement("CREATE TABLE t (id INTEGER, name VARCHAR)")
            .await
            .unwrap();
        adapter
            .execute_statement("INSERT INTO t VALUES (1, 'Alice')")
            .await
            .unwrap();

        let result = adapter.execute_query("SELECT * FROM t").await.unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.columns, vec!["id", "name"]);
    }

    #[tokio::test]
    async fn test_adapter_describe_table() {
        let adapter = DuckDbWarehouseAdapter::in_memory().unwrap();
        adapter
            .execute_statement(
                "CREATE TABLE main.t (id INTEGER NOT NULL, name VARCHAR, amount DECIMAL(10,2))",
            )
            .await
            .unwrap();

        let table = TableRef {
            catalog: String::new(),
            schema: "main".to_string(),
            table: "t".to_string(),
        };
        let cols = adapter.describe_table(&table).await.unwrap();
        assert_eq!(cols.len(), 3);
        assert_eq!(cols[0].name, "id");
        assert!(!cols[0].nullable);
        assert_eq!(cols[1].name, "name");
        assert!(cols[1].nullable);
    }

    #[tokio::test]
    async fn test_adapter_dialect() {
        let adapter = DuckDbWarehouseAdapter::in_memory().unwrap();
        let sql = adapter.dialect().create_table_as("t", "SELECT 1");
        assert!(sql.contains("CREATE OR REPLACE TABLE t"));
    }

    /// Conformance test for the default `clone_table_for_branch` impl.
    ///
    /// DuckDB doesn't override the trait method, so this case proves the
    /// portable CTAS default actually clones rows from the source schema
    /// into the branch schema. Future native-clone adapters
    /// (Databricks SHALLOW CLONE, Snowflake CLONE, BigQuery COPY) will
    /// add their own per-adapter cases; the DuckDB path stays the
    /// canonical regression test for the default implementation.
    #[tokio::test]
    async fn clone_table_for_branch_default_path() {
        let adapter = DuckDbWarehouseAdapter::in_memory().unwrap();

        // Stand up a base schema with one row in it.
        adapter
            .execute_statement("CREATE SCHEMA IF NOT EXISTS demo")
            .await
            .unwrap();
        adapter
            .execute_statement("CREATE TABLE demo.orders AS SELECT 1 AS id, 'a' AS name")
            .await
            .unwrap();

        // Create the branch schema and clone the table into it.
        adapter
            .execute_statement("CREATE SCHEMA IF NOT EXISTS branch__pr1")
            .await
            .unwrap();

        let source = TableRef {
            catalog: String::new(),
            schema: "demo".into(),
            table: "orders".into(),
        };
        adapter
            .clone_table_for_branch(&source, "branch__pr1")
            .await
            .expect("default CTAS clone must succeed on DuckDB");

        // Verify the cloned table contains the original row.
        let result = adapter
            .execute_query("SELECT id FROM branch__pr1.orders")
            .await
            .unwrap();
        assert_eq!(result.rows.len(), 1, "cloned table should carry the row");

        // Validation guard: rejects identifiers that would otherwise enable
        // SQL injection through the branch / source / table names.
        assert!(
            adapter
                .clone_table_for_branch(&source, "branch; DROP TABLE demo.orders; --")
                .await
                .is_err(),
            "branch_schema must be validated as a SQL identifier"
        );
    }

    /// Live conformance test for the Arrow inter-adapter path.
    ///
    /// Marked `#[ignore]` because it links the bundled DuckDB C++ binary
    /// (already in the dev dependency graph, but the explicit gate keeps
    /// the default `cargo test` profile lean) and exercises the
    /// `query_arrow` → workspace-arrow-58 path end-to-end. Run with:
    ///
    /// ```bash
    /// cargo test -p rocky-duckdb -- --ignored fetch_arrow_batch
    /// ```
    #[ignore]
    #[tokio::test]
    async fn fetch_arrow_batch_returns_workspace_arrow_batch() {
        use arrow::array::{Int32Array, StringArray};
        use arrow::datatypes::DataType;

        let adapter = DuckDbWarehouseAdapter::in_memory().unwrap();
        let batch = adapter
            .fetch_arrow_batch("SELECT 1 AS n, 'foo' AS s")
            .await
            .expect("fetch_arrow_batch should succeed on rocky-duckdb");

        // Schema — 2 columns, named + typed as expected.
        let schema = batch.schema();
        assert_eq!(schema.fields().len(), 2, "expected 2 columns");
        assert_eq!(schema.field(0).name(), "n");
        assert_eq!(schema.field(0).data_type(), &DataType::Int32);
        assert_eq!(schema.field(1).name(), "s");
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);

        // Rows + values.
        assert_eq!(batch.num_rows(), 1);
        let n = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("column 0 must downcast to Int32Array");
        assert_eq!(n.value(0), 1);
        let s = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("column 1 must downcast to StringArray");
        assert_eq!(s.value(0), "foo");
    }
}
