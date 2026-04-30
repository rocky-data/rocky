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

use async_trait::async_trait;

use rocky_core::ir::{ColumnInfo, TableRef};
use rocky_core::traits::{
    AdapterError, AdapterResult, ExplainResult, QueryResult, SqlDialect, WarehouseAdapter,
};
use rocky_sql::validation;

use crate::DuckDbConnector;
use crate::dialect::DuckDbSqlDialect;

/// DuckDB warehouse adapter wrapping [`DuckDbConnector`] behind the
/// [`WarehouseAdapter`] trait.
///
/// Uses `Arc<Mutex<>>` so the same connector can be shared with a
/// [`crate::discovery::DuckDbDiscoveryAdapter`]. `duckdb::Connection` is
/// `Send` but not `Sync`, so the mutex is required. All DuckDB operations
/// are synchronous and fast, so holding the mutex in an async context is
/// acceptable.
pub struct DuckDbWarehouseAdapter {
    connector: Arc<Mutex<DuckDbConnector>>,
    dialect: DuckDbSqlDialect,
}

impl DuckDbWarehouseAdapter {
    /// Create an adapter wrapping an in-memory DuckDB database.
    pub fn in_memory() -> Result<Self, crate::DuckDbError> {
        let connector = DuckDbConnector::in_memory()?;
        Ok(Self {
            connector: Arc::new(Mutex::new(connector)),
            dialect: DuckDbSqlDialect,
        })
    }

    /// Create an adapter wrapping a persistent DuckDB database file.
    pub fn open(path: &std::path::Path) -> Result<Self, crate::DuckDbError> {
        let connector = DuckDbConnector::open(path)?;
        Ok(Self {
            connector: Arc::new(Mutex::new(connector)),
            dialect: DuckDbSqlDialect,
        })
    }

    /// Create an adapter from an existing connector.
    pub fn new(connector: DuckDbConnector) -> Self {
        Self {
            connector: Arc::new(Mutex::new(connector)),
            dialect: DuckDbSqlDialect,
        }
    }

    /// Create an adapter from an existing shared connector.
    /// Used by the registry when registering DuckDB as both warehouse and
    /// discovery from a single `[adapter.NAME]` block.
    pub fn from_shared(connector: Arc<Mutex<DuckDbConnector>>) -> Self {
        Self {
            connector,
            dialect: DuckDbSqlDialect,
        }
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
        let conn = self
            .connector
            .lock()
            .map_err(|e| AdapterError::msg(format!("mutex poisoned: {e}")))?;
        conn.execute_statement(sql).map_err(AdapterError::new)
    }

    async fn execute_query(&self, sql: &str) -> AdapterResult<QueryResult> {
        let conn = self
            .connector
            .lock()
            .map_err(|e| AdapterError::msg(format!("mutex poisoned: {e}")))?;
        let result = conn.execute_sql(sql).map_err(AdapterError::new)?;
        Ok(QueryResult {
            columns: result.columns,
            rows: result.rows,
        })
    }

    async fn describe_table(&self, table: &TableRef) -> AdapterResult<Vec<ColumnInfo>> {
        let table_ref = if table.catalog.is_empty() {
            format!("{}.{}", table.schema, table.table)
        } else {
            format!("{}.{}.{}", table.catalog, table.schema, table.table)
        };

        let conn = self
            .connector
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
    }

    async fn explain(&self, sql: &str) -> AdapterResult<ExplainResult> {
        let explain_sql = format!("EXPLAIN {sql}");
        let conn = self
            .connector
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
    }

    async fn list_tables(&self, _catalog: &str, schema: &str) -> AdapterResult<Vec<String>> {
        validation::validate_identifier(schema).map_err(AdapterError::new)?;
        let sql = format!(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema}'"
        );
        let conn = self
            .connector
            .lock()
            .map_err(|e| AdapterError::msg(format!("mutex poisoned: {e}")))?;
        let result = conn.execute_sql(&sql).map_err(AdapterError::new)?;
        let tables = result
            .rows
            .iter()
            .filter_map(|row| row.first().and_then(|v| v.as_str()).map(str::to_lowercase))
            .collect();
        Ok(tables)
    }
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
}
