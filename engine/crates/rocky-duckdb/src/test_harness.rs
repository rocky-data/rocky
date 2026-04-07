//! Reusable test harness for DuckDB-backed integration tests.
//!
//! Provides [`TestPipeline`] which bundles a [`DuckDbWarehouseAdapter`],
//! [`StateStore`], and temporary directory into a single object for
//! convenient test setup.

use std::path::PathBuf;

use rocky_core::ir::{ColumnInfo, TableRef};
use rocky_core::state::StateStore;
use rocky_core::traits::WarehouseAdapter;
use tempfile::TempDir;

use crate::DuckDbConnector;
use crate::adapter::DuckDbWarehouseAdapter;
use crate::seed::{SeedScale, SeedStats, seed_database};

/// A self-contained test environment with DuckDB adapter and state store.
///
/// The temporary directory and all data are cleaned up when the
/// `TestPipeline` is dropped.
pub struct TestPipeline {
    adapter: DuckDbWarehouseAdapter,
    state: StateStore,
    _temp_dir: TempDir,
}

impl Default for TestPipeline {
    fn default() -> Self {
        Self::new()
    }
}

impl TestPipeline {
    /// Creates a new test pipeline with an empty in-memory DuckDB database
    /// and a temporary state store.
    pub fn new() -> Self {
        let temp_dir = TempDir::new().expect("failed to create temp dir");
        let state_path = temp_dir.path().join("test-state.redb");
        let state = StateStore::open(&state_path).expect("failed to open state store");
        let adapter = DuckDbWarehouseAdapter::in_memory().expect("failed to create DuckDB adapter");

        TestPipeline {
            adapter,
            state,
            _temp_dir: temp_dir,
        }
    }

    /// Creates a test pipeline seeded with test data at the given scale.
    pub fn with_seed(scale: SeedScale) -> (Self, SeedStats) {
        // Use a persistent file so seed and adapter share the same database.
        let temp_dir = TempDir::new().expect("failed to create temp dir");
        let db_path = temp_dir.path().join("test.duckdb");
        let state_path = temp_dir.path().join("test-state.redb");

        let conn = DuckDbConnector::open(&db_path).expect("failed to open DuckDB");
        let stats = seed_database(&conn, scale).expect("failed to seed database");
        let adapter = DuckDbWarehouseAdapter::open(&db_path).expect("failed to open adapter");
        let state = StateStore::open(&state_path).expect("failed to open state store");

        let pipeline = TestPipeline {
            adapter,
            state,
            _temp_dir: temp_dir,
        };

        (pipeline, stats)
    }

    /// Returns a reference to the warehouse adapter.
    pub fn adapter(&self) -> &DuckDbWarehouseAdapter {
        &self.adapter
    }

    /// Returns a reference to the state store.
    pub fn state(&self) -> &StateStore {
        &self.state
    }

    /// Execute a SQL statement (CREATE, INSERT, etc.) against the database.
    pub async fn execute(&self, sql: &str) -> Result<(), String> {
        self.adapter
            .execute_statement(sql)
            .await
            .map_err(|e| e.to_string())
    }

    /// Execute a query and return results.
    pub async fn query(&self, sql: &str) -> Result<rocky_core::traits::QueryResult, String> {
        self.adapter
            .execute_query(sql)
            .await
            .map_err(|e| e.to_string())
    }

    /// Get the row count for a table.
    pub async fn row_count(&self, table: &str) -> Result<u64, String> {
        let result = self
            .adapter
            .execute_query(&format!("SELECT COUNT(*) AS cnt FROM {table}"))
            .await
            .map_err(|e| e.to_string())?;

        result.rows[0][0]
            .as_str()
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| "failed to parse row count".to_string())
    }

    /// Describe a table's columns.
    pub async fn describe(&self, schema: &str, table: &str) -> Result<Vec<ColumnInfo>, String> {
        let table_ref = TableRef {
            catalog: String::new(),
            schema: schema.to_string(),
            table: table.to_string(),
        };
        self.adapter
            .describe_table(&table_ref)
            .await
            .map_err(|e| e.to_string())
    }

    /// Check if a table exists.
    pub async fn table_exists(&self, schema: &str, table: &str) -> bool {
        let sql = format!(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '{schema}' AND table_name = '{table}'"
        );
        self.adapter
            .execute_query(&sql)
            .await
            .ok()
            .and_then(|r| r.rows.first()?.first()?.as_str().map(|s| s != "0"))
            .unwrap_or(false)
    }

    /// Seed a flat table for simple test scenarios.
    pub fn seed_table(
        &self,
        schema: &str,
        table: &str,
        columns: &[(&str, &str)],
        row_count: u64,
    ) -> Result<u64, String> {
        // For in-memory adapter, we need to use execute_statement
        // because we can't get the underlying connector.
        // Build the SQL directly.
        let col_defs: Vec<String> = columns
            .iter()
            .map(|(name, dtype)| format!("{name} {dtype}"))
            .collect();

        let schema_sql = format!("CREATE SCHEMA IF NOT EXISTS {schema}");
        let create_sql = format!(
            "CREATE OR REPLACE TABLE {schema}.{table} ({})",
            col_defs.join(", ")
        );

        // Generate column expressions
        let col_exprs: Vec<String> = columns
            .iter()
            .map(|(name, dtype)| {
                let dt = dtype.to_uppercase();
                let expr = if dt.contains("INT") {
                    "i".to_string()
                } else if dt.contains("VARCHAR") || dt.contains("TEXT") || dt.contains("STRING") {
                    format!("'{name}_' || i")
                } else if dt.contains("DECIMAL") || dt.contains("FLOAT") || dt.contains("DOUBLE") {
                    "ROUND(CAST(random() * 1000.0 AS DECIMAL(10,2)), 2)".to_string()
                } else if dt.contains("TIMESTAMP") {
                    format!(
                        "TIMESTAMP '2026-01-01' + INTERVAL (i * 86400 * 90 / {row_count}) SECOND"
                    )
                } else if dt.contains("BOOL") {
                    "CASE WHEN random() < 0.5 THEN true ELSE false END".to_string()
                } else {
                    "i".to_string()
                };
                format!("{expr} AS {name}")
            })
            .collect();

        let insert_sql = format!(
            "INSERT INTO {schema}.{table} SELECT {} FROM generate_series(1, {row_count}) AS t(i)",
            col_exprs.join(", ")
        );

        // Execute synchronously via the adapter's blocking interface
        // We use tokio's block_in_place since tests run in a tokio runtime
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                self.adapter
                    .execute_statement("SELECT SETSEED(0.42)")
                    .await
                    .map_err(|e| e.to_string())?;
                self.adapter
                    .execute_statement(&schema_sql)
                    .await
                    .map_err(|e| e.to_string())?;
                self.adapter
                    .execute_statement(&create_sql)
                    .await
                    .map_err(|e| e.to_string())?;
                self.adapter
                    .execute_statement(&insert_sql)
                    .await
                    .map_err(|e| e.to_string())?;
                Ok::<_, String>(())
            })
        })?;

        Ok(row_count)
    }

    /// Return the path to the temporary directory (for state files, etc.).
    pub fn temp_dir(&self) -> PathBuf {
        self._temp_dir.path().to_path_buf()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_new_pipeline() {
        let p = TestPipeline::new();
        p.execute("CREATE TABLE t (id INTEGER)").await.unwrap();
        p.execute("INSERT INTO t VALUES (1), (2), (3)")
            .await
            .unwrap();
        assert_eq!(p.row_count("t").await.unwrap(), 3);
    }

    #[tokio::test]
    async fn test_with_seed() {
        let (p, stats) = TestPipeline::with_seed(SeedScale::Small);
        assert_eq!(stats.tables.len(), 3);
        assert_eq!(p.row_count("source.raw_orders").await.unwrap(), 10_000);
    }

    #[tokio::test]
    async fn test_table_exists() {
        let p = TestPipeline::new();
        assert!(!p.table_exists("main", "foo").await);
        p.execute("CREATE TABLE foo (id INTEGER)").await.unwrap();
        assert!(p.table_exists("main", "foo").await);
    }

    #[tokio::test]
    async fn test_describe() {
        let p = TestPipeline::new();
        p.execute("CREATE TABLE main.t (id INTEGER NOT NULL, name VARCHAR, amount DECIMAL(10,2))")
            .await
            .unwrap();
        let cols = p.describe("main", "t").await.unwrap();
        assert_eq!(cols.len(), 3);
        assert_eq!(cols[0].name, "id");
        assert!(!cols[0].nullable);
    }
}
