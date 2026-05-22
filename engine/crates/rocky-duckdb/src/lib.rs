//! DuckDB local execution adapter for Rocky.
//!
//! Provides the same query interface as `rocky-databricks` but backed by
//! an in-process DuckDB database. Useful for:
//! - Local development and testing without a Databricks warehouse
//! - CI/CD pipeline validation
//! - Unit testing SQL generation output

pub mod adapter;
pub mod dialect;
pub mod discovery;
pub mod loader;
pub mod seed;
pub mod test_harness;
pub mod types;

use std::path::Path;

use duckdb::{Connection, params, types::Value};
use thiserror::Error;
use tracing::debug;

#[derive(Debug, Error)]
pub enum DuckDbError {
    #[error("DuckDB error: {0}")]
    Database(#[from] duckdb::Error),

    #[error("no rows returned")]
    NoRows,

    /// Arrow IPC encode/decode failure on the `fetch_arrow_batch` bridge.
    #[error("Arrow IPC error: {0}")]
    Arrow(String),
}

/// Query result matching the structure of rocky-databricks QueryResult.
#[derive(Debug)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
}

/// DuckDB local connector for testing and development.
pub struct DuckDbConnector {
    conn: Connection,
}

impl DuckDbConnector {
    /// Creates an in-memory DuckDB database.
    pub fn in_memory() -> Result<Self, DuckDbError> {
        let conn = Connection::open_in_memory()?;
        Ok(DuckDbConnector { conn })
    }

    /// Opens or creates a persistent DuckDB database file.
    pub fn open(path: &Path) -> Result<Self, DuckDbError> {
        let conn = Connection::open(path)?;
        Ok(DuckDbConnector { conn })
    }

    /// Executes a SQL statement and returns results.
    pub fn execute_sql(&self, sql: &str) -> Result<QueryResult, DuckDbError> {
        debug!(sql = sql, "executing DuckDB SQL");

        let mut stmt = self.conn.prepare(sql)?;
        let mut result_rows = stmt.query(params![])?;

        // Get column info from the result set
        let stmt_ref = result_rows.as_ref().expect("query not executed");
        let column_count = stmt_ref.column_count();
        let columns: Vec<String> = (0..column_count)
            .map(|i| stmt_ref.column_name(i).map_or("?", |v| v).to_string())
            .collect();

        let mut rows = Vec::new();
        while let Some(row) = result_rows.next()? {
            let mut values = Vec::with_capacity(column_count);
            for i in 0..column_count {
                let val: Value = row.get(i)?;
                let json_val = match val {
                    Value::Null => serde_json::Value::Null,
                    Value::Boolean(b) => serde_json::Value::String(b.to_string()),
                    Value::TinyInt(n) => serde_json::Value::String(n.to_string()),
                    Value::SmallInt(n) => serde_json::Value::String(n.to_string()),
                    Value::Int(n) => serde_json::Value::String(n.to_string()),
                    Value::BigInt(n) => serde_json::Value::String(n.to_string()),
                    // Unsigned integers — DuckDB's `hash()` returns UBIGINT
                    // and `SUM(hash(...))` widens to HUGEINT, so both arms
                    // matter for partition-checksum aggregation (Layer 0).
                    Value::UTinyInt(n) => serde_json::Value::String(n.to_string()),
                    Value::USmallInt(n) => serde_json::Value::String(n.to_string()),
                    Value::UInt(n) => serde_json::Value::String(n.to_string()),
                    Value::UBigInt(n) => serde_json::Value::String(n.to_string()),
                    Value::HugeInt(n) => serde_json::Value::String(n.to_string()),
                    Value::Float(n) => serde_json::Value::String(n.to_string()),
                    Value::Double(n) => serde_json::Value::String(n.to_string()),
                    Value::Text(s) => serde_json::Value::String(s),
                    Value::Timestamp(unit, val) => {
                        // Convert timestamp to string representation
                        let secs = match unit {
                            duckdb::types::TimeUnit::Second => val,
                            duckdb::types::TimeUnit::Millisecond => val / 1_000,
                            duckdb::types::TimeUnit::Microsecond => val / 1_000_000,
                            duckdb::types::TimeUnit::Nanosecond => val / 1_000_000_000,
                        };
                        serde_json::Value::String(
                            chrono::DateTime::from_timestamp(secs, 0)
                                .map(|dt| dt.to_rfc3339())
                                .unwrap_or_default(),
                        )
                    }
                    _ => serde_json::Value::String(format!("{val:?}")),
                };
                values.push(json_val);
            }
            rows.push(values);
        }

        Ok(QueryResult { columns, rows })
    }

    /// Executes a SQL statement without returning results.
    pub fn execute_statement(&self, sql: &str) -> Result<(), DuckDbError> {
        debug!(sql = sql, "executing DuckDB statement");
        self.conn.execute_batch(sql)?;
        Ok(())
    }

    /// Execute `sql` via DuckDB's native `query_arrow` and return the
    /// resulting batches concatenated and serialized as Arrow IPC stream
    /// bytes.
    ///
    /// This is the producer half of the
    /// [`crate::adapter::DuckDbWarehouseAdapter::fetch_arrow_batch`] PoC
    /// bridge — keeping the arrow-56 types inside this function avoids
    /// leaking the duckdb-pinned arrow major across the crate's public
    /// surface. The consumer side (workspace arrow 58) decodes the bytes
    /// back into a `RecordBatch` via `arrow::ipc::reader::StreamReader`.
    ///
    /// # Errors
    ///
    /// Surfaces any underlying [`duckdb::Error`] from `prepare` /
    /// `query_arrow`, plus arrow encode failures wrapped as
    /// [`DuckDbError::Arrow`].
    pub fn query_arrow_ipc_bytes(&self, sql: &str) -> Result<Vec<u8>, DuckDbError> {
        use arrow56::ipc::writer::StreamWriter;
        use arrow56::record_batch::RecordBatch as Arrow56RecordBatch;

        debug!(sql = sql, "executing DuckDB query_arrow");
        let mut stmt = self.conn.prepare(sql)?;
        let batches: Vec<Arrow56RecordBatch> = stmt.query_arrow(params![])?.collect();

        // Empty result sets are legal. Fall back to an empty schema so the
        // IPC stream still carries a valid schema header.
        let schema = batches.first().map_or_else(
            || std::sync::Arc::new(arrow56::datatypes::Schema::empty()),
            Arrow56RecordBatch::schema,
        );

        let combined = if batches.len() <= 1 {
            batches.into_iter().next()
        } else {
            Some(
                arrow56::compute::concat_batches(&schema, batches.iter())
                    .map_err(|e| DuckDbError::Arrow(e.to_string()))?,
            )
        };

        let mut buf: Vec<u8> = Vec::new();
        let mut writer = StreamWriter::try_new(&mut buf, &schema)
            .map_err(|e| DuckDbError::Arrow(e.to_string()))?;
        if let Some(batch) = combined {
            writer
                .write(&batch)
                .map_err(|e| DuckDbError::Arrow(e.to_string()))?;
        }
        writer
            .finish()
            .map_err(|e| DuckDbError::Arrow(e.to_string()))?;
        Ok(buf)
    }

    /// Creates a table from a schema definition (for test setup).
    pub fn create_table(&self, table: &str, columns: &[(&str, &str)]) -> Result<(), DuckDbError> {
        let cols: Vec<String> = columns
            .iter()
            .map(|(name, dtype)| format!("{name} {dtype}"))
            .collect();
        let sql = format!("CREATE TABLE IF NOT EXISTS {table} ({})", cols.join(", "));
        self.execute_statement(&sql)
    }

    /// Inserts a row into a table (for test setup).
    pub fn insert_row(&self, table: &str, values: &[&str]) -> Result<(), DuckDbError> {
        let sql = format!("INSERT INTO {table} VALUES ({})", values.join(", "));
        self.execute_statement(&sql)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_in_memory() {
        let db = DuckDbConnector::in_memory().unwrap();
        let result = db.execute_sql("SELECT 1 AS value").unwrap();
        assert_eq!(result.columns, vec!["value"]);
        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn test_create_and_query() {
        let db = DuckDbConnector::in_memory().unwrap();
        db.create_table("users", &[("id", "INTEGER"), ("name", "VARCHAR")])
            .unwrap();
        db.insert_row("users", &["1", "'Alice'"]).unwrap();
        db.insert_row("users", &["2", "'Bob'"]).unwrap();

        let result = db.execute_sql("SELECT * FROM users ORDER BY id").unwrap();
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][1], "Alice");
        assert_eq!(result.rows[1][1], "Bob");
    }

    #[test]
    fn test_count() {
        let db = DuckDbConnector::in_memory().unwrap();
        db.create_table("t", &[("id", "INTEGER")]).unwrap();
        db.insert_row("t", &["1"]).unwrap();
        db.insert_row("t", &["2"]).unwrap();
        db.insert_row("t", &["3"]).unwrap();

        let result = db.execute_sql("SELECT COUNT(*) AS cnt FROM t").unwrap();
        assert_eq!(result.rows[0][0], "3");
    }

    #[test]
    fn test_join() {
        let db = DuckDbConnector::in_memory().unwrap();
        db.create_table("orders", &[("id", "INTEGER"), ("customer_id", "INTEGER")])
            .unwrap();
        db.create_table("customers", &[("id", "INTEGER"), ("name", "VARCHAR")])
            .unwrap();

        db.insert_row("orders", &["1", "10"]).unwrap();
        db.insert_row("customers", &["10", "'Alice'"]).unwrap();

        let result = db
            .execute_sql(
                "SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id",
            )
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][1], "Alice");
    }

    #[test]
    fn test_ctas() {
        let db = DuckDbConnector::in_memory().unwrap();
        db.create_table("source", &[("id", "INTEGER"), ("val", "VARCHAR")])
            .unwrap();
        db.insert_row("source", &["1", "'hello'"]).unwrap();

        db.execute_statement("CREATE OR REPLACE TABLE target AS SELECT id, val FROM source")
            .unwrap();

        let result = db.execute_sql("SELECT * FROM target").unwrap();
        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn test_incremental_pattern() {
        let db = DuckDbConnector::in_memory().unwrap();
        db.create_table("source", &[("id", "INTEGER"), ("ts", "TIMESTAMP")])
            .unwrap();
        db.create_table("target", &[("id", "INTEGER"), ("ts", "TIMESTAMP")])
            .unwrap();

        db.insert_row("source", &["1", "TIMESTAMP '2026-01-01'"])
            .unwrap();
        db.insert_row("source", &["2", "TIMESTAMP '2026-03-01'"])
            .unwrap();
        db.insert_row("target", &["1", "TIMESTAMP '2026-01-01'"])
            .unwrap();

        // Incremental: only rows newer than target max
        db.execute_statement(
            "INSERT INTO target SELECT * FROM source WHERE ts > (SELECT COALESCE(MAX(ts), TIMESTAMP '1970-01-01') FROM target)",
        )
        .unwrap();

        let result = db
            .execute_sql("SELECT COUNT(*) AS cnt FROM target")
            .unwrap();
        assert_eq!(result.rows[0][0], "2"); // 1 existing + 1 new
    }

    #[test]
    fn test_rocky_generated_sql_pattern() {
        let db = DuckDbConnector::in_memory().unwrap();
        db.create_table(
            "source_catalog__source_schema__my_table",
            &[
                ("id", "INTEGER"),
                ("name", "VARCHAR"),
                ("_fivetran_synced", "TIMESTAMP"),
            ],
        )
        .unwrap();

        db.insert_row(
            "source_catalog__source_schema__my_table",
            &["1", "'test'", "TIMESTAMP '2026-03-29'"],
        )
        .unwrap();

        // Test the full refresh pattern Rocky generates (adapted for DuckDB single-catalog)
        let sql = "CREATE OR REPLACE TABLE target_table AS\n\
                    SELECT *, CAST(NULL AS VARCHAR) AS _loaded_by\n\
                    FROM source_catalog__source_schema__my_table";

        db.execute_statement(sql).unwrap();

        let result = db.execute_sql("SELECT * FROM target_table").unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.columns.len(), 4); // id, name, _fivetran_synced, _loaded_by
    }
}
