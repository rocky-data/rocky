//! DuckDB discovery adapter implementing [`DiscoveryAdapter`].
//!
//! Lists schemas + tables in a DuckDB database that match a given prefix,
//! returning them as [`DiscoveredConnector`]s. Useful for the playground
//! and local-only pipelines that don't want a real source like Fivetran.
//!
//! Discovery and the warehouse adapter share a single [`DuckDbConnector`]
//! via `Arc<Mutex<>>` so writes from one are visible to the other.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use rocky_core::source::{DiscoveredConnector, DiscoveredTable, DiscoveryResult};
use rocky_core::sql_gen::string_literal;
use rocky_core::traits::{AdapterError, AdapterResult, DiscoveryAdapter};

use crate::DuckDbConnector;
use crate::dialect::DuckDbSqlDialect;

/// DuckDB discovery adapter that lists schemas + tables matching a prefix.
pub struct DuckDbDiscoveryAdapter {
    connector: Arc<Mutex<DuckDbConnector>>,
}

impl DuckDbDiscoveryAdapter {
    pub fn new(connector: Arc<Mutex<DuckDbConnector>>) -> Self {
        Self { connector }
    }
}

#[async_trait]
impl DiscoveryAdapter for DuckDbDiscoveryAdapter {
    async fn discover(&self, schema_prefix: &str) -> AdapterResult<DiscoveryResult> {
        let conn = self
            .connector
            .lock()
            .map_err(|e| AdapterError::msg(format!("mutex poisoned: {e}")))?;

        // 1. Find schemas matching the prefix. The prefix is user text from
        // `schema_pattern.prefix`, encoded by this dialect's own lexer rule.
        //
        // `starts_with`, not `LIKE '<prefix>%'`: in a LIKE pattern `_` matches
        // any one character, so a prefix like `src__` also matched `srcab`
        // (#1649). `starts_with` reads the prefix as plain text, which is what
        // a prefix means, and it drops the `%` too. BigQuery's adapter uses
        // `STARTS_WITH` for the same reason.
        let schema_sql = format!(
            "SELECT schema_name FROM information_schema.schemata \
             WHERE starts_with(schema_name, {}) \
             ORDER BY schema_name",
            string_literal(&DuckDbSqlDialect, schema_prefix)
        );
        let schema_result = conn.execute_sql(&schema_sql).map_err(AdapterError::new)?;

        let mut connectors = Vec::new();
        for row in &schema_result.rows {
            let schema = row
                .first()
                .and_then(|v| v.as_str())
                .ok_or_else(|| AdapterError::msg("schema_name not a string"))?
                .to_string();

            // 2. Find tables in this schema. The name came back from the
            // warehouse and can hold any character, so it is encoded the
            // same way.
            let table_sql = format!(
                "SELECT table_name FROM information_schema.tables \
                 WHERE table_schema = {} AND table_type = 'BASE TABLE' \
                 ORDER BY table_name",
                string_literal(&DuckDbSqlDialect, &schema)
            );
            let table_result = conn.execute_sql(&table_sql).map_err(AdapterError::new)?;

            let tables: Vec<DiscoveredTable> = table_result
                .rows
                .iter()
                .filter_map(|r| r.first().and_then(|v| v.as_str()).map(String::from))
                .map(|name| DiscoveredTable {
                    name,
                    row_count: None,
                })
                .collect();

            connectors.push(DiscoveredConnector {
                id: schema.clone(),
                schema: schema.clone(),
                source_type: "duckdb".to_string(),
                last_sync_at: None,
                tables,
                metadata: Default::default(),
                external_object_ids: Vec::new(),
            });
        }

        // DuckDB discovery is a single-shot information_schema query — there
        // is no per-source metadata fetch that could partially fail.
        Ok(DiscoveryResult::ok(connectors))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Executed proof, not a string assertion: the prefix and each schema
    /// name are encoded by DuckDB's own lexer rule (issue #1596). A schema
    /// whose name ends in a backslash, and one holding a quote, are both
    /// found; a backslash in the prefix matches only itself, because
    /// DuckDB's `LIKE` has no default escape character.
    #[tokio::test]
    async fn discover_finds_schemas_whose_names_hold_a_backslash_or_a_quote() {
        let connector = Arc::new(Mutex::new(DuckDbConnector::in_memory().unwrap()));
        {
            let conn = connector.lock().unwrap();
            for stmt in [
                r#"CREATE SCHEMA "raw\""#,
                r#"CREATE SCHEMA "raw\x""#,
                r#"CREATE SCHEMA "rawx""#,
                r#"CREATE SCHEMA "it's""#,
                r#"CREATE TABLE "it's".t (id INTEGER)"#,
            ] {
                conn.execute_sql(stmt).unwrap();
            }
        }
        let adapter = DuckDbDiscoveryAdapter::new(connector);

        let found = adapter.discover(r"raw\").await.unwrap();
        let names: Vec<&str> = found.connectors.iter().map(|c| c.schema.as_str()).collect();
        assert_eq!(names, [r"raw\", r"raw\x"]);

        let found = adapter.discover("it'").await.unwrap();
        let names: Vec<&str> = found.connectors.iter().map(|c| c.schema.as_str()).collect();
        assert_eq!(names, ["it's"]);
        assert_eq!(found.connectors[0].tables[0].name, "t");
    }

    #[tokio::test]
    async fn test_discover_empty() {
        let connector = Arc::new(Mutex::new(DuckDbConnector::in_memory().unwrap()));
        let adapter = DuckDbDiscoveryAdapter::new(connector);
        let result = adapter.discover("raw__").await.unwrap();
        assert_eq!(result.connectors.len(), 0);
        assert!(result.failed.is_empty());
    }

    #[tokio::test]
    async fn test_discover_matching_schemas() {
        let connector = Arc::new(Mutex::new(DuckDbConnector::in_memory().unwrap()));
        {
            let conn = connector.lock().unwrap();
            conn.execute_statement("CREATE SCHEMA raw__orders").unwrap();
            conn.execute_statement("CREATE TABLE raw__orders.orders (id INT)")
                .unwrap();
            conn.execute_statement("CREATE TABLE raw__orders.line_items (id INT, order_id INT)")
                .unwrap();
            conn.execute_statement("CREATE SCHEMA raw__customers")
                .unwrap();
            conn.execute_statement("CREATE TABLE raw__customers.customers (id INT)")
                .unwrap();
            conn.execute_statement("CREATE SCHEMA other").unwrap();
            conn.execute_statement("CREATE TABLE other.ignored (id INT)")
                .unwrap();
            // `_` is a single-character wildcard in a LIKE pattern, so
            // `LIKE 'raw__%'` also matched this one (#1649).
            conn.execute_statement("CREATE SCHEMA rawXY").unwrap();
            conn.execute_statement("CREATE TABLE rawXY.decoy (id INT)")
                .unwrap();
        }

        let adapter = DuckDbDiscoveryAdapter::new(connector);
        let result = adapter.discover("raw__").await.unwrap();
        assert_eq!(
            result.connectors.len(),
            2,
            "expected exactly the 2 raw__ schemas, not the `rawXY` a LIKE \
             wildcard would also match: {:?}",
            result.connectors
        );
        assert!(result.failed.is_empty());

        let orders = result
            .connectors
            .iter()
            .find(|c| c.schema == "raw__orders")
            .unwrap();
        assert_eq!(orders.tables.len(), 2);
        assert!(orders.tables.iter().any(|t| t.name == "orders"));
        assert!(orders.tables.iter().any(|t| t.name == "line_items"));

        let customers = result
            .connectors
            .iter()
            .find(|c| c.schema == "raw__customers")
            .unwrap();
        assert_eq!(customers.tables.len(), 1);
        assert_eq!(customers.tables[0].name, "customers");
    }
}
