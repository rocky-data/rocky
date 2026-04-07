//! Snowflake warehouse adapter implementing [`WarehouseAdapter`].

use async_trait::async_trait;

use rocky_core::ir::{ColumnInfo, TableRef};
use rocky_core::traits::{AdapterError, AdapterResult, QueryResult, SqlDialect, WarehouseAdapter};
use rocky_sql::validation;

use crate::connector::SnowflakeConnector;
use crate::dialect::SnowflakeSqlDialect;

/// Snowflake warehouse adapter wrapping [`SnowflakeConnector`] behind the
/// [`WarehouseAdapter`] trait.
pub struct SnowflakeWarehouseAdapter {
    connector: SnowflakeConnector,
    dialect: SnowflakeSqlDialect,
}

impl SnowflakeWarehouseAdapter {
    pub fn new(connector: SnowflakeConnector) -> Self {
        Self {
            connector,
            dialect: SnowflakeSqlDialect,
        }
    }

    /// Access the underlying connector (for adapter-specific operations).
    pub fn connector(&self) -> &SnowflakeConnector {
        &self.connector
    }
}

#[async_trait]
impl WarehouseAdapter for SnowflakeWarehouseAdapter {
    fn dialect(&self) -> &dyn SqlDialect {
        &self.dialect
    }

    async fn execute_statement(&self, sql: &str) -> AdapterResult<()> {
        self.connector
            .execute_statement(sql)
            .await
            .map(|_| ())
            .map_err(AdapterError::new)
    }

    async fn execute_query(&self, sql: &str) -> AdapterResult<QueryResult> {
        let result = self
            .connector
            .execute_sql(sql)
            .await
            .map_err(AdapterError::new)?;
        Ok(QueryResult {
            columns: result.columns.iter().map(|c| c.name.clone()).collect(),
            rows: result.rows,
        })
    }

    async fn describe_table(&self, table: &TableRef) -> AdapterResult<Vec<ColumnInfo>> {
        let table_ref =
            self.dialect
                .format_table_ref(&table.catalog, &table.schema, &table.table)?;
        let describe_sql = self.dialect.describe_table_sql(&table_ref);
        let result = self
            .connector
            .execute_sql(&describe_sql)
            .await
            .map_err(AdapterError::new)?;

        // Snowflake DESCRIBE TABLE returns rows with columns:
        // name, type, kind, null?, default, primary_key, unique_key, ...
        // Look up column positions by name so we're resilient to column
        // order changes across Snowflake versions.
        let col_headers: Vec<String> = result
            .columns
            .iter()
            .map(|c| c.name.to_lowercase())
            .collect();
        let name_idx = col_headers.iter().position(|c| c == "name").unwrap_or(0);
        let type_idx = col_headers.iter().position(|c| c == "type").unwrap_or(1);
        let null_idx = col_headers
            .iter()
            .position(|c| c == "null?")
            .unwrap_or(3);

        let mut columns = Vec::new();
        for row in &result.rows {
            let name = row
                .get(name_idx)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_lowercase();
            let data_type = row
                .get(type_idx)
                .and_then(|v| v.as_str())
                .unwrap_or("VARCHAR")
                .to_string();
            let nullable = row
                .get(null_idx)
                .and_then(|v| v.as_str())
                .map(|s| s == "Y")
                .unwrap_or(true);

            columns.push(ColumnInfo {
                name,
                data_type,
                nullable,
            });
        }

        Ok(columns)
    }

    async fn ping(&self) -> AdapterResult<()> {
        // Cheaper than SELECT 1 — no compute warehouse query, just a session check
        self.connector
            .execute_statement("SELECT CURRENT_WAREHOUSE()")
            .await
            .map(|_| ())
            .map_err(AdapterError::new)
    }

    async fn list_tables(&self, catalog: &str, schema: &str) -> AdapterResult<Vec<String>> {
        validation::validate_identifier(catalog).map_err(AdapterError::new)?;
        validation::validate_identifier(schema).map_err(AdapterError::new)?;
        // Snowflake: SELECT table_name FROM <database>.information_schema.tables
        let sql = format!(
            "SELECT table_name FROM {catalog}.information_schema.tables \
             WHERE table_schema = '{schema}'"
        );
        let result = self
            .connector
            .execute_sql(&sql)
            .await
            .map_err(AdapterError::new)?;
        let tables = result
            .rows
            .iter()
            .filter_map(|row| {
                row.first()
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_lowercase())
            })
            .collect();
        Ok(tables)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::{Auth, AuthConfig};
    use crate::connector::ConnectorConfig;
    use rocky_core::config::RetryConfig;
    use std::time::Duration;

    /// Verifies that the adapter can be constructed and used as a trait object.
    fn _assert_warehouse_adapter_trait_object(_: &dyn WarehouseAdapter) {}

    /// Verifies construction compiles and the dialect is correct.
    #[test]
    fn test_adapter_construction() {
        let auth = Auth::from_config(AuthConfig {
            account: "test_account".into(),
            username: None,
            password: None,
            oauth_token: Some("test_token".into()),
            private_key_path: None,
        })
        .unwrap();

        let config = ConnectorConfig {
            account: "test_account".into(),
            warehouse: "COMPUTE_WH".into(),
            database: Some("MY_DB".into()),
            schema: None,
            role: None,
            timeout: Duration::from_secs(120),
            retry: RetryConfig::default(),
        };

        let connector = SnowflakeConnector::new(config, auth);
        let adapter = SnowflakeWarehouseAdapter::new(connector);

        // Verify dialect produces Snowflake-specific SQL
        let sql = adapter
            .dialect()
            .create_catalog_sql("my_warehouse")
            .unwrap()
            .unwrap();
        assert!(sql.contains("DATABASE"));
        assert!(!sql.contains("CATALOG"));
    }

    #[test]
    fn test_adapter_dialect_format_table_ref() {
        let auth = Auth::from_config(AuthConfig {
            account: "test_account".into(),
            username: None,
            password: None,
            oauth_token: Some("token".into()),
            private_key_path: None,
        })
        .unwrap();

        let config = ConnectorConfig {
            account: "test_account".into(),
            warehouse: "WH".into(),
            database: None,
            schema: None,
            role: None,
            timeout: Duration::from_secs(60),
            retry: RetryConfig::default(),
        };

        let connector = SnowflakeConnector::new(config, auth);
        let adapter = SnowflakeWarehouseAdapter::new(connector);

        let ref_str = adapter
            .dialect()
            .format_table_ref("mydb", "public", "users")
            .unwrap();
        assert_eq!(ref_str, "mydb.public.users");
    }

    #[test]
    fn test_adapter_connector_access() {
        let auth = Auth::from_config(AuthConfig {
            account: "test_account".into(),
            username: None,
            password: None,
            oauth_token: Some("token".into()),
            private_key_path: None,
        })
        .unwrap();

        let config = ConnectorConfig {
            account: "test_account".into(),
            warehouse: "WH".into(),
            database: None,
            schema: None,
            role: None,
            timeout: Duration::from_secs(60),
            retry: RetryConfig::default(),
        };

        let connector = SnowflakeConnector::new(config, auth);
        let adapter = SnowflakeWarehouseAdapter::new(connector);

        // Just verify we can access the connector without panic
        let _connector_ref = adapter.connector();
    }
}
