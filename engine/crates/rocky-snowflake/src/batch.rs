//! Batched warehouse operations for Snowflake.
//!
//! The only operation currently implemented is `batch_describe_schema`,
//! which replaces N `DESCRIBE TABLE` calls with one
//! `INFORMATION_SCHEMA.COLUMNS` query per schema. `batch_row_counts` and
//! `batch_freshness` remain unimplemented — callers fall back to the
//! per-table [`WarehouseAdapter`] path for those.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use thiserror::Error;

use rocky_core::ir::{ColumnInfo, TableRef};
use rocky_core::traits::{
    AdapterError, AdapterResult, BatchCheckAdapter, FreshnessResult, RowCountResult,
};
use rocky_sql::validation::{self, ValidationError};

use crate::connector::{ConnectorError, SnowflakeConnector};

#[derive(Debug, Error)]
pub enum BatchError {
    #[error("validation error: {0}")]
    Validation(#[from] ValidationError),

    #[error("connector error: {0}")]
    Connector(#[from] ConnectorError),
}

/// Batched check adapter for Snowflake.
pub struct SnowflakeBatchCheckAdapter {
    connector: Arc<SnowflakeConnector>,
}

impl SnowflakeBatchCheckAdapter {
    pub fn new(connector: Arc<SnowflakeConnector>) -> Self {
        Self { connector }
    }
}

/// Generates the Snowflake `INFORMATION_SCHEMA.COLUMNS` query that describes
/// every table in a single schema in one round trip.
///
/// Snowflake exposes `INFORMATION_SCHEMA.COLUMNS` per database; the query
/// scopes to `<database>.information_schema.columns`. `table_schema` values
/// are stored uppercase in Snowflake, so we use case-insensitive match.
fn generate_batch_describe_sql(catalog: &str, schema: &str) -> Result<String, BatchError> {
    validation::validate_identifier(catalog)?;
    validation::validate_identifier(schema)?;

    Ok(format!(
        "SELECT LOWER(table_name), LOWER(column_name), data_type, is_nullable\n\
         FROM {catalog}.information_schema.columns\n\
         WHERE UPPER(table_schema) = UPPER('{schema}')\n\
         ORDER BY table_name, ordinal_position"
    ))
}

#[async_trait]
impl BatchCheckAdapter for SnowflakeBatchCheckAdapter {
    async fn batch_row_counts(&self, _tables: &[TableRef]) -> AdapterResult<Vec<RowCountResult>> {
        // Snowflake could batch these with a UNION ALL over the supplied
        // tables, but it's not wired yet. Return an error so `run.rs` falls
        // back to per-table row-count queries via `WarehouseAdapter`.
        Err(AdapterError::msg(
            "batch_row_counts not yet implemented for Snowflake",
        ))
    }

    async fn batch_freshness(
        &self,
        _tables: &[TableRef],
        _timestamp_col: &str,
    ) -> AdapterResult<Vec<FreshnessResult>> {
        Err(AdapterError::msg(
            "batch_freshness not yet implemented for Snowflake",
        ))
    }

    async fn batch_describe_schema(
        &self,
        catalog: &str,
        schema: &str,
    ) -> AdapterResult<HashMap<String, Vec<ColumnInfo>>> {
        let sql = generate_batch_describe_sql(catalog, schema).map_err(AdapterError::new)?;
        let result = self
            .connector
            .execute_sql(&sql)
            .await
            .map_err(AdapterError::new)?;

        let mut map: HashMap<String, Vec<ColumnInfo>> = HashMap::new();

        for row in &result.rows {
            let table = row
                .first()
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let col_name = row
                .get(1)
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let data_type = row
                .get(2)
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            // Snowflake's `is_nullable` is "YES" / "NO".
            let nullable = row
                .get(3)
                .and_then(|v| v.as_str())
                .map(|s| s.eq_ignore_ascii_case("YES"))
                .unwrap_or(true);

            if table.is_empty() || col_name.is_empty() {
                continue;
            }

            map.entry(table).or_default().push(ColumnInfo {
                name: col_name,
                data_type,
                nullable,
            });
        }

        Ok(map)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_describe_sql_scopes_to_database_and_schema() {
        let sql = generate_batch_describe_sql("MY_DB", "analytics").unwrap();
        assert!(sql.contains("MY_DB.information_schema.columns"));
        assert!(sql.contains("UPPER('analytics')"));
        assert!(sql.contains("ORDER BY table_name, ordinal_position"));
    }

    #[test]
    fn batch_describe_sql_rejects_invalid_identifiers() {
        assert!(generate_batch_describe_sql("bad; drop table users", "s").is_err());
        assert!(generate_batch_describe_sql("c", "bad'; select *").is_err());
    }
}
