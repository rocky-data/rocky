//! DuckDB SQL dialect implementation.
//!
//! DuckDB differences from Databricks:
//! - Supports both two-part (`schema.table`) and three-part (`catalog.schema.table`) naming
//! - `CREATE OR REPLACE TABLE` works the same way
//! - No `UPDATE SET *` in MERGE — must enumerate columns
//! - No `CREATE CATALOG` — uses databases/schemas
//! - `TABLESAMPLE` uses different syntax: `USING SAMPLE N PERCENT (bernoulli)`

use std::fmt::Write;

use rocky_core::ir::{ColumnSelection, MetadataColumn};
use rocky_core::traits::{AdapterError, AdapterResult, SqlDialect};
use rocky_sql::validation;

/// DuckDB SQL dialect.
#[derive(Debug, Clone, Default)]
pub struct DuckDbSqlDialect;

impl SqlDialect for DuckDbSqlDialect {
    fn format_table_ref(&self, catalog: &str, schema: &str, table: &str) -> AdapterResult<String> {
        // DuckDB supports three-part names; use two-part if catalog is empty
        if catalog.is_empty() {
            validation::validate_identifier(schema).map_err(AdapterError::new)?;
            validation::validate_identifier(table).map_err(AdapterError::new)?;
            Ok(format!("{schema}.{table}"))
        } else {
            validation::format_table_ref(catalog, schema, table).map_err(AdapterError::new)
        }
    }

    fn create_table_as(&self, target: &str, select_sql: &str) -> String {
        format!("CREATE OR REPLACE TABLE {target} AS\n{select_sql}")
    }

    fn insert_into(&self, target: &str, select_sql: &str) -> String {
        format!("INSERT INTO {target}\n{select_sql}")
    }

    fn merge_into(
        &self,
        target: &str,
        source_sql: &str,
        keys: &[String],
        update_cols: &ColumnSelection,
    ) -> AdapterResult<String> {
        if keys.is_empty() {
            return Err(AdapterError::msg(
                "merge strategy requires at least one unique_key column",
            ));
        }

        for key in keys {
            validation::validate_identifier(key).map_err(AdapterError::new)?;
        }

        let on_clause = keys
            .iter()
            .map(|k| format!("t.{k} = s.{k}"))
            .collect::<Vec<_>>()
            .join(" AND ");

        // DuckDB does NOT support `UPDATE SET *` — must enumerate columns
        let update_clause = match update_cols {
            ColumnSelection::All => {
                // DuckDB requires explicit column names — we can't use * here.
                // Use a DELETE + INSERT pattern instead when columns are unknown.
                return Err(AdapterError::msg(
                    "DuckDB MERGE does not support UPDATE SET *. Use explicit update_columns.",
                ));
            }
            ColumnSelection::Explicit(cols) => {
                let sets = cols
                    .iter()
                    .map(|c| {
                        validation::validate_identifier(c).map_err(AdapterError::new)?;
                        Ok(format!("t.{c} = s.{c}"))
                    })
                    .collect::<AdapterResult<Vec<_>>>()?;
                format!("UPDATE SET {}", sets.join(", "))
            }
        };

        Ok(format!(
            "MERGE INTO {target} AS t\n\
             USING (\n{source_sql}\n) AS s\n\
             ON {on_clause}\n\
             WHEN MATCHED THEN {update_clause}\n\
             WHEN NOT MATCHED THEN INSERT *"
        ))
    }

    fn select_clause(
        &self,
        columns: &ColumnSelection,
        metadata: &[MetadataColumn],
    ) -> AdapterResult<String> {
        let mut sql = String::from("SELECT ");

        match columns {
            ColumnSelection::All => sql.push('*'),
            ColumnSelection::Explicit(cols) => {
                for (i, col) in cols.iter().enumerate() {
                    validation::validate_identifier(col).map_err(AdapterError::new)?;
                    if i > 0 {
                        sql.push_str(", ");
                    }
                    sql.push_str(col);
                }
            }
        }

        for mc in metadata {
            validation::validate_identifier(&mc.name).map_err(AdapterError::new)?;
            write!(
                sql,
                ", CAST({} AS {}) AS {}",
                mc.value, mc.data_type, mc.name
            )
            .unwrap();
        }

        Ok(sql)
    }

    fn watermark_where(&self, timestamp_col: &str, target_ref: &str) -> AdapterResult<String> {
        validation::validate_identifier(timestamp_col).map_err(AdapterError::new)?;
        Ok(format!(
            "WHERE {timestamp_col} > (\n\
             \x20   SELECT COALESCE(MAX({timestamp_col}), TIMESTAMP '1970-01-01')\n\
             \x20   FROM {target_ref}\n\
             )"
        ))
    }

    fn describe_table_sql(&self, table_ref: &str) -> String {
        format!("DESCRIBE {table_ref}")
    }

    fn drop_table_sql(&self, table_ref: &str) -> String {
        format!("DROP TABLE IF EXISTS {table_ref}")
    }

    fn create_catalog_sql(&self, _name: &str) -> Option<AdapterResult<String>> {
        // DuckDB doesn't have catalogs in the Databricks sense
        None
    }

    fn create_schema_sql(&self, _catalog: &str, schema: &str) -> Option<AdapterResult<String>> {
        Some(
            validation::validate_identifier(schema)
                .map(|_| format!("CREATE SCHEMA IF NOT EXISTS {schema}"))
                .map_err(AdapterError::new),
        )
    }

    fn tablesample_clause(&self, percent: u32) -> Option<String> {
        Some(format!("USING SAMPLE {percent} PERCENT (bernoulli)"))
    }

    fn insert_overwrite_partition(
        &self,
        target: &str,
        partition_filter: &str,
        select_sql: &str,
    ) -> AdapterResult<Vec<String>> {
        // DuckDB has full transaction support. The runtime executes each
        // statement separately so the abstraction stays symmetric with
        // Snowflake (which can't accept multi-statement bodies).
        Ok(vec![
            "BEGIN".into(),
            format!("DELETE FROM {target} WHERE {partition_filter}"),
            format!("INSERT INTO {target}\n{select_sql}"),
            "COMMIT".into(),
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dialect() -> DuckDbSqlDialect {
        DuckDbSqlDialect
    }

    #[test]
    fn test_format_table_ref_three_part() {
        let d = dialect();
        assert_eq!(
            d.format_table_ref("cat", "sch", "tbl").unwrap(),
            "cat.sch.tbl"
        );
    }

    #[test]
    fn test_format_table_ref_two_part() {
        let d = dialect();
        assert_eq!(d.format_table_ref("", "sch", "tbl").unwrap(), "sch.tbl");
    }

    #[test]
    fn test_create_table_as() {
        let d = dialect();
        let sql = d.create_table_as("sch.tbl", "SELECT * FROM src");
        assert_eq!(sql, "CREATE OR REPLACE TABLE sch.tbl AS\nSELECT * FROM src");
    }

    #[test]
    fn test_describe_table() {
        let d = dialect();
        // DuckDB uses DESCRIBE (no TABLE keyword)
        assert_eq!(d.describe_table_sql("sch.tbl"), "DESCRIBE sch.tbl");
    }

    #[test]
    fn test_no_create_catalog() {
        let d = dialect();
        assert!(d.create_catalog_sql("my_catalog").is_none());
    }

    #[test]
    fn test_create_schema() {
        let d = dialect();
        assert_eq!(
            d.create_schema_sql("", "my_schema").unwrap().unwrap(),
            "CREATE SCHEMA IF NOT EXISTS my_schema"
        );
    }

    #[test]
    fn test_tablesample() {
        let d = dialect();
        assert_eq!(
            d.tablesample_clause(10).unwrap(),
            "USING SAMPLE 10 PERCENT (bernoulli)"
        );
    }

    #[test]
    fn test_merge_rejects_update_set_star() {
        let d = dialect();
        let result = d.merge_into("tbl", "SELECT 1", &["id".into()], &ColumnSelection::All);
        assert!(result.is_err());
    }

    #[test]
    fn test_merge_explicit_columns() {
        let d = dialect();
        let sql = d
            .merge_into(
                "tbl",
                "SELECT 1",
                &["id".into()],
                &ColumnSelection::Explicit(vec!["name".into(), "status".into()]),
            )
            .unwrap();
        assert!(sql.contains("UPDATE SET t.name = s.name, t.status = s.status"));
    }

    #[test]
    fn test_insert_overwrite_partition_four_statement_transaction() {
        let d = dialect();
        let stmts = d
            .insert_overwrite_partition(
                "marts.fct_daily_orders",
                "order_date >= '2026-04-07 00:00:00' AND order_date < '2026-04-08 00:00:00'",
                "SELECT order_date, COUNT(*) FROM stg_orders GROUP BY 1",
            )
            .unwrap();
        // DuckDB matches Snowflake: explicit BEGIN/DELETE/INSERT/COMMIT so the
        // runtime can roll back on partial failure regardless of warehouse.
        assert_eq!(stmts.len(), 4);
        assert_eq!(stmts[0], "BEGIN");
        assert!(stmts[1].starts_with("DELETE FROM marts.fct_daily_orders WHERE "));
        assert!(stmts[2].starts_with("INSERT INTO marts.fct_daily_orders"));
        assert_eq!(stmts[3], "COMMIT");
    }
}
