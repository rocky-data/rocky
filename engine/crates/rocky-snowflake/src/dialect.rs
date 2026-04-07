//! Snowflake SQL dialect implementation.
//!
//! Snowflake differences from Databricks:
//! - Uses `database.schema.table` naming (not catalog)
//! - Double-quoted identifiers for special characters
//! - No `UPDATE SET *` in MERGE — must enumerate columns
//! - `SAMPLE (N ROWS)` instead of `TABLESAMPLE (N PERCENT)`
//! - Tags via TAG objects (separate DDL)

use std::fmt::Write;

use rocky_core::ir::{ColumnSelection, MetadataColumn};
use rocky_core::traits::{AdapterError, AdapterResult, SqlDialect};
use rocky_sql::validation;

/// Snowflake SQL dialect.
#[derive(Debug, Clone, Default)]
pub struct SnowflakeSqlDialect;

impl SqlDialect for SnowflakeSqlDialect {
    fn format_table_ref(&self, catalog: &str, schema: &str, table: &str) -> AdapterResult<String> {
        // Snowflake uses database.schema.table (catalog = database)
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

        // Snowflake does NOT support `UPDATE SET *` — must enumerate columns
        let update_clause = match update_cols {
            ColumnSelection::All => {
                return Err(AdapterError::msg(
                    "Snowflake MERGE does not support UPDATE SET *. Use explicit update_columns.",
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

        // Snowflake requires explicit column list for INSERT
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
            // Snowflake uses :: for casting: value::TYPE
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
             \x20   SELECT COALESCE(MAX({timestamp_col}), '1970-01-01'::TIMESTAMP_NTZ)\n\
             \x20   FROM {target_ref}\n\
             )"
        ))
    }

    fn describe_table_sql(&self, table_ref: &str) -> String {
        format!("DESCRIBE TABLE {table_ref}")
    }

    fn drop_table_sql(&self, table_ref: &str) -> String {
        format!("DROP TABLE IF EXISTS {table_ref}")
    }

    fn create_catalog_sql(&self, name: &str) -> Option<AdapterResult<String>> {
        // Snowflake uses databases instead of catalogs
        Some(
            validation::validate_identifier(name)
                .map(|_| format!("CREATE DATABASE IF NOT EXISTS {name}"))
                .map_err(AdapterError::new),
        )
    }

    fn create_schema_sql(&self, catalog: &str, schema: &str) -> Option<AdapterResult<String>> {
        Some((|| {
            validation::validate_identifier(catalog).map_err(AdapterError::new)?;
            validation::validate_identifier(schema).map_err(AdapterError::new)?;
            Ok(format!("CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}"))
        })())
    }

    fn tablesample_clause(&self, percent: u32) -> Option<String> {
        // Snowflake uses SAMPLE instead of TABLESAMPLE
        Some(format!("SAMPLE ({percent})"))
    }

    fn insert_overwrite_partition(
        &self,
        target: &str,
        partition_filter: &str,
        select_sql: &str,
    ) -> AdapterResult<Vec<String>> {
        // Snowflake has no native partition-replace operation. We wrap a
        // DELETE + INSERT in an explicit transaction; the runtime issues
        // each statement as a separate REST API call (Snowflake's SQL
        // Statement Execution API runs one statement per call by default
        // and rejects multi-statement bodies without MULTI_STATEMENT_COUNT).
        // On any failure mid-batch the runtime issues `ROLLBACK`.
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

    fn dialect() -> SnowflakeSqlDialect {
        SnowflakeSqlDialect
    }

    #[test]
    fn test_format_table_ref() {
        let d = dialect();
        assert_eq!(
            d.format_table_ref("mydb", "public", "users").unwrap(),
            "mydb.public.users"
        );
    }

    #[test]
    fn test_format_table_ref_two_part() {
        let d = dialect();
        assert_eq!(
            d.format_table_ref("", "public", "users").unwrap(),
            "public.users"
        );
    }

    #[test]
    fn test_create_table_as() {
        let d = dialect();
        let sql = d.create_table_as("db.schema.tbl", "SELECT * FROM src");
        assert_eq!(
            sql,
            "CREATE OR REPLACE TABLE db.schema.tbl AS\nSELECT * FROM src"
        );
    }

    #[test]
    fn test_create_catalog_is_create_database() {
        let d = dialect();
        let sql = d.create_catalog_sql("my_warehouse").unwrap().unwrap();
        assert_eq!(sql, "CREATE DATABASE IF NOT EXISTS my_warehouse");
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
    fn test_watermark_uses_timestamp_ntz() {
        let d = dialect();
        let sql = d.watermark_where("_fivetran_synced", "db.sch.tbl").unwrap();
        assert!(sql.contains("TIMESTAMP_NTZ"));
    }

    #[test]
    fn test_tablesample_is_sample() {
        let d = dialect();
        assert_eq!(d.tablesample_clause(10).unwrap(), "SAMPLE (10)");
    }

    #[test]
    fn test_describe_table() {
        let d = dialect();
        assert_eq!(
            d.describe_table_sql("db.public.users"),
            "DESCRIBE TABLE db.public.users"
        );
    }

    #[test]
    fn test_drop_table() {
        let d = dialect();
        assert_eq!(
            d.drop_table_sql("db.public.users"),
            "DROP TABLE IF EXISTS db.public.users"
        );
    }

    #[test]
    fn test_create_schema() {
        let d = dialect();
        let sql = d.create_schema_sql("mydb", "staging").unwrap().unwrap();
        assert_eq!(sql, "CREATE SCHEMA IF NOT EXISTS mydb.staging");
    }

    #[test]
    fn test_snowflake_create_database_not_catalog() {
        let d = dialect();
        let sql = d.create_catalog_sql("warehouse").unwrap().unwrap();
        // Snowflake says "DATABASE", not "CATALOG"
        assert!(sql.contains("DATABASE"));
        assert!(!sql.contains("CATALOG"));
    }

    #[test]
    fn test_insert_overwrite_partition_four_statement_transaction() {
        // Snowflake's REST API runs one statement per call by default — so we
        // emit 4 statements: BEGIN; DELETE; INSERT; COMMIT. The runtime
        // executes them in order and rolls back on partial failure.
        let d = dialect();
        let stmts = d
            .insert_overwrite_partition(
                "warehouse.marts.fct_daily_orders",
                "order_date >= '2026-04-07 00:00:00' AND order_date < '2026-04-08 00:00:00'",
                "SELECT order_date, COUNT(*) FROM stg_orders GROUP BY 1",
            )
            .unwrap();
        assert_eq!(stmts.len(), 4, "Snowflake emits BEGIN/DELETE/INSERT/COMMIT");
        assert_eq!(stmts[0], "BEGIN");
        assert!(stmts[1].starts_with("DELETE FROM warehouse.marts.fct_daily_orders WHERE "));
        assert!(stmts[1].contains("order_date >= '2026-04-07 00:00:00'"));
        assert!(stmts[2].starts_with("INSERT INTO warehouse.marts.fct_daily_orders"));
        assert_eq!(stmts[3], "COMMIT");
    }
}
