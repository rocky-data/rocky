//! Databricks SQL dialect implementation.
//!
//! Generates Databricks-specific SQL syntax: three-part table references,
//! `CREATE OR REPLACE TABLE`, `MERGE INTO ... UPDATE SET *`, `TABLESAMPLE`,
//! `ALTER ... SET TAGS`, etc.

use std::fmt::Write;

use rocky_core::ir::{ColumnSelection, MetadataColumn};
use rocky_core::traits::{AdapterError, AdapterResult, SqlDialect};
use rocky_sql::validation;

/// Databricks SQL dialect for Unity Catalog.
///
/// Uses three-part table naming (`catalog.schema.table`),
/// Databricks-specific DDL syntax, and TABLESAMPLE for sampling.
#[derive(Debug, Clone, Default)]
pub struct DatabricksSqlDialect;

impl SqlDialect for DatabricksSqlDialect {
    fn format_table_ref(&self, catalog: &str, schema: &str, table: &str) -> AdapterResult<String> {
        validation::format_table_ref(catalog, schema, table).map_err(AdapterError::new)
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
        keys: &[std::sync::Arc<str>],
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

        let update_clause = match update_cols {
            ColumnSelection::All => "UPDATE SET *".to_string(),
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
        format!("DESCRIBE TABLE {table_ref}")
    }

    fn drop_table_sql(&self, table_ref: &str) -> String {
        format!("DROP TABLE IF EXISTS {table_ref}")
    }

    fn create_catalog_sql(&self, name: &str) -> Option<AdapterResult<String>> {
        Some(
            validation::validate_identifier(name)
                .map(|_| format!("CREATE CATALOG IF NOT EXISTS {name}"))
                .map_err(AdapterError::new),
        )
    }

    fn create_schema_sql(&self, catalog: &str, schema: &str) -> Option<AdapterResult<String>> {
        let result = (|| {
            validation::validate_identifier(catalog).map_err(AdapterError::new)?;
            validation::validate_identifier(schema).map_err(AdapterError::new)?;
            Ok(format!("CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}"))
        })();
        Some(result)
    }

    fn insert_overwrite_partition(
        &self,
        target: &str,
        partition_filter: &str,
        select_sql: &str,
    ) -> AdapterResult<Vec<String>> {
        // Delta Lake's `INSERT INTO ... REPLACE WHERE ...` is atomic in a
        // single statement and works on un-partitioned tables too. Preferred
        // over `INSERT OVERWRITE PARTITION` (which requires physical
        // partitioning) because Rocky's time_interval doesn't constrain
        // the underlying table layout.
        Ok(vec![format!(
            "INSERT INTO {target} REPLACE WHERE {partition_filter}\n{select_sql}"
        )])
    }

    fn tablesample_clause(&self, percent: u32) -> Option<String> {
        Some(format!("TABLESAMPLE ({percent} PERCENT)"))
    }

    fn regex_match_predicate(
        &self,
        column: &str,
        pattern: &str,
    ) -> rocky_core::traits::AdapterResult<String> {
        Ok(format!("{column} RLIKE '{pattern}'"))
    }

    fn quote_identifier(&self, name: &str) -> String {
        // Backticks are the universally-safe Databricks identifier
        // quote — they work in both `ANSI_MODE = on` (the default since
        // DBR 14.x, where `"col"` is also a valid identifier) and
        // `ANSI_MODE = off` (where `"col"` is a STRING literal). Using
        // backticks keeps generated SQL stable across the workspace
        // SQL-config knob without requiring runtime introspection.
        format!("`{name}`")
    }

    fn row_hash_expr(&self, columns: &[String]) -> AdapterResult<String> {
        if columns.is_empty() {
            return Err(AdapterError::msg(
                "row_hash_expr requires at least one column to hash",
            ));
        }
        for col in columns {
            validation::validate_identifier(col).map_err(AdapterError::new)?;
        }
        // `xxhash64(col_a, col_b, ...)` — Spark's multi-arg form hashes
        // the binary representation of each column with positional NULL
        // handling built in: `xxhash64(NULL, 'x')` ≠ `xxhash64('x', NULL)`,
        // so two rows that swap a NULL across columns hash differently
        // (a `concat_ws`-based scheme would silently collide them
        // because `concat_ws` skips NULL arguments). Type-aware as a
        // bonus: an INT-to-STRING column-type change shows up as a
        // diff. `xxhash64` returns BIGINT; `BIT_XOR(BIGINT)` returns
        // BIGINT, which round-trips cleanly to the kernel's `i128`
        // slot (sign-extended; the parser bit-casts into `u128`).
        let arg_list = columns
            .iter()
            .map(|c| format!("`{c}`"))
            .collect::<Vec<_>>()
            .join(", ");
        Ok(format!("xxhash64({arg_list})"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dialect() -> DatabricksSqlDialect {
        DatabricksSqlDialect
    }

    #[test]
    fn test_format_table_ref() {
        let d = dialect();
        assert_eq!(
            d.format_table_ref("cat", "sch", "tbl").unwrap(),
            "cat.sch.tbl"
        );
    }

    #[test]
    fn test_format_table_ref_rejects_bad_identifier() {
        let d = dialect();
        assert!(d.format_table_ref("bad; DROP", "sch", "tbl").is_err());
    }

    #[test]
    fn test_create_table_as() {
        let d = dialect();
        let sql = d.create_table_as("cat.sch.tbl", "SELECT * FROM src");
        assert_eq!(
            sql,
            "CREATE OR REPLACE TABLE cat.sch.tbl AS\nSELECT * FROM src"
        );
    }

    #[test]
    fn test_insert_into() {
        let d = dialect();
        let sql = d.insert_into("cat.sch.tbl", "SELECT * FROM src");
        assert_eq!(sql, "INSERT INTO cat.sch.tbl\nSELECT * FROM src");
    }

    #[test]
    fn test_merge_into() {
        let d = dialect();
        let sql = d
            .merge_into(
                "cat.sch.tbl",
                "SELECT * FROM src",
                &["id".into()],
                &ColumnSelection::All,
            )
            .unwrap();
        assert!(sql.contains("MERGE INTO cat.sch.tbl AS t"));
        assert!(sql.contains("ON t.id = s.id"));
        assert!(sql.contains("WHEN MATCHED THEN UPDATE SET *"));
        assert!(sql.contains("WHEN NOT MATCHED THEN INSERT *"));
    }

    #[test]
    fn test_merge_composite_key_explicit_cols() {
        let d = dialect();
        let sql = d
            .merge_into(
                "cat.sch.tbl",
                "SELECT * FROM src",
                &["id".into(), "date".into()],
                &ColumnSelection::Explicit(vec!["status".into(), "amount".into()]),
            )
            .unwrap();
        assert!(sql.contains("ON t.id = s.id AND t.date = s.date"));
        assert!(sql.contains("UPDATE SET t.status = s.status, t.amount = s.amount"));
    }

    #[test]
    fn test_merge_no_key_fails() {
        let d = dialect();
        assert!(
            d.merge_into("tbl", "SELECT 1", &[], &ColumnSelection::All)
                .is_err()
        );
    }

    #[test]
    fn test_select_clause_all() {
        let d = dialect();
        let sql = d.select_clause(&ColumnSelection::All, &[]).unwrap();
        assert_eq!(sql, "SELECT *");
    }

    #[test]
    fn test_select_clause_with_metadata() {
        let d = dialect();
        let sql = d
            .select_clause(
                &ColumnSelection::All,
                &[MetadataColumn {
                    name: "_loaded_by".into(),
                    data_type: "STRING".into(),
                    value: "NULL".into(),
                }],
            )
            .unwrap();
        assert_eq!(sql, "SELECT *, CAST(NULL AS STRING) AS _loaded_by");
    }

    #[test]
    fn test_watermark_where() {
        let d = dialect();
        let sql = d
            .watermark_where("_fivetran_synced", "cat.sch.tbl")
            .unwrap();
        assert!(sql.starts_with("WHERE _fivetran_synced > ("));
        assert!(sql.contains("COALESCE(MAX(_fivetran_synced), TIMESTAMP '1970-01-01')"));
        assert!(sql.contains("FROM cat.sch.tbl"));
    }

    #[test]
    fn test_describe_table() {
        let d = dialect();
        assert_eq!(
            d.describe_table_sql("cat.sch.tbl"),
            "DESCRIBE TABLE cat.sch.tbl"
        );
    }

    #[test]
    fn test_drop_table() {
        let d = dialect();
        assert_eq!(
            d.drop_table_sql("cat.sch.tbl"),
            "DROP TABLE IF EXISTS cat.sch.tbl"
        );
    }

    #[test]
    fn test_create_catalog() {
        let d = dialect();
        assert_eq!(
            d.create_catalog_sql("my_catalog").unwrap().unwrap(),
            "CREATE CATALOG IF NOT EXISTS my_catalog"
        );
    }

    #[test]
    fn test_create_schema() {
        let d = dialect();
        assert_eq!(
            d.create_schema_sql("cat", "sch").unwrap().unwrap(),
            "CREATE SCHEMA IF NOT EXISTS cat.sch"
        );
    }

    #[test]
    fn test_tablesample() {
        let d = dialect();
        assert_eq!(
            d.tablesample_clause(10).unwrap(),
            "TABLESAMPLE (10 PERCENT)"
        );
    }

    #[test]
    fn test_list_tables_sql_uses_catalog_prefix() {
        let d = dialect();
        let sql = d.list_tables_sql("acme", "staging__orders").unwrap();
        // Databricks Unity Catalog exposes per-catalog information_schema.
        assert!(
            sql.contains("FROM acme.information_schema.tables"),
            "sql: {sql}"
        );
        assert!(
            sql.contains("table_schema = 'staging__orders'"),
            "sql: {sql}"
        );
    }

    #[test]
    fn test_insert_overwrite_partition_single_replace_where() {
        // Databricks Delta: single REPLACE WHERE statement, atomic.
        let d = dialect();
        let stmts = d
            .insert_overwrite_partition(
                "warehouse.marts.fct_daily_orders",
                "order_date >= '2026-04-07 00:00:00' AND order_date < '2026-04-08 00:00:00'",
                "SELECT order_date, customer_id, COUNT(*) FROM stg_orders GROUP BY 1, 2",
            )
            .unwrap();
        assert_eq!(stmts.len(), 1, "Databricks emits a single REPLACE WHERE");
        assert!(stmts[0].starts_with("INSERT INTO warehouse.marts.fct_daily_orders REPLACE WHERE"));
        assert!(stmts[0].contains("order_date >= '2026-04-07 00:00:00'"));
        assert!(stmts[0].contains("SELECT order_date"));
    }

    #[test]
    fn test_quote_identifier_uses_backticks() {
        let d = dialect();
        assert_eq!(d.quote_identifier("id"), "`id`");
        assert_eq!(d.quote_identifier("customer_id"), "`customer_id`");
    }

    #[test]
    fn test_row_hash_expr_emits_multi_arg_xxhash64() {
        let d = dialect();
        let sql = d.row_hash_expr(&["name".into(), "value".into()]).unwrap();
        assert_eq!(sql, "xxhash64(`name`, `value`)");
    }

    #[test]
    fn test_row_hash_expr_single_column() {
        let d = dialect();
        let sql = d.row_hash_expr(&["only".into()]).unwrap();
        assert_eq!(sql, "xxhash64(`only`)");
    }

    #[test]
    fn test_row_hash_expr_rejects_empty_columns() {
        let d = dialect();
        assert!(d.row_hash_expr(&[]).is_err());
    }

    #[test]
    fn test_row_hash_expr_rejects_invalid_identifier() {
        let d = dialect();
        // SQL-injection attempt: column name carrying a quote.
        assert!(d.row_hash_expr(&["a`; DROP TABLE x; --".into()]).is_err());
    }
}
