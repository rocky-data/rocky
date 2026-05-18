//! DuckDB SQL dialect implementation.
//!
//! DuckDB differences from Databricks:
//! - Supports both two-part (`schema.table`) and three-part (`catalog.schema.table`) naming
//! - `CREATE OR REPLACE TABLE` works the same way
//! - No `UPDATE SET *` in MERGE — must enumerate columns
//! - No `CREATE CATALOG` — uses databases/schemas
//! - `TABLESAMPLE` uses different syntax: `USING SAMPLE N PERCENT (bernoulli)`

use std::fmt::Write;

use rocky_core::traits::{AdapterError, AdapterResult, SqlDialect};
use rocky_ir::{ColumnSelection, MetadataColumn};
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

    /// Render a DuckDB-native `MERGE INTO ... USING ... WHEN MATCHED ... WHEN NOT MATCHED`.
    ///
    /// DuckDB 0.10+ supports the standard ANSI MERGE statement, but with two
    /// dialect-specific quirks Rocky has to honour:
    ///
    /// 1. **No `UPDATE SET *` shorthand.** DuckDB rejects the wildcard form
    ///    accepted by Databricks; the SET clause has to enumerate every
    ///    target column. This matches Snowflake's behaviour. Callers that
    ///    pass [`ColumnSelection::All`] get a fail-fast error — the model
    ///    TOML has to declare `update_columns` explicitly. Tracking
    ///    auto-enumeration via warehouse introspection is a follow-up; the
    ///    current `SqlDialect::merge_into` signature carries no schema hint.
    /// 2. **No qualified column names on the SET left-hand side.** DuckDB
    ///    raises `Parser Error: Qualified column names in UPDATE .. SET not
    ///    supported` for `t.col = s.col`. The target column stays unqualified
    ///    (`col = s.col`); the source side keeps its alias.
    ///
    /// DuckDB *does* accept `INSERT *` shorthand in the `WHEN NOT MATCHED`
    /// branch — the live regression test in this module exercises that path
    /// end-to-end, so the wildcard insert is safe to keep.
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

        // DuckDB does NOT support `UPDATE SET *` — must enumerate columns.
        let update_clause = match update_cols {
            ColumnSelection::All => {
                return Err(AdapterError::msg(
                    "DuckDB MERGE does not support UPDATE SET *. \
                     Declare `update_columns` explicitly in the model TOML.",
                ));
            }
            ColumnSelection::Explicit(cols) => {
                // DuckDB rejects qualified column names on the left side of
                // `UPDATE .. SET` (`Parser Error: Qualified column names in
                // UPDATE .. SET not supported`), so the target column stays
                // unqualified — `name = s.name` rather than `t.name = s.name`.
                let sets = cols
                    .iter()
                    .map(|c| {
                        validation::validate_identifier(c).map_err(AdapterError::new)?;
                        Ok(format!("{c} = s.{c}"))
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

    fn list_tables_sql(&self, catalog: &str, schema: &str) -> AdapterResult<String> {
        // DuckDB's information_schema is a flat, un-prefixed catalog —
        // the catalog name has to be pushed into a WHERE filter instead
        // of the `FROM` expression.
        rocky_sql::validation::validate_identifier(catalog)
            .map_err(rocky_core::traits::AdapterError::new)?;
        rocky_sql::validation::validate_identifier(schema)
            .map_err(rocky_core::traits::AdapterError::new)?;
        Ok(format!(
            "SELECT table_name FROM information_schema.tables \
             WHERE table_catalog = '{catalog}' AND table_schema = '{schema}'"
        ))
    }

    fn regex_match_predicate(&self, column: &str, pattern: &str) -> AdapterResult<String> {
        Ok(format!("regexp_matches({column}, '{pattern}')"))
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
        // DuckDB's `hash(expr_list)` accepts any number of arguments and
        // returns UBIGINT (xxhash64). Cast to HUGEINT so `BIT_XOR(...)`
        // widens cleanly to i128 — matches the [`ChunkChecksum::checksum`]
        // u128 contract without truncation when the hash's high bit is
        // set.
        let arg_list = columns
            .iter()
            .map(|c| format!("\"{c}\""))
            .collect::<Vec<_>>()
            .join(", ");
        Ok(format!("CAST(hash({arg_list}) AS HUGEINT)"))
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
    fn test_list_tables_sql_filters_catalog_and_schema() {
        let d = dialect();
        let sql = d.list_tables_sql("poc", "staging__orders").unwrap();
        // DuckDB requires the un-prefixed information_schema + catalog filter.
        assert!(sql.contains("FROM information_schema.tables"), "sql: {sql}");
        assert!(sql.contains("table_catalog = 'poc'"), "sql: {sql}");
        assert!(
            sql.contains("table_schema = 'staging__orders'"),
            "sql: {sql}"
        );
    }

    #[test]
    fn test_list_tables_sql_rejects_injection() {
        let d = dialect();
        assert!(d.list_tables_sql("poc; DROP TABLE users; --", "s").is_err());
        assert!(d.list_tables_sql("poc", "s'; --").is_err());
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
        // DuckDB rejects qualified column names on the SET LHS — see
        // `merge_into` for the parser-error rationale.
        assert!(sql.contains("UPDATE SET name = s.name, status = s.status"));
    }

    #[test]
    fn test_merge_compound_unique_key() {
        let d = dialect();
        let sql = d
            .merge_into(
                "tbl",
                "SELECT 1",
                &["id".into(), "created_at".into()],
                &ColumnSelection::Explicit(vec!["status".into()]),
            )
            .unwrap();
        // ON clause concatenates predicates with AND, in declaration order.
        assert!(
            sql.contains("ON t.id = s.id AND t.created_at = s.created_at"),
            "expected compound ON clause: {sql}"
        );
        // The SET clause uses unqualified target column names (DuckDB parser
        // requirement — see `merge_into` doc).
        assert!(
            sql.contains("UPDATE SET status = s.status"),
            "expected unqualified SET LHS: {sql}"
        );
    }

    #[test]
    fn test_merge_rejects_empty_keys() {
        let d = dialect();
        let result = d.merge_into(
            "tbl",
            "SELECT 1",
            &[],
            &ColumnSelection::Explicit(vec!["name".into()]),
        );
        assert!(result.is_err(), "empty unique_key must error");
    }

    #[test]
    fn test_merge_rejects_injection_in_key() {
        let d = dialect();
        let result = d.merge_into(
            "tbl",
            "SELECT 1",
            &["id; DROP TABLE x; --".into()],
            &ColumnSelection::Explicit(vec!["name".into()]),
        );
        assert!(result.is_err(), "injection in unique_key must error");
    }

    #[test]
    fn test_merge_rejects_injection_in_update_column() {
        let d = dialect();
        let result = d.merge_into(
            "tbl",
            "SELECT 1",
            &["id".into()],
            &ColumnSelection::Explicit(vec!["name; DROP TABLE x; --".into()]),
        );
        assert!(result.is_err(), "injection in update_columns must error");
    }

    /// Live regression: the MERGE SQL produced by `merge_into` must execute
    /// successfully against a real DuckDB instance. The previous form
    /// (`UPDATE SET t.<col> = s.<col>`) passed every string-content unit test
    /// but failed at execute time with `Parser Error: Qualified column names
    /// in UPDATE .. SET not supported`. This test catches that regression by
    /// actually running the MERGE against an in-memory DuckDB.
    #[test]
    fn test_merge_executes_against_live_duckdb() {
        use crate::DuckDbConnector;
        let conn = DuckDbConnector::in_memory().expect("in-memory DuckDB");

        // Target seeded with 2 rows (id=1 'old', id=2 'unchanged').
        // Source delta: id=1 update + id=3 insert.
        conn.execute_sql(
            "CREATE TABLE tgt (id INTEGER, name VARCHAR, status VARCHAR);\n\
             INSERT INTO tgt VALUES (1, 'old',  'active'), (2, 'keep', 'active');\n\
             CREATE TABLE src (id INTEGER, name VARCHAR, status VARCHAR);\n\
             INSERT INTO src VALUES (1, 'new', 'active'), (3, 'fresh', 'active');",
        )
        .expect("seed target+source");

        let merge_sql = dialect()
            .merge_into(
                "tgt",
                "SELECT id, name, status FROM src",
                &["id".into()],
                &ColumnSelection::Explicit(vec!["name".into(), "status".into()]),
            )
            .expect("build MERGE SQL");

        // Pre-fix this fails with `Parser Error: Qualified column names in
        // UPDATE .. SET not supported`. Post-fix it succeeds.
        conn.execute_sql(&merge_sql).expect("MERGE must execute");

        let after = conn
            .execute_sql("SELECT id, name FROM tgt ORDER BY id")
            .expect("query post-MERGE");
        // Expected: id=1 updated to 'new', id=2 unchanged, id=3 inserted.
        assert_eq!(after.rows.len(), 3, "MERGE should leave 3 rows in target");
        assert_eq!(after.rows[0][1].as_str(), Some("new"));
        assert_eq!(after.rows[1][1].as_str(), Some("keep"));
        assert_eq!(after.rows[2][1].as_str(), Some("fresh"));
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
