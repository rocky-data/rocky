//! Snowflake SQL dialect implementation.
//!
//! Snowflake differences from Databricks:
//! - Uses `database.schema.table` naming (not catalog)
//! - Double-quoted identifiers for special characters
//! - No `UPDATE SET *` in MERGE — must enumerate columns
//! - `SAMPLE (N ROWS)` instead of `TABLESAMPLE (N PERCENT)`
//! - Tags via TAG objects (separate DDL)

use std::fmt::Write;

use rocky_core::traits::{AdapterError, AdapterResult, SqlDialect};
use rocky_ir::{ColumnSelection, MetadataColumn};
use rocky_sql::validation;

/// Snowflake SQL dialect.
#[derive(Debug, Clone, Default)]
pub struct SnowflakeSqlDialect;

impl SqlDialect for SnowflakeSqlDialect {
    fn format_table_ref(&self, catalog: &str, schema: &str, table: &str) -> AdapterResult<String> {
        // Snowflake folds *unquoted* identifiers to UPPERCASE at parse time, so
        // case-preserving (lowercase or mixed-case) names need explicit
        // double-quoting to round-trip. The shared `validation::format_table_ref`
        // emits unquoted identifiers, which silently breaks against any schema
        // / table created with quoted lowercase names. Quote each component.
        validation::validate_identifier(schema).map_err(AdapterError::new)?;
        validation::validate_identifier(table).map_err(AdapterError::new)?;
        let schema_q = self.quote_identifier(schema);
        let table_q = self.quote_identifier(table);
        if catalog.is_empty() {
            Ok(format!("{schema_q}.{table_q}"))
        } else {
            validation::validate_identifier(catalog).map_err(AdapterError::new)?;
            let catalog_q = self.quote_identifier(catalog);
            Ok(format!("{catalog_q}.{schema_q}.{table_q}"))
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

    fn watermark_where(
        &self,
        timestamp_col: &str,
        last_watermark: Option<&chrono::DateTime<chrono::Utc>>,
    ) -> AdapterResult<String> {
        validation::validate_identifier(timestamp_col).map_err(AdapterError::new)?;
        // Snowflake casts string literals to TIMESTAMP_NTZ explicitly so the
        // result type matches the column under the default
        // `TIMESTAMP_TYPE_MAPPING` setting. Sub-second precision is
        // preserved by chrono's `%.f` directive.
        let literal = last_watermark
            .map(|t| t.format("%Y-%m-%d %H:%M:%S%.f").to_string())
            .unwrap_or_else(|| "1970-01-01 00:00:00".to_string());
        Ok(format!(
            "WHERE {timestamp_col} > '{literal}'::TIMESTAMP_NTZ"
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
        // DELETE + INSERT in an explicit `BEGIN ... COMMIT` transaction.
        //
        // Snowflake's `/api/v2/statements` runs ONE statement per call by
        // default — every call opens an independent session, so emitting
        // BEGIN/DELETE/INSERT/COMMIT as four separate REST calls is silently
        // non-transactional: each call autocommits, `BEGIN` is discarded, the
        // `DELETE` autocommits, and the trailing `COMMIT`/`ROLLBACK` is a
        // no-op. To get true atomicity we submit the four statements as a
        // single semicolon-joined script in one call; the connector sets
        // `MULTI_STATEMENT_COUNT` in `parameters` so Snowflake accepts the
        // multi-statement body and runs them in one session.
        //
        // The connector ALSO sets `TRANSACTION_ABORT_ON_ERROR = TRUE` for
        // multi-statement bodies. Without that parameter, a failed DML in
        // a Snowflake transaction rolls back only its own changes and leaves
        // the transaction open — subsequent statements (including the
        // trailing `COMMIT`) are skipped, and the in-flight transaction only
        // unwinds implicitly when the REST session ends. That session-close
        // cleanup is incidental, not contractual: `ABORT_ON_ERROR` makes
        // Snowflake roll back the whole transaction at error-time, which
        // is what partition-replace semantically requires. Mirrors BigQuery's
        // single-script `BEGIN TRANSACTION; ...; COMMIT TRANSACTION` form.
        Ok(vec![format!(
            "BEGIN;\n\
             DELETE FROM {target} WHERE {partition_filter};\n\
             INSERT INTO {target}\n{select_sql};\n\
             COMMIT"
        )])
    }

    fn regex_match_predicate(
        &self,
        column: &str,
        pattern: &str,
    ) -> rocky_core::traits::AdapterResult<String> {
        Ok(format!("REGEXP_LIKE({column}, '{pattern}')"))
    }

    fn quote_identifier(&self, name: &str) -> String {
        // Snowflake folds *unquoted* identifiers to UPPER (`id` becomes
        // `ID`); double-quoted identifiers are taken verbatim and
        // case-sensitive. The trait default already returns
        // double-quoted form, but we override anyway so the dialect's
        // contract is explicit at the call site and so we can pin the
        // documentation alongside the BigQuery (backticks) and
        // Databricks (backticks) overrides for the cross-adapter
        // diff-bisection sweep. Callers in the bisection kernel build
        // schemas with lowercase column names — the double quotes here
        // make those references resolve correctly without depending on
        // case-fold defaults.
        format!("\"{name}\"")
    }

    fn materialized_view_ddl(&self, target: &str, select_sql: &str) -> AdapterResult<String> {
        Ok(format!(
            "CREATE OR REPLACE MATERIALIZED VIEW {target} AS\n{select_sql}"
        ))
    }

    fn dynamic_table_ddl(
        &self,
        target: &str,
        select_sql: &str,
        target_lag: &str,
        warehouse: &str,
    ) -> AdapterResult<String> {
        // `target_lag` is a Snowflake lag specifier — accepted values are
        // either a duration string like `"1 minute"` / `"5 hours"` or the
        // literal `"downstream"`. Hand-validate the allowlist here rather
        // than reaching into rocky-core (which would invert the layering):
        // alphanumeric + space only, non-empty.
        if target_lag.is_empty()
            || !target_lag
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == ' ')
        {
            return Err(AdapterError::msg(format!(
                "invalid target_lag for Snowflake dynamic table: {target_lag:?} \
                 (expected e.g. \"1 minute\", \"5 hours\", or \"downstream\")"
            )));
        }
        validation::validate_identifier(warehouse).map_err(AdapterError::new)?;
        Ok(format!(
            "CREATE OR REPLACE DYNAMIC TABLE {target}\n  \
             TARGET_LAG = '{target_lag}'\n  \
             WAREHOUSE = {warehouse}\nAS\n{select_sql}"
        ))
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
        // `HASH(col_a, col_b, ...)` is Snowflake's proprietary
        // 64-bit non-cryptographic hash, the canonical analogue of
        // BigQuery's `FARM_FINGERPRINT` and Spark's `xxhash64`. It
        // accepts variadic column args directly (no `STRUCT` /
        // `TO_JSON_STRING` wrapping like BQ; no `concat_ws`
        // contortions). Per Snowflake's docs, `HASH` is positionally
        // NULL-aware: `HASH(NULL, 'x')` ≠ `HASH('x', NULL)`, so two
        // rows that swap a NULL across columns hash differently — the
        // same correctness property the Databricks override pins.
        // `HASH` returns `NUMBER(19, 0)` (signed 64-bit); the outer
        // `BITXOR_AGG` (which Snowflake uses in place of `BIT_XOR`)
        // round-trips cleanly to the kernel's `i128` checksum slot
        // via sign-extension.
        //
        // **NULL-row handling.** `HASH(NULL)` returns `NULL`, and
        // Snowflake's aggregate functions skip `NULL` rows. We wrap
        // the call in `COALESCE(..., 0)` so an all-NULL value-column
        // row still contributes a non-NULL deterministic hash to the
        // chunk checksum (per the kernel's contract that every row
        // is hashed into exactly one chunk). The `0` sentinel is
        // safe because real `HASH` outputs span the full signed
        // 64-bit range with effectively zero collision probability
        // at non-NULL inputs.
        let arg_list = columns
            .iter()
            .map(|c| format!("\"{c}\""))
            .collect::<Vec<_>>()
            .join(", ");
        Ok(format!("COALESCE(HASH({arg_list}), 0)"))
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
            "\"mydb\".\"public\".\"users\""
        );
    }

    #[test]
    fn test_format_table_ref_two_part() {
        let d = dialect();
        assert_eq!(
            d.format_table_ref("", "public", "users").unwrap(),
            "\"public\".\"users\""
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
    fn test_watermark_no_prior_uses_sentinel_timestamp_ntz() {
        let d = dialect();
        let sql = d.watermark_where("_fivetran_synced", None).unwrap();
        assert_eq!(
            sql,
            "WHERE _fivetran_synced > '1970-01-01 00:00:00'::TIMESTAMP_NTZ"
        );
    }

    #[test]
    fn test_watermark_with_prior_substitutes_literal() {
        use chrono::TimeZone;
        let d = dialect();
        let prior = chrono::Utc.with_ymd_and_hms(2026, 4, 17, 9, 30, 0).unwrap();
        let sql = d.watermark_where("_fivetran_synced", Some(&prior)).unwrap();
        // Source-side semantics: literal substitution, no correlated subquery.
        assert_eq!(
            sql,
            "WHERE _fivetran_synced > '2026-04-17 09:30:00'::TIMESTAMP_NTZ"
        );
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
    fn test_quote_identifier_uses_double_quotes() {
        let d = dialect();
        assert_eq!(d.quote_identifier("id"), "\"id\"");
        assert_eq!(d.quote_identifier("customer_id"), "\"customer_id\"");
    }

    #[test]
    fn test_row_hash_expr_emits_hash_with_coalesce_null_guard() {
        let d = dialect();
        let sql = d.row_hash_expr(&["name".into(), "value".into()]).unwrap();
        // Wrap in COALESCE so the all-NULL row case (HASH(NULL, NULL)
        // returns NULL) still contributes a deterministic 0 hash to
        // the chunk checksum, since BITXOR_AGG skips NULLs.
        assert_eq!(sql, "COALESCE(HASH(\"name\", \"value\"), 0)");
    }

    #[test]
    fn test_row_hash_expr_single_column() {
        let d = dialect();
        let sql = d.row_hash_expr(&["only".into()]).unwrap();
        assert_eq!(sql, "COALESCE(HASH(\"only\"), 0)");
    }

    #[test]
    fn test_row_hash_expr_rejects_empty_columns() {
        let d = dialect();
        assert!(d.row_hash_expr(&[]).is_err());
    }

    #[test]
    fn test_row_hash_expr_rejects_invalid_identifier() {
        let d = dialect();
        // Identifier-injection attempt: column name carrying a quote.
        assert!(d.row_hash_expr(&["a\"; DROP TABLE x; --".into()]).is_err());
    }

    #[test]
    fn test_view_ddl_emits_create_or_replace_view() {
        let d = dialect();
        let sql = d
            .view_ddl("\"db\".\"sch\".\"v\"", "SELECT * FROM src")
            .unwrap();
        assert_eq!(
            sql,
            "CREATE OR REPLACE VIEW \"db\".\"sch\".\"v\" AS\nSELECT * FROM src"
        );
    }

    #[test]
    fn test_materialized_view_ddl_emits_create_or_replace_mv() {
        let d = dialect();
        let sql = d
            .materialized_view_ddl(
                "\"db\".\"sch\".\"mv\"",
                "SELECT customer_id, SUM(total) FROM orders GROUP BY 1",
            )
            .unwrap();
        assert_eq!(
            sql,
            "CREATE OR REPLACE MATERIALIZED VIEW \"db\".\"sch\".\"mv\" AS\n\
             SELECT customer_id, SUM(total) FROM orders GROUP BY 1"
        );
    }

    #[test]
    fn test_dynamic_table_ddl_threads_warehouse() {
        let d = dialect();
        let sql = d
            .dynamic_table_ddl(
                "\"db\".\"sch\".\"dt\"",
                "SELECT id, value FROM src",
                "1 minute",
                "compute_wh",
            )
            .unwrap();
        // Pin the exact Snowflake syntax — refresh-policy clause + WAREHOUSE
        // ident is positional-required.
        assert_eq!(
            sql,
            "CREATE OR REPLACE DYNAMIC TABLE \"db\".\"sch\".\"dt\"\n  \
             TARGET_LAG = '1 minute'\n  \
             WAREHOUSE = compute_wh\nAS\n\
             SELECT id, value FROM src"
        );
    }

    #[test]
    fn test_dynamic_table_ddl_rejects_target_lag_injection() {
        let d = dialect();
        // Single-quote injection — the lag specifier is interpolated into a
        // single-quoted SQL string so a free quote would break out.
        let err = d
            .dynamic_table_ddl("t", "SELECT 1", "1'; DROP TABLE x; --", "wh")
            .expect_err("rejects target_lag carrying quotes");
        assert!(
            err.to_string().to_lowercase().contains("target_lag"),
            "error should mention target_lag: {err}"
        );
    }

    #[test]
    fn test_dynamic_table_ddl_rejects_empty_target_lag() {
        let d = dialect();
        let err = d
            .dynamic_table_ddl("t", "SELECT 1", "", "wh")
            .expect_err("rejects empty target_lag");
        assert!(
            err.to_string().to_lowercase().contains("target_lag"),
            "error should mention target_lag: {err}"
        );
    }

    #[test]
    fn test_dynamic_table_ddl_rejects_warehouse_injection() {
        let d = dialect();
        let err = d
            .dynamic_table_ddl("t", "SELECT 1", "1 minute", "wh; DROP TABLE x; --")
            .expect_err("rejects warehouse carrying SQL");
        // The identifier-validation error message; just make sure we
        // surface SOMETHING rather than emitting the bad SQL.
        let _ = err;
    }

    #[test]
    fn test_insert_overwrite_partition_single_script_transaction() {
        // Snowflake's `/api/v2/statements` runs one statement per call by
        // default; each call is its own session, so emitting BEGIN/DELETE/
        // INSERT/COMMIT as four separate calls is silently non-transactional.
        // Emit a single semicolon-joined script — the connector sets
        // `MULTI_STATEMENT_COUNT` so Snowflake runs all four in one session
        // and auto-rolls-back on partial failure. Mirrors BigQuery's
        // single-script transaction form.
        let d = dialect();
        let stmts = d
            .insert_overwrite_partition(
                "warehouse.marts.fct_daily_orders",
                "order_date >= '2026-04-07 00:00:00' AND order_date < '2026-04-08 00:00:00'",
                "SELECT order_date, COUNT(*) FROM stg_orders GROUP BY 1",
            )
            .unwrap();
        assert_eq!(
            stmts.len(),
            1,
            "Snowflake emits one semicolon-joined script"
        );
        let script = &stmts[0];
        assert!(script.starts_with("BEGIN;\n"));
        assert!(script.contains(
            "DELETE FROM warehouse.marts.fct_daily_orders WHERE \
             order_date >= '2026-04-07 00:00:00' AND order_date < '2026-04-08 00:00:00';"
        ));
        assert!(script.contains("INSERT INTO warehouse.marts.fct_daily_orders"));
        assert!(script.ends_with("COMMIT"));
    }
}
