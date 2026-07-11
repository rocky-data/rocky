//! Snowflake SQL dialect implementation.
//!
//! Snowflake differences from Databricks:
//! - Uses `database.schema.table` naming (not catalog)
//! - Double-quoted identifiers for special characters
//! - No `UPDATE SET *` or `INSERT *` shorthand in MERGE — must enumerate
//!   columns for both the `WHEN MATCHED` and `WHEN NOT MATCHED` branches
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
    fn name(&self) -> &'static str {
        "snowflake"
    }

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

        // Snowflake folds unquoted identifiers to UPPERCASE at parse time —
        // any lowercase / mixed-case column in the source/target table is
        // case-sensitive and only resolves under explicit double-quoting.
        // Mirror `format_table_ref`'s policy here: every identifier the
        // MERGE clause references is double-quoted via `quote_identifier`.
        let on_clause = keys
            .iter()
            .map(|k| {
                let q = self.quote_identifier(k);
                format!("t.{q} = s.{q}")
            })
            .collect::<Vec<_>>()
            .join(" AND ");

        // Snowflake does NOT support `UPDATE SET *` or `INSERT *` shorthand
        // in MERGE — must enumerate columns for both branches. The replication
        // runner resolves `ColumnSelection::All` against the discovered source
        // schema before SQL-gen, so the call site under
        // `commands/run.rs::process_table` always reaches us with `Explicit`.
        // Transformation pipelines specify `update_columns` on the model
        // sidecar TOML; absence (`All`) is a config error surfaced here.
        let (update_clause, insert_clause) = match update_cols {
            ColumnSelection::All => {
                return Err(AdapterError::msg(
                    "Snowflake MERGE does not support UPDATE SET * / INSERT *. \
                     Use explicit update_columns.",
                ));
            }
            ColumnSelection::Explicit(cols) => {
                let mut sets = Vec::with_capacity(cols.len());
                let mut col_list = Vec::with_capacity(cols.len());
                let mut value_list = Vec::with_capacity(cols.len());
                for c in cols {
                    validation::validate_identifier(c).map_err(AdapterError::new)?;
                    let q = self.quote_identifier(c);
                    sets.push(format!("t.{q} = s.{q}"));
                    col_list.push(q.clone());
                    value_list.push(format!("s.{q}"));
                }
                (
                    format!("UPDATE SET {}", sets.join(", ")),
                    format!(
                        "INSERT ({}) VALUES ({})",
                        col_list.join(", "),
                        value_list.join(", ")
                    ),
                )
            }
        };

        Ok(format!(
            "MERGE INTO {target} AS t\n\
             USING (\n{source_sql}\n) AS s\n\
             ON {on_clause}\n\
             WHEN MATCHED THEN {update_clause}\n\
             WHEN NOT MATCHED THEN {insert_clause}"
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
                    // Double-quote so the reference resolves against a
                    // column stored lowercase — matching the
                    // `format_table_ref` / `merge_into` quoted path. An
                    // unquoted reference folds to UPPER and breaks on a
                    // lowercase-stored column.
                    sql.push_str(&self.quote_identifier(col));
                }
            }
        }

        for mc in metadata {
            validation::validate_identifier(&mc.name).map_err(AdapterError::new)?;
            // Validate `data_type` before interpolating it raw into the CAST
            // (same guard as `alter_column_type_sql`) — a metadata `type` from
            // a hostile config must not break out of the cast expression.
            rocky_core::sql_gen::validate_sql_type(&mc.data_type).map_err(AdapterError::new)?;
            // Snowflake uses :: for casting: value::TYPE. Quote the output
            // alias so the materialized metadata column is stored
            // lowercase-preserving, consistent with the quoted column path
            // (and so a later quoted reference to it resolves).
            let alias_q = self.quote_identifier(&mc.name);
            write!(sql, ", CAST({} AS {}) AS {alias_q}", mc.value, mc.data_type).unwrap();
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
        // Double-quote the column to match Rocky's quoted-identifier
        // convention (`format_table_ref` / `merge_into`), which reference
        // columns quoted (case-sensitive, lowercase-preserving). An
        // *unquoted* reference here folds to UPPER and raises `invalid
        // identifier` against a column stored lowercase — which the
        // quoted-create / bisection-seed path produces. Quoting fixes that
        // case. (Trade-off: this is a fixed-case reference, so it will not
        // match a column stored UPPER from an unquoted create — Rocky's own
        // tables are quoted lowercase, so that's the case we target.)
        let col_q = self.quote_identifier(timestamp_col);
        Ok(format!("WHERE {col_q} > '{literal}'::TIMESTAMP_NTZ"))
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
        // The connector ALSO enables `TRANSACTION_ABORT_ON_ERROR` for
        // multi-statement bodies (by prepending `ALTER SESSION SET ...` — the
        // SQL API rejects it as a request parameter). Without it, a failed DML
        // in a Snowflake transaction rolls back only its own changes and leaves
        // the transaction open — subsequent statements (including the
        // trailing `COMMIT`) are skipped, and the in-flight transaction only
        // unwinds implicitly when the REST session ends. That session-close
        // cleanup is incidental, not contractual: abort-on-error makes
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

    /// Snowflake's `DESCRIBE TABLE` returns native type names
    /// (`NUMBER(p,s)`, `VARCHAR(n)`, `TIMESTAMP_NTZ`), not the
    /// Databricks-flavored names the default allowlist matches.
    /// Override so drift evolution doesn't fall through to
    /// `DropAndRecreate` on safe widenings.
    ///
    /// Snowflake's `ALTER COLUMN ... SET DATA TYPE` accepts:
    /// - `NUMBER(p1,s) -> NUMBER(p2,s)` where `p2 >= p1` (precision
    ///   widening, scale preserved).
    /// - `VARCHAR(n) -> VARCHAR(m)` where `m >= n` (length widening).
    ///
    /// Family changes (`NUMBER -> VARCHAR`, `TIMESTAMP_NTZ ->
    /// TIMESTAMP_TZ`) and scale changes on `NUMBER` are not allowed
    /// by `ALTER COLUMN`, so treating them as unsafe avoids a
    /// partial-execution failure.
    fn is_safe_type_widening(&self, source_type: &str, target_type: &str) -> bool {
        is_safe_number_widening(target_type, source_type)
            || is_safe_snowflake_varchar_widening(target_type, source_type)
    }

    /// Snowflake folds unquoted identifiers to UPPERCASE at parse time.
    /// The default `alter_column_type_sql` interpolates `column` without
    /// quotes, which silently breaks against any column created with
    /// quoted lowercase names. `format_table_ref` already quotes the
    /// table parts; this override does the same for the column.
    fn alter_column_type_sql(
        &self,
        table_ref: &str,
        column: &str,
        new_type: &str,
    ) -> AdapterResult<String> {
        validation::validate_identifier(column).map_err(AdapterError::new)?;
        rocky_core::sql_gen::validate_sql_type(new_type).map_err(AdapterError::new)?;
        let col_q = self.quote_identifier(column);
        Ok(format!(
            "ALTER TABLE {table_ref} ALTER COLUMN {col_q} TYPE {new_type}"
        ))
    }
}

/// `NUMBER(p1,s) -> NUMBER(p2,s)` where `p2 >= p1` (precision widens,
/// scale must match). Snowflake's `DESCRIBE TABLE` returns `NUMBER` as
/// the canonical name even when the column was declared as `DECIMAL`,
/// `NUMERIC`, `INT`, or `BIGINT`, so this is the surface to match.
fn is_safe_number_widening(target: &str, source: &str) -> bool {
    fn parse_number(t: &str) -> Option<(u32, u32)> {
        let t = t.trim();
        if !t.starts_with("NUMBER(") || !t.ends_with(')') {
            return None;
        }
        let inner = &t[7..t.len() - 1];
        let parts: Vec<&str> = inner.split(',').collect();
        if parts.len() != 2 {
            return None;
        }
        let p = parts[0].trim().parse::<u32>().ok()?;
        let s = parts[1].trim().parse::<u32>().ok()?;
        Some((p, s))
    }

    if let (Some((tp, ts)), Some((sp, ss))) = (parse_number(target), parse_number(source)) {
        sp >= tp && ss == ts
    } else {
        false
    }
}

/// `VARCHAR(n) -> VARCHAR(m)` where `m >= n`.
///
/// Snowflake aliases `STRING`, `TEXT`, `CHAR`, `CHARACTER` to `VARCHAR`,
/// and `DESCRIBE TABLE` returns the canonical `VARCHAR(n)` regardless
/// of which alias was used at create time. Parsing only `VARCHAR(`
/// covers every text-column case.
fn is_safe_snowflake_varchar_widening(target: &str, source: &str) -> bool {
    fn parse_varchar(t: &str) -> Option<u32> {
        let t = t.trim();
        if !t.starts_with("VARCHAR(") || !t.ends_with(')') {
            return None;
        }
        t[8..t.len() - 1].trim().parse::<u32>().ok()
    }

    if let (Some(tn), Some(sn)) = (parse_varchar(target), parse_varchar(source)) {
        sn >= tn
    } else {
        false
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
    fn string_type_name_is_varchar() {
        // Snowflake accepts `CAST(... AS VARCHAR)`, so the data-grounding cast
        // uses the trait default rather than overriding to STRING.
        let d = dialect();
        assert_eq!(d.string_type_name(), "VARCHAR");
    }

    #[test]
    fn ground_table_ref_stays_unquoted() {
        // Critical regression guard: the grounding ref must stay UNQUOTED on
        // Snowflake even though `format_table_ref` double-quotes. Snowflake
        // folds unquoted identifiers to its default uppercase casing, so an
        // unquoted lowercase input still resolves a default-uppercase object;
        // double-quoting would lock in a case-sensitive lowercase name that
        // would not match. The MCP grounding path was live-verified against
        // Snowflake on the unquoted form.
        let d = dialect();
        assert_eq!(
            d.ground_table_ref(&["mydb", "public", "users"]).unwrap(),
            "mydb.public.users"
        );
        assert_eq!(
            d.ground_table_ref(&["public", "users"]).unwrap(),
            "public.users"
        );
        assert!(
            d.ground_table_ref(&["public", "users; DROP TABLE x"])
                .is_err()
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
        // Identifiers double-quoted so they survive Snowflake's
        // case-fold-on-unquoted rule — source/target tables created with
        // lowercase column names (the common case for Fivetran-loaded
        // raw tables) only resolve under explicit quoting.
        assert!(
            sql.contains("ON t.\"id\" = s.\"id\""),
            "expected double-quoted ON-clause keys, got: {sql}"
        );
        assert!(
            sql.contains("UPDATE SET t.\"name\" = s.\"name\", t.\"status\" = s.\"status\""),
            "expected double-quoted UPDATE SET cols, got: {sql}"
        );
        // Snowflake's MERGE rejects `INSERT *` shorthand too — the
        // WHEN NOT MATCHED branch must emit explicit
        // `INSERT (cols) VALUES (s.cols)`. Pin both clauses; a regression
        // that re-introduces `INSERT *` against Snowflake fails at the
        // parser, not at the runner.
        assert!(
            sql.contains("INSERT (\"name\", \"status\") VALUES (s.\"name\", s.\"status\")"),
            "expected explicit INSERT column list with double-quoted idents, got: {sql}"
        );
        assert!(
            !sql.contains("INSERT *"),
            "Snowflake MERGE must not emit `INSERT *`, got: {sql}"
        );
    }

    #[test]
    fn test_watermark_no_prior_uses_sentinel_timestamp_ntz() {
        let d = dialect();
        let sql = d.watermark_where("_fivetran_synced", None).unwrap();
        // The column is double-quoted so it resolves against a
        // lowercase-stored column (matching the merge/format_table_ref path)
        // rather than folding to UPPER.
        assert_eq!(
            sql,
            "WHERE \"_fivetran_synced\" > '1970-01-01 00:00:00'::TIMESTAMP_NTZ"
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
            "WHERE \"_fivetran_synced\" > '2026-04-17 09:30:00'::TIMESTAMP_NTZ"
        );
    }

    #[test]
    fn test_select_clause_double_quotes_columns_and_metadata_alias() {
        use rocky_ir::MetadataColumn;
        let d = dialect();
        let cols = ColumnSelection::Explicit(vec!["id".into(), "ts".into()]);
        let meta = vec![MetadataColumn {
            name: "_loaded_by".to_string(),
            data_type: "VARCHAR".to_string(),
            value: "'rocky'".to_string(),
        }];
        let sql = d.select_clause(&cols, &meta).unwrap();
        // Data columns and the metadata alias are double-quoted so they
        // resolve / materialize lowercase, matching the quoted merge path.
        assert_eq!(
            sql,
            "SELECT \"id\", \"ts\", CAST('rocky' AS VARCHAR) AS \"_loaded_by\""
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

    // Trait signature: `is_safe_type_widening(source_type, target_type)`
    // where `source_type` is the NEW desired type and `target_type` is
    // the CURRENT warehouse type. Widening means current -> new, so
    // call sites read `(new, current)`.

    #[test]
    fn is_safe_type_widening_number_precision_widens() {
        let d = dialect();
        // Warehouse currently NUMBER(10,0); want NUMBER(38,0).
        assert!(d.is_safe_type_widening("NUMBER(38,0)", "NUMBER(10,0)"));
        assert!(d.is_safe_type_widening("NUMBER(20,2)", "NUMBER(10,2)"));
    }

    #[test]
    fn is_safe_type_widening_number_narrowing_rejected() {
        let d = dialect();
        // Narrowing loses data.
        assert!(!d.is_safe_type_widening("NUMBER(10,0)", "NUMBER(38,0)"));
    }

    #[test]
    fn is_safe_type_widening_number_scale_change_rejected() {
        let d = dialect();
        // Snowflake's ALTER COLUMN refuses scale changes even if precision
        // widens. Reject so drift skips ALTER and uses DropAndRecreate.
        assert!(!d.is_safe_type_widening("NUMBER(10,4)", "NUMBER(10,2)"));
        assert!(!d.is_safe_type_widening("NUMBER(20,4)", "NUMBER(10,2)"));
    }

    #[test]
    fn is_safe_type_widening_varchar_length_widens() {
        let d = dialect();
        // Warehouse currently VARCHAR(100); want VARCHAR(1000).
        assert!(d.is_safe_type_widening("VARCHAR(1000)", "VARCHAR(100)"));
    }

    #[test]
    fn is_safe_type_widening_varchar_narrowing_rejected() {
        let d = dialect();
        assert!(!d.is_safe_type_widening("VARCHAR(100)", "VARCHAR(1000)"));
    }

    #[test]
    fn is_safe_type_widening_family_change_rejected() {
        let d = dialect();
        // Snowflake's ALTER COLUMN can't change type families.
        assert!(!d.is_safe_type_widening("VARCHAR(100)", "NUMBER(10,0)"));
        assert!(!d.is_safe_type_widening("NUMBER(10,0)", "VARCHAR(100)"));
    }

    #[test]
    fn is_safe_type_widening_databricks_names_rejected() {
        let d = dialect();
        // Default Databricks-flavored allowlist would accept these. On
        // Snowflake, DESCRIBE never returns these names; if they show up
        // they're from a wrong-dialect source, not a safe Snowflake widening.
        assert!(!d.is_safe_type_widening("STRING", "BIGINT"));
        assert!(!d.is_safe_type_widening("BIGINT", "INT"));
    }

    #[test]
    fn alter_column_type_sql_quotes_column() {
        let d = dialect();
        let sql = d
            .alter_column_type_sql("\"db\".\"sch\".\"tbl\"", "id", "NUMBER(38,0)")
            .unwrap();
        assert_eq!(
            sql,
            "ALTER TABLE \"db\".\"sch\".\"tbl\" ALTER COLUMN \"id\" TYPE NUMBER(38,0)"
        );
    }

    #[test]
    fn alter_column_type_sql_rejects_bad_identifier() {
        let d = dialect();
        let err = d
            .alter_column_type_sql("\"db\".\"sch\".\"tbl\"", "bad; DROP", "NUMBER(38,0)")
            .unwrap_err();
        assert!(err.to_string().contains("identifier"));
    }

    #[test]
    fn is_safe_type_widening_malformed_input_rejected() {
        let d = dialect();
        // Garbage in, false out (not a panic).
        assert!(!d.is_safe_type_widening("NUMBER(38,0)", "NUMBER("));
        assert!(!d.is_safe_type_widening("NUMBER(38,0)", "NUMBER(abc,0)"));
        assert!(!d.is_safe_type_widening("VARCHAR(100)", "VARCHAR()"));
    }
}
