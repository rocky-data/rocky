//! BigQuery SQL dialect implementation.

use rocky_core::traits::{AdapterError, AdapterResult, SqlDialect};
use rocky_ir::{ColumnSelection, MetadataColumn};
use rocky_sql::validation;

/// BigQuery SQL dialect.
#[derive(Debug, Clone)]
pub struct BigQueryDialect;

impl SqlDialect for BigQueryDialect {
    fn name(&self) -> &'static str {
        "bigquery"
    }

    fn format_table_ref(&self, catalog: &str, schema: &str, table: &str) -> AdapterResult<String> {
        // BigQuery uses project.dataset.table (three-part). The project
        // (catalog) component allows hyphens; dataset + table stay on
        // the stricter SQL-identifier rule.
        validation::validate_gcp_project_id(catalog).map_err(AdapterError::new)?;
        validation::validate_identifier(schema).map_err(AdapterError::new)?;
        validation::validate_identifier(table).map_err(AdapterError::new)?;
        Ok(format!("`{catalog}`.`{schema}`.`{table}`"))
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
                "MERGE requires at least one unique_key column",
            ));
        }

        for key in keys {
            validation::validate_identifier(key).map_err(AdapterError::new)?;
        }

        let join_cond = keys
            .iter()
            .map(|k| format!("target.{k} = source.{k}"))
            .collect::<Vec<_>>()
            .join(" AND ");

        let update_set = match update_cols {
            ColumnSelection::All => "target = source".to_string(),
            ColumnSelection::Explicit(cols) => cols
                .iter()
                .map(|c| format!("target.{c} = source.{c}"))
                .collect::<Vec<_>>()
                .join(", "),
        };

        Ok(format!(
            "MERGE INTO {target} AS target\n\
             USING ({source_sql}) AS source\n\
             ON {join_cond}\n\
             WHEN MATCHED THEN UPDATE SET {update_set}\n\
             WHEN NOT MATCHED THEN INSERT ROW"
        ))
    }

    fn select_clause(
        &self,
        columns: &ColumnSelection,
        metadata: &[MetadataColumn],
    ) -> AdapterResult<String> {
        let base = match columns {
            ColumnSelection::All => "SELECT *".to_string(),
            ColumnSelection::Explicit(cols) => {
                for col in cols {
                    validation::validate_identifier(col).map_err(AdapterError::new)?;
                }
                format!("SELECT {}", cols.join(", "))
            }
        };

        if metadata.is_empty() {
            return Ok(base);
        }

        let meta_cols: Vec<String> = metadata
            .iter()
            .map(|m| format!("CAST({} AS {}) AS {}", m.value, m.data_type, m.name))
            .collect();

        Ok(format!("{base}, {}", meta_cols.join(", ")))
    }

    fn watermark_where(
        &self,
        timestamp_col: &str,
        last_watermark: Option<&chrono::DateTime<chrono::Utc>>,
    ) -> AdapterResult<String> {
        validation::validate_identifier(timestamp_col).map_err(AdapterError::new)?;
        // BigQuery's `TIMESTAMP '...'` literal accepts the ISO 8601 form
        // with microsecond precision (six digits after the decimal). The
        // 1970-01-01 sentinel covers the first-run / post-`delete_watermark`
        // case so the WHERE clause stays permissive.
        let literal = last_watermark
            .map(|t| t.format("%Y-%m-%d %H:%M:%S%.f").to_string())
            .unwrap_or_else(|| "1970-01-01 00:00:00".to_string());
        Ok(format!("WHERE {timestamp_col} > TIMESTAMP '{literal}'"))
    }

    fn describe_table_sql(&self, table_ref: &str) -> String {
        // BigQuery doesn't have DESCRIBE; use INFORMATION_SCHEMA
        format!(
            "SELECT column_name, data_type, is_nullable \
             FROM `INFORMATION_SCHEMA`.`COLUMNS` \
             WHERE table_name = '{table_ref}'"
        )
    }

    fn drop_table_sql(&self, table_ref: &str) -> String {
        format!("DROP TABLE IF EXISTS {table_ref}")
    }

    fn create_catalog_sql(&self, _name: &str) -> Option<AdapterResult<String>> {
        // BigQuery projects are not created via SQL
        None
    }

    fn create_schema_sql(&self, catalog: &str, schema: &str) -> Option<AdapterResult<String>> {
        // BigQuery: CREATE SCHEMA = CREATE DATASET. Project (catalog)
        // allows hyphens; dataset stays on the strict identifier rule.
        let validate = || -> AdapterResult<String> {
            validation::validate_gcp_project_id(catalog).map_err(AdapterError::new)?;
            validation::validate_identifier(schema).map_err(AdapterError::new)?;
            Ok(format!(
                "CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`"
            ))
        };
        Some(validate())
    }

    fn tablesample_clause(&self, percent: u32) -> Option<String> {
        Some(format!("TABLESAMPLE SYSTEM ({percent} PERCENT)"))
    }

    fn insert_overwrite_partition(
        &self,
        target: &str,
        partition_filter: &str,
        select_sql: &str,
    ) -> AdapterResult<Vec<String>> {
        // BigQuery's REST API is stateless — each `jobs.query` call is its
        // own session, so issuing BEGIN/COMMIT as separate statements
        // fails with `Transaction control statements are supported only in
        // scripts or sessions`. Emit the DML transaction as a single
        // semicolon-joined script so the runtime submits all four
        // statements as one job, which BigQuery executes atomically and
        // auto-rolls-back on any sub-statement error.
        Ok(vec![format!(
            "BEGIN TRANSACTION;\n\
             DELETE FROM {target} WHERE {partition_filter};\n\
             INSERT INTO {target}\n{select_sql};\n\
             COMMIT TRANSACTION"
        )])
    }

    fn list_tables_sql(
        &self,
        catalog: &str,
        schema: &str,
    ) -> rocky_core::traits::AdapterResult<String> {
        // BigQuery's INFORMATION_SCHEMA.TABLES lives per-dataset, so the
        // four-part `project.dataset.INFORMATION_SCHEMA.TABLES` form is
        // required. Backtick-quote project + dataset per BQ conventions.
        // Project (catalog) allows hyphens; dataset stays strict.
        rocky_sql::validation::validate_gcp_project_id(catalog)
            .map_err(rocky_core::traits::AdapterError::new)?;
        rocky_sql::validation::validate_identifier(schema)
            .map_err(rocky_core::traits::AdapterError::new)?;
        Ok(format!(
            "SELECT table_name FROM `{catalog}`.`{schema}`.INFORMATION_SCHEMA.TABLES"
        ))
    }

    fn regex_match_predicate(
        &self,
        column: &str,
        pattern: &str,
    ) -> rocky_core::traits::AdapterResult<String> {
        Ok(format!("REGEXP_CONTAINS({column}, r'{pattern}')"))
    }

    fn date_minus_days_expr(&self, days: u32) -> rocky_core::traits::AdapterResult<String> {
        Ok(format!("DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)"))
    }

    fn current_timestamp_expr(&self) -> &'static str {
        "CURRENT_TIMESTAMP()"
    }

    fn string_type_name(&self) -> &'static str {
        // BigQuery has no `VARCHAR` type; the variable-length string type is
        // `STRING`. The trait default `"VARCHAR"` would make `CAST(... AS
        // VARCHAR)` a query error on BigQuery.
        "STRING"
    }

    fn surrogate_key_expr(&self, columns: &[&str]) -> String {
        // BigQuery's `MD5()` returns BYTES, so dbt-bigquery wraps it in
        // `to_hex(...)`; it also concatenates with `concat(...)` rather than the
        // `||` operator. Otherwise identical to the trait default.
        let str_type = self.string_type_name();
        let fields: Vec<String> = columns
            .iter()
            .map(|c| format!("coalesce(cast({c} as {str_type}), '_dbt_utils_surrogate_key_null_')"))
            .collect();
        let concatenated = if fields.is_empty() {
            "''".to_string()
        } else {
            let mut args = Vec::with_capacity(fields.len() * 2 - 1);
            for (i, f) in fields.into_iter().enumerate() {
                if i > 0 {
                    args.push("'-'".to_string());
                }
                args.push(f);
            }
            format!("concat({})", args.join(", "))
        };
        format!("to_hex(md5(cast({concatenated} as {str_type})))")
    }

    fn ground_table_ref(&self, parts: &[&str]) -> AdapterResult<String> {
        // BigQuery refs are `project.dataset.table`. The project (catalog)
        // segment allows hyphens — every real GCP project ID has them — so it
        // gets the hyphen-allowing `validate_gcp_project_id` rule while the
        // dataset + table stay on the strict SQL-identifier rule. Each segment
        // is backtick-quoted: a hyphenated ref like
        // `bigquery-public-data.samples.shakespeare` parses as subtraction
        // unless quoted, and double quotes denote STRING literals in BigQuery.
        match parts {
            [project, dataset, table] => {
                validation::validate_gcp_project_id(project).map_err(AdapterError::new)?;
                validation::validate_identifier(dataset).map_err(AdapterError::new)?;
                validation::validate_identifier(table).map_err(AdapterError::new)?;
                Ok(format!("`{project}`.`{dataset}`.`{table}`"))
            }
            [dataset, table] => {
                validation::validate_identifier(dataset).map_err(AdapterError::new)?;
                validation::validate_identifier(table).map_err(AdapterError::new)?;
                Ok(format!("`{dataset}`.`{table}`"))
            }
            _ => Err(AdapterError::msg(
                "table reference must be `dataset.table` or `project.dataset.table`",
            )),
        }
    }

    fn quote_identifier(&self, name: &str) -> String {
        // BigQuery uses backticks for identifiers; double quotes denote
        // STRING literals. The default `"name"` quoting from the trait
        // would silently turn column references into the literal text
        // and break any generated SQL that compares the column against
        // a numeric (BQ throws "STRING vs INT64" on the implicit cast).
        format!("`{name}`")
    }

    fn materialized_view_ddl(&self, target: &str, select_sql: &str) -> AdapterResult<String> {
        // BigQuery supports `CREATE OR REPLACE MATERIALIZED VIEW` as of
        // GA — the same form Databricks + Snowflake emit. Refresh schedule
        // is configured via the OPTIONS clause; Rocky's first-pass DDL
        // leaves it at BigQuery's default (auto-refresh on table change)
        // so callers can layer refresh-policy DDL on top later.
        Ok(format!(
            "CREATE OR REPLACE MATERIALIZED VIEW {target} AS\n{select_sql}"
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
        // FARM_FINGERPRINT(TO_JSON_STRING(STRUCT(...))) is the
        // collision-resistant integer hash BigQuery exposes natively;
        // see https://cloud.google.com/bigquery/docs/reference/standard-sql/hash_functions#farm_fingerprint.
        // TO_JSON_STRING serialises NULLs as `null` and stringifies
        // every column type deterministically, so per-row hashes are
        // stable across data-type promotions and `NULL`-handling
        // edge cases that would trip a CONCAT-based scheme.
        // FARM_FINGERPRINT returns INT64; `BIT_XOR(INT64)` returns
        // INT64, which round-trips cleanly to the kernel's `i128`
        // checksum slot (sign-extended; the parser bit-casts into
        // `u128`).
        let arg_list = columns
            .iter()
            .map(|c| format!("`{c}`"))
            .collect::<Vec<_>>()
            .join(", ");
        Ok(format!(
            "FARM_FINGERPRINT(TO_JSON_STRING(STRUCT({arg_list})))"
        ))
    }

    /// BigQuery's safe-widening allowlist.
    ///
    /// Strict subset of BigQuery's documented `ALTER COLUMN SET DATA
    /// TYPE` widenings — only the lossless numeric promotions:
    ///
    /// - `INT64 → NUMERIC`, `INT64 → BIGNUMERIC` — full INT64 range
    ///   fits losslessly in NUMERIC (precision 38) and BIGNUMERIC.
    /// - `NUMERIC → BIGNUMERIC` — strict precision widening.
    ///
    /// **Excluded by design:**
    ///
    /// - `INT64 → FLOAT64`, `NUMERIC → FLOAT64`, `BIGNUMERIC → FLOAT64`
    ///   — BigQuery accepts these via `SET DATA TYPE` but they are
    ///   lossy for absolute values > 2^53. Rocky's "safe widening"
    ///   contract excludes any conversion with potential value loss
    ///   (matches the strictness of the default Databricks/Spark
    ///   allowlist, which also omits `INT → FLOAT`).
    /// - **All `… → STRING` conversions.** Despite being lossless at
    ///   the value level, BigQuery's `ALTER COLUMN SET DATA TYPE`
    ///   rejects any conversion to `STRING` (`existing column type X
    ///   is not assignable to STRING`). The default allowlist's
    ///   "any numeric → STRING" pattern doesn't transfer to BigQuery.
    ///
    /// Drift involving any excluded pair falls through to
    /// `DropAndRecreate`.
    fn is_safe_type_widening(&self, source_type: &str, target_type: &str) -> bool {
        let src = source_type.to_uppercase();
        let tgt = target_type.to_uppercase();
        matches!(
            (tgt.as_str(), src.as_str()),
            ("INT64", "NUMERIC") | ("INT64", "BIGNUMERIC") | ("NUMERIC", "BIGNUMERIC"),
        )
    }

    /// BigQuery requires `ALTER COLUMN ... SET DATA TYPE ...`; the ANSI
    /// `ALTER COLUMN ... TYPE ...` form returns `Expected keyword DROP
    /// or keyword SET`. Other dialects use the default impl on the
    /// `SqlDialect` trait.
    fn alter_column_type_sql(
        &self,
        table_ref: &str,
        column: &str,
        new_type: &str,
    ) -> AdapterResult<String> {
        validation::validate_identifier(column).map_err(AdapterError::new)?;
        rocky_core::sql_gen::validate_sql_type(new_type).map_err(AdapterError::new)?;
        Ok(format!(
            "ALTER TABLE {table_ref} ALTER COLUMN {column} SET DATA TYPE {new_type}"
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_table_ref() {
        let d = BigQueryDialect;
        let r = d.format_table_ref("project", "dataset", "table").unwrap();
        assert_eq!(r, "`project`.`dataset`.`table`");
    }

    #[test]
    fn test_create_table_as() {
        let d = BigQueryDialect;
        let sql = d.create_table_as("`p`.`d`.`t`", "SELECT 1");
        assert!(sql.starts_with("CREATE OR REPLACE TABLE"));
    }

    #[test]
    fn test_merge_into() {
        let d = BigQueryDialect;
        let sql = d
            .merge_into(
                "`p`.`d`.`t`",
                "SELECT * FROM src",
                &["id".into()],
                &ColumnSelection::All,
            )
            .unwrap();
        assert!(sql.contains("MERGE INTO"));
        assert!(sql.contains("WHEN NOT MATCHED THEN INSERT ROW"));
    }

    #[test]
    fn test_view_ddl_emits_create_or_replace_view() {
        let d = BigQueryDialect;
        let sql = d.view_ddl("`p`.`d`.`v`", "SELECT * FROM src").unwrap();
        assert_eq!(
            sql,
            "CREATE OR REPLACE VIEW `p`.`d`.`v` AS\nSELECT * FROM src"
        );
    }

    #[test]
    fn test_materialized_view_ddl_emits_create_or_replace_mv() {
        let d = BigQueryDialect;
        let sql = d
            .materialized_view_ddl(
                "`p`.`d`.`mv`",
                "SELECT customer_id, SUM(total) FROM orders GROUP BY 1",
            )
            .unwrap();
        assert_eq!(
            sql,
            "CREATE OR REPLACE MATERIALIZED VIEW `p`.`d`.`mv` AS\n\
             SELECT customer_id, SUM(total) FROM orders GROUP BY 1"
        );
    }

    #[test]
    fn test_dynamic_table_ddl_unsupported_on_bigquery() {
        let d = BigQueryDialect;
        let err = d
            .dynamic_table_ddl("t", "SELECT 1", "1 minute", "wh")
            .expect_err("BigQuery has no dynamic-table concept");
        assert!(
            err.to_string().contains("DYNAMIC TABLE"),
            "error message should mention DT unsupported: {err}"
        );
    }

    #[test]
    fn test_insert_overwrite_partition() {
        let d = BigQueryDialect;
        let stmts = d
            .insert_overwrite_partition(
                "`p`.`d`.`t`",
                "date_col >= '2024-01-01'",
                "SELECT * FROM src",
            )
            .unwrap();
        // BigQuery requires the transaction to be a single script (its
        // REST API rejects standalone BEGIN/COMMIT calls).
        assert_eq!(stmts.len(), 1);
        let script = &stmts[0];
        assert!(script.starts_with("BEGIN TRANSACTION;\n"));
        assert!(script.contains("DELETE FROM `p`.`d`.`t` WHERE date_col >= '2024-01-01';"));
        assert!(script.contains("INSERT INTO `p`.`d`.`t`\nSELECT * FROM src;"));
        assert!(script.ends_with("COMMIT TRANSACTION"));
    }

    #[test]
    fn test_merge_requires_keys() {
        let d = BigQueryDialect;
        let result = d.merge_into("`p`.`d`.`t`", "SELECT 1", &[], &ColumnSelection::All);
        assert!(result.is_err());
    }

    #[test]
    fn test_select_clause_all() {
        let d = BigQueryDialect;
        let sql = d.select_clause(&ColumnSelection::All, &[]).unwrap();
        assert_eq!(sql, "SELECT *");
    }

    #[test]
    fn test_select_clause_with_metadata() {
        let d = BigQueryDialect;
        let meta = vec![MetadataColumn {
            name: "_loaded_by".to_string(),
            data_type: "STRING".to_string(),
            value: "NULL".to_string(),
        }];
        let sql = d.select_clause(&ColumnSelection::All, &meta).unwrap();
        assert!(sql.contains("CAST(NULL AS STRING) AS _loaded_by"));
    }

    #[test]
    fn test_watermark_where_no_prior_uses_sentinel() {
        let d = BigQueryDialect;
        let sql = d.watermark_where("_fivetran_synced", None).unwrap();
        assert_eq!(
            sql,
            "WHERE _fivetran_synced > TIMESTAMP '1970-01-01 00:00:00'"
        );
    }

    #[test]
    fn test_watermark_where_with_prior_substitutes_literal() {
        use chrono::TimeZone;
        let d = BigQueryDialect;
        let prior = chrono::Utc.with_ymd_and_hms(2026, 4, 17, 9, 30, 0).unwrap();
        let sql = d.watermark_where("_fivetran_synced", Some(&prior)).unwrap();
        // Source-side semantics: literal substitution, no MAX subquery.
        assert_eq!(
            sql,
            "WHERE _fivetran_synced > TIMESTAMP '2026-04-17 09:30:00'"
        );
    }

    #[test]
    fn test_create_catalog_returns_none() {
        let d = BigQueryDialect;
        assert!(d.create_catalog_sql("project").is_none());
    }

    #[test]
    fn test_create_schema() {
        let d = BigQueryDialect;
        let sql = d.create_schema_sql("project", "dataset").unwrap().unwrap();
        assert_eq!(sql, "CREATE SCHEMA IF NOT EXISTS `project`.`dataset`");
    }

    #[test]
    fn surrogate_key_uses_to_hex_md5_and_concat() {
        let d = BigQueryDialect;
        assert_eq!(
            d.surrogate_key_expr(&["a", "b"]),
            "to_hex(md5(cast(concat(coalesce(cast(a as STRING), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(b as STRING), '_dbt_utils_surrogate_key_null_')) as STRING)))"
        );
        // Single column: still wrapped in concat(...).
        assert_eq!(
            d.surrogate_key_expr(&["id"]),
            "to_hex(md5(cast(concat(coalesce(cast(id as STRING), '_dbt_utils_surrogate_key_null_')) as STRING)))"
        );
    }

    #[test]
    fn test_tablesample() {
        let d = BigQueryDialect;
        assert_eq!(
            d.tablesample_clause(10).unwrap(),
            "TABLESAMPLE SYSTEM (10 PERCENT)"
        );
    }

    #[test]
    fn test_drop_table() {
        let d = BigQueryDialect;
        assert_eq!(
            d.drop_table_sql("`p`.`d`.`t`"),
            "DROP TABLE IF EXISTS `p`.`d`.`t`"
        );
    }

    #[test]
    fn string_type_name_is_bigquery_string_not_varchar() {
        // BigQuery has no VARCHAR; the variable-length string type is STRING.
        let d = BigQueryDialect;
        assert_eq!(d.string_type_name(), "STRING");
    }

    #[test]
    fn ground_table_ref_backtick_quotes_hyphenated_project() {
        // Every real GCP project ID carries hyphens. The grounding ref builder
        // must accept the hyphenated project segment (via the project-id rule)
        // and backtick-quote the ref so `project.dataset.table` doesn't parse
        // as subtraction.
        let d = BigQueryDialect;
        assert_eq!(
            d.ground_table_ref(&["bigquery-public-data", "samples", "shakespeare"])
                .unwrap(),
            "`bigquery-public-data`.`samples`.`shakespeare`"
        );
        assert_eq!(
            d.ground_table_ref(&["my-proj-123", "ds", "tbl"]).unwrap(),
            "`my-proj-123`.`ds`.`tbl`"
        );
    }

    #[test]
    fn ground_table_ref_two_part_is_backtick_quoted() {
        // Dataset-qualified ref (no project) — both segments stay on the strict
        // identifier rule but are still backtick-quoted.
        let d = BigQueryDialect;
        assert_eq!(
            d.ground_table_ref(&["samples", "shakespeare"]).unwrap(),
            "`samples`.`shakespeare`"
        );
    }

    #[test]
    fn ground_table_ref_rejects_injection_and_bad_arity() {
        let d = BigQueryDialect;
        // Injection in any segment is rejected.
        assert!(
            d.ground_table_ref(&["bigquery-public-data", "samples", "a; DROP TABLE x"])
                .is_err()
        );
        // The strict project-id rule still rejects a hyphen in the dataset /
        // table segments (only the project segment allows hyphens).
        assert!(
            d.ground_table_ref(&["proj-ok-123", "bad-dataset", "tbl"])
                .is_err()
        );
        // Out-of-range arity.
        assert!(d.ground_table_ref(&["a", "b", "c", "d"]).is_err());
        assert!(d.ground_table_ref(&["just_one"]).is_err());
    }

    #[test]
    fn alter_column_type_sql_uses_bq_set_data_type_form() {
        let d = BigQueryDialect;
        let sql = d
            .alter_column_type_sql("`p`.`d`.`t`", "score", "NUMERIC")
            .unwrap();
        assert_eq!(
            sql,
            "ALTER TABLE `p`.`d`.`t` ALTER COLUMN score SET DATA TYPE NUMERIC"
        );
    }

    #[test]
    fn alter_column_type_sql_rejects_bad_identifier() {
        let d = BigQueryDialect;
        assert!(
            d.alter_column_type_sql("`p`.`d`.`t`", "bad; DROP", "NUMERIC")
                .is_err()
        );
    }

    #[test]
    fn alter_column_type_sql_rejects_bad_type() {
        let d = BigQueryDialect;
        assert!(
            d.alter_column_type_sql("`p`.`d`.`t`", "score", "NUMERIC; DROP")
                .is_err()
        );
    }

    #[test]
    fn is_safe_type_widening_int64_to_numeric() {
        let d = BigQueryDialect;
        assert!(d.is_safe_type_widening("NUMERIC", "INT64"));
        assert!(d.is_safe_type_widening("BIGNUMERIC", "INT64"));
        assert!(d.is_safe_type_widening("BIGNUMERIC", "NUMERIC"));
    }

    #[test]
    fn is_safe_type_widening_excludes_to_string() {
        // BigQuery's ALTER COLUMN SET DATA TYPE rejects any conversion
        // to STRING (`existing column type INT64 is not assignable to
        // STRING`), so even though STRING is lossless at the value
        // level, it can't be used as a drift evolution target.
        let d = BigQueryDialect;
        assert!(!d.is_safe_type_widening("STRING", "INT64"));
        assert!(!d.is_safe_type_widening("STRING", "NUMERIC"));
        assert!(!d.is_safe_type_widening("STRING", "FLOAT64"));
        assert!(!d.is_safe_type_widening("STRING", "BOOL"));
    }

    #[test]
    fn is_safe_type_widening_excludes_to_float64() {
        // BigQuery accepts these conversions via SET DATA TYPE, but
        // they are lossy for absolute values > 2^53. Rocky's "safe"
        // contract is strict — drift involving these falls through to
        // DropAndRecreate.
        let d = BigQueryDialect;
        assert!(!d.is_safe_type_widening("FLOAT64", "INT64"));
        assert!(!d.is_safe_type_widening("FLOAT64", "NUMERIC"));
        assert!(!d.is_safe_type_widening("FLOAT64", "BIGNUMERIC"));
    }

    #[test]
    fn is_safe_type_widening_excludes_narrowing_and_unrelated() {
        let d = BigQueryDialect;
        // Narrowing is never safe.
        assert!(!d.is_safe_type_widening("INT64", "STRING"));
        assert!(!d.is_safe_type_widening("INT64", "NUMERIC"));
        // Unrelated type families fall through.
        assert!(!d.is_safe_type_widening("DATE", "TIMESTAMP"));
    }

    /// Arg-order pin: `is_safe_type_widening(source_type, target_type)`
    /// answers "can the EXISTING target column be ALTERed to the new
    /// source type?" — i.e. the conversion direction is `target → source`.
    ///
    /// The call site (`rocky_core::drift`) invokes
    /// `is_safe_type_widening(&drifted.source_type, &drifted.target_type)`,
    /// so `source_type` is the new (source-side) type and `target_type` is
    /// the current target column type. A real INT64-column → NUMERIC
    /// widening is therefore `(source="NUMERIC", target="INT64")` and MUST
    /// be safe; the reverse `(source="INT64", target="NUMERIC")` is a
    /// narrowing and MUST NOT be. This pins the convention against the
    /// internal `(tgt, src)` match order — the match arm `("INT64",
    /// "NUMERIC")` is `(target=INT64, source=NUMERIC)`, which is the
    /// correct, non-inverted encoding of INT64 → NUMERIC.
    #[test]
    fn is_safe_type_widening_arg_order_matches_call_site() {
        let d = BigQueryDialect;
        // INT64 target column, NUMERIC source: a real widening → safe.
        assert!(
            d.is_safe_type_widening("NUMERIC", "INT64"),
            "INT64 column widening to NUMERIC source must be safe"
        );
        // The reverse is a narrowing → never safe.
        assert!(
            !d.is_safe_type_widening("INT64", "NUMERIC"),
            "NUMERIC column narrowing to INT64 source must NOT be safe"
        );
    }
}
