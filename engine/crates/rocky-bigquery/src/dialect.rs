//! BigQuery SQL dialect implementation.

use rocky_core::ir::{ColumnSelection, MetadataColumn};
use rocky_core::traits::{AdapterError, AdapterResult, SqlDialect};
use rocky_sql::validation;

/// BigQuery SQL dialect.
#[derive(Debug, Clone)]
pub struct BigQueryDialect;

impl SqlDialect for BigQueryDialect {
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

    fn watermark_where(&self, timestamp_col: &str, target_ref: &str) -> AdapterResult<String> {
        validation::validate_identifier(timestamp_col).map_err(AdapterError::new)?;
        Ok(format!(
            "WHERE {timestamp_col} > (\n  \
             SELECT COALESCE(MAX({timestamp_col}), TIMESTAMP '1970-01-01')\n  \
             FROM {target_ref}\n)"
        ))
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
    fn test_watermark_where() {
        let d = BigQueryDialect;
        let sql = d
            .watermark_where("_fivetran_synced", "`p`.`d`.`t`")
            .unwrap();
        assert!(sql.contains("TIMESTAMP '1970-01-01'"));
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
}
