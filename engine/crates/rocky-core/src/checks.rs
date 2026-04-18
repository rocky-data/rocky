use std::fmt::Write;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::column_map;

use crate::ir::{ColumnInfo, TableRef};
use crate::sql_gen::SqlGenError;
use crate::tests::TestSeverity;
use crate::traits::SqlDialect;

/// Result of a single data quality check.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CheckResult {
    pub name: String,
    pub passed: bool,
    /// Severity reported when the check fails. `error` causes the quality
    /// pipeline to exit non-zero (subject to `fail_on_error`); `warning`
    /// is advisory and does not fail the run.
    #[serde(default)]
    pub severity: TestSeverity,
    #[serde(flatten)]
    pub details: CheckDetails,
}

/// Details of a specific check type, serialized as a flat JSON object.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum CheckDetails {
    /// Row count comparison between source and target tables.
    RowCount {
        source_count: u64,
        target_count: u64,
    },
    /// Column presence comparison between source and target schemas.
    ColumnMatch {
        missing: Vec<String>,
        extra: Vec<String>,
    },
    /// Data freshness measured as lag from the max timestamp to now.
    Freshness {
        lag_seconds: u64,
        threshold_seconds: u64,
    },
    /// Null rate for a specific column, sampled via TABLESAMPLE.
    NullRate {
        column: String,
        null_rate: f64,
        threshold: f64,
    },
    /// Row-level assertion evaluated via the `TestDecl` surface
    /// (`not_null`, `unique`, `accepted_values`, `relationships`,
    /// `expression`, `row_count_range`).
    ///
    /// `failing_rows` is the count of rows that violated the assertion;
    /// `0` means the check passed. For `row_count_range`, `failing_rows`
    /// stores the observed row count (the caller asserts pass/fail via
    /// the configured bounds).
    Assertion {
        /// Assertion kind — the `TestType` discriminant serialized as
        /// snake_case (e.g., `"not_null"`, `"accepted_values"`).
        kind: String,
        /// Column under test, when the assertion has one.
        #[serde(skip_serializing_if = "Option::is_none")]
        column: Option<String>,
        /// Number of failing rows (0 when passed). For `row_count_range`,
        /// this stores the observed total row count.
        failing_rows: u64,
    },
    /// User-defined SQL check evaluated against a threshold.
    Custom {
        query: String,
        result_value: u64,
        threshold: u64,
    },
}

/// Generates a batched null rate query using the given dialect's TABLESAMPLE syntax.
/// Falls back to a full table scan if the dialect does not support TABLESAMPLE.
pub fn generate_null_rate_sql(
    table: &TableRef,
    columns: &[String],
    sample_percent: u32,
    dialect: &dyn SqlDialect,
) -> Result<String, SqlGenError> {
    let ref_str = dialect.format_table_ref(&table.catalog, &table.schema, &table.table)?;

    let sample_clause = dialect
        .tablesample_clause(sample_percent)
        .unwrap_or_default();

    let mut sql = String::new();
    for (i, col) in columns.iter().enumerate() {
        rocky_sql::validation::validate_identifier(col)?;
        if i > 0 {
            let _ = write!(sql, "\nUNION ALL\n");
        }
        if sample_clause.is_empty() {
            // No TABLESAMPLE support — full table scan
            let _ = write!(
                sql,
                "SELECT '{col}' AS col, \
                 SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS nulls, \
                 COUNT(*) AS sampled \
                 FROM {ref_str}"
            );
        } else {
            let _ = write!(
                sql,
                "SELECT '{col}' AS col, \
                 SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS nulls, \
                 COUNT(*) AS sampled \
                 FROM (SELECT {col} FROM {ref_str} {sample_clause}) t"
            );
        }
    }

    Ok(sql)
}

/// Generates row count SQL using the given dialect.
pub fn generate_row_count_sql(
    table: &TableRef,
    dialect: &dyn SqlDialect,
) -> Result<String, SqlGenError> {
    let ref_str = dialect.format_table_ref(&table.catalog, &table.schema, &table.table)?;
    Ok(format!("SELECT COUNT(*) AS cnt FROM {ref_str}"))
}

/// Generates freshness SQL using the given dialect.
pub fn generate_freshness_sql(
    table: &TableRef,
    timestamp_column: &str,
    dialect: &dyn SqlDialect,
) -> Result<String, SqlGenError> {
    rocky_sql::validation::validate_identifier(timestamp_column)?;
    let ref_str = dialect.format_table_ref(&table.catalog, &table.schema, &table.table)?;
    Ok(format!(
        "SELECT MAX({timestamp_column}) AS max_ts FROM {ref_str}"
    ))
}

/// Generates SQL for a custom check using the given dialect.
/// `{target}` in the SQL template is replaced with the fully-qualified table reference.
pub fn generate_custom_check_sql(
    table: &TableRef,
    sql_template: &str,
    dialect: &dyn SqlDialect,
) -> Result<String, SqlGenError> {
    let ref_str = dialect.format_table_ref(&table.catalog, &table.schema, &table.table)?;
    Ok(sql_template.replace("{target}", &ref_str))
}

/// Compares source and target row counts.
pub fn check_row_count(source_count: u64, target_count: u64) -> CheckResult {
    CheckResult {
        name: "row_count".to_string(),
        passed: source_count == target_count,
        severity: TestSeverity::Error,
        details: CheckDetails::RowCount {
            source_count,
            target_count,
        },
    }
}

/// Compares source and target column sets.
///
/// Columns in `exclude` are ignored in both source and target (case-insensitive).
/// This is used to skip metadata columns added by Rocky (e.g., `permission_key`)
/// that exist only in the target.
pub fn check_column_match(
    source_columns: &[ColumnInfo],
    target_columns: &[ColumnInfo],
    exclude: &[String],
) -> CheckResult {
    let source_names = column_map::build_column_name_set(source_columns, exclude);
    let target_names = column_map::build_column_name_set(target_columns, exclude);

    let missing: Vec<String> = source_names.difference(&target_names).cloned().collect();
    let extra: Vec<String> = target_names.difference(&source_names).cloned().collect();
    let passed = missing.is_empty() && extra.is_empty();

    CheckResult {
        name: "column_match".to_string(),
        passed,
        severity: TestSeverity::Error,
        details: CheckDetails::ColumnMatch { missing, extra },
    }
}

/// Checks null rate for a column.
pub fn check_null_rate(column: &str, null_rate: f64, threshold: f64) -> CheckResult {
    CheckResult {
        name: "null_rate".to_string(),
        passed: null_rate <= threshold,
        severity: TestSeverity::Error,
        details: CheckDetails::NullRate {
            column: column.to_string(),
            null_rate,
            threshold,
        },
    }
}

/// Checks a custom SQL check result against a threshold.
pub fn check_custom(name: &str, query: &str, result_value: u64, threshold: u64) -> CheckResult {
    CheckResult {
        name: name.to_string(),
        passed: result_value <= threshold,
        severity: TestSeverity::Error,
        details: CheckDetails::Custom {
            query: query.to_string(),
            result_value,
            threshold,
        },
    }
}

/// Builds a `CheckResult` for a row-level assertion.
///
/// `kind` is the `TestType` discriminant in snake_case (e.g. `"not_null"`).
/// `passed` is classified by the caller — the classification is
/// kind-dependent (count-based for `not_null`/`expression`,
/// empty-result-set for `unique`/`accepted_values`/`relationships`,
/// range-check for `row_count_range`).
pub fn check_assertion(
    name: impl Into<String>,
    kind: impl Into<String>,
    column: Option<String>,
    failing_rows: u64,
    passed: bool,
    severity: TestSeverity,
) -> CheckResult {
    CheckResult {
        name: name.into(),
        passed,
        severity,
        details: CheckDetails::Assertion {
            kind: kind.into(),
            column,
            failing_rows,
        },
    }
}

/// Checks data freshness based on the lag between now and the max timestamp.
pub fn check_freshness(lag_seconds: u64, threshold_seconds: u64) -> CheckResult {
    CheckResult {
        name: "freshness".to_string(),
        passed: lag_seconds <= threshold_seconds,
        severity: TestSeverity::Error,
        details: CheckDetails::Freshness {
            lag_seconds,
            threshold_seconds,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::ColumnSelection;
    use crate::ir::MetadataColumn;
    use crate::traits::{AdapterError, AdapterResult, SqlDialect};

    /// Test dialect that mirrors Databricks behavior for rocky-core tests.
    struct TestDialect;

    impl SqlDialect for TestDialect {
        fn format_table_ref(
            &self,
            catalog: &str,
            schema: &str,
            table: &str,
        ) -> AdapterResult<String> {
            rocky_sql::validation::format_table_ref(catalog, schema, table)
                .map_err(AdapterError::new)
        }

        fn create_table_as(&self, target: &str, select_sql: &str) -> String {
            format!("CREATE OR REPLACE TABLE {target} AS\n{select_sql}")
        }

        fn insert_into(&self, target: &str, select_sql: &str) -> String {
            format!("INSERT INTO {target}\n{select_sql}")
        }

        fn merge_into(
            &self,
            _target: &str,
            _source_sql: &str,
            _keys: &[String],
            _update_cols: &ColumnSelection,
        ) -> AdapterResult<String> {
            unimplemented!()
        }

        fn select_clause(
            &self,
            _columns: &ColumnSelection,
            _metadata: &[MetadataColumn],
        ) -> AdapterResult<String> {
            unimplemented!()
        }

        fn watermark_where(
            &self,
            _timestamp_col: &str,
            _target_ref: &str,
        ) -> AdapterResult<String> {
            unimplemented!()
        }

        fn describe_table_sql(&self, table_ref: &str) -> String {
            format!("DESCRIBE TABLE {table_ref}")
        }

        fn drop_table_sql(&self, table_ref: &str) -> String {
            format!("DROP TABLE IF EXISTS {table_ref}")
        }

        fn create_catalog_sql(&self, _name: &str) -> Option<AdapterResult<String>> {
            None
        }

        fn create_schema_sql(
            &self,
            _catalog: &str,
            _schema: &str,
        ) -> Option<AdapterResult<String>> {
            None
        }

        fn tablesample_clause(&self, percent: u32) -> Option<String> {
            Some(format!("TABLESAMPLE ({percent} PERCENT)"))
        }

        fn insert_overwrite_partition(
            &self,
            _target: &str,
            _partition_filter: &str,
            _select_sql: &str,
        ) -> AdapterResult<Vec<String>> {
            unimplemented!()
        }
    }

    fn dialect() -> TestDialect {
        TestDialect
    }

    fn col(name: &str) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            data_type: "STRING".to_string(),
            nullable: true,
        }
    }

    fn table() -> TableRef {
        TableRef {
            catalog: "cat".into(),
            schema: "sch".into(),
            table: "tbl".into(),
        }
    }

    #[test]
    fn test_row_count_pass() {
        let r = check_row_count(100, 100);
        assert!(r.passed);
        assert_eq!(r.name, "row_count");
    }

    #[test]
    fn test_row_count_fail() {
        let r = check_row_count(100, 99);
        assert!(!r.passed);
    }

    #[test]
    fn test_column_match_pass() {
        let source = vec![col("id"), col("name")];
        let target = vec![col("id"), col("name")];
        let r = check_column_match(&source, &target, &[]);
        assert!(r.passed);
    }

    #[test]
    fn test_column_match_missing() {
        let source = vec![col("id"), col("name"), col("email")];
        let target = vec![col("id"), col("name")];
        let r = check_column_match(&source, &target, &[]);
        assert!(!r.passed);
        if let CheckDetails::ColumnMatch { missing, extra } = &r.details {
            assert_eq!(missing, &["email"]);
            assert!(extra.is_empty());
        }
    }

    #[test]
    fn test_column_match_extra() {
        let source = vec![col("id")];
        let target = vec![col("id"), col("old_col")];
        let r = check_column_match(&source, &target, &[]);
        assert!(!r.passed);
        if let CheckDetails::ColumnMatch { missing, extra } = &r.details {
            assert!(missing.is_empty());
            assert_eq!(extra, &["old_col"]);
        }
    }

    #[test]
    fn test_column_match_case_insensitive() {
        let source = vec![col("ID"), col("Name")];
        let target = vec![col("id"), col("name")];
        let r = check_column_match(&source, &target, &[]);
        assert!(r.passed);
    }

    #[test]
    fn test_column_match_exclude_metadata_columns() {
        let source = vec![col("id"), col("name")];
        let target = vec![col("id"), col("name"), col("permission_key")];
        // Without exclude: fails (permission_key is extra)
        let r = check_column_match(&source, &target, &[]);
        assert!(!r.passed);
        // With exclude: passes (permission_key is ignored)
        let r = check_column_match(&source, &target, &["permission_key".into()]);
        assert!(r.passed);
    }

    #[test]
    fn test_freshness_pass() {
        let r = check_freshness(300, 86400);
        assert!(r.passed);
    }

    #[test]
    fn test_freshness_fail() {
        let r = check_freshness(100000, 86400);
        assert!(!r.passed);
    }

    #[test]
    fn test_row_count_sql() {
        let sql = generate_row_count_sql(&table(), &dialect()).unwrap();
        assert_eq!(sql, "SELECT COUNT(*) AS cnt FROM cat.sch.tbl");
    }

    #[test]
    fn test_freshness_sql() {
        let sql = generate_freshness_sql(&table(), "_fivetran_synced", &dialect()).unwrap();
        assert_eq!(
            sql,
            "SELECT MAX(_fivetran_synced) AS max_ts FROM cat.sch.tbl"
        );
    }

    #[test]
    fn test_freshness_sql_rejects_bad_column() {
        assert!(generate_freshness_sql(&table(), "col; DROP TABLE", &dialect()).is_err());
    }

    #[test]
    fn test_check_result_serialization() {
        let r = check_row_count(100, 100);
        let json = serde_json::to_string(&r).unwrap();
        assert!(json.contains("row_count"));
        assert!(json.contains("source_count"));
    }

    #[test]
    fn test_null_rate_pass() {
        let r = check_null_rate("email", 0.02, 0.05);
        assert!(r.passed);
        assert_eq!(r.name, "null_rate");
    }

    #[test]
    fn test_null_rate_fail() {
        let r = check_null_rate("email", 0.10, 0.05);
        assert!(!r.passed);
    }

    #[test]
    fn test_null_rate_sql() {
        let sql = generate_null_rate_sql(
            &table(),
            &["email".into(), "customer_id".into()],
            10,
            &dialect(),
        )
        .unwrap();
        assert!(sql.contains("TABLESAMPLE (10 PERCENT)"));
        assert!(sql.contains("'email' AS col"));
        assert!(sql.contains("'customer_id' AS col"));
        assert!(sql.contains("UNION ALL"));
    }

    #[test]
    fn test_null_rate_sql_single_column() {
        let sql = generate_null_rate_sql(&table(), &["name".into()], 5, &dialect()).unwrap();
        assert!(sql.contains("TABLESAMPLE (5 PERCENT)"));
        assert!(!sql.contains("UNION ALL"));
    }

    #[test]
    fn test_null_rate_sql_rejects_bad_column() {
        assert!(generate_null_rate_sql(&table(), &["col; DROP".into()], 10, &dialect()).is_err());
    }

    #[test]
    fn test_custom_check_pass() {
        let r = check_custom("no_future_dates", "SELECT COUNT(*) ...", 0, 0);
        assert!(r.passed);
    }

    #[test]
    fn test_custom_check_fail() {
        let r = check_custom("no_future_dates", "SELECT COUNT(*) ...", 5, 0);
        assert!(!r.passed);
    }

    #[test]
    fn test_custom_check_sql_template() {
        let sql = generate_custom_check_sql(
            &table(),
            "SELECT COUNT(*) FROM {target} WHERE created_at > CURRENT_TIMESTAMP()",
            &dialect(),
        )
        .unwrap();
        assert_eq!(
            sql,
            "SELECT COUNT(*) FROM cat.sch.tbl WHERE created_at > CURRENT_TIMESTAMP()"
        );
    }

    #[test]
    fn test_custom_check_sql_multiple_placeholders() {
        let sql = generate_custom_check_sql(
            &table(),
            "SELECT COUNT(*) FROM {target} t1 JOIN {target} t2 ON t1.id = t2.parent_id",
            &dialect(),
        )
        .unwrap();
        assert!(sql.contains("FROM cat.sch.tbl t1 JOIN cat.sch.tbl t2"));
    }

    #[test]
    fn test_null_rate_serialization() {
        let r = check_null_rate("email", 0.03, 0.05);
        let json = serde_json::to_string(&r).unwrap();
        assert!(json.contains("null_rate"));
        assert!(json.contains("email"));
    }

    #[test]
    fn test_custom_check_serialization() {
        let r = check_custom("my_check", "SELECT 1", 0, 0);
        let json = serde_json::to_string(&r).unwrap();
        assert!(json.contains("my_check"));
    }
}
