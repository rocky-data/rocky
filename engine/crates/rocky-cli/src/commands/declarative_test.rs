//! The declarative `[[tests]]` evaluator, shared by `rocky test --declarative`
//! and `rocky run --tests` (#2611).
//!
//! One evaluator, so a test means the same thing wherever it runs. It lives
//! outside `test.rs` because that module is built only with the `duckdb`
//! feature and the run loop is not.

use rocky_core::tests::{TestSeverity, TestType, generate_test_sql_for_table_ref};
use rocky_core::traits::WarehouseAdapter;
use tracing::warn;

use crate::output::DeclarativeTestResult;

/// Execute a single declarative test and return a result.
///
/// Shared by `rocky test --declarative` and `rocky run --tests`, so a test
/// means the same thing in both.
///
/// The query reads the table as the dialect quotes it
/// ([`rocky_core::traits::SqlDialect::format_table_ref`]), which is how the
/// run wrote it. The result's `table` is the plain `catalog.schema.table`.
pub(crate) async fn execute_one_test(
    test_decl: &rocky_core::tests::TestDecl,
    model_name: &str,
    target: &rocky_core::models::TargetConfig,
    adapter: &dyn WarehouseAdapter,
) -> DeclarativeTestResult {
    let test_type_name = test_type_label(&test_decl.test_type);
    let severity_str = match test_decl.severity {
        TestSeverity::Error => "error",
        TestSeverity::Warning => "warning",
    };
    let fq_table = format!("{}.{}.{}", target.catalog, target.schema, target.table);
    let fq_table = fq_table.as_str();

    // Generate SQL against the dialect-quoted reference.
    let generated = adapter
        .dialect()
        .format_table_ref(&target.catalog, &target.schema, &target.table)
        .map_err(|e| e.to_string())
        .and_then(|table_ref| {
            generate_test_sql_for_table_ref(test_decl, &table_ref, adapter.dialect())
                .map_err(|e| e.to_string())
        });
    let sql = match generated {
        Ok(sql) => sql,
        Err(e) => {
            return DeclarativeTestResult {
                model: model_name.to_string(),
                table: fq_table.to_string(),
                test_type: test_type_name.to_string(),
                column: test_decl.column.clone(),
                status: "error".to_string(),
                severity: severity_str.to_string(),
                detail: Some(format!("SQL generation error: {e}")),
                sql: String::new(),
            };
        }
    };

    // Execute SQL.
    let query_result = match adapter.execute_query(&sql).await {
        Ok(r) => r,
        Err(e) => {
            warn!(
                model = model_name,
                test_type = test_type_name,
                "declarative test execution error: {e}"
            );
            return DeclarativeTestResult {
                model: model_name.to_string(),
                table: fq_table.to_string(),
                test_type: test_type_name.to_string(),
                column: test_decl.column.clone(),
                status: "error".to_string(),
                severity: severity_str.to_string(),
                detail: Some(format!("execution error: {e}")),
                sql: sql.clone(),
            };
        }
    };

    // Interpret the result based on test type.
    let (status, detail) = interpret_result(&test_decl.test_type, &query_result);

    DeclarativeTestResult {
        model: model_name.to_string(),
        table: fq_table.to_string(),
        test_type: test_type_name.to_string(),
        column: test_decl.column.clone(),
        status,
        severity: severity_str.to_string(),
        detail,
        sql,
    }
}

/// Interpret the query result based on the test type.
///
/// Returns `(status, detail)` where status is "pass" or "fail", or "error"
/// when a count cell cannot be read (fail closed, never a pass).
fn interpret_result(
    test_type: &TestType,
    result: &rocky_core::traits::QueryResult,
) -> (String, Option<String>) {
    match test_type {
        // not_null / expression / in_range / regex_match / time_window / aggregate:
        // single-cell numeric result — 0 = pass, >0 = fail.
        TestType::NotNull
        | TestType::Expression { .. }
        | TestType::InRange { .. }
        | TestType::RegexMatch { .. }
        | TestType::NotInFuture
        | TestType::OlderThanNDays { .. }
        | TestType::Aggregate { .. } => {
            let Some(count) = first_row_count(result) else {
                return unreadable_count(result);
            };
            if count == 0 {
                ("pass".to_string(), None)
            } else {
                let what = match test_type {
                    TestType::NotNull => "NULL row(s)",
                    TestType::InRange { .. } => "out-of-range row(s)",
                    TestType::RegexMatch { .. } => "non-matching row(s)",
                    TestType::NotInFuture => "future-timestamped row(s)",
                    TestType::OlderThanNDays { .. } => "too-recent row(s)",
                    TestType::Aggregate { .. } => "aggregate failure",
                    _ => "violating row(s)",
                };
                ("fail".to_string(), Some(format!("{count} {what} found")))
            }
        }

        // unique / unique_expr / accepted_values / relationships / composite:
        // rows returned = failures
        TestType::Unique
        | TestType::UniqueExpr { .. }
        | TestType::AcceptedValues { .. }
        | TestType::Relationships { .. }
        | TestType::Composite { .. } => {
            let row_count = result.rows.len();
            if row_count == 0 {
                ("pass".to_string(), None)
            } else {
                let what = match test_type {
                    TestType::Unique => "duplicate value(s)",
                    TestType::UniqueExpr { .. } => "duplicate key(s)",
                    TestType::AcceptedValues { .. } => "unexpected value(s)",
                    TestType::Relationships { .. } => "orphaned row(s)",
                    TestType::Composite { .. } => "duplicate key(s)",
                    _ => unreachable!(),
                };
                (
                    "fail".to_string(),
                    Some(format!("{row_count} {what} found")),
                )
            }
        }

        // row_count_range: SELECT COUNT(*) — pass when min <= count <= max
        TestType::RowCountRange { min, max } => {
            let Some(count) = first_row_count(result) else {
                return unreadable_count(result);
            };
            let above_min = min.is_none_or(|lo| count >= lo);
            let below_max = max.is_none_or(|hi| count <= hi);
            let in_range = above_min && below_max;
            if in_range {
                ("pass".to_string(), Some(format!("row count: {count}")))
            } else {
                let bound = match (min, max) {
                    (Some(lo), Some(hi)) => format!("[{lo}, {hi}]"),
                    (Some(lo), None) => format!("[{lo}, +inf)"),
                    (None, Some(hi)) => format!("(-inf, {hi}]"),
                    (None, None) => "any".to_string(),
                };
                (
                    "fail".to_string(),
                    Some(format!("row count {count} outside range {bound}")),
                )
            }
        }
    }
}

/// Extract the count from the first row's first column of a COUNT(*) query.
///
/// `None` when the cell is missing, NULL, negative, fractional or not a
/// number. The caller reports that as status `error`, never as a pass.
fn first_row_count(result: &rocky_core::traits::QueryResult) -> Option<u64> {
    result
        .rows
        .first()
        .and_then(|row| row.first())
        .and_then(|v| match v {
            serde_json::Value::Number(n) => n.as_u64(),
            serde_json::Value::String(s) => s.trim().parse::<u64>().ok(),
            _ => None,
        })
}

/// The status and detail for a count cell that could not be read.
fn unreadable_count(result: &rocky_core::traits::QueryResult) -> (String, Option<String>) {
    let cell = result
        .rows
        .first()
        .and_then(|row| row.first())
        .map_or_else(|| "no row".to_string(), ToString::to_string);
    (
        "error".to_string(),
        Some(format!(
            "the test query returned {cell}, not a non-negative whole count"
        )),
    )
}

/// Human-readable label for a test type.
fn test_type_label(tt: &TestType) -> &'static str {
    match tt {
        TestType::NotNull => "not_null",
        TestType::Unique => "unique",
        TestType::UniqueExpr { .. } => "unique_expr",
        TestType::AcceptedValues { .. } => "accepted_values",
        TestType::Relationships { .. } => "relationships",
        TestType::Expression { .. } => "expression",
        TestType::RowCountRange { .. } => "row_count_range",
        TestType::InRange { .. } => "in_range",
        TestType::RegexMatch { .. } => "regex_match",
        TestType::Aggregate { .. } => "aggregate",
        TestType::Composite { .. } => "composite",
        TestType::NotInFuture => "not_in_future",
        TestType::OlderThanNDays { .. } => "older_than_n_days",
    }
}

#[cfg(test)]
mod tests {
    use super::interpret_result;
    use rocky_core::tests::TestType;
    use rocky_core::traits::QueryResult;
    use serde_json::{Value, json};

    fn one_cell(v: Value) -> QueryResult {
        QueryResult {
            columns: vec!["n".to_string()],
            rows: vec![vec![v]],
        }
    }

    #[test]
    fn a_zero_count_passes_and_a_positive_count_fails() {
        assert_eq!(
            interpret_result(&TestType::NotNull, &one_cell(json!(0))).0,
            "pass"
        );
        assert_eq!(
            interpret_result(&TestType::NotNull, &one_cell(json!("0"))).0,
            "pass"
        );
        assert_eq!(
            interpret_result(&TestType::NotNull, &one_cell(json!(3))).0,
            "fail"
        );
    }

    #[test]
    fn a_count_that_cannot_be_read_is_an_error_never_a_pass() {
        let unreadable = [
            one_cell(Value::Null),
            one_cell(json!(-1)),
            one_cell(json!(1.5)),
            one_cell(json!("many")),
            QueryResult {
                columns: vec!["n".to_string()],
                rows: vec![],
            },
        ];
        for result in &unreadable {
            for test_type in [
                TestType::NotNull,
                TestType::RowCountRange {
                    min: None,
                    max: None,
                },
            ] {
                let (status, detail) = interpret_result(&test_type, result);
                assert_eq!(status, "error", "{result:?} {detail:?}");
            }
        }
    }
}
