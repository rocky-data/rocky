//! The declarative `[[tests]]` evaluator, shared by `rocky test --declarative`
//! and `rocky run --tests` (#2611).
//!
//! One evaluator, so a test means the same thing wherever it runs. It lives
//! outside `test.rs` because that module is built only with the `duckdb`
//! feature and the run loop is not.

use rocky_core::tests::{
    TestDecl, TestSeverity, TestType, generate_batched_not_null_sql, generate_batched_unique_sql,
    generate_test_sql_for_table_ref,
};
use rocky_core::traits::{QueryResult, WarehouseAdapter};
use tracing::{debug, warn};

use crate::output::DeclarativeTestResult;

/// Whether a model's default-config `unique` and `not_null` tests share a
/// query (#2756). `rocky test --declarative --batch-tests` and
/// `rocky run --batch-tests` turn it on. Off by default.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum TestBatching {
    /// One query per test. The default.
    #[default]
    Off,
    /// One query for a model's eligible `not_null` tests, and one for its
    /// eligible `unique` tests, when it has at least two of that type.
    PerModel,
}

impl TestBatching {
    /// `PerModel` when the flag is set, `Off` otherwise.
    #[must_use]
    pub fn from_flag(batch_tests: bool) -> Self {
        if batch_tests {
            Self::PerModel
        } else {
            Self::Off
        }
    }
}

/// The results of one model's `[[tests]]`, in declaration order, and how many
/// queries produced them.
pub(crate) struct ModelTestRun {
    /// One result per test, in the order the tests are declared.
    pub(crate) results: Vec<DeclarativeTestResult>,
    /// How many test queries were sent to the warehouse.
    pub(crate) queries: usize,
}

/// The two kinds a batch can hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BatchKind {
    NotNull,
    Unique,
}

/// The batch a test may join: `Some` only for a `not_null` or `unique` test
/// with default config — error severity, no `filter`, and a column that
/// passes identifier validation. Everything else runs singly, so its result
/// (an error message included) is exactly the unbatched one.
fn batch_kind(test: &TestDecl) -> Option<BatchKind> {
    let kind = match test.test_type {
        TestType::NotNull => BatchKind::NotNull,
        TestType::Unique => BatchKind::Unique,
        TestType::UniqueExpr { .. }
        | TestType::AcceptedValues { .. }
        | TestType::Relationships { .. }
        | TestType::Expression { .. }
        | TestType::RowCountRange { .. }
        | TestType::InRange { .. }
        | TestType::RegexMatch { .. }
        | TestType::Aggregate { .. }
        | TestType::Composite { .. }
        | TestType::NotInFuture
        | TestType::OlderThanNDays { .. } => return None,
    };
    if test.severity != TestSeverity::Error || test.filter.is_some() {
        return None;
    }
    let column = test.column.as_deref()?;
    rocky_sql::validation::validate_identifier(column).ok()?;
    Some(kind)
}

/// Execute every test of one model and return the results in declaration
/// order.
///
/// Shared by `rocky test --declarative` and `rocky run --tests`, so batching
/// means the same thing in both. With [`TestBatching::Off`] this is one
/// single query per test, exactly as before batching existed.
///
/// With [`TestBatching::PerModel`], two or more eligible tests of one kind
/// (see [`batch_kind`]) share one query. Each batched result has the same
/// model, table, type, column, severity, status and detail as the single
/// query gives; only `sql` names the shared query. When the shared query
/// errors, or returns a shape this code does not expect, its tests run
/// singly instead, so one bad column errors one test.
pub(crate) async fn execute_model_tests(
    tests: &[TestDecl],
    model_name: &str,
    target: &rocky_core::models::TargetConfig,
    adapter: &dyn WarehouseAdapter,
    batching: TestBatching,
) -> ModelTestRun {
    let mut slots: Vec<Option<DeclarativeTestResult>> = tests.iter().map(|_| None).collect();
    let mut queries = 0;

    if batching == TestBatching::PerModel
        && let Ok(table_ref) =
            adapter
                .dialect()
                .format_table_ref(&target.catalog, &target.schema, &target.table)
    {
        for kind in [BatchKind::NotNull, BatchKind::Unique] {
            let members: Vec<usize> = tests
                .iter()
                .enumerate()
                .filter(|(_, t)| batch_kind(t) == Some(kind))
                .map(|(i, _)| i)
                .collect();
            if members.len() < 2 {
                continue;
            }
            let decls: Vec<&TestDecl> = members.iter().map(|&i| &tests[i]).collect();
            let (batched, queried) =
                run_batch(kind, &decls, &table_ref, model_name, target, adapter).await;
            queries += usize::from(queried);
            let Some(batched) = batched else {
                continue;
            };
            for (slot, result) in members.into_iter().zip(batched) {
                slots[slot] = Some(result);
            }
        }
    }

    let mut results = Vec::with_capacity(tests.len());
    for (decl, slot) in tests.iter().zip(slots) {
        let result = match slot {
            Some(result) => result,
            None => {
                let (result, queried) = execute_single(decl, model_name, target, adapter).await;
                queries += usize::from(queried);
                result
            }
        };
        results.push(result);
    }
    debug!(
        model = model_name,
        tests = tests.len(),
        queries,
        "declarative tests executed"
    );
    ModelTestRun { results, queries }
}

/// Run one batch. A `None` result means the caller runs these tests singly:
/// the SQL could not be generated, the query errored, or its result was not
/// the shape the batch SQL promises. A surprise never passes a test. The
/// `bool` is whether a query was sent.
async fn run_batch(
    kind: BatchKind,
    decls: &[&TestDecl],
    table_ref: &str,
    model_name: &str,
    target: &rocky_core::models::TargetConfig,
    adapter: &dyn WarehouseAdapter,
) -> (Option<Vec<DeclarativeTestResult>>, bool) {
    let Some(columns) = decls
        .iter()
        .map(|d| d.column.as_deref())
        .collect::<Option<Vec<&str>>>()
    else {
        return (None, false);
    };
    let generated = match kind {
        BatchKind::NotNull => generate_batched_not_null_sql(&columns, table_ref),
        BatchKind::Unique => generate_batched_unique_sql(&columns, table_ref),
    };
    let Ok(sql) = generated else {
        return (None, false);
    };
    let result = match adapter.execute_query(&sql).await {
        Ok(result) => result,
        Err(e) => {
            warn!(
                model = model_name,
                tests = decls.len(),
                "batched declarative test query failed, running its tests singly: {e}"
            );
            return (None, true);
        }
    };
    let counts = match kind {
        BatchKind::NotNull => not_null_batch_counts(&result, decls.len()),
        BatchKind::Unique => unique_batch_counts(&result, decls.len()),
    };
    let Some(counts) = counts else {
        warn!(
            model = model_name,
            tests = decls.len(),
            "batched declarative test query returned an unexpected shape, running its tests singly"
        );
        return (None, true);
    };
    let fq_table = format!("{}.{}.{}", target.catalog, target.schema, target.table);
    let results = decls
        .iter()
        .zip(counts)
        .map(|(decl, count)| {
            let (status, detail) = count_verdict(&decl.test_type, count);
            DeclarativeTestResult {
                model: model_name.to_string(),
                table: fq_table.clone(),
                test_type: test_type_label(&decl.test_type).to_string(),
                column: decl.column.clone(),
                status,
                severity: severity_label(decl.severity).to_string(),
                detail,
                sql: sql.clone(),
            }
        })
        .collect();
    (Some(results), true)
}

/// The per-column counts of a batched `not_null` query: one row with exactly
/// `n` readable counts.
fn not_null_batch_counts(result: &QueryResult, n: usize) -> Option<Vec<u64>> {
    let [row] = result.rows.as_slice() else {
        return None;
    };
    if row.len() != n {
        return None;
    }
    row.iter().map(cell_count).collect()
}

/// The per-column counts of a batched `unique` query: exactly `n` rows, each
/// `(index, count)`, every index in `0..n` exactly once.
fn unique_batch_counts(result: &QueryResult, n: usize) -> Option<Vec<u64>> {
    if result.rows.len() != n {
        return None;
    }
    let mut counts: Vec<Option<u64>> = vec![None; n];
    for row in &result.rows {
        let [index, count] = row.as_slice() else {
            return None;
        };
        let index = usize::try_from(cell_count(index)?).ok()?;
        let slot = counts.get_mut(index)?;
        if slot.is_some() {
            return None;
        }
        *slot = Some(cell_count(count)?);
    }
    counts.into_iter().collect()
}

fn severity_label(severity: TestSeverity) -> &'static str {
    match severity {
        TestSeverity::Error => "error",
        TestSeverity::Warning => "warning",
    }
}

/// Execute a single declarative test. The `bool` is whether a query was
/// sent (`false` when SQL generation failed first).
///
/// The query reads the table as the dialect quotes it
/// ([`rocky_core::traits::SqlDialect::format_table_ref`]), which is how the
/// run wrote it. The result's `table` is the plain `catalog.schema.table`.
async fn execute_single(
    test_decl: &TestDecl,
    model_name: &str,
    target: &rocky_core::models::TargetConfig,
    adapter: &dyn WarehouseAdapter,
) -> (DeclarativeTestResult, bool) {
    let test_type_name = test_type_label(&test_decl.test_type);
    let severity_str = severity_label(test_decl.severity);
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
            let result = DeclarativeTestResult {
                model: model_name.to_string(),
                table: fq_table.to_string(),
                test_type: test_type_name.to_string(),
                column: test_decl.column.clone(),
                status: "error".to_string(),
                severity: severity_str.to_string(),
                detail: Some(format!("SQL generation error: {e}")),
                sql: String::new(),
            };
            return (result, false);
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
            let result = DeclarativeTestResult {
                model: model_name.to_string(),
                table: fq_table.to_string(),
                test_type: test_type_name.to_string(),
                column: test_decl.column.clone(),
                status: "error".to_string(),
                severity: severity_str.to_string(),
                detail: Some(format!("execution error: {e}")),
                sql: sql.clone(),
            };
            return (result, true);
        }
    };

    // Interpret the result based on test type.
    let (status, detail) = interpret_result(&test_decl.test_type, &query_result);

    let result = DeclarativeTestResult {
        model: model_name.to_string(),
        table: fq_table.to_string(),
        test_type: test_type_name.to_string(),
        column: test_decl.column.clone(),
        status,
        severity: severity_str.to_string(),
        detail,
        sql,
    };
    (result, true)
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
            count_verdict(test_type, count)
        }

        // unique / unique_expr / accepted_values / relationships / composite:
        // rows returned = failures
        TestType::Unique
        | TestType::UniqueExpr { .. }
        | TestType::AcceptedValues { .. }
        | TestType::Relationships { .. }
        | TestType::Composite { .. } => count_verdict(test_type, result.rows.len() as u64),

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

/// The verdict for a test whose failing count is `count`: `pass` at zero,
/// otherwise `fail` with `"{count} <what> found"`.
///
/// The one place this text is made, so a batched result (#2756) and a single
/// one cannot word the same count differently. `row_count_range` has its own
/// verdict in [`interpret_result`] and never reaches here.
fn count_verdict(test_type: &TestType, count: u64) -> (String, Option<String>) {
    if count == 0 {
        return ("pass".to_string(), None);
    }
    let what = match test_type {
        TestType::NotNull => "NULL row(s)",
        TestType::InRange { .. } => "out-of-range row(s)",
        TestType::RegexMatch { .. } => "non-matching row(s)",
        TestType::NotInFuture => "future-timestamped row(s)",
        TestType::OlderThanNDays { .. } => "too-recent row(s)",
        TestType::Aggregate { .. } => "aggregate failure",
        TestType::Expression { .. } | TestType::RowCountRange { .. } => "violating row(s)",
        TestType::Unique => "duplicate value(s)",
        TestType::UniqueExpr { .. } | TestType::Composite { .. } => "duplicate key(s)",
        TestType::AcceptedValues { .. } => "unexpected value(s)",
        TestType::Relationships { .. } => "orphaned row(s)",
    };
    ("fail".to_string(), Some(format!("{count} {what} found")))
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
        .and_then(cell_count)
}

/// A count cell as a non-negative whole number, or `None`.
fn cell_count(v: &serde_json::Value) -> Option<u64> {
    match v {
        serde_json::Value::Number(n) => n.as_u64(),
        serde_json::Value::String(s) => s.trim().parse::<u64>().ok(),
        _ => None,
    }
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
mod batch_shape_tests {
    use super::{not_null_batch_counts, unique_batch_counts};
    use rocky_core::traits::QueryResult;
    use serde_json::{Value, json};

    fn rows(rows: Vec<Vec<Value>>) -> QueryResult {
        QueryResult {
            columns: vec![],
            rows,
        }
    }

    #[test]
    fn a_well_formed_batch_result_is_read_by_index() {
        assert_eq!(
            not_null_batch_counts(&rows(vec![vec![json!(0), json!("3")]]), 2),
            Some(vec![0, 3])
        );
        // Rows out of order and a string-typed index still map by index.
        assert_eq!(
            unique_batch_counts(
                &rows(vec![vec![json!("1"), json!(4)], vec![json!(0), json!(0)]]),
                2
            ),
            Some(vec![0, 4])
        );
    }

    /// Every surprise falls back to single queries; none reads as a pass.
    #[test]
    fn an_unexpected_batch_shape_is_refused() {
        let not_null = [
            rows(vec![]),
            rows(vec![vec![json!(0)]]),
            rows(vec![vec![json!(0), json!(0), json!(0)]]),
            rows(vec![vec![json!(0), Value::Null]]),
            rows(vec![vec![json!(0), json!(-1)]]),
            rows(vec![vec![json!(0), json!(1.5)]]),
            rows(vec![vec![json!(0), json!(0)], vec![json!(0), json!(0)]]),
        ];
        for result in &not_null {
            assert_eq!(not_null_batch_counts(result, 2), None, "{result:?}");
        }
        let unique = [
            rows(vec![vec![json!(0), json!(0)]]),
            rows(vec![vec![json!(0), json!(0)], vec![json!(0), json!(1)]]),
            rows(vec![vec![json!(0), json!(0)], vec![json!(2), json!(1)]]),
            rows(vec![vec![json!(0), json!(0)], vec![json!(1)]]),
            rows(vec![vec![json!(0), json!(0)], vec![json!(1), Value::Null]]),
            rows(vec![vec![json!(0), json!(0)], vec![Value::Null, json!(0)]]),
            rows(vec![
                vec![json!(0), json!(0)],
                vec![json!(1), json!(0)],
                vec![json!(1), json!(0)],
            ]),
        ];
        for result in &unique {
            assert_eq!(unique_batch_counts(result, 2), None, "{result:?}");
        }
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

/// Batched and unbatched agree (#2756). Each test runs the same `[[tests]]`
/// in both modes against one DuckDB table and compares every field but `sql`.
#[cfg(all(test, feature = "duckdb"))]
mod batching_tests {
    use super::{ModelTestRun, TestBatching, execute_model_tests};
    use rocky_core::models::TargetConfig;
    use rocky_core::tests::{CompositeKind, TestDecl, TestSeverity, TestType};
    use rocky_core::traits::WarehouseAdapter;
    use rocky_duckdb::adapter::DuckDbWarehouseAdapter;

    fn decl(test_type: TestType, column: Option<&str>) -> TestDecl {
        TestDecl {
            test_type,
            column: column.map(str::to_string),
            severity: TestSeverity::Error,
            filter: None,
        }
    }

    fn unique(column: &str) -> TestDecl {
        decl(TestType::Unique, Some(column))
    }

    fn not_null(column: &str) -> TestDecl {
        decl(TestType::NotNull, Some(column))
    }

    fn target() -> TargetConfig {
        TargetConfig {
            catalog: String::new(),
            schema: "main".to_string(),
            table: "t".to_string(),
        }
    }

    async fn adapter_with(create: &str) -> DuckDbWarehouseAdapter {
        let adapter = DuckDbWarehouseAdapter::in_memory().expect("open duckdb");
        adapter.execute_statement(create).await.expect("create");
        adapter
    }

    /// The fields the issue binds, `detail` (which carries the failing
    /// count) included. `sql` is the only field allowed to differ.
    fn identity(run: &ModelTestRun) -> Vec<[Option<String>; 7]> {
        run.results
            .iter()
            .map(|r| {
                [
                    Some(r.model.clone()),
                    Some(r.table.clone()),
                    Some(r.test_type.clone()),
                    r.column.clone(),
                    Some(r.severity.clone()),
                    Some(r.status.clone()),
                    r.detail.clone(),
                ]
            })
            .collect()
    }

    /// Run `tests` in both modes and assert the results agree. Returns
    /// `(unbatched, batched)` for the caller's own assertions.
    async fn both_modes(
        adapter: &DuckDbWarehouseAdapter,
        tests: &[TestDecl],
    ) -> (ModelTestRun, ModelTestRun) {
        let single = execute_model_tests(tests, "m", &target(), adapter, TestBatching::Off).await;
        let batched =
            execute_model_tests(tests, "m", &target(), adapter, TestBatching::PerModel).await;
        assert_eq!(identity(&single), identity(&batched));
        assert_eq!(single.results.len(), tests.len());
        assert_eq!(
            single.queries,
            tests.len(),
            "unbatched is one query per test"
        );
        (single, batched)
    }

    fn statuses(run: &ModelTestRun) -> Vec<&str> {
        run.results.iter().map(|r| r.status.as_str()).collect()
    }

    #[tokio::test]
    async fn a_failing_unique_beside_a_passing_not_null() {
        let adapter = adapter_with(
            "CREATE TABLE main.t AS SELECT * FROM (VALUES \
             (1, 'a', 'x'), (1, 'b', 'y'), (2, 'b', 'z')) v(id, name, tag)",
        )
        .await;
        let tests = [
            unique("id"),
            not_null("id"),
            unique("name"),
            not_null("tag"),
        ];
        let (single, batched) = both_modes(&adapter, &tests).await;
        assert_eq!(batched.queries, 2, "one unique query, one not_null query");
        assert_eq!(statuses(&single), ["fail", "pass", "fail", "pass"]);
        assert_eq!(
            single.results[0].detail.as_deref(),
            Some("1 duplicate value(s) found")
        );
        assert!(batched.results[0].sql.contains("UNION ALL"));
        assert!(batched.results[1].sql.contains("rocky_n1"));
    }

    #[tokio::test]
    async fn null_keys_count_as_a_duplicate_group() {
        let adapter = adapter_with(
            "CREATE TABLE main.t AS SELECT * FROM (VALUES \
             (NULL, 1), (NULL, 2), (3, NULL), (4, NULL), (4, 5)) v(a, b)",
        )
        .await;
        let tests = [unique("a"), unique("b"), not_null("a"), not_null("b")];
        let (single, batched) = both_modes(&adapter, &tests).await;
        assert_eq!(batched.queries, 2);
        let details: Vec<Option<&str>> =
            single.results.iter().map(|r| r.detail.as_deref()).collect();
        assert_eq!(
            details,
            [
                // a: the NULL group and the 4 group.
                Some("2 duplicate value(s) found"),
                // b: the NULL group only.
                Some("1 duplicate value(s) found"),
                Some("2 NULL row(s) found"),
                Some("2 NULL row(s) found"),
            ]
        );
    }

    #[tokio::test]
    async fn an_empty_table_passes_in_both_modes() {
        let adapter = adapter_with("CREATE TABLE main.t (a INTEGER, b VARCHAR)").await;
        let tests = [unique("a"), unique("b"), not_null("a"), not_null("b")];
        let (single, batched) = both_modes(&adapter, &tests).await;
        assert_eq!(batched.queries, 2);
        assert_eq!(statuses(&single), ["pass", "pass", "pass", "pass"]);
    }

    #[tokio::test]
    async fn an_all_null_column() {
        let adapter = adapter_with(
            "CREATE TABLE main.t AS SELECT CAST(NULL AS INTEGER) AS a, i AS b FROM range(3) r(i)",
        )
        .await;
        let tests = [unique("a"), unique("b"), not_null("a"), not_null("b")];
        let (single, batched) = both_modes(&adapter, &tests).await;
        assert_eq!(batched.queries, 2);
        assert_eq!(statuses(&single), ["fail", "pass", "fail", "pass"]);
        assert_eq!(
            single.results[2].detail.as_deref(),
            Some("3 NULL row(s) found")
        );
    }

    #[tokio::test]
    async fn one_test_of_a_type_runs_singly() {
        let adapter =
            adapter_with("CREATE TABLE main.t AS SELECT * FROM (VALUES (1, 2)) v(a, b)").await;
        let tests = [unique("a"), not_null("b")];
        let (single, batched) = both_modes(&adapter, &tests).await;
        assert_eq!(batched.queries, single.queries);
        for (s, b) in single.results.iter().zip(&batched.results) {
            assert_eq!(s.sql, b.sql, "a lone test keeps its single query");
        }
    }

    #[tokio::test]
    async fn a_missing_column_errors_alone_after_the_rerun() {
        let adapter =
            adapter_with("CREATE TABLE main.t AS SELECT * FROM (VALUES (1, 2)) v(a, b)").await;
        let tests = [
            unique("a"),
            unique("missing"),
            unique("b"),
            not_null("a"),
            not_null("missing"),
            not_null("b"),
        ];
        let (single, batched) = both_modes(&adapter, &tests).await;
        // Both batch queries fail, then all six tests rerun singly.
        assert_eq!(batched.queries, 2 + tests.len());
        assert_eq!(
            statuses(&single),
            ["pass", "error", "pass", "pass", "error", "pass"]
        );
    }

    #[tokio::test]
    async fn a_non_default_test_stays_single() {
        let adapter =
            adapter_with("CREATE TABLE main.t AS SELECT * FROM (VALUES (1, NULL), (1, 2)) v(a, b)")
                .await;
        let mut warned = unique("a");
        warned.severity = TestSeverity::Warning;
        let mut filtered = not_null("b");
        filtered.filter = Some("a > 0".to_string());
        let tests = [
            warned,
            unique("b"),
            filtered,
            not_null("a"),
            decl(
                TestType::Composite {
                    kind: CompositeKind::Unique,
                    columns: vec!["a".to_string(), "b".to_string()],
                },
                None,
            ),
        ];
        let (single, batched) = both_modes(&adapter, &tests).await;
        // One default-config test of each type: nothing batches.
        assert_eq!(batched.queries, single.queries);
        for (s, b) in single.results.iter().zip(&batched.results) {
            assert_eq!(s.sql, b.sql);
        }
    }
}
