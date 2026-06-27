//! `rocky test` — local model testing.
//!
//! Compiles models, executes them locally via DuckDB, validates
//! output against contracts, and reports pass/fail.

use std::collections::HashMap;
use std::path::Path;

use rocky_compiler::compile::CompilerConfig;
use rocky_core::models::load_unit_tests_from_dir;
use rocky_core::unit_test::{
    MismatchKind, RowMismatch, UnitTestDef, UnitTestResult, fixture_to_sql, json_to_sql_literal,
};
use rocky_duckdb::DuckDbConnector;
use rocky_sql::validation::validate_identifier;
use tracing::info;

/// Per-model outcome of a local test run.
///
/// Surfaces passes alongside failures so consumers (the VS Code Inspector
/// Tests tab, the dagster integration) can render "good_mart: pass" instead
/// of inferring it from `total - failures`. `error` is populated only for
/// `status = "fail"`.
#[derive(Debug, Clone)]
pub struct ModelTestResult {
    pub model: String,
    pub status: ModelTestStatus,
    pub error: Option<String>,
}

/// Outcome of executing one model locally.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelTestStatus {
    Pass,
    Fail,
}

/// Result of a test run.
#[derive(Debug)]
pub struct TestResult {
    /// Total models tested (post-filter).
    pub total: usize,
    /// Models that passed all checks (post-filter).
    pub passed: usize,
    /// Models that failed (name, reason) (post-filter).
    pub failures: Vec<(String, String)>,
    /// Per-model outcomes (post-filter). Includes passes — the only way to
    /// surface "good_mart: pass" without inferring from total - failures.
    pub model_results: Vec<ModelTestResult>,
    /// Compilation diagnostics.
    pub diagnostics: Vec<rocky_compiler::diagnostic::Diagnostic>,
}

/// Run tests on a project.
///
/// 1. Compile models
/// 2. Execute each model locally via DuckDB (dependencies always run, even
///    when `model_filter` is set, so the filtered model's SQL can resolve)
/// 3. Validate output against contracts (if present)
/// 4. Report results (filtered to `model_filter` when set)
///
/// `run_vars` supplies per-run `@var(name)` substitutions so a required-var
/// model compiles under `rocky test --var name=value`; pass
/// [`rocky_core::run_vars::RunVars::new`] when the caller has none.
pub fn run_tests(
    models_dir: &Path,
    contracts_dir: Option<&Path>,
    model_filter: Option<&str>,
    run_vars: &rocky_core::run_vars::RunVars,
) -> anyhow::Result<TestResult> {
    let config = CompilerConfig {
        models_dir: models_dir.to_path_buf(),
        contracts_dir: contracts_dir.map(std::path::Path::to_path_buf),
        source_schemas: HashMap::new(),
        source_column_info: HashMap::new(),
        run_vars: run_vars.clone(),
        ..Default::default()
    };

    let compile_result = rocky_compiler::compile::compile(&config)?;

    let mut result = TestResult {
        total: 0,
        passed: 0,
        failures: Vec::new(),
        model_results: Vec::new(),
        diagnostics: compile_result.diagnostics.clone(),
    };

    // Check for compilation errors
    if compile_result.has_errors {
        for d in &compile_result.diagnostics {
            if d.is_error() && include_model(model_filter, &d.model) {
                result
                    .failures
                    .push((d.model.clone(), d.message.to_string()));
                result.model_results.push(ModelTestResult {
                    model: d.model.clone(),
                    status: ModelTestStatus::Fail,
                    error: Some(d.message.to_string()),
                });
            }
        }
        result.total = result.model_results.len();
        return Ok(result);
    }

    // Execute locally
    let db = DuckDbConnector::in_memory()?;

    // Auto-load seed data if a `data/seed.sql` file exists alongside `models/`.
    // Lets `rocky test` work on projects (like the playground) that ship inline
    // seed SQL without requiring users to load it manually.
    if let Some(project_root) = models_dir.parent() {
        let seed_path = project_root.join("data").join("seed.sql");
        if seed_path.exists() {
            let seed_sql = std::fs::read_to_string(&seed_path)?;
            if let Err(e) = db.execute_statement(&seed_sql) {
                result.failures.push((
                    "seed".to_string(),
                    format!("failed to load data/seed.sql: {e}"),
                ));
                result.model_results.push(ModelTestResult {
                    model: "seed".to_string(),
                    status: ModelTestStatus::Fail,
                    error: Some(format!("failed to load data/seed.sql: {e}")),
                });
                result.total = result.model_results.len();
                return Ok(result);
            }
            info!(path = %seed_path.display(), "loaded seed data");
        }
    }

    // Always execute every model so a filtered model's upstream dependencies
    // resolve. The filter is applied below when assembling the reported
    // results, so passes/failures match the requested scope.
    let exec_result = crate::executor::execute_locally(&compile_result, &db);

    for name in &exec_result.succeeded {
        if include_model(model_filter, name) {
            result.model_results.push(ModelTestResult {
                model: name.clone(),
                status: ModelTestStatus::Pass,
                error: None,
            });
        }
    }
    for (name, err) in &exec_result.failed {
        if include_model(model_filter, name) {
            result.failures.push((name.clone(), err.clone()));
            result.model_results.push(ModelTestResult {
                model: name.clone(),
                status: ModelTestStatus::Fail,
                error: Some(err.clone()),
            });
        }
    }
    result.total = result.model_results.len();
    result.passed = result
        .model_results
        .iter()
        .filter(|m| m.status == ModelTestStatus::Pass)
        .count();

    info!(
        total = result.total,
        passed = result.passed,
        failed = result.failures.len(),
        filter = model_filter.unwrap_or("<none>"),
        "test run complete"
    );

    Ok(result)
}

/// Filter helper: include a model when there's no filter, or when the filter
/// matches the model name exactly. Centralized so callers can't accidentally
/// substring-match.
fn include_model(filter: Option<&str>, model: &str) -> bool {
    match filter {
        None => true,
        Some(target) => target == model,
    }
}

/// Outcome of running all fixture-driven unit tests (`[[test]]` blocks) in a
/// project. Each [`UnitTestResult`] records one test's pass/fail against its
/// mock-input → expected-output expectation.
#[derive(Debug)]
pub struct UnitTestRun {
    pub results: Vec<UnitTestResult>,
}

impl UnitTestRun {
    /// Total unit tests executed.
    pub fn total(&self) -> usize {
        self.results.len()
    }

    /// Unit tests that passed.
    pub fn passed(&self) -> usize {
        self.results.iter().filter(|r| r.passed).count()
    }
}

/// Run every fixture-driven unit test (`[[test]]` blocks) declared in the
/// project's model sidecars.
///
/// For each test a fresh in-memory DuckDB is seeded with the `given` fixtures,
/// the model's compiled SQL is materialized against them, and the output is
/// compared to `expect` — a multiset comparison by default, positional when
/// `expect.ordered` is set. Models are filtered by `model_filter` when set;
/// models declaring no `[[test]]` blocks are skipped entirely (no compile).
pub fn run_unit_tests(
    models_dir: &Path,
    model_filter: Option<&str>,
) -> anyhow::Result<UnitTestRun> {
    let unit_tests = load_unit_tests_from_dir(models_dir)?;
    if unit_tests.is_empty() {
        return Ok(UnitTestRun {
            results: Vec::new(),
        });
    }

    let config = CompilerConfig {
        models_dir: models_dir.to_path_buf(),
        ..Default::default()
    };
    let compile_result = rocky_compiler::compile::compile(&config)?;

    // Stable, name-sorted iteration so output order is deterministic.
    let mut names: Vec<&String> = unit_tests.keys().collect();
    names.sort();

    let mut results = Vec::new();
    for name in names {
        if !include_model(model_filter, name) {
            continue;
        }
        let compiled_sql = compile_result.project.model(name).map(|m| m.sql.clone());
        for test in &unit_tests[name] {
            match &compiled_sql {
                Some(sql) => results.push(run_one_unit_test(name, sql, test)),
                None => results.push(UnitTestResult {
                    model: name.clone(),
                    test: test.name.clone(),
                    passed: false,
                    error: Some(format!(
                        "model '{name}' did not compile — cannot run its unit tests"
                    )),
                    mismatches: Vec::new(),
                }),
            }
        }
    }

    info!(
        total = results.len(),
        passed = results.iter().filter(|r| r.passed).count(),
        "unit test run complete"
    );
    Ok(UnitTestRun { results })
}

/// Execute one unit test: seed `given` fixtures, materialize the model against
/// them, and compare the output to `expect`.
fn run_one_unit_test(model: &str, compiled_sql: &str, test: &UnitTestDef) -> UnitTestResult {
    let fail = |msg: String, mismatches: Vec<RowMismatch>| UnitTestResult {
        model: model.to_string(),
        test: test.name.clone(),
        passed: false,
        error: Some(msg),
        mismatches,
    };
    let pass = || UnitTestResult {
        model: model.to_string(),
        test: test.name.clone(),
        passed: true,
        error: None,
        mismatches: Vec::new(),
    };

    let db = match DuckDbConnector::in_memory() {
        Ok(d) => d,
        Err(e) => return fail(format!("DuckDB init failed: {e}"), Vec::new()),
    };

    // Seed the mock input fixtures.
    for fx in &test.given {
        if validate_identifier(&fx.model_ref).is_err() {
            return fail(
                format!("invalid fixture ref '{}'", fx.model_ref),
                Vec::new(),
            );
        }
        if let Some(sql) = fixture_to_sql(fx)
            && let Err(e) = db.execute_statement(&sql)
        {
            return fail(
                format!("failed to seed fixture '{}': {e}", fx.model_ref),
                Vec::new(),
            );
        }
    }

    // Materialize the model output against the mocked inputs.
    let sql = compiled_sql.trim().trim_end_matches(';');
    if let Err(e) =
        db.execute_statement(&format!("CREATE OR REPLACE TABLE __rocky_actual AS\n{sql}"))
    {
        return fail(format!("model execution failed: {e}"), Vec::new());
    }

    // Empty expectation ⇒ the model must produce no rows.
    if test.expect.rows.is_empty() {
        return match query_scalar_u64(&db, "SELECT COUNT(*) AS n FROM __rocky_actual") {
            Ok(0) => pass(),
            Ok(n) => fail(format!("expected 0 rows, got {n}"), Vec::new()),
            Err(e) => fail(format!("row count query failed: {e}"), Vec::new()),
        };
    }

    // Comparison columns come from the expected rows — only the columns the
    // author asserts on are compared (extra model columns are ignored).
    let cols: Vec<String> = match test.expect.rows[0].as_object() {
        Some(obj) => obj.keys().cloned().collect(),
        None => {
            return fail(
                "expect rows must be tables (key = value)".into(),
                Vec::new(),
            );
        }
    };
    for c in &cols {
        if validate_identifier(c).is_err() {
            return fail(format!("invalid column name in expect: '{c}'"), Vec::new());
        }
    }
    if let Err(e) = db.execute_statement(&expected_table_sql(&test.expect.rows, &cols)) {
        return fail(format!("failed to build expected table: {e}"), Vec::new());
    }
    let col_list = cols.join(", ");

    if test.expect.ordered {
        // Positional comparison: actual in the model's output order (re-run as a
        // subquery so its ORDER BY survives), expected by declaration order.
        let actual =
            match db.execute_sql(&format!("SELECT {col_list} FROM (\n{sql}\n) AS __rocky_m")) {
                Ok(r) => r.rows,
                Err(e) => return fail(format!("actual query failed: {e}"), Vec::new()),
            };
        let expected = match db.execute_sql(&format!(
            "SELECT {col_list} FROM __rocky_expected ORDER BY __rocky_ord"
        )) {
            Ok(r) => r.rows,
            Err(e) => return fail(format!("expected query failed: {e}"), Vec::new()),
        };
        if actual == expected {
            return pass();
        }
        let mismatches = ordered_mismatches(&cols, &expected, &actual);
        return fail(
            format!(
                "ordered output mismatch ({} expected vs {} actual row(s))",
                expected.len(),
                actual.len()
            ),
            mismatches,
        );
    }

    // Default: multiset comparison via EXCEPT ALL both directions.
    let missing = match query_scalar_u64(
        &db,
        &format!(
            "SELECT COUNT(*) AS n FROM (SELECT {col_list} FROM __rocky_expected \
             EXCEPT ALL SELECT {col_list} FROM __rocky_actual)"
        ),
    ) {
        Ok(n) => n,
        Err(e) => return fail(format!("diff query failed: {e}"), Vec::new()),
    };
    let extra = match query_scalar_u64(
        &db,
        &format!(
            "SELECT COUNT(*) AS n FROM (SELECT {col_list} FROM __rocky_actual \
             EXCEPT ALL SELECT {col_list} FROM __rocky_expected)"
        ),
    ) {
        Ok(n) => n,
        Err(e) => return fail(format!("diff query failed: {e}"), Vec::new()),
    };

    if missing == 0 && extra == 0 {
        return pass();
    }
    let mismatches = unordered_mismatches(&db, &cols, &col_list);
    fail(
        format!("output mismatch: {missing} expected row(s) missing, {extra} unexpected row(s)"),
        mismatches,
    )
}

/// Build a `CREATE TABLE __rocky_expected` statement from the expected rows,
/// carrying a `__rocky_ord` ordinal so ordered comparison can recover the
/// declaration order. Values are rendered identically to the `given` fixtures.
fn expected_table_sql(rows: &[serde_json::Value], cols: &[String]) -> String {
    let selects: Vec<String> = rows
        .iter()
        .enumerate()
        .map(|(i, row)| {
            let obj = row.as_object();
            let vals: Vec<String> = cols
                .iter()
                .map(|c| {
                    let lit = obj
                        .and_then(|o| o.get(c))
                        .map_or_else(|| "NULL".to_string(), json_to_sql_literal);
                    format!("{lit} AS {c}")
                })
                .collect();
            format!("SELECT {i} AS __rocky_ord, {}", vals.join(", "))
        })
        .collect();
    format!(
        "CREATE OR REPLACE TABLE __rocky_expected AS\n{}",
        selects.join("\nUNION ALL\n")
    )
}

/// Run a single-cell `COUNT(*)`-style query and parse the result as `u64`.
fn query_scalar_u64(db: &DuckDbConnector, sql: &str) -> Result<u64, String> {
    let r = db.execute_sql(sql).map_err(|e| e.to_string())?;
    let cell = r
        .rows
        .first()
        .and_then(|row| row.first())
        .ok_or_else(|| "query returned no rows".to_string())?;
    // `execute_sql` returns every cell stringified, so a `COUNT(*)` comes back
    // as e.g. `"0"`; accept both the string and native-number forms.
    let text = match cell {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    };
    text.trim()
        .parse::<u64>()
        .map_err(|e| format!("expected an integer count, got '{text}': {e}"))
}

/// Render a single result cell for diagnostics (strings unquoted, null as NULL).
fn value_to_display(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Null => "NULL".to_string(),
        other => other.to_string(),
    }
}

/// Render a result row as `col=val, col=val` for diagnostics.
fn render_row(cols: &[String], row: &[serde_json::Value]) -> String {
    cols.iter()
        .zip(row.iter())
        .map(|(c, v)| format!("{c}={}", value_to_display(v)))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Sample up to 10 missing + 10 unexpected rows for an unordered mismatch.
fn unordered_mismatches(db: &DuckDbConnector, cols: &[String], col_list: &str) -> Vec<RowMismatch> {
    let mut out = Vec::new();
    if let Ok(r) = db.execute_sql(&format!(
        "SELECT * FROM (SELECT {col_list} FROM __rocky_expected \
         EXCEPT ALL SELECT {col_list} FROM __rocky_actual) AS __d LIMIT 10"
    )) {
        for row in &r.rows {
            let row_index = out.len();
            out.push(RowMismatch {
                row_index,
                expected: render_row(cols, row),
                actual: None,
                kind: MismatchKind::Missing,
            });
        }
    }
    if let Ok(r) = db.execute_sql(&format!(
        "SELECT * FROM (SELECT {col_list} FROM __rocky_actual \
         EXCEPT ALL SELECT {col_list} FROM __rocky_expected) AS __d LIMIT 10"
    )) {
        for row in &r.rows {
            let row_index = out.len();
            out.push(RowMismatch {
                row_index,
                expected: String::new(),
                actual: Some(render_row(cols, row)),
                kind: MismatchKind::Extra,
            });
        }
    }
    out
}

/// Build positional mismatches for an ordered comparison (up to 10).
fn ordered_mismatches(
    cols: &[String],
    expected: &[Vec<serde_json::Value>],
    actual: &[Vec<serde_json::Value>],
) -> Vec<RowMismatch> {
    let mut out = Vec::new();
    for i in 0..expected.len().max(actual.len()) {
        match (expected.get(i), actual.get(i)) {
            (Some(e), Some(a)) if e == a => {}
            (Some(e), Some(a)) => out.push(RowMismatch {
                row_index: i,
                expected: render_row(cols, e),
                actual: Some(render_row(cols, a)),
                kind: MismatchKind::ValueDiff,
            }),
            (Some(e), None) => out.push(RowMismatch {
                row_index: i,
                expected: render_row(cols, e),
                actual: None,
                kind: MismatchKind::Missing,
            }),
            (None, Some(a)) => out.push(RowMismatch {
                row_index: i,
                expected: String::new(),
                actual: Some(render_row(cols, a)),
                kind: MismatchKind::Extra,
            }),
            (None, None) => {}
        }
        if out.len() >= 10 {
            break;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Set up a temp project with two SQL models (`raw_orders`, `good_mart`)
    /// where `good_mart` reads from `raw_orders`. Returns the temp dir guard
    /// so the caller can drop it.
    fn scaffold_two_model_project() -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let models = dir.path().join("models");
        std::fs::create_dir_all(&models).unwrap();
        std::fs::write(
            models.join("raw_orders.sql"),
            "SELECT 1 AS id, 'a' AS status",
        )
        .unwrap();
        std::fs::write(
            models.join("raw_orders.toml"),
            "[strategy]\ntype = \"full_refresh\"\n[target]\ncatalog=\"wh\"\nschema=\"main\"\n",
        )
        .unwrap();
        std::fs::write(
            models.join("good_mart.sql"),
            "SELECT id, status FROM raw_orders",
        )
        .unwrap();
        std::fs::write(
            models.join("good_mart.toml"),
            "[strategy]\ntype = \"full_refresh\"\n[target]\ncatalog=\"wh\"\nschema=\"main\"\n",
        )
        .unwrap();
        (dir, models)
    }

    /// Unfiltered run reports every model — passes too, not just failures —
    /// so the Inspector Tests tab can render "raw_orders: pass" without
    /// inferring it from `total - failures`.
    #[test]
    fn full_run_itemizes_passes() {
        let (_tmp, models) = scaffold_two_model_project();
        let result = run_tests(&models, None, None, &rocky_core::run_vars::RunVars::new()).unwrap();
        assert_eq!(result.total, 2);
        assert_eq!(result.passed, 2);
        assert!(result.failures.is_empty());
        let names: Vec<_> = result
            .model_results
            .iter()
            .map(|m| (m.model.as_str(), m.status))
            .collect();
        assert!(names.contains(&("raw_orders", ModelTestStatus::Pass)));
        assert!(names.contains(&("good_mart", ModelTestStatus::Pass)));
    }

    /// `--model good_mart` filters the reported results to one model. The
    /// upstream `raw_orders` still executes (so good_mart's SQL resolves)
    /// but doesn't appear in `model_results`. Closes the TODO that had
    /// `rocky test --model X` silently testing every model.
    #[test]
    fn model_filter_scopes_results_to_one_model() {
        let (_tmp, models) = scaffold_two_model_project();
        let result = run_tests(
            &models,
            None,
            Some("good_mart"),
            &rocky_core::run_vars::RunVars::new(),
        )
        .unwrap();
        assert_eq!(result.total, 1);
        assert_eq!(result.passed, 1);
        assert_eq!(result.model_results.len(), 1);
        assert_eq!(result.model_results[0].model, "good_mart");
        assert_eq!(result.model_results[0].status, ModelTestStatus::Pass);
    }

    /// Scaffold a project where `flagged` filters an upstream `orders` model
    /// and a `[[test]]` mocks `orders` with fixture rows. `expect_match`
    /// toggles whether the asserted output matches the model's real output.
    fn scaffold_unit_test_project(expect_match: bool) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let models = dir.path().join("models");
        std::fs::create_dir_all(&models).unwrap();
        // Upstream model the unit test mocks.
        std::fs::write(models.join("orders.sql"), "SELECT 0 AS id, 0 AS amount").unwrap();
        std::fs::write(
            models.join("orders.toml"),
            "[strategy]\ntype = \"full_refresh\"\n[target]\ncatalog=\"wh\"\nschema=\"main\"\n",
        )
        .unwrap();
        // Model under test: keep only high-value orders.
        std::fs::write(
            models.join("flagged.sql"),
            "SELECT id, amount, true AS is_high FROM orders WHERE amount > 100",
        )
        .unwrap();
        let expected_amount = if expect_match { 150 } else { 999 };
        std::fs::write(
            models.join("flagged.toml"),
            format!(
                "[strategy]\ntype = \"full_refresh\"\n\
                 [target]\ncatalog = \"wh\"\nschema = \"main\"\n\n\
                 [[test]]\nname = \"flags_high_value\"\n\n\
                 [[test.given]]\nref = \"orders\"\n\
                 rows = [ {{ id = 1, amount = 150 }}, {{ id = 2, amount = 50 }} ]\n\n\
                 [test.expect]\n\
                 rows = [ {{ id = 1, amount = {expected_amount}, is_high = true }} ]\n"
            ),
        )
        .unwrap();
        (dir, models)
    }

    /// A unit test passes when the model's output against the mocked inputs
    /// matches the expectation (the high-value filter keeps only `id = 1`).
    #[test]
    fn unit_test_passes_when_output_matches() {
        let (_tmp, models) = scaffold_unit_test_project(true);
        let run = run_unit_tests(&models, None).unwrap();
        assert_eq!(run.total(), 1);
        assert_eq!(run.passed(), 1, "error: {:?}", run.results[0].error);
        assert!(run.results[0].passed);
    }

    /// A unit test fails and reports row-level diagnostics when the model's
    /// output diverges from the expectation.
    #[test]
    fn unit_test_fails_and_reports_mismatch() {
        let (_tmp, models) = scaffold_unit_test_project(false);
        let run = run_unit_tests(&models, None).unwrap();
        assert_eq!(run.total(), 1);
        assert_eq!(run.passed(), 0);
        assert!(!run.results[0].passed);
        assert!(
            !run.results[0].mismatches.is_empty(),
            "expected mismatch diagnostics"
        );
    }

    /// A project whose models declare no `[[test]]` blocks runs zero unit
    /// tests (and skips compilation entirely).
    #[test]
    fn unit_tests_empty_when_none_declared() {
        let (_tmp, models) = scaffold_two_model_project();
        let run = run_unit_tests(&models, None).unwrap();
        assert_eq!(run.total(), 0);
    }
}
