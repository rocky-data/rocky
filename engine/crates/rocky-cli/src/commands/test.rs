//! `rocky test` — local model testing via DuckDB + declarative test execution.

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use tracing::{debug, info, warn};

use rocky_core::models;
use rocky_core::tests::{TestSeverity, TestType, generate_test_sql_with_dialect};
use rocky_core::traits::WarehouseAdapter;

use crate::output::{
    DeclarativeTestResult, DeclarativeTestSummary, ModelTestResult, TestFailure, TestOutput,
    UnitTestSummary, print_json,
};
use crate::registry::{self, AdapterRegistry};

/// Map the engine test runner's `ModelTestResult` to the JsonSchema-derived
/// output shape. Centralized so `test_output` + `run_test` agree.
fn to_output_results(
    results: &[rocky_engine::test_runner::ModelTestResult],
) -> Vec<ModelTestResult> {
    results
        .iter()
        .map(|r| ModelTestResult {
            model: r.model.clone(),
            status: match r.status {
                rocky_engine::test_runner::ModelTestStatus::Pass => "pass".to_string(),
                rocky_engine::test_runner::ModelTestStatus::Fail => "fail".to_string(),
            },
            error: r.error.clone(),
        })
        .collect()
}

/// Build the JSON-output unit-test summary from the engine runner, or `None`
/// when the project declares no `[[test]]` blocks.
fn unit_summary(run: &rocky_engine::test_runner::UnitTestRun) -> Option<UnitTestSummary> {
    if run.results.is_empty() {
        return None;
    }
    Some(UnitTestSummary {
        total: run.total(),
        passed: run.passed(),
        failed: run.total() - run.passed(),
        results: run.results.clone(),
    })
}

/// Side-effect-free core of `rocky test` (DuckDB-based local tests): run the
/// tests and assemble the typed [`TestOutput`] without printing.
///
/// The `run_test` wrapper calls this and prints; the in-process MCP server
/// (`rocky-mcp`) obtains the struct directly.
// Reusable typed-output core for the in-process MCP server. `run_test`
// re-runs the test runner so it can also render text.
pub fn test_output(
    models_dir: &Path,
    contracts_dir: Option<&Path>,
    model_filter: Option<&str>,
) -> Result<TestOutput> {
    let result = rocky_engine::test_runner::run_tests(
        models_dir,
        contracts_dir,
        model_filter,
        &rocky_core::run_vars::RunVars::new(),
    )?;
    let failures: Vec<TestFailure> = result
        .failures
        .iter()
        .map(|(name, error)| TestFailure {
            name: name.clone(),
            error: error.clone(),
        })
        .collect();
    let model_results = to_output_results(&result.model_results);
    let mut output =
        TestOutput::new(result.total, result.passed, failures).with_model_results(model_results);
    let unit_run = rocky_engine::test_runner::run_unit_tests(models_dir, model_filter)?;
    if let Some(summary) = unit_summary(&unit_run) {
        output = output.with_unit_tests(summary);
    }
    Ok(output)
}

/// Execute `rocky test` (DuckDB-based local tests).
pub fn run_test(
    models_dir: &Path,
    contracts_dir: Option<&Path>,
    model_filter: Option<&str>,
    output_json: bool,
    run_vars: &rocky_core::run_vars::RunVars,
) -> Result<()> {
    let result =
        rocky_engine::test_runner::run_tests(models_dir, contracts_dir, model_filter, run_vars)?;
    let unit_run = rocky_engine::test_runner::run_unit_tests(models_dir, model_filter)?;
    let unit_failed = unit_run.total() - unit_run.passed();

    if output_json {
        let failures: Vec<TestFailure> = result
            .failures
            .iter()
            .map(|(name, error)| TestFailure {
                name: name.clone(),
                error: error.clone(),
            })
            .collect();
        let model_results = to_output_results(&result.model_results);
        let mut output = TestOutput::new(result.total, result.passed, failures)
            .with_model_results(model_results);
        if let Some(summary) = unit_summary(&unit_run) {
            output = output.with_unit_tests(summary);
        }
        print_json(&output)?;
    } else {
        let scope = match model_filter {
            Some(m) => format!(" (model={m})"),
            None => String::new(),
        };
        println!("Testing {} models{scope}...", result.total);
        println!();

        for d in &result.diagnostics {
            if d.is_error() {
                println!("  \u{2717} {} — {}", d.model, d.message);
            }
        }

        if result.failures.is_empty()
            && !result
                .diagnostics
                .iter()
                .any(rocky_compiler::diagnostic::Diagnostic::is_error)
        {
            println!("  All {} models passed", result.passed);
        } else {
            for (name, err) in &result.failures {
                println!("  \u{2717} {name} — {err}");
            }
        }

        println!();
        println!(
            "  Result: {} passed, {} failed",
            result.passed,
            result.failures.len()
        );

        if unit_run.total() > 0 {
            println!();
            println!(
                "  Unit tests: {} passed, {unit_failed} failed",
                unit_run.passed()
            );
            for r in &unit_run.results {
                if !r.passed {
                    println!(
                        "  \u{2717} {}::{} — {}",
                        r.model,
                        r.test,
                        r.error.as_deref().unwrap_or("output mismatch")
                    );
                }
            }
        }
    }

    if !result.failures.is_empty() || unit_failed > 0 {
        anyhow::bail!("test failures detected");
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Declarative test runner (`rocky test --declarative`)
// ---------------------------------------------------------------------------

/// Load all models from a directory including one level of subdirectories.
fn load_all_models(models_dir: &Path) -> Result<Vec<models::Model>> {
    let mut all = models::load_models_from_dir(models_dir).context(format!(
        "failed to load models from {}",
        models_dir.display()
    ))?;

    if let Ok(entries) = std::fs::read_dir(models_dir) {
        for entry in entries.flatten() {
            if entry.path().is_dir()
                && let Ok(sub) = models::load_models_from_dir(&entry.path())
            {
                all.extend(sub);
            }
        }
    }
    all.sort_unstable_by(|a, b| a.config.name.cmp(&b.config.name));
    Ok(all)
}

/// Execute `rocky test --declarative`: run `[[tests]]` from model sidecars
/// against the configured warehouse adapter.
pub async fn run_declarative_tests(
    config_path: &Path,
    models_dir: &Path,
    pipeline_name: Option<&str>,
    model_filter: Option<&str>,
    output_json: bool,
) -> Result<()> {
    // 1. Load config + adapter registry.
    let rocky_cfg = rocky_core::config::load_rocky_config(config_path).context(format!(
        "failed to load config from {}",
        config_path.display()
    ))?;
    let (_, pipeline) = registry::resolve_pipeline(&rocky_cfg, pipeline_name)?;
    let adapter_registry = AdapterRegistry::from_config(&rocky_cfg)?;
    let warehouse_adapter = adapter_registry.warehouse_adapter(pipeline.target_adapter())?;

    // 2. Load all models.
    let all_models = load_all_models(models_dir)?;

    // 3. Filter to models that have [[tests]] declared.
    let models_with_tests: Vec<_> = all_models
        .iter()
        .filter(|m| {
            if m.config.tests.is_empty() {
                return false;
            }
            if let Some(filter) = model_filter {
                m.config.name == filter
            } else {
                true
            }
        })
        .collect();

    if models_with_tests.is_empty() {
        info!("no declarative tests found in models directory");
        if output_json {
            let output = TestOutput {
                declarative: Some(DeclarativeTestSummary {
                    total: 0,
                    passed: 0,
                    failed: 0,
                    warned: 0,
                    errored: 0,
                    results: vec![],
                }),
                ..TestOutput::new(0, 0, vec![])
            };
            print_json(&output)?;
        } else {
            println!("No declarative tests found.");
        }
        return Ok(());
    }

    // 4. Execute each test.
    let mut results = Vec::new();
    for model in &models_with_tests {
        let target = &model.config.target;
        let fq_table = format!("{}.{}.{}", target.catalog, target.schema, target.table);
        debug!(model = %model.config.name, tests = model.config.tests.len(), "running declarative tests");

        for test_decl in &model.config.tests {
            let result =
                execute_one_test(test_decl, &model.config.name, &fq_table, &warehouse_adapter)
                    .await;
            results.push(result);
        }
    }

    // 5. Tally results.
    let total = results.len();
    let passed = results.iter().filter(|r| r.status == "pass").count();
    let failed = results
        .iter()
        .filter(|r| r.status == "fail" && r.severity == "error")
        .count();
    let warned = results
        .iter()
        .filter(|r| r.status == "fail" && r.severity == "warning")
        .count();
    let errored = results.iter().filter(|r| r.status == "error").count();

    // 6. Report.
    if output_json {
        let output = TestOutput {
            declarative: Some(DeclarativeTestSummary {
                total,
                passed,
                failed,
                warned,
                errored,
                results,
            }),
            ..TestOutput::new(0, 0, vec![])
        };
        print_json(&output)?;
    } else {
        println!("Declarative tests: {} total", total);
        println!();

        for r in &results {
            let icon = match r.status.as_str() {
                "pass" => "\u{2713}",
                "fail" if r.severity == "warning" => "\u{26A0}",
                _ => "\u{2717}",
            };
            let col = r
                .column
                .as_deref()
                .map(|c| format!(".{c}"))
                .unwrap_or_default();
            let detail = r
                .detail
                .as_deref()
                .map(|d| format!(" — {d}"))
                .unwrap_or_default();
            println!(
                "  {icon} {model}{col} [{test_type}]{detail}",
                model = r.model,
                test_type = r.test_type,
            );
        }

        println!();
        println!("  Result: {passed} passed, {failed} failed, {warned} warned, {errored} errored");
    }

    if failed > 0 || errored > 0 {
        anyhow::bail!(
            "declarative test failures: {failed} hard failure(s), {errored} execution error(s)"
        );
    }

    Ok(())
}

/// Execute a single declarative test and return a result.
async fn execute_one_test(
    test_decl: &rocky_core::tests::TestDecl,
    model_name: &str,
    fq_table: &str,
    adapter: &Arc<dyn WarehouseAdapter>,
) -> DeclarativeTestResult {
    let test_type_name = test_type_label(&test_decl.test_type);
    let severity_str = match test_decl.severity {
        TestSeverity::Error => "error",
        TestSeverity::Warning => "warning",
    };

    // Generate SQL.
    let sql = match generate_test_sql_with_dialect(test_decl, fq_table, adapter.dialect()) {
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
/// Returns `(status, detail)` where status is "pass" or "fail".
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
            let count = first_row_count(result);
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
            let count = first_row_count(result);
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
fn first_row_count(result: &rocky_core::traits::QueryResult) -> u64 {
    result
        .rows
        .first()
        .and_then(|row| row.first())
        .and_then(|v| match v {
            serde_json::Value::Number(n) => n.as_u64(),
            serde_json::Value::String(s) => s.parse::<u64>().ok(),
            _ => None,
        })
        .unwrap_or(0)
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
