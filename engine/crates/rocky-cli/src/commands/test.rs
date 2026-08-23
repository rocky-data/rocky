//! `rocky test` — local model testing via DuckDB + declarative test execution.

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use tracing::{debug, info, warn};

use rocky_core::tests::{TestSeverity, TestType, generate_test_sql_with_dialect};
use rocky_core::traits::WarehouseAdapter;

use crate::output::{
    DeclarativeTestResult, DeclarativeTestSummary, ModelTestResult, TestFailure, TestOutput,
    UnitTestSummary, print_json,
};
use crate::registry::{self, AdapterRegistry};

use super::ModelNotFound;

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
/// Refuse a `--model` selector that names no model in the project.
///
/// Every count on [`rocky_engine::test_runner::TestResult`] is post-filter, so
/// without this an unknown name is reported as `total: 0` — byte-identical to
/// a real model that declares no tests, and exit 0 either way (#1428). Mirrors
/// the check `rocky compile --model` performs (`commands::compile`), and
/// returns the same typed [`ModelNotFound`] so the message matches `rocky run
/// --model` and the MCP layer can classify it as `model_not_found`.
fn reject_unknown_model(model_filter: Option<&str>, all_models: &[String]) -> Result<()> {
    if let Some(filter) = model_filter
        && !all_models.iter().any(|name| name == filter)
    {
        return Err(anyhow::Error::new(ModelNotFound(filter.to_string())));
    }
    Ok(())
}

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
    reject_unknown_model(model_filter, &result.all_models)?;
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
    // `run_test` deliberately re-runs the engine rather than calling
    // `test_output` (see that function's note), so the check has to be made
    // here too — this is the path the CLI actually takes.
    reject_unknown_model(model_filter, &result.all_models)?;
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

/// Load every `.sql` and `.rocky` model beneath the models directory.
fn load_all_models(models_dir: &Path) -> Result<Vec<rocky_core::models::Model>> {
    let mut all = crate::models_loader::load_project_models(models_dir)?;
    all.sort_unstable_by(|a, b| a.config.name.cmp(&b.config.name));
    Ok(all)
}

/// How many declarative tests [`run_declarative_tests`] would execute for
/// `model` — the number a caller may report as deferred when the target is
/// not materialised yet.
///
/// Counts the model loader's EXPANDED `ModelConfig.tests` vector, which is
/// the exact vector the runner iterates. That matters: the loader appends
/// every `[[use_test]]` reference to `tests` after resolving it against
/// `test_definitions.toml`, and the runner cannot tell an expanded
/// reference from an inline test. Counting the sidecar's raw `[[tests]]`
/// array instead would undercount by exactly the `[[use_test]]`
/// references — and the sidecar merge preserves those, because the product
/// lowering owns only `tests`.
///
/// Goes through [`load_all_models`], the same loader the runner uses, so
/// the counted set is the executed set by construction rather than by a
/// re-derivation that has to be kept in step by hand.
///
/// An unknown model is an error, never a zero: a caller must be able to
/// tell "no checks" from "no answer".
pub fn declarative_test_count(models_dir: &Path, model: &str) -> Result<usize> {
    let all_models = load_all_models(models_dir)?;
    let found = all_models
        .iter()
        .find(|loaded| loaded.config.name == model)
        .ok_or_else(|| anyhow::Error::new(ModelNotFound(model.to_string())))?;
    Ok(found.config.tests.len())
}

/// A digest over the EXPANDED check set a model would execute.
///
/// Hashes what the loader actually produces — `ModelConfig.tests` after
/// every `[[use_test]]` reference has been resolved — rather than the
/// files it produced them from. That distinction is the whole point: a
/// sidecar's `[[use_test]]` entry carries only a name and a binding, and
/// the check's TYPE and SQL come from `models/test_definitions.toml`,
/// which is not a lowering artifact and is not hashed anywhere. Hashing
/// the source files therefore misses an edit to a shared definition,
/// while hashing the expansion cannot — and cannot be bypassed by a
/// future layer of indirection either, because any expansion has to land
/// in this same vector before it can run.
///
/// Deterministic: the vector is file order (inline tests, then resolved
/// references in reference order), `TestDecl` serializes its fields in
/// declaration order, and no field is a map or set. Pinned by
/// `the_check_digest_is_stable_across_loads`.
///
/// An unknown model is an error, never a digest over an empty set — the
/// same rule [`declarative_test_count`] follows, for the same reason.
#[cfg(feature = "duckdb")]
pub fn declarative_check_digest(models_dir: &Path, model: &str) -> Result<String> {
    let all_models = load_all_models(models_dir)?;
    let found = all_models
        .iter()
        .find(|loaded| loaded.config.name == model)
        .ok_or_else(|| anyhow::Error::new(ModelNotFound(model.to_string())))?;
    let bytes = serde_json::to_vec(&found.config.tests)
        .context("failed to serialize the expanded check set")?;
    Ok(rocky_core::product::manifest::content_digest(&bytes))
}

/// Model names, for [`reject_unknown_model`] — which takes `&[String]` so it
/// can serve both the declarative path (loaded `Model`s) and the compiled
/// path (`TestResult::all_models`).
fn model_names(models: &[rocky_core::models::Model]) -> Vec<String> {
    models.iter().map(|m| m.config.name.clone()).collect()
}

/// One declarative run, typed: what the loader said should run, and what
/// actually ran.
///
/// The two counts are the honesty carrier. They are derived from ONE
/// [`load_all_models`] call — so there is no second reading to drift —
/// but on two independent predicates: `declared` counts the expanded
/// `[[tests]]` of every model the selection names, while `results` is
/// produced by the execution loop. A caller can therefore tell "every
/// declared check ran and passed" from "nothing ran", which are the same
/// tally otherwise. Today the two counts always agree; the point is that
/// a future short-circuit in the execution loop shows up as a shortfall
/// instead of silently reading as health.
pub(crate) struct DeclarativeRun {
    /// Expanded `[[tests]]` the loader produced for the selected models.
    pub(crate) declared: usize,
    /// One entry per check the runner actually executed.
    pub(crate) results: Vec<DeclarativeTestResult>,
}

impl DeclarativeRun {
    /// Checks the loader declared that produced no result at all.
    pub(crate) fn unevaluated(&self) -> usize {
        self.declared.saturating_sub(self.results.len())
    }

    /// Checks that failed at `severity = "error"` — the applied output
    /// contradicting something the product declared about itself.
    pub(crate) fn failed(&self) -> usize {
        self.results
            .iter()
            .filter(|r| r.status == "fail" && r.severity == "error")
            .count()
    }

    /// Checks that failed at `severity = "warning"`: reported, never
    /// treated as a defect.
    pub(crate) fn warned(&self) -> usize {
        self.results
            .iter()
            .filter(|r| r.status == "fail" && r.severity == "warning")
            .count()
    }

    /// Checks whose execution errored — the runner could not tell
    /// whether the data is right.
    pub(crate) fn errored(&self) -> usize {
        self.results.iter().filter(|r| r.status == "error").count()
    }

    /// Checks that passed.
    pub(crate) fn passed(&self) -> usize {
        self.results.iter().filter(|r| r.status == "pass").count()
    }
}

/// Load, select, and execute the declarative checks — the typed core of
/// `rocky test --declarative`.
///
/// [`run_declarative_tests`] is this function plus reporting and the exit
/// rule, and the fulfillment loop's observation façade is this function
/// plus a verdict. Sharing it is deliberate: a second check engine that
/// drifted from the one `rocky test` runs would let the loop bless data
/// the CLI calls broken.
pub(crate) async fn declarative_run(
    config_path: &Path,
    models_dir: &Path,
    pipeline_name: Option<&str>,
    model_filter: Option<&str>,
) -> Result<DeclarativeRun> {
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

    // The loader is deliberately tolerant of a missing or empty directory
    // (`models_loader::load_project_models` returns an empty Vec, pinned by
    // its own `a_missing_directory_yields_no_models` test), so this path used
    // to report `total: 0` and exit 0 for a models dir that does not exist —
    // while `rocky test` WITHOUT `--declarative` compiles the project and
    // fails with `no models found in <dir>`. One command, two answers to the
    // same question. Refuse here so both paths agree (#1428).
    if all_models.is_empty() {
        anyhow::bail!("no models found in {}", models_dir.display());
    }
    // Checked against `all_models`, NOT the filtered set below: the filter and
    // the has-tests predicate are fused in one closure, and `tests.is_empty()`
    // short-circuits first, so a model that exists but declares no tests is
    // indistinguishable there from a name that does not exist at all. The
    // former must stay exit 0; only the latter is an error.
    reject_unknown_model(model_filter, &model_names(&all_models))?;

    // The declared count, on the NAME predicate alone — deliberately not
    // the has-tests closure below. A model the selection names but whose
    // checks never reach the execution loop is a shortfall a caller can
    // see, not a silent zero.
    let selected = |m: &&rocky_core::models::Model| match model_filter {
        Some(filter) => m.config.name == filter,
        None => true,
    };
    let declared: usize = all_models
        .iter()
        .filter(selected)
        .map(|m| m.config.tests.len())
        .sum();

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

    Ok(DeclarativeRun { declared, results })
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
    let run = declarative_run(config_path, models_dir, pipeline_name, model_filter).await?;
    let results = run.results;

    if results.is_empty() {
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

#[cfg(test)]
mod tests {
    use super::{declarative_check_digest, declarative_test_count, load_all_models};

    /// Write a one-model project whose sidecar carries `sidecar_extra`,
    /// plus a named-test registry. Returns the models dir.
    fn project(sidecar_extra: &str) -> (tempfile::TempDir, std::path::PathBuf) {
        let tmp = tempfile::tempdir().expect("tempdir");
        let models = tmp.path().join("models");
        std::fs::create_dir(&models).expect("create models dir");
        std::fs::write(
            models.join("test_definitions.toml"),
            "[positive]\ntype = \"expression\"\nexpression = \"amount > 0\"\n",
        )
        .expect("write test definitions");
        std::fs::write(models.join("orders.sql"), "SELECT 1 AS id").expect("write model sql");
        std::fs::write(
            models.join("orders.toml"),
            format!(
                "name = \"orders\"\n\n[strategy]\ntype = \"full_refresh\"\n\n\
                 [target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"orders\"\n\n\
                 [[tests]]\ntype = \"not_null\"\ncolumn = \"id\"\n{sidecar_extra}"
            ),
        )
        .expect("write model sidecar");
        (tmp, models)
    }

    /// The digest must be reproducible across loads, or the custody gate
    /// it backs reports divergence on every run and the loop never
    /// observes anything.
    ///
    /// Also the load-bearing half: the digest must MOVE when the shared
    /// `test_definitions.toml` changes, even though the model sidecar is
    /// byte-identical. That is the whole bypass — a `[[use_test]]` entry
    /// names a check whose SQL lives in that file, which no manifest
    /// hashes — and a digest that did not move here would be a gate that
    /// reports clean while the executed SQL changed.
    #[test]
    fn the_check_digest_is_stable_across_loads_and_moves_with_the_definitions() {
        let (_tmp, models) = project_with_use_test("amount > 0");
        let first = declarative_check_digest(&models, "orders").expect("digest");
        let second = declarative_check_digest(&models, "orders").expect("digest");
        assert_eq!(first, second, "the same files must digest the same");

        // Change ONLY the shared definition. The sidecar is untouched.
        let sidecar_before = std::fs::read(models.join("orders.toml")).expect("sidecar readable");
        let (_tmp2, edited) = project_with_use_test("amount > -1000000");
        let sidecar_after = std::fs::read(edited.join("orders.toml")).expect("sidecar readable");
        assert_eq!(
            sidecar_before, sidecar_after,
            "the fixture must differ ONLY in test_definitions.toml, or this proves nothing"
        );
        let moved = declarative_check_digest(&edited, "orders").expect("digest");
        assert_ne!(
            first, moved,
            "editing the shared definition changes what would execute, so the digest \
             must move — the sidecar hash alone cannot see this"
        );
    }

    /// A model sidecar whose only check is a `[[use_test]]` reference,
    /// plus the shared definition that gives it its SQL.
    fn project_with_use_test(expression: &str) -> (tempfile::TempDir, std::path::PathBuf) {
        let tmp = tempfile::tempdir().expect("tempdir");
        let models = tmp.path().join("models");
        std::fs::create_dir_all(&models).expect("models dir");
        std::fs::write(
            models.join("_defaults.toml"),
            "[target]\ncatalog = \"wh\"\nschema = \"out\"\n",
        )
        .expect("defaults");
        std::fs::write(models.join("orders.sql"), "SELECT 1 AS amount").expect("sql");
        std::fs::write(
            models.join("orders.toml"),
            "name = \"orders\"\n\n[[use_test]]\nname = \"positive_amount\"\ncolumn = \"amount\"\n",
        )
        .expect("sidecar");
        std::fs::write(
            models.join("test_definitions.toml"),
            format!("[positive_amount]\ntype = \"expression\"\nexpression = \"{expression}\"\n"),
        )
        .expect("definitions");
        (tmp, models)
    }

    #[test]
    fn declarative_test_count_includes_expanded_use_test_references() {
        // #1495: the model loader resolves every `[[use_test]]` against
        // `test_definitions.toml` and APPENDS it to `ModelConfig.tests`,
        // which is the vector `run_declarative_tests` iterates. Counting
        // the sidecar's raw `[[tests]]` array would report 1 here and
        // silently drop the reference — an undercount, and the third
        // one found in this area. Counting through the runner's own
        // loader makes the counted set the executed set.
        let (_tmp, models) = project("\n[[use_test]]\nname = \"positive\"\n");
        assert_eq!(
            declarative_test_count(&models, "orders").expect("count"),
            2,
            "the expanded [[use_test]] reference must be counted alongside the inline test"
        );
        // The loader agrees with the counter, by construction.
        let loaded = load_all_models(&models).expect("load models");
        assert_eq!(loaded[0].config.tests.len(), 2);
    }

    #[test]
    fn declarative_test_count_refuses_an_unknown_model_rather_than_returning_zero() {
        // A caller must be able to tell "no checks" from "no answer".
        let (_tmp, models) = project("");
        assert_eq!(declarative_test_count(&models, "orders").expect("count"), 1);
        let err = declarative_test_count(&models, "nope").expect_err("unknown model");
        assert!(
            err.to_string().contains("nope"),
            "the refusal must name the model: {err}"
        );
    }

    #[test]
    fn declarative_loader_includes_tests_from_rocky_models() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let models = tmp.path().join("models");
        std::fs::create_dir(&models).expect("create models dir");
        std::fs::write(
            models.join("orders.rocky"),
            "from raw_orders\nselect { id }\n",
        )
        .expect("write rocky model");
        std::fs::write(
            models.join("orders.toml"),
            "name = \"orders\"\n\n[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"orders\"\n\n[[tests]]\ntype = \"not_null\"\ncolumn = \"id\"\n",
        )
        .expect("write model sidecar");

        let loaded = load_all_models(&models).expect("load models");

        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].config.name, "orders");
        assert_eq!(loaded[0].config.tests.len(), 1);
        assert_eq!(loaded[0].config.tests[0].column.as_deref(), Some("id"));
    }
}
