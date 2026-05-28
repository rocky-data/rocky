//! `rocky test` — local model testing.
//!
//! Compiles models, executes them locally via DuckDB, validates
//! output against contracts, and reports pass/fail.

use std::collections::HashMap;
use std::path::Path;

use rocky_compiler::compile::CompilerConfig;
use rocky_duckdb::DuckDbConnector;
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
pub fn run_tests(
    models_dir: &Path,
    contracts_dir: Option<&Path>,
    model_filter: Option<&str>,
) -> anyhow::Result<TestResult> {
    let config = CompilerConfig {
        models_dir: models_dir.to_path_buf(),
        contracts_dir: contracts_dir.map(std::path::Path::to_path_buf),
        source_schemas: HashMap::new(),
        source_column_info: HashMap::new(),
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
        let result = run_tests(&models, None, None).unwrap();
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
        let result = run_tests(&models, None, Some("good_mart")).unwrap();
        assert_eq!(result.total, 1);
        assert_eq!(result.passed, 1);
        assert_eq!(result.model_results.len(), 1);
        assert_eq!(result.model_results[0].model, "good_mart");
        assert_eq!(result.model_results[0].status, ModelTestStatus::Pass);
    }
}
