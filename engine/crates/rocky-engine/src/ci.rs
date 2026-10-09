//! `rocky ci` — CI/CD integration runner.
//!
//! Runs in CI without warehouse credentials:
//! 1. Run the project's seed file in an in-memory DuckDB
//! 2. `rocky compile` — type-check (typed from the seed), lineage, contracts
//! 3. `rocky test` — execute every model on that seed
//! 4. Report results with exit code

use std::path::Path;

use tracing::info;

use crate::test_runner::{TestModels, TestRunInputs};

/// CI run result.
#[derive(Debug)]
pub struct CiResult {
    /// Compilation passed (no errors).
    pub compile_ok: bool,
    /// Test execution passed.
    pub tests_ok: bool,
    /// Number of models compiled.
    pub models_compiled: usize,
    /// Number of tests passed.
    pub tests_passed: usize,
    /// Number of tests failed.
    pub tests_failed: usize,
    /// All diagnostics.
    pub diagnostics: Vec<rocky_compiler::diagnostic::Diagnostic>,
    /// Test failures.
    pub failures: Vec<(String, String)>,
}

impl CiResult {
    /// Overall pass/fail.
    pub fn passed(&self) -> bool {
        self.compile_ok && self.tests_ok
    }

    /// The process exit code: `0` if compile and tests pass, `1` otherwise.
    ///
    /// `rocky ci` exits with this code and reports the same number in its
    /// JSON. Advisory warnings do not change it: a CI step that wants to act
    /// on them reads the warning-severity entries of `diagnostics`.
    pub fn exit_code(&self) -> i32 {
        if self.passed() { 0 } else { 1 }
    }
}

/// Run the full CI pipeline on the models at `models_dir`, with the seed
/// file at `data/seed.sql` beside it. See [`run_ci_with`].
pub fn run_ci(
    models_dir: &Path,
    contracts_dir: Option<&Path>,
    run_vars: &rocky_core::run_vars::RunVars,
) -> anyhow::Result<CiResult> {
    run_ci_with(TestRunInputs {
        models_dir,
        project_root: models_dir.parent().unwrap_or_else(|| Path::new(".")),
        models: TestModels::Dir,
        contracts_dir,
        model_filter: None,
        run_vars,
        gates: None,
        inlined_gates: None,
        strict_contracts: false,
    })
}

/// Run the full CI pipeline: compile, typed from the project's seed file,
/// then execute every model on that seed in one in-memory DuckDB.
///
/// `inputs.run_vars` supplies per-run `@var(name)` substitutions so a
/// required-var model passes `rocky ci --var name=value`.
pub fn run_ci_with(inputs: TestRunInputs<'_>) -> anyhow::Result<CiResult> {
    info!("running CI pipeline");

    let test_result = crate::test_runner::run_tests_with(inputs)?;

    let compile_ok = !test_result
        .diagnostics
        .iter()
        .any(rocky_compiler::diagnostic::Diagnostic::is_error);

    let tests_ok = test_result.failures.is_empty();

    let result = CiResult {
        compile_ok,
        tests_ok,
        models_compiled: test_result.total,
        tests_passed: test_result.passed,
        tests_failed: test_result.failures.len(),
        diagnostics: test_result.diagnostics,
        failures: test_result.failures,
    };

    info!(
        compile_ok = result.compile_ok,
        tests_ok = result.tests_ok,
        models = result.models_compiled,
        passed = result.tests_passed,
        failed = result.tests_failed,
        exit_code = result.exit_code(),
        "CI pipeline complete"
    );

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `rocky ci` fails on a bad consumer record (E060) but reports it as a
    /// diagnostic: the model tests ran and passed, and no model failed.
    #[test]
    fn a_bad_consumer_record_fails_ci_without_failing_a_model() {
        let dir = tempfile::tempdir().unwrap();
        let models = dir.path().join("models");
        std::fs::create_dir_all(&models).unwrap();
        std::fs::write(models.join("orders.sql"), "SELECT 1 AS id").unwrap();
        std::fs::write(
            models.join("orders.toml"),
            "[strategy]\ntype = \"full_refresh\"\n[target]\ncatalog=\"wh\"\nschema=\"main\"\n",
        )
        .unwrap();
        std::fs::create_dir_all(dir.path().join("consumers")).unwrap();
        std::fs::write(
            dir.path().join("consumers").join("board.toml"),
            "depends_on = [\"nowhere\"]\n",
        )
        .unwrap();
        let result = run_ci(&models, None, &rocky_core::run_vars::RunVars::new()).unwrap();
        assert!(!result.compile_ok);
        assert_eq!(result.exit_code(), 1);
        assert!(result.failures.is_empty(), "{:?}", result.failures);
        assert_eq!(result.tests_passed, 1);
        assert_eq!(result.models_compiled, 1);
    }

    #[test]
    fn test_exit_codes() {
        let ok = CiResult {
            compile_ok: true,
            tests_ok: true,
            models_compiled: 3,
            tests_passed: 3,
            tests_failed: 0,
            diagnostics: vec![],
            failures: vec![],
        };
        assert_eq!(ok.exit_code(), 0);
        assert!(ok.passed());

        let fail = CiResult {
            compile_ok: false,
            tests_ok: true,
            models_compiled: 3,
            tests_passed: 3,
            tests_failed: 0,
            diagnostics: vec![],
            failures: vec![],
        };
        assert_eq!(fail.exit_code(), 1);
        assert!(!fail.passed());

        let test_fail = CiResult {
            compile_ok: true,
            tests_ok: false,
            models_compiled: 3,
            tests_passed: 2,
            tests_failed: 1,
            diagnostics: vec![],
            failures: vec![("m".to_string(), "boom".to_string())],
        };
        assert_eq!(test_fail.exit_code(), 1);
        assert!(!test_fail.passed());

        // Warnings-only: compile + tests pass, but a warning diagnostic is
        // present. The reported code is the code the process exits with, so
        // it stays 0 (the process exited 0 while the JSON said 4).
        let warn = CiResult {
            compile_ok: true,
            tests_ok: true,
            models_compiled: 1,
            tests_passed: 1,
            tests_failed: 0,
            diagnostics: vec![rocky_compiler::diagnostic::Diagnostic::warning(
                "W001", "m", "advisory",
            )],
            failures: vec![],
        };
        assert_eq!(warn.exit_code(), 0);
        assert!(warn.passed());
    }
}
