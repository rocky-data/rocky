//! `rocky ci` — CI/CD integration runner.
//!
//! Runs in CI without warehouse credentials:
//! 1. `rocky compile` — type-check, lineage, contracts
//! 2. `rocky test` — local execution against DuckDB
//! 3. Report results with exit code

use std::path::Path;

use tracing::info;

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

    /// Exit code: 0 if pass, 1 if errors, 2 if warnings-only.
    pub fn exit_code(&self) -> i32 {
        if !self.compile_ok || !self.tests_ok {
            1
        } else if self
            .diagnostics
            .iter()
            .any(|d| d.severity == rocky_compiler::diagnostic::Severity::Warning)
        {
            2
        } else {
            0
        }
    }
}

/// Run the full CI pipeline.
pub fn run_ci(models_dir: &Path, contracts_dir: Option<&Path>) -> anyhow::Result<CiResult> {
    info!("running CI pipeline");

    // Step 1: Compile
    info!("step 1: compile");
    let test_result = crate::test_runner::run_tests(models_dir, contracts_dir)?;

    let compile_ok = !test_result.diagnostics.iter().any(|d| d.is_error());

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
    }
}
