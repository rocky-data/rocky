//! `rocky test` — local model testing.
//!
//! Compiles models, executes them locally via DuckDB, validates
//! output against contracts, and reports pass/fail.

use std::collections::HashMap;
use std::path::Path;

use rocky_compiler::compile::CompilerConfig;
use rocky_duckdb::DuckDbConnector;
use tracing::info;

/// Result of a test run.
#[derive(Debug)]
pub struct TestResult {
    /// Total models tested.
    pub total: usize,
    /// Models that passed all checks.
    pub passed: usize,
    /// Models that failed (name, reason).
    pub failures: Vec<(String, String)>,
    /// Compilation diagnostics.
    pub diagnostics: Vec<rocky_compiler::diagnostic::Diagnostic>,
}

/// Run tests on a project.
///
/// 1. Compile models
/// 2. Execute each model locally via DuckDB
/// 3. Validate output against contracts (if present)
/// 4. Report results
pub fn run_tests(models_dir: &Path, contracts_dir: Option<&Path>) -> anyhow::Result<TestResult> {
    let config = CompilerConfig {
        models_dir: models_dir.to_path_buf(),
        contracts_dir: contracts_dir.map(std::path::Path::to_path_buf),
        source_schemas: HashMap::new(),
        source_column_info: HashMap::new(),
    };

    let compile_result = rocky_compiler::compile::compile(&config)?;

    let mut result = TestResult {
        total: compile_result.project.model_count(),
        passed: 0,
        failures: Vec::new(),
        diagnostics: compile_result.diagnostics.clone(),
    };

    // Check for compilation errors
    if compile_result.has_errors {
        for d in &compile_result.diagnostics {
            if d.is_error() {
                result.failures.push((d.model.clone(), d.message.clone()));
            }
        }
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
                return Ok(result);
            }
            info!(path = %seed_path.display(), "loaded seed data");
        }
    }

    let exec_result = crate::executor::execute_locally(&compile_result, &db);

    result.passed = exec_result.succeeded.len();
    result.failures.extend(exec_result.failed);

    info!(
        total = result.total,
        passed = result.passed,
        failed = result.failures.len(),
        "test run complete"
    );

    Ok(result)
}
