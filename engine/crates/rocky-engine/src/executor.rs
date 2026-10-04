//! Local SQL execution via DuckDB.
//!
//! Executes compiled models locally against a DuckDB instance,
//! either with sampled data or in-memory test data.

use std::collections::HashMap;
use std::path::Path;

use rocky_compiler::compile::{CompileResult, CompilerConfig};
use rocky_duckdb::DuckDbConnector;
use rocky_sql::validation::validate_identifier;
use tracing::info;

/// Result of local execution.
#[derive(Debug)]
pub struct ExecutionResult {
    /// Models executed successfully.
    pub succeeded: Vec<String>,
    /// Models that failed (name, error).
    pub failed: Vec<(String, String)>,
}

/// Execute compiled models locally using DuckDB.
///
/// Models are executed in DAG layer order. Each model's compiled SQL
/// is run against the DuckDB instance.
///
/// # Divergence: surrogate keys are not applied here
///
/// Unlike `rocky run` and `rocky emit-sql` — which call
/// [`rocky_core::models::apply_surrogate_keys`] — this path materializes each
/// model from its compiled SQL alone. A model that declares a `[[surrogate_key]]`
/// block is built here *without* the surrogate column. This backs `rocky test`'s
/// model-execution check ([`crate::test_runner::run_tests`]), so a test that
/// depends on a surrogate column (e.g. a `unique` assertion on it) would behave
/// differently from a real run. Aligning the two would change existing
/// `rocky test` outcomes, so it is left as a known divergence pending a decision
/// on whether `rocky test` should mirror run-time surrogate keys.
pub fn execute_locally(compile_result: &CompileResult, db: &DuckDbConnector) -> ExecutionResult {
    let mut result = ExecutionResult {
        succeeded: Vec::new(),
        failed: Vec::new(),
    };

    // User-defined functions first, as `rocky run` does, so models that call
    // them resolve. A function that fails is reported under its own name.
    result
        .failed
        .extend(create_local_functions(compile_result, db));

    for layer in &compile_result.project.layers {
        for model_name in layer {
            if let Some(model) = compile_result.project.model(model_name) {
                if let Err(e) = validate_identifier(model_name) {
                    result.failed.push((model_name.clone(), e.to_string()));
                    continue;
                }
                // Wrap model SQL in CREATE TABLE AS for local execution.
                // `model_name` was validated above; `model.sql` is compiler-emitted SQL.
                let exec_sql = format!(
                    "CREATE OR REPLACE TABLE {model_name} AS\n{}",
                    rocky_core::sql_gen::local_test_sql(model)
                );

                match db.execute_statement(&exec_sql) {
                    Ok(()) => {
                        info!(model = model_name.as_str(), "model executed locally");
                        result.succeeded.push(model_name.clone());
                    }
                    Err(e) => {
                        result.failed.push((model_name.clone(), e.to_string()));
                    }
                }
            }
        }
    }

    result
}

/// Create every valid user-defined function (`functions/`) as a DuckDB macro,
/// callees first. Returns `(function, error)` for each one that failed.
pub fn create_local_functions(
    compile_result: &CompileResult,
    db: &DuckDbConnector,
) -> Vec<(String, String)> {
    let registry = compile_result.semantic_graph.functions();
    let names: Vec<String> = registry.functions().map(|f| f.def.name.clone()).collect();
    let mut failed = Vec::new();
    for sig in registry.creation_order(names.iter().map(String::as_str)) {
        let created = rocky_core::functions::create_function_sql(
            &sig.def,
            rocky_core::functions::FunctionDialect::DuckDb,
        )
        .map_err(|e| e.to_string())
        .and_then(|sql| db.execute_statement(&sql).map_err(|e| e.to_string()));
        if let Err(e) = created {
            failed.push((sig.def.name.clone(), e));
        }
    }
    failed
}

/// Compile and execute a project locally.
pub fn compile_and_execute(models_dir: &Path) -> anyhow::Result<ExecutionResult> {
    let config = CompilerConfig {
        models_dir: models_dir.to_path_buf(),
        contracts_dir: None,
        source_schemas: HashMap::new(),
        ..Default::default()
    };

    let compile_result = rocky_compiler::compile::compile(&config)?;

    if compile_result.has_errors {
        anyhow::bail!("compilation has errors — cannot execute");
    }

    let db = DuckDbConnector::in_memory()?;
    Ok(execute_locally(&compile_result, &db))
}
