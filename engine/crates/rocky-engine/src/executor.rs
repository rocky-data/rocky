//! Local SQL execution via DuckDB.
//!
//! Executes compiled models locally against a DuckDB instance,
//! either with sampled data or in-memory test data.

use std::collections::HashMap;
use std::path::Path;

use rocky_compiler::compile::{CompileResult, CompilerConfig};
use rocky_duckdb::DuckDbConnector;
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
pub fn execute_locally(compile_result: &CompileResult, db: &DuckDbConnector) -> ExecutionResult {
    let mut result = ExecutionResult {
        succeeded: Vec::new(),
        failed: Vec::new(),
    };

    for layer in &compile_result.project.layers {
        for model_name in layer {
            if let Some(model) = compile_result.project.model(model_name) {
                // Wrap model SQL in CREATE TABLE AS for local execution
                let exec_sql = format!("CREATE OR REPLACE TABLE {model_name} AS\n{}", model.sql);

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

/// Compile and execute a project locally.
pub fn compile_and_execute(models_dir: &Path) -> anyhow::Result<ExecutionResult> {
    let config = CompilerConfig {
        models_dir: models_dir.to_path_buf(),
        contracts_dir: None,
        source_schemas: HashMap::new(),
        source_column_info: HashMap::new(),
        ..Default::default()
    };

    let compile_result = rocky_compiler::compile::compile(&config)?;

    if compile_result.has_errors {
        anyhow::bail!("compilation has errors — cannot execute");
    }

    let db = DuckDbConnector::in_memory()?;
    Ok(execute_locally(&compile_result, &db))
}
