//! Top-level compiler entry point.
//!
//! Orchestrates: load models → resolve deps → build semantic graph →
//! type check → validate contracts → produce `CompileResult`.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use indexmap::IndexMap;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::contracts::{self, CompilerContract};
use crate::diagnostic::{Diagnostic, W011};
use crate::project::{Project, ProjectError};
use crate::semantic::{self, SemanticGraph};
use crate::typecheck::{self, TypeCheckResult};
use crate::types::{RockyType, TypedColumn};

/// Wall-clock duration of each compile phase.
///
/// Surfaced in `CompileResult` so callers (CLI, Dagster, LSP) can attribute
/// compile time to a specific stage instead of treating compile as a black box.
/// `typecheck_join_keys_ms` is a sub-timer of `typecheck_ms` so we can decide
/// whether the cross-model join-key check needs further optimization without
/// re-instrumenting later.
#[derive(Debug, Default, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PhaseTimings {
    pub project_load_ms: u64,
    pub semantic_graph_ms: u64,
    pub typecheck_ms: u64,
    /// Portion of `typecheck_ms` spent inside `check_join_keys`.
    pub typecheck_join_keys_ms: u64,
    pub contracts_ms: u64,
    pub total_ms: u64,
}

/// Per-model compile cost, broken out from `PhaseTimings.typecheck_ms`.
///
/// Today only `typecheck_ms` is populated (semantic-graph build is whole-DAG
/// rather than per-model, so attributing it would require extra bookkeeping
/// that isn't worth it yet). `total_ms` equals `typecheck_ms` for now and is
/// kept as a separate field so future per-model phases (semantic split,
/// per-model contract validation) can extend it without changing the wire
/// shape.
#[derive(Debug, Default, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ModelCompileTimings {
    pub typecheck_ms: u64,
    pub total_ms: u64,
}

/// Configuration for the compiler.
#[derive(Clone)]
pub struct CompilerConfig {
    /// Directory containing model files.
    pub models_dir: PathBuf,
    /// Optional directory containing `.contract.toml` files.
    pub contracts_dir: Option<PathBuf>,
    /// Known source schemas (from warehouse DESCRIBE or cache).
    /// Keys are fully qualified table names (e.g., "catalog.schema.table").
    pub source_schemas: HashMap<String, Vec<TypedColumn>>,
    /// Known source column info for semantic graph (name, type string, nullable).
    pub source_column_info: HashMap<String, Vec<rocky_core::ir::ColumnInfo>>,
}

/// Result of compilation.
pub struct CompileResult {
    /// The loaded and resolved project.
    pub project: Project,
    /// The semantic graph (cross-DAG lineage).
    pub semantic_graph: SemanticGraph,
    /// Type check results (per-model typed schemas).
    pub type_check: TypeCheckResult,
    /// Contract validation diagnostics.
    pub contract_diagnostics: Vec<Diagnostic>,
    /// All diagnostics (type check + contracts merged).
    pub diagnostics: Vec<Diagnostic>,
    /// Whether compilation has errors (vs just warnings).
    pub has_errors: bool,
    /// Per-phase wall-clock timings.
    pub timings: PhaseTimings,
    /// Per-model compile cost, derived from typecheck instrumentation.
    /// Empty when no models were compiled. Source schemas don't appear
    /// here because they're injected pre-typecheck.
    pub model_timings: HashMap<String, ModelCompileTimings>,
}

/// Compile error.
#[derive(Debug, thiserror::Error)]
pub enum CompileError {
    #[error("project loading failed: {0}")]
    Project(#[from] ProjectError),

    #[error("semantic graph failed: {0}")]
    SemanticGraph(String),

    #[error("contract loading failed: {0}")]
    ContractLoad(String),
}

/// Compile a project from a models directory.
///
/// This is the main entry point for `rocky compile`.
pub fn compile(config: &CompilerConfig) -> Result<CompileResult, CompileError> {
    let total_start = Instant::now();

    // 1. Load and resolve project
    let load_start = Instant::now();
    let project = Project::load(&config.models_dir)?;
    let project_load_ms = load_start.elapsed().as_millis() as u64;

    let mut result = compile_project(project, config)?;
    result.timings.project_load_ms = project_load_ms;
    result.timings.total_ms = total_start.elapsed().as_millis() as u64;

    tracing::info!(
        target: "rocky::compile::timings",
        project_load_ms = result.timings.project_load_ms,
        semantic_graph_ms = result.timings.semantic_graph_ms,
        typecheck_ms = result.timings.typecheck_ms,
        typecheck_join_keys_ms = result.timings.typecheck_join_keys_ms,
        contracts_ms = result.timings.contracts_ms,
        total_ms = result.timings.total_ms,
        models = result.project.model_count(),
        "compile finished"
    );

    Ok(result)
}

/// Compile a pre-loaded project (useful when models come from other sources).
pub fn compile_project(
    project: Project,
    config: &CompilerConfig,
) -> Result<CompileResult, CompileError> {
    let mut timings = PhaseTimings::default();

    // 2. Build semantic graph
    let sg_start = Instant::now();
    let semantic_graph = semantic::build_semantic_graph(&project, &config.source_column_info)
        .map_err(CompileError::SemanticGraph)?;
    timings.semantic_graph_ms = sg_start.elapsed().as_millis() as u64;

    // 3. Type check (with model SQL/paths for reference tracking)
    let join_keys_acc = Arc::new(AtomicU64::new(0));
    let tc_start = Instant::now();
    let mut type_check = typecheck::typecheck_project_with_models(
        &semantic_graph,
        &config.source_schemas,
        None,
        &project.models,
        Some(&join_keys_acc),
    );
    timings.typecheck_ms = tc_start.elapsed().as_millis() as u64;
    timings.typecheck_join_keys_ms = join_keys_acc.load(Ordering::Relaxed);

    // 4. Load and validate contracts (explicit dir + auto-discovered from model sidecars)
    let contracts_start = Instant::now();
    let contract_diagnostics = {
        // Start with auto-discovered contracts from model.contract_path
        let mut contract_map = contracts::discover_contracts_from_models(&project.models)
            .map_err(CompileError::ContractLoad)?;

        // Merge explicit contracts dir (explicit wins on collision)
        if let Some(ref contracts_dir) = config.contracts_dir {
            let explicit =
                contracts::load_contracts(contracts_dir).map_err(CompileError::ContractLoad)?;
            contract_map.extend(explicit);
        }

        if contract_map.is_empty() {
            Vec::new()
        } else {
            validate_all_contracts(&contract_map, &type_check.typed_models)
        }
    };
    timings.contracts_ms = contracts_start.elapsed().as_millis() as u64;

    // 5. Extract per-model timings before borrowing type_check for diagnostics.
    // `std::mem::take` moves the map out, avoiding 50k+ String clones from `.iter().map(clone)`.
    let model_timings: HashMap<String, ModelCompileTimings> =
        std::mem::take(&mut type_check.model_typecheck_ms)
            .into_iter()
            .map(|(name, ms)| {
                (
                    name,
                    ModelCompileTimings {
                        typecheck_ms: ms,
                        total_ms: ms,
                    },
                )
            })
            .collect();

    // 6. Merge all diagnostics.
    let mut diagnostics = type_check.diagnostics.clone();
    diagnostics.extend(contract_diagnostics.iter().cloned());

    let has_errors = diagnostics
        .iter()
        .any(super::diagnostic::Diagnostic::is_error);

    Ok(CompileResult {
        project,
        semantic_graph,
        type_check,
        contract_diagnostics,
        diagnostics,
        has_errors,
        timings,
        model_timings,
    })
}

fn validate_all_contracts(
    contract_map: &HashMap<String, CompilerContract>,
    typed_models: &IndexMap<String, Vec<TypedColumn>>,
) -> Vec<Diagnostic> {
    let mut all_diags = Vec::new();

    for (model_name, contract) in contract_map {
        if let Some(schema) = typed_models.get(model_name) {
            let diags = contracts::validate_contract(model_name, schema, contract);
            all_diags.extend(diags);
        } else {
            all_diags.push(Diagnostic::warning(
                W011,
                model_name,
                format!("contract exists for '{model_name}' but model was not found in project"),
            ));
        }
    }

    all_diags
}

/// Convert warehouse type strings to RockyType (default mapper).
pub fn default_type_mapper(warehouse_type: &str) -> RockyType {
    let upper = warehouse_type.trim().to_uppercase();
    match upper.as_str() {
        "BOOLEAN" | "BOOL" => RockyType::Boolean,
        "TINYINT" | "BYTE" | "SMALLINT" | "SHORT" | "INT" | "INTEGER" => RockyType::Int32,
        "BIGINT" | "LONG" => RockyType::Int64,
        "FLOAT" | "REAL" => RockyType::Float32,
        "DOUBLE" | "DOUBLE PRECISION" => RockyType::Float64,
        "STRING" | "VARCHAR" | "TEXT" => RockyType::String,
        "BINARY" => RockyType::Binary,
        "DATE" => RockyType::Date,
        "TIMESTAMP" => RockyType::Timestamp,
        "TIMESTAMP_NTZ" => RockyType::TimestampNtz,
        "VARIANT" => RockyType::Variant,
        _ if upper.starts_with("DECIMAL") || upper.starts_with("NUMERIC") => {
            // Try to parse precision and scale
            if let Some(params) = upper
                .strip_prefix("DECIMAL(")
                .or_else(|| upper.strip_prefix("NUMERIC("))
                .and_then(|s| s.strip_suffix(')'))
            {
                let parts: Vec<&str> = params.split(',').collect();
                if parts.len() == 2 {
                    if let (Ok(p), Ok(s)) = (parts[0].trim().parse(), parts[1].trim().parse()) {
                        return RockyType::Decimal {
                            precision: p,
                            scale: s,
                        };
                    }
                }
            }
            RockyType::Decimal {
                precision: 38,
                scale: 0,
            }
        }
        _ => RockyType::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_type_mapper() {
        assert_eq!(default_type_mapper("STRING"), RockyType::String);
        assert_eq!(default_type_mapper("BIGINT"), RockyType::Int64);
        assert_eq!(default_type_mapper("BOOLEAN"), RockyType::Boolean);
        assert_eq!(
            default_type_mapper("DECIMAL(10,2)"),
            RockyType::Decimal {
                precision: 10,
                scale: 2
            }
        );
        assert_eq!(default_type_mapper("unknown_type"), RockyType::Unknown);
    }
}
