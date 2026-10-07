//! The per-model-target checks of `rocky compile`, for `rocky serve` and
//! `rocky lsp`.
//!
//! `rocky compile` judges each model against the warehouses of the pipelines
//! that load it (E042/E043, E044, E049, E051, E053, E054). Those checks need
//! the adapter registry, which lives in `rocky-cli`; this crate stays
//! adapter-free (the `rocky-lsp` binary links only this crate). So the checks
//! are one function in `rocky-cli` that the `rocky` binary installs here
//! before it starts `serve` or `lsp`. Every compile in this crate then runs
//! its result through [`apply_project_gates`], the single call site that
//! reaches that function.
//!
//! With nothing installed (the standalone `rocky-lsp` binary, unit tests) the
//! compile result is left as the compiler produced it.

use std::path::Path;
use std::sync::OnceLock;

use rocky_compiler::compile::CompileResult;
use rocky_core::config::RockyConfig;

/// Whether a model's SQL on the compile result still has its ephemeral
/// upstreams inlined as CTEs (the statement `rocky run` sends).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelSqlForm {
    /// The authored text.
    Authored,
    /// Ephemeral upstreams inlined.
    Inlined,
}

/// The installed checks: the compile result to extend, the loaded
/// `rocky.toml`, its path, and the form of the SQL.
pub type ProjectGates = fn(&mut CompileResult, &RockyConfig, &Path, ModelSqlForm);

static INSTALLED: OnceLock<ProjectGates> = OnceLock::new();

/// Install the checks for this process. The first call wins.
pub fn install_project_gates(gates: ProjectGates) {
    let _ = INSTALLED.set(gates);
}

/// Run the installed checks over `result`. A no-op when none are installed.
pub(crate) fn apply_project_gates(
    result: &mut CompileResult,
    config: &RockyConfig,
    config_path: &Path,
    sql: ModelSqlForm,
) {
    if let Some(gates) = INSTALLED.get() {
        gates(result, config, config_path, sql);
    }
}
