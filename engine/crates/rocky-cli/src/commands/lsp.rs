//! `rocky lsp` — Language Server Protocol for IDE integration.

use std::path::PathBuf;

use anyhow::Result;

/// Execute `rocky lsp` — starts the LSP server on stdin/stdout.
///
/// `state_path` and `state_namespace` are the global `--state-path` and
/// `--state-namespace` flags. The server spools its AI usage next to the
/// store they select, as the CLI does (#2587).
pub async fn run_lsp(state_path: Option<PathBuf>, state_namespace: Option<String>) -> Result<()> {
    // The per-model-target checks of `rocky compile`; the standalone
    // adapter-free `rocky-lsp` binary cannot install them.
    rocky_server::project_gates::install_project_gates(super::apply_model_target_gates);
    rocky_server::lsp::set_state_location(rocky_server::lsp::StateLocation {
        explicit: state_path,
        namespace: state_namespace,
    });
    rocky_server::lsp::run_lsp().await;
    Ok(())
}
