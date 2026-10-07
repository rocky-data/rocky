//! `rocky lsp` — Language Server Protocol for IDE integration.

use anyhow::Result;

/// Execute `rocky lsp` — starts the LSP server on stdin/stdout.
pub async fn run_lsp() -> Result<()> {
    // The per-model-target checks of `rocky compile`; the standalone
    // adapter-free `rocky-lsp` binary cannot install them.
    rocky_server::project_gates::install_project_gates(super::apply_model_target_gates);
    rocky_server::lsp::run_lsp().await;
    Ok(())
}
