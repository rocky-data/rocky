//! `rocky lsp` — Language Server Protocol for IDE integration.

use anyhow::Result;

/// Execute `rocky lsp` — starts the LSP server on stdin/stdout.
pub async fn run_lsp() -> Result<()> {
    rocky_server::lsp::run_lsp().await;
    Ok(())
}
