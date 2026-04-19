//! §P3.11 — `rocky-lsp` standalone binary.
//!
//! A minimal entry point that starts the Rocky language server on
//! stdin/stdout. Unlike the full `rocky` CLI (which links every
//! adapter), this binary only pulls in `rocky-server` — compiler +
//! LSP surface. Editors that install Rocky solely for IDE support
//! avoid paying for the adapter graph.
//!
//! Transport is stdio, matching tower-lsp conventions. Arguments are
//! ignored; editors typically launch this with no flags or a
//! `--stdio` token for compatibility with language-server clients
//! that always pass one.

#[tokio::main]
async fn main() {
    rocky_server::lsp::run_lsp().await;
}
