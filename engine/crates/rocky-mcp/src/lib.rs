//! `rocky-mcp` — a Model Context Protocol (MCP) server exposing Rocky's
//! read-only verification and data-grounding capabilities over stdio.
//!
//! The differentiated value is the **typed** verification surface
//! (`compile` / `plan_preview` / `lineage` / `test` / `inspect_schema`) — a
//! harness can't reproduce these with a raw shell. Materialization stays
//! human-gated: the agent can only *propose* an AI-authored plan; a human
//! runs `rocky review --approve` + `rocky apply`.
//!
//! ## Statelessness
//!
//! Each tool call resolves the project from the server's config + models dir
//! and **compiles fresh** via the rocky-cli cores, so it always reflects the
//! current on-disk files. The server holds only the config path / models dir
//! / root. A warm Salsa cache is a deferred optimization (not implemented
//! here) — correctness-first.
//!
//! ## schemars dual-major note
//!
//! rmcp 1.7 pulls schemars 1.x; the rest of the Rocky workspace uses
//! schemars 0.8. The two `JsonSchema` traits are disjoint. Every result
//! struct returned inside `Json<T>` therefore derives schemars **1.x** and is
//! built from "pure" types only (`String`, `usize`, `bool`, `Vec<_>`, local
//! lite structs) — Rocky's 0.8-deriving `*Output` types are projected into
//! these lite shapes at the tool boundary.

mod error;
mod result_types;
mod tools;

use rmcp::{ServiceExt, transport::stdio};

pub use error::{ToolError, ToolErrorCode, ToolResult};
pub use tools::RockyMcpServer;

/// Serve the Rocky MCP server over stdio until the client disconnects.
///
/// `config_path` is the project's `rocky.toml`; the models directory is
/// resolved as `<config-dir>/models`, matching the CLI's top-level
/// convention. Logging goes to stderr (stdout is reserved for the MCP wire
/// protocol).
pub async fn serve_stdio(config_path: std::path::PathBuf) -> anyhow::Result<()> {
    let server = RockyMcpServer::new(config_path);
    tracing::info!("starting rocky MCP server over stdio");
    let service = server.serve(stdio()).await.inspect_err(|e| {
        tracing::error!("rocky MCP serve error: {e:?}");
    })?;
    service.waiting().await?;
    Ok(())
}
