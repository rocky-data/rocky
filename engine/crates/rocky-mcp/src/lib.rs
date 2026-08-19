//! `rocky-mcp` — a Model Context Protocol (MCP) server exposing Rocky's
//! verification, data-grounding, drafting, and governor capabilities over
//! stdio.
//!
//! The differentiated value is the **typed** verification surface
//! (`compile` / `plan_preview` / `lineage` / `test` / `inspect_schema`) — a
//! harness can't reproduce these with a raw shell — plus the policy-gated
//! write path (`draft_model` / `draft_contract` / `draft_check` /
//! `draft_metadata`). Materialization stays human-gated: the agent can only
//! *propose* an AI-authored plan; a human runs `rocky review --approve` +
//! `rocky apply` (a product-bound plan additionally requires
//! `--expect-spec-digest`). `rocky mcp --profile worker` serves a minimal
//! drafting allowlist for untrusted workers ([`McpProfile`]).
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
//! rmcp 3.x pulls schemars 1.x; the rest of the Rocky workspace uses
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
pub use tools::{McpProfile, RockyMcpServer};

/// Serve the Rocky MCP server over stdio until the client disconnects.
///
/// `config_path` is the project's `rocky.toml`; the models directory is
/// resolved as `<config-dir>/models`, matching the CLI's top-level
/// convention. `profile` selects the tool surface: [`McpProfile::Default`]
/// serves everything, [`McpProfile::Worker`] the minimal drafting allowlist.
/// Logging goes to stderr (stdout is reserved for the MCP wire protocol).
pub async fn serve_stdio(
    config_path: std::path::PathBuf,
    profile: McpProfile,
) -> anyhow::Result<()> {
    let server = RockyMcpServer::new_with_profile(config_path, profile);
    tracing::info!(?profile, "starting rocky MCP server over stdio");
    let service = server.serve(stdio()).await.inspect_err(|e| {
        tracing::error!("rocky MCP serve error: {e:?}");
    })?;
    service.waiting().await?;
    Ok(())
}
