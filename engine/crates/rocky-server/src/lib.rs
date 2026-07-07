//! Rocky server infrastructure for `rocky serve` and the IDE LSP.
//!
//! This crate holds the shared server state, auth/CORS middleware, the
//! server-rendered dashboard, the filesystem watcher, and the LSP. The
//! HTTP router and `/api/v1` handlers themselves live in `rocky-cli`
//! (`rocky_cli::api`), where they can serve the canonical typed output
//! cores that back `rocky <verb> --output json`.
//!
//! - **`rocky serve`** — HTTP API exposing compilation, lineage, and model metadata
//! - **`rocky lsp`** — Language Server Protocol for `.rocky` and `.sql` IDE support
//!
//! # Security expectations
//!
//! `rocky serve` exposes model SQL, file paths, the DAG, and run history.
//! Treat the bind address as production-sensitive — the server defaults
//! to loopback (`127.0.0.1`) and refuses to bind a non-loopback host
//! without a Bearer token (CLI: `--token` / env `ROCKY_SERVE_TOKEN`),
//! held on [`state::ServerState`]. The CORS allowlist is empty by default
//! (same-origin only); cross-origin clients must be enumerated via
//! `--allowed-origin`.
//!
//! See [`auth`] for the middleware and CORS helpers.

pub mod auth;
pub mod dag_viz;
pub mod dashboard;
pub mod lsp;
pub(crate) mod schema_cache_throttle;
pub mod state;
pub mod watch;
