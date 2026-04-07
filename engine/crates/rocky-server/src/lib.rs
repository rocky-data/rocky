//! Rocky HTTP API server and LSP for IDE integration.
//!
//! Two components:
//! - **`rocky serve`** — HTTP API exposing compilation, lineage, and model metadata
//! - **`rocky lsp`** — Language Server Protocol for `.rocky` and `.sql` IDE support

pub mod api;
pub mod dag_viz;
pub mod dashboard;
pub mod lsp;
pub mod state;
pub mod watch;
