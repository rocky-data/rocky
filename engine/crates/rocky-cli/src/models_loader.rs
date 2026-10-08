//! The model-directory loader, re-exported from `rocky-compiler`.
//!
//! It lives in `rocky_compiler::models_loader` so `rocky serve`'s resident
//! compile (`rocky-server`, which does not depend on this crate) loads the same
//! per-pipeline model set `rocky dag` and `rocky run --dag` do (#2011).

pub use rocky_compiler::models_loader::*;
