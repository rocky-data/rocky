//! Re-export shim. The DAG primitives + topological-sort helpers live in
//! the `rocky-ir` crate.
//!
//! Existing in-crate `use crate::dag::X` call sites keep working via this
//! re-export. External consumers should reach for `rocky_ir::dag` directly.

pub use rocky_ir::dag::*;
