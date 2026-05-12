//! Re-export shim. The IR types live in the `rocky-ir` crate.
//!
//! Existing in-crate `use crate::ir::X` call sites keep working via this
//! re-export. External consumers should reach for `rocky_ir::X` directly.

pub use rocky_ir::*;
