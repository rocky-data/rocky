//! Re-export shim. The type system primitives live in the `rocky-ir` crate.
//!
//! Existing in-crate `use crate::types::X` call sites keep working via this
//! re-export. External consumers should reach for `rocky_ir::types`
//! directly.

pub use rocky_ir::types::*;
