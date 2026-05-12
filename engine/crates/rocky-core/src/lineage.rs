//! Re-export shim. The lineage primitives live in the `rocky-ir` crate.
//!
//! Existing in-crate `use crate::lineage::X` call sites keep working via
//! this re-export. External consumers should reach for `rocky_ir::lineage`
//! directly.

pub use rocky_ir::lineage::*;
