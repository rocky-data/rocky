//! Rocky's unified type system for compile-time type checking.
//!
//! These types now live in `rocky-core::types`; this module re-exports them so
//! existing `rocky_compiler::types::*` call sites continue to compile.
//!
//! The relocation was done as PR-1 of the Typed-IR Option B Phase 1 to break
//! the dependency cycle that would have arisen once `ModelIr` (in
//! `rocky-core::ir`) needed to carry `Vec<TypedColumn>` — `rocky-compiler`
//! depends on `rocky-core` (one-way), so the typed-column primitives must
//! live in `rocky-core` for that to work.

pub use rocky_ir::types::{
    DecimalOp, MAX_DECLARED_PRECISION, MAX_INFERRED_PRECISION, RockyType, StructField, TypedColumn,
    arithmetic_result_type, common_supertype, decimal_from_parts, decimal_parts_of, is_assignable,
    validate_decimal_params,
};
