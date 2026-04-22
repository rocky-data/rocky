//! Rocky compiler: semantic analysis, type checking, and contract validation.
//!
//! The compiler orchestrates:
//! 1. **Dependency resolution** — extract table refs from SQL, auto-resolve model dependencies
//! 2. **Semantic graph** — cross-DAG column-level lineage
//! 3. **Type system** — column type inference and checking
//! 4. **Contract validation** — compile-time schema contract enforcement
//! 5. **Compilation** — top-level `compile()` entry point

pub mod arena;
pub mod blast_radius;
pub mod cache;
pub mod compile;
pub mod contracts;
pub mod diagnostic;
pub mod import;
pub mod incrementality;
pub mod limits;
pub mod partial;
pub mod project;
pub mod resolve;
pub mod schema_cache;
pub mod semantic;
pub mod typecheck;
pub mod types;

// Re-export miette for downstream crates that need the rendering types.
pub use miette;
