//! AI intent layer for Rocky.
//!
//! Natural language → Rocky model generation with compile-verify loop.
//! Intent as metadata: explain, sync, and test generation.
//! The compiler is the safety net — LLM output gets type-checked before execution.

pub mod client;
pub mod explain;
pub mod generate;
pub mod prompt;
pub mod sync;
pub mod testgen;
