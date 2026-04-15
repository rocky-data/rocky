//! Rocky DSL: a pipeline-oriented language for SQL transformations.
//!
//! The Rocky DSL compiles through SQL to the same IR used by SQL models.
//! Key semantic improvements over raw SQL:
//! - `!=` compiles to `IS DISTINCT FROM` (NULL-safe)
//! - `@2025-01-01` for date literals
//! - Pipeline-oriented data flow (top to bottom, not inside-out)
//! - `match { ... }` compiles to `CASE WHEN`

pub mod ast;
pub mod error;
pub mod fmt;
pub mod lower;
pub mod parser;
pub mod token;

pub use error::ParseError;
pub use parser::parse;
