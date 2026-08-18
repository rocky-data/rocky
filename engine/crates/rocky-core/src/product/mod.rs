//! Product specs: the `products/<name>.toml` surface and its lowering.
//!
//! A product spec declares WHAT a data product must be — grain, columns,
//! checks, freshness, classifications — and lowers onto primitives the
//! engine already enforces (contracts, declarative tests, sidecar
//! metadata, policy posture). The spec introduces no new runtime
//! semantics: a field that cannot lower is refused at parse time rather
//! than shimmed.
//!
//! # Modules
//!
//! - [`spec`] — the strict parser and the identity (digest) rules.
//! - [`toml_compat`] — the order-preserving reader and the byte-stable
//!   writer the lowered artifacts are rendered through.

pub mod spec;
pub mod toml_compat;
