//! `rocky-catalog-core` — catalog client abstraction for Rocky.
//!
//! The data-catalog ecosystem (Iceberg REST, Apache Polaris, Unity Catalog,
//! Project Nessie) converges at the table-CRUD layer: namespaces, tables,
//! schemas, and atomic transactions are spoken over a small, broadly
//! interoperable REST surface. Above that layer the catalogs diverge — RBAC
//! models, branch/tag semantics, materialized-view support, and tagging
//! systems are catalog-specific.
//!
//! This crate defines the convergent surface as the [`CatalogClient`] trait
//! and a handful of supporting value types. Concrete implementations live in
//! the warehouse / catalog adapter crates that compose a `CatalogClient`
//! field; catalog-specific extensions (per-catalog RBAC, branch merge
//! semantics, MVs) stay in those adapter crates rather than leaking into
//! this trait.
//!
//! The trait is intentionally pure-abstraction: no implementations, no
//! callers, no behavioural opinions. It serves as the contract that
//! adapter crates implement and that higher layers compose against.
//!
//! Methods that are not universally supported (e.g. `tag_table`,
//! `get_grants`) return [`CatalogError::UnsupportedOperation`] from
//! catalogs that lack a REST surface for the operation. Callers are
//! expected to treat that variant as a soft signal — typically by falling
//! back to a SQL-driven path — rather than as a hard failure.

#![forbid(unsafe_code)]

pub mod client;
pub mod error;
pub mod types;

#[cfg(any(test, feature = "testing"))]
pub mod testing;

pub use client::CatalogClient;
pub use error::{CatalogError, CatalogResult};
pub use types::{BranchKind, BranchRef, ColumnSchema, Grant, TableCommit, TableRef, TableSchema};
