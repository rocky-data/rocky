//! Trino warehouse adapter for Rocky.
//!
//! Implements [`WarehouseAdapter`](rocky_core::traits::WarehouseAdapter)
//! and [`SqlDialect`](rocky_core::traits::SqlDialect) against Trino's
//! `POST /v1/statement` HTTP API. The supported surface is intentionally
//! narrow — the gaps below are explicit at validate time, not implicit:
//!
//! - **Auth:** HTTP Basic (`X-Trino-User` + `Authorization: Basic ...`)
//!   and JWT bearer. OAuth and Kerberos are out of scope.
//! - **Catalog model:** Trino catalogs are server-side connector
//!   instances (Iceberg, Hive, Postgres, etc.) — `CREATE CATALOG` is not
//!   supported by OSS Trino, so the adapter returns `None` from
//!   `create_catalog_sql`. `auto_create_catalogs = true` against this
//!   adapter trips at validate time.
//! - **No MERGE** — Trino's MERGE support is connector-dependent (Iceberg
//!   yes, Hive limited). The dialect's `merge_into` returns an explicit
//!   "not supported" error so `strategy = "merge"` fails loudly rather
//!   than emitting broken SQL.
//! - **No governance / loader / batch-checks** — deferred follow-ups
//!   gated by demand.
//!
//! Conformance against a real Trino coordinator is exercised by the
//! Docker harness behind the `trino-conformance` feature flag; see
//! `tests/conformance.rs` and the README for invocation.

pub mod adapter;
pub mod arrow_stream;
pub mod auth;
pub mod connector;
pub mod dialect;

#[cfg(test)]
mod test_helpers;

pub use adapter::TrinoAdapter;
pub use auth::{AuthError, TrinoAuth};
pub use connector::{
    DEFAULT_TIMEOUT_SECS, TrinoClient, TrinoClientConfig, TrinoColumnMeta, TrinoError,
    TrinoQueryRows,
};
pub use dialect::TrinoDialect;

/// Alias matching the public-facing name used by the brief and the SDK
/// guide: `TrinoConfig`. Internally the type is named `TrinoClientConfig`
/// to keep symmetry with `TrinoClient`. Both names point at the same
/// struct.
pub type TrinoConfig = TrinoClientConfig;
