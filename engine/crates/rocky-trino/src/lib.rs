//! Trino warehouse adapter for Rocky (v0, experimental).
//!
//! Implements [`WarehouseAdapter`](rocky_core::traits::WarehouseAdapter)
//! and [`SqlDialect`](rocky_core::traits::SqlDialect) against Trino's
//! `POST /v1/statement` HTTP API. v0 is intentionally minimal:
//!
//! - **Auth:** HTTP Basic (`X-Trino-User` + `Authorization: Basic ...`)
//!   and JWT bearer. OAuth and Kerberos are out of scope.
//! - **Catalog default:** `memory` — every Trino catalog is connector-
//!   scoped (Iceberg, Hive, Postgres, etc.) and the Docker sandbox ships
//!   `memory` and `tpch` out-of-the-box. Real deployments map a catalog
//!   to a connector via the Trino server's catalog properties.
//! - **No MERGE in v0** — Trino's MERGE support is connector-dependent
//!   (Iceberg yes, Hive limited). The adapter advertises `merge: false`
//!   so the planner rejects `strategy = "merge"` at validate time.
//! - **No governance / loader / batch checks** — those are deferred
//!   follow-ups gated by demand.
//!
//! The adapter is marked `is_experimental: true` — the runtime logs a
//! warning when it's selected and the experimental flag will drop in a
//! follow-up once the Docker sandbox is exercised end-to-end across the
//! four pipeline strategies.

pub mod adapter;
pub mod auth;
pub mod connector;
pub mod dialect;

pub use adapter::TrinoAdapter;
pub use auth::TrinoAuth;
pub use connector::{TrinoClient, TrinoClientConfig, TrinoError};
pub use dialect::TrinoDialect;
