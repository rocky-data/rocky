//! PostgreSQL and Amazon Redshift warehouse adapters for Rocky.
//!
//! One crate, two adapter types, because both speak the PostgreSQL wire
//! protocol and share most of their SQL:
//!
//! - `type = "postgres"` — [`PostgresDialect`]: `MERGE` (15+) or
//!   `INSERT … ON CONFLICT` (`merge_mode = "on_conflict"`), materialized
//!   views, `pg_catalog` introspection, transactional DDL.
//! - `type = "redshift"` (beta) — [`RedshiftDialect`]: Redshift `MERGE`,
//!   late-binding views, `svv_columns` introspection, 127-byte identifiers,
//!   `GETDATE()`. Password auth; IAM (`GetClusterCredentials` /
//!   Serverless `GetCredentials`) is a documented follow-up.
//!
//! The connection is native (`tokio-postgres`, no libpq) with rustls TLS
//! (`sslmode` = `disable` / `prefer` / `require` / `verify-full`). Every
//! statement runs over the simple query protocol, where a `;`-joined string
//! is one implicit transaction — the dialects use that to make full
//! refreshes, partition overwrites and materialized-view rebuilds atomic on
//! a pooled connection. See [`connector`].
//!
//! Live coverage: `tests/live_postgres.rs` runs against a real PostgreSQL
//! when `ROCKY_POSTGRES_TEST_HOST` is set. Redshift is SQL-generation-tested
//! only.

pub mod adapter;
pub mod config;
pub mod connector;
pub mod dialect;
pub mod tls;
pub mod types;

pub use adapter::PostgresWarehouseAdapter;
pub use config::{Flavor, MergeMode, PgConfig, SslMode};
pub use connector::{PgClient, PgError};
pub use dialect::{PostgresDialect, RedshiftDialect};
