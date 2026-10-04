//! Microsoft SQL Server, Azure SQL and Fabric Warehouse adapter for Rocky.
//!
//! `type = "sqlserver"` speaks TDS natively (`tiberius`, no ODBC driver)
//! with rustls TLS, and renders T-SQL:
//!
//! - `[bracket]`-quoted identifiers, `[db].[schema].[table]`.
//! - Full refresh builds `<table>__rocky_new` with `SELECT … INTO`, then
//!   swaps it in with `DROP` + `sp_rename` inside one transaction.
//! - `CREATE OR ALTER VIEW`, `MERGE … ;` with `HOLDLOCK`, `TOP (n)`,
//!   `DATEADD` lookback, `INFORMATION_SCHEMA.COLUMNS` introspection.
//! - Multi-statement writes run as `SET XACT_ABORT ON; BEGIN TRANSACTION; …;
//!   COMMIT TRANSACTION;`, and a connection that errors is discarded so the
//!   server rolls back anything left open.
//! - Every model SELECT is embedded with its CTEs lifted to the head of the
//!   statement ([`tsql::hoist_ctes`]), because T-SQL refuses `WITH` inside a
//!   derived table.
//!
//! Auth: SQL authentication, a pre-acquired Microsoft Entra ID access token,
//! or an Entra ID service principal (client credentials). See [`config`].
//!
//! Live coverage: `tests/live_sqlserver.rs` runs against a real SQL Server
//! when `ROCKY_SQLSERVER_TEST_HOST` is set. Azure SQL and Fabric are
//! SQL-generation-tested only.

pub mod adapter;
pub mod auth;
pub mod config;
pub mod connector;
pub mod dialect;
pub mod tsql;
pub mod types;

pub use adapter::SqlServerWarehouseAdapter;
pub use config::{Auth, Credentials, Encrypt, Flavor, SqlServerConfig};
pub use connector::{SqlServerClient, SqlServerError};
pub use dialect::SqlServerDialect;
