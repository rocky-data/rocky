//! Snowflake warehouse adapter for Rocky.
//!
//! Implements the `SqlDialect` and `WarehouseAdapter` traits for Snowflake,
//! enabling Rocky to generate Snowflake-specific SQL and execute against
//! Snowflake warehouses.
//!
//! Key Snowflake differences from Databricks:
//! - Double-quoted identifiers (not backticks)
//! - `CREATE DATABASE` instead of `CREATE CATALOG`
//! - Explicit column lists in MERGE for both branches (no `UPDATE SET *`,
//!   no `INSERT *` shorthand)
//! - `SAMPLE` instead of `TABLESAMPLE`
//! - `NUMBER(p,s)` instead of `DECIMAL(p,s)` (aliases)
//! - TAG objects for metadata (different from Databricks ALTER SET TAGS)
//!
//! Note: `WarehouseAdapter::fetch_arrow_batch` is intentionally left at the default
//! "not supported" `Err` — Snowflake's public SQL REST API is JSON-only (Arrow lives
//! only in the native driver protocol). See the [`adapter`] module docs for details.

pub mod adapter;
pub mod auth;
pub mod batch;
pub mod connector;
pub mod dialect;
pub mod governance;
pub mod loader;
pub mod stage;
pub mod types;
