//! Snowflake warehouse adapter for Rocky.
//!
//! Implements the `SqlDialect` and `WarehouseAdapter` traits for Snowflake,
//! enabling Rocky to generate Snowflake-specific SQL and execute against
//! Snowflake warehouses.
//!
//! Key Snowflake differences from Databricks:
//! - Double-quoted identifiers (not backticks)
//! - `CREATE DATABASE` instead of `CREATE CATALOG`
//! - Explicit column lists in MERGE (no `UPDATE SET *`)
//! - `SAMPLE` instead of `TABLESAMPLE`
//! - `NUMBER(p,s)` instead of `DECIMAL(p,s)` (aliases)
//! - TAG objects for metadata (different from Databricks ALTER SET TAGS)

pub mod adapter;
pub mod auth;
pub mod connector;
pub mod dialect;
pub mod governance;
pub mod types;
