//! Airbyte source integration for Rocky.
//!
//! # Scope
//!
//! `rocky-airbyte` is a **source-side** integration: it talks to the Airbyte
//! Configuration API to enumerate connections and their configured streams.
//! The only adapter trait it implements is
//! [`rocky_core::traits::DiscoveryAdapter`].
//!
//! # Relationship to `WarehouseAdapter::fetch_arrow_batch`
//!
//! [`rocky_core::traits::WarehouseAdapter::fetch_arrow_batch`] (added as a
//! PoC against `rocky-duckdb`) is **intentionally not implemented here** —
//! Airbyte is not a warehouse and Rocky never executes queries against it.
//! Sync output lands in a downstream warehouse (Databricks, Snowflake,
//! BigQuery, Trino, Iceberg, DuckDB), and Arrow-shaped reads of that data go
//! through the warehouse adapter for that destination — not through this
//! crate.
//!
//! The Cluster H "Arrow rollout" docstring in `rocky-core::traits` lists
//! Airbyte alongside the warehouse adapters; that listing is aspirational
//! and does not reflect a `WarehouseAdapter` impl plan for this crate.

pub mod adapter;
pub mod client;
