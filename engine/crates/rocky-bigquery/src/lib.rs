//! BigQuery warehouse adapter for Rocky.
//!
//! Implements the WarehouseAdapter, SqlDialect, and DiscoveryAdapter traits
//! for Google BigQuery using the REST API (jobs.query + jobs.getQueryResults).

pub mod auth;
pub mod connector;
pub mod dialect;
pub mod governance;
pub mod loader;

pub use connector::BigQueryAdapter;
pub use dialect::BigQueryDialect;
pub use loader::BigQueryLoaderAdapter;
