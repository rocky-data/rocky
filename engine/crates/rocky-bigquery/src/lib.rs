//! BigQuery warehouse adapter for Rocky.
//!
//! Implements [`WarehouseAdapter`](rocky_core::traits::WarehouseAdapter),
//! [`SqlDialect`](rocky_core::traits::SqlDialect), and
//! [`DiscoveryAdapter`](rocky_core::traits::DiscoveryAdapter) for Google
//! BigQuery using the REST API (jobs.query + jobs.getQueryResults).

pub mod auth;
pub mod batch;
pub mod connector;
pub mod dialect;
pub mod discovery;
pub mod governance;
pub mod loader;

pub use connector::BigQueryAdapter;
pub use dialect::BigQueryDialect;
pub use discovery::BigQueryDiscoveryAdapter;
pub use loader::BigQueryLoaderAdapter;
