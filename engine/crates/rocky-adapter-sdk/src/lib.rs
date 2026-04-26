//! Rocky Adapter SDK
//!
//! This crate defines the stable adapter interface for Rocky. Warehouse adapters
//! (Databricks, Snowflake, BigQuery, DuckDB, etc.) implement these traits to
//! plug into Rocky's transformation engine.
//!
//! The SDK provides:
//! - **Stable traits** ([`WarehouseAdapter`], [`SqlDialect`], [`GovernanceAdapter`], etc.)
//! - **Adapter manifest** ([`AdapterManifest`]) for capability discovery
//! - **Process adapter protocol** ([`ProcessAdapter`]) for out-of-process adapters
//! - **Conformance test harness** ([`conformance`]) for validating implementations
//!
//! # Versioning
//!
//! Adapters declare which SDK version they target via [`AdapterManifest::sdk_version`].
//! Breaking changes to traits require a minor version bump with a migration guide.

pub mod conformance;
pub mod manifest;
pub mod process;
pub mod throttle;
pub mod traits;

/// Semantic version for the adapter protocol.
/// Adapters must declare which SDK version they target.
pub const SDK_VERSION: &str = env!("CARGO_PKG_VERSION");

// Re-export core types that adapters need.
pub use manifest::{AdapterCapabilities, AdapterManifest};
pub use traits::{
    AdapterError, AdapterResult, BatchCheckAdapter, ColumnInfo, ColumnSelection,
    DiscoveredConnector, DiscoveredTable, DiscoveryAdapter, DiscoveryResult, FailedSource,
    FailedSourceErrorClass, FileFormat, FreshnessResult, GovernanceAdapter, Grant, GrantTarget,
    LoadOptions, LoadResult, LoadSource, LoaderAdapter, MetadataColumn, Permission, QueryResult,
    RowCountResult, SqlDialect, TableRef, TagTarget, TypeMapper, WarehouseAdapter, is_cloud_uri,
};
