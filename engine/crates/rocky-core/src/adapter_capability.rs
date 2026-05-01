//! Canonical adapter-capability table.
//!
//! Answers a narrow question: for a given `adapter_type` string (as it
//! appears in `[adapter.*]` `type = "..."`), which Rocky traits can that
//! adapter fulfil — [`WarehouseAdapter`](crate::traits::WarehouseAdapter)
//! (data movement) and/or [`DiscoveryAdapter`](crate::traits::DiscoveryAdapter)
//! (metadata enumeration)?
//!
//! Used by [`config`](crate::config) to validate the `kind` field on
//! `[adapter.*]` blocks and the target of
//! `[pipeline.*.source.discovery]` references at parse time, before
//! the CLI registry tries to instantiate anything.

/// Which Rocky roles an adapter type can play.
///
/// Note the asymmetry:
///
/// - An adapter can support **both** roles (e.g. DuckDB serves as a local
///   warehouse *and* enumerates its own schemas for discovery).
/// - An adapter can support **only** one (e.g. Fivetran has no data path
///   at all — its sole job is to enumerate Fivetran connectors via the
///   Fivetran REST API).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdapterCapability {
    /// Implements [`WarehouseAdapter`](crate::traits::WarehouseAdapter) —
    /// reads and writes table data.
    pub supports_data: bool,

    /// Implements [`DiscoveryAdapter`](crate::traits::DiscoveryAdapter) —
    /// enumerates source schemas / connectors.
    pub supports_discovery: bool,
}

impl AdapterCapability {
    const DATA_ONLY: Self = Self {
        supports_data: true,
        supports_discovery: false,
    };
    const DISCOVERY_ONLY: Self = Self {
        supports_data: false,
        supports_discovery: true,
    };
    const BOTH: Self = Self {
        supports_data: true,
        supports_discovery: true,
    };
}

/// Returns the capability profile for a known adapter type.
///
/// Returns `None` for unknown types — callers should surface an
/// "unsupported adapter type" error with the list of known types.
///
/// Keep this table in lockstep with the match arms in
/// `rocky_cli::registry::AdapterRegistry::from_config` and with the
/// adapter implementations in the workspace.
pub fn capability_for(adapter_type: &str) -> Option<AdapterCapability> {
    let cap = match adapter_type {
        "databricks" | "snowflake" => AdapterCapability::DATA_ONLY,
        "fivetran" | "airbyte" | "iceberg" | "manual" => AdapterCapability::DISCOVERY_ONLY,
        "duckdb" | "bigquery" => AdapterCapability::BOTH,
        _ => return None,
    };
    Some(cap)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_only_adapters() {
        for adapter_type in ["databricks", "snowflake"] {
            let cap = capability_for(adapter_type).expect("known type");
            assert!(cap.supports_data, "{adapter_type} should support data");
            assert!(
                !cap.supports_discovery,
                "{adapter_type} should not support discovery"
            );
        }
    }

    #[test]
    fn discovery_only_adapters() {
        for adapter_type in ["fivetran", "airbyte", "iceberg", "manual"] {
            let cap = capability_for(adapter_type).expect("known type");
            assert!(!cap.supports_data, "{adapter_type} should not support data");
            assert!(
                cap.supports_discovery,
                "{adapter_type} should support discovery"
            );
        }
    }

    #[test]
    fn duckdb_and_bigquery_support_both() {
        for adapter_type in ["duckdb", "bigquery"] {
            let cap = capability_for(adapter_type).expect("known type");
            assert!(cap.supports_data, "{adapter_type} should support data");
            assert!(
                cap.supports_discovery,
                "{adapter_type} should support discovery"
            );
        }
    }

    #[test]
    fn unknown_returns_none() {
        assert!(capability_for("snowpipe").is_none());
        assert!(capability_for("").is_none());
    }
}
