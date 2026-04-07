//! Adapter manifest for capability discovery.
//!
//! Each adapter declares its capabilities via an [`AdapterManifest`]. Rocky uses
//! this to validate configuration, skip unsupported features, and display
//! adapter information in `rocky validate`.

use serde::{Deserialize, Serialize};

/// Declares an adapter's identity, capabilities, and configuration schema.
///
/// Adapters return their manifest at registration time (for compiled-in adapters)
/// or in response to the `initialize` RPC call (for process adapters).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdapterManifest {
    /// Adapter name (e.g., "databricks", "bigquery", "snowflake").
    pub name: String,

    /// Adapter version (semver).
    pub version: String,

    /// SDK version this adapter targets (e.g., "0.1.0").
    pub sdk_version: String,

    /// SQL dialect name (e.g., "databricks", "bigquery").
    pub dialect: String,

    /// What this adapter can do.
    pub capabilities: AdapterCapabilities,

    /// Supported authentication methods (e.g., ["pat", "oauth_m2m"]).
    pub auth_methods: Vec<String>,

    /// JSON Schema describing adapter-specific configuration.
    /// Allows Rocky to validate config before passing it to the adapter.
    pub config_schema: serde_json::Value,
}

/// Boolean flags indicating which optional features an adapter supports.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdapterCapabilities {
    /// Core warehouse operations (execute, query, describe). Always true.
    pub warehouse: bool,

    /// Source discovery (schema/table enumeration).
    pub discovery: bool,

    /// Governance (tags, grants, isolation).
    pub governance: bool,

    /// Batched quality checks (UNION ALL row counts, freshness).
    pub batch_checks: bool,

    /// CREATE CATALOG support (some warehouses don't have catalogs).
    pub create_catalog: bool,

    /// CREATE SCHEMA support.
    pub create_schema: bool,

    /// MERGE INTO (upsert) support.
    pub merge: bool,

    /// TABLESAMPLE support.
    pub tablesample: bool,

    /// File loading (CSV, Parquet, JSONL ingestion).
    #[serde(default)]
    pub file_load: bool,
}

impl AdapterCapabilities {
    /// A minimal capabilities set with only warehouse operations.
    pub fn warehouse_only() -> Self {
        Self {
            warehouse: true,
            discovery: false,
            governance: false,
            batch_checks: false,
            create_catalog: false,
            create_schema: false,
            merge: false,
            tablesample: false,
            file_load: false,
        }
    }

    /// Full capabilities (all features supported).
    pub fn full() -> Self {
        Self {
            warehouse: true,
            discovery: true,
            governance: true,
            batch_checks: true,
            create_catalog: true,
            create_schema: true,
            merge: true,
            tablesample: true,
            file_load: true,
        }
    }
}

impl Default for AdapterCapabilities {
    fn default() -> Self {
        Self::warehouse_only()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_manifest_serialization_roundtrip() {
        let manifest = AdapterManifest {
            name: "bigquery".into(),
            version: "0.1.0".into(),
            sdk_version: crate::SDK_VERSION.into(),
            dialect: "bigquery".into(),
            capabilities: AdapterCapabilities {
                warehouse: true,
                discovery: true,
                governance: false,
                batch_checks: true,
                create_catalog: false,
                create_schema: true,
                merge: true,
                tablesample: false,
                file_load: false,
            },
            auth_methods: vec!["service_account".into(), "oauth".into()],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "project_id": { "type": "string" },
                    "dataset": { "type": "string" }
                },
                "required": ["project_id"]
            }),
        };

        let json = serde_json::to_string_pretty(&manifest).unwrap();
        let deserialized: AdapterManifest = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.name, "bigquery");
        assert_eq!(deserialized.version, "0.1.0");
        assert_eq!(deserialized.sdk_version, crate::SDK_VERSION);
        assert!(deserialized.capabilities.warehouse);
        assert!(deserialized.capabilities.discovery);
        assert!(!deserialized.capabilities.governance);
        assert!(deserialized.capabilities.batch_checks);
        assert!(!deserialized.capabilities.create_catalog);
        assert!(deserialized.capabilities.create_schema);
        assert!(deserialized.capabilities.merge);
        assert!(!deserialized.capabilities.tablesample);
        assert_eq!(deserialized.auth_methods.len(), 2);
    }

    #[test]
    fn test_manifest_deserialization_from_json() {
        let json = r#"{
            "name": "duckdb",
            "version": "0.1.0",
            "sdk_version": "0.1.0",
            "dialect": "duckdb",
            "capabilities": {
                "warehouse": true,
                "discovery": false,
                "governance": false,
                "batch_checks": false,
                "create_catalog": false,
                "create_schema": true,
                "merge": false,
                "tablesample": true
            },
            "auth_methods": [],
            "config_schema": {}
        }"#;

        let manifest: AdapterManifest = serde_json::from_str(json).unwrap();
        assert_eq!(manifest.name, "duckdb");
        assert!(!manifest.capabilities.merge);
        assert!(manifest.capabilities.tablesample);
    }

    #[test]
    fn test_capabilities_warehouse_only() {
        let caps = AdapterCapabilities::warehouse_only();
        assert!(caps.warehouse);
        assert!(!caps.discovery);
        assert!(!caps.governance);
        assert!(!caps.batch_checks);
        assert!(!caps.merge);
    }

    #[test]
    fn test_capabilities_full() {
        let caps = AdapterCapabilities::full();
        assert!(caps.warehouse);
        assert!(caps.discovery);
        assert!(caps.governance);
        assert!(caps.batch_checks);
        assert!(caps.create_catalog);
        assert!(caps.create_schema);
        assert!(caps.merge);
        assert!(caps.tablesample);
    }
}
