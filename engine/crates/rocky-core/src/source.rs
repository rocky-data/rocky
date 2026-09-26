use chrono::{DateTime, Utc};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

/// A discovered connector from any source system (Fivetran, Airbyte, Stitch, manual).
///
/// `metadata` carries adapter-specific signals the discovery adapter wants to
/// surface to downstream consumers without baking service-specific shapes into
/// Rocky core. Keys are conventionally namespaced by the adapter kind
/// (`fivetran.service`, `fivetran.custom_reports`, `snowflake.share_id`,
/// `bigquery.labels`, etc.) so entries from different adapters don't collide
/// when an orchestrator folds them into an asset graph.
///
/// Values are opaque [`serde_json::Value`] — Rocky doesn't interpret the
/// payloads, it just relays them. Defaults to empty; adapters opt in by
/// populating keys during [`DiscoveryAdapter::discover`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveredConnector {
    pub id: String,
    pub schema: String,
    pub source_type: String,
    pub last_sync_at: Option<DateTime<Utc>>,
    pub tables: Vec<DiscoveredTable>,
    /// Adapter-namespaced metadata. Empty for adapters that haven't opted
    /// in. Uses [`IndexMap`] so iteration order is insertion-stable — that
    /// keeps downstream JSON output byte-stable across runs (important for
    /// the dagster fixture corpus + codegen-drift CI).
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub metadata: IndexMap<String, serde_json::Value>,
    /// Stable identities of the underlying external object(s) this source
    /// replicates (e.g. the Fivetran ad-account ids recovered from connector
    /// config). A single connector can replicate MANY objects (Fivetran's
    /// `accounts: ["act_1", "act_2"]`), so this is a set, not a scalar. Empty
    /// when the adapter can't determine any — collision detection is then
    /// skipped for this source. Populated lazily by
    /// [`crate::traits::DiscoveryAdapter::enrich_external_object_ids`], not by
    /// [`crate::traits::DiscoveryAdapter::discover`], because recovering the
    /// ids can cost an extra API call per connector. The schema *name*
    /// deliberately does not encode this, which is exactly why two onboards of
    /// the same object under different paths can collide unnoticed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub external_object_ids: Vec<String>,
}

/// A discovered table within a connector.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveredTable {
    pub name: String,
    pub row_count: Option<u64>,
}

/// A source the discovery adapter attempted to fetch metadata for and failed.
///
/// Distinct from "removed upstream" — distinguishing the two is the contract
/// that lets downstream consumers safely diff one discover result against the
/// next without misinterpreting a transient fetch failure as a deletion.
///
/// A connector that is **absent from `connectors`** AND **absent from `failed`**
/// was removed upstream. **Absent from `connectors`** but **present in `failed`**
/// is unknown state — consumers must not delete derived assets for it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailedSource {
    pub id: String,
    pub schema: String,
    pub source_type: String,
    pub error_class: FailedSourceErrorClass,
    pub message: String,
    /// Backoff hint in whole seconds. Populated by adapters whose
    /// failure mode carries a known cooldown — currently only the
    /// Fivetran adapter when its shared circuit breaker trips, where
    /// this is the initial `CircuitConfig::cooldown` for the
    /// configured breaker. Orchestrators (Dagster, etc.) use it as a
    /// `retry_after` hint when scheduling a delayed re-discover.
    /// `None` for all other failure classes (no engine-supplied hint).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cooldown_seconds: Option<u64>,
}

/// Coarse classification of why a discovery fetch failed.
///
/// Adapters that wrap an HTTP-shaped API map their concrete errors onto this
/// enum so downstream consumers can branch on operating-mode without knowing
/// the adapter's own error type. A `Transient` or `Timeout` outcome is
/// expected to clear on the next discover tick; `RateLimit` clears once
/// upstream throttling expires; `Auth` requires a credentials rotation;
/// `Unknown` is the catch-all for anything else (malformed response, JSON
/// decode failure, adapter-specific shapes that don't fit the other
/// classes).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailedSourceErrorClass {
    Transient,
    Timeout,
    RateLimit,
    Auth,
    Unknown,
}

impl std::fmt::Display for FailedSourceErrorClass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            FailedSourceErrorClass::Transient => "transient",
            FailedSourceErrorClass::Timeout => "timeout",
            FailedSourceErrorClass::RateLimit => "rate_limit",
            FailedSourceErrorClass::Auth => "auth",
            FailedSourceErrorClass::Unknown => "unknown",
        };
        f.write_str(s)
    }
}

/// Result of a discovery operation: successful connectors plus any sources
/// the adapter attempted to fetch metadata for and failed on.
///
/// `failed` is empty when discovery completed cleanly. A non-empty `failed`
/// list signals "degraded but useful" — the consumer may still act on
/// `connectors`, but must NOT treat sources missing from this snapshot
/// (relative to a prior one) as deletions until they're absent from both
/// fields.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryResult {
    pub connectors: Vec<DiscoveredConnector>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub failed: Vec<FailedSource>,
}

impl DiscoveryResult {
    /// Build a clean result with no failures — convenience for adapters
    /// whose discovery surface is single-shot (Airbyte's `list_connections`,
    /// DuckDB's `information_schema` query) and therefore can't surface a
    /// per-source failure on top of an overall success.
    pub fn ok(connectors: Vec<DiscoveredConnector>) -> Self {
        Self {
            connectors,
            failed: Vec::new(),
        }
    }
}

/// Source configuration that can be deserialized from rocky.toml.
///
/// The `type` field determines which adapter to use:
/// - `"fivetran"` → rocky-fivetran adapter
/// - `"manual"` → static table list from config
///
/// Future: `"airbyte"`, `"stitch"`, etc.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceType {
    #[serde(rename = "type")]
    pub source_type: String,
}

/// One schema of a `type = "manual"` discovery adapter: a static list of
/// tables for a source with no discovery API.
///
/// ```toml
/// [adapter.local_discovery]
/// type = "manual"
/// kind = "discovery"
///
/// [[adapter.local_discovery.schemas]]
/// name = "raw__orders"
/// tables = ["orders", "order_items", "returns"]
///
/// [[adapter.local_discovery.schemas]]
/// name = "raw__customers"
/// tables = ["customers", "addresses"]
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ManualSchemaConfig {
    /// The source schema name, as the pipeline's `schema_pattern` parses it.
    pub name: String,
    /// The tables in this schema.
    pub tables: Vec<String>,
}

/// The discovery adapter for `type = "manual"`: it reports the schemas and
/// tables declared in config, and makes no call to any system.
///
/// Each declared schema whose name starts with the pipeline's prefix becomes
/// one [`DiscoveredConnector`] with `source_type = "manual"`. A manual source
/// has no sync clock, no row counts and no external ids, so those stay empty
/// and the cross-source collision check skips it.
pub struct ManualDiscoveryAdapter {
    schemas: Vec<ManualSchemaConfig>,
}

impl ManualDiscoveryAdapter {
    pub fn new(schemas: Vec<ManualSchemaConfig>) -> Self {
        Self { schemas }
    }
}

#[async_trait::async_trait]
impl crate::traits::DiscoveryAdapter for ManualDiscoveryAdapter {
    async fn discover(&self, schema_prefix: &str) -> crate::traits::AdapterResult<DiscoveryResult> {
        let connectors = self
            .schemas
            .iter()
            .filter(|s| s.name.starts_with(schema_prefix))
            .map(|s| DiscoveredConnector {
                id: s.name.clone(),
                schema: s.name.clone(),
                source_type: "manual".to_string(),
                last_sync_at: None,
                tables: s
                    .tables
                    .iter()
                    .map(|t| DiscoveredTable {
                        name: t.clone(),
                        row_count: None,
                    })
                    .collect(),
                metadata: IndexMap::new(),
                external_object_ids: Vec::new(),
            })
            .collect();
        // The list is static config, so nothing can partially fail.
        Ok(DiscoveryResult::ok(connectors))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::DiscoveryAdapter;

    #[tokio::test]
    async fn manual_discovery_lists_declared_schemas_matching_the_prefix() {
        let adapter = ManualDiscoveryAdapter::new(vec![
            ManualSchemaConfig {
                name: "raw__orders".into(),
                tables: vec!["orders".into(), "order_items".into()],
            },
            ManualSchemaConfig {
                name: "other".into(),
                tables: vec!["ignored".into()],
            },
        ]);
        let result = adapter.discover("raw__").await.unwrap();
        assert!(result.failed.is_empty());
        assert_eq!(result.connectors.len(), 1);
        let c = &result.connectors[0];
        assert_eq!(c.schema, "raw__orders");
        assert_eq!(c.id, "raw__orders");
        assert_eq!(c.source_type, "manual");
        assert!(c.last_sync_at.is_none());
        assert!(c.external_object_ids.is_empty());
        let names: Vec<&str> = c.tables.iter().map(|t| t.name.as_str()).collect();
        assert_eq!(names, ["orders", "order_items"]);
    }

    #[test]
    fn test_discovered_connector_serialization() {
        let conn = DiscoveredConnector {
            id: "conn_123".into(),
            schema: "raw_shopify".into(),
            source_type: "shopify".into(),
            last_sync_at: Some(Utc::now()),
            tables: vec![
                DiscoveredTable {
                    name: "campaigns".into(),
                    row_count: Some(1500),
                },
                DiscoveredTable {
                    name: "ad_sets".into(),
                    row_count: None,
                },
            ],
            metadata: IndexMap::new(),
            external_object_ids: Vec::new(),
        };
        let json = serde_json::to_string(&conn).unwrap();
        // Empty metadata is skipped from the wire form so adapters that
        // haven't opted in don't add noise to the discover fixture corpus.
        assert!(!json.contains("metadata"));
        let deserialized: DiscoveredConnector = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.id, "conn_123");
        assert_eq!(deserialized.tables.len(), 2);
        assert!(deserialized.metadata.is_empty());
    }

    #[test]
    fn test_discovered_connector_round_trip_with_metadata() {
        let mut metadata = IndexMap::new();
        metadata.insert("fivetran.service".into(), serde_json::json!("shopify"));
        metadata.insert(
            "fivetran.connector_id".into(),
            serde_json::json!("conn_123"),
        );
        metadata.insert(
            "fivetran.reports".into(),
            serde_json::json!([{"name": "custom_report"}]),
        );

        let conn = DiscoveredConnector {
            id: "conn_123".into(),
            schema: "raw_shopify".into(),
            source_type: "shopify".into(),
            last_sync_at: None,
            tables: vec![],
            metadata,
            external_object_ids: Vec::new(),
        };

        let json = serde_json::to_string(&conn).unwrap();
        assert!(json.contains("\"fivetran.service\""));

        let deserialized: DiscoveredConnector = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.metadata.len(), 3);
        assert_eq!(deserialized.metadata["fivetran.service"], "shopify");
        assert!(deserialized.metadata["fivetran.reports"].is_array());
    }

    #[test]
    fn test_manual_schema_deserialization() {
        let toml_str = r#"
            name = "raw_orders"
            tables = ["orders", "order_items", "returns"]
        "#;
        let schema: ManualSchemaConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(schema.name, "raw_orders");
        assert_eq!(schema.tables.len(), 3);
    }
}
