use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// A discovered connector from any source system (Fivetran, Airbyte, Stitch, manual).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveredConnector {
    pub id: String,
    pub schema: String,
    pub source_type: String,
    pub last_sync_at: Option<DateTime<Utc>>,
    pub tables: Vec<DiscoveredTable>,
}

/// A discovered table within a connector.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveredTable {
    pub name: String,
    pub row_count: Option<u64>,
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

/// Manual source: a static list of schemas/tables for teams without Fivetran.
///
/// ```toml
/// [source]
/// type = "manual"
/// catalog = "my_catalog"
///
/// [[source.schemas]]
/// name = "raw_orders"
/// tables = ["orders", "order_items", "returns"]
///
/// [[source.schemas]]
/// name = "raw_customers"
/// tables = ["customers", "addresses"]
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManualSchemaConfig {
    pub name: String,
    pub tables: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

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
        };
        let json = serde_json::to_string(&conn).unwrap();
        let deserialized: DiscoveredConnector = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.id, "conn_123");
        assert_eq!(deserialized.tables.len(), 2);
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
