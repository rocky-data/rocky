use std::collections::HashMap;

use serde::Deserialize;

/// Fivetran schema configuration for a connector (nested JSON structure).
///
/// Endpoint: `GET /v1/connectors/{connectorId}/schemas`
#[derive(Debug, Clone, Deserialize)]
pub struct SchemaConfig {
    pub schemas: HashMap<String, SchemaEntry>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SchemaEntry {
    #[serde(default)]
    pub enabled: bool,
    /// Destination schema name. When Fivetran renames a schema in the
    /// destination (manual override, or automatic conflict resolution) the
    /// logical map key here diverges from what is actually written to the
    /// warehouse. `enabled_tables()` prefers this when present so consumers
    /// see the name that matches `information_schema.schemata`.
    #[serde(default)]
    pub name_in_destination: Option<String>,
    #[serde(default)]
    pub tables: HashMap<String, TableEntry>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TableEntry {
    #[serde(default)]
    pub enabled: bool,
    /// Destination table name. Fivetran sets this to the actual warehouse
    /// table name, which diverges from the logical map key when the user
    /// has renamed the destination or when Fivetran has auto-prefixed the
    /// table with `do_not_alter_` following a breaking schema change. The
    /// destination name is what `information_schema.tables` reports, so
    /// downstream source-existence checks must use this — not the logical
    /// key — or they will silently drop every renamed table.
    #[serde(default)]
    pub name_in_destination: Option<String>,
    #[serde(default)]
    pub sync_mode: Option<String>,
    #[serde(default)]
    pub columns: HashMap<String, ColumnEntry>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ColumnEntry {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub hashed: bool,
}

/// A flattened table from the schema config.
#[derive(Debug, Clone)]
pub struct EnabledTable {
    pub schema_name: String,
    pub table_name: String,
    pub sync_mode: Option<String>,
    pub column_count: usize,
}

impl SchemaConfig {
    /// Returns all enabled tables across all enabled schemas.
    ///
    /// When `name_in_destination` is set on a schema or table entry, that
    /// value is used in place of the logical map key. This matches what
    /// Fivetran actually writes to the destination warehouse, which is
    /// the only name that will appear in `information_schema`. Leaving
    /// `enabled_tables` stuck on the logical key caused the
    /// `do_not_alter_` bug where Fivetran auto-renamed tables were
    /// silently excluded from discovery.
    pub fn enabled_tables(&self) -> Vec<EnabledTable> {
        let mut tables = Vec::new();

        for (schema_key, schema) in &self.schemas {
            if !schema.enabled {
                continue;
            }
            let schema_name = schema
                .name_in_destination
                .clone()
                .unwrap_or_else(|| schema_key.clone());
            for (table_key, table) in &schema.tables {
                if !table.enabled {
                    continue;
                }
                let table_name = table
                    .name_in_destination
                    .as_deref()
                    .unwrap_or(table_key)
                    .to_lowercase();
                tables.push(EnabledTable {
                    schema_name: schema_name.clone(),
                    table_name,
                    sync_mode: table.sync_mode.clone(),
                    column_count: table.columns.len(),
                });
            }
        }

        tables.sort_by(|a, b| {
            a.schema_name
                .cmp(&b.schema_name)
                .then(a.table_name.cmp(&b.table_name))
        });
        tables
    }

    /// Returns the total count of enabled tables.
    pub fn enabled_table_count(&self) -> usize {
        self.schemas
            .values()
            .filter(|s| s.enabled)
            .flat_map(|s| s.tables.values())
            .filter(|t| t.enabled)
            .count()
    }
}

/// Fetches the schema config for a connector.
pub async fn get_schema_config(
    client: &crate::client::FivetranClient,
    connector_id: &str,
) -> Result<SchemaConfig, crate::client::FivetranError> {
    let path = format!("/v1/connectors/{connector_id}/schemas");
    client.get(&path).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_config() -> SchemaConfig {
        let json = r#"{
            "schemas": {
                "src__acme__na__fb_ads": {
                    "enabled": true,
                    "tables": {
                        "orders": {
                            "enabled": true,
                            "sync_mode": "SOFT_DELETE",
                            "columns": {
                                "id": { "enabled": true, "hashed": false },
                                "name": { "enabled": true, "hashed": false },
                                "_fivetran_synced": { "enabled": true, "hashed": false }
                            }
                        },
                        "payments": {
                            "enabled": true,
                            "sync_mode": "SOFT_DELETE",
                            "columns": {
                                "id": { "enabled": true, "hashed": false }
                            }
                        },
                        "disabled_table": {
                            "enabled": false,
                            "columns": {}
                        }
                    }
                },
                "disabled_schema": {
                    "enabled": false,
                    "tables": {
                        "some_table": {
                            "enabled": true,
                            "columns": {}
                        }
                    }
                }
            }
        }"#;
        serde_json::from_str(json).unwrap()
    }

    #[test]
    fn test_enabled_tables() {
        let config = sample_config();
        let tables = config.enabled_tables();

        assert_eq!(tables.len(), 2);
        assert_eq!(tables[0].table_name, "orders");
        assert_eq!(tables[0].column_count, 3);
        assert_eq!(tables[1].table_name, "payments");
    }

    #[test]
    fn test_enabled_table_count() {
        let config = sample_config();
        assert_eq!(config.enabled_table_count(), 2);
    }

    #[test]
    fn test_disabled_schema_excluded() {
        let config = sample_config();
        let tables = config.enabled_tables();
        // "some_table" in disabled_schema should not appear
        assert!(!tables.iter().any(|t| t.table_name == "some_table"));
    }

    #[test]
    fn test_disabled_table_excluded() {
        let config = sample_config();
        let tables = config.enabled_tables();
        assert!(!tables.iter().any(|t| t.table_name == "disabled_table"));
    }

    #[test]
    fn test_sync_mode() {
        let config = sample_config();
        let tables = config.enabled_tables();
        assert_eq!(tables[0].sync_mode.as_deref(), Some("SOFT_DELETE"));
    }

    #[test]
    fn test_empty_config() {
        let config: SchemaConfig = serde_json::from_str(r#"{"schemas": {}}"#).unwrap();
        assert!(config.enabled_tables().is_empty());
        assert_eq!(config.enabled_table_count(), 0);
    }

    #[test]
    fn test_table_names_lowercased() {
        let json = r#"{
            "schemas": {
                "test_schema": {
                    "enabled": true,
                    "tables": {
                        "UPPER_CASE_TABLE": {
                            "enabled": true,
                            "columns": {}
                        },
                        "Mixed_Case": {
                            "enabled": true,
                            "columns": {}
                        }
                    }
                }
            }
        }"#;
        let config: SchemaConfig = serde_json::from_str(json).unwrap();
        let tables = config.enabled_tables();
        assert_eq!(tables.len(), 2);
        assert!(
            tables
                .iter()
                .all(|t| t.table_name == t.table_name.to_lowercase())
        );
        assert!(tables.iter().any(|t| t.table_name == "upper_case_table"));
        assert!(tables.iter().any(|t| t.table_name == "mixed_case"));
    }

    #[test]
    fn test_table_name_in_destination_wins_over_logical_key() {
        // Fivetran returns both a logical map key and `name_in_destination`.
        // When they diverge (manual destination rename, or auto-renames like
        // the `do_not_alter_` prefix Fivetran applies after a breaking schema
        // change), the warehouse only has the destination name. Discovery
        // must report the destination name so downstream existence checks
        // against `information_schema.tables` match.
        let json = r#"{
            "schemas": {
                "q__raw__acme__namer__usa__redditads": {
                    "enabled": true,
                    "tables": {
                        "do_not_alter__dpm_ad_group_report_spend": {
                            "enabled": true,
                            "name_in_destination": "do_not_alter_dpm_ad_group_report_spend",
                            "columns": {}
                        },
                        "plain_table": {
                            "enabled": true,
                            "columns": {}
                        }
                    }
                }
            }
        }"#;
        let config: SchemaConfig = serde_json::from_str(json).unwrap();
        let tables = config.enabled_tables();
        assert_eq!(tables.len(), 2);
        // The renamed table reports the destination name, not the logical key.
        assert!(
            tables
                .iter()
                .any(|t| t.table_name == "do_not_alter_dpm_ad_group_report_spend"),
            "expected destination name to win over logical key, got {tables:?}"
        );
        assert!(
            !tables
                .iter()
                .any(|t| t.table_name == "do_not_alter__dpm_ad_group_report_spend"),
            "logical key must not leak through when name_in_destination is set"
        );
        // Tables without name_in_destination continue to use the logical key.
        assert!(tables.iter().any(|t| t.table_name == "plain_table"));
    }

    #[test]
    fn test_schema_name_in_destination_wins_over_logical_key() {
        let json = r#"{
            "schemas": {
                "logical_schema": {
                    "enabled": true,
                    "name_in_destination": "actual_destination_schema",
                    "tables": {
                        "a_table": {
                            "enabled": true,
                            "columns": {}
                        }
                    }
                }
            }
        }"#;
        let config: SchemaConfig = serde_json::from_str(json).unwrap();
        let tables = config.enabled_tables();
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].schema_name, "actual_destination_schema");
    }

    #[test]
    fn test_table_without_columns_field() {
        // Some Fivetran connectors omit the "columns" field entirely
        let json = r#"{
            "schemas": {
                "test_schema": {
                    "enabled": true,
                    "tables": {
                        "has_columns": {
                            "enabled": true,
                            "columns": {"id": {"enabled": true}}
                        },
                        "no_columns": {
                            "enabled": true
                        }
                    }
                }
            }
        }"#;
        let config: SchemaConfig = serde_json::from_str(json).unwrap();
        let tables = config.enabled_tables();
        assert_eq!(tables.len(), 2);
        let no_cols = tables
            .iter()
            .find(|t| t.table_name == "no_columns")
            .unwrap();
        assert_eq!(no_cols.column_count, 0);
    }
}
