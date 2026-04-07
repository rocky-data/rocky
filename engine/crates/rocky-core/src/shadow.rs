//! Shadow mode: target rewriting for write-to-temp-tables validation.
//!
//! Shadow mode runs the full Rocky pipeline against a real warehouse but writes
//! to temporary shadow tables instead of production targets. After execution,
//! a comparison engine checks shadow output against existing production tables.

use serde::{Deserialize, Serialize};

use crate::ir::TargetRef;

/// Configuration for shadow mode target rewriting.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShadowConfig {
    /// Suffix appended to table names in suffix mode (default: `"_rocky_shadow"`).
    #[serde(default = "default_suffix")]
    pub suffix: String,

    /// If set, all shadow tables are written to this schema instead of appending
    /// a suffix. The table name stays the same; only the schema changes.
    /// Example: `"_rocky_shadow"` -> `analytics._rocky_shadow.fct_orders`
    #[serde(default)]
    pub schema_override: Option<String>,

    /// Whether to drop shadow tables after comparison completes (default: `true`).
    #[serde(default = "default_cleanup")]
    pub cleanup_after: bool,
}

fn default_suffix() -> String {
    "_rocky_shadow".to_string()
}

fn default_cleanup() -> bool {
    true
}

impl Default for ShadowConfig {
    fn default() -> Self {
        Self {
            suffix: default_suffix(),
            schema_override: None,
            cleanup_after: true,
        }
    }
}

/// A paired mapping of original production target to its shadow counterpart.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShadowTarget {
    /// The original production target reference.
    pub original: TargetRef,
    /// The rewritten shadow target reference.
    pub shadow: TargetRef,
}

/// Rewrite a production target into a shadow target based on the config.
///
/// Two modes:
/// - **Suffix mode** (default): appends `config.suffix` to the table name.
///   `analytics.marts.fct_orders` -> `analytics.marts.fct_orders_rocky_shadow`
/// - **Schema override mode**: moves the table to a dedicated shadow schema.
///   `analytics.marts.fct_orders` -> `analytics._rocky_shadow.fct_orders`
pub fn shadow_target(original: &TargetRef, config: &ShadowConfig) -> TargetRef {
    if let Some(schema) = &config.schema_override {
        TargetRef {
            catalog: original.catalog.clone(),
            schema: schema.clone(),
            table: original.table.clone(),
        }
    } else {
        TargetRef {
            catalog: original.catalog.clone(),
            schema: original.schema.clone(),
            table: format!("{}{}", original.table, config.suffix),
        }
    }
}

/// Build a list of [`ShadowTarget`] pairs from original targets and a config.
pub fn shadow_targets(originals: &[TargetRef], config: &ShadowConfig) -> Vec<ShadowTarget> {
    originals
        .iter()
        .map(|original| ShadowTarget {
            shadow: shadow_target(original, config),
            original: original.clone(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_target() -> TargetRef {
        TargetRef {
            catalog: "analytics".into(),
            schema: "marts".into(),
            table: "fct_orders".into(),
        }
    }

    #[test]
    fn test_default_suffix() {
        let config = ShadowConfig::default();
        let result = shadow_target(&sample_target(), &config);

        assert_eq!(result.catalog, "analytics");
        assert_eq!(result.schema, "marts");
        assert_eq!(result.table, "fct_orders_rocky_shadow");
    }

    #[test]
    fn test_custom_suffix() {
        let config = ShadowConfig {
            suffix: "_shadow_v2".into(),
            ..Default::default()
        };
        let result = shadow_target(&sample_target(), &config);

        assert_eq!(result.catalog, "analytics");
        assert_eq!(result.schema, "marts");
        assert_eq!(result.table, "fct_orders_shadow_v2");
    }

    #[test]
    fn test_schema_override() {
        let config = ShadowConfig {
            schema_override: Some("_rocky_shadow".into()),
            ..Default::default()
        };
        let result = shadow_target(&sample_target(), &config);

        assert_eq!(result.catalog, "analytics");
        assert_eq!(result.schema, "_rocky_shadow");
        assert_eq!(result.table, "fct_orders");
    }

    #[test]
    fn test_schema_override_preserves_catalog() {
        let target = TargetRef {
            catalog: "acme_warehouse".into(),
            schema: "staging__us_west__shopify".into(),
            table: "orders".into(),
        };
        let config = ShadowConfig {
            schema_override: Some("_rocky_shadow".into()),
            ..Default::default()
        };
        let result = shadow_target(&target, &config);

        assert_eq!(result.catalog, "acme_warehouse");
        assert_eq!(result.schema, "_rocky_shadow");
        assert_eq!(result.table, "orders");
    }

    #[test]
    fn test_shadow_targets_builds_pairs() {
        let originals = vec![
            TargetRef {
                catalog: "cat".into(),
                schema: "sch".into(),
                table: "orders".into(),
            },
            TargetRef {
                catalog: "cat".into(),
                schema: "sch".into(),
                table: "products".into(),
            },
        ];
        let config = ShadowConfig::default();
        let pairs = shadow_targets(&originals, &config);

        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0].original.table, "orders");
        assert_eq!(pairs[0].shadow.table, "orders_rocky_shadow");
        assert_eq!(pairs[1].original.table, "products");
        assert_eq!(pairs[1].shadow.table, "products_rocky_shadow");
    }

    #[test]
    fn test_shadow_target_full_name() {
        let config = ShadowConfig::default();
        let result = shadow_target(&sample_target(), &config);

        assert_eq!(
            result.full_name(),
            "analytics.marts.fct_orders_rocky_shadow"
        );
    }

    #[test]
    fn test_schema_override_full_name() {
        let config = ShadowConfig {
            schema_override: Some("_rocky_shadow".into()),
            ..Default::default()
        };
        let result = shadow_target(&sample_target(), &config);

        assert_eq!(result.full_name(), "analytics._rocky_shadow.fct_orders");
    }

    #[test]
    fn test_default_config_serialization() {
        let config = ShadowConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: ShadowConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.suffix, "_rocky_shadow");
        assert!(deserialized.schema_override.is_none());
        assert!(deserialized.cleanup_after);
    }

    #[test]
    fn test_cleanup_after_defaults_true() {
        let config = ShadowConfig::default();
        assert!(config.cleanup_after);
    }
}
