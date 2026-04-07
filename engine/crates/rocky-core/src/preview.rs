//! Virtual preview environments for safe change testing.
//!
//! Extends Rocky's shadow mode (`_rocky_shadow` tables) to a full
//! preview environment concept. Preview environments create isolated
//! shadow schemas for a branch, execute models there, and diff the
//! results against production.
//!
//! This is similar to SQLMesh's "virtual environments" — you can
//! preview changes without touching production tables.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Configuration for a preview environment.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PreviewConfig {
    /// Branch name or PR number — used to namespace the preview schema.
    pub branch: String,
    /// Suffix for preview schemas (default: `_preview`).
    #[serde(default = "default_preview_suffix")]
    pub suffix: String,
    /// Maximum age of preview schemas before auto-cleanup (hours).
    /// Default: 72 (3 days).
    #[serde(default = "default_ttl_hours")]
    pub ttl_hours: u64,
    /// Whether to run data quality checks in the preview environment.
    #[serde(default = "default_true")]
    pub run_checks: bool,
    /// Whether to diff preview results against production.
    #[serde(default = "default_true")]
    pub diff_enabled: bool,
    /// Maximum rows to sample for diff comparison.
    #[serde(default = "default_diff_sample_size")]
    pub diff_sample_size: usize,
}

fn default_preview_suffix() -> String {
    "_preview".to_string()
}

fn default_ttl_hours() -> u64 {
    72
}

fn default_true() -> bool {
    true
}

fn default_diff_sample_size() -> usize {
    10000
}

impl Default for PreviewConfig {
    fn default() -> Self {
        Self {
            branch: String::new(),
            suffix: default_preview_suffix(),
            ttl_hours: default_ttl_hours(),
            run_checks: true,
            diff_enabled: true,
            diff_sample_size: default_diff_sample_size(),
        }
    }
}

impl PreviewConfig {
    /// Generate the preview schema name for a given production schema.
    ///
    /// Example: `"silver__orders"` + branch `"feat-123"` → `"silver__orders_preview_feat_123"`
    pub fn preview_schema(&self, production_schema: &str) -> String {
        let sanitized_branch = self.branch.replace(['/', '-', '.'], "_");
        format!(
            "{production_schema}{suffix}_{sanitized_branch}",
            suffix = self.suffix
        )
    }

    /// Generate the preview catalog name (same as production — schemas are namespaced).
    pub fn preview_catalog(&self, production_catalog: &str) -> String {
        production_catalog.to_string()
    }
}

/// Result of diffing preview vs production.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PreviewDiff {
    /// Model name.
    pub model: String,
    /// Production table reference.
    pub production: String,
    /// Preview table reference.
    pub preview: String,
    /// Summary of differences.
    pub summary: DiffSummary,
    /// Sample of differing rows (up to `diff_sample_size`).
    pub sample_diffs: Vec<RowDiff>,
}

/// Summary statistics for a preview diff.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DiffSummary {
    /// Rows only in production (deletions in preview).
    pub production_only: u64,
    /// Rows only in preview (additions).
    pub preview_only: u64,
    /// Rows in both but with different values.
    pub modified: u64,
    /// Rows identical in both.
    pub identical: u64,
    /// Schema changes (added/removed/changed columns).
    pub schema_changes: Vec<SchemaChange>,
}

/// A schema change between production and preview.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SchemaChange {
    pub column: String,
    pub change_type: SchemaChangeType,
    pub production_type: Option<String>,
    pub preview_type: Option<String>,
}

/// Type of schema change.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SchemaChangeType {
    Added,
    Removed,
    TypeChanged,
}

/// A row-level difference between production and preview.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RowDiff {
    /// Which side(s) the row exists on.
    pub side: DiffSide,
    /// Key columns for identifying the row.
    pub key: serde_json::Value,
    /// Changed column values (column name → (production, preview)).
    pub changes: Vec<ColumnDiff>,
}

/// Which side of the diff a row appears on.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DiffSide {
    ProductionOnly,
    PreviewOnly,
    Both,
}

/// A single column's diff.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ColumnDiff {
    pub column: String,
    pub production_value: Option<serde_json::Value>,
    pub preview_value: Option<serde_json::Value>,
}

/// Generate SQL to create a diff between production and preview tables.
///
/// Returns EXCEPT-based SQL that finds rows in one but not the other.
pub fn generate_diff_sql(
    production_ref: &str,
    preview_ref: &str,
    sample_size: usize,
) -> Vec<String> {
    vec![
        // Rows only in production
        format!(
            "SELECT 'production_only' AS _diff_side, * FROM (\
             SELECT * FROM {production_ref}\
             EXCEPT\
             SELECT * FROM {preview_ref}\
             ) LIMIT {sample_size}"
        ),
        // Rows only in preview
        format!(
            "SELECT 'preview_only' AS _diff_side, * FROM (\
             SELECT * FROM {preview_ref}\
             EXCEPT\
             SELECT * FROM {production_ref}\
             ) LIMIT {sample_size}"
        ),
    ]
}

/// Generate SQL to clean up expired preview schemas.
pub fn generate_cleanup_sql(catalog: &str, schema_prefix: &str, suffix: &str) -> String {
    format!(
        "SELECT schema_name FROM {catalog}.information_schema.schemata \
         WHERE schema_name LIKE '{schema_prefix}%{suffix}_%'"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_preview_schema_name() {
        let config = PreviewConfig {
            branch: "feat/add-orders".into(),
            ..Default::default()
        };
        let schema = config.preview_schema("silver__orders");
        assert_eq!(schema, "silver__orders_preview_feat_add_orders");
    }

    #[test]
    fn test_preview_schema_pr_number() {
        let config = PreviewConfig {
            branch: "pr-123".into(),
            ..Default::default()
        };
        let schema = config.preview_schema("raw__shopify");
        assert_eq!(schema, "raw__shopify_preview_pr_123");
    }

    #[test]
    fn test_diff_sql_generation() {
        let stmts = generate_diff_sql("prod.schema.table", "prod.schema_preview_main.table", 1000);
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains("production_only"));
        assert!(stmts[1].contains("preview_only"));
    }

    #[test]
    fn test_default_config() {
        let config = PreviewConfig::default();
        assert_eq!(config.suffix, "_preview");
        assert_eq!(config.ttl_hours, 72);
        assert!(config.run_checks);
        assert!(config.diff_enabled);
    }
}
