//! BigQuery governance adapter implementing [`GovernanceAdapter`].
//!
//! BigQuery uses **labels** (not tags) and **IAM** (not SQL GRANT/REVOKE):
//!
//! - **Tags -> Labels:** `ALTER SCHEMA ... SET OPTIONS(labels=[...])` for datasets,
//!   `ALTER TABLE ... SET OPTIONS(labels=[...])` for tables. BigQuery projects
//!   (the catalog equivalent) do not support labels via SQL -- those require the
//!   Resource Manager API, so `TagTarget::Catalog` logs a warning and returns Ok.
//!
//! - **Grants -> IAM:** BigQuery uses IAM role bindings, not SQL GRANT/REVOKE.
//!   `apply_grants` / `revoke_grants` log a warning and return Ok. Actual IAM
//!   integration is a follow-up.
//!
//! - **Workspace isolation:** Not applicable to BigQuery. Returns Ok.
//!
//! ## Label restrictions
//!
//! BigQuery labels must conform to:
//! - Keys and values: lowercase `[a-z0-9_-]`, max 63 characters each.
//! - Keys must start with a letter or underscore.
//! - This module sanitizes inputs automatically via [`sanitize_label`].

use std::collections::BTreeMap;

use async_trait::async_trait;
use tracing::warn;

use rocky_core::ir::{Grant, GrantTarget};
use rocky_core::traits::{
    AdapterError, AdapterResult, GovernanceAdapter, TagTarget, WarehouseAdapter,
};
use rocky_sql::validation;

use crate::connector::BigQueryAdapter;

/// Maximum length for BigQuery label keys and values.
const MAX_LABEL_LEN: usize = 63;

/// Sanitize a string for use as a BigQuery label key or value.
///
/// Applies the following transformations:
/// 1. Lowercase the input.
/// 2. Replace any character not in `[a-z0-9_-]` with `_`.
/// 3. Truncate to 63 characters.
/// 4. For keys: ensure the first character is a letter or underscore
///    (prepend `_` if it starts with a digit or hyphen).
pub fn sanitize_label(value: &str, is_key: bool) -> String {
    let lowered = value.to_lowercase();
    let mut sanitized: String = lowered
        .chars()
        .map(|c| {
            if c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect();

    if is_key && !sanitized.is_empty() {
        let first = sanitized.as_bytes()[0];
        if !(first.is_ascii_lowercase() || first == b'_') {
            sanitized.insert(0, '_');
        }
    }

    sanitized.truncate(MAX_LABEL_LEN);
    sanitized
}

/// Generate `ALTER SCHEMA ... SET OPTIONS(labels=[...])` SQL for a dataset.
pub fn generate_schema_labels_sql(
    project: &str,
    dataset: &str,
    labels: &BTreeMap<String, String>,
) -> AdapterResult<String> {
    validation::validate_identifier(project).map_err(AdapterError::new)?;
    validation::validate_identifier(dataset).map_err(AdapterError::new)?;

    let label_pairs: Vec<String> = labels
        .iter()
        .map(|(k, v)| {
            let sk = sanitize_label(k, true);
            let sv = sanitize_label(v, false);
            format!("(\"{sk}\", \"{sv}\")")
        })
        .collect();

    Ok(format!(
        "ALTER SCHEMA `{project}`.`{dataset}` SET OPTIONS(labels=[{}])",
        label_pairs.join(", ")
    ))
}

/// Generate `ALTER TABLE ... SET OPTIONS(labels=[...])` SQL for a table.
pub fn generate_table_labels_sql(
    project: &str,
    dataset: &str,
    table: &str,
    labels: &BTreeMap<String, String>,
) -> AdapterResult<String> {
    validation::validate_identifier(project).map_err(AdapterError::new)?;
    validation::validate_identifier(dataset).map_err(AdapterError::new)?;
    validation::validate_identifier(table).map_err(AdapterError::new)?;

    let label_pairs: Vec<String> = labels
        .iter()
        .map(|(k, v)| {
            let sk = sanitize_label(k, true);
            let sv = sanitize_label(v, false);
            format!("(\"{sk}\", \"{sv}\")")
        })
        .collect();

    Ok(format!(
        "ALTER TABLE `{project}`.`{dataset}`.`{table}` SET OPTIONS(labels=[{}])",
        label_pairs.join(", ")
    ))
}

/// BigQuery governance adapter.
///
/// Implements [`GovernanceAdapter`] with labels for tagging and no-op stubs
/// for IAM-based grant operations and workspace isolation (not applicable).
pub struct BigQueryGovernanceAdapter<'a> {
    adapter: &'a BigQueryAdapter,
}

impl<'a> BigQueryGovernanceAdapter<'a> {
    /// Create a new BigQuery governance adapter wrapping the warehouse adapter.
    pub fn new(adapter: &'a BigQueryAdapter) -> Self {
        Self { adapter }
    }
}

#[async_trait]
impl GovernanceAdapter for BigQueryGovernanceAdapter<'_> {
    async fn set_tags(
        &self,
        target: &TagTarget,
        tags: &BTreeMap<String, String>,
    ) -> AdapterResult<()> {
        match target {
            TagTarget::Catalog(project) => {
                warn!(
                    project = project.as_str(),
                    "BigQuery projects do not support labels via SQL; \
                     use the Resource Manager API to set project labels"
                );
                Ok(())
            }
            TagTarget::Schema { catalog, schema } => {
                let sql = generate_schema_labels_sql(catalog, schema, tags)?;
                self.adapter.execute_statement(&sql).await
            }
            TagTarget::Table {
                catalog,
                schema,
                table,
            } => {
                let sql = generate_table_labels_sql(catalog, schema, table, tags)?;
                self.adapter.execute_statement(&sql).await
            }
        }
    }

    async fn get_grants(&self, _target: &GrantTarget) -> AdapterResult<Vec<Grant>> {
        warn!(
            "BigQuery uses IAM for access control; \
             SQL-based SHOW GRANTS is not available. Returning empty grants."
        );
        Ok(vec![])
    }

    async fn apply_grants(&self, _grants: &[Grant]) -> AdapterResult<()> {
        warn!(
            "BigQuery uses IAM role bindings for access control, not SQL GRANT. \
             Skipping. Configure permissions via Google Cloud IAM."
        );
        Ok(())
    }

    async fn revoke_grants(&self, _grants: &[Grant]) -> AdapterResult<()> {
        warn!(
            "BigQuery uses IAM role bindings for access control, not SQL REVOKE. \
             Skipping. Configure permissions via Google Cloud IAM."
        );
        Ok(())
    }

    async fn bind_workspace(
        &self,
        _catalog: &str,
        _workspace_id: u64,
        _binding_type: &str,
    ) -> AdapterResult<()> {
        // Workspace binding is a Databricks concept; not applicable to BigQuery.
        Ok(())
    }

    async fn set_isolation(&self, _catalog: &str, _enabled: bool) -> AdapterResult<()> {
        // Catalog isolation is a Databricks concept; not applicable to BigQuery.
        Ok(())
    }

    async fn list_workspace_bindings(&self, _catalog: &str) -> AdapterResult<Vec<(u64, String)>> {
        // BigQuery has no workspace-binding concept; reconcile sees no drift.
        Ok(vec![])
    }

    async fn remove_workspace_binding(
        &self,
        _catalog: &str,
        _workspace_id: u64,
    ) -> AdapterResult<()> {
        // BigQuery has no workspace-binding concept; silent success.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // sanitize_label
    // -----------------------------------------------------------------------

    #[test]
    fn sanitize_lowercases() {
        assert_eq!(sanitize_label("ManagedBy", true), "managedby");
    }

    #[test]
    fn sanitize_replaces_invalid_chars() {
        assert_eq!(sanitize_label("hello world!", false), "hello_world_");
    }

    #[test]
    fn sanitize_key_prepends_underscore_for_digit_start() {
        assert_eq!(sanitize_label("1abc", true), "_1abc");
    }

    #[test]
    fn sanitize_key_prepends_underscore_for_hyphen_start() {
        assert_eq!(sanitize_label("-key", true), "_-key");
    }

    #[test]
    fn sanitize_key_no_prepend_for_letter_start() {
        assert_eq!(sanitize_label("abc", true), "abc");
    }

    #[test]
    fn sanitize_key_no_prepend_for_underscore_start() {
        assert_eq!(sanitize_label("_abc", true), "_abc");
    }

    #[test]
    fn sanitize_truncates_to_63_chars() {
        let long = "a".repeat(100);
        assert_eq!(sanitize_label(&long, false).len(), MAX_LABEL_LEN);
    }

    #[test]
    fn sanitize_preserves_hyphens() {
        assert_eq!(sanitize_label("my-value", false), "my-value");
    }

    #[test]
    fn sanitize_empty_string() {
        assert_eq!(sanitize_label("", true), "");
        assert_eq!(sanitize_label("", false), "");
    }

    #[test]
    fn sanitize_value_allows_digit_start() {
        // Values (not keys) may start with a digit.
        assert_eq!(sanitize_label("1abc", false), "1abc");
    }

    // -----------------------------------------------------------------------
    // SQL generation
    // -----------------------------------------------------------------------

    #[test]
    fn schema_labels_single() {
        let mut labels = BTreeMap::new();
        labels.insert("managed_by".to_string(), "rocky".to_string());

        let sql = generate_schema_labels_sql("my_project", "my_dataset", &labels).unwrap();
        assert_eq!(
            sql,
            "ALTER SCHEMA `my_project`.`my_dataset` SET OPTIONS(labels=[(\"managed_by\", \"rocky\")])"
        );
    }

    #[test]
    fn schema_labels_multiple_sorted() {
        let mut labels = BTreeMap::new();
        labels.insert("env".to_string(), "prod".to_string());
        labels.insert("managed_by".to_string(), "rocky".to_string());

        let sql = generate_schema_labels_sql("project", "dataset", &labels).unwrap();
        // BTreeMap iterates in sorted key order.
        assert_eq!(
            sql,
            "ALTER SCHEMA `project`.`dataset` SET OPTIONS(labels=[(\"env\", \"prod\"), (\"managed_by\", \"rocky\")])"
        );
    }

    #[test]
    fn schema_labels_empty() {
        let labels = BTreeMap::new();
        let sql = generate_schema_labels_sql("project", "dataset", &labels).unwrap();
        assert_eq!(
            sql,
            "ALTER SCHEMA `project`.`dataset` SET OPTIONS(labels=[])"
        );
    }

    #[test]
    fn schema_labels_sanitizes_keys_and_values() {
        let mut labels = BTreeMap::new();
        labels.insert("Managed By".to_string(), "Rocky Engine".to_string());

        let sql = generate_schema_labels_sql("project", "dataset", &labels).unwrap();
        assert!(sql.contains("\"managed_by\""));
        assert!(sql.contains("\"rocky_engine\""));
    }

    #[test]
    fn table_labels_sql() {
        let mut labels = BTreeMap::new();
        labels.insert("tier".to_string(), "gold".to_string());

        let sql = generate_table_labels_sql("project", "dataset", "orders", &labels).unwrap();
        assert_eq!(
            sql,
            "ALTER TABLE `project`.`dataset`.`orders` SET OPTIONS(labels=[(\"tier\", \"gold\")])"
        );
    }

    #[test]
    fn schema_labels_rejects_invalid_identifier() {
        let labels = BTreeMap::new();
        let result = generate_schema_labels_sql("invalid project!", "dataset", &labels);
        assert!(result.is_err());
    }

    #[test]
    fn table_labels_rejects_invalid_identifier() {
        let labels = BTreeMap::new();
        let result = generate_table_labels_sql("project", "dataset", "bad table!", &labels);
        assert!(result.is_err());
    }
}
