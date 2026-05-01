//! BigQuery discovery adapter implementing [`DiscoveryAdapter`].
//!
//! Lists datasets in a GCP project that match a given prefix, returning
//! them as [`DiscoveredConnector`]s with their tables. Discovery and
//! the warehouse adapter share a single [`BigQueryAdapter`] so the same
//! auth + retry budget cover both surfaces — there is no separate REST
//! client to keep in sync.
//!
//! BigQuery's `INFORMATION_SCHEMA.SCHEMATA` view is region-scoped.
//! Cross-region projects need the explicit
//! `<project>.region-<location>.INFORMATION_SCHEMA.SCHEMATA` form;
//! this adapter reads the region from the underlying
//! [`BigQueryAdapter`]'s `location` so it always queries the same
//! region the warehouse adapter writes to.
//!
//! The dataset prefix is matched via SQL `STARTS_WITH` rather than
//! `LIKE 'prefix%'` because dataset names commonly contain the literal
//! `_` character which `LIKE` treats as a wildcard.

use std::sync::Arc;

use async_trait::async_trait;

use rocky_core::source::{DiscoveredConnector, DiscoveredTable, DiscoveryResult};
use rocky_core::traits::{AdapterError, AdapterResult, DiscoveryAdapter, WarehouseAdapter};
use rocky_sql::validation::{validate_gcp_project_id, validate_identifier};

use crate::connector::BigQueryAdapter;

/// BigQuery discovery adapter that lists datasets matching a prefix.
pub struct BigQueryDiscoveryAdapter {
    adapter: Arc<BigQueryAdapter>,
}

impl BigQueryDiscoveryAdapter {
    pub fn new(adapter: Arc<BigQueryAdapter>) -> Self {
        Self { adapter }
    }

    /// Build the region qualifier (e.g. `region-eu`) used in
    /// `INFORMATION_SCHEMA.SCHEMATA` references. BigQuery accepts only
    /// lowercase letters, digits, and hyphens here, so an unexpected
    /// character is treated as an invalid configuration error rather
    /// than silently embedded into SQL.
    fn region_qualifier(&self) -> AdapterResult<String> {
        let region = format!("region-{}", self.adapter.location().to_lowercase());
        if region.is_empty()
            || region == "region-"
            || !region
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        {
            return Err(AdapterError::msg(format!(
                "invalid BigQuery location for INFORMATION_SCHEMA query: '{}'",
                self.adapter.location()
            )));
        }
        Ok(region)
    }
}

#[async_trait]
impl DiscoveryAdapter for BigQueryDiscoveryAdapter {
    async fn discover(&self, schema_prefix: &str) -> AdapterResult<DiscoveryResult> {
        let project = self.adapter.project_id();
        validate_gcp_project_id(project).map_err(AdapterError::new)?;
        let region = self.region_qualifier()?;

        // SQL string-literal escape. The prefix is user input; `'`
        // would otherwise close the literal. `STARTS_WITH` makes
        // wildcard characters (`_`, `%`) safe — they're treated as
        // literals.
        let prefix_lit = schema_prefix.replace('\'', "''");

        let schemas_sql = format!(
            "SELECT schema_name FROM `{project}.{region}.INFORMATION_SCHEMA.SCHEMATA` \
             WHERE STARTS_WITH(schema_name, '{prefix_lit}') \
             ORDER BY schema_name"
        );

        let schema_result = self.adapter.execute_query(&schemas_sql).await?;

        let mut connectors = Vec::new();
        for row in &schema_result.rows {
            let schema = match row.first().and_then(|v| v.as_str()) {
                Some(s) => s.to_string(),
                None => continue,
            };

            // Per-dataset table list. `INFORMATION_SCHEMA.TABLES` lives
            // under the dataset itself in BigQuery (four-part name),
            // not at the project level — same shape `list_tables`
            // already uses on the warehouse adapter.
            validate_identifier(&schema).map_err(AdapterError::new)?;
            let tables_sql = format!(
                "SELECT table_name FROM `{project}`.`{schema}`.INFORMATION_SCHEMA.TABLES \
                 WHERE table_type IN ('BASE TABLE', 'EXTERNAL') \
                 ORDER BY table_name"
            );

            let table_result = self.adapter.execute_query(&tables_sql).await?;

            let tables: Vec<DiscoveredTable> = table_result
                .rows
                .iter()
                .filter_map(|r| r.first().and_then(|v| v.as_str()).map(String::from))
                .map(|name| DiscoveredTable {
                    name,
                    row_count: None,
                })
                .collect();

            connectors.push(DiscoveredConnector {
                id: schema.clone(),
                schema,
                source_type: "bigquery".to_string(),
                last_sync_at: None,
                tables,
                metadata: Default::default(),
            });
        }

        // `INFORMATION_SCHEMA` queries are atomic per call — a partial
        // result here would mean the dataset listing succeeded but
        // table enumeration for one dataset failed. Today that throws
        // out of `execute_query` and the whole `discover` returns an
        // error; surfacing per-dataset partial failures is a future
        // refinement (mirrors Fivetran's `failed` slot).
        Ok(DiscoveryResult::ok(connectors))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::BigQueryAuth;

    fn fake_adapter(location: &str) -> Arc<BigQueryAdapter> {
        // BigQueryAuth::Bearer doesn't make a network call at
        // construction; safe to use in unit tests that exercise pure
        // logic on the adapter (region qualifier, etc.).
        Arc::new(BigQueryAdapter::new(
            "rocky-sandbox-hc-test-63874".to_string(),
            location.to_string(),
            BigQueryAuth::Bearer(rocky_core::redacted::RedactedString::new(
                "test-token".to_string(),
            )),
        ))
    }

    #[test]
    fn region_qualifier_accepts_eu() {
        let disc = BigQueryDiscoveryAdapter::new(fake_adapter("EU"));
        assert_eq!(disc.region_qualifier().unwrap(), "region-eu");
    }

    #[test]
    fn region_qualifier_accepts_multi_part_location() {
        let disc = BigQueryDiscoveryAdapter::new(fake_adapter("us-east1"));
        assert_eq!(disc.region_qualifier().unwrap(), "region-us-east1");
    }

    #[test]
    fn region_qualifier_rejects_injection_attempt() {
        // Anything outside `[a-z0-9-]` is treated as misconfiguration
        // rather than embedded into SQL.
        let disc = BigQueryDiscoveryAdapter::new(fake_adapter("US`; DROP TABLE x; --"));
        assert!(disc.region_qualifier().is_err());
    }

    #[test]
    fn region_qualifier_rejects_empty_location() {
        let disc = BigQueryDiscoveryAdapter::new(fake_adapter(""));
        assert!(disc.region_qualifier().is_err());
    }
}
