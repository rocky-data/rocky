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
use rocky_core::sql_gen::string_literal;
use rocky_core::traits::{AdapterError, AdapterResult, DiscoveryAdapter, WarehouseAdapter};
use rocky_sql::validation::{validate_gcp_project_id, validate_identifier};

use crate::connector::BigQueryAdapter;
use crate::dialect::BigQueryDialect;

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

/// The `INFORMATION_SCHEMA.SCHEMATA` query for every dataset whose name
/// starts with `schema_prefix`.
///
/// `project` and `region` are validated by the caller. The prefix is user
/// text from `schema_pattern.prefix`, encoded by BigQuery's own lexer rule
/// (it reads backslash escapes, and `''` is a syntax error there rather
/// than an escaped quote). `STARTS_WITH` reads the result as plain text, so
/// `_` and `%` need no `LIKE` treatment.
fn schemata_sql(project: &str, region: &str, schema_prefix: &str) -> String {
    let prefix_lit = string_literal(&BigQueryDialect, schema_prefix);
    format!(
        "SELECT schema_name FROM `{project}.{region}.INFORMATION_SCHEMA.SCHEMATA` \
         WHERE STARTS_WITH(schema_name, {prefix_lit}) \
         ORDER BY schema_name"
    )
}

#[async_trait]
impl DiscoveryAdapter for BigQueryDiscoveryAdapter {
    async fn discover(&self, schema_prefix: &str) -> AdapterResult<DiscoveryResult> {
        let project = self.adapter.project_id();
        validate_gcp_project_id(project).map_err(AdapterError::new)?;
        let region = self.region_qualifier()?;

        let schemas_sql = schemata_sql(project, &region, schema_prefix);

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
                external_object_ids: Vec::new(),
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
            "test-project".to_string(),
            location.to_string(),
            BigQueryAuth::Bearer(rocky_core::redacted::RedactedString::new(
                "test-token".to_string(),
            )),
        ))
    }

    /// The prefix is encoded by BigQuery's own lexer rule (issue #1596):
    /// its lexer reads backslash escapes, so a backslash is doubled and a
    /// quote is `\'` — and, per the GoogleSQL lexical spec, `''` would be a
    /// syntax error there rather than an escaped quote.
    #[test]
    fn schemata_sql_encodes_the_prefix_by_bigquerys_lexer_rule() {
        assert_eq!(
            schemata_sql("p", "region-eu", r"raw\"),
            r"SELECT schema_name FROM `p.region-eu.INFORMATION_SCHEMA.SCHEMATA` WHERE STARTS_WITH(schema_name, 'raw\\') ORDER BY schema_name"
        );
        assert!(
            schemata_sql("p", "region-eu", "it's").contains(r"STARTS_WITH(schema_name, 'it\'s')")
        );
    }

    /// A prefix without a quote or a backslash is spliced byte-identically to
    /// the pre-encoder form.
    #[test]
    fn schemata_sql_plain_prefix_is_byte_identical() {
        assert_eq!(
            schemata_sql("p", "region-eu", "src__"),
            "SELECT schema_name FROM `p.region-eu.INFORMATION_SCHEMA.SCHEMATA` WHERE STARTS_WITH(schema_name, 'src__') ORDER BY schema_name"
        );
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
