use std::collections::BTreeMap;

use rocky_core::catalog as catalog_sql;
use rocky_core::ir::TableRef;
use tracing::{debug, warn};

use crate::connector::{ConnectorError, DatabricksConnector};

/// Manages Unity Catalog lifecycle: create catalogs/schemas, set tags, discover.
pub struct CatalogManager<'a> {
    connector: &'a DatabricksConnector,
}

impl<'a> CatalogManager<'a> {
    pub fn new(connector: &'a DatabricksConnector) -> Self {
        CatalogManager { connector }
    }

    /// Creates a catalog if it doesn't exist.
    pub async fn create_catalog(&self, catalog: &str) -> Result<(), CatalogManagerError> {
        let sql = catalog_sql::generate_create_catalog_sql(catalog)?;
        debug!(catalog, "creating catalog");
        self.connector.execute_statement(&sql).await?;
        Ok(())
    }

    /// Creates a schema if it doesn't exist.
    pub async fn create_schema(
        &self,
        catalog: &str,
        schema: &str,
    ) -> Result<(), CatalogManagerError> {
        let sql = catalog_sql::generate_create_schema_sql(catalog, schema)?;
        debug!(catalog, schema, "creating schema");
        self.connector.execute_statement(&sql).await?;
        Ok(())
    }

    /// Sets tags on a catalog.
    pub async fn set_catalog_tags(
        &self,
        catalog: &str,
        tags: &BTreeMap<String, String>,
    ) -> Result<(), CatalogManagerError> {
        let sql = catalog_sql::generate_set_catalog_tags_sql(catalog, tags)?;
        debug!(catalog, "setting catalog tags");
        self.connector.execute_statement(&sql).await?;
        Ok(())
    }

    /// Sets tags on a schema.
    pub async fn set_schema_tags(
        &self,
        catalog: &str,
        schema: &str,
        tags: &BTreeMap<String, String>,
    ) -> Result<(), CatalogManagerError> {
        let sql = catalog_sql::generate_set_schema_tags_sql(catalog, schema, tags)?;
        debug!(catalog, schema, "setting schema tags");
        self.connector.execute_statement(&sql).await?;
        Ok(())
    }

    /// Sets tags on a table.
    pub async fn set_table_tags(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        tags: &BTreeMap<String, String>,
    ) -> Result<(), CatalogManagerError> {
        let sql = catalog_sql::generate_set_table_tags_sql(catalog, schema, table, tags)?;
        debug!(catalog, schema, table, "setting table tags");
        self.connector.execute_statement(&sql).await?;
        Ok(())
    }

    /// Sets tags on a single column of a table. Empty tag maps are skipped
    /// (Unity Catalog rejects `SET TAGS ()`). One statement per column —
    /// Databricks does not support multi-column tag application in a
    /// single DDL.
    pub async fn set_column_tags(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        column: &str,
        tags: &BTreeMap<String, String>,
    ) -> Result<(), CatalogManagerError> {
        let maybe_sql =
            catalog_sql::generate_set_column_tags_sql(catalog, schema, table, column, tags)?;
        if let Some(sql) = maybe_sql {
            debug!(catalog, schema, table, column, "setting column tags");
            self.connector.execute_statement(&sql).await?;
        }
        Ok(())
    }

    /// Sets Delta Lake time-travel retention on a table by updating the pair
    /// of `TBLPROPERTIES` (`delta.logRetentionDuration` +
    /// `delta.deletedFileRetentionDuration`) in a single ALTER TABLE.
    ///
    /// Used by [`GovernanceAdapter::apply_retention_policy`] on Databricks.
    ///
    /// [`GovernanceAdapter::apply_retention_policy`]: rocky_core::traits::GovernanceAdapter::apply_retention_policy
    pub async fn set_delta_retention(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        duration_days: u32,
    ) -> Result<(), CatalogManagerError> {
        let sql =
            catalog_sql::generate_set_delta_retention_sql(catalog, schema, table, duration_days)?;
        debug!(
            catalog,
            schema, table, duration_days, "setting Delta retention properties"
        );
        self.connector.execute_statement(&sql).await?;
        Ok(())
    }

    /// Probes the paired Delta retention properties on a table.
    ///
    /// Issues `SHOW TBLPROPERTIES <cat>.<schema>.<table> (
    /// 'delta.logRetentionDuration', 'delta.deletedFileRetentionDuration')` and
    /// parses the returned `"interval N days"` / `"N days"` value strings into
    /// a `u32` day count.
    ///
    /// Prefers `delta.deletedFileRetentionDuration` (the VACUUM-eligibility
    /// knob, which is what user-facing retention semantics actually hinge
    /// on); falls back to `delta.logRetentionDuration` when only the log
    /// property is set. If neither is set on the table, returns `Ok(None)`
    /// — the default Delta values are not read back here, since the
    /// user-facing semantics are "did Rocky (or someone) override the
    /// default?". If both are set to different values, prefers
    /// `delta.deletedFileRetentionDuration` and emits a `warn!` trace event
    /// so the divergence is observable without aborting a read.
    ///
    /// Used by
    /// [`GovernanceAdapter::read_retention_days`](rocky_core::traits::GovernanceAdapter::read_retention_days)
    /// on Databricks to back `rocky retention-status --drift`.
    pub async fn get_delta_retention_days(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Result<Option<u32>, CatalogManagerError> {
        let sql = catalog_sql::generate_show_delta_retention_sql(catalog, schema, table)?;
        debug!(catalog, schema, table, "probing Delta retention properties");
        let result = self.connector.execute_sql(&sql).await?;
        Ok(parse_delta_retention_rows(&result.rows))
    }

    /// Lists schemas in a catalog.
    pub async fn list_schemas(&self, catalog: &str) -> Result<Vec<String>, CatalogManagerError> {
        let sql = catalog_sql::generate_show_schemas_sql(catalog)?;
        let result = self.connector.execute_sql(&sql).await?;

        let schemas = result
            .rows
            .iter()
            .filter_map(|row| {
                row.first()
                    .and_then(|v| v.as_str())
                    .map(std::string::ToString::to_string)
            })
            .collect();
        Ok(schemas)
    }

    /// Lists tables in a schema via `information_schema.tables`.
    pub async fn list_tables(
        &self,
        catalog: &str,
        schema: &str,
    ) -> Result<Vec<String>, CatalogManagerError> {
        let sql = catalog_sql::generate_list_tables_sql(catalog, schema)?;
        let result = self.connector.execute_sql(&sql).await?;

        let tables = result
            .rows
            .iter()
            .filter_map(|row| {
                row.first()
                    .and_then(|v| v.as_str())
                    .map(std::string::ToString::to_string)
            })
            .collect();
        Ok(tables)
    }

    /// Discovers catalogs managed by a specific tag.
    pub async fn discover_managed_catalogs(
        &self,
        tag_name: &str,
        tag_value: &str,
    ) -> Result<Vec<String>, CatalogManagerError> {
        let sql = catalog_sql::generate_discover_managed_catalogs_sql(tag_name, tag_value)?;
        let result = self.connector.execute_sql(&sql).await?;

        let catalogs = result
            .rows
            .iter()
            .filter_map(|row| {
                row.first()
                    .and_then(|v| v.as_str())
                    .map(std::string::ToString::to_string)
            })
            .collect();
        Ok(catalogs)
    }

    /// Describes a table's columns via `DESCRIBE TABLE`.
    pub async fn describe_table(
        &self,
        table: &TableRef,
    ) -> Result<Vec<rocky_core::ir::ColumnInfo>, CatalogManagerError> {
        let dialect = crate::dialect::DatabricksSqlDialect;
        let sql = rocky_core::drift::generate_describe_table_sql(table, &dialect)
            .map_err(SqlGenError::from)?;
        let result = self.connector.execute_sql(&sql).await?;

        let columns = result
            .rows
            .iter()
            .filter_map(|row| {
                let name = row.first().and_then(|v| v.as_str())?.to_string();
                let data_type = row.get(1).and_then(|v| v.as_str())?.to_string();
                // Skip partition/metadata separator rows (empty name or starts with #)
                if name.is_empty() || name.starts_with('#') {
                    return None;
                }
                Some(rocky_core::ir::ColumnInfo {
                    name,
                    data_type,
                    nullable: true, // DESCRIBE TABLE doesn't reliably report nullability
                })
            })
            .collect();
        Ok(columns)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CatalogManagerError {
    #[error("SQL generation error: {0}")]
    SqlGen(#[from] catalog_sql::CatalogError),

    #[error("SQL generation error: {0}")]
    SqlGenCore(#[from] SqlGenError),

    #[error("connector error: {0}")]
    Connector(#[from] ConnectorError),
}

// Bridge for drift module's SqlGenError
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct SqlGenError(#[from] rocky_core::sql_gen::SqlGenError);

/// Parses the two-column `SHOW TBLPROPERTIES` response into a `u32` day count.
///
/// Accepts any `"interval N days"` / `"N days"` / `"N.000000000 days"`
/// form Databricks has emitted across Delta runtime versions — the parser
/// scans whitespace-split tokens and takes the first integer-parseable one.
///
/// Prefers `delta.deletedFileRetentionDuration` when both are present; if
/// they disagree, emits a `warn!` so operators can see the divergence
/// without aborting the read (see
/// [`CatalogManager::get_delta_retention_days`] for the rationale).
fn parse_delta_retention_rows(rows: &[Vec<serde_json::Value>]) -> Option<u32> {
    let mut log_days: Option<u32> = None;
    let mut deleted_days: Option<u32> = None;
    for row in rows {
        let key = row.first().and_then(|v| v.as_str()).unwrap_or_default();
        let value = row.get(1).and_then(|v| v.as_str()).unwrap_or_default();
        let Some(days) = parse_delta_duration_days(value) else {
            continue;
        };
        match key {
            "delta.logRetentionDuration" => log_days = Some(days),
            "delta.deletedFileRetentionDuration" => deleted_days = Some(days),
            _ => {}
        }
    }
    match (log_days, deleted_days) {
        (Some(l), Some(d)) if l != d => {
            warn!(
                log_days = l,
                deleted_file_days = d,
                "delta retention properties diverge; preferring deletedFileRetentionDuration"
            );
            Some(d)
        }
        (_, Some(d)) => Some(d),
        (Some(l), None) => Some(l),
        (None, None) => None,
    }
}

/// Extracts the integer day count from a Delta duration string.
///
/// Handles the three forms seen in the wild across DBR versions:
/// - `"interval 90 days"`
/// - `"90 days"`
/// - `"90.000000000 days"`
///
/// Returns the first whitespace-split token that parses as `u32` (trimming
/// a trailing `.0*` if present to handle the decimal form). Returns `None`
/// if no token parses — the caller treats that as "no observation".
fn parse_delta_duration_days(value: &str) -> Option<u32> {
    for tok in value.split_whitespace() {
        // Handle the `"90.000000000"` decimal form by truncating at `.`
        // — Delta has always meant integer days here.
        let tok_int = tok.split('.').next().unwrap_or(tok);
        if let Ok(days) = tok_int.parse::<u32>() {
            return Some(days);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_duration_accepts_interval_form() {
        assert_eq!(parse_delta_duration_days("interval 90 days"), Some(90));
    }

    #[test]
    fn parse_duration_accepts_bare_form() {
        assert_eq!(parse_delta_duration_days("90 days"), Some(90));
    }

    #[test]
    fn parse_duration_accepts_decimal_form() {
        // Some DBR versions emit "90.000000000 days".
        assert_eq!(parse_delta_duration_days("90.000000000 days"), Some(90));
    }

    #[test]
    fn parse_duration_returns_none_for_garbage() {
        assert_eq!(parse_delta_duration_days(""), None);
        assert_eq!(parse_delta_duration_days("days"), None);
        assert_eq!(parse_delta_duration_days("interval weeks"), None);
    }

    #[test]
    fn parse_rows_prefers_deleted_file_over_log() {
        let rows = vec![
            vec![
                json!("delta.logRetentionDuration"),
                json!("interval 30 days"),
            ],
            vec![
                json!("delta.deletedFileRetentionDuration"),
                json!("interval 90 days"),
            ],
        ];
        // Diverging values: prefer deletedFileRetentionDuration (the
        // VACUUM-eligibility knob).
        assert_eq!(parse_delta_retention_rows(&rows), Some(90));
    }

    #[test]
    fn parse_rows_falls_back_to_log_when_only_log_set() {
        let rows = vec![vec![
            json!("delta.logRetentionDuration"),
            json!("interval 30 days"),
        ]];
        assert_eq!(parse_delta_retention_rows(&rows), Some(30));
    }

    #[test]
    fn parse_rows_returns_none_when_neither_set() {
        // Neither property in the result rows (e.g., SHOW TBLPROPERTIES on a
        // non-Delta table, or a Delta table with no overrides).
        let rows: Vec<Vec<serde_json::Value>> = vec![];
        assert_eq!(parse_delta_retention_rows(&rows), None);
    }

    #[test]
    fn parse_rows_ignores_unrelated_properties() {
        // Extra properties shouldn't confuse the filter (paranoia — the SQL
        // explicitly scopes to the two keys, but guard against loose impls).
        let rows = vec![
            vec![json!("delta.enableDeletionVectors"), json!("true")],
            vec![
                json!("delta.deletedFileRetentionDuration"),
                json!("interval 120 days"),
            ],
        ];
        assert_eq!(parse_delta_retention_rows(&rows), Some(120));
    }

    #[test]
    fn parse_rows_handles_both_equal() {
        // Common case: Rocky writes both, reads both, they agree.
        let rows = vec![
            vec![
                json!("delta.logRetentionDuration"),
                json!("interval 90 days"),
            ],
            vec![
                json!("delta.deletedFileRetentionDuration"),
                json!("interval 90 days"),
            ],
        ];
        assert_eq!(parse_delta_retention_rows(&rows), Some(90));
    }
}
