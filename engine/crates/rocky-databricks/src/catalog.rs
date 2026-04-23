use std::collections::BTreeMap;

use rocky_core::catalog as catalog_sql;
use rocky_core::ir::TableRef;
use tracing::debug;

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
