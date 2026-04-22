//! Databricks warehouse adapter implementing [`WarehouseAdapter`] and [`BatchCheckAdapter`].

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use rocky_core::ir::{ColumnInfo, TableRef};
use rocky_core::traits::{
    AdapterError, AdapterResult, BatchCheckAdapter, ExecutionStats, FreshnessResult, QueryResult,
    RowCountResult, SqlDialect, WarehouseAdapter,
};

use crate::batch::{self, BatchTableRef};
use crate::connector::DatabricksConnector;
use crate::dialect::DatabricksSqlDialect;

/// Databricks warehouse adapter wrapping [`DatabricksConnector`] behind the
/// [`WarehouseAdapter`] trait.
pub struct DatabricksWarehouseAdapter {
    connector: DatabricksConnector,
    dialect: DatabricksSqlDialect,
}

impl DatabricksWarehouseAdapter {
    pub fn new(connector: DatabricksConnector) -> Self {
        Self {
            connector,
            dialect: DatabricksSqlDialect,
        }
    }

    /// Access the underlying connector (for adapter-specific operations).
    pub fn connector(&self) -> &DatabricksConnector {
        &self.connector
    }
}

#[async_trait]
impl WarehouseAdapter for DatabricksWarehouseAdapter {
    fn dialect(&self) -> &dyn SqlDialect {
        &self.dialect
    }

    async fn execute_statement(&self, sql: &str) -> AdapterResult<()> {
        self.connector
            .execute_statement(sql)
            .await
            .map(|_| ())
            .map_err(AdapterError::new)
    }

    async fn execute_statement_with_stats(&self, sql: &str) -> AdapterResult<ExecutionStats> {
        // `total_byte_count` from the Databricks manifest is the
        // byte count Databricks natively reports for a statement —
        // surfaced here in the `bytes_scanned` slot to match the
        // BigQuery `totalBytesBilled` convention set in PR #219.
        // Databricks is DBU-priced (not bytes-priced), so this
        // figure isn't a cost driver today — it's still threaded
        // through so downstream observability has a real number.
        self.connector
            .execute_statement_with_stats(sql)
            .await
            .map_err(AdapterError::new)
    }

    async fn execute_query(&self, sql: &str) -> AdapterResult<QueryResult> {
        let result = self
            .connector
            .execute_sql(sql)
            .await
            .map_err(AdapterError::new)?;
        Ok(QueryResult {
            columns: result.columns.iter().map(|c| c.name.clone()).collect(),
            rows: result.rows,
        })
    }

    async fn describe_table(&self, table: &TableRef) -> AdapterResult<Vec<ColumnInfo>> {
        let catalog_mgr = crate::catalog::CatalogManager::new(&self.connector);
        catalog_mgr
            .describe_table(table)
            .await
            .map_err(AdapterError::new)
    }

    async fn list_tables(&self, catalog: &str, schema: &str) -> AdapterResult<Vec<String>> {
        let catalog_mgr = crate::catalog::CatalogManager::new(&self.connector);
        catalog_mgr
            .list_tables(catalog, schema)
            .await
            .map(|tables| tables.into_iter().map(|t| t.to_lowercase()).collect())
            .map_err(AdapterError::new)
    }
}

// ---------------------------------------------------------------------------
// Batch check adapter
// ---------------------------------------------------------------------------

/// Databricks batch check adapter using UNION ALL query batching.
pub struct DatabricksBatchCheckAdapter {
    connector: Arc<DatabricksConnector>,
}

impl DatabricksBatchCheckAdapter {
    pub fn new(connector: Arc<DatabricksConnector>) -> Self {
        Self { connector }
    }
}

fn table_refs_to_batch(tables: &[TableRef]) -> Vec<BatchTableRef> {
    tables
        .iter()
        .map(|t| BatchTableRef {
            catalog: t.catalog.clone(),
            schema: t.schema.clone(),
            table: t.table.clone(),
        })
        .collect()
}

#[async_trait]
impl BatchCheckAdapter for DatabricksBatchCheckAdapter {
    async fn batch_row_counts(&self, tables: &[TableRef]) -> AdapterResult<Vec<RowCountResult>> {
        let batch_refs = table_refs_to_batch(tables);
        let results = batch::execute_batch_row_counts(&self.connector, &batch_refs)
            .await
            .map_err(AdapterError::new)?;

        Ok(results
            .into_iter()
            .map(|r| RowCountResult {
                table: TableRef {
                    catalog: r.catalog,
                    schema: r.schema,
                    table: r.table,
                },
                count: r.count,
            })
            .collect())
    }

    async fn batch_freshness(
        &self,
        tables: &[TableRef],
        timestamp_col: &str,
    ) -> AdapterResult<Vec<FreshnessResult>> {
        let batch_refs = table_refs_to_batch(tables);
        let results = batch::execute_batch_freshness(&self.connector, &batch_refs, timestamp_col)
            .await
            .map_err(AdapterError::new)?;

        Ok(results
            .into_iter()
            .map(|r| {
                // Databricks returns timestamps as strings via `CAST(... AS STRING)`.
                // Accept RFC 3339 first, then fall back to the Databricks default
                // format "YYYY-MM-DD HH:MM:SS[.fff]" — matches the parse logic
                // previously inlined in run.rs before this dispatch was lifted
                // behind the BatchCheckAdapter trait.
                let max_timestamp = r.max_timestamp.and_then(|ts| {
                    ts.parse::<DateTime<Utc>>().ok().or_else(|| {
                        chrono::NaiveDateTime::parse_from_str(&ts, "%Y-%m-%d %H:%M:%S%.f")
                            .or_else(|_| {
                                chrono::NaiveDateTime::parse_from_str(&ts, "%Y-%m-%d %H:%M:%S")
                            })
                            .ok()
                            .map(|naive| naive.and_utc())
                    })
                });
                FreshnessResult {
                    table: TableRef {
                        catalog: r.catalog,
                        schema: r.schema,
                        table: r.table,
                    },
                    max_timestamp,
                }
            })
            .collect())
    }

    async fn batch_describe_schema(
        &self,
        catalog: &str,
        schema: &str,
    ) -> AdapterResult<std::collections::HashMap<String, Vec<ColumnInfo>>> {
        batch::execute_batch_describe(&self.connector, catalog, schema)
            .await
            .map_err(AdapterError::new)
    }
}
