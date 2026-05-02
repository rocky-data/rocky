//! Databricks warehouse adapter implementing [`WarehouseAdapter`] and [`BatchCheckAdapter`].

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use rocky_core::ir::{ColumnInfo, TableRef};
use rocky_core::traits::{
    AdapterError, AdapterResult, BatchCheckAdapter, ChunkChecksum, ExecutionStats, FreshnessResult,
    PkRange, QueryResult, RowCountResult, SqlDialect, WarehouseAdapter,
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

    /// Databricks override of the checksum-bisection chunk-checksum
    /// query. The kernel default in `rocky-core` uses double-quoted
    /// identifiers (`"id"`), which Databricks treats as a STRING
    /// literal under `ANSI_MODE = off` (and even under ANSI mode the
    /// implicit cast can confuse the type-checker on numeric
    /// comparisons). This override quotes columns with backticks,
    /// emits Spark's `BIGINT` cast (matching the dialect's native
    /// integer type), and composes the per-row hash with
    /// `BIT_XOR(<dialect.row_hash_expr>)`.
    ///
    /// The `LEAST(K-1, FLOOR(...))` clamp matches the kernel default
    /// — `split_int_range` lets the last chunk absorb the integer-
    /// step truncation remainder, so the SQL bucketing has to pin the
    /// highest pk values back into chunk K-1 instead of overflowing
    /// to K.
    async fn checksum_chunks(
        &self,
        table: &TableRef,
        pk_column: &str,
        value_columns: &[String],
        pk_ranges: &[PkRange],
    ) -> AdapterResult<Vec<ChunkChecksum>> {
        if pk_ranges.is_empty() {
            return Ok(Vec::new());
        }
        let (lo_min, hi_max, k) = databricks_uniform_int_window(pk_ranges)?;
        let step = (hi_max - lo_min) / (k as i128);
        if step <= 0 {
            return Err(AdapterError::msg(
                "Databricks checksum_chunks requires a positive integer step \
                 across the chunk window (lo == hi or hi < lo)",
            ));
        }

        rocky_sql::validation::validate_identifier(pk_column).map_err(AdapterError::new)?;
        for col in value_columns {
            rocky_sql::validation::validate_identifier(col).map_err(AdapterError::new)?;
        }

        let table_ref =
            self.dialect
                .format_table_ref(&table.catalog, &table.schema, &table.table)?;
        let row_hash = self.dialect.row_hash_expr(value_columns)?;
        let last_id = k - 1;

        let sql = format!(
            "SELECT chunk_id, COUNT(*) AS row_count, BIT_XOR({row_hash}) AS chk \
             FROM ( \
                 SELECT *, \
                        LEAST( \
                            CAST(FLOOR((CAST(`{pk_column}` AS DOUBLE) - {lo}) / {step}) AS BIGINT), \
                            {last_id} \
                        ) AS chunk_id \
                 FROM {table_ref} \
                 WHERE `{pk_column}` IS NOT NULL \
                   AND `{pk_column}` >= {lo} \
                   AND `{pk_column}` < {hi} \
             ) AS chunked \
             GROUP BY chunk_id \
             ORDER BY chunk_id",
            lo = lo_min,
            hi = hi_max,
        );

        let result = self.execute_query(&sql).await?;
        parse_databricks_chunk_checksums(&result, k)
    }

    /// Override: emit `SHALLOW CLONE` instead of CTAS. Branch table lands in
    /// `<source.catalog>.<branch_schema>.<source.table>` — same catalog as the
    /// source, since the trait surface scopes branches by schema only.
    async fn clone_table_for_branch(
        &self,
        source: &TableRef,
        branch_schema: &str,
    ) -> AdapterResult<()> {
        rocky_sql::validation::validate_identifier(&source.catalog).map_err(AdapterError::new)?;
        rocky_sql::validation::validate_identifier(&source.schema).map_err(AdapterError::new)?;
        rocky_sql::validation::validate_identifier(&source.table).map_err(AdapterError::new)?;
        rocky_sql::validation::validate_identifier(branch_schema).map_err(AdapterError::new)?;

        let catalog = &source.catalog;
        let src_schema = &source.schema;
        let table = &source.table;
        let sql = format!(
            "CREATE OR REPLACE TABLE {catalog}.{branch_schema}.{table} \
             SHALLOW CLONE {catalog}.{src_schema}.{table}"
        );
        self.execute_statement(&sql).await
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

/// Verify the bisection runner passed K contiguous IntRange chunks and
/// return `(lo_min, hi_max, K)` for SQL interpolation. Mirrors the
/// kernel default's `uniform_int_range_window`; composite + hash-bucket
/// strategies are out of scope and surface as an explicit error.
fn databricks_uniform_int_window(pk_ranges: &[PkRange]) -> AdapterResult<(i128, i128, u32)> {
    let mut lo_min: Option<i128> = None;
    let mut hi_max: Option<i128> = None;
    for range in pk_ranges {
        match range {
            PkRange::IntRange { lo, hi } => {
                lo_min = Some(lo_min.map_or(*lo, |x| x.min(*lo)));
                hi_max = Some(hi_max.map_or(*hi, |x| x.max(*hi)));
            }
            PkRange::Composite { .. } | PkRange::HashBucket { .. } => {
                return Err(AdapterError::msg(
                    "Databricks checksum_chunks supports IntRange only; \
                     composite and hash-bucket strategies require follow-up changes",
                ));
            }
        }
    }
    let lo = lo_min.expect("non-empty pk_ranges checked by caller");
    let hi = hi_max.expect("non-empty pk_ranges checked by caller");
    let k = u32::try_from(pk_ranges.len())
        .map_err(|_| AdapterError::msg("checksum_chunks accepts up to u32::MAX chunks per call"))?;
    Ok((lo, hi, k))
}

/// Parse a Databricks `(chunk_id, row_count, chk)` result set into
/// [`ChunkChecksum`] rows. Databricks's connector returns every cell
/// as a `serde_json::Value` (string for BIGINT, number for smaller
/// ints); the parser handles both shapes.
fn parse_databricks_chunk_checksums(
    result: &QueryResult,
    k: u32,
) -> AdapterResult<Vec<ChunkChecksum>> {
    let mut out = Vec::with_capacity(result.rows.len());
    for row in &result.rows {
        if row.len() < 3 {
            return Err(AdapterError::msg(
                "checksum_chunks query returned a row with fewer than 3 columns",
            ));
        }
        let chunk_id_raw = parse_databricks_i128(&row[0])?;
        if chunk_id_raw < 0 || chunk_id_raw >= i128::from(k) {
            return Err(AdapterError::msg(format!(
                "checksum_chunks returned chunk_id={chunk_id_raw}, outside [0, {k})"
            )));
        }
        let row_count_raw = parse_databricks_i128(&row[1])?;
        let row_count = u64::try_from(row_count_raw).map_err(|_| {
            AdapterError::msg(format!(
                "checksum_chunks returned row_count={row_count_raw}, expected non-negative u64"
            ))
        })?;
        let checksum = parse_databricks_i128(&row[2])? as u128;
        out.push(ChunkChecksum {
            chunk_id: chunk_id_raw as u32,
            row_count,
            checksum,
        });
    }
    Ok(out)
}

fn parse_databricks_i128(v: &serde_json::Value) -> AdapterResult<i128> {
    if let Some(s) = v.as_str() {
        return s.parse::<i128>().map_err(|e| {
            AdapterError::msg(format!(
                "failed to parse {s:?} as i128 from Databricks result: {e}"
            ))
        });
    }
    if let Some(n) = v.as_i64() {
        return Ok(n.into());
    }
    if let Some(n) = v.as_u64() {
        return Ok(n.into());
    }
    Err(AdapterError::msg(format!(
        "Databricks checksum_chunks result column had unexpected JSON shape: {v:?}"
    )))
}
