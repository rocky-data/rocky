//! Databricks warehouse adapter implementing [`WarehouseAdapter`] and [`BatchCheckAdapter`].

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use rocky_catalog_core::{
    CatalogClient, CatalogError, TableRef as CatalogTableRef, TableSchema as CatalogTableSchema,
};
use rocky_core::traits::{
    AdapterError, AdapterResult, BatchCheckAdapter, ChunkChecksum, ExecutionStats, FreshnessResult,
    PkRange, QueryResult, RowCountResult, SqlDialect, WarehouseAdapter,
};
use rocky_ir::{ColumnInfo, TableRef};
use tracing::debug;

use crate::batch::{self, BatchTableRef};
use crate::connector::DatabricksConnector;
use crate::dialect::DatabricksSqlDialect;
use crate::unity_catalog_client::UnityCatalogClient;

/// Databricks warehouse adapter wrapping [`DatabricksConnector`] behind the
/// [`WarehouseAdapter`] trait.
///
/// An optional [`UnityCatalogClient`] can be wired in via
/// [`DatabricksWarehouseAdapter::with_catalog_client`] to route read-side
/// catalog operations (today: [`WarehouseAdapter::describe_table`] and
/// [`WarehouseAdapter::list_tables`]) through Unity's REST surface instead
/// of `DESCRIBE TABLE` / `information_schema` SQL. The SQL path remains the
/// default and the fallback whenever the REST attempt returns
/// [`CatalogError::UnsupportedOperation`] or [`CatalogError::Transport`].
pub struct DatabricksWarehouseAdapter {
    connector: DatabricksConnector,
    dialect: DatabricksSqlDialect,
    catalog_client: Option<UnityCatalogClient>,
}

impl DatabricksWarehouseAdapter {
    pub fn new(connector: DatabricksConnector) -> Self {
        Self {
            connector,
            dialect: DatabricksSqlDialect,
            catalog_client: None,
        }
    }

    /// Attach a [`UnityCatalogClient`] for REST-backed catalog reads.
    ///
    /// When set, [`WarehouseAdapter::describe_table`] and
    /// [`WarehouseAdapter::list_tables`] try the Unity REST path first and
    /// fall back to the existing SQL path on
    /// [`CatalogError::UnsupportedOperation`] or
    /// [`CatalogError::Transport`]. Any other catalog error (auth,
    /// permission, table-not-found, malformed response) is surfaced as
    /// [`AdapterError`] without falling through, so credential or shape
    /// problems on the REST side aren't silently masked.
    #[must_use]
    pub fn with_catalog_client(mut self, client: UnityCatalogClient) -> Self {
        self.catalog_client = Some(client);
        self
    }

    /// Access the underlying connector (for adapter-specific operations).
    pub fn connector(&self) -> &DatabricksConnector {
        &self.connector
    }

    /// Access the optionally-wired Unity REST catalog client.
    pub fn catalog_client(&self) -> Option<&UnityCatalogClient> {
        self.catalog_client.as_ref()
    }
}

/// Project a warehouse-side three-part [`rocky_ir::TableRef`] onto the
/// catalog-agnostic [`rocky_catalog_core::TableRef`] shape.
///
/// Unity is rigidly `catalog.schema.table`, so the IR's three fields map
/// straight onto `catalog = Some(_)`, `namespace = [schema]`, `name = table`.
/// Adapters that target multi-level Iceberg namespaces would project
/// differently; this helper is Databricks-specific.
fn ir_to_catalog_ref(table: &TableRef) -> CatalogTableRef {
    CatalogTableRef {
        catalog: Some(table.catalog.clone()),
        namespace: vec![table.schema.clone()],
        name: table.table.clone(),
    }
}

/// Project a [`CatalogTableSchema`] onto the engine's `Vec<ColumnInfo>` shape.
///
/// The trait carries `name + type_str + nullable` per column; the warehouse-
/// side `ColumnInfo` carries `name + data_type + nullable`. The projection is
/// one-to-one — no caller of [`WarehouseAdapter::describe_table`] reads any
/// REST-richer field (storage location, owner, etc.) so this lossless
/// mapping is sufficient.
fn catalog_schema_to_column_info(schema: CatalogTableSchema) -> Vec<ColumnInfo> {
    schema
        .columns
        .into_iter()
        .map(|c| ColumnInfo {
            name: c.name,
            data_type: c.type_str,
            nullable: c.nullable,
        })
        .collect()
}

/// Whether a [`CatalogError`] should fall through to the SQL path.
///
/// Only [`CatalogError::UnsupportedOperation`] and
/// [`CatalogError::Transport`] are recoverable here:
///
/// - `UnsupportedOperation` is the catalog telling us it doesn't serve this
///   surface over REST (today: any catalog that wires a stub client).
/// - `Transport` is a network / 5xx blip that the SQL path may navigate
///   around (the SQL connector has its own retry layer).
///
/// All other variants — `AuthFailed`, `PermissionDenied`, `TableNotFound`,
/// `NamespaceNotFound`, `InvalidResponse`, `CommitConflict` — are
/// deterministic catalog answers we'd rather surface than mask by silently
/// routing to SQL (e.g. a Unity-side auth misconfig should not look like a
/// successful SQL describe).
fn catalog_error_is_fallback(err: &CatalogError) -> bool {
    matches!(
        err,
        CatalogError::UnsupportedOperation(_) | CatalogError::Transport(_)
    )
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
        // Try the Unity REST path first when a catalog client is wired —
        // `GET /api/2.1/unity-catalog/tables/{full_name}` returns the column
        // list directly without paying the latency of a `DESCRIBE TABLE`
        // round-trip against the SQL warehouse. Fall back to the SQL path
        // on `UnsupportedOperation` or `Transport` errors; everything else
        // (auth, permission, table-not-found, invalid shape) surfaces as
        // `AdapterError` to avoid masking real REST-side problems.
        if let Some(client) = &self.catalog_client {
            let catalog_ref = ir_to_catalog_ref(table);
            match client.describe_table(&catalog_ref).await {
                Ok(schema) => {
                    debug!(
                        catalog = %table.catalog,
                        schema = %table.schema,
                        table = %table.table,
                        "describe_table served from Unity REST"
                    );
                    return Ok(catalog_schema_to_column_info(schema));
                }
                Err(e) if catalog_error_is_fallback(&e) => {
                    debug!(
                        catalog = %table.catalog,
                        schema = %table.schema,
                        table = %table.table,
                        error = %e,
                        "describe_table REST path declined; falling back to SQL"
                    );
                }
                Err(e) => return Err(AdapterError::new(e)),
            }
        }

        let catalog_mgr = crate::catalog::CatalogManager::new(&self.connector);
        catalog_mgr
            .describe_table(table)
            .await
            .map_err(AdapterError::new)
    }

    async fn list_tables(&self, catalog: &str, schema: &str) -> AdapterResult<Vec<String>> {
        // Same shape as `describe_table`: try the Unity REST path first
        // (`GET /api/2.1/unity-catalog/tables?catalog_name=X&schema_name=Y`),
        // fall back on `UnsupportedOperation` or `Transport` only. The
        // trait contract is "table names returned lowercase" so both paths
        // post-process identically — REST returns Unity's as-stored
        // identifiers and we lowercase before returning.
        if let Some(client) = &self.catalog_client {
            let namespace = [catalog.to_string(), schema.to_string()];
            match client.list_tables(&namespace).await {
                Ok(tables) => {
                    debug!(
                        catalog,
                        schema,
                        count = tables.len(),
                        "list_tables served from Unity REST"
                    );
                    return Ok(tables.into_iter().map(|t| t.name.to_lowercase()).collect());
                }
                Err(e) if catalog_error_is_fallback(&e) => {
                    debug!(
                        catalog,
                        schema,
                        error = %e,
                        "list_tables REST path declined; falling back to SQL"
                    );
                }
                Err(e) => return Err(AdapterError::new(e)),
            }
        }

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

#[cfg(test)]
mod tests {
    use super::*;
    use rocky_catalog_core::{ColumnSchema as CatalogColumnSchema, TableSchema as CatalogSchema};

    #[test]
    fn ir_to_catalog_ref_projects_three_part_name() {
        let ir = TableRef {
            catalog: "hc_cat".into(),
            schema: "hc_sch".into(),
            table: "hc_tbl".into(),
        };
        let catalog_ref = ir_to_catalog_ref(&ir);
        assert_eq!(catalog_ref.catalog.as_deref(), Some("hc_cat"));
        assert_eq!(catalog_ref.namespace, vec!["hc_sch".to_string()]);
        assert_eq!(catalog_ref.name, "hc_tbl");
    }

    #[test]
    fn catalog_schema_to_column_info_preserves_fields() {
        let schema = CatalogSchema {
            columns: vec![
                CatalogColumnSchema {
                    name: "id".into(),
                    type_str: "bigint".into(),
                    nullable: false,
                },
                CatalogColumnSchema {
                    name: "amount".into(),
                    type_str: "decimal(10,2)".into(),
                    nullable: true,
                },
            ],
        };
        let cols = catalog_schema_to_column_info(schema);
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].name, "id");
        assert_eq!(cols[0].data_type, "bigint");
        assert!(!cols[0].nullable);
        assert_eq!(cols[1].name, "amount");
        assert_eq!(cols[1].data_type, "decimal(10,2)");
        assert!(cols[1].nullable);
    }

    #[test]
    fn catalog_error_is_fallback_only_for_unsupported_and_transport() {
        // Recoverable: caller should fall through to SQL.
        assert!(catalog_error_is_fallback(
            &CatalogError::UnsupportedOperation("nope")
        ));
        assert!(catalog_error_is_fallback(&CatalogError::Transport(
            Box::new(std::io::Error::other("network blip"))
        )));

        // Non-recoverable: caller must surface as AdapterError.
        assert!(!catalog_error_is_fallback(&CatalogError::AuthFailed(
            "bad token".into()
        )));
        assert!(!catalog_error_is_fallback(&CatalogError::PermissionDenied(
            "denied".into()
        )));
        assert!(!catalog_error_is_fallback(&CatalogError::TableNotFound(
            "missing".into()
        )));
        assert!(!catalog_error_is_fallback(
            &CatalogError::NamespaceNotFound("missing".into())
        ));
        assert!(!catalog_error_is_fallback(&CatalogError::InvalidResponse(
            "bad json".into()
        )));
        assert!(!catalog_error_is_fallback(&CatalogError::CommitConflict(
            "cas".into()
        )));
    }

    #[test]
    fn adapter_defaults_to_no_catalog_client() {
        // Constructor signature must stay backward-compatible: existing
        // `::new(connector)` call sites get SQL-only behaviour. Build a
        // real connector pointed at an unreachable host — `::new` doesn't
        // hit the network, so the test stays offline.
        let auth = crate::auth::Auth::from_config(crate::auth::AuthConfig {
            host: "offline.databricks.test".into(),
            token: Some("offline-token".into()),
            client_id: None,
            client_secret: None,
        })
        .expect("PAT auth is infallible");
        let config = crate::connector::ConnectorConfig {
            host: "offline.databricks.test".into(),
            warehouse_id: "noop".into(),
            timeout: std::time::Duration::from_secs(1),
            retry: Default::default(),
        };
        let connector = DatabricksConnector::new(config, auth);
        let adapter = DatabricksWarehouseAdapter::new(connector);
        assert!(
            adapter.catalog_client().is_none(),
            "default constructor must not wire a catalog client"
        );
    }

    #[test]
    fn with_catalog_client_attaches_the_client() {
        let auth = crate::auth::Auth::from_config(crate::auth::AuthConfig {
            host: "offline.databricks.test".into(),
            token: Some("offline-token".into()),
            client_id: None,
            client_secret: None,
        })
        .expect("PAT auth is infallible");
        let config = crate::connector::ConnectorConfig {
            host: "offline.databricks.test".into(),
            warehouse_id: "noop".into(),
            timeout: std::time::Duration::from_secs(1),
            retry: Default::default(),
        };
        let connector = DatabricksConnector::new(config, auth.clone());
        let client = UnityCatalogClient::new("offline.databricks.test".into(), auth);
        let adapter = DatabricksWarehouseAdapter::new(connector).with_catalog_client(client);
        assert!(
            adapter.catalog_client().is_some(),
            "with_catalog_client must populate the catalog_client slot"
        );
    }
}
