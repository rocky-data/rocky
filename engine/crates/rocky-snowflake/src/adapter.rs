//! Snowflake warehouse adapter implementing [`WarehouseAdapter`].

use async_trait::async_trait;

use rocky_core::ir::{ColumnInfo, TableRef};
use rocky_core::traits::{
    AdapterError, AdapterResult, ChunkChecksum, PkRange, QueryResult, SqlDialect, WarehouseAdapter,
};
use rocky_sql::validation;

use crate::connector::SnowflakeConnector;
use crate::dialect::SnowflakeSqlDialect;

/// Snowflake warehouse adapter wrapping [`SnowflakeConnector`] behind the
/// [`WarehouseAdapter`] trait.
pub struct SnowflakeWarehouseAdapter {
    connector: SnowflakeConnector,
    dialect: SnowflakeSqlDialect,
}

impl SnowflakeWarehouseAdapter {
    pub fn new(connector: SnowflakeConnector) -> Self {
        Self {
            connector,
            dialect: SnowflakeSqlDialect,
        }
    }

    /// Access the underlying connector (for adapter-specific operations).
    pub fn connector(&self) -> &SnowflakeConnector {
        &self.connector
    }
}

#[async_trait]
impl WarehouseAdapter for SnowflakeWarehouseAdapter {
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

    // `execute_statement_with_stats` intentionally NOT overridden.
    //
    // Snowflake does not return `bytes_scanned` in the immediate SQL API
    // response; it only surfaces per-statement in QUERY_HISTORY (keyed
    // by `query_id`), so populating it would add a second round-trip per
    // statement — violating the implicit "cheap, piggybacked on the
    // existing response" contract that BigQuery and Databricks satisfy.
    //
    // More importantly, Snowflake cost is duration-based, not
    // bytes-based: `rocky_core::cost::compute_observed_cost_usd` routes
    // the Snowflake branch through `duration_hours × dbu_per_hour ×
    // cost_per_dbu` and never reads `bytes_scanned`. Adding a
    // QUERY_HISTORY lookup per statement would only affect display in
    // `rocky trace` / `rocky history`, not `rocky cost` accuracy.
    //
    // If bytes_scanned is wanted for display, revisit with a batched
    // QUERY_HISTORY lookup at run-finalise time (one query for all
    // statement IDs, not N queries). If Snowflake's pricing ever shifts
    // to bytes-based (e.g., serverless compute, query acceleration),
    // reconsider the trade-off — at that point the cost-correctness win
    // would justify the extra round-trip.
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
        let table_ref =
            self.dialect
                .format_table_ref(&table.catalog, &table.schema, &table.table)?;
        let describe_sql = self.dialect.describe_table_sql(&table_ref);
        let result = self
            .connector
            .execute_sql(&describe_sql)
            .await
            .map_err(AdapterError::new)?;

        // Snowflake DESCRIBE TABLE returns rows with columns:
        // name, type, kind, null?, default, primary_key, unique_key, ...
        // Look up column positions by name so we're resilient to column
        // order changes across Snowflake versions.
        let col_headers: Vec<String> = result
            .columns
            .iter()
            .map(|c| c.name.to_lowercase())
            .collect();
        let name_idx = col_headers.iter().position(|c| c == "name").unwrap_or(0);
        let type_idx = col_headers.iter().position(|c| c == "type").unwrap_or(1);
        let null_idx = col_headers.iter().position(|c| c == "null?").unwrap_or(3);

        let mut columns = Vec::new();
        for row in &result.rows {
            let name = row
                .get(name_idx)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_lowercase();
            let data_type = row
                .get(type_idx)
                .and_then(|v| v.as_str())
                .unwrap_or("VARCHAR")
                .to_string();
            let nullable = row
                .get(null_idx)
                .and_then(|v| v.as_str())
                .map(|s| s == "Y")
                .unwrap_or(true);

            columns.push(ColumnInfo {
                name,
                data_type,
                nullable,
            });
        }

        Ok(columns)
    }

    async fn ping(&self) -> AdapterResult<()> {
        // Cheaper than SELECT 1 — no compute warehouse query, just a session check
        self.connector
            .execute_statement("SELECT CURRENT_WAREHOUSE()")
            .await
            .map(|_| ())
            .map_err(AdapterError::new)
    }

    async fn list_tables(&self, catalog: &str, schema: &str) -> AdapterResult<Vec<String>> {
        validation::validate_identifier(catalog).map_err(AdapterError::new)?;
        validation::validate_identifier(schema).map_err(AdapterError::new)?;
        // Snowflake: SELECT table_name FROM <database>.information_schema.tables
        let sql = format!(
            "SELECT table_name FROM {catalog}.information_schema.tables \
             WHERE table_schema = '{schema}'"
        );
        let result = self
            .connector
            .execute_sql(&sql)
            .await
            .map_err(AdapterError::new)?;
        let tables = result
            .rows
            .iter()
            .filter_map(|row| row.first().and_then(|v| v.as_str()).map(str::to_lowercase))
            .collect();
        Ok(tables)
    }

    /// Snowflake override of the checksum-bisection chunk-checksum
    /// query. The kernel default in `rocky-core` emits
    /// `BIT_XOR(<row_hash>)`, which Snowflake does not have — its
    /// aggregate equivalent is `BITXOR_AGG`. The override also pins
    /// the cast type to Snowflake-canonical `INTEGER` (alias for
    /// `NUMBER(38, 0)`) instead of the kernel's `BIGINT`, and routes
    /// per-row hashing through `SnowflakeSqlDialect::row_hash_expr`
    /// (which already wraps `HASH(...)` in a `COALESCE` so all-NULL
    /// rows do not get skipped by `BITXOR_AGG`'s NULL-skipping
    /// behavior).
    ///
    /// The `LEAST(K-1, FLOOR(...))` clamp matches the kernel default
    /// — `split_int_range` lets the last chunk absorb the integer-
    /// step truncation remainder, so the SQL bucketing has to pin
    /// the highest pk values back into chunk K-1 instead of
    /// overflowing to K. Identifier quoting routes through the
    /// dialect's `quote_identifier` (double quotes), so lowercase
    /// column names survive Snowflake's case-fold-on-unquoted rule.
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
        let (lo_min, hi_max, k) = snowflake_uniform_int_window(pk_ranges)?;
        let step = (hi_max - lo_min) / (k as i128);
        if step <= 0 {
            return Err(AdapterError::msg(
                "Snowflake checksum_chunks requires a positive integer step \
                 across the chunk window (lo == hi or hi < lo)",
            ));
        }

        validation::validate_identifier(pk_column).map_err(AdapterError::new)?;
        for col in value_columns {
            validation::validate_identifier(col).map_err(AdapterError::new)?;
        }

        let table_ref =
            self.dialect
                .format_table_ref(&table.catalog, &table.schema, &table.table)?;
        let row_hash = self.dialect.row_hash_expr(value_columns)?;
        let pk_quoted = self.dialect.quote_identifier(pk_column);
        let last_id = k - 1;

        let sql = format!(
            "SELECT chunk_id, COUNT(*) AS row_count, BITXOR_AGG({row_hash}) AS chk \
             FROM ( \
                 SELECT *, \
                        LEAST( \
                            CAST(FLOOR((CAST({pk_quoted} AS DOUBLE) - {lo}) / {step}) AS INTEGER), \
                            {last_id} \
                        ) AS chunk_id \
                 FROM {table_ref} \
                 WHERE {pk_quoted} IS NOT NULL \
                   AND {pk_quoted} >= {lo} \
                   AND {pk_quoted} < {hi} \
             ) AS chunked \
             GROUP BY chunk_id \
             ORDER BY chunk_id",
            lo = lo_min,
            hi = hi_max,
        );

        let result = self.execute_query(&sql).await?;
        parse_snowflake_chunk_checksums(&result, k)
    }
}

/// Verify the bisection runner passed K contiguous IntRange chunks and
/// return `(lo_min, hi_max, K)` for SQL interpolation. Mirrors the
/// kernel default's `uniform_int_range_window`; composite +
/// hash-bucket strategies are out of scope for this override and
/// surface as an explicit error.
fn snowflake_uniform_int_window(pk_ranges: &[PkRange]) -> AdapterResult<(i128, i128, u32)> {
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
                    "Snowflake checksum_chunks supports IntRange only; \
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

/// Parse a Snowflake `(chunk_id, row_count, chk)` result set into
/// [`ChunkChecksum`] rows. Snowflake's REST API returns numerics as
/// JSON strings; we still accept the integer JSON shape as a
/// belt-and-braces guard for any future driver change.
fn parse_snowflake_chunk_checksums(
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
        let chunk_id_raw = parse_snowflake_i128(&row[0])?;
        if chunk_id_raw < 0 || chunk_id_raw >= i128::from(k) {
            return Err(AdapterError::msg(format!(
                "checksum_chunks returned chunk_id={chunk_id_raw}, outside [0, {k})"
            )));
        }
        let row_count_raw = parse_snowflake_i128(&row[1])?;
        let row_count = u64::try_from(row_count_raw).map_err(|_| {
            AdapterError::msg(format!(
                "checksum_chunks returned row_count={row_count_raw}, expected non-negative u64"
            ))
        })?;
        let checksum = parse_snowflake_i128(&row[2])? as u128;
        out.push(ChunkChecksum {
            chunk_id: chunk_id_raw as u32,
            row_count,
            checksum,
        });
    }
    Ok(out)
}

fn parse_snowflake_i128(v: &serde_json::Value) -> AdapterResult<i128> {
    if let Some(s) = v.as_str() {
        return s.parse::<i128>().map_err(|e| {
            AdapterError::msg(format!(
                "failed to parse {s:?} as i128 from Snowflake result: {e}"
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
        "Snowflake checksum_chunks result column had unexpected JSON shape: {v:?}"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::{Auth, AuthConfig};
    use crate::connector::ConnectorConfig;
    use rocky_core::config::RetryConfig;
    use std::time::Duration;

    /// Verifies that the adapter can be constructed and used as a trait object.
    fn _assert_warehouse_adapter_trait_object(_: &dyn WarehouseAdapter) {}

    /// Verifies construction compiles and the dialect is correct.
    #[test]
    fn test_adapter_construction() {
        let auth = Auth::from_config(AuthConfig {
            account: "test_account".into(),
            username: None,
            password: None,
            oauth_token: Some("test_token".into()),
            private_key_path: None,
            pat: None,
        })
        .unwrap();

        let config = ConnectorConfig {
            account: "test_account".into(),
            warehouse: "COMPUTE_WH".into(),
            database: Some("MY_DB".into()),
            schema: None,
            role: None,
            timeout: Duration::from_secs(120),
            retry: RetryConfig::default(),
        };

        let connector = SnowflakeConnector::new(config, auth);
        let adapter = SnowflakeWarehouseAdapter::new(connector);

        // Verify dialect produces Snowflake-specific SQL
        let sql = adapter
            .dialect()
            .create_catalog_sql("my_warehouse")
            .unwrap()
            .unwrap();
        assert!(sql.contains("DATABASE"));
        assert!(!sql.contains("CATALOG"));
    }

    #[test]
    fn test_adapter_dialect_format_table_ref() {
        let auth = Auth::from_config(AuthConfig {
            account: "test_account".into(),
            username: None,
            password: None,
            oauth_token: Some("token".into()),
            private_key_path: None,
            pat: None,
        })
        .unwrap();

        let config = ConnectorConfig {
            account: "test_account".into(),
            warehouse: "WH".into(),
            database: None,
            schema: None,
            role: None,
            timeout: Duration::from_secs(60),
            retry: RetryConfig::default(),
        };

        let connector = SnowflakeConnector::new(config, auth);
        let adapter = SnowflakeWarehouseAdapter::new(connector);

        let ref_str = adapter
            .dialect()
            .format_table_ref("mydb", "public", "users")
            .unwrap();
        assert_eq!(ref_str, "\"mydb\".\"public\".\"users\"");
    }

    #[test]
    fn test_adapter_connector_access() {
        let auth = Auth::from_config(AuthConfig {
            account: "test_account".into(),
            username: None,
            password: None,
            oauth_token: Some("token".into()),
            private_key_path: None,
            pat: None,
        })
        .unwrap();

        let config = ConnectorConfig {
            account: "test_account".into(),
            warehouse: "WH".into(),
            database: None,
            schema: None,
            role: None,
            timeout: Duration::from_secs(60),
            retry: RetryConfig::default(),
        };

        let connector = SnowflakeConnector::new(config, auth);
        let adapter = SnowflakeWarehouseAdapter::new(connector);

        // Just verify we can access the connector without panic
        let _connector_ref = adapter.connector();
    }
}
