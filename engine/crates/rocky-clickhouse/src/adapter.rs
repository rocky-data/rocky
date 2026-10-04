//! [`WarehouseAdapter`] for ClickHouse.

use async_trait::async_trait;
use rocky_core::failure_class::{FailureClass, TransientKind};
use rocky_core::traits::{
    AdapterError, AdapterResult, CaseSignificance, ChunkChecksum, ExecutionStats, ExplainResult,
    ObjectKind, PkRange, QueryResult, SqlDialect, WarehouseAdapter,
};
use rocky_ir::{ColumnInfo, TableRef};

use crate::config::ChConfig;
use crate::connector::{ChClient, ChError};
use crate::dialect::ClickHouseDialect;
use crate::types::column_type;

/// ClickHouse warehouse adapter (beta).
pub struct ClickHouseWarehouseAdapter {
    client: ChClient,
    dialect: ClickHouseDialect,
}

impl ClickHouseWarehouseAdapter {
    /// Build from a config.
    ///
    /// # Errors
    ///
    /// See [`ChClient::new`].
    pub fn new(config: ChConfig) -> Result<Self, ChError> {
        Ok(Self {
            client: ChClient::new(config)?,
            dialect: ClickHouseDialect::new(),
        })
    }

    /// The underlying client.
    #[must_use]
    pub fn client(&self) -> &ChClient {
        &self.client
    }

    fn lit(&self, value: &str) -> String {
        rocky_core::sql_gen::string_literal(&self.dialect, value)
    }

    /// Validate a table reference and return its `(database, table)`.
    fn locate<'a>(&self, table: &'a TableRef) -> AdapterResult<(&'a str, &'a str)> {
        // Same rules as rendering a target: no catalog, ClickHouse names.
        self.dialect
            .format_table_ref(&table.catalog, &table.schema, &table.table)?;
        Ok((&table.schema, &table.table))
    }
}

fn wrap(err: ChError) -> AdapterError {
    AdapterError::new(err)
}

fn cell(row: &[serde_json::Value], i: usize) -> Option<&str> {
    row.get(i).and_then(serde_json::Value::as_str)
}

#[async_trait]
impl WarehouseAdapter for ClickHouseWarehouseAdapter {
    fn dialect(&self) -> &dyn SqlDialect {
        &self.dialect
    }

    fn is_missing_object_error(&self, error: &AdapterError) -> bool {
        error
            .inner()
            .downcast_ref::<ChError>()
            .is_some_and(ChError::is_missing_object)
    }

    /// ClickHouse database and table names are case-sensitive, quoted or
    /// not — a property of the server, not of a session setting.
    async fn identifier_case_significance(&self) -> AdapterResult<CaseSignificance> {
        Ok(CaseSignificance::Significant)
    }

    async fn execute_statement(&self, sql: &str) -> AdapterResult<()> {
        self.client.execute(sql).await.map(|_| ()).map_err(wrap)
    }

    async fn execute_statement_with_stats(&self, sql: &str) -> AdapterResult<ExecutionStats> {
        let rows = self.client.execute(sql).await.map_err(wrap)?;
        Ok(ExecutionStats {
            rows_affected: rows,
            ..ExecutionStats::default()
        })
    }

    /// A kind switch (view ↔ table) as ONE statement: ClickHouse's
    /// `CREATE OR REPLACE TABLE` and `CREATE OR REPLACE VIEW` each replace an
    /// object of the other kind atomically, so the DROP is never run and a
    /// failing CREATE leaves the old object in place. Any other CREATE
    /// (which would need the DROP first) is not supported: `None`, so the
    /// caller drops nothing.
    async fn atomic_drop_and_create(
        &self,
        _drop_sql: &str,
        create_sql: &str,
    ) -> AdapterResult<Option<ExecutionStats>> {
        let head = create_sql.trim_start().to_ascii_uppercase();
        if !(head.starts_with("CREATE OR REPLACE TABLE ")
            || head.starts_with("CREATE OR REPLACE VIEW "))
        {
            return Ok(None);
        }
        self.execute_statement_with_stats(create_sql)
            .await
            .map(Some)
    }

    fn supports_object_kind_probe(&self) -> bool {
        true
    }

    fn classify_failure(&self, err: &AdapterError) -> FailureClass {
        let Some(ch) = err.inner().downcast_ref::<ChError>() else {
            return FailureClass::Unknown;
        };
        // A client-side timeout says nothing about whether the server ran
        // the statement; never retried (see `ChError::is_transient`).
        if matches!(ch, ChError::Timeout { .. }) {
            return FailureClass::Unknown;
        }
        if !ch.is_transient() {
            return FailureClass::Permanent;
        }
        let kind = match ch {
            ChError::Transport(_) => TransientKind::Network,
            _ => TransientKind::ServerBusy,
        };
        FailureClass::Transient(kind)
    }

    async fn execute_query(&self, sql: &str) -> AdapterResult<QueryResult> {
        self.client.query(sql).await.map_err(wrap)
    }

    /// `system.columns`: every table engine and views alike, with the full
    /// type including `Nullable` / `LowCardinality` wrappers, which
    /// [`column_type`] splits into a nullability flag and a canonical name.
    async fn describe_table(&self, table: &TableRef) -> AdapterResult<Vec<ColumnInfo>> {
        let (database, name) = self.locate(table)?;
        let sql = format!(
            "SELECT name, type FROM system.columns WHERE database = {} AND table = {} \
             ORDER BY position",
            self.lit(database),
            self.lit(name)
        );
        let result = self.client.query(&sql).await.map_err(wrap)?;
        let columns: Vec<ColumnInfo> = result
            .rows
            .iter()
            .filter_map(|row| {
                let ty = column_type(cell(row, 1)?);
                Some(ColumnInfo {
                    name: cell(row, 0)?.to_string(),
                    data_type: ty.data_type,
                    nullable: ty.nullable,
                })
            })
            .collect();
        if columns.is_empty() {
            // `system.columns` returns no rows for an absent table; the run
            // loop needs a typed "missing" error, not an empty schema.
            return Err(wrap(ChError::NotFound {
                database: database.to_string(),
                table: name.to_string(),
            }));
        }
        Ok(columns)
    }

    async fn object_kind(&self, table: &TableRef) -> AdapterResult<ObjectKind> {
        let (database, name) = self.locate(table)?;
        let sql = format!(
            "SELECT engine FROM system.tables WHERE database = {} AND name = {}",
            self.lit(database),
            self.lit(name)
        );
        let result = self.client.query(&sql).await.map_err(wrap)?;
        Ok(match result.rows.first().and_then(|row| cell(row, 0)) {
            Some("View") => ObjectKind::View,
            // Rocky's tables are MergeTree-family. A materialized view,
            // dictionary or other engine is not what `CREATE OR REPLACE
            // TABLE|VIEW` targets, so the check skips it.
            Some(engine) if engine.ends_with("MergeTree") => ObjectKind::Table,
            _ => ObjectKind::Unknown,
        })
    }

    async fn ping(&self) -> AdapterResult<()> {
        self.client.ping().await.map_err(wrap)
    }

    /// `EXPLAIN ESTIMATE`: rows the query would read, summed over the
    /// tables it reads.
    async fn explain(&self, sql: &str) -> AdapterResult<ExplainResult> {
        let result = self
            .client
            .query(&format!("EXPLAIN ESTIMATE {sql}"))
            .await
            .map_err(wrap)?;
        let rows_col = result.columns.iter().position(|c| c == "rows");
        let estimated_rows = rows_col.map(|i| {
            result
                .rows
                .iter()
                .filter_map(|row| cell(row, i)?.parse::<u64>().ok())
                .sum()
        });
        let raw = result
            .rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|v| v.as_str().unwrap_or("NULL"))
                    .collect::<Vec<_>>()
                    .join("\t")
            })
            .collect::<Vec<_>>()
            .join("\n");
        Ok(ExplainResult {
            estimated_bytes_scanned: None,
            estimated_rows,
            estimated_compute_units: None,
            raw_explain: raw,
        })
    }

    /// Beta: SQL generation is unit-tested and the adapter is live-tested
    /// against a local server, but it has had no production use yet.
    fn is_experimental(&self) -> bool {
        true
    }

    async fn list_tables(&self, catalog: &str, schema: &str) -> AdapterResult<Vec<String>> {
        let sql = self.dialect.list_tables_sql(catalog, schema)?;
        let result = self.client.query(&sql).await.map_err(wrap)?;
        Ok(result
            .rows
            .iter()
            .filter_map(|row| cell(row, 0).map(str::to_lowercase))
            .collect())
    }

    /// The trait default emits a quoted `CREATE OR REPLACE TABLE … AS` with
    /// no `ENGINE`, which ClickHouse rejects. Copy the source's structure
    /// and engine (`CREATE TABLE … AS source`), then its rows.
    async fn clone_table_for_branch(
        &self,
        source: &TableRef,
        branch_schema: &str,
    ) -> AdapterResult<()> {
        let target = self
            .dialect
            .format_table_ref("", branch_schema, &source.table)?;
        let src = self
            .dialect
            .format_table_ref(&source.catalog, &source.schema, &source.table)?;
        self.execute_statement(&format!("DROP TABLE IF EXISTS {target}"))
            .await?;
        self.execute_statement(&format!("CREATE TABLE {target} AS {src}"))
            .await?;
        self.execute_statement(&format!("INSERT INTO {target} SELECT * FROM {src}"))
            .await
    }

    /// The portable default aggregates with `BIT_XOR` over a dialect row
    /// hash this dialect does not define yet. Refused until a native
    /// (`sipHash64` + `groupBitXor`) implementation lands.
    async fn checksum_chunks(
        &self,
        _table: &TableRef,
        _pk_column: &str,
        _value_columns: &[String],
        _pk_ranges: &[PkRange],
    ) -> AdapterResult<Vec<ChunkChecksum>> {
        Err(AdapterError::msg(
            "checksum-bisection diff is not supported on clickhouse yet; use the sampled \
             comparison",
        ))
    }
}
