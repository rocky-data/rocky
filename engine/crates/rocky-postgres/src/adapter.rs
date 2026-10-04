//! [`WarehouseAdapter`] for PostgreSQL and Amazon Redshift.

use async_trait::async_trait;
use rocky_core::failure_class::{FailureClass, TransientKind};
use rocky_core::traits::{
    AdapterError, AdapterResult, CaseSignificance, ChunkChecksum, ExecutionStats, ExplainResult,
    ObjectKind, PkRange, QueryResult, SqlDialect, WarehouseAdapter,
};
use rocky_ir::{ColumnInfo, TableRef};
use rocky_sql::validation;

use crate::config::{Flavor, PgConfig};
use crate::connector::{PgClient, PgError};
use crate::dialect::{PostgresDialect, RedshiftDialect};
use crate::types::{canonical_type, type_from_parts};

enum Dialect {
    Postgres(PostgresDialect),
    Redshift(RedshiftDialect),
}

/// PostgreSQL / Redshift warehouse adapter.
pub struct PostgresWarehouseAdapter {
    client: PgClient,
    dialect: Dialect,
}

impl PostgresWarehouseAdapter {
    /// PostgreSQL adapter. The dialect's merge rendering follows
    /// `config.merge_mode`.
    ///
    /// # Errors
    ///
    /// See [`PgClient::new`].
    pub fn postgres(config: PgConfig) -> Result<Self, PgError> {
        let dialect = Dialect::Postgres(PostgresDialect::with_merge_mode(config.merge_mode));
        Ok(Self {
            client: PgClient::new(config)?,
            dialect,
        })
    }

    /// Redshift adapter.
    ///
    /// # Errors
    ///
    /// See [`PgClient::new`].
    pub fn redshift(config: PgConfig, late_binding_views: bool) -> Result<Self, PgError> {
        Ok(Self {
            client: PgClient::new(config)?,
            dialect: Dialect::Redshift(RedshiftDialect::with_late_binding_views(
                late_binding_views,
            )),
        })
    }

    /// Build from a config, picking the dialect by its flavor.
    ///
    /// # Errors
    ///
    /// See [`PgClient::new`].
    pub fn from_config(config: PgConfig, late_binding_views: bool) -> Result<Self, PgError> {
        match config.flavor {
            Flavor::Postgres => Self::postgres(config),
            Flavor::Redshift => Self::redshift(config, late_binding_views),
        }
    }

    /// The underlying client.
    #[must_use]
    pub fn client(&self) -> &PgClient {
        &self.client
    }

    fn flavor(&self) -> Flavor {
        self.client.flavor()
    }

    fn lit(&self, value: &str) -> String {
        rocky_core::sql_gen::string_literal(self.dialect(), value)
    }

    /// Refuse a catalog that is not the connected database: neither server
    /// resolves another database's tables through these catalog views.
    fn check_catalog(&self, catalog: &str) -> AdapterResult<()> {
        if catalog.is_empty() || catalog.eq_ignore_ascii_case(&self.client.config().database) {
            return Ok(());
        }
        Err(AdapterError::msg(format!(
            "catalog '{catalog}' is not the connected database '{}': {} cannot introspect \
             another database over this connection",
            self.client.config().database,
            self.flavor().adapter_type()
        )))
    }
}

fn wrap(err: PgError) -> AdapterError {
    AdapterError::new(err)
}

fn cell(row: &[serde_json::Value], i: usize) -> Option<&str> {
    row.get(i).and_then(serde_json::Value::as_str)
}

#[async_trait]
impl WarehouseAdapter for PostgresWarehouseAdapter {
    fn dialect(&self) -> &dyn SqlDialect {
        match &self.dialect {
            Dialect::Postgres(d) => d,
            Dialect::Redshift(d) => d,
        }
    }

    fn is_missing_object_error(&self, error: &AdapterError) -> bool {
        error
            .inner()
            .downcast_ref::<PgError>()
            .is_some_and(PgError::is_missing_object)
    }

    /// PostgreSQL treats a quoted identifier's case as identity (`"Orders"`
    /// and `"orders"` are two tables) — a property of the server, not of a
    /// session setting. Redshift's answer depends on the session's
    /// `enable_case_sensitive_identifier`, so it is not reported.
    async fn identifier_case_significance(&self) -> AdapterResult<CaseSignificance> {
        match self.flavor() {
            Flavor::Postgres => Ok(CaseSignificance::Significant),
            Flavor::Redshift => Err(AdapterError::msg(
                "redshift identifier case depends on the session's \
                 enable_case_sensitive_identifier setting; not probed",
            )),
        }
    }

    /// The connected database is the catalog a two-part name resolves in,
    /// and the configuration names it exactly.
    fn default_catalog(&self) -> Option<String> {
        Some(self.client.config().database.clone())
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

    /// DDL is transactional on both servers: the drop and the create run as
    /// one implicit transaction, so a failed create leaves the old object.
    async fn atomic_drop_and_create(
        &self,
        drop_sql: &str,
        create_sql: &str,
    ) -> AdapterResult<Option<ExecutionStats>> {
        let rows = self
            .client
            .execute(&format!("{drop_sql};\n{create_sql}"))
            .await
            .map_err(wrap)?;
        Ok(Some(ExecutionStats {
            rows_affected: rows,
            ..ExecutionStats::default()
        }))
    }

    fn supports_object_kind_probe(&self) -> bool {
        true
    }

    fn classify_failure(&self, err: &AdapterError) -> FailureClass {
        let Some(pg) = err.inner().downcast_ref::<PgError>() else {
            return FailureClass::Unknown;
        };
        // A client-side timeout says nothing about whether the server
        // committed; never retried (see `PgError::is_transient`).
        if matches!(pg, PgError::Timeout { .. }) {
            return FailureClass::Unknown;
        }
        if !pg.is_transient() {
            return FailureClass::Permanent;
        }
        let kind = match pg {
            PgError::Transport(_) | PgError::Connect { .. } => TransientKind::Network,
            PgError::Query { sqlstate, .. } if sqlstate.starts_with("08") => TransientKind::Network,
            PgError::Query { .. } => TransientKind::ServerBusy,
            _ => TransientKind::Other,
        };
        FailureClass::Transient(kind)
    }

    async fn execute_query(&self, sql: &str) -> AdapterResult<QueryResult> {
        self.client.query_typed(sql).await.map_err(wrap)
    }

    async fn describe_table(&self, table: &TableRef) -> AdapterResult<Vec<ColumnInfo>> {
        validation::validate_identifier(&table.schema).map_err(AdapterError::new)?;
        validation::validate_identifier(&table.table).map_err(AdapterError::new)?;
        self.check_catalog(&table.catalog)?;
        // Targets render bare, so the server folded them to lower case.
        let schema = table.schema.to_lowercase();
        let name = table.table.to_lowercase();
        let columns = match self.flavor() {
            Flavor::Postgres => {
                // `pg_catalog` rather than `information_schema.columns`: the
                // latter omits materialized views and splits type modifiers
                // across columns. Relation kinds: table, partitioned table,
                // view, materialized view, foreign table.
                let sql = format!(
                    "SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod), \
                     CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END \
                     FROM pg_catalog.pg_attribute a \
                     JOIN pg_catalog.pg_class c ON c.oid = a.attrelid \
                     JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
                     WHERE n.nspname = {} AND c.relname = {} \
                     AND c.relkind IN ('r', 'p', 'v', 'm', 'f') \
                     AND a.attnum > 0 AND NOT a.attisdropped \
                     ORDER BY a.attnum",
                    self.lit(&schema),
                    self.lit(&name)
                );
                let result = self.client.query(&sql).await.map_err(wrap)?;
                result
                    .rows
                    .iter()
                    .filter_map(|row| {
                        Some(ColumnInfo {
                            name: cell(row, 0)?.to_string(),
                            data_type: canonical_type(cell(row, 1)?),
                            nullable: cell(row, 2) != Some("NO"),
                        })
                    })
                    .collect::<Vec<_>>()
            }
            Flavor::Redshift => {
                // `svv_columns` covers local tables, regular and late-binding
                // views, and external (Spectrum) tables — `pg_attribute` does
                // not see late-binding views.
                let sql = format!(
                    "SELECT column_name, data_type, is_nullable, character_maximum_length, \
                     numeric_precision, numeric_scale \
                     FROM svv_columns WHERE table_schema = {} AND table_name = {} \
                     ORDER BY ordinal_position",
                    self.lit(&schema),
                    self.lit(&name)
                );
                let result = self.client.query(&sql).await.map_err(wrap)?;
                result
                    .rows
                    .iter()
                    .filter_map(|row| {
                        Some(ColumnInfo {
                            name: cell(row, 0)?.to_string(),
                            data_type: type_from_parts(
                                cell(row, 1)?,
                                cell(row, 3),
                                cell(row, 4),
                                cell(row, 5),
                            ),
                            nullable: !cell(row, 2).is_some_and(|v| v.eq_ignore_ascii_case("NO")),
                        })
                    })
                    .collect()
            }
        };
        if columns.is_empty() {
            // The catalog views return no rows for an absent relation; the
            // run loop needs that as a typed "missing" error, not an empty
            // schema.
            return Err(wrap(PgError::NotFound {
                schema: table.schema.clone(),
                table: table.table.clone(),
            }));
        }
        Ok(columns)
    }

    async fn object_kind(&self, table: &TableRef) -> AdapterResult<ObjectKind> {
        validation::validate_identifier(&table.schema).map_err(AdapterError::new)?;
        validation::validate_identifier(&table.table).map_err(AdapterError::new)?;
        self.check_catalog(&table.catalog)?;
        let sql = format!(
            "SELECT c.relkind FROM pg_catalog.pg_class c \
             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = {} AND c.relname = {}",
            self.lit(&table.schema.to_lowercase()),
            self.lit(&table.table.to_lowercase())
        );
        let result = self.client.query(&sql).await.map_err(wrap)?;
        let kind = result.rows.first().and_then(|row| cell(row, 0));
        Ok(match kind {
            Some("r" | "p") => ObjectKind::Table,
            Some("v") => ObjectKind::View,
            // Materialized views, foreign tables, sequences, indexes: none is
            // what `CREATE OR REPLACE TABLE|VIEW` targets, so the check skips.
            _ => ObjectKind::Unknown,
        })
    }

    async fn ping(&self) -> AdapterResult<()> {
        self.client.ping().await.map_err(wrap)
    }

    async fn explain(&self, sql: &str) -> AdapterResult<ExplainResult> {
        let result = self
            .client
            .query(&format!("EXPLAIN {sql}"))
            .await
            .map_err(wrap)?;
        let lines: Vec<String> = result
            .rows
            .iter()
            .filter_map(|row| cell(row, 0).map(str::to_string))
            .collect();
        // Top plan node: `... (cost=0.00..35.50 rows=2550 width=4)`.
        let estimated_rows = lines.first().and_then(|line| {
            let rest = &line[line.find("rows=")? + 5..];
            rest.split(|c: char| !c.is_ascii_digit())
                .next()?
                .parse()
                .ok()
        });
        Ok(ExplainResult {
            estimated_bytes_scanned: None,
            estimated_rows,
            estimated_compute_units: None,
            raw_explain: lines.join("\n"),
        })
    }

    fn is_experimental(&self) -> bool {
        // Redshift has no local emulator: its SQL is unit-tested, never run
        // in CI.
        self.flavor() == Flavor::Redshift
    }

    async fn list_tables(&self, catalog: &str, schema: &str) -> AdapterResult<Vec<String>> {
        let sql = self.dialect().list_tables_sql(catalog, schema)?;
        self.check_catalog(catalog)?;
        let result = self.client.query(&sql).await.map_err(wrap)?;
        Ok(result
            .rows
            .iter()
            .filter_map(|row| cell(row, 0).map(str::to_lowercase))
            .collect())
    }

    /// The trait default emits `CREATE OR REPLACE TABLE "…"`, which neither
    /// server accepts. Drop-and-create in one transaction instead.
    async fn clone_table_for_branch(
        &self,
        source: &TableRef,
        branch_schema: &str,
    ) -> AdapterResult<()> {
        let dialect = self.dialect();
        let target = dialect.format_table_ref("", branch_schema, &source.table)?;
        let src = dialect.format_table_ref("", &source.schema, &source.table)?;
        self.execute_statement(&dialect.create_table_as(&target, &format!("SELECT * FROM {src}")))
            .await
    }

    /// The portable default casts to `DOUBLE`, which is not a PostgreSQL
    /// type name, and Redshift has no `BIT_XOR` aggregate. Refused until a
    /// native implementation lands.
    async fn checksum_chunks(
        &self,
        _table: &TableRef,
        _pk_column: &str,
        _value_columns: &[String],
        _pk_ranges: &[PkRange],
    ) -> AdapterResult<Vec<ChunkChecksum>> {
        Err(AdapterError::msg(format!(
            "checksum-bisection diff is not supported on {} yet; use the sampled comparison",
            self.flavor().adapter_type()
        )))
    }
}
