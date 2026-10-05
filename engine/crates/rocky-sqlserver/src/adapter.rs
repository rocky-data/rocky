//! [`WarehouseAdapter`] for SQL Server, Azure SQL and Fabric Warehouse.

use async_trait::async_trait;
use rocky_core::failure_class::{FailureClass, TransientKind};
use rocky_core::traits::{
    AdapterError, AdapterResult, CaseSignificance, ChunkChecksum, ExecutionStats, ObjectKind,
    PkRange, QueryResult, SqlDialect, WarehouseAdapter,
};
use rocky_ir::{ColumnInfo, TableRef};
use rocky_sql::validation;

use crate::config::{Flavor, SqlServerConfig};
use crate::connector::{SqlServerClient, SqlServerError};
use crate::dialect::SqlServerDialect;
use crate::tsql::quote_ident;
use crate::types::type_from_parts;

/// SQL Server / Azure SQL / Fabric warehouse adapter.
pub struct SqlServerWarehouseAdapter {
    client: SqlServerClient,
    dialect: SqlServerDialect,
}

impl SqlServerWarehouseAdapter {
    /// Build an adapter. No connection is opened until the first statement.
    ///
    /// # Errors
    ///
    /// See [`SqlServerClient::new`].
    pub fn new(config: SqlServerConfig) -> Result<Self, SqlServerError> {
        let dialect = SqlServerDialect::with_flavor(config.flavor);
        Ok(Self {
            client: SqlServerClient::new(config)?,
            dialect,
        })
    }

    /// The underlying client.
    #[must_use]
    pub fn client(&self) -> &SqlServerClient {
        &self.client
    }

    /// `N'…'` for a value read back by the catalog queries.
    fn lit(value: &str) -> String {
        format!("N'{}'", value.replace('\'', "''"))
    }

    /// Refuse a catalog that is not the connected database: the
    /// `INFORMATION_SCHEMA` views answer for the current database only.
    fn check_catalog(&self, catalog: &str) -> AdapterResult<()> {
        let db = &self.client.config().database;
        if catalog.is_empty() || catalog.eq_ignore_ascii_case(db) {
            return Ok(());
        }
        Err(AdapterError::msg(format!(
            "catalog '{catalog}' is not the connected database '{db}': sqlserver introspects the \
             connected database only; point `database` at '{catalog}' or drop the catalog"
        )))
    }

    fn validate_table(&self, table: &TableRef) -> AdapterResult<()> {
        validation::validate_identifier(&table.schema).map_err(AdapterError::new)?;
        validation::validate_identifier(&table.table).map_err(AdapterError::new)?;
        self.check_catalog(&table.catalog)
    }
}

fn wrap(err: SqlServerError) -> AdapterError {
    AdapterError::new(err)
}

fn cell(row: &[serde_json::Value], i: usize) -> Option<&str> {
    row.get(i).and_then(serde_json::Value::as_str)
}

/// A statement for a multi-statement script: `CREATE VIEW` runs through
/// `EXEC` (see [`crate::connector::exec_wrapped`]); anything else loses its
/// trailing `;` (the script adds one).
fn batch_safe(stmt: &str) -> String {
    crate::connector::exec_wrapped(stmt)
        .unwrap_or_else(|| stmt.trim().trim_end_matches(';').to_string())
}

#[async_trait]
impl WarehouseAdapter for SqlServerWarehouseAdapter {
    fn dialect(&self) -> &dyn SqlDialect {
        &self.dialect
    }

    fn is_missing_object_error(&self, error: &AdapterError) -> bool {
        error
            .inner()
            .downcast_ref::<SqlServerError>()
            .is_some_and(SqlServerError::is_missing_object)
    }

    /// Identifier case follows the database's collation, not quoting: a
    /// `_CS_` collation makes `[Orders]` and `[orders]` two tables, the
    /// default `_CI_` collations make them one.
    async fn identifier_case_significance(&self) -> AdapterResult<CaseSignificance> {
        let result = self
            .client
            .query("SELECT CONVERT(NVARCHAR(128), DATABASEPROPERTYEX(DB_NAME(), 'Collation'))")
            .await
            .map_err(wrap)?;
        let collation = result
            .rows
            .first()
            .and_then(|r| cell(r, 0))
            .unwrap_or_default()
            .to_ascii_uppercase();
        if collation.contains("_CS") || collation.ends_with("_BIN") || collation.ends_with("_BIN2")
        {
            Ok(CaseSignificance::Significant)
        } else if collation.contains("_CI") {
            Ok(CaseSignificance::Insignificant)
        } else {
            Err(AdapterError::msg(format!(
                "cannot tell identifier case sensitivity from collation '{collation}'"
            )))
        }
    }

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

    /// Drop and create in one transaction: DDL is transactional on SQL
    /// Server, so a failed create leaves the old object in place.
    async fn atomic_drop_and_create(
        &self,
        drop_sql: &str,
        create_sql: &str,
    ) -> AdapterResult<Option<ExecutionStats>> {
        let script = format!(
            "SET XACT_ABORT ON;\nBEGIN TRANSACTION;\n{};\n{};\nCOMMIT TRANSACTION;",
            batch_safe(drop_sql),
            batch_safe(create_sql)
        );
        let rows = self.client.execute(&script).await.map_err(wrap)?;
        Ok(Some(ExecutionStats {
            rows_affected: rows,
            ..ExecutionStats::default()
        }))
    }

    fn supports_object_kind_probe(&self) -> bool {
        true
    }

    fn classify_failure(&self, err: &AdapterError) -> FailureClass {
        let Some(e) = err.inner().downcast_ref::<SqlServerError>() else {
            return FailureClass::Unknown;
        };
        if matches!(e, SqlServerError::Timeout { .. }) {
            return FailureClass::Unknown;
        }
        if !e.is_transient() {
            return FailureClass::Permanent;
        }
        let kind = match e {
            SqlServerError::Transport(_) | SqlServerError::Connect { .. } => TransientKind::Network,
            SqlServerError::Query {
                number: 10928 | 10929 | 40501,
                ..
            } => TransientKind::RateLimit,
            SqlServerError::Query { .. } => TransientKind::ServerBusy,
            _ => TransientKind::Other,
        };
        FailureClass::Transient(kind)
    }

    async fn execute_query(&self, sql: &str) -> AdapterResult<QueryResult> {
        self.client.query(sql).await.map_err(wrap)
    }

    async fn describe_table(&self, table: &TableRef) -> AdapterResult<Vec<ColumnInfo>> {
        self.validate_table(table)?;
        let sql = format!(
            "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, \
             CAST(CHARACTER_MAXIMUM_LENGTH AS NVARCHAR(20)), \
             CAST(NUMERIC_PRECISION AS NVARCHAR(20)), CAST(NUMERIC_SCALE AS NVARCHAR(20)), \
             CAST(DATETIME_PRECISION AS NVARCHAR(20)) \
             FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = {} AND TABLE_NAME = {} \
             ORDER BY ORDINAL_POSITION",
            Self::lit(&table.schema),
            Self::lit(&table.table)
        );
        let result = self.client.query(&sql).await.map_err(wrap)?;
        let columns: Vec<ColumnInfo> = result
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
                        cell(row, 6),
                    ),
                    nullable: !cell(row, 2).is_some_and(|v| v.eq_ignore_ascii_case("NO")),
                })
            })
            .collect();
        if columns.is_empty() {
            // The view returns no rows for an absent table; the run loop
            // needs a typed "missing" error, not an empty schema.
            return Err(wrap(SqlServerError::NotFound {
                schema: table.schema.clone(),
                table: table.table.clone(),
            }));
        }
        Ok(columns)
    }

    async fn object_kind(&self, table: &TableRef) -> AdapterResult<ObjectKind> {
        self.validate_table(table)?;
        let sql = format!(
            "SELECT RTRIM(type) FROM sys.objects WHERE object_id = OBJECT_ID({})",
            Self::lit(&format!(
                "{}.{}",
                quote_ident(&table.schema),
                quote_ident(&table.table)
            ))
        );
        let result = self.client.query(&sql).await.map_err(wrap)?;
        Ok(match result.rows.first().and_then(|r| cell(r, 0)) {
            Some("U") => ObjectKind::Table,
            Some("V") => ObjectKind::View,
            // Synonyms, functions, procedures: not something `CREATE OR
            // ALTER VIEW` / a table build replaces, so the check skips.
            _ => ObjectKind::Unknown,
        })
    }

    async fn promotion_destination_kind(
        &self,
        table: &TableRef,
    ) -> AdapterResult<Option<ObjectKind>> {
        self.validate_table(table)?;
        let schema = self
            .client
            .query(&format!("SELECT SCHEMA_ID({})", Self::lit(&table.schema)))
            .await
            .map_err(wrap)?;
        if schema.rows.first().and_then(|r| cell(r, 0)).is_none() {
            return Err(AdapterError::msg(format!(
                "schema '{}' does not exist",
                table.schema
            )));
        }
        match self.object_kind(table).await? {
            ObjectKind::Unknown => {
                let exists = self
                    .client
                    .query(&format!(
                        "SELECT OBJECT_ID({})",
                        Self::lit(&format!(
                            "{}.{}",
                            quote_ident(&table.schema),
                            quote_ident(&table.table)
                        ))
                    ))
                    .await
                    .map_err(wrap)?;
                if exists.rows.first().and_then(|r| cell(r, 0)).is_some() {
                    Err(AdapterError::msg(format!(
                        "{}.{} exists but is neither a table nor a view",
                        table.schema, table.table
                    )))
                } else {
                    Ok(None)
                }
            }
            kind => Ok(Some(kind)),
        }
    }

    async fn ping(&self) -> AdapterResult<()> {
        self.client.ping().await.map_err(wrap)
    }

    fn is_experimental(&self) -> bool {
        // Fabric Warehouse is SQL-generation-tested only.
        self.client.config().flavor == Flavor::Fabric
    }

    async fn list_tables(&self, catalog: &str, schema: &str) -> AdapterResult<Vec<String>> {
        let sql = self.dialect.list_tables_sql(catalog, schema)?;
        self.check_catalog(catalog)?;
        let result = self.client.query(&sql).await.map_err(wrap)?;
        Ok(result
            .rows
            .iter()
            .filter_map(|row| cell(row, 0).map(str::to_string))
            .collect())
    }

    /// The trait default emits `CREATE OR REPLACE TABLE "…"`, which T-SQL
    /// does not have.
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
            .format_table_ref("", &source.schema, &source.table)?;
        self.execute_statement(
            &self
                .dialect
                .create_table_as(&target, &format!("SELECT * FROM {src}")),
        )
        .await
    }

    /// T-SQL has no `BIT_XOR` aggregate before SQL Server 2022, and the
    /// portable default casts to `DOUBLE`, which is not a T-SQL type.
    async fn checksum_chunks(
        &self,
        _table: &TableRef,
        _pk_column: &str,
        _value_columns: &[String],
        _pk_ranges: &[PkRange],
    ) -> AdapterResult<Vec<ChunkChecksum>> {
        Err(AdapterError::msg(
            "checksum-bisection diff is not supported on sqlserver yet; use the sampled \
             comparison",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_view_is_wrapped_in_exec_inside_a_script() {
        assert_eq!(
            batch_safe("CREATE OR ALTER VIEW [m].[v] AS\nSELECT 'a' AS x"),
            "EXEC(N'CREATE OR ALTER VIEW [m].[v] AS\nSELECT ''a'' AS x')"
        );
        assert_eq!(
            batch_safe("DROP TABLE IF EXISTS [m].[v];"),
            "DROP TABLE IF EXISTS [m].[v]"
        );
    }
}
