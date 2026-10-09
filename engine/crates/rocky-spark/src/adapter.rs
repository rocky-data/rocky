//! [`WarehouseAdapter`] for Apache Spark over Spark Connect.

use async_trait::async_trait;
use rocky_core::failure_class::{FailureClass, TransientKind};
use rocky_core::traits::{
    AdapterError, AdapterResult, CaseSignificance, ExecutionStats, ExplainResult, ObjectKind,
    QueryResult, SqlDialect, WarehouseAdapter,
};
use rocky_ir::{ColumnInfo, TableRef};

use crate::config::SparkConfig;
use crate::connector::{SparkClient, SparkError, batches_to_query_result};
use crate::dialect::{SparkDialect, TableFormat};

/// Apache Spark warehouse adapter (beta).
pub struct SparkWarehouseAdapter {
    client: SparkClient,
    dialect: SparkDialect,
    session_ready: tokio::sync::OnceCell<()>,
}

impl SparkWarehouseAdapter {
    /// Build from a config and the table format Rocky's tables use.
    ///
    /// # Errors
    ///
    /// See [`SparkClient::new`].
    pub fn new(config: SparkConfig, format: TableFormat) -> Result<Self, SparkError> {
        Ok(Self {
            client: SparkClient::new(config)?,
            dialect: SparkDialect::with_table_format(format),
            session_ready: tokio::sync::OnceCell::new(),
        })
    }

    /// The underlying client.
    #[must_use]
    pub fn client(&self) -> &SparkClient {
        &self.client
    }

    /// Set the session's default table format once, before the first
    /// statement, so a `CREATE TABLE` without `USING` (every one Rocky
    /// renders) makes a Delta or Iceberg table rather than a Parquet table
    /// that cannot `MERGE`, `DELETE` or be replaced.
    async fn prepare_session(&self) -> AdapterResult<()> {
        self.session_ready
            .get_or_try_init(|| async {
                let sql = format!(
                    "SET spark.sql.sources.default = {}",
                    self.dialect.table_format().source_name()
                );
                self.client.execute(&sql).await.map(|_| ()).map_err(wrap)
            })
            .await
            .map(|()| ())
    }

    async fn run(&self, sql: &str) -> AdapterResult<Vec<arrow::record_batch::RecordBatch>> {
        self.prepare_session().await?;
        self.client.execute(sql).await.map_err(wrap)
    }

    async fn rows(&self, sql: &str) -> AdapterResult<QueryResult> {
        let batches = self.run(sql).await?;
        batches_to_query_result(&batches).map_err(wrap)
    }

    fn table_ref(&self, table: &TableRef) -> AdapterResult<String> {
        self.dialect
            .format_table_ref(&table.catalog, &table.schema, &table.table)
    }
}

fn wrap(err: SparkError) -> AdapterError {
    AdapterError::new(err)
}

fn cell(row: &[serde_json::Value], i: usize) -> Option<&str> {
    row.get(i).and_then(serde_json::Value::as_str)
}

/// `DESCRIBE TABLE` rows up to the first metadata section
/// (`# Partition Information`, `# Detailed Table Information`). A section
/// header has a `#` name and an empty type; a real column named `#x` always
/// has a type, so it stays.
fn primary_describe_columns(rows: &[Vec<serde_json::Value>]) -> Vec<ColumnInfo> {
    rows.iter()
        .take_while(|row| {
            let name = cell(row, 0).unwrap_or("");
            let data_type = cell(row, 1).unwrap_or("");
            !(name.starts_with('#') && data_type.trim().is_empty())
        })
        .filter_map(|row| {
            let name = cell(row, 0)?;
            let data_type = cell(row, 1)?;
            if name.is_empty() {
                return None;
            }
            Some(ColumnInfo {
                name: name.to_string(),
                data_type: data_type.to_string(),
                // `DESCRIBE TABLE` does not report nullability.
                nullable: true,
            })
        })
        .collect()
}

/// The `Type` row of `DESCRIBE TABLE EXTENDED`: `VIEW`, `MANAGED` or
/// `EXTERNAL`.
fn describe_extended_kind(rows: &[Vec<serde_json::Value>]) -> ObjectKind {
    let kind = rows
        .iter()
        .find(|row| cell(row, 0) == Some("Type"))
        .and_then(|row| cell(row, 1));
    match kind {
        Some("VIEW") => ObjectKind::View,
        Some("MANAGED" | "EXTERNAL") => ObjectKind::Table,
        _ => ObjectKind::Unknown,
    }
}

/// The affected-row count a DML statement reports (`num_affected_rows`, the
/// first column of Delta's `MERGE` / `UPDATE` / `DELETE` / `INSERT` result).
fn rows_affected(result: &QueryResult) -> Option<u64> {
    let i = result
        .columns
        .iter()
        .position(|c| c == "num_affected_rows")?;
    result
        .rows
        .first()
        .and_then(|row| cell(row, i))
        .and_then(|v| v.parse().ok())
}

#[async_trait]
impl WarehouseAdapter for SparkWarehouseAdapter {
    fn dialect(&self) -> &dyn SqlDialect {
        &self.dialect
    }

    fn is_missing_object_error(&self, error: &AdapterError) -> bool {
        error
            .inner()
            .downcast_ref::<SparkError>()
            .is_some_and(SparkError::is_missing_object)
    }

    /// `spark.sql.caseSensitive`, read from the session: `false` (Spark's
    /// default) folds identifier case, so `Orders` and `orders` name one
    /// object.
    async fn identifier_case_significance(&self) -> AdapterResult<CaseSignificance> {
        let result = self.rows("SET spark.sql.caseSensitive").await?;
        match result.rows.first().and_then(|row| cell(row, 1)) {
            Some(v) if v.eq_ignore_ascii_case("false") => Ok(CaseSignificance::Insignificant),
            Some(v) if v.eq_ignore_ascii_case("true") => Ok(CaseSignificance::Significant),
            other => Err(AdapterError::msg(format!(
                "spark.sql.caseSensitive returned an unexpected value: {other:?}"
            ))),
        }
    }

    async fn execute_statement(&self, sql: &str) -> AdapterResult<()> {
        self.run(sql).await.map(|_| ())
    }

    async fn execute_statement_with_stats(&self, sql: &str) -> AdapterResult<ExecutionStats> {
        let result = self.rows(sql).await?;
        Ok(ExecutionStats {
            rows_affected: rows_affected(&result),
            ..ExecutionStats::default()
        })
    }

    fn supports_object_kind_probe(&self) -> bool {
        true
    }

    fn classify_failure(&self, err: &AdapterError) -> FailureClass {
        let Some(spark) = err.inner().downcast_ref::<SparkError>() else {
            return FailureClass::Unknown;
        };
        match spark {
            // The connection never opened: nothing ran.
            SparkError::Connect(_) => FailureClass::Transient(TransientKind::Network),
            // Sent, then cut off or timed out: the statement may have run.
            SparkError::Timeout { .. } => FailureClass::Unknown,
            SparkError::Status { code, .. } if *code == tonic::Code::Unavailable => {
                FailureClass::Unknown
            }
            SparkError::Status { .. } | SparkError::Config(_) | SparkError::Arrow(_) => {
                FailureClass::Permanent
            }
        }
    }

    async fn execute_query(&self, sql: &str) -> AdapterResult<QueryResult> {
        self.rows(sql).await
    }

    /// Spark Connect streams Arrow natively; the batches are concatenated
    /// as they arrived.
    async fn fetch_arrow_batch(
        &self,
        sql: &str,
    ) -> AdapterResult<arrow::record_batch::RecordBatch> {
        let batches = self.run(sql).await?;
        let Some(first) = batches.first() else {
            return Err(AdapterError::msg(
                "spark returned no Arrow batch for the query (no schema to build an empty batch)",
            ));
        };
        arrow::compute::concat_batches(&first.schema(), &batches).map_err(AdapterError::new)
    }

    async fn describe_table(&self, table: &TableRef) -> AdapterResult<Vec<ColumnInfo>> {
        let target = self.table_ref(table)?;
        let result = self.rows(&self.dialect.describe_table_sql(&target)).await?;
        Ok(primary_describe_columns(&result.rows))
    }

    async fn object_kind(&self, table: &TableRef) -> AdapterResult<ObjectKind> {
        let target = self.table_ref(table)?;
        match self
            .rows(&format!("DESCRIBE TABLE EXTENDED {target}"))
            .await
        {
            Ok(result) => Ok(describe_extended_kind(&result.rows)),
            Err(e) if self.is_missing_object_error(&e) => Ok(ObjectKind::Unknown),
            Err(e) => Err(e),
        }
    }

    async fn ping(&self) -> AdapterResult<()> {
        self.prepare_session().await?;
        self.client.ping().await.map_err(wrap)
    }

    /// `EXPLAIN COST`: the optimized plan with Spark's size estimate. The
    /// estimate is a statistic, not a scan measurement, so it is reported in
    /// the raw text only.
    async fn explain(&self, sql: &str) -> AdapterResult<ExplainResult> {
        let result = self.rows(&format!("EXPLAIN COST {sql}")).await?;
        let raw = result
            .rows
            .iter()
            .filter_map(|row| cell(row, 0))
            .collect::<Vec<_>>()
            .join("\n");
        Ok(ExplainResult {
            estimated_bytes_scanned: None,
            estimated_rows: None,
            estimated_compute_units: None,
            raw_explain: raw,
        })
    }

    /// Beta: SQL generation is unit-tested and the adapter is tested against
    /// a local Spark 4.0 server with Delta Lake, but it has had no
    /// production use yet.
    fn is_experimental(&self) -> bool {
        true
    }

    /// `SHOW TABLES IN catalog.schema` (tables and views), without the
    /// session's temporary views.
    async fn list_tables(&self, catalog: &str, schema: &str) -> AdapterResult<Vec<String>> {
        rocky_sql::validation::validate_identifier(catalog).map_err(AdapterError::new)?;
        rocky_sql::validation::validate_identifier(schema).map_err(AdapterError::new)?;
        let result = self
            .rows(&format!("SHOW TABLES IN {catalog}.{schema}"))
            .await?;
        let name = result.columns.iter().position(|c| c == "tableName");
        let temporary = result.columns.iter().position(|c| c == "isTemporary");
        let Some(name) = name else {
            return Err(AdapterError::msg(
                "SHOW TABLES returned no tableName column",
            ));
        };
        Ok(result
            .rows
            .iter()
            .filter(|row| temporary.and_then(|i| cell(row, i)) != Some("true"))
            .filter_map(|row| cell(row, name).map(str::to_lowercase))
            .collect())
    }

    /// The trait default quotes with `"`, which Spark reads as a string
    /// literal. A plain copy (`CREATE OR REPLACE TABLE … AS SELECT *`) works
    /// for Delta and Iceberg alike.
    async fn clone_table_for_branch(
        &self,
        source: &TableRef,
        branch_schema: &str,
    ) -> AdapterResult<()> {
        let src = self.table_ref(source)?;
        let target =
            self.dialect
                .format_table_ref(&source.catalog, branch_schema, &source.table)?;
        self.execute_statement(
            &self
                .dialect
                .create_table_as(&target, &format!("SELECT * FROM {src}")),
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &str) -> serde_json::Value {
        serde_json::Value::String(v.into())
    }

    #[test]
    fn describe_stops_at_the_first_metadata_section() {
        let rows = vec![
            vec![s("id"), s("int"), serde_json::Value::Null],
            vec![s("#tag"), s("string"), serde_json::Value::Null],
            vec![s("day"), s("date"), serde_json::Value::Null],
            vec![s("# Partition Information"), s(""), s("")],
            vec![s("# col_name"), s("data_type"), s("comment")],
            vec![s("day"), s("date"), serde_json::Value::Null],
        ];
        let cols = primary_describe_columns(&rows);
        let names: Vec<_> = cols.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["id", "#tag", "day"]);
        assert!(cols.iter().all(|c| c.nullable));
        assert_eq!(cols[0].data_type, "int");
    }

    #[test]
    fn describe_extended_reads_the_type_row() {
        let rows = |t: &str| vec![vec![s("id"), s("int"), s("")], vec![s("Type"), s(t), s("")]];
        assert_eq!(describe_extended_kind(&rows("VIEW")), ObjectKind::View);
        assert_eq!(describe_extended_kind(&rows("MANAGED")), ObjectKind::Table);
        assert_eq!(describe_extended_kind(&rows("EXTERNAL")), ObjectKind::Table);
        assert_eq!(
            describe_extended_kind(&rows("MATERIALIZED_VIEW")),
            ObjectKind::Unknown
        );
        assert_eq!(describe_extended_kind(&[]), ObjectKind::Unknown);
    }

    #[test]
    fn rows_affected_reads_the_delta_dml_result() {
        let merge = QueryResult {
            columns: vec!["num_affected_rows".into(), "num_inserted_rows".into()],
            rows: vec![vec![s("3"), s("1")]],
        };
        assert_eq!(rows_affected(&merge), Some(3));
        let ddl = QueryResult {
            columns: vec![],
            rows: vec![],
        };
        assert_eq!(rows_affected(&ddl), None);
    }

    fn adapter() -> SparkWarehouseAdapter {
        let cfg = SparkConfig::new(
            Some("127.0.0.1:1"),
            None,
            None,
            std::time::Duration::from_secs(5),
        )
        .unwrap();
        SparkWarehouseAdapter::new(cfg, TableFormat::Delta).unwrap()
    }

    #[test]
    fn failure_classes() {
        let a = adapter();
        let class = |e: SparkError| a.classify_failure(&AdapterError::new(e));
        assert_eq!(
            class(SparkError::Connect("refused".into())),
            FailureClass::Transient(TransientKind::Network)
        );
        assert_eq!(
            class(SparkError::Timeout { secs: 1 }),
            FailureClass::Unknown
        );
        assert_eq!(
            class(SparkError::Status {
                code: tonic::Code::Unavailable,
                message: "reset".into()
            }),
            FailureClass::Unknown
        );
        assert_eq!(
            class(SparkError::Status {
                code: tonic::Code::Internal,
                message: "[PARSE_SYNTAX_ERROR]".into()
            }),
            FailureClass::Permanent
        );
        assert_eq!(
            a.classify_failure(&AdapterError::msg("other")),
            FailureClass::Unknown
        );
    }

    #[test]
    fn adapter_is_beta_and_probes_object_kind() {
        let a = adapter();
        assert!(a.is_experimental());
        assert!(a.supports_object_kind_probe());
        assert_eq!(a.dialect().name(), "spark");
    }
}
