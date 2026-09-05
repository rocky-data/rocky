//! Test-only warehouse doubles shared across `rocky-cli`'s unit tests.
//!
//! Compiled **only** under `cfg(test)` — `lib.rs` declares this module behind
//! that gate, and so does the `"recording"` arm in
//! [`crate::registry::AdapterRegistry::from_config`]. A released `rocky` binary
//! contains none of it, and `KNOWN_ADAPTER_TYPES` is untouched, so
//! `rocky validate`'s V017 and the "did you mean?" suggester still see exactly
//! the nine real adapter types.
//!
//! # Why this exists (#1609)
//!
//! `rocky run` refuses some configurations before it touches the warehouse.
//! Proving a refusal landed *early* used to mean reading control flow, then
//! checking one DuckDB observation — "does the target schema exist?". That
//! observation cannot distinguish "nothing was issued" from "the catalog step
//! ran and the schema step did not", because DuckDB's
//! [`SqlDialect::create_catalog_sql`] returns `None` and there is no catalog
//! statement to observe.
//!
//! [`RecordingWarehouseAdapter`] answers the question directly: it records
//! every call in order and executes nothing, and its dialect **does** emit a
//! `CREATE CATALOG` statement. A run that refuses before touching the
//! warehouse leaves an empty log; a run that does not leaves the catalog
//! statement first.
//!
//! ```text
//!   [adapter.rec] type = "recording"     ── registry (cfg(test) arm) ──┐
//!   path = "<unique key>"                                             ▼
//!   test ── recorder("<unique key>") ─────────────────────► Arc<CallLog>
//!                                        (same Arc, keyed by `path`)
//! ```
//!
//! The log is keyed by the adapter's `path`, so two tests running
//! concurrently in the same process never share one — give each test a unique
//! key (a temp-dir path is convenient and already unique).

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};

use chrono::{DateTime, Utc};
use rocky_core::traits::{
    AdapterError, AdapterResult, ExecutionStats, LiteralEscape, QueryResult, SqlDialect,
    WarehouseAdapter,
};
use rocky_ir::{ColumnInfo, ColumnSelection, MetadataColumn, TableRef};

/// One call the run made on the warehouse adapter, in the order it was made.
///
/// `ExecuteStatement` is the write surface: catalog creation, schema creation,
/// pre-drops, `CREATE TABLE AS`, `INSERT`, `MERGE`. Tag / workspace-binding /
/// grant emission on a real warehouse also lands here, because the governance
/// adapters build SQL and hand it to `execute_statement`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AdapterCall {
    /// `execute_statement` / `execute_statement_with_stats` — the write surface.
    ExecuteStatement(String),
    /// `execute_query` — a read.
    ExecuteQuery(String),
    /// `describe_table` — a metadata read.
    DescribeTable(String),
    /// `list_tables` — a metadata read.
    ListTables { catalog: String, schema: String },
}

/// An ordered, shared record of what a run asked the warehouse to do.
#[derive(Debug, Default)]
pub(crate) struct CallLog(Mutex<Vec<AdapterCall>>);

impl CallLog {
    fn push(&self, call: AdapterCall) {
        self.0.lock().expect("call log mutex").push(call);
    }

    /// Every call, in order.
    pub(crate) fn calls(&self) -> Vec<AdapterCall> {
        self.0.lock().expect("call log mutex").clone()
    }

    /// Only the statements the run executed, in order — the write surface.
    pub(crate) fn statements(&self) -> Vec<String> {
        self.calls()
            .into_iter()
            .filter_map(|c| match c {
                AdapterCall::ExecuteStatement(sql) => Some(sql),
                _ => None,
            })
            .collect()
    }
}

/// Every recorder handed out so far, keyed by the adapter's configured `path`.
static RECORDERS: LazyLock<Mutex<HashMap<String, Arc<CallLog>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// The shared [`CallLog`] for `key`, creating it if this is the first ask.
///
/// The registry calls this with the adapter's `path`; the test calls it with
/// the same string to read the log back. Keys must be unique per test — the
/// map is process-global and `cargo test` runs tests in parallel threads.
pub(crate) fn recorder(key: &str) -> Arc<CallLog> {
    let mut map = RECORDERS.lock().expect("recorder registry mutex");
    Arc::clone(map.entry(key.to_string()).or_default())
}

/// A warehouse adapter that records every call and executes nothing.
///
/// Deliberately **not** a decorator over a real adapter. A decorator has to
/// mirror whichever `SqlDialect` methods the wrapped adapter overrides, and a
/// method it forgets silently changes the SQL under test. This double owns its
/// own dialect, so what it emits is stated here and nowhere else.
///
/// Reads (`execute_query`, `describe_table`, `list_tables`) are recorded and
/// then refused. Refusing rather than answering keeps the double honest — it
/// holds no data — and the refusals are on paths the caller already handles:
/// `run`'s source-table listing falls back to the discovered table set, and the
/// transformation existence probe treats a non-retryable failure as "absent".
pub(crate) struct RecordingWarehouseAdapter {
    log: Arc<CallLog>,
    dialect: RecordingDialect,
}

impl RecordingWarehouseAdapter {
    /// Build an adapter recording into the log registered under `key`.
    pub(crate) fn new(key: &str) -> Self {
        Self {
            log: recorder(key),
            dialect: RecordingDialect,
        }
    }
}

#[async_trait::async_trait]
impl WarehouseAdapter for RecordingWarehouseAdapter {
    fn dialect(&self) -> &dyn SqlDialect {
        &self.dialect
    }

    async fn execute_statement(&self, sql: &str) -> AdapterResult<()> {
        self.log
            .push(AdapterCall::ExecuteStatement(sql.to_string()));
        Ok(())
    }

    async fn execute_statement_with_stats(&self, sql: &str) -> AdapterResult<ExecutionStats> {
        self.execute_statement(sql).await?;
        Ok(ExecutionStats::default())
    }

    async fn execute_query(&self, sql: &str) -> AdapterResult<QueryResult> {
        self.log.push(AdapterCall::ExecuteQuery(sql.to_string()));
        Err(AdapterError::msg(
            "the recording adapter holds no data and does not answer queries",
        ))
    }

    async fn describe_table(&self, table: &TableRef) -> AdapterResult<Vec<ColumnInfo>> {
        self.log.push(AdapterCall::DescribeTable(table.full_name()));
        Err(AdapterError::msg(
            "the recording adapter holds no tables to describe",
        ))
    }

    async fn list_tables(&self, catalog: &str, schema: &str) -> AdapterResult<Vec<String>> {
        self.log.push(AdapterCall::ListTables {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
        });
        Err(AdapterError::msg(
            "the recording adapter holds no tables to list",
        ))
    }

    /// One statement at a time, so a recorded log is a total order rather than
    /// an interleaving.
    fn supports_concurrent_execution(&self) -> bool {
        false
    }
}

/// The dialect the recording adapter reports.
///
/// The one thing it does that DuckDB's dialect does not is answer
/// [`SqlDialect::create_catalog_sql`] with `Some`. That is the whole point: a
/// catalog-bearing warehouse issues a catalog statement *before* the schema
/// statement, so a guard that lands between the two is observable here and is
/// not observable on DuckDB.
struct RecordingDialect;

impl SqlDialect for RecordingDialect {
    fn literal_escape(&self) -> LiteralEscape {
        LiteralEscape::Standard
    }

    fn format_table_ref(&self, catalog: &str, schema: &str, table: &str) -> AdapterResult<String> {
        rocky_sql::validation::format_table_ref(catalog, schema, table).map_err(AdapterError::new)
    }

    fn create_table_as(&self, target: &str, select_sql: &str) -> String {
        format!("CREATE OR REPLACE TABLE {target} AS\n{select_sql}")
    }

    fn insert_into(&self, target: &str, select_sql: &str) -> String {
        format!("INSERT INTO {target}\n{select_sql}")
    }

    fn merge_into(
        &self,
        target: &str,
        source_sql: &str,
        _keys: &[Arc<str>],
        _update_cols: &ColumnSelection,
    ) -> AdapterResult<String> {
        Ok(format!("MERGE INTO {target} USING ({source_sql}) s"))
    }

    fn select_clause(
        &self,
        columns: &ColumnSelection,
        metadata: &[MetadataColumn],
    ) -> AdapterResult<String> {
        let mut sql = String::from("SELECT ");
        match columns {
            ColumnSelection::All => sql.push('*'),
            ColumnSelection::Explicit(cols) => {
                for col in cols.iter() {
                    rocky_sql::validation::validate_identifier(col).map_err(AdapterError::new)?;
                }
                sql.push_str(&cols.join(", "));
            }
        }
        // Same three guards every real dialect applies (#1594). A double that
        // skipped them would let a test pass on SQL no shipped adapter emits.
        for mc in metadata {
            rocky_sql::validation::validate_identifier(mc.name()).map_err(AdapterError::new)?;
            rocky_core::sql_gen::validate_sql_type(mc.data_type()).map_err(AdapterError::new)?;
            rocky_sql::validation::reject_statement_terminator(
                "metadata_columns[].value",
                mc.value(),
            )
            .map_err(AdapterError::new)?;
            sql.push_str(&format!(
                ", CAST({} AS {}) AS {}",
                mc.value(),
                mc.data_type(),
                mc.name()
            ));
        }
        Ok(sql)
    }

    fn watermark_where(
        &self,
        timestamp_col: &str,
        last_watermark: Option<&DateTime<Utc>>,
    ) -> AdapterResult<String> {
        let literal = last_watermark
            .map(|t| t.format("%Y-%m-%d %H:%M:%S%.f").to_string())
            .unwrap_or_else(|| "1970-01-01 00:00:00".to_string());
        Ok(format!("WHERE {timestamp_col} > TIMESTAMP '{literal}'"))
    }

    fn describe_table_sql(&self, table_ref: &str) -> String {
        format!("DESCRIBE {table_ref}")
    }

    fn drop_table_sql(&self, table_ref: &str) -> String {
        format!("DROP TABLE IF EXISTS {table_ref}")
    }

    /// `Some`, unlike DuckDB — see the type's doc comment.
    fn create_catalog_sql(&self, name: &str) -> Option<AdapterResult<String>> {
        Some(
            rocky_sql::validation::validate_identifier(name)
                .map_err(AdapterError::new)
                .map(|_| format!("CREATE CATALOG IF NOT EXISTS \"{name}\"")),
        )
    }

    fn create_schema_sql(&self, catalog: &str, schema: &str) -> Option<AdapterResult<String>> {
        Some(
            rocky_sql::validation::validate_identifier(catalog)
                .and_then(|_| rocky_sql::validation::validate_identifier(schema))
                .map_err(AdapterError::new)
                .map(|_| format!("CREATE SCHEMA IF NOT EXISTS \"{catalog}\".\"{schema}\"")),
        )
    }

    fn tablesample_clause(&self, percent: u32) -> Option<String> {
        Some(format!("TABLESAMPLE ({percent} PERCENT)"))
    }

    fn insert_overwrite_partition(
        &self,
        target: &str,
        partition_filter: &str,
        select_sql: &str,
    ) -> AdapterResult<Vec<String>> {
        Ok(vec![format!(
            "INSERT INTO {target} REPLACE WHERE {partition_filter}\n{select_sql}"
        )])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The double records in order, and separates the write surface from reads.
    #[tokio::test]
    async fn the_recorder_keeps_calls_in_order_and_separates_writes_from_reads() {
        let log = recorder("testing::order-probe");
        let adapter = RecordingWarehouseAdapter::new("testing::order-probe");

        adapter.execute_statement("CREATE CATALOG c").await.unwrap();
        let _ = adapter.execute_query("SELECT 1").await;
        adapter.execute_statement("CREATE SCHEMA s").await.unwrap();

        assert_eq!(
            log.calls(),
            vec![
                AdapterCall::ExecuteStatement("CREATE CATALOG c".into()),
                AdapterCall::ExecuteQuery("SELECT 1".into()),
                AdapterCall::ExecuteStatement("CREATE SCHEMA s".into()),
            ]
        );
        assert_eq!(
            log.statements(),
            vec!["CREATE CATALOG c", "CREATE SCHEMA s"]
        );
    }

    /// The property the #1594 test leans on: this dialect answers
    /// `create_catalog_sql`, so a catalog statement exists to be counted.
    #[test]
    fn the_recording_dialect_emits_a_catalog_statement() {
        let recording = RecordingDialect.create_catalog_sql("fixture");
        assert!(
            recording.is_some_and(|r| r.is_ok_and(|sql| sql.contains("CREATE CATALOG"))),
            "the recording dialect must emit a catalog statement"
        );
    }

    /// The grounding for the sentence above: DuckDB emits none, which is why a
    /// DuckDB-only observation cannot tell "no statement was issued" from "the
    /// catalog statement was issued and the schema statement was not".
    #[cfg(feature = "duckdb")]
    #[test]
    fn duckdb_emits_no_catalog_statement_which_is_why_this_double_exists() {
        use rocky_core::traits::SqlDialect as _;
        assert!(
            rocky_duckdb::dialect::DuckDbSqlDialect
                .create_catalog_sql("fixture")
                .is_none(),
            "DuckDB gained a catalog statement — the justification for this \
             double's own dialect needs rewriting, not deleting"
        );
    }
}
