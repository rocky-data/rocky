//! The output version identity each model execution records (RV1-P1b).
//!
//! Each execution site calls one of these right after its write. The result
//! rides on `MaterializationOutput::output_version` (never serialized) and
//! `RunOutput::to_run_record` copies it onto the persisted
//! [`rocky_core::state::ModelExecution::output_version`].
//!
//! ```text
//!   strategy has no stored data? ──yes──▶ Unversioned { fixed reason }
//!            │ no
//!   warehouse job id recorded?   ──yes──▶ WarehouseJob   (BigQuery)
//!            │ no
//!   observed_table_version(t)  ── Some(v) ─▶ DeltaObserved  (Databricks)
//!                              ── None ────▶ Unversioned { adapter_has_no_version }
//!                              ── Err ─────▶ Unversioned { observe_failed }
//! ```
//!
//! A failed version read never fails the run: the data is already written.
//! The adapter makes the read with one attempt, no retries and no draw from
//! the run's retry budget. Failed reads are summed into one warning per run
//! by [`observe_failed_summary`]. Replicated tables are not observed at all
//! ([`UnversionedReason::NotObserved`]), so replication cost is unchanged.

use chrono::{DateTime, Utc};
use rocky_core::state::{OutputVersion, UnversionedReason};
use rocky_core::traits::WarehouseAdapter;
use rocky_ir::{MaterializationStrategy, ModelIr, TableRef};
use tracing::debug;

use super::run_content_addressed::ContentAddressedRunSummary;

/// The `catalog.schema.table` name recorded on a version, unquoted. It
/// matches `MaterializationMetadata::target_table_full_name`.
fn table_name(table: &TableRef) -> String {
    format!("{}.{}.{}", table.catalog, table.schema, table.table)
}

/// The reason a strategy can never carry a data version, or `None` when the
/// strategy writes a table whose version is worth reading.
///
/// Exhaustive on purpose (no `_ =>`): a new strategy fails to compile here
/// until someone decides whether its output has a version.
pub(crate) fn fixed_unversioned_reason(
    strategy: &MaterializationStrategy,
) -> Option<UnversionedReason> {
    match strategy {
        MaterializationStrategy::View => Some(UnversionedReason::ViewHasNoStoredData),
        MaterializationStrategy::MaterializedView
        | MaterializationStrategy::DynamicTable { .. } => {
            Some(UnversionedReason::WarehouseManagedRefresh)
        }
        MaterializationStrategy::Ephemeral => Some(UnversionedReason::NoOutput),
        // Content-addressed outputs take `content_addressed_output_version`;
        // if one ever reaches here it is a table and its version is read.
        MaterializationStrategy::ContentAddressed { .. }
        | MaterializationStrategy::FullRefresh
        | MaterializationStrategy::Incremental { .. }
        | MaterializationStrategy::Merge { .. }
        | MaterializationStrategy::TimeInterval { .. }
        | MaterializationStrategy::DeleteInsert { .. }
        | MaterializationStrategy::Microbatch { .. }
        | MaterializationStrategy::Snapshot(_) => None,
    }
}

/// The version of a written table: the last warehouse job id when the
/// adapter reported one, otherwise the adapter's observed table version.
///
/// `job_ids` are the statement job ids in execution order; the last one is
/// recorded (it may not be the statement that wrote the data). `recorded_at`
/// is Rocky's clock after the write returned.
pub(crate) async fn observe_table_version(
    warehouse: &dyn WarehouseAdapter,
    table: &TableRef,
    job_ids: &[String],
    recorded_at: DateTime<Utc>,
) -> OutputVersion {
    if let Some(job_id) = job_ids.last() {
        return OutputVersion::WarehouseJob {
            table: table_name(table),
            job_id: job_id.clone(),
            recorded_at,
        };
    }
    match warehouse.observed_table_version(table).await {
        Ok(Some(version)) => OutputVersion::DeltaObserved {
            table: table_name(table),
            version,
        },
        Ok(None) => OutputVersion::Unversioned {
            reason: UnversionedReason::AdapterHasNoVersion,
        },
        Err(e) => {
            // One summary warning per run comes from `observe_failed_summary`.
            debug!(
                table = %table_name(table),
                error = %e,
                "could not read the output version after the write; \
                 recording observe_failed (the run is not affected)"
            );
            OutputVersion::Unversioned {
                reason: UnversionedReason::ObserveFailed,
            }
        }
    }
}

/// The version of one model's output: a fixed reason when the strategy
/// stores no data, otherwise [`observe_table_version`].
pub(crate) async fn observe_output_version(
    warehouse: &dyn WarehouseAdapter,
    strategy: &MaterializationStrategy,
    table: &TableRef,
    job_ids: &[String],
    recorded_at: DateTime<Utc>,
) -> OutputVersion {
    match fixed_unversioned_reason(strategy) {
        Some(reason) => OutputVersion::Unversioned { reason },
        None => observe_table_version(warehouse, table, job_ids, recorded_at).await,
    }
}

/// The version a replicated table records. Replication sends no version
/// query, so its cost is unchanged.
pub(crate) fn replication_output_version() -> OutputVersion {
    OutputVersion::Unversioned {
        reason: UnversionedReason::NotObserved,
    }
}

/// One warning line for every failed version read in a run, or `None` when
/// no read failed. Names the count and the first three tables.
pub(crate) fn observe_failed_summary<'a>(
    versions: impl IntoIterator<Item = (&'a str, Option<&'a OutputVersion>)>,
) -> Option<String> {
    let failed: Vec<&str> = versions
        .into_iter()
        .filter(|(_, v)| {
            matches!(
                v,
                Some(OutputVersion::Unversioned {
                    reason: UnversionedReason::ObserveFailed
                })
            )
        })
        .map(|(name, _)| name)
        .collect();
    if failed.is_empty() {
        return None;
    }
    let shown = failed
        .iter()
        .take(3)
        .copied()
        .collect::<Vec<_>>()
        .join(", ");
    let more = failed.len().saturating_sub(3);
    let tail = if more > 0 {
        format!(" and {more} more")
    } else {
        String::new()
    };
    Some(format!(
        "could not read the output version of {} model(s) after the write ({shown}{tail}); \
         recorded observe_failed, the run is not affected",
        failed.len()
    ))
}

/// The version of a content-addressed write: the one Delta version whose
/// snapshot is exactly this output, and the blake3 of every file (folded for
/// a partitioned table).
///
/// The run makes one replace commit (RV1-P1a), so `delta_versions` holds
/// exactly one entry: that commit, or the current head when the output was
/// already live and no commit was written.
pub(crate) fn content_addressed_output_version(
    model_ir: &ModelIr,
    summary: &ContentAddressedRunSummary,
) -> OutputVersion {
    let partitioned = matches!(
        &model_ir.materialization,
        MaterializationStrategy::ContentAddressed { partition_columns, .. }
            if !partition_columns.is_empty()
    );
    let table = TableRef {
        catalog: model_ir.target.catalog.clone(),
        schema: model_ir.target.schema.clone(),
        table: model_ir.target.table.clone(),
    };
    OutputVersion::content_addressed(
        table_name(&table),
        vec![summary.commit_version],
        summary
            .written_files
            .iter()
            .map(|f| f.blake3_hash.clone())
            .collect(),
        partitioned,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocky_core::traits::{AdapterError, AdapterResult, QueryResult, SqlDialect};
    use rocky_ir::ColumnInfo;

    /// A warehouse whose only interesting answer is the version read.
    struct VersionWarehouse {
        dialect_source: crate::testing::RecordingWarehouseAdapter,
        answer: Result<Option<u64>, String>,
        calls: std::sync::atomic::AtomicUsize,
    }

    impl VersionWarehouse {
        fn new(answer: Result<Option<u64>, String>) -> Self {
            Self {
                dialect_source: crate::testing::RecordingWarehouseAdapter::new(
                    "run_output_version::tests",
                ),
                answer,
                calls: std::sync::atomic::AtomicUsize::new(0),
            }
        }
        fn calls(&self) -> usize {
            self.calls.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl WarehouseAdapter for VersionWarehouse {
        fn dialect(&self) -> &dyn SqlDialect {
            self.dialect_source.dialect()
        }
        async fn execute_statement(&self, _sql: &str) -> AdapterResult<()> {
            Ok(())
        }
        async fn execute_query(&self, _sql: &str) -> AdapterResult<QueryResult> {
            Ok(QueryResult {
                columns: vec![],
                rows: vec![],
            })
        }
        async fn describe_table(&self, _table: &TableRef) -> AdapterResult<Vec<ColumnInfo>> {
            Ok(Vec::new())
        }
        async fn observed_table_version(&self, _table: &TableRef) -> AdapterResult<Option<u64>> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.answer.clone().map_err(AdapterError::msg)
        }
    }

    fn table() -> TableRef {
        TableRef {
            catalog: "c".into(),
            schema: "s".into(),
            table: "t".into(),
        }
    }

    #[tokio::test]
    async fn an_observed_version_is_delta_observed() {
        let wh = VersionWarehouse::new(Ok(Some(9)));
        let v = observe_output_version(
            &wh,
            &MaterializationStrategy::FullRefresh,
            &table(),
            &[],
            Utc::now(),
        )
        .await;
        assert_eq!(
            v,
            OutputVersion::DeltaObserved {
                table: "c.s.t".into(),
                version: 9
            }
        );
    }

    #[tokio::test]
    async fn no_version_is_adapter_has_no_version() {
        let wh = VersionWarehouse::new(Ok(None));
        let v = observe_table_version(&wh, &table(), &[], Utc::now()).await;
        assert_eq!(
            v,
            OutputVersion::Unversioned {
                reason: UnversionedReason::AdapterHasNoVersion
            }
        );
    }

    /// A failed read is recorded, never raised: the helper has no error path.
    #[tokio::test]
    async fn a_failed_read_is_observe_failed() {
        let wh = VersionWarehouse::new(Err("DESCRIBE HISTORY failed".into()));
        let v = observe_table_version(&wh, &table(), &[], Utc::now()).await;
        assert_eq!(
            v,
            OutputVersion::Unversioned {
                reason: UnversionedReason::ObserveFailed
            }
        );
    }

    /// BigQuery: the last job id wins, and no version query is sent.
    #[tokio::test]
    async fn a_job_id_is_warehouse_job() {
        let wh = VersionWarehouse::new(Ok(Some(1)));
        let ended_at: DateTime<Utc> = "2026-10-06T10:00:00Z".parse().unwrap();
        let v = observe_output_version(
            &wh,
            &MaterializationStrategy::FullRefresh,
            &table(),
            &["job_a".to_string(), "job_b".to_string()],
            ended_at,
        )
        .await;
        assert_eq!(
            v,
            OutputVersion::WarehouseJob {
                table: "c.s.t".into(),
                job_id: "job_b".into(),
                recorded_at: ended_at
            }
        );
        assert_eq!(wh.calls(), 0);
    }

    /// Strategies with no stored data get their fixed reason without a
    /// version query, even when a job id exists.
    #[tokio::test]
    async fn no_stored_data_strategies_never_query() {
        let wh = VersionWarehouse::new(Ok(Some(1)));
        for (strategy, reason) in [
            (
                MaterializationStrategy::View,
                UnversionedReason::ViewHasNoStoredData,
            ),
            (
                MaterializationStrategy::MaterializedView,
                UnversionedReason::WarehouseManagedRefresh,
            ),
            (
                MaterializationStrategy::Ephemeral,
                UnversionedReason::NoOutput,
            ),
        ] {
            let v =
                observe_output_version(&wh, &strategy, &table(), &["job".into()], Utc::now()).await;
            assert_eq!(v, OutputVersion::Unversioned { reason });
        }
        assert_eq!(wh.calls(), 0);
    }

    /// Failed reads collapse into one line with the count and the first
    /// three tables.
    #[test]
    fn observe_failed_summary_names_count_and_first_tables() {
        let failed = OutputVersion::Unversioned {
            reason: UnversionedReason::ObserveFailed,
        };
        let ok = OutputVersion::DeltaObserved {
            table: "t".into(),
            version: 1,
        };
        assert_eq!(
            observe_failed_summary([("a", Some(&ok)), ("b", None)]),
            None
        );
        let line = observe_failed_summary([
            ("a", Some(&failed)),
            ("b", Some(&ok)),
            ("c", Some(&failed)),
            ("d", Some(&failed)),
            ("e", Some(&failed)),
        ])
        .unwrap();
        assert!(line.contains("4 model(s)"), "{line}");
        assert!(line.contains("(a, c, d and 1 more)"), "{line}");
    }

    // --- Through the real `run()` entry point ----------------------------

    #[cfg(feature = "duckdb")]
    mod through_run {
        use super::super::super::run::{DeferOptions, PartitionRunOptions, SkipRunOptions, run};
        use rocky_core::state::{OutputVersion, StateStore, UnversionedReason};

        /// Write a transformation project, run it, and return the recorded
        /// executions of the newest run, keyed by model name.
        async fn run_project(
            adapter_toml: &str,
            models: &[(&str, &str, &str)],
            setup: impl FnOnce(&std::path::Path),
        ) -> (
            anyhow::Result<()>,
            std::collections::BTreeMap<String, rocky_core::state::ModelExecution>,
            String,
            tempfile::TempDir,
        ) {
            run_project_with(
                adapter_toml,
                None,
                models,
                setup,
                PartitionRunOptions::default(),
            )
            .await
        }

        /// [`run_project`] with a custom pipeline section (`None` keeps the
        /// default `tx` transformation pipeline over `models/`) and
        /// partition options.
        async fn run_project_with(
            adapter_toml: &str,
            pipeline_toml: Option<&str>,
            models: &[(&str, &str, &str)],
            setup: impl FnOnce(&std::path::Path),
            opts: PartitionRunOptions,
        ) -> (
            anyhow::Result<()>,
            std::collections::BTreeMap<String, rocky_core::state::ModelExecution>,
            String,
            tempfile::TempDir,
        ) {
            let tmp = tempfile::TempDir::new().unwrap();
            let dir = tmp.path();
            let models_dir = dir.join("models");
            std::fs::create_dir_all(&models_dir).unwrap();
            for (name, sql, toml) in models {
                std::fs::write(models_dir.join(format!("{name}.sql")), sql).unwrap();
                std::fs::write(models_dir.join(format!("{name}.toml")), toml).unwrap();
            }
            let config_path = dir.join("rocky.toml");
            std::fs::write(
                &config_path,
                match pipeline_toml {
                    Some(pipeline) => {
                        format!("{adapter_toml}\n[state]\nbackend = \"local\"\n\n{pipeline}")
                    }
                    None => format!(
                        "{adapter_toml}\n[state]\nbackend = \"local\"\n\n\
                         [pipeline.tx]\ntype = \"transformation\"\nmodels = '{}'\n\n\
                         [pipeline.tx.target]\nadapter = \"default\"\n",
                        models_dir.join("**").display(),
                    ),
                },
            )
            .unwrap();
            let state_path = dir.join("state.redb");
            setup(&state_path);
            let run_id = format!("rv1-p1b-{}", uuid::Uuid::new_v4());
            let loaded = std::sync::Arc::new(
                rocky_core::config::load_rocky_config_fingerprinted(&config_path).unwrap(),
            );
            let result = run(
                &config_path,
                loaded,
                None,
                None,
                &state_path,
                None,
                true,
                None,
                false,
                None,
                false,
                None,
                &opts,
                None,
                None,
                None,
                None,
                &DeferOptions::default(),
                &SkipRunOptions::default(),
                &rocky_core::run_vars::RunVars::new(),
                Some(&run_id),
                None,
                false,
                None,
                &rocky_core::config::PrincipalRef::unnamed(),
            )
            .await
            .map(|_| ());
            super::super::super::run::CAPTURED_RUN_OUTPUT_FOR_TEST
                .lock()
                .unwrap()
                .remove(&run_id);
            let state = StateStore::open(&state_path).unwrap();
            let latest = state.list_runs(1).unwrap().remove(0);
            let run_id = latest.run_id.clone();
            let by_name = latest
                .models_executed
                .into_iter()
                .map(|e| (e.model_name.clone(), e))
                .collect();
            drop(state);
            (result, by_name, run_id, tmp)
        }

        /// Seed a `v=0` bootstrap for the one-column fixture table the
        /// `test-fail-write` adapter answers (`content_addressed_failure_probe`).
        async fn seed_content_addressed_table(
            store: &object_store::memory::InMemory,
            prefix: &str,
        ) {
            use object_store::{ObjectStoreExt as _, PutPayload, path::Path as ObjPath};
            let protocol = serde_json::json!({"protocol": {
                "minReaderVersion": 2,
                "minWriterVersion": 7,
                "writerFeatures": ["columnMapping", "icebergCompatV2", "invariants", "appendOnly"]
            }});
            let metadata = serde_json::json!({"metaData": {
                "id": "00000000-0000-0000-0000-000000000000",
                "format": {"provider": "parquet", "options": {}},
                "schemaString": serde_json::to_string(&serde_json::json!({
                    "type": "struct", "fields": [{
                        "name": "content_addressed_failure_probe", "type": "long",
                        "nullable": false, "metadata": {
                            "delta.columnMapping.id": 1,
                            "delta.columnMapping.physicalName": "col-id-uuid"
                        }
                    }]
                })).unwrap(),
                "partitionColumns": [],
                "configuration": {
                    "delta.columnMapping.mode": "name",
                    "delta.universalFormat.enabledFormats": "iceberg",
                    "delta.enableIcebergCompatV2": "true"
                },
                "createdTime": 0
            }});
            let body = format!("{protocol}\n{metadata}\n");
            store
                .put(
                    &ObjPath::from(format!("{prefix}/_delta_log/00000000000000000000.json")),
                    PutPayload::from(body.into_bytes()),
                )
                .await
                .unwrap();
        }

        const MAIN: &str = "[target]\ncatalog = \"\"\nschema = \"main\"\n";

        fn strategy(kind: &str) -> String {
            format!("[strategy]\ntype = \"{kind}\"\n\n{MAIN}")
        }

        fn unversioned(reason: UnversionedReason) -> Option<OutputVersion> {
            Some(OutputVersion::Unversioned { reason })
        }

        /// DuckDB offline: a table has no version, a view stores no data,
        /// and an ephemeral model is never executed, so it records nothing.
        #[tokio::test]
        async fn duckdb_records_unversioned_with_the_right_reason() {
            let full_refresh = strategy("full_refresh");
            let view = strategy("view");
            let ephemeral = strategy("ephemeral");
            let models = [
                ("fr", "SELECT 1 AS x\n", full_refresh.as_str()),
                ("v", "SELECT 2 AS y\n", view.as_str()),
                ("e", "SELECT 3 AS z\n", ephemeral.as_str()),
            ];
            let db = tempfile::TempDir::new().unwrap();
            let adapter = format!(
                "[adapter]\ntype = \"duckdb\"\npath = \"{}\"\n",
                db.path().join("w.duckdb").display()
            );
            let (result, execs, _, _tmp) = run_project(&adapter, &models, |_| {}).await;
            result.expect("the DuckDB run must succeed");
            assert_eq!(
                execs["fr"].output_version,
                unversioned(UnversionedReason::AdapterHasNoVersion)
            );
            assert_eq!(
                execs["v"].output_version,
                unversioned(UnversionedReason::ViewHasNoStoredData)
            );
            assert!(
                !execs.contains_key("e"),
                "an ephemeral model is inlined, never executed: {:?}",
                execs.keys().collect::<Vec<_>>()
            );
        }

        /// A Delta-style adapter: the version read after the write lands on
        /// the record as `delta_observed`. A view still sends no query.
        #[tokio::test]
        async fn an_observed_version_is_recorded_as_delta_observed() {
            let full_refresh = strategy("full_refresh");
            let view = strategy("view");
            let models = [
                ("fr", "SELECT 1 AS x\n", full_refresh.as_str()),
                ("v", "SELECT 2 AS y\n", view.as_str()),
            ];
            let adapter = "[adapter]\ntype = \"test-fail-write\"\npath = \"observe-version\"\n";
            let (result, execs, _, _tmp) = run_project(adapter, &models, |_| {}).await;
            result.expect("the run must succeed");
            assert_eq!(
                execs["fr"].output_version,
                Some(OutputVersion::DeltaObserved {
                    table: ".main.fr".into(),
                    version: 42
                })
            );
            assert_eq!(
                execs["v"].output_version,
                unversioned(UnversionedReason::ViewHasNoStoredData)
            );
        }

        /// A failed version read never fails the run. It is recorded.
        #[tokio::test]
        async fn a_failed_version_read_does_not_fail_the_run() {
            let full_refresh = strategy("full_refresh");
            let models = [("fr", "SELECT 1 AS x\n", full_refresh.as_str())];
            let adapter = "[adapter]\ntype = \"test-fail-write\"\npath = \"observe-fail\"\n";
            let (result, execs, _, _tmp) = run_project(adapter, &models, |_| {}).await;
            result.expect("a failed version read must not fail the run");
            assert_eq!(execs["fr"].status, "success");
            assert_eq!(
                execs["fr"].output_version,
                unversioned(UnversionedReason::ObserveFailed)
            );
        }

        fn delta_observed(table: &str) -> Option<OutputVersion> {
            Some(OutputVersion::DeltaObserved {
                table: table.into(),
                version: 42,
            })
        }

        /// Replication (`process_table`) records `not_observed` and sends no
        /// version query, so its cost is unchanged.
        #[tokio::test]
        async fn replication_records_not_observed() {
            use rocky_core::traits::WarehouseAdapter;
            let db_dir = tempfile::TempDir::new().unwrap();
            let db = db_dir.path().join("warehouse.duckdb");
            {
                let a = rocky_duckdb::adapter::DuckDbWarehouseAdapter::open(&db).unwrap();
                a.execute_statement("CREATE SCHEMA raw__acme")
                    .await
                    .unwrap();
                a.execute_statement("CREATE TABLE raw__acme.orders AS SELECT 1 AS id")
                    .await
                    .unwrap();
            }
            let adapter = format!(
                "[adapter]\ntype = \"duckdb\"\npath = \"{}\"\n",
                db.display()
            );
            let pipeline = "[pipeline.p1]\ntype = \"replication\"\nstrategy = \"full_refresh\"\n\n\
                 [pipeline.p1.source.discovery]\nadapter = \"default\"\n\n\
                 [pipeline.p1.source.schema_pattern]\nprefix = \"raw__\"\nseparator = \"__\"\n\
                 components = [\"source\"]\n\n\
                 [pipeline.p1.target]\nadapter = \"default\"\ncatalog_template = \"warehouse\"\n\
                 schema_template = \"staging__{source}\"\n\n\
                 [pipeline.p1.target.governance]\nauto_create_schemas = true\n";
            let (result, execs, _, _tmp) = run_project_with(
                &adapter,
                Some(pipeline),
                &[],
                |_| {},
                PartitionRunOptions::default(),
            )
            .await;
            result.expect("the replication run must succeed");
            assert_eq!(
                execs["orders"].output_version,
                unversioned(UnversionedReason::NotObserved),
                "{:?}",
                execs.keys().collect::<Vec<_>>()
            );
        }

        /// A snapshot model (`execute_snapshot_model`) records the version
        /// read after its write.
        #[tokio::test]
        async fn snapshot_model_records_its_version() {
            let toml = format!(
                "[strategy]\ntype = \"snapshot\"\nunique_key = \"id\"\nstrategy = \"timestamp\"\n\
                 updated_at = \"updated_at\"\n\n{MAIN}"
            );
            let models = [(
                "snap",
                "SELECT 1 AS id, TIMESTAMP '2026-01-01 00:00:00' AS updated_at\n",
                toml.as_str(),
            )];
            let adapter = "[adapter]\ntype = \"test-fail-write\"\npath = \"observe-version\"\n";
            let (result, execs, _, _tmp) = run_project(adapter, &models, |_| {}).await;
            result.expect("the snapshot model run must succeed");
            assert_eq!(execs["snap"].output_version, delta_observed(".main.snap"));
        }

        /// A time_interval partition (`run_one_partition`) records the
        /// version read after its write.
        #[tokio::test]
        async fn time_interval_partition_records_its_version() {
            let toml = format!(
                "[strategy]\ntype = \"time_interval\"\ntime_column = \"order_date\"\n\
                 granularity = \"day\"\nfirst_partition = \"2026-01-01\"\n\n{MAIN}"
            );
            let models = [(
                "ti",
                "SELECT CAST(TIMESTAMP '2026-01-01 12:00:00' AS DATE) AS order_date \
                 WHERE TIMESTAMP '2026-01-01 12:00:00' >= @start_date \
                 AND TIMESTAMP '2026-01-01 12:00:00' < @end_date\n",
                toml.as_str(),
            )];
            let adapter = "[adapter]\ntype = \"test-fail-write\"\npath = \"observe-version\"\n";
            let opts = PartitionRunOptions {
                latest: true,
                ..PartitionRunOptions::default()
            };
            let (result, execs, _, _tmp) =
                run_project_with(adapter, None, &models, |_| {}, opts).await;
            result.expect("the time_interval run must succeed");
            assert_eq!(execs["ti"].output_version, delta_observed(".main.ti"));
        }

        /// A `snapshot` pipeline (`run_local::run_snapshot`) records the
        /// version read after its write.
        #[tokio::test]
        async fn snapshot_pipeline_records_its_version() {
            let adapter = "[adapter]\ntype = \"test-fail-write\"\npath = \"accept-all\"\n";
            let pipeline = "[pipeline.snap]\ntype = \"snapshot\"\nunique_key = [\"id\"]\n\
                 updated_at = \"updated_at\"\n\n\
                 [pipeline.snap.source]\ncatalog = \"c\"\nschema = \"raw\"\ntable = \"customers\"\n\n\
                 [pipeline.snap.target]\ncatalog = \"c\"\nschema = \"snapshots\"\ntable = \"hist\"\n";
            let (result, execs, _, _tmp) = run_project_with(
                adapter,
                Some(pipeline),
                &[],
                |_| {},
                PartitionRunOptions::default(),
            )
            .await;
            result.expect("the snapshot pipeline run must succeed");
            assert_eq!(
                execs["hist"].output_version,
                delta_observed("c.snapshots.hist")
            );
        }

        /// content_addressed: the record names the Delta commit Rocky made
        /// and the blake3 of the file it wrote, the same hash the artifact
        /// ledger holds.
        #[tokio::test]
        async fn content_addressed_records_the_commit_and_the_hash() {
            use object_store::memory::InMemory;

            let prefix = format!("rv1_p1b_{}", uuid::Uuid::new_v4().simple());
            let storage_prefix = format!("s3://test-bucket/{prefix}");
            let store = std::sync::Arc::new(InMemory::new());
            seed_content_addressed_table(&store, &prefix).await;
            super::super::super::run_content_addressed::register_test_object_store(
                &storage_prefix,
                store.clone(),
            );
            let toml = format!(
                "[[sources]]\ncatalog = \"c\"\nschema = \"raw\"\ntable = \"events\"\n\n\
                 [strategy]\ntype = \"content_addressed\"\nstorage_prefix = \"{storage_prefix}\"\n\n\
                 [target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"m\"\n"
            );
            let models = [(
                "m",
                "SELECT content_addressed_failure_probe FROM raw.events\n",
                toml.as_str(),
            )];
            let adapter = "[adapter]\ntype = \"test-fail-write\"\npath = \"content-ok\"\n";
            let (result, execs, run_id, tmp) = run_project(adapter, &models, |state_path| {
                use rocky_core::schema_cache::{SchemaCacheEntry, StoredColumn, schema_cache_key};
                let state = StateStore::open(state_path).unwrap();
                state
                    .write_schema_cache_entry(
                        &schema_cache_key("c", "raw", "events"),
                        &SchemaCacheEntry {
                            columns: vec![StoredColumn {
                                name: "content_addressed_failure_probe".to_string(),
                                data_type: "BIGINT".to_string(),
                                nullable: false,
                            }],
                            cached_at: chrono::Utc::now(),
                        },
                    )
                    .unwrap();
            })
            .await;
            super::super::super::run_content_addressed::remove_test_object_store(&storage_prefix);
            result.expect("the content-addressed run must succeed");

            let state = StateStore::open(&tmp.path().join("state.redb")).unwrap();
            let artifacts = state.list_artifacts_for_run(&run_id).unwrap();
            assert_eq!(artifacts.len(), 1, "{artifacts:?}");
            let artifact = &artifacts[0];
            assert_eq!(
                execs["m"].output_version,
                Some(OutputVersion::ContentAddressed {
                    table: "c.s.m".into(),
                    delta_versions: vec![1],
                    blake3: artifact.blake3_hash.clone(),
                    files: vec![artifact.blake3_hash.clone()],
                })
            );
            assert_eq!(artifact.commit_version, 1);
        }
    }
}
