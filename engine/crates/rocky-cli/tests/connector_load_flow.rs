//! `rocky load --connector`, the whole flow on DuckDB: claim, run, stage,
//! drift, promote, commit.
//!
//! The fake connector is a shell script. Before each test the test writes the
//! batch the connector will "fetch" for each saved cursor (Parquet files and
//! the result file), and the script copies the batch that matches the cursor
//! in its request. Unix only: the script needs `sh`.
#![cfg(unix)]

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Duration as ChronoDuration, TimeZone, Utc};
use parquet::arrow::ArrowWriter;
use rocky_cli::commands::{
    ConnectorLoadOutcome, ConnectorLoadParams, claim_key, claim_ttl_hours, execute_connector_load,
};
use rocky_cli::output::ConnectorLoadStatus;
use rocky_core::config::{
    DedupPolicy, IdempotencyConfig, LoadedConfig, load_rocky_config_fingerprinted,
};
use rocky_core::connector::StateEnvelope;
use rocky_core::idempotency::{IdempotencyBackend, IdempotencyCheck};
use rocky_core::state::StateStore;

fn t0() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 10, 22, 8, 0, 0).unwrap()
}

fn ids(values: &[i64]) -> ArrayRef {
    Arc::new(Int64Array::from(values.to_vec()))
}

fn names(values: &[&str]) -> ArrayRef {
    Arc::new(StringArray::from(values.to_vec()))
}

/// One Parquet file to hand out: its name and its columns.
type FileSpec<'a> = (&'a str, Vec<(&'a str, ArrayRef)>);

struct Project {
    root: tempfile::TempDir,
}

impl Project {
    fn new(connector_extra: &str) -> Self {
        Self::with_catalog("", connector_extra)
    }

    /// A project whose connector target sets `catalog`. The warehouse file is
    /// `warehouse.duckdb`, so the current database is `warehouse`.
    fn with_catalog(catalog: &str, connector_extra: &str) -> Self {
        let root = tempfile::tempdir().unwrap();
        let p = root.path();
        std::fs::create_dir_all(p.join("connector")).unwrap();
        std::fs::create_dir_all(p.join("fx")).unwrap();
        std::fs::write(
            p.join("rocky.toml"),
            format!(
                r#"[adapter.default]
type = "duckdb"
path = "{db}"

[connector.demo]
manifest = "connector/connector.toml"
output_uri = "file://./landing"
target = {{ catalog = "{catalog}", schema = "main", table = "events" }}
{connector_extra}
"#,
                db = p.join("warehouse.duckdb").display()
            ),
        )
        .unwrap();
        std::fs::write(
            p.join("connector/connector.toml"),
            "protocol_version = 0\nname = \"demo\"\ncommand = [\"./run.sh\"]\n\
             state_schema_version = 1\negress_hosts = []\n",
        )
        .unwrap();
        let project = Self { root };
        project.write_script("");
        project
    }

    fn path(&self) -> &Path {
        self.root.path()
    }

    fn config_path(&self) -> PathBuf {
        self.path().join("rocky.toml")
    }

    fn state_path(&self) -> PathBuf {
        self.path().join("state.redb")
    }

    fn db_path(&self) -> PathBuf {
        self.path().join("warehouse.duckdb")
    }

    fn marker(&self, name: &str) -> PathBuf {
        self.path().join(name)
    }

    /// The connector: record that it started, wait if `pause` exists, then
    /// copy the batch for the cursor in the request.
    fn write_script(&self, extra: &str) {
        let script = self.path().join("connector/run.sh");
        let body = format!(
            "#!/bin/sh\n\
             cat > request.json\n\
             touch '{started}'\n\
             {extra}\n\
             cursor=$(sed -n 's/.*\"cursor\":\\([0-9][0-9]*\\).*/\\1/p' request.json)\n\
             cursor=${{cursor:-0}}\n\
             if [ -e '{fx}/c'$cursor/fail ]; then exit 1; fi\n\
             cp '{fx}'/c$cursor/* .\n\
             mv result.json _rocky_result.json\n",
            started = self.marker("started").display(),
            fx = self.path().join("fx").display(),
        );
        std::fs::write(&script, body).unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    /// The batch the connector returns when its saved cursor is `cursor_in`.
    fn batch(&self, cursor_in: u64, cursor_out: u64, files: &[FileSpec<'_>]) {
        let dir = self.path().join("fx").join(format!("c{cursor_in}"));
        std::fs::create_dir_all(&dir).unwrap();
        let mut listed = Vec::new();
        for (name, columns) in files {
            let batch = RecordBatch::try_from_iter(columns.iter().cloned()).unwrap();
            let file = std::fs::File::create(dir.join(name)).unwrap();
            let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
            listed.push(serde_json::json!({
                "path": name,
                "bytes": std::fs::metadata(dir.join(name)).unwrap().len(),
                "rows": batch.num_rows(),
            }));
        }
        std::fs::write(
            dir.join("result.json"),
            serde_json::json!({
                "protocol_version": 0,
                "files": listed,
                "state": {"schema_version": 1, "value": {"cursor": cursor_out}},
            })
            .to_string(),
        )
        .unwrap();
    }

    /// Make the connector fail when its saved cursor is `cursor_in`.
    fn failing_batch(&self, cursor_in: u64) {
        let dir = self.path().join("fx").join(format!("c{cursor_in}"));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("fail"), "").unwrap();
    }

    fn loaded(&self) -> LoadedConfig {
        load_rocky_config_fingerprinted(&self.config_path()).expect("config loads")
    }

    async fn run_at(&self, now: Option<DateTime<Utc>>) -> ConnectorLoadOutcome {
        let loaded = self.loaded();
        let config_path = self.config_path();
        let state_path = self.state_path();
        let mut params = ConnectorLoadParams::new(&config_path, &loaded, &state_path, "demo");
        params.now = now;
        params.promote_now = now;
        params.output_poll = Duration::from_millis(100);
        execute_connector_load(&params).await
    }

    async fn run(&self) -> ConnectorLoadOutcome {
        self.run_at(None).await
    }

    /// Claim at `now`, and check the promote deadline against `promote_now`.
    async fn run_with_promote_clock(
        &self,
        now: DateTime<Utc>,
        promote_now: DateTime<Utc>,
    ) -> ConnectorLoadOutcome {
        let loaded = self.loaded();
        let config_path = self.config_path();
        let state_path = self.state_path();
        let mut params = ConnectorLoadParams::new(&config_path, &loaded, &state_path, "demo");
        params.now = Some(now);
        params.promote_now = Some(promote_now);
        params.output_poll = Duration::from_millis(100);
        execute_connector_load(&params).await
    }

    fn state(&self) -> Option<rocky_core::connector::ConnectorStateRecord> {
        StateStore::open_read_only_or_empty(&self.state_path())
            .unwrap()
            .connector_state_get("demo")
            .unwrap()
    }

    /// Run `sql` against the warehouse file; the run has closed it.
    fn sql<T>(&self, f: impl FnOnce(&duckdb::Connection) -> T) -> T {
        let conn = duckdb::Connection::open(self.db_path()).unwrap();
        f(&conn)
    }

    fn row_count(&self) -> Option<i64> {
        self.sql(|c| {
            c.query_row("SELECT count(*) FROM main.events", [], |r| r.get(0))
                .ok()
        })
    }

    fn staging_tables(&self) -> i64 {
        self.sql(|c| {
            c.query_row(
                "SELECT count(*) FROM information_schema.tables WHERE table_name LIKE '%__rocky_stg_%'",
                [],
                |r| r.get(0),
            )
            .unwrap()
        })
    }

    fn started(&self) -> bool {
        self.marker("started").exists()
    }
}

/// The run's scratch directory, as the load output names it.
fn scratch_dir(outcome: &ConnectorLoadOutcome) -> PathBuf {
    PathBuf::from(&outcome.output.as_ref().unwrap().source_dir)
}

fn info(outcome: &ConnectorLoadOutcome) -> &rocky_cli::output::ConnectorLoadInfo {
    outcome
        .output
        .as_ref()
        .and_then(|o| o.connector.as_ref())
        .expect("the run reported connector info")
}

fn standard_batches(project: &Project) {
    project.batch(
        0,
        2,
        &[
            (
                "a.parquet",
                vec![("id", ids(&[1])), ("name", names(&["a"]))],
            ),
            (
                "b.parquet",
                vec![("id", ids(&[2])), ("name", names(&["b"]))],
            ),
        ],
    );
    project.batch(
        2,
        4,
        &[
            (
                "a.parquet",
                vec![("id", ids(&[3])), ("name", names(&["c"]))],
            ),
            (
                "b.parquet",
                vec![("id", ids(&[4])), ("name", names(&["d"]))],
            ),
        ],
    );
    project.batch(
        4,
        6,
        &[
            (
                "a.parquet",
                vec![("id", ids(&[5])), ("name", names(&["e"]))],
            ),
            (
                "b.parquet",
                vec![("id", ids(&[6])), ("name", names(&["f"]))],
            ),
        ],
    );
}

#[tokio::test]
async fn three_runs_append_two_rows_each_and_advance_the_cursor() {
    let project = Project::new("");
    standard_batches(&project);

    for (run, expected_rows) in [(1u64, 2i64), (2, 4), (3, 6)] {
        let outcome = project.run().await;
        outcome.result.as_ref().expect("the run succeeds");
        let connector = info(&outcome);
        assert_eq!(connector.status, ConnectorLoadStatus::Loaded);
        let output = outcome.output.as_ref().unwrap();
        assert_eq!(output.files_loaded, 2);
        assert_eq!(output.files_failed, 0);
        assert_eq!(output.total_rows, 2);
        assert_eq!(connector.attempts, 1);
        assert_eq!(connector.commit_seq_before, run - 1);
        assert_eq!(connector.commit_seq_after, run);
        assert!(!connector.adopted_expired_claim);
        assert_eq!(project.row_count(), Some(expected_rows));
        assert_eq!(project.staging_tables(), 0, "staging is dropped");
    }
    let saved = project.state().unwrap();
    assert_eq!(saved.commit_seq, 3);
    assert_eq!(saved.envelope.value["cursor"], 6);
    let store = StateStore::open_read_only(&project.state_path()).unwrap();
    assert_eq!(
        store.list_loaded_files("connector:demo").unwrap().len(),
        6,
        "one ledger row per file"
    );
}

#[tokio::test]
async fn a_failing_child_changes_nothing_and_frees_the_claim() {
    let project = Project::new("retries = 0");
    standard_batches(&project);
    project.run().await.result.as_ref().unwrap();
    project.failing_batch(2);

    let outcome = project.run().await;
    assert!(outcome.result.is_err());
    assert_eq!(info(&outcome).status, ConnectorLoadStatus::Failed);
    assert_eq!(info(&outcome).attempts, 1);
    assert_eq!(project.row_count(), Some(2));
    assert_eq!(project.state().unwrap().commit_seq, 1);

    // The failed run freed the claim, so the next run does not skip.
    std::fs::remove_file(project.path().join("fx/c2/fail")).unwrap();
    let again = project.run().await;
    again.result.as_ref().unwrap();
    assert_eq!(info(&again).status, ConnectorLoadStatus::Loaded);
    assert_eq!(project.row_count(), Some(4));
}

#[tokio::test]
async fn a_second_file_with_other_columns_breaks_staging_and_changes_nothing() {
    let project = Project::new("retries = 0");
    standard_batches(&project);
    project.run().await.result.as_ref().unwrap();
    // The second file has an extra column.
    project.batch(
        2,
        4,
        &[
            (
                "a.parquet",
                vec![("id", ids(&[3])), ("name", names(&["c"]))],
            ),
            (
                "b.parquet",
                vec![
                    ("id", ids(&[4])),
                    ("name", names(&["d"])),
                    ("extra", names(&["x"])),
                ],
            ),
        ],
    );

    let outcome = project.run().await;
    let error = format!("{:#}", outcome.result.as_ref().unwrap_err());
    assert!(error.contains("b.parquet"), "{error}");
    // A staging failure reports every file as failed, like the directory loader.
    let output = outcome.output.as_ref().unwrap();
    assert_eq!(output.files.len(), 2);
    assert!(output.files.iter().all(|f| f.error.is_some()));
    assert_eq!(output.files_loaded, 0);
    assert_eq!(output.files_failed, 2);
    assert_eq!(output.total_rows, 0);
    assert_eq!(output.total_bytes, 0);
    assert_eq!(project.row_count(), Some(2), "the target is untouched");
    assert_eq!(project.staging_tables(), 0, "staging is dropped");
    assert!(
        scratch_dir(&outcome).join("b.parquet").exists(),
        "a failed run keeps its scratch directory"
    );
    let saved = project.state().unwrap();
    assert_eq!(saved.commit_seq, 1, "the state did not move");
    assert_eq!(saved.envelope.value["cursor"], 2);
}

#[tokio::test]
async fn a_first_run_that_breaks_staging_creates_no_target() {
    let project = Project::new("retries = 0");
    project.batch(
        0,
        2,
        &[
            (
                "a.parquet",
                vec![("id", ids(&[1])), ("name", names(&["a"]))],
            ),
            (
                "b.parquet",
                vec![("name", names(&["b"])), ("id", ids(&[2]))],
            ),
        ],
    );
    let outcome = project.run().await;
    assert!(outcome.result.is_err());
    assert_eq!(project.row_count(), None, "no target table");
    assert_eq!(project.staging_tables(), 0);
    assert!(project.state().is_none());
}

/// #2478: a target catalog equal to the current database, in any case, loads
/// into it.
#[tokio::test]
async fn a_target_catalog_naming_the_current_database_loads_into_it() {
    for catalog in ["warehouse", "Warehouse"] {
        let project = Project::with_catalog(catalog, "");
        standard_batches(&project);
        let outcome = project.run().await;
        outcome
            .result
            .as_ref()
            .unwrap_or_else(|e| panic!("catalog {catalog}: {e:#}"));
        assert_eq!(info(&outcome).status, ConnectorLoadStatus::Loaded);
        assert_eq!(project.row_count(), Some(2));
        assert_eq!(project.staging_tables(), 0, "staging is dropped");
    }
}

/// #2478: a target catalog that is not the current database fails before
/// anything is written, and the error names the accepted value.
#[tokio::test]
async fn an_unknown_target_catalog_fails_and_creates_no_target() {
    let project = Project::with_catalog("nope", "retries = 0");
    standard_batches(&project);
    let outcome = project.run().await;
    let err = outcome.result.as_ref().expect_err("the run must fail");
    let msg = format!("{err:#}");
    assert!(
        msg.contains(
            "DuckDB has no attached database named 'nope'; the current database is \
             'warehouse'. Set [load] target.catalog to \"\" or to 'warehouse'."
        ),
        "unexpected error: {msg}"
    );
    assert_eq!(project.row_count(), None, "no target table");
    assert_eq!(project.staging_tables(), 0);
    assert!(project.state().is_none());
}

#[tokio::test]
async fn a_new_column_is_added_and_values_land_in_the_right_columns() {
    let project = Project::new("");
    project.batch(
        0,
        1,
        &[(
            "a.parquet",
            vec![("id", ids(&[1, 2])), ("name", names(&["a", "b"]))],
        )],
    );
    // The next batch lists its columns in another order and adds one.
    project.batch(
        1,
        2,
        &[(
            "a.parquet",
            vec![
                ("name", names(&["c"])),
                ("extra", names(&["x"])),
                ("id", ids(&[3])),
            ],
        )],
    );
    project.run().await.result.as_ref().unwrap();
    project.run().await.result.as_ref().unwrap();

    let rows: Vec<(i64, String, Option<String>)> = project.sql(|c| {
        let mut stmt = c
            .prepare("SELECT id, name, extra FROM main.events ORDER BY id")
            .unwrap();
        stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .unwrap()
            .map(Result::unwrap)
            .collect()
    });
    assert_eq!(
        rows,
        vec![
            (1, "a".to_string(), None),
            (2, "b".to_string(), None),
            (3, "c".to_string(), Some("x".to_string())),
        ]
    );
}

#[tokio::test]
async fn a_contract_violation_on_the_staging_table_blocks_the_promote() {
    let project = Project::new(
        "retries = 0\n[connector.demo.contract]\nrequired_columns = [{ name = \"missing\", type = \"VARCHAR\" }]",
    );
    standard_batches(&project);
    let outcome = project.run().await;
    let error = format!("{:#}", outcome.result.as_ref().unwrap_err());
    assert!(error.contains("contract"), "{error}");
    assert_eq!(project.row_count(), None);
    assert_eq!(project.staging_tables(), 0);
    assert!(project.state().is_none());
}

#[tokio::test]
async fn a_held_claim_skips_the_run_without_starting_the_child() {
    let project = Project::new("");
    standard_batches(&project);
    // Another run holds the claim for the batch after commit sequence 0.
    claim_as_other_run(&project, t0()).await;

    let outcome = project
        .run_at(Some(t0() + ChronoDuration::minutes(5)))
        .await;
    outcome.result.as_ref().unwrap();
    assert_eq!(info(&outcome).status, ConnectorLoadStatus::Skipped);
    assert_eq!(info(&outcome).attempts, 0);
    assert!(!project.started(), "the child never started");
    assert_eq!(project.row_count(), None);
}

/// Claim `connector:demo:0` as another run, at `now`.
async fn claim_as_other_run(project: &Project, now: DateTime<Utc>) {
    let loaded = project.loaded();
    let connector = &loaded.config.connectors["demo"];
    let config = IdempotencyConfig {
        in_flight_ttl_hours: claim_ttl_hours(3600, connector.retries),
        dedup_on: DedupPolicy::Success,
        ..IdempotencyConfig::default()
    };
    let store = StateStore::open(&project.state_path()).unwrap();
    let verdict = IdempotencyBackend::Local
        .check_and_claim_at(
            Some(&store),
            &claim_key("demo", 0),
            "other-run",
            &config,
            now,
        )
        .await
        .unwrap();
    assert!(matches!(verdict, IdempotencyCheck::Proceed));
}

#[tokio::test]
async fn an_expired_claim_is_adopted_with_a_warning_flag() {
    let project = Project::new("");
    standard_batches(&project);
    claim_as_other_run(&project, t0()).await;
    // Default limits and one retry: 3600 * 2 + 3600 = 3 hours.
    assert_eq!(claim_ttl_hours(3600, 1), 3);

    let before_expiry = project.run_at(Some(t0() + ChronoDuration::hours(2))).await;
    assert_eq!(info(&before_expiry).status, ConnectorLoadStatus::Skipped);
    assert!(!project.started());

    let after_expiry = project.run_at(Some(t0() + ChronoDuration::hours(4))).await;
    after_expiry.result.as_ref().unwrap();
    assert_eq!(info(&after_expiry).status, ConnectorLoadStatus::Loaded);
    assert!(info(&after_expiry).adopted_expired_claim);
    assert_eq!(project.row_count(), Some(2));
}

#[test]
fn the_claim_lives_for_every_attempt_plus_an_hour_rounded_up() {
    assert_eq!(claim_ttl_hours(3600, 0), 2);
    assert_eq!(claim_ttl_hours(3600, 1), 3);
    assert_eq!(claim_ttl_hours(1, 0), 2, "an hour of headroom, rounded up");
    assert_eq!(claim_ttl_hours(2, 0), 2);
    assert_eq!(claim_ttl_hours(3601, 0), 3);
    assert_eq!(claim_ttl_hours(7200, 1), 5);
    assert_eq!(claim_key("demo", 7), "connector:demo:7");
}

/// A crash between the promote and the state commit, simulated by holding the
/// state store's writer lock while the child runs: the commit cannot open the
/// store. The rows are in the table, the state did not move, and the claim
/// stays in flight.
#[tokio::test]
async fn a_failed_commit_after_the_promote_leaves_the_claim_in_flight() {
    let project = Project::new("");
    standard_batches(&project);
    let state_path = project.state_path();
    drop(StateStore::open(&state_path).unwrap());

    // Take the lock once the child has started: the claim is already made.
    let started = project.marker("started");
    let (release, released) = std::sync::mpsc::channel::<()>();
    let holder_state_path = state_path.clone();
    let holder = std::thread::spawn(move || {
        while !started.exists() {
            std::thread::sleep(Duration::from_millis(20));
        }
        let _store = StateStore::open(&holder_state_path).unwrap();
        let _ = released.recv();
    });

    let crashed = project.run_at(Some(t0())).await;
    let _ = release.send(());
    holder.join().unwrap();

    let error = format!("{:#}", crashed.result.as_ref().unwrap_err());
    assert!(error.contains("not committed"), "{error}");
    assert!(
        scratch_dir(&crashed).is_dir(),
        "a run whose commit failed keeps its scratch directory"
    );
    assert_eq!(project.row_count(), Some(2), "the batch is in the table");
    assert!(project.state().is_none(), "the state did not move");

    // Before the claim expires, the next run skips: no duplicate load.
    std::fs::remove_file(project.marker("started")).unwrap();
    let during = project.run_at(Some(t0() + ChronoDuration::hours(1))).await;
    assert_eq!(info(&during).status, ConnectorLoadStatus::Skipped);
    assert!(!project.started());
    assert_eq!(project.row_count(), Some(2));

    // After it expires, the run adopts the claim and loads the batch again.
    let after = project.run_at(Some(t0() + ChronoDuration::hours(4))).await;
    after.result.as_ref().unwrap();
    assert!(info(&after).adopted_expired_claim);
    assert_eq!(
        project.row_count(),
        Some(4),
        "at least once: a duplicate batch"
    );
    assert_eq!(project.state().unwrap().commit_seq, 1);
}

#[tokio::test]
async fn a_commit_is_refused_when_the_sequence_moved_since_the_claim() {
    let project = Project::new("");
    standard_batches(&project);
    let state_path = project.state_path();
    drop(StateStore::open(&state_path).unwrap());
    // The child pauses; meanwhile another run commits batch 1.
    project.write_script(&format!(
        "while [ ! -e '{}' ]; do sleep 0.05; done",
        project.marker("go").display()
    ));
    let started = project.marker("started");
    let go = project.marker("go");
    let interloper_state_path = state_path.clone();
    let interloper = std::thread::spawn(move || {
        while !started.exists() {
            std::thread::sleep(Duration::from_millis(20));
        }
        {
            let store = StateStore::open(&interloper_state_path).unwrap();
            store
                .connector_commit(
                    "demo",
                    0,
                    &StateEnvelope {
                        schema_version: 1,
                        value: serde_json::json!({"cursor": 100}),
                    },
                )
                .unwrap();
        }
        std::fs::write(go, "").unwrap();
    });

    let outcome = project.run().await;
    interloper.join().unwrap();

    let error = format!("{:#}", outcome.result.as_ref().unwrap_err());
    assert!(
        error.contains("expected commit sequence 0, found 1"),
        "{error}"
    );
    let saved = project.state().unwrap();
    assert_eq!(saved.commit_seq, 1);
    assert_eq!(
        saved.envelope.value["cursor"], 100,
        "the winner's state stays"
    );
    assert_eq!(info(&outcome).status, ConnectorLoadStatus::Failed);
}

#[tokio::test]
async fn a_saved_state_newer_than_the_manifest_is_refused_before_the_child_starts() {
    let project = Project::new("");
    standard_batches(&project);
    {
        let store = StateStore::open(&project.state_path()).unwrap();
        store
            .connector_commit(
                "demo",
                0,
                &StateEnvelope {
                    schema_version: 2,
                    value: serde_json::json!({"cursor": 0}),
                },
            )
            .unwrap();
    }
    let outcome = project.run().await;
    let error = format!("{:#}", outcome.result.as_ref().unwrap_err());
    assert!(error.contains("schema version 2"), "{error}");
    assert!(outcome.output.is_none(), "nothing was claimed or reported");
    assert!(!project.started());
    assert_eq!(project.row_count(), None);
}

#[tokio::test]
async fn an_older_saved_state_is_handed_to_the_connector_unchanged() {
    let project = Project::new("");
    {
        let store = StateStore::open(&project.state_path()).unwrap();
        store
            .connector_commit(
                "demo",
                0,
                &StateEnvelope {
                    schema_version: 0,
                    value: serde_json::json!({"cursor": 2}),
                },
            )
            .unwrap();
    }
    // The connector reads cursor 2 from the old-version state and upgrades it.
    project.batch(
        2,
        4,
        &[(
            "a.parquet",
            vec![("id", ids(&[3])), ("name", names(&["c"]))],
        )],
    );
    let outcome = project.run().await;
    outcome.result.as_ref().unwrap();
    assert_eq!(info(&outcome).commit_seq_after, 2);
    assert_eq!(project.state().unwrap().envelope.schema_version, 1);
    assert_eq!(project.row_count(), Some(1));
}

#[tokio::test]
async fn an_empty_batch_still_advances_the_state() {
    let project = Project::new("");
    project.batch(0, 5, &[]);
    let outcome = project.run().await;
    outcome.result.as_ref().unwrap();
    assert_eq!(info(&outcome).status, ConnectorLoadStatus::Loaded);
    assert_eq!(outcome.output.as_ref().unwrap().files_loaded, 0);
    assert_eq!(project.row_count(), None, "no rows, so no table");
    assert_eq!(project.state().unwrap().envelope.value["cursor"], 5);
    assert!(
        !scratch_dir(&outcome).exists(),
        "a committed run removes its scratch"
    );
}

/// A committed run removes its own scratch directory and nothing else in the
/// landing directory, which another run or connector may share.
#[tokio::test]
async fn a_committed_run_removes_only_its_own_scratch_directory() {
    let project = Project::new("");
    standard_batches(&project);
    let other = project.path().join("landing/other-1");
    std::fs::create_dir_all(&other).unwrap();
    std::fs::write(other.join("keep.txt"), "x").unwrap();

    let outcome = project.run().await;
    outcome.result.as_ref().unwrap();
    let scratch = scratch_dir(&outcome);
    assert!(scratch.starts_with(project.path().canonicalize().unwrap()));
    assert!(!scratch.exists(), "the committed run's scratch is removed");
    assert!(project.path().join("landing").is_dir(), "landing stays");
    assert!(
        other.join("keep.txt").exists(),
        "another directory is untouched"
    );
}

/// `keep_scratch = true` keeps a committed run's scratch directory, owner-only.
#[tokio::test]
async fn keep_scratch_keeps_the_committed_scratch_directory() {
    let project = Project::new("keep_scratch = true");
    standard_batches(&project);
    let outcome = project.run().await;
    outcome.result.as_ref().unwrap();
    let scratch = scratch_dir(&outcome);
    assert!(scratch.join("a.parquet").exists(), "{}", scratch.display());
    let mode = std::fs::metadata(&scratch).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o700, "the scratch directory is owner-only");
    // A directory made the default way shows the umask. When it is not 0700,
    // the 0700 above can only come from the explicit mode.
    let control = project.path().join("control");
    std::fs::create_dir(&control).unwrap();
    let default_mode = std::fs::metadata(&control).unwrap().permissions().mode() & 0o777;
    if default_mode == 0o700 {
        eprintln!("note: umask already yields 0700; the mode check is not discriminating here");
    }
}

#[tokio::test]
async fn a_finished_claim_with_a_state_that_did_not_move_is_a_failure_not_a_skip() {
    let project = Project::new("");
    standard_batches(&project);
    // A run finished the batch after commit sequence 0, yet the saved state
    // is still at 0: it was reset or restored.
    let loaded = project.loaded();
    let config = IdempotencyConfig {
        in_flight_ttl_hours: claim_ttl_hours(3600, loaded.config.connectors["demo"].retries),
        dedup_on: DedupPolicy::Success,
        ..IdempotencyConfig::default()
    };
    {
        let store = StateStore::open(&project.state_path()).unwrap();
        let key = claim_key("demo", 0);
        IdempotencyBackend::Local
            .check_and_claim_at(Some(&store), &key, "other-run", &config, t0())
            .await
            .unwrap();
        IdempotencyBackend::Local
            .finalize_at(
                Some(&store),
                &key,
                "other-run",
                rocky_core::idempotency::FinalOutcome::Succeeded,
                &config,
                t0(),
            )
            .await
            .unwrap();
    }
    let outcome = project
        .run_at(Some(t0() + ChronoDuration::minutes(5)))
        .await;
    let error = format!("{:#}", outcome.result.as_ref().unwrap_err());
    assert!(error.contains("saved state is behind claim"), "{error}");
    assert!(
        error.contains("retention_days"),
        "the way out is named: {error}"
    );
    assert_eq!(info(&outcome).status, ConnectorLoadStatus::Failed);
    assert!(!project.started(), "the child never started");
    assert_eq!(project.row_count(), None);
}

/// The target has a NOT NULL column the batch does not carry, so the INSERT
/// fails after the drift step added the batch's new column.
fn target_that_rejects_the_batch(project: &Project) {
    project.sql(|c| {
        c.execute_batch("CREATE TABLE main.events (id BIGINT, name VARCHAR NOT NULL)")
            .unwrap();
    });
    project.batch(
        0,
        1,
        &[(
            "a.parquet",
            vec![("id", ids(&[1])), ("extra", names(&["x"]))],
        )],
    );
}

fn has_column(project: &Project, column: &str) -> bool {
    project.sql(|c| {
        c.query_row(
            "SELECT count(*) FROM information_schema.columns \
             WHERE table_name = 'events' AND column_name = ?",
            [column],
            |r| r.get::<_, i64>(0),
        )
        .unwrap()
            > 0
    })
}

/// Once the promote statement was sent, a failure leaves the claim in flight:
/// the rows may be in the table, and a retry would load them again.
#[tokio::test]
async fn a_failed_promote_keeps_the_claim_and_does_not_roll_back_drift() {
    let project = Project::new("");
    target_that_rejects_the_batch(&project);

    let failed = project.run_at(Some(t0())).await;
    let error = format!("{:#}", failed.result.as_ref().unwrap_err());
    assert!(error.contains("promote failed"), "{error}");
    assert!(error.contains("left in flight"), "{error}");
    let output = failed.output.as_ref().unwrap();
    assert_eq!(output.files_loaded, 0);
    assert_eq!(output.files_failed, output.files.len());
    assert!(output.files_failed > 0);
    assert_eq!(output.total_rows, 0);
    assert_eq!(output.total_bytes, 0);
    for file in &output.files {
        assert_eq!(file.rows_loaded, 0, "{file:?}");
        assert_eq!(file.bytes_read, 0, "{file:?}");
    }
    assert_eq!(project.row_count(), Some(0), "no rows landed");
    assert!(
        has_column(&project, "extra"),
        "the ADD COLUMN of the drift step is not rolled back"
    );
    assert_eq!(project.staging_tables(), 0);
    assert!(project.state().is_none());

    // The claim is still held, so the next run skips instead of loading again.
    std::fs::remove_file(project.marker("started")).unwrap();
    let next = project.run_at(Some(t0() + ChronoDuration::hours(1))).await;
    assert_eq!(info(&next).status, ConnectorLoadStatus::Skipped);
    assert!(!project.started());
}

/// A failure before the promote frees the claim and leaves the rows alone.
#[tokio::test]
async fn a_promote_past_the_claim_expiry_is_refused_and_frees_the_claim() {
    let project = Project::new("");
    target_that_rejects_the_batch(&project);

    // Default limits and one retry: the claim lives 3 hours.
    let late = project
        .run_with_promote_clock(t0(), t0() + ChronoDuration::hours(4))
        .await;
    let error = format!("{:#}", late.result.as_ref().unwrap_err());
    assert!(error.contains("refusing to promote"), "{error}");
    assert_eq!(project.row_count(), Some(0));
    assert_eq!(info(&late).status, ConnectorLoadStatus::Failed);

    // The claim was freed: a run just after is not skipped.
    std::fs::remove_file(project.marker("started")).unwrap();
    let next = project
        .run_at(Some(t0() + ChronoDuration::minutes(1)))
        .await;
    assert!(project.started(), "the child ran again");
    assert_ne!(info(&next).status, ConnectorLoadStatus::Skipped);
}

#[tokio::test]
async fn a_promote_inside_the_claim_lifetime_goes_ahead() {
    let project = Project::new("");
    standard_batches(&project);
    let ok = project
        .run_with_promote_clock(t0(), t0() + ChronoDuration::hours(2))
        .await;
    ok.result.as_ref().unwrap();
    assert_eq!(project.row_count(), Some(2));
}

#[test]
fn a_huge_wall_time_does_not_overflow_the_claim() {
    assert_eq!(claim_ttl_hours(u64::MAX, u32::MAX), 24 * 366);
    assert_eq!(claim_ttl_hours(0, 0), 1);
}

/// A signal before the promote is a failure before the promote: staging is
/// dropped, the claim is freed and the target is untouched.
#[tokio::test]
async fn a_signal_before_the_promote_frees_the_claim_and_leaves_the_target_alone() {
    let project = Project::new("");
    standard_batches(&project);
    project.run().await.result.as_ref().unwrap();
    let loaded = project.loaded();
    let config_path = project.config_path();
    let state_path = project.state_path();
    let mut params = ConnectorLoadParams::new(&config_path, &loaded, &state_path, "demo");
    params.output_poll = Duration::from_millis(100);
    params.interrupt_before_promote = true;
    let outcome = execute_connector_load(&params).await;

    let error = format!("{:#}", outcome.result.as_ref().unwrap_err());
    assert!(
        error.contains("interrupted by a signal before the promote"),
        "{error}"
    );
    assert_eq!(info(&outcome).status, ConnectorLoadStatus::Failed);
    assert_eq!(project.row_count(), Some(2), "the target is untouched");
    assert_eq!(project.staging_tables(), 0);
    assert_eq!(project.state().unwrap().commit_seq, 1);

    // The claim was freed, so the next run loads the batch.
    let again = project.run().await;
    again.result.as_ref().unwrap();
    assert_eq!(info(&again).status, ConnectorLoadStatus::Loaded);
    assert_eq!(project.row_count(), Some(4));
}
