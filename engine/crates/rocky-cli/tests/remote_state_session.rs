//! WP-01 PR-B (2a) integration tests: the replication run's
//! [`RemoteStateSession`] lifecycle — terminal-upload reposition, durability
//! split, and the error-path abandon choke-point.
//!
//! Drives the REAL `commands::run` replication path end-to-end against a temp
//! DuckDB project, with the PR-1 remote-state rig — a fault-injecting
//! in-memory "S3" installed as the process-global provider override — standing
//! in for the `[state] backend = "s3"` remote. Each test holds
//! `remote_testing::serial_guard()` because the override is process-global.
//!
//! What is pinned here:
//! - **terminal writes ride the upload** (behavior delta ii): the run record
//!   (and with it the verify-after custody row + idempotency stamp, written at
//!   the same terminal site) is persisted BEFORE the terminal upload, so a
//!   second pod's start-download sees it. RED before stage 2a: the upload
//!   fired first and the uploaded snapshot predated the record.
//! - **`on_upload_failure = "fail"` is honored at the terminal upload**
//!   (behavior delta i-b): an ungoverned run configured `fail` exits nonzero
//!   when the terminal Put fails. RED before stage 2a: the end-of-run upload
//!   was warn-and-continue regardless of the configured mode.
//! - **default `skip` keeps warn-and-exit-0** for ungoverned runs (parity
//!   pin).
//! - **error-path exits abandon without uploading** (parity pin — error /
//!   interrupted paths never uploaded).
//!
//! The GOVERNED `Durable` half of delta (i) — a governed run failing on a lost
//! terminal upload even under the default `skip` — is driven at the session
//! level in `rocky-core::state_sync::tests` (`session_durable_forces_fail_…`):
//! constructing a full governed apply context (plan + `[policy]` plane +
//! execution-fingerprint gate) in this rig is disproportionately heavy, and
//! the run-level wiring it would re-prove (`governed_with_policy` selecting
//! `FinalizeDurability::Durable`) is a one-line construction at the seam. The
//! ungoverned explicit-`fail` run-level test below exercises the same
//! propagation path through `finalize`.

#![cfg(feature = "duckdb")]

use std::path::{Path, PathBuf};

use rocky_core::fault_store::{FaultMode, FaultOp};
use rocky_core::state_sync::{StateAuthority, remote_testing};
use rocky_core::test_harness::CrossPodHarness;
use rocky_core::traits::WarehouseAdapter;
use rocky_duckdb::adapter::DuckDbWarehouseAdapter;

/// Seed `raw__acme.orders` in the persistent DuckDB file, dropping the
/// connection before returning so `rocky run` can reopen the file.
async fn seed_source(db: &Path) {
    let a = DuckDbWarehouseAdapter::open(db).expect("seed open");
    a.execute_statement("CREATE SCHEMA IF NOT EXISTS raw__acme")
        .await
        .unwrap();
    a.execute_statement("DROP TABLE IF EXISTS raw__acme.orders")
        .await
        .unwrap();
    a.execute_statement("CREATE TABLE raw__acme.orders AS SELECT 1 AS id, 'widget' AS name")
        .await
        .unwrap();
}

/// A minimal temp DuckDB replication project with a remote `[state]` backend
/// (`s3`, resolved to the harness's fault-injecting in-memory store by the
/// process-global provider override).
struct TestProject {
    /// Owned so the temp dir lives (and is cleaned up) with the project.
    _dir: tempfile::TempDir,
    config_path: PathBuf,
    state_path: PathBuf,
}

impl TestProject {
    /// `state_extra` is appended inside the `[state]` table (before the
    /// `[state.retry]` subtable) — e.g. `on_upload_failure = "fail"`. Retries
    /// are disabled for every project so an armed terminal-Put fault fails on
    /// the first attempt instead of sleeping through backoff.
    ///
    /// `sabotage_adapter` points the DuckDB adapter at a file inside a
    /// nonexistent directory, so `AdapterRegistry::from_config` — which runs
    /// AFTER the session's `acquire` — fails deterministically (the error-path
    /// abandon rig).
    async fn new(state_extra: &str, sabotage_adapter: bool) -> Self {
        let dir = tempfile::tempdir().expect("create project temp dir");
        let db = dir.path().join("wh.duckdb");
        seed_source(&db).await;
        let adapter_path = if sabotage_adapter {
            dir.path().join("nonexistent-dir").join("wh.duckdb")
        } else {
            db
        };
        let config_path = dir.path().join("rocky.toml");
        // `catalog_template = "wh"` matches the DuckDB catalog of `wh.duckdb`.
        std::fs::write(
            &config_path,
            format!(
                r#"
[adapter]
type = "duckdb"
path = "{db}"

[pipeline.poc]
strategy = "full_refresh"

[pipeline.poc.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.poc.target]
catalog_template = "wh"
schema_template = "staging__{{source}}"

[pipeline.poc.target.governance]
auto_create_schemas = true

[state]
backend = "s3"
s3_bucket = "test"
{state_extra}

[state.retry]
max_retries = 0
"#,
                db = adapter_path.display()
            ),
        )
        .expect("write rocky.toml");
        let state_path = dir.path().join(".rocky-state.redb");
        Self {
            _dir: dir,
            config_path,
            state_path,
        }
    }
}

/// Drive the real `commands::run` replication path (ungoverned: no plan, no
/// `[policy]`, no governance context).
async fn drive_run(
    config_path: &Path,
    state_path: &Path,
    run_id_override: Option<&str>,
) -> anyhow::Result<()> {
    let loaded = std::sync::Arc::new(rocky_core::config::load_rocky_config_fingerprinted(
        config_path,
    )?);
    rocky_cli::commands::run(
        config_path,
        loaded,
        None, // filter
        None, // pipeline_name_arg — single pipeline resolves
        state_path,
        None,  // governance_override
        false, // output_json
        None,  // models_dir
        false, // run_all
        None,  // resume_run_id
        false, // resume_latest
        None,  // shadow_config
        &rocky_cli::commands::PartitionRunOptions::default(),
        None, // model_name_filter
        None, // cache_ttl_override
        None, // idempotency_key
        None, // env
        &rocky_cli::commands::DeferOptions::default(),
        &rocky_cli::commands::SkipRunOptions::default(),
        &rocky_core::run_vars::RunVars::new(),
        run_id_override,
        None,  // governed_ctx
        false, // assume_fresh_state
    )
    .await
}

/// Behavior delta (ii): the terminal state writes precede the terminal upload,
/// so they RIDE it — after a successful run, a second pod's start-download
/// sees this run's record. RED before stage 2a: `persist_run_record` ran after
/// the upload, so the uploaded snapshot predated the record and pod B saw
/// nothing.
#[tokio::test]
async fn terminal_writes_ride_the_upload() {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();
    let project = TestProject::new("", false).await;

    drive_run(
        &project.config_path,
        &project.state_path,
        Some("run-ride-2a"),
    )
    .await
    .expect("a clean replication run succeeds");

    let authority = harness
        .download(&harness.pod_b)
        .await
        .expect("pod B start-download");
    assert_eq!(
        authority,
        StateAuthority::Authoritative,
        "the run's terminal upload must have published a remote object"
    );
    let store = harness.open_store(&harness.pod_b);
    let record = store
        .get_run("run-ride-2a")
        .expect("read pod B run history");
    assert!(
        record.is_some(),
        "the run record must ride the terminal upload: a second pod's download sees it \
         (terminal writes now PRECEDE the upload)"
    );
}

/// Behavior delta (i-b): an ungoverned run with an explicit
/// `on_upload_failure = "fail"` now exits NONZERO when the terminal upload
/// fails — that is the configured contract. RED before stage 2a: the
/// end-of-run upload was `warn!`-and-continue even under `fail`.
#[tokio::test]
async fn ungoverned_explicit_fail_exits_nonzero_on_terminal_upload_failure() {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();
    let project = TestProject::new(r#"on_upload_failure = "fail""#, false).await;

    harness.faults.arm(FaultOp::Put, FaultMode::FailAll);

    let err = drive_run(&project.config_path, &project.state_path, None)
        .await
        .expect_err("a failed terminal upload under explicit `fail` must err the run");

    harness.faults.clear();
    assert!(
        format!("{err:#}").contains("could not be persisted to the remote [state] backend"),
        "the exit must cite the refused state persistence; got: {err:#}"
    );
    assert!(
        harness.faults.count(FaultOp::Put) >= 1,
        "the terminal upload must have been attempted"
    );
}

/// Parity pin: an ungoverned run under the DEFAULT `on_upload_failure = skip`
/// keeps warn-and-exit-0 on a failed terminal upload (`ConfigDefault` honors
/// the configured liveness contract; the `skip` swallow happens inside
/// `upload_state`).
#[tokio::test]
async fn ungoverned_skip_swallows_terminal_upload_failure() {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();
    let project = TestProject::new("", false).await;

    harness.faults.arm(FaultOp::Put, FaultMode::FailAll);

    drive_run(&project.config_path, &project.state_path, None)
        .await
        .expect("default `skip` must keep a successful run at exit 0 on a failed state PUT");

    harness.faults.clear();
    assert!(
        harness.faults.count(FaultOp::Put) >= 1,
        "the terminal upload must have been attempted (then swallowed by `skip`)"
    );
}

/// Parity pin: an error-path exit consumes the session through the abandon
/// choke-point — no terminal upload (error/interrupted paths never uploaded).
/// The mid-run failure is a sabotaged warehouse adapter path, which fails at
/// `AdapterRegistry::from_config` — deterministically AFTER the session's
/// `acquire` and before any table work.
#[tokio::test]
async fn error_run_abandons_without_upload() {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();
    let project = TestProject::new("", true).await;

    let err = drive_run(&project.config_path, &project.state_path, None)
        .await
        .expect_err("a failed adapter open must err the run");
    assert!(
        format!("{err:#}").contains("failed to open DuckDB"),
        "the failure must be the sabotaged adapter, not something upstream of the session \
         seam; got: {err:#}"
    );

    harness.faults.clear();
    assert_eq!(
        harness.faults.count(FaultOp::Put),
        0,
        "an error-path exit must abandon the session — never upload"
    );
}
