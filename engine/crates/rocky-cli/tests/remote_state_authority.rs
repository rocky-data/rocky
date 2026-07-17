//! WP-01 PR-A integration tests: the replication run's typed remote-state
//! authority (RD-001).
//!
//! Drives the REAL `commands::run` replication path end-to-end against a temp
//! DuckDB project, with the PR-1 remote-state rig — a fault-injecting
//! in-memory "S3" installed as the process-global provider override — standing
//! in for the `[state] backend = "s3"` remote. Each test holds
//! `remote_testing::serial_guard()` because the override is process-global.
//!
//! What is pinned here:
//! - an UNGOVERNED run that elects past a failed download (`Indeterminate`)
//!   suppresses BOTH state uploads (RED before PR-A: the end upload fired);
//! - a genuine fresh start (`FreshStart`) still bootstraps — the first upload
//!   is the bootstrap write;
//! - `--assume-fresh-state` turns a failed download into a trusted fresh
//!   start: the run proceeds AND uploads;
//! - the fail-closed seams (policy freeze) keep their existing `?`-bail and
//!   context message on a failed download.

#![cfg(feature = "duckdb")]

use std::path::{Path, PathBuf};

use rocky_core::fault_store::{FaultMode, FaultOp};
use rocky_core::state_sync::remote_testing;
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
    async fn new() -> Self {
        let dir = tempfile::tempdir().expect("create project temp dir");
        let db = dir.path().join("wh.duckdb");
        seed_source(&db).await;
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
"#,
                db = db.display()
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
    assume_fresh_state: bool,
) -> anyhow::Result<()> {
    // PR-B: `run` executes from the caller's owned fingerprinted snapshot;
    // this driver loads it the way every production entry point does.
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
        None, // run_id_override
        None, // governed_ctx
        assume_fresh_state,
    )
    .await
}

/// (a) An UNGOVERNED run whose remote-state download fails continues in
/// degraded mode (current semantics) but must suppress BOTH state uploads —
/// the periodic spawn and the terminal upload. RED before PR-A: the run
/// proceeded on the old `state_download_ok = false` bool and the end upload
/// still fired, blindly pushing local state over a remote it could not read.
///
/// `FailAll` (not `FailNext`): the download probe must keep failing however
/// many times any leg retries it.
#[tokio::test]
async fn ungoverned_download_failure_suppresses_all_uploads() {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();
    let project = TestProject::new().await;

    harness.faults.arm(FaultOp::Head, FaultMode::FailAll);

    drive_run(&project.config_path, &project.state_path, false)
        .await
        .expect("an ungoverned run continues past a failed download (degraded mode)");

    harness.faults.clear();
    assert_eq!(
        harness.faults.count(FaultOp::Put),
        0,
        "an Indeterminate-authority run must suppress BOTH the periodic and the terminal \
         state upload — never a blind last-writer-wins over an unread remote"
    );
}

/// (b) A genuine fresh start (empty remote, download resolves `FreshStart`)
/// keeps bootstrapping: the run succeeds and the end-of-run upload is the
/// bootstrap write. Only `Indeterminate` suppresses.
#[tokio::test]
async fn fresh_start_bootstrap_still_uploads() {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();
    let project = TestProject::new().await;

    drive_run(&project.config_path, &project.state_path, false)
        .await
        .expect("a fresh-start replication run must succeed");

    assert!(
        harness.faults.count(FaultOp::Put) >= 1,
        "a FreshStart run must still perform its bootstrap upload"
    );
}

/// (c) `--assume-fresh-state`: an armed download fault + the operator flag ⇒
/// the run treats the failure as an intentional fresh start — it proceeds AND
/// its uploads stay enabled (authority is the trusted `FreshStart`, not
/// `Indeterminate`).
#[tokio::test]
async fn assume_fresh_state_proceeds_and_uploads() {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();
    let project = TestProject::new().await;

    harness.faults.arm(FaultOp::Head, FaultMode::FailAll);

    drive_run(&project.config_path, &project.state_path, true)
        .await
        .expect("--assume-fresh-state elects past the failed download");

    harness.faults.clear();
    assert!(
        harness.faults.count(FaultOp::Put) >= 1,
        "an asserted fresh start is trusted: the end-of-run upload must fire"
    );
}

/// (d) Seam regression: the pre-gate download seams keep their fail-closed
/// `?`-bail with the existing context on a failed download — the PR-A
/// `let _authority` bind is behavior-inert. Pinned through `policy freeze`
/// (the kill switch), whose download half must abort the command rather than
/// record a freeze a later remote download would clobber.
#[test]
fn policy_freeze_download_failure_fails_closed() {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();
    let dir = tempfile::tempdir().expect("create freeze temp dir");
    let config_path = dir.path().join("rocky.toml");
    std::fs::write(
        &config_path,
        "[state]\nbackend = \"s3\"\ns3_bucket = \"test\"\n",
    )
    .expect("write rocky.toml");
    let state_path = dir.path().join(".rocky-state.redb");

    harness.faults.arm(FaultOp::Head, FaultMode::FailAll);

    let err = rocky_cli::commands::run_policy_freeze(
        &config_path,
        &state_path,
        None,  // principal — both
        None,  // scope — any
        false, // lift
        false, // json
    )
    .expect_err("a remote-backend freeze must fail closed when the download fails");

    harness.faults.clear();
    assert!(
        format!("{err:#}")
            .contains("failed to download remote state before recording the policy freeze"),
        "the freeze seam must keep its existing fail-closed context; got: {err:#}"
    );
    assert_eq!(
        harness.faults.count(FaultOp::Put),
        0,
        "a refused freeze must not upload anything"
    );
}
