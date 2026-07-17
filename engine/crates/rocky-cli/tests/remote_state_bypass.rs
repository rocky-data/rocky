//! WP-01 PR-B (2b) integration tests: RD-003 bypass closure — the model-only,
//! transformation, and load arms ride a [`RemoteStateSession`], the load arm
//! is unified onto the canonical state path (remote-key collision fix +
//! legacy logical import), and backfill runs ONE session acquired before the
//! policy gate (R3-4).
//!
//! Drives the REAL CLI paths end-to-end against temp DuckDB projects, with
//! the PR-1 remote-state rig — a fault-injecting in-memory "S3" installed as
//! the process-global provider override — standing in for `[state] backend =
//! "s3"`. Each test holds `remote_testing::serial_guard()` because the
//! override is process-global (the backfill tests also change the process
//! cwd under that same guard).
//!
//! RED-before map (each `download_before_read_*` / `final_persistence_*` /
//! `download_failure_fails_closed_load` / `load_rides_canonical_ledger…` row
//! fails on the pre-2b tree, where these arms performed NO remote sync):
//! - `download_before_read_*`: the mode read stale local state — pod A's
//!   seeded replicated row was invisible.
//! - `final_persistence_*`: the mode's mutation never reached the remote —
//!   pod A's follow-up download saw nothing.
//! - `download_failure_fails_closed_load`: load succeeded against an
//!   unreachable remote.
//! - `load_rides_canonical_ledger_without_clobber`: the loaded-file record
//!   was missing from the shared remote object (and pins that the fix rides
//!   the CANONICAL object without erasing a replication watermark — the
//!   remote-key collision regression guard).
//! - `load_partial_failure_still_finalizes`: RED by design against an
//!   `abandon`-on-partial-failure implementation — file A's record must
//!   survive file B's failure.
//! - `legacy_rows_logically_imported_once`: legacy `<config_dir>/.rocky_state`
//!   rows were the ONLY store load wrote to; now they import (selected
//!   pipeline only, idempotently) into the canonical store.
//! - backfill rows: `backfill_gate_decision_survives_to_remote` guards the
//!   R3-4 one-session consolidation (a second in-executor download would
//!   erase the gate's decision row) — the OLD two-seam shape also passed it,
//!   so it is a regression guard, not RED-before;
//!   `backfill_finalizes_even_on_execution_failure` pins parity with the
//!   deleted upload-even-on-failure.

#![cfg(feature = "duckdb")]

use std::path::{Path, PathBuf};

use rocky_core::fault_store::{FaultMode, FaultOp};
use rocky_core::state::{LoadedFileRecord, StateStore};
use rocky_core::state_sync::remote_testing;
use rocky_core::test_harness::{CrossPodHarness, Pod};

/// A loaded-file marker row seeded into a pod's store — the replicated-table
/// canary the download-before-read assertions look for.
fn marker_record() -> LoadedFileRecord {
    LoadedFileRecord {
        loaded_at: chrono::Utc::now(),
        rows_loaded: 7,
        bytes_read: 42,
        duration_ms: 1,
        file_hash: None,
    }
}

/// Seed pod A's local store with a replicated marker row and upload it, so
/// the shared remote object carries state the project pod has never seen.
async fn seed_remote_marker(harness: &CrossPodHarness, pod: &Pod) {
    {
        let store = harness.open_store(pod);
        store
            .record_loaded_file("seed", "marker.csv", &marker_record())
            .expect("seed marker row");
    }
    harness.upload(pod).await.expect("seed upload");
}

/// True when `state_path`'s store carries the seeded marker row.
fn has_marker(state_path: &Path) -> bool {
    let store = StateStore::open_read_only(state_path).expect("open store read-only");
    store
        .get_loaded_file("seed", "marker.csv")
        .expect("read marker row")
        .is_some()
}

/// A temp DuckDB project with a transformation pipeline + one model
/// (`m1` → `wh.main.m1`), remote `[state] backend = "s3"` resolved to the
/// harness store. Serves both the transformation arm (pipeline dispatch) and
/// the model-only arm (`--model m1`).
struct ModelProject {
    _dir: tempfile::TempDir,
    config_path: PathBuf,
    state_path: PathBuf,
    models_dir: PathBuf,
}

impl ModelProject {
    fn new(model_sql: &str) -> Self {
        let dir = tempfile::tempdir().expect("create project temp dir");
        let db = dir.path().join("wh.duckdb");
        let models_dir = dir.path().join("models");
        std::fs::create_dir(&models_dir).expect("create models dir");
        std::fs::write(models_dir.join("m1.sql"), model_sql).expect("write m1.sql");
        std::fs::write(
            models_dir.join("m1.toml"),
            r#"
name = "m1"

[strategy]
type = "full_refresh"

[target]
catalog = "wh"
schema = "main"
table = "m1"
"#,
        )
        .expect("write m1.toml");
        let config_path = dir.path().join("rocky.toml");
        std::fs::write(
            &config_path,
            format!(
                r#"
[adapter.wh]
type = "duckdb"
path = "{db}"

[pipeline.silver]
type = "transformation"
models = "models/**"

[pipeline.silver.target]
adapter = "wh"

[state]
backend = "s3"
s3_bucket = "test"

[state.retry]
max_retries = 0
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
            models_dir,
        }
    }
}

/// Drive the real `commands::run` (ungoverned). `model` selects the
/// model-only arm; `None` dispatches on the resolved pipeline.
async fn drive_run(
    project: &ModelProject,
    model: Option<&str>,
    run_id_override: Option<&str>,
) -> anyhow::Result<()> {
    let loaded = std::sync::Arc::new(rocky_core::config::load_rocky_config_fingerprinted(
        &project.config_path,
    )?);
    rocky_cli::commands::run(
        &project.config_path,
        loaded,
        None, // filter
        None, // pipeline_name_arg — single pipeline resolves
        &project.state_path,
        None,  // governance_override
        false, // output_json
        Some(&project.models_dir),
        false, // run_all
        None,  // resume_run_id
        false, // resume_latest
        None,  // shadow_config
        &rocky_cli::commands::PartitionRunOptions::default(),
        model,
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

/// A temp DuckDB project with a load pipeline (`ingest`, `data/*.csv` →
/// `wh.main`), remote or local `[state]` per `state_block`.
struct LoadProject {
    _dir: tempfile::TempDir,
    dir: PathBuf,
    config_path: PathBuf,
    state_path: PathBuf,
    csv_path: PathBuf,
}

impl LoadProject {
    fn new(state_block: &str, contract: bool) -> Self {
        let dir = tempfile::tempdir().expect("create project temp dir");
        let root = dir.path().to_path_buf();
        let db = root.join("wh.duckdb");
        let data_dir = root.join("data");
        std::fs::create_dir(&data_dir).expect("create data dir");
        let csv_path = data_dir.join("a_good.csv");
        std::fs::write(&csv_path, "id,product\n1,Widget\n2,Gadget\n").expect("write csv");
        let contract_block = if contract {
            r#"
[pipeline.ingest.contract]
required_columns = [
    { name = "id", type = "BIGINT" },
    { name = "product", type = "VARCHAR" },
]
"#
        } else {
            ""
        };
        let config_path = root.join("rocky.toml");
        std::fs::write(
            &config_path,
            format!(
                r#"
[adapter.wh]
type = "duckdb"
path = "{db}"

[pipeline.ingest]
type = "load"
source_dir = "data/"
format = "csv"

[pipeline.ingest.target]
adapter = "wh"
catalog = ""
schema = "main"
table = "orders"

[pipeline.ingest.options]
create_table = true
{contract_block}
{state_block}
"#,
                db = db.display()
            ),
        )
        .expect("write rocky.toml");
        let state_path = root.join(".rocky-state.redb");
        Self {
            _dir: dir,
            dir: root,
            config_path,
            state_path,
            csv_path,
        }
    }

    fn remote(contract: bool) -> Self {
        Self::new(
            "[state]\nbackend = \"s3\"\ns3_bucket = \"test\"\n\n[state.retry]\nmax_retries = 0\n",
            contract,
        )
    }
}

/// Drive the real `run_load` the way `main.rs`'s `Command::Load` does.
async fn drive_load(project: &LoadProject) -> anyhow::Result<()> {
    let loaded = rocky_core::config::load_rocky_config_fingerprinted(&project.config_path)?;
    rocky_cli::commands::run_load(
        &project.config_path,
        &loaded,
        &project.state_path,
        None, // cli_source_dir
        None, // format
        None, // target_table
        None, // pipeline — single pipeline resolves
        false,
        rocky_core::state_sync::FinalizeDurability::ConfigDefault,
        false,
    )
    .await
}

// ---------------------------------------------------------------------------
// download-before-read (RED before 2b: the bypassed arms read stale local)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn download_before_read_model_only() {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();
    seed_remote_marker(&harness, &harness.pod_a).await;
    let project = ModelProject::new("SELECT 1 AS id\n");

    drive_run(&project, Some("m1"), None)
        .await
        .expect("model-only run succeeds");

    assert!(
        has_marker(&project.state_path),
        "the model-only arm must download-before-read: pod A's replicated row must be \
         visible in the project's local ledger"
    );
}

#[tokio::test]
async fn download_before_read_transformation() {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();
    seed_remote_marker(&harness, &harness.pod_a).await;
    let project = ModelProject::new("SELECT 1 AS id\n");

    drive_run(&project, None, None)
        .await
        .expect("transformation run succeeds");

    assert!(
        has_marker(&project.state_path),
        "the transformation arm must download-before-read: pod A's replicated row must be \
         visible in the project's local ledger"
    );
}

#[tokio::test]
async fn download_before_read_load() {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();
    seed_remote_marker(&harness, &harness.pod_a).await;
    let project = LoadProject::remote(false);

    drive_load(&project).await.expect("load succeeds");

    assert!(
        has_marker(&project.state_path),
        "the load arm must download-before-read on the CANONICAL state path: pod A's \
         replicated row must be visible in the project's local ledger"
    );
}

// ---------------------------------------------------------------------------
// final persistence (RED before 2b: the arms' mutations never reached remote)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn final_persistence_model_only() {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();
    let project = ModelProject::new("SELECT 1 AS id\n");

    drive_run(&project, Some("m1"), Some("run-model-2b"))
        .await
        .expect("model-only run succeeds");

    let _authority = harness
        .download(&harness.pod_b)
        .await
        .expect("pod B start-download");
    let store = harness.open_store(&harness.pod_b);
    assert!(
        store.get_run("run-model-2b").expect("read run").is_some(),
        "the model-only run record must ride the terminal upload: a second pod's \
         download sees it"
    );
}

#[tokio::test]
async fn final_persistence_transformation() {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();
    let project = ModelProject::new("SELECT 1 AS id\n");

    drive_run(&project, None, Some("run-tx-2b"))
        .await
        .expect("transformation run succeeds");

    let _authority = harness
        .download(&harness.pod_b)
        .await
        .expect("pod B start-download");
    let store = harness.open_store(&harness.pod_b);
    assert!(
        store.get_run("run-tx-2b").expect("read run").is_some(),
        "the transformation run record must ride the terminal upload: a second pod's \
         download sees it"
    );
}

#[tokio::test]
async fn final_persistence_load() {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();
    let project = LoadProject::remote(false);

    drive_load(&project).await.expect("load succeeds");

    let _authority = harness
        .download(&harness.pod_b)
        .await
        .expect("pod B start-download");
    let store = harness.open_store(&harness.pod_b);
    assert!(
        store
            .get_loaded_file("ingest", &project.csv_path.display().to_string())
            .expect("read loaded-file row")
            .is_some(),
        "the loaded-file record must ride the terminal upload: a second pod's download \
         sees it (it will not re-load the file)"
    );
}

// ---------------------------------------------------------------------------
// download-failure behavior
// ---------------------------------------------------------------------------

/// Load is a mutating single-purpose command ⇒ `require_synced` is
/// UNCONDITIONAL: a failed download refuses the load (fail-closed) and never
/// uploads. RED before 2b: load ignored the remote entirely and succeeded.
#[tokio::test]
async fn download_failure_fails_closed_load() {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();
    let project = LoadProject::remote(false);

    harness.faults.arm(FaultOp::Head, FaultMode::FailAll);
    let err = drive_load(&project)
        .await
        .expect_err("a load on an unreadable remote must fail closed");
    harness.faults.clear();

    assert!(
        format!("{err:#}").contains("failed to download remote state before the load"),
        "the refusal must cite the failed download; got: {err:#}"
    );
    assert_eq!(
        harness.faults.count(FaultOp::Put),
        0,
        "a refused load must not upload anything"
    );
}

/// Ungoverned model-only run: a failed download warns-and-continues on local
/// state (availability parity with the replication arm) but suppresses the
/// terminal upload (`count(Put) == 0` — never a blind last-writer-wins).
#[tokio::test]
async fn ungoverned_continues_and_suppresses_model_only() {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();
    let project = ModelProject::new("SELECT 1 AS id\n");

    harness.faults.arm(FaultOp::Head, FaultMode::FailAll);
    drive_run(&project, Some("m1"), None)
        .await
        .expect("an ungoverned model-only run continues past a failed download");
    harness.faults.clear();

    assert_eq!(
        harness.faults.count(FaultOp::Put),
        0,
        "an Indeterminate-authority model-only run must suppress the terminal upload"
    );
}

/// Same availability + suppression contract for the transformation arm.
#[tokio::test]
async fn ungoverned_continues_and_suppresses_transformation() {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();
    let project = ModelProject::new("SELECT 1 AS id\n");

    harness.faults.arm(FaultOp::Head, FaultMode::FailAll);
    drive_run(&project, None, None)
        .await
        .expect("an ungoverned transformation run continues past a failed download");
    harness.faults.clear();

    assert_eq!(
        harness.faults.count(FaultOp::Put),
        0,
        "an Indeterminate-authority transformation run must suppress the terminal upload"
    );
}

// ---------------------------------------------------------------------------
// load: canonical-object collision regression + legacy logical import
// ---------------------------------------------------------------------------

/// The collision regression test (ADR §3): the legacy load path and the
/// canonical state path map to ONE remote object (`state.redb` — pinned by
/// the `remote_state_key` unit test in rocky-core). After the fix, a load
/// RIDES the canonical ledger: the remote object ends up carrying BOTH the
/// pre-existing replication watermark AND the new loaded-file row — nothing
/// clobbered in either direction.
#[tokio::test]
async fn load_rides_canonical_ledger_without_clobber() {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();

    // Pod A: a replication run's watermark reaches the shared remote object.
    {
        let store = harness.open_store(&harness.pod_a);
        store
            .set_watermark(
                "wh.raw.orders",
                &rocky_ir::WatermarkState {
                    last_value: chrono::Utc::now(),
                    updated_at: chrono::Utc::now(),
                },
            )
            .expect("seed watermark");
    }
    harness.upload(&harness.pod_a).await.expect("seed upload");

    let project = LoadProject::remote(false);
    drive_load(&project).await.expect("load succeeds");

    // A fresh pod's download must see BOTH rows on the one shared object.
    let _authority = harness
        .download(&harness.pod_b)
        .await
        .expect("pod B start-download");
    let store = harness.open_store(&harness.pod_b);
    assert!(
        store
            .get_watermark("wh.raw.orders")
            .expect("read watermark")
            .is_some(),
        "the load must NOT clobber the canonical ledger: the replication watermark \
         survives the load's terminal upload"
    );
    assert!(
        store
            .get_loaded_file("ingest", &project.csv_path.display().to_string())
            .expect("read loaded-file row")
            .is_some(),
        "the load's own record must ride the same canonical object"
    );
}

/// Legacy migration is a LOGICAL IMPORT, never a rename: only the selected
/// pipeline's rows are imported into the canonical store, the import is
/// idempotent (existing canonical keys win), and the legacy file is left in
/// place with all of its rows.
#[tokio::test]
async fn legacy_rows_logically_imported_once() {
    let _serial = remote_testing::serial_guard();
    // Local backend: the session is a zero-I/O no-op; the import logic is
    // backend-independent.
    let project = LoadProject::new("", false);

    // A shared legacy DB holding TWO pipelines' rows.
    let legacy_path = project.dir.join(".rocky_state");
    let legacy_record = marker_record();
    {
        let legacy = StateStore::open(&legacy_path).expect("create legacy store");
        legacy
            .record_loaded_file("ingest", "/old/ingested.csv", &legacy_record)
            .expect("legacy ingest row");
        legacy
            .record_loaded_file("other", "/old/other.csv", &legacy_record)
            .expect("legacy other row");
    }

    drive_load(&project).await.expect("first load succeeds");

    {
        let store = StateStore::open_read_only(&project.state_path).expect("open canonical");
        let imported = store
            .get_loaded_file("ingest", "/old/ingested.csv")
            .expect("read imported row")
            .expect("the selected pipeline's legacy row must be imported");
        assert_eq!(imported.rows_loaded, legacy_record.rows_loaded);
        assert!(
            store
                .get_loaded_file("other", "/old/other.csv")
                .expect("read other row")
                .is_none(),
            "ONLY the selected pipeline's rows are imported — a shared legacy DB can \
             hold other pipelines' rows"
        );
        assert!(
            store
                .get_loaded_file("ingest", &project.csv_path.display().to_string())
                .expect("read new row")
                .is_some(),
            "the load itself records into the canonical store"
        );
    }

    // Second run: idempotent (existing canonical keys win — the imported
    // row's original timestamp survives), and the legacy file is untouched.
    drive_load(&project).await.expect("second load succeeds");
    {
        let store = StateStore::open_read_only(&project.state_path).expect("open canonical");
        let imported = store
            .get_loaded_file("ingest", "/old/ingested.csv")
            .expect("read imported row")
            .expect("still present");
        assert_eq!(
            imported.loaded_at, legacy_record.loaded_at,
            "re-running the import must not overwrite the canonical row"
        );
    }
    let legacy = StateStore::open_read_only(&legacy_path).expect("legacy file still opens");
    assert!(
        legacy
            .get_loaded_file("ingest", "/old/ingested.csv")
            .expect("read legacy row")
            .is_some()
            && legacy
                .get_loaded_file("other", "/old/other.csv")
                .expect("read legacy other row")
                .is_some(),
        "the legacy file is left in place with ALL of its rows (never renamed or drained)"
    );
}

/// Partial failure still finalizes (RED by design vs an `abandon`-on-failure
/// implementation): with two files where the second violates the contract,
/// the load exits nonzero AND file A's record still reaches the remote.
#[tokio::test]
async fn load_partial_failure_still_finalizes() {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();
    let project = LoadProject::remote(true);
    // Second file (sorts after a_good.csv): missing `product`, `id` lands as
    // VARCHAR — violates the contract, fails its per-file gate.
    std::fs::write(project.dir.join("data/b_bad.csv"), "id,qty\nabc,10\n").expect("write bad csv");

    let err = drive_load(&project)
        .await
        .expect_err("a partial load failure must exit nonzero");
    assert!(
        format!("{err:#}").contains("failed to load"),
        "the exit must report the failed file(s); got: {err:#}"
    );

    let _authority = harness
        .download(&harness.pod_b)
        .await
        .expect("pod B start-download");
    let store = harness.open_store(&harness.pod_b);
    assert!(
        store
            .get_loaded_file("ingest", &project.csv_path.display().to_string())
            .expect("read loaded-file row")
            .is_some(),
        "file A's record must survive file B's failure: the session captures per-file \
         results and ALWAYS finalizes — an abandon would drop file A's state and the \
         next pod would re-load it"
    );
}

// ---------------------------------------------------------------------------
// backfill: one session, acquired before the gate (R3-4)
// ---------------------------------------------------------------------------

/// Build a backfill project (transformation model + `[policy]` plane +
/// remote state), persist a Backfill plan + review marker, and return
/// `(project, plan_id)`.
fn backfill_fixture(model_sql: &str) -> (ModelProject, String) {
    let project = ModelProject::new(model_sql);
    let root = project.config_path.parent().unwrap().to_path_buf();
    // Add a `[policy]` plane so the gate writes a decision row. No rules:
    // the default agent effect (`require_review`) is satisfied by the review
    // marker the always-on backfill gate demands anyway.
    let mut cfg = std::fs::read_to_string(&project.config_path).expect("read config");
    cfg.push_str("\n[policy]\nversion = 1\n");
    std::fs::write(&project.config_path, cfg).expect("append policy block");

    let plan_id = rocky_cli::plan_store::write_plan(
        &root,
        rocky_cli::plan_store::PlanKind::Backfill,
        &serde_json::json!({
            "models": ["m1"],
            "models_dir": "models",
        }),
    )
    .expect("persist backfill plan");
    let marker = root.join(".rocky").join("plans");
    std::fs::create_dir_all(&marker).expect("plans dir");
    std::fs::write(marker.join(format!("{plan_id}.reviewed.json")), "{}")
        .expect("write review marker");
    (project, plan_id)
}

/// Apply the backfill plan through the real `run_apply` dispatcher. `root`
/// becomes the process cwd for the duration (plans + relative `models_dir`
/// resolve against it) — safe under the held `serial_guard`.
async fn drive_backfill_apply(project: &ModelProject, plan_id: &str) -> anyhow::Result<()> {
    let root = project.config_path.parent().unwrap().to_path_buf();
    let prev_cwd = std::env::current_dir().expect("read cwd");
    std::env::set_current_dir(&root).expect("enter project root");
    let result = rocky_cli::commands::run_apply(
        &project.config_path,
        plan_id,
        &project.state_path,
        rocky_core::config::PolicyPrincipal::Human, // kind-forces Agent anyway
        false,
    )
    .await;
    std::env::set_current_dir(prev_cwd).expect("restore cwd");
    result
}

/// R3-4: the policy gate's decision row — written BETWEEN the session's
/// single download and the terminal upload — survives to the remote. Guards
/// the one-session consolidation: a second in-executor download would
/// wholesale-replace `POLICY_DECISIONS` and erase the row. (The old
/// two-seam shape also passed this — it is the regression guard for the
/// consolidation, not a RED-before row.)
#[tokio::test]
async fn backfill_gate_decision_survives_to_remote() {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();
    let (project, plan_id) = backfill_fixture("SELECT 1 AS id\n");

    drive_backfill_apply(&project, &plan_id)
        .await
        .expect("reviewed backfill applies");

    let _authority = harness
        .download(&harness.pod_b)
        .await
        .expect("pod B start-download");
    let store = harness.open_store(&harness.pod_b);
    let decisions = store.list_policy_decisions().expect("list decisions");
    assert!(
        decisions.iter().any(|d| d.plan_id == plan_id),
        "the gate's decision row must survive to the remote ledger (one session: no \
         second download between gate and upload); got {} decision(s)",
        decisions.len()
    );
    assert!(
        harness.faults.count(FaultOp::Put) >= 1,
        "the backfill's terminal finalize must upload"
    );
}

/// Parity pin with the deleted `upload_remote_ledger_fail_closed`-even-on-
/// failure: a failing backfill still finalizes, so its partial ledger
/// mutations (the Failure run record) reach the remote.
#[tokio::test]
async fn backfill_finalizes_even_on_execution_failure() {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();
    // A model that cannot compile/execute: unknown upstream reference.
    let (project, plan_id) = backfill_fixture("SELECT * FROM no_such_upstream\n");

    let err = drive_backfill_apply(&project, &plan_id)
        .await
        .expect_err("a failing backfill must exit nonzero");
    assert!(
        format!("{err:#}").contains(&plan_id) || format!("{err:#}").contains("failed"),
        "the error must be the execution failure; got: {err:#}"
    );

    assert!(
        harness.faults.count(FaultOp::Put) >= 1,
        "the backfill session must ALWAYS finalize — even on execution failure — so \
         partial ledger mutations reach the remote (the old upload-even-on-failure \
         contract)"
    );
}
