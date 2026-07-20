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
//!
//! Red-team round (RED against the pre-fix 2b tree):
//! - `governed_without_policy_durable_*`: durability selection was
//!   conjoined with `[policy]`, so a governed apply without a policy plane
//!   exited 0 on a lost terminal upload (FIX 1).
//! - `backfill_forward_incompat_recreate_suppresses_upload`: the backfill
//!   executor uploaded a downgraded recreated blob over newer shared state
//!   (FIX 2).
//! - `noop_transformation_without_models_dir_does_no_remote_io`: the
//!   transformation arm acquired + finalized a session for a run that
//!   mutates no state (FIX 3).
//!
//! Re-review round (RED against the pre-fix red-team tree):
//! - `noop_transformation_with_existing_state_file_skips_sessionless_sweep`:
//!   the no-op arm fell through to the end-of-run retention auto-sweep,
//!   which opened + stamped the state store with NO session (FIX B); the
//!   fresh-file FIX 3 test missed it via the sweep's absent-file early
//!   return. The single-decision gate half (FIX A) is pinned by
//!   `passed_absent_decision_governs_even_when_dir_exists` in
//!   `run_local.rs`'s unit tests (it needs the private entry point).

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
        Self::new_inner(Some(model_sql))
    }

    /// A project whose `rocky.toml` declares `models = "models/**"` but whose
    /// models directory does NOT exist — the transformation no-op path
    /// (`run_local` opens no state store, executes nothing).
    fn without_models_dir() -> Self {
        Self::new_inner(None)
    }

    fn new_inner(model_sql: Option<&str>) -> Self {
        let dir = tempfile::tempdir().expect("create project temp dir");
        let db = dir.path().join("wh.duckdb");
        let models_dir = dir.path().join("models");
        if let Some(model_sql) = model_sql {
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
        }
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

/// Drive the real `commands::run` GOVERNED, against a config that has NO
/// `[policy]` block: a directly-constructed legacy-shaped
/// [`rocky_cli::commands::apply::GovernedRunContext`] (no bound fingerprint,
/// `require_fingerprint: false`, no reviewed schemas), which every governed
/// gate lets through. `model` selects the model-only arm; `None` dispatches
/// on the resolved pipeline (transformation or load).
async fn drive_run_governed(
    config_path: &Path,
    state_path: &Path,
    models_dir: Option<&Path>,
    model: Option<&str>,
) -> anyhow::Result<()> {
    let root = config_path.parent().expect("project root").to_path_buf();
    let ctx = rocky_cli::commands::apply::GovernedRunContext {
        principal: rocky_core::config::PolicyPrincipal::Agent,
        plan_id: "plan-durability-redteam",
        root: &root,
        config_path,
        expected_ir_fingerprint: None,
        expected_config_identity: None,
        require_fingerprint: false,
        reviewed_source_schemas: None,
        expects_models: false,
        replication_verify_after: Default::default(),
    };
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
        models_dir,
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
        None, // run_id_override
        Some(&ctx),
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
         sees the shared ledger entry"
    );
}

// ---------------------------------------------------------------------------
// download-failure behavior
// ---------------------------------------------------------------------------

/// Load is a mutating single-purpose command ⇒ `require_synced` is
/// UNCONDITIONAL: a failed download refuses the load (fail-closed) and never
/// uploads. RED before 2b: load ignored the remote entirely and succeeded.
///
/// This is a DELIBERATE, documented exception to the general ungoverned
/// degraded-continue contract (ADR-STATE-SESSION §3). Honest scope
/// (re-review corrected): nothing in the engine consults `loaded_files` to
/// skip a load today — `run_load` loads every discovered file without
/// reading the ledger and records only after success — so a missing remote
/// record does not by itself cause duplicate ingestion. The rationale is
/// ledger integrity, not dedup: `loaded_files` is the replicated cross-pod
/// record of what each pod ingested (the audit trail and the foundation any
/// future incremental-skip consumer reads), and proceeding degraded would
/// silently orphan it (the Indeterminate authority suppresses the upload,
/// stranding this run's records locally, invisible to other pods). Refusing
/// beats proceeding blind on unreadable shared state for a command whose
/// only job is the mutation being recorded. Do NOT "fix" this to the run
/// arms' governed-only bail.
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
// durability decoupled from [policy] (red-team FIX 1, ADR-AUTHORITY §5)
// ---------------------------------------------------------------------------
//
// "Ledger durability is orthogonal to policy enforcement — a governed apply
// that recorded any state mutation must persist it, whether or not a
// `[policy]` plane is present." Each arm's durability selection must key on
// the governed context ALONE; `&& rocky_cfg.policy.is_some()` stays only on
// the download-failure fail-closed bail. Pre-fix RED (all three): the arms
// selected `Durable` via `exec_fp_gate.is_some() && rocky_cfg.policy
// .is_some()`, so a governed apply without `[policy]` fell to
// `ConfigDefault` + default `skip` and exited 0 on a lost terminal upload.
// (The replication seam's twin lives in `remote_state_session.rs`.)

#[tokio::test]
async fn governed_without_policy_durable_model_only() {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();
    let project = ModelProject::new("SELECT 1 AS id\n");

    harness.faults.arm(FaultOp::Put, FaultMode::FailAll);
    let err = drive_run_governed(
        &project.config_path,
        &project.state_path,
        Some(&project.models_dir),
        Some("m1"),
    )
    .await
    .expect_err("a governed model-only run must refuse exit 0 on a lost terminal upload");
    harness.faults.clear();

    assert!(
        format!("{err:#}").contains("could not be persisted to the remote [state] backend"),
        "the exit must cite the refused state persistence; got: {err:#}"
    );
}

#[tokio::test]
async fn governed_without_policy_durable_transformation() {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();
    let project = ModelProject::new("SELECT 1 AS id\n");

    harness.faults.arm(FaultOp::Put, FaultMode::FailAll);
    let err = drive_run_governed(
        &project.config_path,
        &project.state_path,
        Some(&project.models_dir),
        None,
    )
    .await
    .expect_err("a governed transformation run must refuse exit 0 on a lost terminal upload");
    harness.faults.clear();

    assert!(
        format!("{err:#}").contains("could not be persisted to the remote [state] backend"),
        "the exit must cite the refused state persistence; got: {err:#}"
    );
}

#[tokio::test]
async fn governed_without_policy_durable_load_arm() {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();
    let project = LoadProject::remote(false);

    harness.faults.arm(FaultOp::Put, FaultMode::FailAll);
    let err = drive_run_governed(&project.config_path, &project.state_path, None, None)
        .await
        .expect_err("a governed load-arm run must refuse exit 0 on a lost terminal upload");
    harness.faults.clear();

    assert!(
        format!("{err:#}").contains("could not be persisted to the"),
        "the exit must cite the refused state persistence; got: {err:#}"
    );
}

// ---------------------------------------------------------------------------
// lazy finalize: the no-op transformation does ZERO remote I/O (red-team FIX 3)
// ---------------------------------------------------------------------------

/// ADR-STATE-SESSION (lazy finalize): a transformation run whose models
/// directory does not exist executes nothing and opens no state store
/// (`run_local`), so it must perform NO remote state I/O at all — the
/// session is not even created (mirroring load's empty-file pre-session
/// return). Uploading the unchanged blob anyway would be a pure
/// last-writer-wins erasure vector against concurrent pods. Pre-fix RED: the
/// transformation arm acquired + finalized unconditionally (a `Head`/`Get`
/// on acquire, a `Put` on finalize).
#[tokio::test]
async fn noop_transformation_without_models_dir_does_no_remote_io() {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();
    let project = ModelProject::without_models_dir();

    drive_run(&project, None, None)
        .await
        .expect("a no-op transformation run (no models dir) exits 0");

    assert_eq!(
        harness.faults.count(FaultOp::Put),
        0,
        "no state mutation occurred — nothing may be uploaded"
    );
    assert_eq!(
        harness.faults.count(FaultOp::Get) + harness.faults.count(FaultOp::Head),
        0,
        "no session at all: the no-op arm must not even download"
    );
}

/// Re-review FIX B: the same no-op transformation on a pod WITH an existing
/// state file — the fresh-project test above let the sweep's
/// `!state_path.exists()` early return mask this. "Mutates no state" must
/// include `run()`'s end-of-run retention auto-sweep: pre-fix RED, the no-op
/// arm's `return` only exited the inner `run_result` async block, fell
/// through to `auto_sweep_retention_at_end_of_run`, opened the store, swept
/// (default retention domains are non-empty), and stamped
/// `last_retention_sweep_at` — a sessionless state mutation (RD-003 shape) on
/// the one path DEFINED by mutating nothing. Post-fix: zero remote I/O AND
/// the state file byte-identical, with no sweep stamp.
#[tokio::test]
async fn noop_transformation_with_existing_state_file_skips_sessionless_sweep() {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();
    let project = ModelProject::without_models_dir();

    // A pre-existing local state file with NO prior sweep stamp — the pod
    // shape the fresh-absent-file test misses.
    {
        let store = StateStore::open(&project.state_path).expect("seed state file");
        store
            .record_loaded_file("seed", "local.csv", &marker_record())
            .expect("seed a row");
    }
    let before = std::fs::read(&project.state_path).expect("read state file before");

    drive_run(&project, None, None)
        .await
        .expect("a no-op transformation run (no models dir) exits 0");

    assert_eq!(
        harness.faults.count(FaultOp::Put)
            + harness.faults.count(FaultOp::Get)
            + harness.faults.count(FaultOp::Head),
        0,
        "the no-op arm performs zero remote I/O even when local state exists"
    );
    let after = std::fs::read(&project.state_path).expect("read state file after");
    assert_eq!(
        before, after,
        "a no-op run mutates NOTHING: the end-of-run retention sweep must not \
         open + mutate the state store sessionlessly"
    );
    let store = StateStore::open_read_only(&project.state_path).expect("open read-only");
    assert!(
        store
            .get_last_retention_sweep_at()
            .expect("read sweep stamp")
            .is_none(),
        "the sessionless retention sweep must be SKIPPED on the no-op path — \
         no last_retention_sweep_at stamp"
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
         results and ALWAYS finalizes — an abandon would drop file A's ledger entry \
         from the shared remote"
    );
}

// ---------------------------------------------------------------------------
// backfill: one session, acquired before the gate (R3-4)
// ---------------------------------------------------------------------------

/// Build a backfill project (transformation model + optional `[policy]`
/// plane + remote state), persist a Backfill plan + review marker, and
/// return `(project, plan_id)`.
fn backfill_fixture(model_sql: &str) -> (ModelProject, String) {
    backfill_fixture_with(model_sql, true)
}

fn backfill_fixture_with(model_sql: &str, with_policy: bool) -> (ModelProject, String) {
    let project = ModelProject::new(model_sql);
    let root = project.config_path.parent().unwrap().to_path_buf();
    if with_policy {
        // Add a `[policy]` plane so the gate writes a decision row. No rules:
        // the default agent effect (`require_review`) is satisfied by the
        // review marker the always-on backfill gate demands anyway.
        let mut cfg = std::fs::read_to_string(&project.config_path).expect("read config");
        cfg.push_str("\n[policy]\nversion = 1\n");
        std::fs::write(&project.config_path, cfg).expect("append policy block");
    }

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

/// Red-team FIX 2: a backfill whose canonical store was RECREATED because the
/// on-disk schema was forward-incompatible (written by a NEWER binary, under
/// the default `on_schema_mismatch = "recreate"`) must SUPPRESS its terminal
/// upload — pushing the downgraded recreated blob would clobber the newer
/// shared state upgraded pods depend on (the `state.rs` caller obligation
/// every other arm honors: replication, model-only, load). The
/// always-finalize shape stays: a suppressed finalize consumes the session
/// without uploading. Pre-fix RED: `execute_backfill_set` never consulted
/// `was_recreated_for_forward_incompat()` and always uploaded — the final
/// Put count grew and pod B's download saw the DOWNGRADED (current-version)
/// blob instead of the newer pod's state.
///
/// The forward-incompat state arrives the way it does in production: a
/// NEWER pod uploaded its (v+1) ledger to the shared remote, and this
/// (older) binary's backfill session restores it on acquire. Stamping only
/// the local file cannot drive this path — an absent-remote download
/// rebuilds the local file fresh (replicated tables reset), erasing a local
/// stamp before the executor ever opens the store.
///
/// No `[policy]` plane: suppression is policy-independent, and a forward-
/// incompatible ledger under a `[policy]` plane is refused earlier by the
/// gate's own unreadable-ledger fail-closed deny.
#[tokio::test]
async fn backfill_forward_incompat_recreate_suppresses_upload() {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();
    let (project, plan_id) = backfill_fixture_with("SELECT 1 AS id\n", false);
    let future_version = rocky_core::state::current_schema_version() + 1;

    // Pod A is the "newer binary": its uploaded ledger carries schema v+1.
    // (`upload_state` copies the file raw + drops local-only tables via redb
    // directly — no version check — so the stamp survives to the remote.)
    rocky_core::state::force_schema_version(&harness.pod_a.state_path, &future_version.to_string());
    harness
        .upload(&harness.pod_a)
        .await
        .expect("newer pod's upload");
    let puts_before = harness.faults.count(FaultOp::Put);

    drive_backfill_apply(&project, &plan_id)
        .await
        .expect("a recreate-policy backfill proceeds as a fresh full rebuild");

    assert_eq!(
        harness.faults.count(FaultOp::Put),
        puts_before,
        "a forward-incompat recreated store must suppress the backfill's terminal upload \
         — uploading would push the downgraded blob over the newer shared state"
    );
    // Non-vacuous: the recreate really fired — the acquire restored the v+1
    // blob and the executor's open re-bootstrapped it at this binary's
    // version.
    assert_eq!(
        StateStore::peek_schema_version(&project.state_path).expect("peek recreated version"),
        Some(rocky_core::state::current_schema_version()),
        "the executor must have recreated the store (proves the suppression path was \
         reachable, not vacuously skipped)"
    );
    // The no-clobber end state: a fresh pod's download still restores the
    // NEWER pod's v+1 blob.
    let _authority = harness
        .download(&harness.pod_b)
        .await
        .expect("pod B start-download");
    assert_eq!(
        StateStore::peek_schema_version(&harness.pod_b.state_path).expect("peek pod B version"),
        Some(future_version),
        "the shared remote must still carry the newer pod's state, not a downgraded blob"
    );
}
