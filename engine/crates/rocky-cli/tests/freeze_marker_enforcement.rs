//! WP-01 PR-F integration tests: durable freeze/unfreeze markers — the
//! rollout-independent kill switch (ADR-CONCURRENCY.md D3).
//!
//! Drives the REAL CLI paths (`rocky policy freeze`/`unfreeze`, the governed
//! apply gate stack, `commands::run`, `compute_brief`) end-to-end against
//! temp DuckDB projects, with the PR-1 remote-state rig — a fault-injecting
//! in-memory "S3" installed as the process-global provider override —
//! standing in for `[state] backend = "s3"`. Each test holds
//! `remote_testing::serial_guard()` because the override is process-global.
//!
//! What is pinned here:
//! - **Phase-2 write path:** with `[state] freeze_marker_writes = true`,
//!   `policy freeze` writes one marker per principal (bodies carry
//!   principal/scope/reason) IN ADDITION to the ledger rows; `unfreeze`
//!   writes a marker naming the exact `freeze_id`s it lifts.
//! - **Two-phase default OFF:** with the flag unset no marker is ever
//!   written — the write path is byte-identical to today's.
//! - **Phase-1 reader semantics:** marker READING + enforcement are always
//!   on — a gate denies a seeded marker even when this pod's own write flag
//!   is off, and a marker-only freeze (no ledger row at all — the erasure
//!   shape) denies with the `freeze:<id>` citation.
//! - **Fences:** a marker present at run time withholds the transformation
//!   layer loop and the replication fan-out fail-closed, while the run still
//!   persists + finalizes (uploads) — partial failure, never state loss.
//! - **Fail-closed LIST:** a marker-LIST transport failure refuses a
//!   `[policy]`-configured run at entry, and warns-and-continues without one.
//! - **Reader-less pod (negative documentation):** a gate driven with the
//!   marker projection forced empty ignores a present marker — exactly why
//!   the two-phase rollout (readers everywhere first) is mandatory.
//! - **Status parity:** `rocky brief` merges marker-only freezes, and an
//!   unreachable marker tier is a section note, never a silent all-clear.

#![cfg(feature = "duckdb")]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use rocky_cli::commands::PolicyGate;
use rocky_core::config::{PolicyCapability, PolicyPrincipal};
use rocky_core::fault_store::{FaultMode, FaultOp};
use rocky_core::freeze_marker::{self, FreezeMarker, UnfreezeMarker};
use rocky_core::state::{RunStatus, StateStore};
use rocky_core::state_sync::remote_testing;
use rocky_core::test_harness::CrossPodHarness;
use rocky_core::traits::WarehouseAdapter;
use rocky_duckdb::adapter::DuckDbWarehouseAdapter;

/// Drive an async assertion block from a sync `#[test]` body (the freeze
/// command itself is synchronous).
fn block_on<T>(fut: impl std::future::Future<Output = T>) -> T {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build assertion runtime")
        .block_on(fut)
}

fn seed_marker(id: &str, principal: PolicyPrincipal, scope: &str) -> FreezeMarker {
    FreezeMarker {
        freeze_id: id.to_string(),
        principal,
        scope: scope.to_string(),
        reason: format!("test freeze {id}"),
        created_at: chrono::Utc::now(),
    }
}

// ---------------------------------------------------------------------------
// project fixtures
// ---------------------------------------------------------------------------

/// A `[state]`-only project (no adapter, no pipeline) — the shape
/// `rocky policy freeze` / `compute_brief` / the gate stack run against.
/// `extra` lands inside the `[state]` table (e.g. `freeze_marker_writes =
/// true`); `with_policy` appends a rule-less `[policy]` block.
struct StateOnlyProject {
    dir: tempfile::TempDir,
    config_path: PathBuf,
    state_path: PathBuf,
}

impl StateOnlyProject {
    fn new(extra: &str, with_policy: bool) -> Self {
        let dir = tempfile::tempdir().expect("create project temp dir");
        let policy_block = if with_policy {
            "\n[policy]\nversion = 1\n"
        } else {
            ""
        };
        let config_path = dir.path().join("rocky.toml");
        std::fs::write(
            &config_path,
            format!(
                "[state]\nbackend = \"s3\"\ns3_bucket = \"test\"\n{extra}\n\
                 [state.retry]\nmax_retries = 0\n{policy_block}"
            ),
        )
        .expect("write rocky.toml");
        let state_path = dir.path().join(".rocky-state.redb");
        Self {
            dir,
            config_path,
            state_path,
        }
    }
}

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

/// A minimal temp DuckDB replication project with remote `[state]` and an
/// optional rule-less `[policy]` block (the fence keys on its presence).
struct ReplicationProject {
    _dir: tempfile::TempDir,
    config_path: PathBuf,
    state_path: PathBuf,
}

impl ReplicationProject {
    async fn new(with_policy: bool) -> Self {
        let dir = tempfile::tempdir().expect("create project temp dir");
        let db = dir.path().join("wh.duckdb");
        seed_source(&db).await;
        let policy_block = if with_policy {
            "\n[policy]\nversion = 1\n"
        } else {
            ""
        };
        let config_path = dir.path().join("rocky.toml");
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

[state.retry]
max_retries = 0
{policy_block}"#,
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

/// A temp DuckDB transformation project with one model (`m1` → `wh.main.m1`),
/// remote `[state]`, and an optional rule-less `[policy]` block.
struct ModelProject {
    _dir: tempfile::TempDir,
    config_path: PathBuf,
    state_path: PathBuf,
    models_dir: PathBuf,
}

impl ModelProject {
    fn new(with_policy: bool) -> Self {
        let dir = tempfile::tempdir().expect("create project temp dir");
        let db = dir.path().join("wh.duckdb");
        let models_dir = dir.path().join("models");
        std::fs::create_dir(&models_dir).expect("create models dir");
        std::fs::write(models_dir.join("m1.sql"), "SELECT 1 AS id\n").expect("write m1.sql");
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
        let policy_block = if with_policy {
            "\n[policy]\nversion = 1\n"
        } else {
            ""
        };
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
{policy_block}"#,
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

/// A temp DuckDB transformation project with TWO independent models (`keep`
/// and `drop`, each `SELECT` → `wh.main.<name>`), remote `[state]`, and a
/// rule-less `[policy]` block (the fence keys on its presence). Used to prove
/// the fence sees only the EXECUTING (selected) models.
struct TwoModelProject {
    _dir: tempfile::TempDir,
    config_path: PathBuf,
    state_path: PathBuf,
    models_dir: PathBuf,
}

impl TwoModelProject {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("create project temp dir");
        let db = dir.path().join("wh.duckdb");
        let models_dir = dir.path().join("models");
        std::fs::create_dir(&models_dir).expect("create models dir");
        for (name, expr) in [("keep", "1"), ("drop", "2")] {
            std::fs::write(
                models_dir.join(format!("{name}.sql")),
                format!("SELECT {expr} AS id\n"),
            )
            .expect("write model sql");
            std::fs::write(
                models_dir.join(format!("{name}.toml")),
                format!(
                    r#"
name = "{name}"

[strategy]
type = "full_refresh"

[target]
catalog = "wh"
schema = "main"
table = "{name}"
"#
                ),
            )
            .expect("write model toml");
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

[policy]
version = 1
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

/// Drive the real `commands::run` (ungoverned). `models_dir` selects the
/// transformation shape; `None` lets the replication pipeline resolve.
/// `model_name_filter` mirrors `rocky run --model <name>`.
async fn drive_run(
    config_path: &Path,
    state_path: &Path,
    models_dir: Option<&Path>,
    model_name_filter: Option<&str>,
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
        models_dir,
        false, // run_all
        None,  // resume_run_id
        false, // resume_latest
        None,  // shadow_config
        &rocky_cli::commands::PartitionRunOptions::default(),
        model_name_filter,
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

// ---------------------------------------------------------------------------
// Phase-2 write path (`[state] freeze_marker_writes = true`)
// ---------------------------------------------------------------------------

/// `policy freeze` with the write flag on: one parseable marker per principal
/// (fresh UUID key, shared scope/reason), written IN ADDITION to the ledger
/// rows — the audit trail is unchanged.
#[test]
fn policy_freeze_writes_one_marker_per_principal_when_enabled() {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();
    let project = StateOnlyProject::new("freeze_marker_writes = true\n", false);

    rocky_cli::commands::run_policy_freeze(
        &project.config_path,
        &project.state_path,
        None, // principal — both
        None, // scope — any
        Some("stop everything".to_string()),
        false, // lift
        false, // json
    )
    .expect("a marker-writing freeze succeeds");

    block_on(async {
        let keys = harness
            .provider
            .list("freeze")
            .await
            .expect("list marker prefix");
        assert_eq!(keys.len(), 2, "one marker per principal, got {keys:?}");
        let mut principals = Vec::new();
        for key in &keys {
            let bytes = harness.provider.get(key).await.expect("get marker body");
            let m: FreezeMarker = serde_json::from_slice(&bytes).expect("marker body must parse");
            assert_eq!(
                format!("freeze/{}.json", m.freeze_id),
                *key,
                "the body's id must match the key stem"
            );
            assert_eq!(m.scope, "any");
            assert_eq!(
                m.reason, "stop everything",
                "--reason rides the marker body"
            );
            principals.push(m.principal);
        }
        assert!(
            principals.contains(&PolicyPrincipal::Agent)
                && principals.contains(&PolicyPrincipal::Human),
            "a both-principals freeze writes one marker per principal, got {principals:?}"
        );
    });

    // The ledger rows are still recorded — markers are IN ADDITION.
    let store = StateStore::open_read_only(&project.state_path).expect("open ledger");
    let decisions = store.list_policy_decisions().expect("list decisions");
    assert_eq!(decisions.len(), 2, "the audit-trail rows are unchanged");
    assert!(decisions.iter().all(|d| d.plan_id.starts_with("freeze:")));
}

/// Two-phase default: with `freeze_marker_writes` unset the freeze writes NO
/// marker objects — the write path is byte-identical to today's (ledger rows
/// + blob upload only).
#[test]
fn policy_freeze_writes_no_markers_by_default() {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();
    let project = StateOnlyProject::new("", false);

    rocky_cli::commands::run_policy_freeze(
        &project.config_path,
        &project.state_path,
        None,
        None,
        None,
        false,
        false,
    )
    .expect("a default-config freeze succeeds");

    block_on(async {
        assert!(
            harness
                .provider
                .list("freeze")
                .await
                .expect("list freeze prefix")
                .is_empty()
                && harness
                    .provider
                    .list("unfreeze")
                    .await
                    .expect("list unfreeze prefix")
                    .is_empty(),
            "with the flag unset no marker object may ever be written"
        );
    });
    assert!(
        harness.faults.count(FaultOp::Put) >= 1,
        "the seam-scoped ledger upload itself is unchanged"
    );
}

/// `policy unfreeze` resolves WHICH markers it lifts: the written unfreeze
/// body names exactly the active freeze ids matching `(principal, scope)`,
/// and the projection is empty afterwards.
#[test]
fn policy_unfreeze_writes_marker_referencing_lifted_ids() {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();
    let project = StateOnlyProject::new("freeze_marker_writes = true\n", false);

    rocky_cli::commands::run_policy_freeze(
        &project.config_path,
        &project.state_path,
        None,
        None,
        None,
        false,
        false,
    )
    .expect("freeze succeeds");
    let mut frozen_ids: Vec<String> = block_on(async {
        harness
            .provider
            .list("freeze")
            .await
            .expect("list freeze prefix")
    })
    .into_iter()
    .map(|k| {
        k.strip_prefix("freeze/")
            .and_then(|rest| rest.strip_suffix(".json"))
            .expect("marker key shape")
            .to_string()
    })
    .collect();
    frozen_ids.sort();
    assert_eq!(frozen_ids.len(), 2);

    rocky_cli::commands::run_policy_freeze(
        &project.config_path,
        &project.state_path,
        None,
        None,
        Some("all clear".to_string()),
        true, // lift
        false,
    )
    .expect("unfreeze succeeds");

    block_on(async {
        let keys = harness
            .provider
            .list("unfreeze")
            .await
            .expect("list unfreeze prefix");
        assert_eq!(keys.len(), 1, "one unfreeze marker, got {keys:?}");
        let bytes = harness.provider.get(&keys[0]).await.expect("get body");
        let m: UnfreezeMarker = serde_json::from_slice(&bytes).expect("unfreeze body must parse");
        let mut lifted = m.lifts.clone();
        lifted.sort();
        assert_eq!(
            lifted, frozen_ids,
            "the unfreeze must name exactly the freeze ids it lifts"
        );
        assert!(
            freeze_marker::load_active_marker_freezes(&harness.provider)
                .await
                .expect("project active set")
                .is_empty(),
            "the projection must be empty after the lift"
        );
    });
}

// ---------------------------------------------------------------------------
// Phase-1 reader semantics (writes off, enforcement on)
// ---------------------------------------------------------------------------

/// A pod whose own write flag is OFF still ENFORCES a marker another
/// (Phase-2) pod wrote: the hoisted gate LIST projects it and the gate
/// denies. Reader enforcement is unconditional — deploying the reader IS
/// Phase 1.
#[tokio::test]
async fn reader_enforces_marker_with_writes_flag_off() {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();
    // Flag deliberately UNSET; `[policy]` configured (the gate's guard).
    let project = StateOnlyProject::new("", true);

    // Another pod's Phase-2 write, simulated via the real marker write path.
    freeze_marker::write_freeze_marker(
        &harness.provider,
        &seed_marker("f-reader", PolicyPrincipal::Agent, "any"),
    )
    .await
    .expect("seed marker");

    let cfg =
        rocky_core::config::load_rocky_config(&project.config_path).expect("load project config");
    let mut touched = BTreeMap::new();
    touched.insert("orders".to_string(), PolicyCapability::Apply);

    let markers = rocky_cli::commands::marker_freezes_before_gate(&cfg, &touched)
        .await
        .expect("the gate hoist must project the marker");
    assert_eq!(markers.len(), 1);
    assert_eq!(markers[0].freeze_id, "f-reader");

    let gate = rocky_cli::commands::evaluate_apply_policy_with_policy(
        cfg.policy.as_ref(),
        "plan-reader",
        PolicyPrincipal::Agent,
        &touched,
        &project.dir.path().join("no-models"),
        &project.state_path,
        &markers,
    );
    match gate {
        PolicyGate::Deny { reason, .. } => assert!(
            reason.contains("freeze:f-reader"),
            "the deny must cite the marker; got: {reason}"
        ),
        other => panic!("a marker freeze must deny even with the write flag off, got {other:?}"),
    }
}

/// The erasure shape the marker exists for: NO ledger row anywhere (a
/// concurrent blob upload erased it, or it never synced) — the marker alone
/// still denies, citing `freeze:<id>`.
#[tokio::test]
async fn marker_only_freeze_denies_gate_with_empty_ledger() {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();
    let project = StateOnlyProject::new("", true);

    freeze_marker::write_freeze_marker(
        &harness.provider,
        &seed_marker("f-marker-only", PolicyPrincipal::Agent, "any"),
    )
    .await
    .expect("seed marker");

    // The prior ledger is genuinely EMPTY — no freeze row exists anywhere.
    {
        let store = StateStore::open(&project.state_path).expect("create empty ledger");
        assert!(store.list_policy_decisions().expect("list").is_empty());
    }

    let cfg =
        rocky_core::config::load_rocky_config(&project.config_path).expect("load project config");
    let mut touched = BTreeMap::new();
    touched.insert("orders".to_string(), PolicyCapability::Apply);
    let markers = rocky_cli::commands::marker_freezes_before_gate(&cfg, &touched)
        .await
        .expect("gate hoist");

    let gate = rocky_cli::commands::evaluate_apply_policy_with_policy(
        cfg.policy.as_ref(),
        "plan-marker-only",
        PolicyPrincipal::Agent,
        &touched,
        &project.dir.path().join("no-models"),
        &project.state_path,
        &markers,
    );
    match gate {
        PolicyGate::Deny { reason, .. } => assert!(
            reason.contains("freeze:f-marker-only"),
            "a marker-only freeze must deny with the freeze:<id> citation; got: {reason}"
        ),
        other => panic!("a marker-only freeze must deny with an empty ledger, got {other:?}"),
    }
}

/// NEGATIVE DOCUMENTATION — the reader-less pod. A gate driven with the
/// marker projection forced empty (`&[]`, the shape of a pre-reader binary
/// that never LISTs the marker prefixes) IGNORES a marker that is durably
/// present: the human base effect stays Allow. This is exactly why the
/// two-phase rollout is mandatory — readers must be deployed to every pod
/// BEFORE the first marker is written, or a reader-less pod sails past the
/// kill switch.
#[tokio::test]
async fn pre_reader_pod_ignores_marker_documented() {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();
    let project = StateOnlyProject::new("", true);

    freeze_marker::write_freeze_marker(
        &harness.provider,
        &seed_marker("f-unread", PolicyPrincipal::Human, "any"),
    )
    .await
    .expect("seed marker");

    let cfg =
        rocky_core::config::load_rocky_config(&project.config_path).expect("load project config");
    let mut touched = BTreeMap::new();
    touched.insert("orders".to_string(), PolicyCapability::Apply);

    let gate = rocky_cli::commands::evaluate_apply_policy_with_policy(
        cfg.policy.as_ref(),
        "plan-pre-reader",
        PolicyPrincipal::Human,
        &touched,
        &project.dir.path().join("no-models"),
        &project.state_path,
        &[], // a pre-PR-F pod has no reader code — no projection exists
    );
    assert_eq!(
        gate,
        PolicyGate::Allow,
        "a reader-less pod ignores the marker — the gap the mandatory readers-first \
         rollout phase closes"
    );
}

// ---------------------------------------------------------------------------
// fences: mid-run withhold + finalize (never abandon)
// ---------------------------------------------------------------------------

/// A marker active at run time withholds the transformation layer loop
/// fail-closed: the run exits nonzero with the recorded `<freeze>` failure,
/// AND the terminal path still ran — the run record rides the finalize
/// upload to the remote (partial failure, never state loss).
#[tokio::test]
async fn layer_fence_withholds_remaining_layers_and_finalizes() {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();
    let project = ModelProject::new(true);

    freeze_marker::write_freeze_marker(
        &harness.provider,
        &seed_marker("f-layer", PolicyPrincipal::Agent, "any"),
    )
    .await
    .expect("seed marker");

    let err = drive_run(
        &project.config_path,
        &project.state_path,
        Some(&project.models_dir),
        None,
        Some("run-layer-freeze"),
    )
    .await
    .expect_err("a layer-fenced run must exit nonzero");
    assert!(
        format!("{err:#}").contains("model(s) failed"),
        "the exit must be the recorded freeze failure, not an abandon; got: {err:#}"
    );

    assert!(
        harness.faults.count(FaultOp::Put) >= 1,
        "the fenced run must still FINALIZE (upload) — an abandon would strand state"
    );
    // The run record rode the finalize upload: a second pod sees the fenced
    // run as a recorded Failure.
    let _authority = harness
        .download(&harness.pod_b)
        .await
        .expect("pod B start-download");
    let store = harness.open_store(&harness.pod_b);
    let record = store
        .get_run("run-layer-freeze")
        .expect("read run record")
        .expect("the fenced run's record must ride the finalize upload");
    assert_eq!(
        record.status,
        RunStatus::Failure,
        "every layer was withheld — the run records as a Failure, not a success"
    );
}

/// The fence sees only the EXECUTING (selected) models: a `model=drop` freeze
/// does NOT withhold a `--model keep` run, because the layer loop filters the
/// fenced names by the same `model_name_filter` the build honours. Paired with
/// its positive control below: without the executing-set narrowing this run
/// would have been fenced on the unselected `drop` model.
#[tokio::test]
async fn freeze_on_unselected_model_does_not_fence_selected_run() {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();
    let project = TwoModelProject::new();

    freeze_marker::write_freeze_marker(
        &harness.provider,
        &seed_marker("f-drop", PolicyPrincipal::Agent, "model=drop"),
    )
    .await
    .expect("seed marker");

    // Selecting only `keep`: the freeze on the unselected `drop` must not fence.
    drive_run(
        &project.config_path,
        &project.state_path,
        Some(&project.models_dir),
        Some("keep"),
        Some("run-select-keep"),
    )
    .await
    .expect("a model=drop freeze must NOT fence a run that selects only keep");
}

/// Positive control on the SAME 2-model fixture: a `model=keep` freeze DOES
/// withhold a `--model keep` run. Proves the filtered fence still fires on a
/// selected match — so the negative test above narrows away a non-selected
/// match rather than never reaching the fence at all.
#[tokio::test]
async fn freeze_on_selected_model_fences_selected_run() {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();
    let project = TwoModelProject::new();

    freeze_marker::write_freeze_marker(
        &harness.provider,
        &seed_marker("f-keep", PolicyPrincipal::Agent, "model=keep"),
    )
    .await
    .expect("seed marker");

    let err = drive_run(
        &project.config_path,
        &project.state_path,
        Some(&project.models_dir),
        Some("keep"),
        Some("run-fence-keep"),
    )
    .await
    .expect_err("a model=keep freeze must fence a run that selects keep");
    assert!(
        format!("{err:#}").contains("model(s) failed"),
        "the exit must be the recorded freeze failure; got: {err:#}"
    );
}

/// The replication fan-out fence (no layer boundaries — one check bounding
/// the gate-to-copy window): a marker active at spawn time withholds every
/// copy, the run exits nonzero, and the terminal drain/persist/finalize path
/// still runs.
#[tokio::test]
async fn replication_fanout_withholds_and_finalizes() {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();
    let project = ReplicationProject::new(true).await;

    freeze_marker::write_freeze_marker(
        &harness.provider,
        &seed_marker("f-fanout", PolicyPrincipal::Agent, "any"),
    )
    .await
    .expect("seed marker");

    let err = drive_run(
        &project.config_path,
        &project.state_path,
        None,
        None,
        Some("run-fanout-freeze"),
    )
    .await
    .expect_err("a fan-out-fenced replication run must exit nonzero");
    assert!(
        format!("{err:#}").contains("table(s) failed"),
        "the exit must be the recorded withhold; got: {err:#}"
    );

    assert!(
        harness.faults.count(FaultOp::Put) >= 1,
        "the fenced replication run must still finalize (upload)"
    );
    let _authority = harness
        .download(&harness.pod_b)
        .await
        .expect("pod B start-download");
    let store = harness.open_store(&harness.pod_b);
    assert!(
        store
            .get_run("run-fanout-freeze")
            .expect("read run record")
            .is_some(),
        "the fenced run's record must ride the finalize upload"
    );
}

// ---------------------------------------------------------------------------
// fail-closed LIST at the run entry
// ---------------------------------------------------------------------------

/// A marker-LIST transport failure with a `[policy]` plane configured
/// refuses the run at entry — an error is never an empty active set — and
/// nothing is uploaded.
#[tokio::test]
async fn list_failure_refuses_governed_entry() {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();
    let project = ReplicationProject::new(true).await;

    harness.faults.arm(FaultOp::List, FaultMode::FailAll);
    let err = drive_run(&project.config_path, &project.state_path, None, None, None)
        .await
        .expect_err("a [policy]-configured run must refuse on a failed marker LIST");
    harness.faults.clear();

    assert!(
        format!("{err:#}").contains("durable freeze-marker listing failed"),
        "the refusal must cite the fail-closed marker LIST; got: {err:#}"
    );
    assert_eq!(
        harness.faults.count(FaultOp::Put),
        0,
        "a refused entry must not upload anything"
    );
}

/// Without a `[policy]` plane the run fence is INERT — it enforces no freeze
/// marker (matching the ledger / apply-gate / drift-governor no-policy floors
/// and `rocky policy freeze`'s "recorded but NOT enforced" contract), so it
/// performs NO durable-tier LIST at all. Proven here by arming a LIST fault
/// that never fires: the ungoverned run completes and finalizes normally, and
/// zero LISTs are attempted. Guards against the run path becoming the lone
/// seam that enforces a freeze without a policy plane.
#[tokio::test]
async fn no_policy_run_fence_is_inert_and_does_not_list() {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();
    let project = ReplicationProject::new(false).await;

    harness.faults.arm(FaultOp::List, FaultMode::FailAll);
    drive_run(&project.config_path, &project.state_path, None, None, None)
        .await
        .expect("an ungoverned run must not be affected by markers");
    let lists = harness.faults.count(FaultOp::List);
    let puts = harness.faults.count(FaultOp::Put);
    harness.faults.clear();

    assert_eq!(
        lists, 0,
        "an ungoverned run's inert fence must perform NO durable-tier marker LIST"
    );
    assert!(
        puts >= 1,
        "the ungoverned run must complete and finalize normally"
    );
}

// ---------------------------------------------------------------------------
// status parity: `rocky brief`
// ---------------------------------------------------------------------------

/// A marker-only freeze (no ledger row) shows in the brief's autonomy
/// section with the `freeze:<id>` citation — reporting cannot disagree with
/// enforcement.
#[test]
fn brief_merges_marker_only_freeze() {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();
    let project = StateOnlyProject::new("", false);

    block_on(freeze_marker::write_freeze_marker(
        &harness.provider,
        &seed_marker("f-brief", PolicyPrincipal::Agent, "any"),
    ))
    .expect("seed marker");
    // The brief needs an existing (empty) state store to project from.
    drop(StateStore::open(&project.state_path).expect("create state store"));

    let output = rocky_cli::commands::compute_brief(
        project.dir.path(),
        &project.state_path,
        &project.config_path,
        rocky_cli::commands::BriefSince::Hours24,
        chrono::Utc::now(),
    )
    .expect("compute brief");

    assert_eq!(
        output.autonomy.active_freezes.len(),
        1,
        "the marker-only freeze must surface in the autonomy section"
    );
    let shown = &output.autonomy.active_freezes[0];
    assert_eq!(
        shown.plan_id, "freeze:f-brief",
        "the citation slot carries the marker id"
    );
    assert_eq!(shown.principal, PolicyPrincipal::Agent);
    assert_eq!(shown.scope, "any");
}

/// An unreachable marker tier is surfaced as a section note — never a
/// silent, unqualified all-clear.
#[test]
fn brief_notes_marker_list_failure() {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();
    let project = StateOnlyProject::new("", false);
    drop(StateStore::open(&project.state_path).expect("create state store"));

    harness.faults.arm(FaultOp::List, FaultMode::FailAll);
    let output = rocky_cli::commands::compute_brief(
        project.dir.path(),
        &project.state_path,
        &project.config_path,
        rocky_cli::commands::BriefSince::Hours24,
        chrono::Utc::now(),
    )
    .expect("the brief itself must still compose");
    harness.faults.clear();

    let note = output
        .autonomy
        .note
        .as_deref()
        .expect("an unreachable marker tier must set the section note");
    assert!(
        note.contains("durable freeze markers unreachable"),
        "the note must qualify the all-clear; got: {note}"
    );
}
