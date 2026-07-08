//! End-to-end reachability test for DAG-order `rocky replay --at <run>
//! --execute [--verify]` (no `--model` filter — the whole recorded run).
//!
//! Multi-model replay reconstructs every model from its recorded
//! [`rocky_core::state::ProvenanceRecord`], executes them in topological order
//! on a **single shared** in-memory DuckDB engine, and materialises each
//! upstream's *replayed* output so a downstream model's `SELECT` reads the
//! replayed bytes — never the recorded object-store bytes, never production.
//! `--verify` compares every node's re-derived blake3 to its recorded hash.
//!
//! Those provenance + artifact rows are only written by the content-addressed
//! write path, which requires `s3://` storage — a plain DuckDB run emits none.
//! So, exactly as the single-model reachability test does, this seeds a
//! realistic multi-model ledger through the *production* APIs
//! ([`rocky_core::reuse::build_records`] →
//! [`rocky_core::state::StateStore::record_reuse_spine`] +
//! [`rocky_core::state::StateStore::record_artifact`]), then spawns the real
//! `rocky` binary against it.
//!
//! The proof is deliberately non-circular:
//!
//! - **Bit-exact whole-DAG replay** ([`dag_replays_whole_run_bit_exact`]): the
//!   recorded hashes a `bit_exact` verdict matches are **not** hand-picked —
//!   they are the digests a *genuine first DAG execution through the binary*
//!   produced (the leaf's digest itself produced by reading the replayed
//!   root). A second, independent DAG execution reproduces both.
//! - **The load-bearing causal proof**
//!   ([`downstream_verdict_tracks_the_replayed_upstream`]): holding the leaf's
//!   recipe *and* its recorded hash fixed, changing only the **upstream's
//!   recipe** flips the leaf from `bit_exact` to `diverged`. The leaf therefore
//!   consumed the *replayed* upstream output, not the original — if it had read
//!   the recorded/original upstream it would have stayed `bit_exact`.
//! - **The honesty boundary** ([`upstream_not_produced_in_run_is_non_replayable`]
//!   and [`watermark_upstream_is_non_replayable`]): a model whose upstream is
//!   not produced by any node in *this* run — its bytes live on object storage
//!   the creds-free replay never reads — or is a mutable-source watermark, is
//!   reported `non_replayable` rather than silently substituted.

use std::path::Path;
use std::process::Command;

use chrono::Utc;
use rocky_core::reuse::{OutputArtifact, UpstreamIdentity, build_records};
use rocky_core::state::{
    ArtifactRecord, ModelExecution, RunRecord, RunStatus, RunTrigger, SessionSource, StateStore,
};
use rocky_ir::types::{RockyType, TypedColumn};
use rocky_ir::{GovernanceConfig, MaterializationStrategy, ModelIr, TargetRef};

const RUN_ID: &str = "dag-run-under-test";
const H_PLACEHOLDER: &str = "0000000000000000000000000000000000000000000000000000000000000000";

fn governance() -> GovernanceConfig {
    GovernanceConfig {
        permissions_file: None,
        auto_create_catalogs: false,
        auto_create_schemas: false,
    }
}

/// A content-addressed transformation model — the only shape that carries a
/// provenance record in the real engine.
fn ca_model(table: &str, sql: &str, typed_columns: Vec<TypedColumn>) -> ModelIr {
    let mut ir = ModelIr::transformation(
        TargetRef {
            catalog: "tgt".into(),
            schema: "raw".into(),
            table: table.into(),
        },
        MaterializationStrategy::ContentAddressed {
            storage_prefix: format!("s3://bucket/tgt/raw/{table}"),
            partition_columns: vec![],
        },
        vec![],
        sql.to_string(),
        governance(),
        None,
        None,
    );
    ir.typed_columns = typed_columns;
    ir
}

fn int_col(name: &str) -> TypedColumn {
    TypedColumn {
        name: name.into(),
        data_type: RockyType::Int64,
        nullable: false,
    }
}

/// A content upstream keyed by the producer's `catalog.schema.table` FQN — the
/// same string [`ca_model`]'s `TargetRef` yields, so the replay DAG joins the
/// edge on it.
fn content_upstream(table: &str, hash: &str) -> UpstreamIdentity {
    UpstreamIdentity::Content {
        upstream_key: format!("tgt.raw.{table}"),
        blake3_hash: hash.to_string(),
    }
}

fn watermark_upstream(table: &str) -> UpstreamIdentity {
    UpstreamIdentity::Watermark {
        upstream_key: format!("tgt.raw.{table}"),
        max_ts: Some("2026-01-01T00:00:00Z".into()),
        row_count: Some(1),
    }
}

/// Seed one model's provenance (via the production `build_records` path),
/// upserting on `(run_id, model_name)`, and record its own output artifact.
fn seed_model(
    store: &StateStore,
    table: &str,
    sql: &str,
    typed_columns: Vec<TypedColumn>,
    upstreams: &[UpstreamIdentity],
    output_hash: &str,
) {
    let ir = ca_model(table, sql, typed_columns);
    let outputs = vec![OutputArtifact {
        blake3_hash: output_hash.to_string(),
        file_path: format!("s3://bucket/tgt/raw/{table}/{output_hash}.parquet"),
    }];
    let (entry, prov) = build_records(&ir, RUN_ID, upstreams, &outputs, Utc::now())
        .expect("content-addressed model has a skip_hash, so build_records yields a record");
    store
        .record_reuse_spine(std::slice::from_ref(&entry), std::slice::from_ref(&prov))
        .expect("record reuse spine");
    store
        .record_artifact(&ArtifactRecord {
            blake3_hash: output_hash.to_string(),
            run_id: RUN_ID.to_string(),
            model_name: table.to_string(),
            file_path: format!("s3://bucket/tgt/raw/{table}/{output_hash}.parquet"),
            commit_version: 0,
            size_bytes: 1024,
            written_at: Utc::now(),
        })
        .expect("record artifact");
}

fn model_exec(name: &str) -> ModelExecution {
    let now = Utc::now();
    ModelExecution {
        model_name: name.to_string(),
        started_at: now,
        finished_at: now,
        duration_ms: 1,
        rows_affected: Some(1),
        status: "success".to_string(),
        sql_hash: format!("sql_{name}"),
        skip_hash: None,
        upstream_freshness: None,
        bytes_scanned: None,
        bytes_written: None,
        tenant: None,
        recipe_hash: None,
        input_hash: None,
        input_proof_class: None,
        env_hash: None,
        hash_scheme: None,
        output_column_hashes: None,
        attempts: Vec::new(),
    }
}

/// Record the run with `models` in the given order (the replay derives its own
/// topological order from the provenance edges, so this order is not load-
/// bearing — recording upstream-before-downstream is not required).
fn record_run(store: &StateStore, models: &[&str]) {
    let now = Utc::now();
    store
        .record_run(&RunRecord {
            run_id: RUN_ID.to_string(),
            started_at: now,
            finished_at: now,
            status: RunStatus::Success,
            models_executed: models.iter().map(|m| model_exec(m)).collect(),
            trigger: RunTrigger::Manual,
            config_hash: "cfg".to_string(),
            triggering_identity: None,
            session_source: SessionSource::Cli,
            git_commit: None,
            git_branch: None,
            idempotency_key: None,
            target_catalog: None,
            hostname: "replay-dag-test".to_string(),
            rocky_version: "0.0.0-test".to_string(),
        })
        .expect("record run");
}

/// Drive the real binary with NO `--model` (whole-DAG replay) and return the
/// parsed `--output json` document.
fn run_dag(state_path: &Path, extra: &[&str]) -> serde_json::Value {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_rocky"));
    cmd.arg("--state-path")
        .arg(state_path)
        .arg("replay")
        .arg("--at")
        .arg(RUN_ID)
        .arg("--execute");
    for a in extra {
        cmd.arg(a);
    }
    let out = cmd
        .arg("--output")
        .arg("json")
        .env("RUST_LOG", "error")
        .output()
        .expect("spawn rocky");
    let stdout = String::from_utf8(out.stdout).expect("utf8 stdout");
    let stderr = String::from_utf8(out.stderr).expect("utf8 stderr");
    assert!(
        out.status.success(),
        "rocky replay --execute (DAG) exited non-zero\n--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}"
    );
    serde_json::from_str(stdout.trim())
        .unwrap_or_else(|e| panic!("stdout is not a single JSON document: {e}\n{stdout}"))
}

/// Drive the read-only `rocky replay --at <run> --check` audit and return the
/// parsed `--output json` document.
fn run_check(state_path: &Path) -> serde_json::Value {
    let out = Command::new(env!("CARGO_BIN_EXE_rocky"))
        .arg("--state-path")
        .arg(state_path)
        .arg("replay")
        .arg("--at")
        .arg(RUN_ID)
        .arg("--check")
        .arg("--output")
        .arg("json")
        .env("RUST_LOG", "error")
        .output()
        .expect("spawn rocky");
    let stdout = String::from_utf8(out.stdout).expect("utf8 stdout");
    assert!(
        out.status.success(),
        "rocky replay --check exited non-zero: {stdout}"
    );
    serde_json::from_str(stdout.trim()).expect("check json")
}

/// Find a model verdict object by name.
fn model<'a>(doc: &'a serde_json::Value, name: &str) -> &'a serde_json::Value {
    doc["models"]
        .as_array()
        .expect("models array")
        .iter()
        .find(|m| m["model_name"] == name)
        .unwrap_or_else(|| panic!("model {name:?} not in output: {doc}"))
}

// root = `SELECT 1 AS id`; leaf = `SELECT id * 10 AS v FROM tgt.raw.root`.
// Deterministic single-row outputs keep the blake3 comparison order-stable.
const ROOT_SQL_ONE: &str = "SELECT CAST(1 AS BIGINT) AS id";
const ROOT_SQL_TWO: &str = "SELECT CAST(2 AS BIGINT) AS id";
const LEAF_SQL: &str = "SELECT id * 10 AS v FROM tgt.raw.root";

/// Seed a fresh root→leaf DAG with the given root recipe + recorded hashes.
fn seed_dag(store: &StateStore, root_sql: &str, root_hash: &str, leaf_hash: &str) {
    seed_model(store, "root", root_sql, vec![int_col("id")], &[], root_hash);
    seed_model(
        store,
        "leaf",
        LEAF_SQL,
        vec![int_col("v")],
        &[content_upstream("root", root_hash)],
        leaf_hash,
    );
    record_run(store, &["root", "leaf"]);
}

/// Learn the genuine root + leaf digests from one real execute-only DAG run
/// (the leaf's digest is itself produced by reading the *replayed* root).
fn learn_digests(root_sql: &str) -> (String, String) {
    let scratch = tempfile::tempdir().expect("scratch");
    let path = scratch.path().join("state.redb");
    {
        let store = StateStore::open(&path).expect("open scratch");
        seed_dag(&store, root_sql, H_PLACEHOLDER, H_PLACEHOLDER);
    }
    let doc = run_dag(&path, &[]);
    let root = model(&doc, "root");
    let leaf = model(&doc, "leaf");
    assert_eq!(root["verdict"], "executed", "root execute-only: {doc}");
    assert_eq!(
        leaf["verdict"], "executed",
        "leaf must execute against the replayed root (not be non_replayable): {doc}"
    );
    assert_eq!(leaf["rows"], 1);
    (
        root["computed_hash"]
            .as_str()
            .expect("root hash")
            .to_string(),
        leaf["computed_hash"]
            .as_str()
            .expect("leaf hash")
            .to_string(),
    )
}

#[test]
fn dag_replays_whole_run_bit_exact() {
    // Learn the real digests from a genuine first DAG execution.
    let (root_hash, leaf_hash) = learn_digests(ROOT_SQL_ONE);
    assert_eq!(root_hash.len(), 64);
    assert_eq!(leaf_hash.len(), 64);
    assert_ne!(root_hash, leaf_hash);

    // Re-seed both provenance records with the REAL recorded hashes.
    let tmp = tempfile::tempdir().expect("tempdir");
    let state_path = tmp.path().join("state.redb");
    {
        let store = StateStore::open(&state_path).expect("open store");
        seed_dag(&store, ROOT_SQL_ONE, &root_hash, &leaf_hash);
    }

    // A second, independent DAG execution must reproduce BOTH digits bit-exact.
    // The leaf can only be bit_exact by reading the freshly-replayed root: the
    // recorded object-store bytes are never fetched by this creds-free path.
    let doc = run_dag(&state_path, &["--verify"]);
    eprintln!(
        "--- whole-DAG bit-exact evidence ---\n{}",
        serde_json::to_string_pretty(&doc).unwrap()
    );
    assert_eq!(doc["verified"], true);
    assert_eq!(doc["model_count"], 2);
    assert_eq!(doc["bit_exact_count"], 2, "both nodes bit-exact: {doc}");

    let root = model(&doc, "root");
    assert_eq!(root["verdict"], "bit_exact");
    assert_eq!(root["recorded_hash"], root_hash);

    let leaf = model(&doc, "leaf");
    assert_eq!(leaf["verdict"], "bit_exact");
    assert_eq!(leaf["recorded_hash"], leaf_hash);
    assert_eq!(leaf["computed_hash"], leaf_hash);
    assert_eq!(leaf["nondeterministic"], false);

    // The P2 done-criterion: on this all-in-run DAG the read-only `--check`
    // audit and `--execute` agree. `--check` calls every model replayable, and
    // `--execute --verify` reproduced every one bit-exact. (Off the all-in-run
    // path the two deliberately diverge: `--check` attests "the bytes exist in
    // the ledger", DAG `--execute` requires "producible by an in-run node" and
    // is intentionally the stricter question.)
    let check = run_check(&state_path);
    eprintln!(
        "--- --check alignment evidence ---\n{}",
        serde_json::to_string_pretty(&check).unwrap()
    );
    assert_eq!(check["replayable"], true);
    assert_eq!(check["model_count"], 2);
    assert_eq!(
        check["replayable_count"], 2,
        "--check must predict both models replayable on the all-in-run DAG: {check}"
    );
}

#[test]
fn recording_order_is_not_load_bearing() {
    // The replay derives its own topological order from the provenance edges,
    // so a run recorded downstream-before-upstream still replays correctly.
    let (root_hash, leaf_hash) = learn_digests(ROOT_SQL_ONE);
    let tmp = tempfile::tempdir().expect("tempdir");
    let state_path = tmp.path().join("state.redb");
    {
        let store = StateStore::open(&state_path).expect("open store");
        seed_model(
            &store,
            "root",
            ROOT_SQL_ONE,
            vec![int_col("id")],
            &[],
            &root_hash,
        );
        seed_model(
            &store,
            "leaf",
            LEAF_SQL,
            vec![int_col("v")],
            &[content_upstream("root", &root_hash)],
            &leaf_hash,
        );
        // Deliberately reversed: leaf (downstream) recorded before root.
        record_run(&store, &["leaf", "root"]);
    }
    let doc = run_dag(&state_path, &["--verify"]);
    eprintln!(
        "--- reversed-record-order evidence ---\n{}",
        serde_json::to_string_pretty(&doc).unwrap()
    );
    assert_eq!(
        doc["bit_exact_count"], 2,
        "topo order is derived from edges, not the recorded model order: {doc}"
    );
    assert_eq!(model(&doc, "root")["verdict"], "bit_exact");
    assert_eq!(model(&doc, "leaf")["verdict"], "bit_exact");
}

#[test]
fn downstream_verdict_tracks_the_replayed_upstream() {
    // Learn the leaf's digest for the root=1 world (leaf reads replayed id=1 →
    // v=10). This exact leaf recording is held fixed across both phases below.
    let (root1_hash, leaf10_hash) = learn_digests(ROOT_SQL_ONE);

    let tmp = tempfile::tempdir().expect("tempdir");
    let state_path = tmp.path().join("state.redb");

    // -- Phase 1 (control): root recipe = `1`. Leaf reads replayed root=1 →
    // reproduces its recorded hash → bit_exact. --
    {
        let store = StateStore::open(&state_path).expect("open store");
        seed_dag(&store, ROOT_SQL_ONE, &root1_hash, &leaf10_hash);
    }
    let control = run_dag(&state_path, &["--verify"]);
    eprintln!(
        "--- control (root=1) evidence ---\n{}",
        serde_json::to_string_pretty(&control).unwrap()
    );
    assert_eq!(
        model(&control, "leaf")["verdict"],
        "bit_exact",
        "control: leaf reproduces its recorded hash off the replayed root=1: {control}"
    );

    // -- Phase 2 (perturbation): change ONLY the root's recipe to `2`. The
    // leaf's recipe AND its recorded hash are byte-for-byte identical to the
    // control. If the leaf read the recorded/original upstream it would stay
    // bit_exact; because it reads the *replayed* root (now id=2 → v=20 ≠ v=10)
    // it must DIVERGE. --
    {
        let store = StateStore::open(&state_path).expect("reopen store");
        // Re-seed root with the changed recipe. Root's own recorded hash is
        // left as root1_hash (root will itself diverge — irrelevant to the
        // claim). Leaf is re-seeded UNCHANGED: same recipe, same leaf10_hash.
        seed_dag(&store, ROOT_SQL_TWO, &root1_hash, &leaf10_hash);
    }
    let perturbed = run_dag(&state_path, &["--verify"]);
    eprintln!(
        "--- perturbation (root=2) evidence ---\n{}",
        serde_json::to_string_pretty(&perturbed).unwrap()
    );
    let leaf = model(&perturbed, "leaf");
    assert_eq!(
        leaf["verdict"], "diverged",
        "leaf must diverge once the replayed upstream changes — proving it consumed the \
         replayed root, not the original: {perturbed}"
    );
    assert_ne!(
        leaf["computed_hash"],
        serde_json::Value::String(leaf10_hash.clone()),
        "leaf's re-derived digest must reflect the changed replayed upstream"
    );
    assert_eq!(leaf["recorded_hash"], leaf10_hash);
    // The leaf's recipe is deterministic — its divergence is a genuine
    // recipe/input-sensitivity signal, not a nondeterminism artefact.
    assert_eq!(leaf["nondeterministic"], false);
}

#[test]
fn upstream_not_produced_in_run_is_non_replayable() {
    // A leaf whose only upstream (`root`) is NOT one of this run's models: its
    // recorded bytes live on object storage the creds-free replay never reads.
    // The DAG replay must report it non_replayable — never substitute data.
    let tmp = tempfile::tempdir().expect("tempdir");
    let state_path = tmp.path().join("state.redb");
    {
        let store = StateStore::open(&state_path).expect("open store");
        seed_model(
            &store,
            "leaf",
            LEAF_SQL,
            vec![int_col("v")],
            &[content_upstream("root", H_PLACEHOLDER)],
            H_PLACEHOLDER,
        );
        // Only `leaf` is in the run; `root` is not produced here.
        record_run(&store, &["leaf"]);
    }
    let doc = run_dag(&state_path, &["--verify"]);
    eprintln!(
        "--- external-upstream evidence ---\n{}",
        serde_json::to_string_pretty(&doc).unwrap()
    );
    let leaf = model(&doc, "leaf");
    assert_eq!(leaf["verdict"], "non_replayable");
    assert!(leaf["computed_hash"].is_null());
    assert_eq!(doc["bit_exact_count"], 0);
    assert!(
        leaf["reasons"]
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r.as_str().unwrap().contains("not produced by any model")),
        "expected a not-produced-in-run reason: {leaf}"
    );
}

#[test]
fn watermark_upstream_is_non_replayable() {
    // A model reading a mutable source via a freshness watermark is never
    // replayable — the source is not content-pinned.
    let tmp = tempfile::tempdir().expect("tempdir");
    let state_path = tmp.path().join("state.redb");
    {
        let store = StateStore::open(&state_path).expect("open store");
        seed_model(
            &store,
            "leaf",
            LEAF_SQL,
            vec![int_col("v")],
            &[watermark_upstream("root")],
            H_PLACEHOLDER,
        );
        record_run(&store, &["leaf"]);
    }
    let doc = run_dag(&state_path, &["--verify"]);
    eprintln!(
        "--- watermark-upstream evidence ---\n{}",
        serde_json::to_string_pretty(&doc).unwrap()
    );
    let leaf = model(&doc, "leaf");
    assert_eq!(leaf["verdict"], "non_replayable");
    assert!(
        leaf["reasons"]
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r.as_str().unwrap().contains("watermark")),
        "expected a watermark reason: {leaf}"
    );
}
