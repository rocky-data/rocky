//! End-to-end reachability test for `rocky replay --at <run> --execute [--verify]`.
//!
//! Single-model re-execution reconstructs a recipe from the recorded
//! [`rocky_core::state::ProvenanceRecord`], re-runs its `SELECT` on an
//! ephemeral in-memory DuckDB engine, and (with `--verify`) compares the
//! re-derived output blake3 against the recorded hash.
//!
//! Those provenance + artifact rows are only written by the content-addressed
//! write path, which requires `s3://` storage — a plain DuckDB run emits none.
//! So, exactly as the `--check` reachability test does, this seeds a realistic
//! ledger through the *production* APIs
//! ([`rocky_core::reuse::build_records`] →
//! [`rocky_core::state::StateStore::record_reuse_spine`] +
//! [`rocky_core::state::StateStore::record_artifact`]), then spawns the real
//! `rocky` binary against it.
//!
//! The proof is deliberately non-circular. The recorded hash a `bit_exact`
//! verdict matches is **not** a hand-picked constant — it is the digest a
//! *genuine first execution through the binary* produced. `--verify` then runs
//! a **second, independent** execution and reproduces it. Three sibling
//! assertions guard against the verdict being a mere echo of the seed:
//!
//! - a nondeterministic recipe (`random()`) re-executed twice **diverges** and
//!   is flagged `nondeterministic`;
//! - a recipe seeded with a *different* recipe's recorded hash **diverges**
//!   (the verdict is a function of the re-executed bytes, not the seed);
//! - a model with a recorded content upstream is **non_replayable** — its
//!   recorded upstream bytes are never read, current data is never
//!   substituted.

use std::path::Path;
use std::process::Command;

use chrono::Utc;
use rocky_core::reuse::{OutputArtifact, UpstreamIdentity, build_records};
use rocky_core::state::{
    ArtifactRecord, ModelExecution, RunRecord, RunStatus, RunTrigger, SessionSource, StateStore,
};
use rocky_ir::types::{RockyType, TypedColumn};
use rocky_ir::{GovernanceConfig, MaterializationStrategy, ModelIr, TargetRef};

const RUN_ID: &str = "run-under-test";

// A 64-hex blake3 stand-in used only where the recorded hash is irrelevant to
// the assertion (an execute-only run, or a deliberately-mismatched seed).
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

fn content_upstream(table: &str, hash: &str) -> UpstreamIdentity {
    UpstreamIdentity::Content {
        upstream_key: format!("tgt.raw.{table}"),
        blake3_hash: hash.to_string(),
    }
}

/// Seed one model's provenance (via the production `build_records` path),
/// upserting on `(run_id, model_name)`. Records the model's own output
/// artifact so a `--check`-style input resolution passes.
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
            hostname: "replay-execute-test".to_string(),
            rocky_version: "0.0.0-test".to_string(),
            check_outcomes: Vec::new(),
        })
        .expect("record run");
}

/// Drive the real binary and return its parsed `--output json` document.
fn run_replay(state_path: &Path, model: &str, extra: &[&str]) -> serde_json::Value {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_rocky"));
    cmd.arg("--state-path")
        .arg(state_path)
        .arg("replay")
        .arg("--at")
        .arg(RUN_ID)
        .arg("--model")
        .arg(model)
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
        "rocky replay --execute exited non-zero\n--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}"
    );
    serde_json::from_str(stdout.trim())
        .unwrap_or_else(|e| panic!("stdout is not a single JSON document: {e}\n{stdout}"))
}

fn only_model(doc: &serde_json::Value) -> &serde_json::Value {
    let models = doc["models"].as_array().expect("models array");
    assert_eq!(models.len(), 1, "expected exactly one model: {doc}");
    &models[0]
}

#[test]
fn deterministic_model_replays_bit_exact() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let state_path = tmp.path().join("state.redb");
    let sql = "SELECT CAST(42 AS BIGINT) AS id";

    // 1. Seed the recipe with a placeholder hash + record the run.
    {
        let store = StateStore::open(&state_path).expect("open store");
        seed_model(&store, "m", sql, vec![int_col("id")], &[], H_PLACEHOLDER);
        record_run(&store, &["m"]);
    }

    // 2. First genuine execution through the binary — learn the real digest.
    let exec_doc = run_replay(&state_path, "m", &[]);
    eprintln!(
        "--- execute-only evidence ---\n{}",
        serde_json::to_string_pretty(&exec_doc).unwrap()
    );
    let executed = only_model(&exec_doc);
    assert_eq!(executed["verdict"], "executed");
    assert_eq!(executed["rows"], 1);
    let real_hash = executed["computed_hash"]
        .as_str()
        .expect("execute-only reports a computed hash")
        .to_string();
    assert_eq!(real_hash.len(), 64);
    assert_eq!(exec_doc["verified"], false);

    // 3. Re-seed the provenance with the REAL recorded hash (upsert).
    {
        let store = StateStore::open(&state_path).expect("reopen store");
        seed_model(&store, "m", sql, vec![int_col("id")], &[], &real_hash);
    }

    // 4. Second, independent execution — must reproduce the digest bit-exact.
    let verify_doc = run_replay(&state_path, "m", &["--verify"]);
    eprintln!(
        "--- verify evidence ---\n{}",
        serde_json::to_string_pretty(&verify_doc).unwrap()
    );
    assert_eq!(verify_doc["verified"], true);
    assert_eq!(verify_doc["bit_exact_count"], 1);
    let verified = only_model(&verify_doc);
    assert_eq!(verified["verdict"], "bit_exact");
    assert_eq!(verified["computed_hash"], real_hash);
    assert_eq!(verified["recorded_hash"], real_hash);
    assert_eq!(verified["nondeterministic"], false);
}

#[test]
fn nondeterministic_model_diverges_with_flag() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let state_path = tmp.path().join("state.redb");
    // random() over a wide range: two independent executions differ.
    let sql = "SELECT CAST(random() * 1000000000000000 AS BIGINT) AS r";

    // Seed a recorded hash from a genuine first execution.
    {
        let store = StateStore::open(&state_path).expect("open store");
        seed_model(&store, "nd", sql, vec![int_col("r")], &[], H_PLACEHOLDER);
        record_run(&store, &["nd"]);
    }
    let first = run_replay(&state_path, "nd", &[]);
    let first_hash = only_model(&first)["computed_hash"]
        .as_str()
        .expect("computed hash")
        .to_string();
    {
        let store = StateStore::open(&state_path).expect("reopen store");
        seed_model(&store, "nd", sql, vec![int_col("r")], &[], &first_hash);
    }

    // Second execution re-rolls random() → diverges; the flag makes it expected.
    let doc = run_replay(&state_path, "nd", &["--verify"]);
    eprintln!(
        "--- nondeterministic evidence ---\n{}",
        serde_json::to_string_pretty(&doc).unwrap()
    );
    assert_eq!(doc["bit_exact_count"], 0);
    let m = only_model(&doc);
    assert_eq!(m["verdict"], "diverged");
    assert_eq!(m["nondeterministic"], true);
    assert_ne!(m["computed_hash"], serde_json::Value::String(first_hash));
    assert!(
        m["reasons"]
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r.as_str().unwrap().contains("expected")),
        "a nondeterministic divergence must be annotated as expected: {m}"
    );
}

#[test]
fn mutated_recipe_diverges_from_seeded_hash() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let state_path = tmp.path().join("state.redb");

    // Learn recipe-1's real digest from a throwaway store.
    let hash_one = {
        let scratch = tempfile::tempdir().expect("scratch");
        let scratch_path = scratch.path().join("state.redb");
        {
            let store = StateStore::open(&scratch_path).expect("open scratch");
            seed_model(
                &store,
                "m",
                "SELECT CAST(1 AS BIGINT) AS id",
                vec![int_col("id")],
                &[],
                H_PLACEHOLDER,
            );
            record_run(&store, &["m"]);
        }
        run_replay(&scratch_path, "m", &[])["models"][0]["computed_hash"]
            .as_str()
            .expect("computed hash")
            .to_string()
    };

    // Seed recipe-2 AS the recording, but with recipe-1's recorded hash.
    {
        let store = StateStore::open(&state_path).expect("open store");
        seed_model(
            &store,
            "m",
            "SELECT CAST(2 AS BIGINT) AS id",
            vec![int_col("id")],
            &[],
            &hash_one,
        );
        record_run(&store, &["m"]);
    }

    let doc = run_replay(&state_path, "m", &["--verify"]);
    eprintln!(
        "--- mutated-recipe evidence ---\n{}",
        serde_json::to_string_pretty(&doc).unwrap()
    );
    // Recipe-2's output must NOT match recipe-1's recorded hash — proving the
    // verdict is a function of the re-executed bytes, not the seed.
    let m = only_model(&doc);
    assert_eq!(m["verdict"], "diverged");
    assert_eq!(m["recorded_hash"], hash_one);
    assert_ne!(m["computed_hash"], serde_json::Value::String(hash_one));
    assert_eq!(m["nondeterministic"], false);
    assert_eq!(doc["bit_exact_count"], 0);
}

#[test]
fn content_upstream_model_is_non_replayable() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let state_path = tmp.path().join("state.redb");
    {
        let store = StateStore::open(&state_path).expect("open store");
        // A leaf that reads a recorded upstream — its bytes live at an s3 path
        // the creds-free execute path never reads.
        seed_model(
            &store,
            "leaf",
            "SELECT id FROM tgt.raw.root",
            vec![int_col("id")],
            &[content_upstream("root", H_PLACEHOLDER)],
            H_PLACEHOLDER,
        );
        record_run(&store, &["leaf"]);
    }

    let doc = run_replay(&state_path, "leaf", &["--verify"]);
    eprintln!(
        "--- content-upstream evidence ---\n{}",
        serde_json::to_string_pretty(&doc).unwrap()
    );
    let m = only_model(&doc);
    assert_eq!(m["verdict"], "non_replayable");
    assert!(m["computed_hash"].is_null());
    assert!(
        m["reasons"]
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r.as_str().unwrap().contains("self-contained recipe")),
        "expected a self-contained-recipe reason: {m}"
    );
}
