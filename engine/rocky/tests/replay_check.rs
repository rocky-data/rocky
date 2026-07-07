//! End-to-end reachability test for `rocky replay --at <run> --check`.
//!
//! The read-only replayability audit consumes the state store's provenance +
//! artifact ledger. Those rows are only written by the content-addressed
//! write path, which requires `s3://` storage — a plain DuckDB run emits
//! none. So rather than fake the reader's input by hand-writing JSON (which
//! would only prove the reader reads what the test wrote), this test seeds a
//! realistic ledger through the *production* APIs —
//! [`rocky_core::reuse::build_records`] →
//! [`rocky_core::state::StateStore::record_reuse_spine`] +
//! [`rocky_core::state::StateStore::record_artifact`] — then spawns the real
//! `rocky` binary against it. The classification path is therefore exercised
//! exactly as it would be over a genuine content-addressed run.
//!
//! Two runs are seeded into two stores:
//!
//! - **clean** — every model's inputs resolve; the audit reports every model
//!   replayable, with the `now()` model additionally flagged
//!   `nondeterministic` (a flag, not a demotion).
//! - **mutated** — one upstream's artifact row is withheld (the honest analog
//!   of a source whose recorded bytes are no longer in the ledger); its
//!   consumer flips to `non_replayable`, and a provenance-less model reports
//!   `non_replayable` with a "no provenance" reason.

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

// 64-hex blake3 stand-ins for each model's output bytes.
const H_RAW: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const H_STG: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const H_FCT: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
const H_ND: &str = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";

fn governance() -> GovernanceConfig {
    GovernanceConfig {
        permissions_file: None,
        auto_create_catalogs: false,
        auto_create_schemas: false,
    }
}

/// A content-addressed transformation model — the only shape that carries a
/// provenance record in the real engine. Typed columns are required for a
/// content-addressed model to have a `skip_hash` (and thus a provenance
/// record), matching the real runtime.
fn ca_model(table: &str, sql: &str) -> ModelIr {
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
    ir.typed_columns = vec![TypedColumn {
        name: "order_id".into(),
        data_type: RockyType::Int64,
        nullable: false,
    }];
    ir
}

fn content_upstream(table: &str, hash: &str) -> UpstreamIdentity {
    UpstreamIdentity::Content {
        upstream_key: format!("tgt.raw.{table}"),
        blake3_hash: hash.to_string(),
    }
}

/// Seed one model: build its provenance record via the production
/// `build_records` path, record it, and (optionally) record its output
/// artifact so downstream consumers can resolve its hash.
fn seed_model(
    store: &StateStore,
    table: &str,
    sql: &str,
    upstreams: &[UpstreamIdentity],
    output_hash: &str,
    record_output_artifact: bool,
) {
    let ir = ca_model(table, sql);
    let outputs = vec![OutputArtifact {
        blake3_hash: output_hash.to_string(),
        file_path: format!("s3://bucket/tgt/raw/{table}/{output_hash}.parquet"),
    }];
    let (entry, prov) = build_records(&ir, RUN_ID, upstreams, &outputs, Utc::now())
        .expect("content-addressed model has a skip_hash, so build_records yields a record");
    store
        .record_reuse_spine(std::slice::from_ref(&entry), std::slice::from_ref(&prov))
        .expect("record reuse spine");
    if record_output_artifact {
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
            hostname: "replay-check-test".to_string(),
            rocky_version: "0.0.0-test".to_string(),
        })
        .expect("record run");
}

/// Drive the real binary and return its parsed `--output json` document.
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
    let stderr = String::from_utf8(out.stderr).expect("utf8 stderr");
    assert!(
        out.status.success(),
        "rocky replay --check exited non-zero\n--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}"
    );
    serde_json::from_str(stdout.trim()).unwrap_or_else(|e| {
        panic!("stdout is not a single JSON document: {e}\n{stdout}");
    })
}

fn model<'a>(doc: &'a serde_json::Value, name: &str) -> &'a serde_json::Value {
    doc["models"]
        .as_array()
        .expect("models array")
        .iter()
        .find(|m| m["model_name"] == name)
        .unwrap_or_else(|| panic!("model {name} not in output: {doc}"))
}

#[test]
fn clean_run_reports_every_model_replayable() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let state_path = tmp.path().join("state.redb");
    {
        let store = StateStore::open(&state_path).expect("open store");
        // raw_orders reads nothing (vacuously input-resolvable).
        seed_model(
            &store,
            "raw_orders",
            "SELECT i AS order_id, i % 5 AS customer_id FROM generate_series(1, 100) AS t(i)",
            &[],
            H_RAW,
            true,
        );
        // stg_orders reads raw_orders (content upstream, artifact present).
        seed_model(
            &store,
            "stg_orders",
            "SELECT order_id, customer_id FROM tgt.raw.raw_orders WHERE order_id > 0",
            &[content_upstream("raw_orders", H_RAW)],
            H_STG,
            true,
        );
        // fct_orders reads stg_orders (content upstream, artifact present).
        seed_model(
            &store,
            "fct_orders",
            "SELECT customer_id, COUNT(*) AS n FROM tgt.raw.stg_orders GROUP BY customer_id",
            &[content_upstream("stg_orders", H_STG)],
            H_FCT,
            true,
        );
        // events_nd is replayable but nondeterministic: now() is volatile.
        seed_model(
            &store,
            "events_nd",
            "SELECT order_id, now() AS loaded_at FROM tgt.raw.raw_orders",
            &[content_upstream("raw_orders", H_RAW)],
            H_ND,
            true,
        );
        record_run(
            &store,
            &["raw_orders", "stg_orders", "fct_orders", "events_nd"],
        );
    }

    let doc = run_check(&state_path);
    eprintln!(
        "--- clean run evidence ---\n{}",
        serde_json::to_string_pretty(&doc).unwrap()
    );

    assert_eq!(doc["replayable"], serde_json::Value::Bool(true));
    assert_eq!(doc["model_count"], 4);
    assert_eq!(doc["replayable_count"], 4);
    for name in ["raw_orders", "stg_orders", "fct_orders", "events_nd"] {
        assert_eq!(model(&doc, name)["verdict"], "replayable", "{name}");
    }
    // Nondeterminism is a flag, not a demotion.
    assert_eq!(model(&doc, "events_nd")["nondeterministic"], true);
    assert_eq!(model(&doc, "fct_orders")["nondeterministic"], false);
}

#[test]
fn mutated_run_flags_unresolvable_consumer_and_missing_provenance() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let state_path = tmp.path().join("state.redb");
    {
        let store = StateStore::open(&state_path).expect("open store");
        seed_model(&store, "raw_orders", "SELECT 1 AS x", &[], H_RAW, true);
        // stg_orders resolves (raw_orders artifact present)...
        seed_model(
            &store,
            "stg_orders",
            "SELECT order_id FROM tgt.raw.raw_orders",
            &[content_upstream("raw_orders", H_RAW)],
            H_STG,
            false, // ...but its OWN output artifact is withheld.
        );
        // ...so fct_orders (which reads stg_orders' bytes) cannot resolve.
        seed_model(
            &store,
            "fct_orders",
            "SELECT * FROM tgt.raw.stg_orders",
            &[content_upstream("stg_orders", H_STG)],
            H_FCT,
            true,
        );
        // legacy_model ran but has no provenance record at all.
        record_run(
            &store,
            &["raw_orders", "stg_orders", "fct_orders", "legacy_model"],
        );
    }

    let doc = run_check(&state_path);
    eprintln!(
        "--- mutated run evidence ---\n{}",
        serde_json::to_string_pretty(&doc).unwrap()
    );

    assert_eq!(doc["replayable"], serde_json::Value::Bool(false));
    assert_eq!(model(&doc, "raw_orders")["verdict"], "replayable");
    assert_eq!(model(&doc, "stg_orders")["verdict"], "replayable");

    // Consumer of the withheld bytes is honestly non-replayable, with a reason.
    let fct = model(&doc, "fct_orders");
    assert_eq!(fct["verdict"], "non_replayable");
    let fct_reasons = fct["reasons"].as_array().unwrap();
    assert!(
        fct_reasons.iter().any(|r| r
            .as_str()
            .unwrap()
            .contains("absent from the artifact ledger")),
        "expected a ledger-absence reason, got {fct_reasons:?}"
    );

    // Provenance-less model reports the honest "no provenance" reason.
    let legacy = model(&doc, "legacy_model");
    assert_eq!(legacy["verdict"], "non_replayable");
    assert_eq!(legacy["has_provenance"], false);
    assert!(
        legacy["reasons"][0]
            .as_str()
            .unwrap()
            .contains("no provenance recorded"),
        "got {:?}",
        legacy["reasons"]
    );
}
