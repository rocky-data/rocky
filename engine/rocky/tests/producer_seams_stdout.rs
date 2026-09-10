//! `rocky trace`, `rocky cost` and `rocky compliance` print, byte for byte,
//! what their producer seam returns.
//!
//! The seams (`compute_trace`, `compute_cost`, `compute_compliance`) exist so
//! a server route can serve the same document the CLI prints. These tests
//! spawn the real binary and compare its stdout against the seam's own
//! serialisation, byte for byte. What they pin is the equality: a `run_*`
//! that serialises differently, or a seam whose document drifts from what
//! the CLI prints, fails here. They cannot see whether `run_*` calls the
//! seam or reproduces its bytes another way; only the bytes are the
//! contract a route needs.

use std::path::Path;
use std::process::Command;

use chrono::{TimeZone, Utc};
use rocky_cli::commands::{CostGroupBy, compute_compliance, compute_cost, compute_trace};
use rocky_core::state::{
    ModelExecution, RunRecord, RunStatus, RunTrigger, SessionSource, StateStore,
};

const RUN_ID: &str = "run-under-test";

/// What `print_json` / `println!("{}", to_string_pretty(..))` emit.
macro_rules! reference_bytes {
    ($output:expr) => {
        serde_json::to_string_pretty(&$output).unwrap() + "\n"
    };
}

fn exec(name: &str, start: chrono::DateTime<Utc>, dur_ms: i64) -> ModelExecution {
    ModelExecution {
        model_name: name.to_string(),
        started_at: start,
        finished_at: start + chrono::Duration::milliseconds(dur_ms),
        duration_ms: dur_ms as u64,
        rows_affected: Some(42),
        status: "success".to_string(),
        sql_hash: format!("hash_{name}"),
        skip_hash: None,
        upstream_freshness: None,
        bytes_scanned: Some(1024),
        bytes_written: Some(2048),
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

/// One recorded run with two sequential models, written through the
/// production ledger API so the binary reads what a real run would leave.
fn seed_state(state_path: &Path) {
    let start = Utc.with_ymd_and_hms(2026, 4, 21, 12, 0, 0).unwrap();
    let models = vec![
        exec("a", start, 1_000),
        exec("b", start + chrono::Duration::milliseconds(1_000), 500),
    ];
    let run_end = models.iter().map(|m| m.finished_at).max().unwrap();
    let store = StateStore::open(state_path).unwrap();
    store
        .record_run(&RunRecord {
            run_id: RUN_ID.to_string(),
            started_at: start,
            finished_at: run_end,
            status: RunStatus::Success,
            models_executed: models,
            trigger: RunTrigger::Manual,
            config_hash: "cfghash".to_string(),
            triggering_identity: None,
            session_source: SessionSource::Cli,
            git_commit: None,
            git_branch: None,
            idempotency_key: None,
            target_catalog: None,
            hostname: "seams-test-host".to_string(),
            rocky_version: "0.0.0-test".to_string(),
            check_outcomes: Vec::new(),
            pipeline: None,
            submission_id: None,
            check_gate_failed: false,
            verify_after_failed: false,
        })
        .unwrap();
}

/// Run the binary from `cwd` and return its stdout, asserting a clean exit.
fn rocky_stdout(cwd: &Path, args: &[&str]) -> String {
    let out = Command::new(env!("CARGO_BIN_EXE_rocky"))
        .current_dir(cwd)
        .args(args)
        .output()
        .expect("spawn rocky");
    assert!(
        out.status.success(),
        "rocky {:?} exited {:?}\nstderr:\n{}",
        args,
        out.status.code(),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).expect("utf-8 stdout")
}

#[test]
fn trace_prints_what_compute_trace_returns() {
    let dir = tempfile::tempdir().unwrap();
    let state_path = dir.path().join("state.redb");
    seed_state(&state_path);
    let state = state_path.to_str().unwrap();

    let whole = rocky_stdout(
        dir.path(),
        &["--state-path", state, "trace", "latest", "--output", "json"],
    );
    assert_eq!(
        whole,
        reference_bytes!(compute_trace(&state_path, "latest", None).unwrap())
    );
    assert!(whole.contains(RUN_ID), "the document names the run");

    let one = rocky_stdout(
        dir.path(),
        &[
            "--state-path",
            state,
            "trace",
            RUN_ID,
            "--model",
            "b",
            "--output",
            "json",
        ],
    );
    assert_eq!(
        one,
        reference_bytes!(compute_trace(&state_path, RUN_ID, Some("b")).unwrap())
    );
    assert_ne!(whole, one, "the model filter changes the document");
}

#[test]
fn cost_prints_what_compute_cost_returns() {
    let dir = tempfile::tempdir().unwrap();
    let state_path = dir.path().join("state.redb");
    seed_state(&state_path);
    let state = state_path.to_str().unwrap();
    // No rocky.toml in the cwd: the rollup carries no adapter pricing, and
    // the seam is handed the same absent path the binary's default resolves.
    let config_path = dir.path().join("rocky.toml");
    assert!(!config_path.exists());

    let whole = rocky_stdout(
        dir.path(),
        &["--state-path", state, "cost", "latest", "--output", "json"],
    );
    assert_eq!(
        whole,
        reference_bytes!(compute_cost(&state_path, &config_path, "latest", None, None).unwrap())
    );

    let by_model = rocky_stdout(
        dir.path(),
        &[
            "--state-path",
            state,
            "cost",
            RUN_ID,
            "--by",
            "model",
            "--output",
            "json",
        ],
    );
    assert_eq!(
        by_model,
        reference_bytes!(
            compute_cost(
                &state_path,
                &config_path,
                RUN_ID,
                None,
                Some(CostGroupBy::parse("model").unwrap()),
            )
            .unwrap()
        )
    );
    assert_ne!(whole, by_model, "grouping changes the document");
}

const MINIMAL_CONFIG: &str = "[adapter]\ntype = \"duckdb\"\npath = \"x.duckdb\"\n\n\
     [pipeline.p]\ntype = \"transformation\"\nmodels = \"models/**\"\n\n\
     [pipeline.p.target.governance]\nauto_create_schemas = true\n";

#[test]
fn compliance_prints_what_compute_compliance_returns() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("rocky.toml");
    std::fs::write(&config_path, MINIMAL_CONFIG).unwrap();
    // An absent models directory is the honest empty report.
    let models_dir = dir.path().join("models");
    assert!(!models_dir.exists());
    let models = models_dir.to_str().unwrap();

    let printed = rocky_stdout(
        dir.path(),
        &["compliance", "--models", models, "--output", "json"],
    );
    assert_eq!(
        printed,
        reference_bytes!(compute_compliance(&config_path, &models_dir, None, false).unwrap())
    );
    assert!(
        printed.contains("\"command\": \"compliance\""),
        "the document is the compliance report: {printed}"
    );
}
