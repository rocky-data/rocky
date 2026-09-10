//! The commands with a producer seam print, byte for byte, what the seam
//! returns.
//!
//! The seams (`compute_trace`, `compute_cost`, `compute_compliance`,
//! `compute_replay_check`, `compute_policy_check`, `compute_policy_test`,
//! `compute_branch_list`, `compute_branch_show`, `compute_estimate`,
//! `history_run_output`) exist so a server route can serve the same document
//! the CLI prints. These tests spawn the real binary and compare its stdout
//! against the seam's own serialisation, byte for byte. What they pin is the
//! equality: a `run_*` that serialises differently, or a seam whose document
//! drifts from what the CLI prints, fails here. They cannot see whether
//! `run_*` calls the seam or reproduces its bytes another way; only the
//! bytes are the contract a route needs.

use std::path::Path;
use std::process::Command;

use chrono::{TimeZone, Utc};
use rocky_cli::commands::{
    CostGroupBy, compute_branch_list, compute_branch_show, compute_compliance, compute_cost,
    compute_estimate, compute_policy_check, compute_policy_test, compute_replay_check,
    compute_trace, history_run_output, run_branch_create,
};
use rocky_core::config::{PolicyCapability, PolicyPrincipal};
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
            triggering_identity: Some("seams-test".to_string()),
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

#[test]
fn replay_check_prints_what_compute_replay_check_returns() {
    let dir = tempfile::tempdir().unwrap();
    let state_path = dir.path().join("state.redb");
    seed_state(&state_path);
    let state = state_path.to_str().unwrap();

    let whole = rocky_stdout(
        dir.path(),
        &[
            "--state-path",
            state,
            "replay",
            RUN_ID,
            "--check",
            "--output",
            "json",
        ],
    );
    assert_eq!(
        whole,
        reference_bytes!(compute_replay_check(&state_path, RUN_ID, None).unwrap())
    );
    assert!(
        whole.contains("\"command\": \"replay --check\""),
        "the document is the replay check: {whole}"
    );

    let one = rocky_stdout(
        dir.path(),
        &[
            "--state-path",
            state,
            "replay",
            "latest",
            "--check",
            "--model",
            "a",
            "--output",
            "json",
        ],
    );
    assert_eq!(
        one,
        reference_bytes!(compute_replay_check(&state_path, "latest", Some("a")).unwrap())
    );
    assert_ne!(whole, one, "the model filter changes the document");
}

#[test]
fn branch_list_and_show_print_what_their_seams_return() {
    let dir = tempfile::tempdir().unwrap();
    let state_path = dir.path().join("state.redb");
    // Seeded through the production create path, in process; its own JSON
    // goes to this test's stdout and is not under test.
    run_branch_create(&state_path, "fix-price", Some("a description"), true).unwrap();
    let state = state_path.to_str().unwrap();

    let list = rocky_stdout(
        dir.path(),
        &["--state-path", state, "branch", "list", "--output", "json"],
    );
    assert_eq!(
        list,
        reference_bytes!(compute_branch_list(&state_path).unwrap())
    );
    assert!(
        list.contains("fix-price"),
        "the list names the branch: {list}"
    );

    let show = rocky_stdout(
        dir.path(),
        &[
            "--state-path",
            state,
            "branch",
            "show",
            "fix-price",
            "--output",
            "json",
        ],
    );
    assert_eq!(
        show,
        reference_bytes!(compute_branch_show(&state_path, "fix-price").unwrap())
    );
    assert!(
        show.contains("\"command\": \"branch show\""),
        "the document is one branch: {show}"
    );
}

const POLICY_CONFIG: &str = "[adapter]\ntype = \"duckdb\"\npath = \"x.duckdb\"\n\n\
     [pipeline.p]\ntype = \"transformation\"\nmodels = \"models/**\"\n\n\
     [pipeline.p.target.governance]\nauto_create_schemas = true\n\n\
     [policy]\nversion = 1\ndefault_agent_effect = \"require_review\"\n\n\
     [[policy.rules]]\nprincipal = \"agent\"\ncapability = \"apply\"\n\
     scope = { contracted = true }\neffect = \"deny\"\n\n\
     [[policy.tests]]\nname = \"contracted apply is denied\"\nprincipal = \"agent\"\n\
     capability = \"apply\"\ncontracted = true\nexpect = \"deny\"\n\n\
     [[policy.tests]]\nname = \"human is ungated\"\nprincipal = \"human\"\n\
     capability = \"apply\"\ncontracted = true\nexpect = \"allow\"\n";

#[test]
fn policy_check_and_test_print_what_their_seams_return() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("rocky.toml");
    std::fs::write(&config_path, POLICY_CONFIG).unwrap();
    let models_dir = dir.path().join("models");
    std::fs::create_dir_all(&models_dir).unwrap();
    std::fs::write(models_dir.join("orders.sql"), "SELECT 1 AS id\n").unwrap();
    std::fs::write(
        models_dir.join("orders.toml"),
        "name = \"orders\"\n[target]\ncatalog = \"w\"\nschema = \"s\"\ntable = \"orders\"\n",
    )
    .unwrap();
    let models = models_dir.to_str().unwrap();

    let check = rocky_stdout(
        dir.path(),
        &[
            "policy",
            "check",
            "--principal",
            "agent",
            "--capability",
            "apply",
            "--model",
            "orders",
            "--models",
            models,
            "--output",
            "json",
        ],
    );
    assert_eq!(
        check,
        reference_bytes!(
            compute_policy_check(
                &config_path,
                &models_dir,
                PolicyPrincipal::Agent,
                PolicyCapability::Apply,
                "orders",
            )
            .unwrap()
        )
    );
    assert!(
        check.contains("\"command\": \"policy_check\""),
        "the document is the decision: {check}"
    );

    let test = rocky_stdout(dir.path(), &["policy", "test", "--output", "json"]);
    assert_eq!(
        test,
        reference_bytes!(compute_policy_test(&config_path).unwrap())
    );
    assert!(
        test.contains("\"command\": \"policy_test\""),
        "the document is the scenario report: {test}"
    );
}

#[tokio::test]
async fn estimate_prints_what_compute_estimate_returns() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("x.duckdb");
    let config_path = dir.path().join("rocky.toml");
    // A TOML literal string, so a Windows path's backslashes survive.
    std::fs::write(
        &config_path,
        format!(
            "[adapter]\ntype = \"duckdb\"\npath = '{}'\n\n\
             [pipeline.p]\ntype = \"transformation\"\nmodels = \"models/**\"\n\n\
             [pipeline.p.target.governance]\nauto_create_schemas = true\n",
            db.display()
        ),
    )
    .unwrap();
    // No models: the report says so, in the JSON document's `message`.
    let models_dir = dir.path().join("models");
    std::fs::create_dir_all(&models_dir).unwrap();
    let models = models_dir.to_str().unwrap();

    let printed = rocky_stdout(
        dir.path(),
        &["estimate", "--models", models, "--output", "json"],
    );
    let report = compute_estimate(&config_path, &models_dir, None, None)
        .await
        .unwrap();
    assert_eq!(printed, reference_bytes!(report.output));
    assert!(
        printed.contains("no models found to estimate"),
        "the document carries the empty-report message: {printed}"
    );
}

#[test]
fn history_run_prints_what_history_run_output_returns() {
    let dir = tempfile::tempdir().unwrap();
    let state_path = dir.path().join("state.redb");
    seed_state(&state_path);
    let state = state_path.to_str().unwrap();

    let one = rocky_stdout(
        dir.path(),
        &[
            "--state-path",
            state,
            "history",
            "--run",
            RUN_ID,
            "--output",
            "json",
        ],
    );
    assert_eq!(
        one,
        reference_bytes!(history_run_output(&state_path, RUN_ID, false).unwrap())
    );
    assert!(one.contains("\"count\": 1"), "one run: {one}");
    assert!(
        !one.contains("seams-test-host"),
        "audit fields stay absent without --audit: {one}"
    );

    let audited = rocky_stdout(
        dir.path(),
        &[
            "--state-path",
            state,
            "history",
            "--run",
            RUN_ID,
            "--audit",
            "--output",
            "json",
        ],
    );
    assert_eq!(
        audited,
        reference_bytes!(history_run_output(&state_path, RUN_ID, true).unwrap())
    );
    assert!(
        audited.contains("seams-test-host"),
        "--audit carries the hostname: {audited}"
    );

    // A run the store does not hold is a refusal that names the id, not an
    // empty list.
    let out = Command::new(env!("CARGO_BIN_EXE_rocky"))
        .current_dir(dir.path())
        .args([
            "--state-path",
            state,
            "history",
            "--run",
            "run-none",
            "--output",
            "json",
        ])
        .output()
        .expect("spawn rocky");
    assert!(!out.status.success(), "an unknown run id must not exit 0");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("run-none"),
        "the refusal names the id: {stderr}"
    );
    assert!(
        out.stdout.is_empty(),
        "nothing is printed for a run that does not exist: {}",
        String::from_utf8_lossy(&out.stdout)
    );
}
