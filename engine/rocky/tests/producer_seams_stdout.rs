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
//!
//! Where a command's text rendering or exit code has structure of its own
//! (`policy test` prints before it exits non-zero; `estimate` prints EXPLAIN
//! failures before the table; `history --run` prints the run table and then
//! the audit table), the text path is pinned here too.

use std::path::Path;
use std::process::{Command, Output};

use chrono::{TimeZone, Utc};
use rocky_cli::commands::{
    CostGroupBy, compute_branch_list, compute_branch_show, compute_compliance, compute_cost,
    compute_estimate, compute_policy_check, compute_policy_show, compute_policy_test,
    compute_replay_check, compute_schedule_spool, compute_trace, history_run_output,
    run_branch_create,
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

/// Run the binary from `cwd` and return everything it produced.
fn rocky(cwd: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_rocky"))
        .current_dir(cwd)
        .args(args)
        .output()
        .expect("spawn rocky")
}

/// Run the binary from `cwd` and return its stdout, asserting a clean exit.
fn rocky_stdout(cwd: &Path, args: &[&str]) -> String {
    let out = rocky(cwd, args);
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

const POLICY_BASE: &str = "[adapter]\ntype = \"duckdb\"\npath = \"x.duckdb\"\n\n\
     [pipeline.p]\ntype = \"transformation\"\nmodels = \"models/**\"\n\n\
     [pipeline.p.target.governance]\nauto_create_schemas = true\n\n\
     [policy]\nversion = 1\ndefault_agent_effect = \"require_review\"\n\n\
     [[policy.rules]]\nprincipal = \"agent\"\ncapability = \"apply\"\n\
     scope = { contracted = true }\neffect = \"deny\"\n\n\
     [[policy.tests]]\nname = \"contracted apply is denied\"\nprincipal = \"agent\"\n\
     capability = \"apply\"\ncontracted = true\nexpect = \"deny\"\n\n";

const PASSING_SCENARIO: &str = "[[policy.tests]]\nname = \"human is ungated\"\nprincipal = \"human\"\n\
     capability = \"apply\"\ncontracted = true\nexpect = \"allow\"\n";

const FAILING_SCENARIO: &str = "[[policy.tests]]\nname = \"wrong on purpose\"\nprincipal = \"agent\"\n\
     capability = \"apply\"\ncontracted = true\nexpect = \"allow\"\n";

fn policy_project(
    config_body: &str,
) -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("rocky.toml");
    std::fs::write(&config_path, config_body).unwrap();
    let models_dir = dir.path().join("models");
    std::fs::create_dir_all(&models_dir).unwrap();
    std::fs::write(models_dir.join("orders.sql"), "SELECT 1 AS id\n").unwrap();
    std::fs::write(
        models_dir.join("orders.toml"),
        "name = \"orders\"\n[target]\ncatalog = \"w\"\nschema = \"s\"\ntable = \"orders\"\n",
    )
    .unwrap();
    (dir, config_path, models_dir)
}

#[test]
fn policy_check_and_test_print_what_their_seams_return() {
    let (dir, config_path, models_dir) =
        policy_project(&format!("{POLICY_BASE}{PASSING_SCENARIO}"));
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

/// `policy show` before and after a freeze recorded through the real binary:
/// an absent ledger says so, and a freeze in force is in the document.
#[tokio::test]
async fn policy_show_prints_what_compute_policy_show_returns() {
    let (dir, config_path, _models_dir) =
        policy_project(&format!("{POLICY_BASE}{PASSING_SCENARIO}"));
    let state_path = dir.path().join("state.redb");
    let state = state_path.to_str().unwrap();

    let before = rocky_stdout(
        dir.path(),
        &["--state-path", state, "policy", "show", "--output", "json"],
    );
    assert_eq!(
        before,
        reference_bytes!(
            compute_policy_show(&config_path, &state_path)
                .await
                .unwrap()
        )
    );
    assert!(before.contains("\"ledger\": \"absent\""), "{before}");
    assert!(
        before.contains("\"id\": 0"),
        "the rule carries its position: {before}"
    );

    rocky_stdout(
        dir.path(),
        &[
            "--state-path",
            state,
            "policy",
            "freeze",
            "--principal",
            "agent",
            "--scope",
            "model=fct_*",
            "--reason",
            "incident 42",
            "--output",
            "json",
        ],
    );
    let after = rocky_stdout(
        dir.path(),
        &["--state-path", state, "policy", "show", "--output", "json"],
    );
    assert_eq!(
        after,
        reference_bytes!(
            compute_policy_show(&config_path, &state_path)
                .await
                .unwrap()
        )
    );
    assert!(after.contains("\"source\": \"ledger\""), "{after}");
    assert!(after.contains("incident 42"), "{after}");
    assert_ne!(before, after, "the freeze changes the document");
}

/// The TEXT rendering of `policy show`, which the JSON parity test cannot see.
///
/// Every field the document carries and a person needs must reach the text, or
/// the terminal quietly shows less than the route does. This pins the three
/// that were missing or wrong: a ledger freeze's audit `plan_id`, a rule's
/// `verify_after`, and the fact that an absent principal means the marker body
/// could not be read rather than a deliberate both-principal freeze.
#[test]
fn policy_show_text_carries_the_plan_id_and_verify_after() {
    let policy = format!(
        "{POLICY_BASE}{PASSING_SCENARIO}\n[[policy.rules]]\nprincipal = \"agent\"\n\
         capability = \"apply\"\nscope = {{ any = true }}\neffect = \"allow\"\n\
         verify_after = [\"row_count\"]\n"
    );
    let (dir, _config_path, _models_dir) = policy_project(&policy);
    let state_path = dir.path().join("state.redb");
    let state = state_path.to_str().unwrap();

    rocky_stdout(
        dir.path(),
        &[
            "--state-path",
            state,
            "policy",
            "freeze",
            "--principal",
            "agent",
            "--scope",
            "model=fct_*",
            "--reason",
            "incident 42",
            "--output",
            "json",
        ],
    );

    let text = rocky_stdout(
        dir.path(),
        &["--state-path", state, "policy", "show", "--output", "table"],
    );

    assert!(
        text.contains("verify_after=row_count"),
        "a rule's post-apply gate must be visible in the text: {text}"
    );
    assert!(
        text.contains("plan="),
        "a ledger freeze's audit plan id must be visible in the text: {text}"
    );
    assert!(text.contains("incident 42"), "{text}");
    assert!(
        text.contains("freezes in force"),
        "the freeze heading is present when a [policy] block exists: {text}"
    );
    assert!(
        !text.contains("conditions"),
        "a rule's conditions are not carried at all: {text}"
    );
}

/// A failing scenario is a row in the report, then a non-zero exit. The
/// report must reach stdout first, in full, so CI shows which scenario broke.
#[test]
fn policy_test_prints_the_report_before_it_exits_non_zero() {
    let (dir, config_path, _models_dir) =
        policy_project(&format!("{POLICY_BASE}{FAILING_SCENARIO}"));

    let out = rocky(dir.path(), &["policy", "test", "--output", "json"]);
    assert_eq!(
        out.status.code(),
        Some(1),
        "a failing scenario exits 1, the CI gate code"
    );
    let stdout = String::from_utf8(out.stdout).unwrap();
    let report = compute_policy_test(&config_path).unwrap();
    assert_eq!((report.total, report.passed, report.failed), (2, 1, 1));
    assert_eq!(stdout, reference_bytes!(report));
    assert!(
        stdout.contains("\"name\": \"wrong on purpose\""),
        "the failing scenario is in the printed report: {stdout}"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(
        stderr, "Error: 1 of 2 policy scenario(s) failed\n",
        "the exit reason is the whole of stderr, unchanged"
    );
}

/// A DuckDB project whose `path` is absolute, so the binary and the in-process
/// seam open the same database. A TOML literal string, so a Windows path's
/// backslashes survive.
fn duckdb_project(dir: &Path) -> std::path::PathBuf {
    let db = dir.join("x.duckdb");
    let config_path = dir.join("rocky.toml");
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
    config_path
}

#[tokio::test]
async fn estimate_prints_what_compute_estimate_returns_with_no_models() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = duckdb_project(dir.path());
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
    assert_eq!(report.matched, 0);
    assert_eq!(printed, reference_bytes!(report.output));
    assert!(
        printed.contains("no models found to estimate"),
        "the document carries the empty-report message: {printed}"
    );

    let text = rocky_stdout(
        dir.path(),
        &["estimate", "--models", models, "--output", "table"],
    );
    assert_eq!(text, "No models found.\n");
}

/// Three models, three fates: `bad` fails EXPLAIN (its table does not exist),
/// `skip` never reaches EXPLAIN (a dynamic table has no DuckDB SQL), `good`
/// is estimated. The JSON holds only `good`; the text prints the EXPLAIN
/// failure before the table, and the skip only under `--verbose`.
#[tokio::test]
async fn estimate_prints_what_compute_estimate_returns_with_mixed_outcomes() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = duckdb_project(dir.path());
    let models_dir = dir.path().join("models");
    std::fs::create_dir_all(&models_dir).unwrap();
    let sidecar = |name: &str, strategy: &str| {
        format!(
            "name = \"{name}\"\n{strategy}[target]\ncatalog = \"x\"\nschema = \"main\"\ntable = \"{name}\"\n"
        )
    };
    std::fs::write(models_dir.join("good.sql"), "SELECT 1 AS id\n").unwrap();
    std::fs::write(models_dir.join("good.toml"), sidecar("good", "")).unwrap();
    std::fs::write(models_dir.join("bad.sql"), "SELECT * FROM no_such_table\n").unwrap();
    std::fs::write(models_dir.join("bad.toml"), sidecar("bad", "")).unwrap();
    std::fs::write(models_dir.join("skip.sql"), "SELECT 2 AS id\n").unwrap();
    std::fs::write(
        models_dir.join("skip.toml"),
        sidecar(
            "skip",
            "[strategy]\ntype = \"dynamic_table\"\ntarget_lag = \"1 hour\"\nwarehouse = \"wh\"\n",
        ),
    )
    .unwrap();
    let models = models_dir.to_str().unwrap();

    let report = compute_estimate(&config_path, &models_dir, None, None)
        .await
        .unwrap();
    assert_eq!(report.matched, 3);
    assert_eq!(
        report
            .output
            .estimates
            .iter()
            .map(|e| e.model_name.as_str())
            .collect::<Vec<_>>(),
        vec!["good"]
    );
    assert_eq!(report.explain_failed.len(), 1);
    assert_eq!(report.explain_failed[0].0, "bad");
    assert_eq!(report.skipped.len(), 1);
    assert_eq!(report.skipped[0].0, "skip");
    assert!(report.skipped[0].1.starts_with("SQL generation:"));

    let printed = rocky_stdout(
        dir.path(),
        &["estimate", "--models", models, "--output", "json"],
    );
    assert_eq!(printed, reference_bytes!(report.output));

    let text = rocky_stdout(
        dir.path(),
        &["estimate", "--models", models, "--output", "table"],
    );
    let failure_line = format!("  ! bad — explain failed: {}\n", report.explain_failed[0].1);
    assert!(
        text.starts_with(&failure_line),
        "the EXPLAIN failure is the first line, before the table:\n{text}"
    );
    let table_at = text
        .find("Estimated 1 model(s):\n")
        .expect("the table follows");
    assert!(table_at >= failure_line.len());
    assert!(
        text.contains("\n  good\n"),
        "the estimated model is listed:\n{text}"
    );
    assert!(
        !text.contains("Skipped before EXPLAIN"),
        "skips are a --verbose detail:\n{text}"
    );

    let verbose = rocky_stdout(
        dir.path(),
        &[
            "estimate",
            "--models",
            models,
            "--output",
            "table",
            "--verbose",
        ],
    );
    assert!(verbose.starts_with(&failure_line));
    assert!(
        verbose.contains("  Skipped before EXPLAIN (1):\n    skip — SQL generation:"),
        "--verbose names the skipped model and why:\n{verbose}"
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
    let out = rocky(
        dir.path(),
        &[
            "--state-path",
            state,
            "history",
            "--run",
            "run-none",
            "--output",
            "json",
        ],
    );
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

/// The text path of `--run`: the same summary table the list prints, one
/// row, then the governance table when `--audit` is set.
#[test]
fn history_run_text_prints_the_run_table_then_the_audit_table() {
    let dir = tempfile::tempdir().unwrap();
    let state_path = dir.path().join("state.redb");
    seed_state(&state_path);
    let state = state_path.to_str().unwrap();

    let expected_table = format!(
        "{:<12} {:<24} {:<10} {:<8} {:<10}\n{}\n{:<12} {:<24} {:<10} {:<8} {:<10}\n\nTotal runs: 1\n",
        "RUN ID",
        "STARTED",
        "STATUS",
        "MODELS",
        "TRIGGER",
        "-".repeat(66),
        "run-under-t",
        "2026-04-21 12:00:00",
        "Success",
        2,
        "Manual",
    );

    let text = rocky_stdout(
        dir.path(),
        &[
            "--state-path",
            state,
            "history",
            "--run",
            RUN_ID,
            "--output",
            "table",
        ],
    );
    assert_eq!(text, expected_table);

    // With one run in the store, the list prints the same table: `--run`
    // renders through the same rows as the list, not a second layout.
    let list = rocky_stdout(
        dir.path(),
        &["--state-path", state, "history", "--output", "table"],
    );
    assert_eq!(list, expected_table);

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
            "table",
        ],
    );
    assert!(
        audited.starts_with(&expected_table),
        "the run table comes first:\n{audited}"
    );
    let rest = &audited[expected_table.len()..];
    assert!(
        rest.starts_with("\nGovernance audit trail (--audit):\n"),
        "then the audit table:\n{rest}"
    );
    assert!(
        rest.contains("run-under-t  seams-test         cli      -          -                -                    seams-test-"),
        "the audit row carries the identity, source and host:\n{rest}"
    );
    assert!(
        rest.contains("  run-under-t  version=0.0.0-test  idempotency_key=-\n"),
        "the detail line carries the version:\n{rest}"
    );
}

/// `rocky state schedule spool --output json` prints exactly what
/// `compute_schedule_spool` returns, so `GET /api/v1/schedule/spool` and the
/// CLI cannot drift.
#[test]
fn schedule_spool_prints_what_compute_schedule_spool_returns() {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("rocky.toml");
    std::fs::write(&config, "").unwrap();

    // Two demands and one file that will not parse: the document has to carry
    // a pending list, a skipped list and the counts at once, or this pins less
    // than it looks like it does.
    for (token, at) in [
        ("delivery-2", "2026-09-10T11:00:00Z"),
        ("delivery-1", "2026-09-10T10:00:00Z"),
    ] {
        rocky_core::schedule::spool::accept(
            &dir.path().join(".rocky"),
            "orders",
            rocky_core::schedule::spool::WebhookKind::Id,
            token,
            "deadbeef",
            chrono::DateTime::parse_from_rfc3339(at)
                .unwrap()
                .with_timezone(&Utc),
        )
        .unwrap();
    }
    std::fs::write(
        dir.path().join(".rocky/pending-demands/notjson"),
        b"{ not json",
    )
    .unwrap();

    let seam = compute_schedule_spool(&config).unwrap();
    assert_eq!(seam.counts.pending, 2, "both demands are queued");
    assert_eq!(seam.counts.skipped, 1, "the bad file is reported, not hidden");

    // Pass the same absolute `--config` the seam was given: the binary's
    // default is the RELATIVE `rocky.toml`, whose parent is empty, so the
    // spool resolves to `./.rocky` and the two documents would differ on
    // `spool_path` alone. Parity is a claim about equal inputs.
    let stdout = rocky_stdout(
        dir.path(),
        &[
            "--config",
            config.to_str().unwrap(),
            "--output",
            "json",
            "state",
            "schedule",
            "spool",
        ],
    );
    assert_eq!(stdout, reference_bytes!(seam));
}

/// The text path names the spool it read and says how many demands are queued.
/// A wrong-project read must not look like an empty queue.
#[test]
fn schedule_spool_text_names_the_spool_and_its_demands() {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("rocky.toml");
    std::fs::write(&config, "").unwrap();
    rocky_core::schedule::spool::accept(
        &dir.path().join(".rocky"),
        "orders",
        rocky_core::schedule::spool::WebhookKind::Id,
        "delivery-1",
        "deadbeef",
        Utc.with_ymd_and_hms(2026, 9, 10, 10, 0, 0).unwrap(),
    )
    .unwrap();

    let stdout = rocky_stdout(
        dir.path(),
        &["--output", "table", "state", "schedule", "spool"],
    );
    assert!(
        stdout.contains("pending-demands"),
        "the text names the spool read:\n{stdout}"
    );
    assert!(
        stdout.contains("orders"),
        "the text names the queued pipeline:\n{stdout}"
    );
    assert!(
        stdout.contains("delivery-1"),
        "the text names the demand uid or token:\n{stdout}"
    );
}

/// An empty queue says so rather than printing nothing at all — silence reads
/// as a broken command.
#[test]
fn schedule_spool_text_says_the_queue_is_empty() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("rocky.toml"), "").unwrap();

    let stdout = rocky_stdout(
        dir.path(),
        &["--output", "table", "state", "schedule", "spool"],
    );
    assert!(
        stdout.contains("no demands pending"),
        "an empty spool states it:\n{stdout}"
    );
}
