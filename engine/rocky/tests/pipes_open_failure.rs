//! A requested but unusable Dagster Pipes channel must fail before a model
//! writes its DuckDB target or Rocky creates its state store.

use std::fs;
use std::path::Path;
use std::process::{Command, Output};
use std::time::{Duration, Instant};

const VALID_CONTEXT: &str = "eJyrrgUAAXUA+Q=="; // base64(zlib({}))
const UNSUPPORTED_MESSAGES: &str = "eJyrVio2VrJSqFZKKk3OTi0BMpUqlGprAVJABw0="; // {"s3":{"bucket":"x"}}
const MULTILINE_STDIO: &str = "eJyrViouScnMV7JSUEpKTInJKy4pSk3MVaoFAGdYCHs="; // {"stdio":"bad\nstream"}

fn fixture(dir: &Path) {
    let db = dir.join("fixture.duckdb");
    let conn = duckdb::Connection::open(&db).expect("open DuckDB fixture");
    conn.execute_batch("CREATE TABLE main.src AS SELECT 1 AS id;")
        .expect("seed source");
    drop(conn);

    fs::create_dir(dir.join("models")).expect("create models directory");
    fs::write(dir.join("models/stg.sql"), "SELECT id FROM main.src\n").expect("write model");
    fs::write(
        dir.join("models/stg.toml"),
        "[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"\"\nschema = \"main\"\ntable = \"stg\"\n",
    )
    .expect("write sidecar");
    fs::write(
        dir.join("rocky.toml"),
        "[adapter]\ntype = \"duckdb\"\npath = \"fixture.duckdb\"\n\n[pipeline.t]\ntype = \"transformation\"\nmodels = \"models/**\"\n\n[pipeline.t.target.governance]\nauto_create_schemas = true\n",
    )
    .expect("write config");
}

fn run(dir: &Path, context: Option<&str>, messages: Option<&str>) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_rocky"));
    command
        .current_dir(dir)
        .arg("--config")
        .arg(dir.join("rocky.toml"))
        .arg("--state-path")
        .arg(dir.join("state.redb"))
        .arg("run")
        .arg("--pipeline")
        .arg("t")
        .arg("--idempotency-key")
        .arg("pipes-bootstrap-test")
        .arg("--output")
        .arg("json")
        .env("RUST_LOG", "error")
        .env_remove("DAGSTER_PIPES_CONTEXT")
        .env_remove("DAGSTER_PIPES_MESSAGES");
    if let Some(value) = context {
        command.env("DAGSTER_PIPES_CONTEXT", value);
    }
    if let Some(value) = messages {
        command.env("DAGSTER_PIPES_MESSAGES", value);
    }
    command.output().expect("spawn rocky binary")
}

fn target_exists(dir: &Path) -> bool {
    let conn = duckdb::Connection::open(dir.join("fixture.duckdb")).expect("reopen fixture");
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'main' AND table_name = 'stg'",
            [],
            |row| row.get(0),
        )
        .expect("check target table");
    count != 0
}

fn assert_refused_without_work(context: Option<&str>, messages: Option<&str>, reason: &str) {
    let tmp = tempfile::tempdir().expect("tempdir");
    fixture(tmp.path());
    let output = run(tmp.path(), context, messages);
    let stderr = String::from_utf8(output.stderr).expect("utf8 stderr");
    assert!(!output.status.success(), "expected failure: {stderr}");
    let lines: Vec<_> = stderr.lines().collect();
    assert_eq!(lines.len(), 1, "expected one error line: {stderr}");
    assert!(lines[0].contains(reason), "missing {reason:?}: {stderr}");
    assert!(
        !target_exists(tmp.path()),
        "failed Pipes launch wrote target"
    );
    assert!(
        !tmp.path().join("state.redb").exists(),
        "failed Pipes launch wrote state"
    );
}

#[test]
fn pipes_bad_messages_exits_before_work() {
    assert_refused_without_work(
        Some(VALID_CONTEXT),
        Some("not-base64"),
        "DAGSTER_PIPES_MESSAGES cannot be base64-decoded",
    );
}

#[test]
fn pipes_unsupported_channel_exits_before_work() {
    assert_refused_without_work(
        Some(VALID_CONTEXT),
        Some(UNSUPPORTED_MESSAGES),
        "DAGSTER_PIPES_MESSAGES has unsupported channel shape",
    );
}

#[test]
fn pipes_undecodable_context_exits_before_work() {
    assert_refused_without_work(
        Some("not-base64"),
        Some(UNSUPPORTED_MESSAGES),
        "DAGSTER_PIPES_CONTEXT cannot be base64-decoded",
    );
}

#[test]
fn pipes_missing_messages_exits_before_work() {
    assert_refused_without_work(
        Some(VALID_CONTEXT),
        None,
        "DAGSTER_PIPES_MESSAGES is missing",
    );
}

#[test]
fn pipes_unsupported_stdio_keeps_error_on_one_line() {
    assert_refused_without_work(
        Some(VALID_CONTEXT),
        Some(MULTILINE_STDIO),
        "DAGSTER_PIPES_MESSAGES stdio target",
    );
}

#[test]
fn pipes_bad_messages_seed_dag_exits_before_work() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    fs::write(
        dir.join("rocky.toml"),
        "[adapter]\ntype = \"duckdb\"\npath = \"seed.duckdb\"\n\n[pipeline.t]\ntype = \"transformation\"\n\n[pipeline.t.target]\nadapter = \"default\"\n",
    )
    .expect("write config");
    fs::create_dir(dir.join("seeds")).expect("create seeds directory");
    fs::write(dir.join("seeds/countries.csv"), "code,name\nPT,Portugal\n").expect("write seed");
    let output = Command::new(env!("CARGO_BIN_EXE_rocky"))
        .current_dir(dir)
        .arg("--config")
        .arg(dir.join("rocky.toml"))
        .arg("--state-path")
        .arg(dir.join("state.redb"))
        .arg("run")
        .arg("--dag")
        .env("RUST_LOG", "error")
        .env("DAGSTER_PIPES_CONTEXT", VALID_CONTEXT)
        .env("DAGSTER_PIPES_MESSAGES", "not-base64")
        .output()
        .expect("spawn rocky binary");
    let stderr = String::from_utf8(output.stderr).expect("utf8 stderr");
    assert!(!output.status.success(), "expected refusal: {stderr}");
    assert!(
        stderr.contains("DAGSTER_PIPES_MESSAGES cannot be base64-decoded"),
        "wrong refusal: {stderr}"
    );
    assert_eq!(
        stderr.lines().count(),
        1,
        "more than one error line: {stderr}"
    );
    assert!(
        !dir.join("seed.duckdb").exists(),
        "DAG seed wrote warehouse"
    );
    assert!(!dir.join("state.redb").exists(), "DAG seed wrote state");
}

#[test]
fn pipes_bad_messages_apply_exits_before_work() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    let conn = duckdb::Connection::open(dir.join("fixture.duckdb")).expect("open fixture");
    conn.execute_batch(
        "CREATE SCHEMA raw__orders; CREATE TABLE raw__orders.orders AS SELECT 1 AS id;",
    )
    .expect("seed source");
    drop(conn);
    fs::write(
        dir.join("rocky.toml"),
        "[adapter]\ntype = \"duckdb\"\npath = \"fixture.duckdb\"\n\n[pipeline.ingest]\nstrategy = \"full_refresh\"\n\n[pipeline.ingest.source.discovery]\nadapter = \"default\"\n\n[pipeline.ingest.source.schema_pattern]\nprefix = \"raw__\"\nseparator = \"__\"\ncomponents = [\"source\"]\n\n[pipeline.ingest.target]\ncatalog_template = \"fixture\"\nschema_template = \"staging__{source}\"\n\n[pipeline.ingest.target.governance]\nauto_create_schemas = true\n",
    )
    .expect("write config");
    let plan = Command::new(env!("CARGO_BIN_EXE_rocky"))
        .current_dir(dir)
        .args(["--output", "json"])
        .arg("--config")
        .arg(dir.join("rocky.toml"))
        .arg("--state-path")
        .arg(dir.join("state.redb"))
        .args(["plan", "--pipeline", "ingest"])
        .env_remove("DAGSTER_PIPES_CONTEXT")
        .env_remove("DAGSTER_PIPES_MESSAGES")
        .output()
        .expect("spawn plan");
    assert!(
        plan.status.success(),
        "plan failed: {}",
        String::from_utf8_lossy(&plan.stderr)
    );
    let plan_json: serde_json::Value =
        serde_json::from_slice(&plan.stdout).expect("parse plan JSON");
    let plan_id = plan_json["plan_id"].as_str().expect("plan id");
    let state_before = fs::read(dir.join("state.redb")).ok();

    let apply = Command::new(env!("CARGO_BIN_EXE_rocky"))
        .current_dir(dir)
        .args(["--output", "json"])
        .arg("--config")
        .arg(dir.join("rocky.toml"))
        .arg("--state-path")
        .arg(dir.join("state.redb"))
        .args(["apply", plan_id])
        .env("RUST_LOG", "error")
        .env("DAGSTER_PIPES_CONTEXT", VALID_CONTEXT)
        .env("DAGSTER_PIPES_MESSAGES", "not-base64")
        .output()
        .expect("spawn apply");
    let stderr = String::from_utf8(apply.stderr).expect("utf8 stderr");
    assert!(!apply.status.success(), "expected refusal: {stderr}");
    assert!(
        stderr.contains("DAGSTER_PIPES_MESSAGES cannot be base64-decoded"),
        "wrong refusal: {stderr}"
    );
    assert_eq!(
        stderr.lines().count(),
        1,
        "more than one error line: {stderr}"
    );
    let conn = duckdb::Connection::open(dir.join("fixture.duckdb")).expect("reopen fixture");
    let copied: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'staging__orders' AND table_name = 'orders'",
            [],
            |row| row.get(0),
        )
        .expect("check target");
    assert_eq!(copied, 0, "failed apply wrote target");
    assert_eq!(fs::read(dir.join("state.redb")).ok(), state_before);
}

#[test]
fn pipes_bad_messages_watch_exits_before_work() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    fixture(dir);
    let mut child = Command::new(env!("CARGO_BIN_EXE_rocky"))
        .current_dir(dir)
        .arg("--config")
        .arg(dir.join("rocky.toml"))
        .arg("--state-path")
        .arg(dir.join("state.redb"))
        .args(["run", "--pipeline", "t", "--watch"])
        .env("RUST_LOG", "error")
        .env("DAGSTER_PIPES_CONTEXT", VALID_CONTEXT)
        .env("DAGSTER_PIPES_MESSAGES", "not-base64")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn watch");
    // main() waits up to five seconds for its runtime to drain after an
    // error, so the test deadline must exceed that normal shutdown grace.
    let deadline = Instant::now() + Duration::from_secs(12);
    loop {
        if child.try_wait().expect("poll watch").is_some() {
            break;
        }
        if Instant::now() >= deadline {
            child.kill().expect("kill hung watch");
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    let output = child.wait_with_output().expect("collect watch output");
    let stderr = String::from_utf8(output.stderr).expect("utf8 stderr");
    assert!(!output.status.success(), "expected refusal: {stderr}");
    assert!(
        stderr.contains("DAGSTER_PIPES_MESSAGES cannot be base64-decoded"),
        "wrong refusal: {stderr}"
    );
    assert_eq!(
        stderr.lines().count(),
        1,
        "more than one error line: {stderr}"
    );
    assert!(!target_exists(dir), "failed watch wrote target");
    assert!(!dir.join("state.redb").exists(), "failed watch wrote state");
}

#[test]
fn pipes_unset_context_runs_normally() {
    let tmp = tempfile::tempdir().expect("tempdir");
    fixture(tmp.path());
    let output = run(tmp.path(), None, Some("not-base64"));
    assert!(
        output.status.success(),
        "run failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(target_exists(tmp.path()), "normal run did not build target");
}
