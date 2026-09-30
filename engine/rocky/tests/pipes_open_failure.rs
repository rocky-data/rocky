//! A requested but unusable Dagster Pipes channel must fail before a model
//! writes its DuckDB target or Rocky creates its state store.

use std::fs;
use std::path::Path;
use std::process::{Command, Output};
use std::time::{Duration, Instant};

const VALID_CONTEXT: &str = "eJyrrgUAAXUA+Q=="; // base64(zlib({}))
const UNSUPPORTED_MESSAGES: &str = "eJyrVio2VrJSqFZKKk3OTi0BMpUqlGprAVJABw0="; // {"s3":{"bucket":"x"}}
const MULTILINE_STDIO: &str = "eJyrViouScnMV7JSUEpKTInJKy4pSk3MVaoFAGdYCHs="; // {"stdio":"bad\nstream"}
const BAD_ZLIB: &str = "e30="; // base64({}), without zlib

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
        "[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"\"\nschema = \"fresh_pipes\"\ntable = \"stg\"\n",
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
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'fresh_pipes' AND table_name = 'stg'",
            [],
            |row| row.get(0),
        )
        .expect("check target table");
    count != 0
}

fn schema_exists(dir: &Path) -> bool {
    let conn = duckdb::Connection::open(dir.join("fixture.duckdb")).expect("reopen fixture");
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = 'fresh_pipes'",
            [],
            |row| row.get(0),
        )
        .expect("check target schema");
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
        !schema_exists(tmp.path()),
        "failed Pipes launch wrote schema"
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
fn pipes_bad_zlib_exits_before_work() {
    assert_refused_without_work(
        Some(VALID_CONTEXT),
        Some(BAD_ZLIB),
        "DAGSTER_PIPES_MESSAGES cannot be zlib-decompressed",
    );
}

#[test]
fn pipes_bad_json_exits_before_work() {
    let invalid_json = encode_raw_param(b"secret-invalid-json");
    assert_refused_without_work(
        Some(VALID_CONTEXT),
        Some(&invalid_json),
        "DAGSTER_PIPES_MESSAGES cannot be JSON-decoded",
    );
    let tmp = tempfile::tempdir().expect("tempdir");
    fixture(tmp.path());
    let output = run(tmp.path(), Some(VALID_CONTEXT), Some(&invalid_json));
    assert!(!String::from_utf8_lossy(&output.stderr).contains("secret-invalid-json"));
}

#[test]
fn pipes_path_channel_open_failure_exits_before_work() {
    let tmp = tempfile::tempdir().expect("tempdir");
    fixture(tmp.path());
    let missing_parent = tmp.path().join("missing").join("messages.jsonl");
    let payload = serde_json::json!({"path": missing_parent});
    let messages = encode_param(&payload);
    let output = run(tmp.path(), Some(VALID_CONTEXT), Some(&messages));
    let stderr = String::from_utf8(output.stderr).expect("utf8 stderr");
    assert!(!output.status.success(), "expected refusal: {stderr}");
    assert!(
        stderr.contains("cannot be opened"),
        "wrong refusal: {stderr}"
    );
    assert!(!stderr.contains(&missing_parent.to_string_lossy().to_string()));
    assert!(!schema_exists(tmp.path()));
    assert!(!target_exists(tmp.path()));
    assert!(!tmp.path().join("state.redb").exists());
}

#[cfg(unix)]
#[test]
fn pipes_opened_write_failure_releases_idempotency_claim() {
    let tmp = tempfile::tempdir().expect("tempdir");
    fixture(tmp.path());
    let messages = encode_param(&serde_json::json!({"stdio": "stderr"}));
    // The entry gate accepts stderr without writing. Close the pipe's read
    // end before the binary reaches the later `opened` handshake.
    let mut first_child = Command::new(env!("CARGO_BIN_EXE_rocky"))
        .arg("--config")
        .arg(tmp.path().join("rocky.toml"))
        .arg("--state-path")
        .arg(tmp.path().join("state.redb"))
        .args([
            "run",
            "--pipeline",
            "t",
            "--idempotency-key",
            "pipes-bootstrap-test",
            "--output",
            "json",
        ])
        .current_dir(tmp.path())
        .env("RUST_LOG", "error")
        .env("DAGSTER_PIPES_CONTEXT", VALID_CONTEXT)
        .env("DAGSTER_PIPES_MESSAGES", messages)
        .stderr(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .expect("spawn rocky with piped stderr");
    drop(first_child.stderr.take());
    let first = first_child
        .wait_with_output()
        .expect("wait for failed opened write");
    assert!(
        !first.status.success(),
        "opened write unexpectedly succeeded"
    );
    assert!(
        tmp.path().join("state.redb").exists(),
        "claim was not persisted"
    );
    assert!(!target_exists(tmp.path()), "failed launch ran model");

    let retry = run(tmp.path(), None, None);
    assert!(
        retry.status.success(),
        "retry failed: {}",
        String::from_utf8_lossy(&retry.stderr)
    );
    let retry_json: serde_json::Value =
        serde_json::from_slice(&retry.stdout).expect("retry JSON output");
    assert_ne!(
        retry_json["status"],
        "SkippedInFlight",
        "retry was suppressed: {}",
        String::from_utf8_lossy(&retry.stdout)
    );
    assert!(
        target_exists(tmp.path()),
        "retry did not build target: first_status={} first_stdout={} stdout={} stderr={}",
        first.status,
        String::from_utf8_lossy(&first.stdout),
        String::from_utf8_lossy(&retry.stdout),
        String::from_utf8_lossy(&retry.stderr)
    );
}

#[cfg(unix)]
#[test]
fn non_pipes_compile_keeps_sigpipe_with_inherited_context() {
    use std::os::unix::process::ExitStatusExt;

    let tmp = tempfile::tempdir().expect("tempdir");
    fixture(tmp.path());
    for inherited_context in [false, true] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_rocky"));
        command
            .current_dir(tmp.path())
            .args(["--output", "json", "--config"])
            .arg(tmp.path().join("rocky.toml"))
            .arg("compile")
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .env_remove("DAGSTER_PIPES_CONTEXT");
        if inherited_context {
            command.env("DAGSTER_PIPES_CONTEXT", VALID_CONTEXT);
        }
        let mut child = command.spawn().expect("spawn compile");
        drop(child.stdout.take());
        let output = child.wait_with_output().expect("wait for compile");
        assert_eq!(
            output.status.signal(),
            Some(libc::SIGPIPE),
            "context={inherited_context}, status={}, stderr={}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

fn encode_param(value: &serde_json::Value) -> String {
    encode_raw_param(value.to_string().as_bytes())
}

fn encode_raw_param(raw: &[u8]) -> String {
    use base64::Engine as _;
    use std::io::Write as _;
    let mut encoder = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(raw).unwrap();
    base64::engine::general_purpose::STANDARD.encode(encoder.finish().unwrap())
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
        "DAGSTER_PIPES_CONTEXT is set but DAGSTER_PIPES_MESSAGES is missing",
    );
}

#[test]
fn pipes_unsupported_stdio_keeps_error_on_one_line() {
    assert_refused_without_work(
        Some(VALID_CONTEXT),
        Some(MULTILINE_STDIO),
        "DAGSTER_PIPES_MESSAGES stdio target",
    );
    let tmp = tempfile::tempdir().expect("tempdir");
    fixture(tmp.path());
    let output = run(tmp.path(), Some(VALID_CONTEXT), Some(MULTILINE_STDIO));
    let stderr = String::from_utf8(output.stderr).expect("utf8 stderr");
    assert!(!stderr.contains("bad\\nstream"), "stdio value leaked");
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

    let normal_tmp = tempfile::tempdir().expect("normal DAG tempdir");
    fixture(normal_tmp.path());
    let normal_dir = normal_tmp.path();
    let normal = Command::new(env!("CARGO_BIN_EXE_rocky"))
        .current_dir(normal_dir)
        .arg("--config")
        .arg(normal_dir.join("rocky.toml"))
        .arg("--state-path")
        .arg(normal_dir.join("state.redb"))
        .args(["run", "--dag"])
        .env_remove("DAGSTER_PIPES_CONTEXT")
        .env_remove("DAGSTER_PIPES_MESSAGES")
        .output()
        .expect("spawn normal DAG");
    assert!(
        normal.status.success(),
        "normal DAG failed: {}",
        String::from_utf8_lossy(&normal.stderr)
    );
    assert!(target_exists(normal_dir), "normal DAG did not build target");
    assert!(schema_exists(normal_dir), "normal DAG did not build schema");
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
    drop(conn);
    assert_eq!(fs::read(dir.join("state.redb")).ok(), state_before);

    let normal = Command::new(env!("CARGO_BIN_EXE_rocky"))
        .current_dir(dir)
        .args(["--output", "json"])
        .arg("--config")
        .arg(dir.join("rocky.toml"))
        .arg("--state-path")
        .arg(dir.join("state.redb"))
        .args(["apply", plan_id])
        .env_remove("DAGSTER_PIPES_CONTEXT")
        .env_remove("DAGSTER_PIPES_MESSAGES")
        .output()
        .expect("spawn normal apply");
    assert!(
        normal.status.success(),
        "normal apply failed: {}",
        String::from_utf8_lossy(&normal.stderr)
    );
    let conn = duckdb::Connection::open(dir.join("fixture.duckdb")).unwrap();
    let copied: i64 = conn.query_row(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'staging__orders' AND table_name = 'orders'",
        [], |row| row.get(0),
    ).unwrap();
    assert_eq!(copied, 1, "normal apply did not build target");
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
    let deadline = Instant::now() + Duration::from_secs(60);
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

#[cfg(unix)]
#[test]
fn pipes_unset_context_watch_runs_normally() {
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
        .env_remove("DAGSTER_PIPES_CONTEXT")
        .env_remove("DAGSTER_PIPES_MESSAGES")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn normal watch");
    let deadline = Instant::now() + Duration::from_secs(90);
    while rocky_core::state::StateStore::open_read_only(&dir.join("state.redb"))
        .ok()
        .and_then(|store| store.latest_successful_run("t").ok().flatten())
        .is_none()
    {
        if Instant::now() >= deadline {
            child.kill().expect("kill hung watch");
            panic!("normal watch did not complete a run");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    // SAFETY: `child.id()` is a live child process and SIGINT is a valid signal.
    unsafe { libc::kill(child.id() as libc::pid_t, libc::SIGINT) };
    let output = child.wait_with_output().expect("wait for watch shutdown");
    assert!(
        output.status.success(),
        "normal watch failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(target_exists(dir), "normal watch did not build target");
    assert!(schema_exists(dir), "normal watch did not build schema");
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

fn assert_command_refused_at_pipes_gate(args: &[&str]) {
    let tmp = tempfile::tempdir().expect("tempdir");
    fixture(tmp.path());
    let output = Command::new(env!("CARGO_BIN_EXE_rocky"))
        .current_dir(tmp.path())
        .arg("--config")
        .arg(tmp.path().join("rocky.toml"))
        .arg("--state-path")
        .arg(tmp.path().join("state.redb"))
        .args(args)
        .env("RUST_LOG", "error")
        .env("DAGSTER_PIPES_CONTEXT", VALID_CONTEXT)
        .env("DAGSTER_PIPES_MESSAGES", "not-base64")
        .output()
        .expect("spawn rocky");
    let stderr = String::from_utf8(output.stderr).expect("utf8 stderr");
    assert!(!output.status.success());
    assert!(
        stderr.contains("DAGSTER_PIPES_MESSAGES cannot be base64-decoded"),
        "wrong refusal: {stderr}"
    );
    assert!(!tmp.path().join("state.redb").exists());
    assert!(!schema_exists(tmp.path()));
    assert!(!target_exists(tmp.path()));
    assert!(!tmp.path().join(".rocky").exists());
}

#[test]
fn pipes_bad_messages_snapshot_exits_before_work() {
    assert_command_refused_at_pipes_gate(&["snapshot", "--pipeline", "t"]);
}

#[test]
fn pipes_bad_messages_fulfill_exits_before_work() {
    assert_command_refused_at_pipes_gate(&["fulfill", "sample"]);
}

#[test]
fn pipes_refusal_does_not_create_state_namespace() {
    for args in [
        &["snapshot", "--pipeline", "t"][..],
        &["fulfill", "sample"][..],
    ] {
        let tmp = tempfile::tempdir().expect("tempdir");
        fixture(tmp.path());
        let output = Command::new(env!("CARGO_BIN_EXE_rocky"))
            .current_dir(tmp.path())
            .arg("--config")
            .arg(tmp.path().join("rocky.toml"))
            .args(["--state-namespace", "isolated"])
            .args(args)
            .env("RUST_LOG", "error")
            .env("DAGSTER_PIPES_CONTEXT", VALID_CONTEXT)
            .env("DAGSTER_PIPES_MESSAGES", "not-base64")
            .output()
            .expect("spawn rocky");
        assert!(!output.status.success(), "expected refusal for {args:?}");
        assert!(
            String::from_utf8_lossy(&output.stderr)
                .contains("DAGSTER_PIPES_MESSAGES cannot be base64-decoded"),
            "wrong refusal: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            !tmp.path().join("models/.rocky-state").exists(),
            "{args:?} created namespace before refusal"
        );
    }
}
