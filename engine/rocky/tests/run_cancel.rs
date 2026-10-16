//! A signal during a plain `rocky run` settles the run (#1606).
//!
//! Before #1606 only the replication fan-out listened for signals. A
//! transformation run had no listener at all, so SIGTERM killed it on the
//! default disposition: no run record, no released idempotency claim.
//!
//! These tests hold a model's statement open on a FIFO, signal the child while
//! the statement is provably in flight, and then check what the run left
//! behind. Unix-only, like `run_watch.rs`, for the same reason: real signal
//! delivery to a child process.

#![cfg(unix)]

use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{Receiver, RecvTimeoutError, channel};
use std::thread;
use std::time::{Duration, Instant};

/// Max-wait for each step. Generous on purpose: a max-wait costs nothing when
/// things work, and a debug build on a loaded CI box is slow to start.
const STEP_BUDGET: Duration = Duration::from_secs(120);

/// Two models in two layers: `blocking` reads the FIFO, `after` depends on it.
/// `concurrency = 1` keeps one statement in flight at a time.
const ROCKY_TOML: &str = r#"
[adapter]
type = "duckdb"
path = "fixture.duckdb"

[pipeline.transform]
type = "transformation"

[pipeline.transform.target]
adapter = "default"

[pipeline.transform.execution]
concurrency = 1
"#;

const BLOCKING_SQL: &str = "SELECT id FROM main.blocking_source\n";
const BLOCKING_TOML: &str = r#"
[target]
catalog = "fixture"
schema = "staging"
"#;
const AFTER_SQL: &str = "SELECT id FROM staging.blocking\n";
const AFTER_TOML: &str = r#"
depends_on = ["blocking"]

[target]
catalog = "fixture"
schema = "staging"
"#;

/// The file the source view reads. A FIFO during the signalled run.
const SOURCE_FILE: &str = "blocking_source.csv";

struct Fixture {
    _tmp: tempfile::TempDir,
    dir: PathBuf,
    source: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let tmp = tempfile::tempdir().expect("tempdir");
        let dir = tmp.path().to_path_buf();
        let source = dir.join(SOURCE_FILE);
        fs::write(&source, "1\n").expect("write source csv");
        {
            let conn = duckdb::Connection::open(dir.join("fixture.duckdb")).expect("open duckdb");
            conn.execute_batch(&format!(
                "CREATE SCHEMA IF NOT EXISTS staging;\n\
                 CREATE OR REPLACE VIEW main.blocking_source AS\n\
                 SELECT id FROM read_csv('{}', columns = {{'id': 'INTEGER'}}, header = false);\n",
                source.display()
            ))
            .expect("seed sql");
        }
        fs::write(dir.join("rocky.toml"), ROCKY_TOML).expect("write rocky.toml");
        let models = dir.join("models");
        fs::create_dir(&models).expect("mkdir models");
        fs::write(models.join("blocking.sql"), BLOCKING_SQL).expect("write model");
        fs::write(models.join("blocking.toml"), BLOCKING_TOML).expect("write sidecar");
        fs::write(models.join("after.sql"), AFTER_SQL).expect("write model");
        fs::write(models.join("after.toml"), AFTER_TOML).expect("write sidecar");
        Fixture {
            _tmp: tmp,
            dir,
            source,
        }
    }

    fn state_path(&self) -> PathBuf {
        self.dir.join("state.redb")
    }

    /// Replace the source CSV with a FIFO, so the next read of it blocks.
    fn make_source_a_fifo(&self) {
        fs::remove_file(&self.source).expect("remove source csv");
        use std::os::unix::ffi::OsStrExt;
        let c_path = std::ffi::CString::new(self.source.as_os_str().as_bytes())
            .expect("fixture path is a C string");
        // SAFETY: `libc::mkfifo` with a valid NUL-terminated path and a
        // permission mask only touches the filesystem; the return code is
        // checked below.
        let rc = unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) };
        assert_eq!(rc, 0, "mkfifo failed: {}", std::io::Error::last_os_error());
    }

    /// Put an ordinary one-row CSV back in place of the FIFO.
    fn make_source_a_file(&self) {
        fs::remove_file(&self.source).expect("remove fifo");
        fs::write(&self.source, "1\n").expect("write source csv");
    }

    fn rocky(&self) -> Command {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_rocky"));
        cmd.arg("-c")
            .arg(self.dir.join("rocky.toml"))
            .arg("--state-path")
            .arg(self.state_path())
            .arg("--output")
            .arg("json")
            .current_dir(&self.dir)
            .env("RUST_LOG", "error");
        cmd
    }

    /// Spawn `rocky run` and wait until its `blocking` statement holds the
    /// FIFO open. Returns the child, its stderr lines and the FIFO's write
    /// end, which keeps the statement in flight while it lives.
    fn spawn_run_blocked_on_the_fifo(
        &self,
        idempotency_key: &str,
    ) -> (
        Child,
        Receiver<String>,
        thread::JoinHandle<String>,
        fs::File,
    ) {
        self.spawn_blocked_on_the_fifo(&["run", "--idempotency-key", idempotency_key])
    }

    /// [`Self::spawn_run_blocked_on_the_fifo`] with the subcommand arguments
    /// given in full.
    fn spawn_blocked_on_the_fifo(
        &self,
        args: &[&str],
    ) -> (
        Child,
        Receiver<String>,
        thread::JoinHandle<String>,
        fs::File,
    ) {
        self.make_source_a_fifo();
        let (open_tx, open_rx) = channel::<fs::File>();
        let fifo = self.source.clone();
        thread::spawn(move || {
            if let Ok(write_end) = fs::OpenOptions::new().write(true).open(&fifo) {
                let _ = open_tx.send(write_end);
            }
        });
        let mut child = self
            .rocky()
            .args(args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn rocky");
        let stderr = spawn_line_reader(child.stderr.take().expect("stderr"));
        let stdout = spawn_text_reader(child.stdout.take().expect("stdout"));
        let write_end = match open_rx.recv_timeout(STEP_BUDGET) {
            Ok(write_end) => write_end,
            Err(e) => {
                let _ = child.kill();
                let _ = child.wait();
                panic!(
                    "the run never opened the FIFO ({e}); the fixture put no statement in \
                     flight, so this test proves nothing.\nstderr:\n{}",
                    drain(&stderr)
                );
            }
        };
        (child, stderr, stdout, write_end)
    }
}

fn sigterm(pid: u32) {
    // SAFETY: `libc::kill` with a child pid we own and a valid signal number.
    unsafe {
        libc::kill(pid as libc::pid_t, libc::SIGTERM);
    }
}

fn sigint(pid: u32) {
    // SAFETY: as `sigterm`.
    unsafe {
        libc::kill(pid as libc::pid_t, libc::SIGINT);
    }
}

fn spawn_line_reader<R: std::io::Read + Send + 'static>(reader: R) -> Receiver<String> {
    let (tx, rx) = channel::<String>();
    thread::spawn(move || {
        for line in BufReader::new(reader).lines() {
            let Ok(line) = line else { return };
            if tx.send(line).is_err() {
                return;
            }
        }
    });
    rx
}

fn spawn_text_reader<R: std::io::Read + Send + 'static>(
    mut reader: R,
) -> thread::JoinHandle<String> {
    thread::spawn(move || {
        let mut text = String::new();
        let _ = reader.read_to_string(&mut text);
        text
    })
}

/// Wait for a stderr line containing `needle`; return the transcript so far.
fn wait_for_line(rx: &Receiver<String>, needle: &str) -> Result<String, String> {
    let start = Instant::now();
    let mut buf = String::new();
    loop {
        let remaining = STEP_BUDGET.saturating_sub(start.elapsed());
        match rx.recv_timeout(remaining) {
            Ok(line) => {
                buf.push_str(&line);
                buf.push('\n');
                if line.contains(needle) {
                    return Ok(buf);
                }
            }
            Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => return Err(buf),
        }
    }
}

fn drain(rx: &Receiver<String>) -> String {
    let mut buf = String::new();
    while let Ok(line) = rx.recv_timeout(Duration::from_secs(5)) {
        buf.push_str(&line);
        buf.push('\n');
    }
    buf
}

fn wait_exit(child: &mut Child, budget: Duration) -> Option<std::process::ExitStatus> {
    let start = Instant::now();
    loop {
        if let Ok(Some(status)) = child.try_wait() {
            return Some(status);
        }
        if start.elapsed() >= budget {
            return None;
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn table_exists(dir: &Path, table: &str) -> bool {
    let conn = duckdb::Connection::open(dir.join("fixture.duckdb")).expect("open duckdb");
    conn.query_row(
        "SELECT count(*) FROM information_schema.tables \
         WHERE table_schema = 'staging' AND table_name = ?",
        [table],
        |row| row.get::<_, i64>(0),
    )
    .expect("query information_schema")
        > 0
}

/// SIGTERM while the first model's statement is in flight: the run lets that
/// statement finish, starts no further model, records itself as interrupted,
/// releases its idempotency claim, and exits 130.
#[test]
fn sigterm_during_a_transformation_model_settles_the_run() {
    let fx = Fixture::new();
    let (mut child, stderr, stdout, mut write_end) = fx.spawn_run_blocked_on_the_fifo("k-1606");

    sigterm(child.id());
    // The listener's notice is the handshake that the token is cancelled.
    // Before #1606 nothing listened: SIGTERM killed the run right here.
    let mut transcript = match wait_for_line(&stderr, "shutdown signal received") {
        Ok(t) => t,
        Err(t) => {
            let _ = child.kill();
            let status = child.wait();
            panic!("no shutdown notice after SIGTERM (child: {status:?}).\nstderr:\n{t}");
        }
    };

    // Release the in-flight statement: one row, then EOF.
    write_end.write_all(b"1\n").expect("write fifo");
    drop(write_end);

    let status = wait_exit(&mut child, STEP_BUDGET);
    if status.is_none() {
        let _ = child.kill();
        let _ = child.wait();
    }
    transcript.push_str(&drain(&stderr));
    let stdout = stdout.join().expect("stdout reader");
    let status = status.unwrap_or_else(|| {
        panic!("rocky did not exit after its statement finished.\nstderr:\n{transcript}")
    });
    assert_eq!(
        status.code(),
        Some(130),
        "a cancelled run exits 130.\nstderr:\n{transcript}\nstdout:\n{stdout}"
    );

    // The model in flight finished; the next layer never started.
    assert!(table_exists(&fx.dir, "blocking"), "stderr:\n{transcript}");
    assert!(
        !table_exists(&fx.dir, "after"),
        "a cancelled run must start no further model.\nstderr:\n{transcript}"
    );

    // The payload says so.
    let payload: serde_json::Value = serde_json::from_str(stdout.trim())
        .unwrap_or_else(|e| panic!("stdout is one JSON RunOutput ({e}):\n{stdout}"));
    assert_eq!(
        payload["interrupted"],
        serde_json::Value::Bool(true),
        "{payload}"
    );
    assert_eq!(payload["status"], "PartialFailure", "{payload}");

    // The run record landed.
    let history = fx
        .rocky()
        .arg("history")
        .output()
        .expect("run rocky history");
    let history: serde_json::Value =
        serde_json::from_slice(&history.stdout).expect("history is JSON");
    let runs = history["runs"].as_array().expect("runs array");
    assert_eq!(
        runs.len(),
        1,
        "the interrupted run must leave its record: {history}"
    );
    assert_eq!(runs[0]["status"], "PartialFailure", "{history}");

    // The idempotency claim was released: the same key runs again instead of
    // being skipped as in flight.
    fx.make_source_a_file();
    let rerun = fx
        .rocky()
        .arg("run")
        .arg("--idempotency-key")
        .arg("k-1606")
        .output()
        .expect("rerun rocky");
    let rerun_stdout = String::from_utf8_lossy(&rerun.stdout);
    assert!(
        rerun.status.success(),
        "rerun under the same key must run.\nstdout:\n{rerun_stdout}\nstderr:\n{}",
        String::from_utf8_lossy(&rerun.stderr)
    );
    let rerun_payload: serde_json::Value =
        serde_json::from_str(rerun_stdout.trim()).expect("rerun stdout is JSON");
    assert!(
        rerun_payload
            .get("skipped_by_run_id")
            .is_none_or(serde_json::Value::is_null),
        "the interrupted run's claim must not dedup the rerun: {rerun_payload}"
    );
    assert!(table_exists(&fx.dir, "after"));
}

/// Send `second` to a run that is settling with its statement stalled, and
/// require exit 130 from that second signal, well before the settle grace
/// (5 s) could end the wait on its own.
fn second_signal_terminates(idempotency_key: &str, second: fn(u32)) {
    let fx = Fixture::new();
    let (mut child, stderr, _stdout, write_end) = fx.spawn_run_blocked_on_the_fifo(idempotency_key);

    sigterm(child.id());
    if let Err(t) = wait_for_line(&stderr, "shutdown signal received") {
        let _ = child.kill();
        let status = child.wait();
        panic!("no shutdown notice after SIGTERM (child: {status:?}).\nstderr:\n{t}");
    }
    // The statement stays in flight: `write_end` is held to the end.
    second(child.id());
    let status = wait_exit(&mut child, Duration::from_secs(3));
    if status.is_none() {
        let _ = child.kill();
        let _ = child.wait();
    }
    let transcript = drain(&stderr);
    let status = status.unwrap_or_else(|| {
        panic!("a second signal did not terminate within 3s.\nstderr:\n{transcript}")
    });
    assert_eq!(status.code(), Some(130), "stderr:\n{transcript}");
    assert!(
        transcript.contains("second signal"),
        "the exit must come from the second signal, not the grace.\nstderr:\n{transcript}"
    );
    drop(write_end);
}

/// The settle waits for the in-flight statement, so a second SIGINT must still
/// be the operator's way out, as before #1606.
#[test]
fn a_second_sigint_still_terminates_a_cancelled_run_at_once() {
    second_signal_terminates("k-1606-b", sigint);
}

/// A second SIGTERM is treated like a second SIGINT (#1606).
#[test]
fn a_second_sigterm_terminates_a_cancelled_run_at_once() {
    second_signal_terminates("k-1606-c", sigterm);
}

/// SIGTERM stays prompt on a stalled model (#1606). Before, a transformation
/// run had no listener and died at once. Now it gets the settle grace (5 s)
/// to finish its statement and settle; a statement that never returns must
/// not hold the exit past it.
#[test]
fn sigterm_on_a_stalled_model_exits_130_within_the_grace() {
    let fx = Fixture::new();
    let (mut child, stderr, _stdout, write_end) = fx.spawn_run_blocked_on_the_fifo("k-1606-d");

    let signalled = Instant::now();
    sigterm(child.id());
    // The listener exits the process itself, so `main`'s blocking-pool wait
    // does not apply: the bound is the 5 s grace. 15 s is only the give-up
    // point; the latency is pinned below.
    let status = wait_exit(&mut child, Duration::from_secs(15));
    let latency = signalled.elapsed();
    if status.is_none() {
        let _ = child.kill();
        let _ = child.wait();
    }
    let transcript = drain(&stderr);
    let status = status.unwrap_or_else(|| {
        panic!(
            "rocky run ignored SIGTERM for 15s with a stalled statement; the settle wait \
             must be bounded.\nstderr:\n{transcript}"
        )
    });
    assert_eq!(status.code(), Some(130), "stderr:\n{transcript}");
    assert!(
        transcript.contains("did not settle within"),
        "the exit must come from the bounded grace.\nstderr:\n{transcript}"
    );
    assert!(
        latency >= Duration::from_millis(4500) && latency < Duration::from_secs(9),
        "the exit must come from the 5 s grace, not before it and not much after; got \
         {latency:?}.\nstderr:\n{transcript}"
    );
    drop(write_end);
}

/// A third model, downstream of `after`, so the `--dag` test spans three
/// layers: one in flight at the signal and two that must never start.
const LAST_SQL: &str = "SELECT id FROM staging.after\n";
const LAST_TOML: &str = r#"
depends_on = ["after"]

[target]
catalog = "fixture"
schema = "staging"
"#;

/// SIGINT while the first layer of `rocky run --dag` is in flight (#1606).
///
/// Before, `main` dropped the DAG future on a signal: no settle, exit 1. Now
/// the DAG owns the run's token, and its sub-runs adopt it. The model in
/// flight finishes. Its sub-run sees the cancel, settles as interrupted and
/// writes an interrupted record. No later layer starts. Each model not started
/// is reported as interrupted, and the run exits 130 like a plain `rocky run`.
#[test]
fn sigint_during_a_dag_layer_starts_no_further_model_and_exits_130() {
    let fx = Fixture::new();
    let models = fx.dir.join("models");
    fs::write(models.join("last.sql"), LAST_SQL).expect("write model");
    fs::write(models.join("last.toml"), LAST_TOML).expect("write sidecar");

    let (mut child, stderr, stdout, mut write_end) =
        fx.spawn_blocked_on_the_fifo(&["run", "--dag"]);

    sigint(child.id());
    // The DAG's listener notice: the token is cancelled. Before #1606 the
    // outer `select!` dropped the run here and exited 1.
    let mut transcript = match wait_for_line(&stderr, "shutdown signal received") {
        Ok(t) => t,
        Err(t) => {
            let _ = child.kill();
            let status = child.wait();
            panic!("no shutdown notice after SIGINT (child: {status:?}).\nstderr:\n{t}");
        }
    };

    // Release the in-flight statement inside the grace: one row, then EOF.
    write_end.write_all(b"1\n").expect("write fifo");
    drop(write_end);

    let status = wait_exit(&mut child, STEP_BUDGET);
    if status.is_none() {
        let _ = child.kill();
        let _ = child.wait();
    }
    transcript.push_str(&drain(&stderr));
    let stdout = stdout.join().expect("stdout reader");
    let status = status.unwrap_or_else(|| {
        panic!("rocky run --dag did not exit after its statement finished.\nstderr:\n{transcript}")
    });
    assert_eq!(
        status.code(),
        Some(130),
        "a cancelled DAG run exits 130.\nstderr:\n{transcript}\nstdout:\n{stdout}"
    );
    assert!(
        !transcript.contains("did not settle within"),
        "the statement was released inside the grace; the run must settle on its own, not \
         be cut by the grace.\nstderr:\n{transcript}"
    );
    assert!(
        transcript.contains("DAG run interrupted"),
        "stderr:\n{transcript}"
    );

    // The model in flight finished; no later layer started.
    assert!(table_exists(&fx.dir, "blocking"), "stderr:\n{transcript}");
    for table in ["after", "last"] {
        assert!(
            !table_exists(&fx.dir, table),
            "a cancelled DAG must start no further model, but `{table}` was built.\n\
             stderr:\n{transcript}"
        );
    }

    // The payload is the DAG output, with every model not started reported as
    // interrupted.
    let payload: serde_json::Value = serde_json::from_str(stdout.trim())
        .unwrap_or_else(|e| panic!("stdout is one JSON DagRunOutput ({e}):\n{stdout}"));
    let nodes = payload["nodes"].as_array().expect("nodes array");
    let by_label = |label: &str| {
        nodes
            .iter()
            .find(|n| n["label"] == label)
            .unwrap_or_else(|| panic!("no node `{label}`: {payload}"))
    };
    // The node in flight built its model, but its sub-run settled as
    // interrupted, so the node did not complete. The executor has no status
    // for that, so it reads `failed` with the sub-run's interrupted error.
    let blocking = by_label("blocking");
    assert_eq!(blocking["status"], "failed", "{payload}");
    assert_eq!(
        blocking["error"], "run was interrupted by a shutdown signal",
        "{payload}"
    );
    for label in ["after", "last"] {
        let node = by_label(label);
        assert_eq!(node["status"], "skipped", "{payload}");
        assert_eq!(
            node["error"], "interrupted by a shutdown signal before it started",
            "{payload}"
        );
    }
    assert_eq!(payload["failed"], 1, "{payload}");
    assert_eq!(payload["skipped"], 2, "{payload}");

    // The sub-run in flight wrote its interrupted record; no model that never
    // started has one. `--dag` keeps no record of its own: each sub-run
    // records itself.
    let history = fx
        .rocky()
        .arg("history")
        .output()
        .expect("run rocky history");
    let history: serde_json::Value =
        serde_json::from_slice(&history.stdout).expect("history is JSON");
    let runs = history["runs"].as_array().expect("runs array");
    assert_eq!(
        runs.len(),
        1,
        "exactly the in-flight sub-run must leave a record: {history}"
    );
    assert_eq!(
        runs[0]["status"], "PartialFailure",
        "an interrupted run with progress records as PartialFailure: {history}"
    );
    let recorded = runs[0].to_string();
    assert!(recorded.contains("blocking"), "{history}");
    assert!(
        !recorded.contains("\"after\"") && !recorded.contains("\"last\""),
        "no model that never started may appear in a record: {history}"
    );
}
