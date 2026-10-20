//! A Ctrl-Break during `rocky run` on Windows settles the run (#2440).
//!
//! Before #2440 the Windows listener awaited only Ctrl-C, so a Ctrl-Break, a
//! console close, logoff or shutdown ended the run with no settlement. This
//! test sends a real `CTRL_BREAK_EVENT` to a child `rocky run` and checks what
//! the run left behind, as `run_cancel.rs` does with SIGTERM on Unix.
//!
//! The stall is an `on_after_model_run` hook: it writes a marker, then waits
//! about 2 s, then writes a second marker. The test sends Ctrl-Break when the
//! first marker appears. The run checks its cancel token before the next
//! layer, so it must stop there, inside the 5 s settle grace.
//!
//! The hook writes its markers by bare file name into its working
//! directory, the fixture directory it inherits from rocky. It has no double
//! quotes: `cmd /C` receives the command through Rust's argument quoting,
//! which turns `"` into `\"`, and `cmd` does not read that back.
//!
//! The hook's `cmd` and `ping` share rocky's process group, so the break
//! reaches them too. `ping` reports and continues on Ctrl-Break. If the break
//! ends the hook early anyway, the run races its listener to the next layer;
//! the second marker is missing then, and the failure messages say so.
//!
//! `#[ignore]`: it needs a console to send the event through. The weekly
//! `windows-settle` job in `engine-weekly.yml` runs it with `--ignored`.

#![cfg(windows)]

use std::fs;
use std::io::{BufRead, BufReader, Read};
use std::os::windows::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{Receiver, channel};
use std::thread;
use std::time::{Duration, Instant};

/// Max-wait for each step. Generous on purpose: a debug build on a loaded CI
/// box is slow to start.
const STEP_BUDGET: Duration = Duration::from_secs(120);

const CTRL_BREAK_EVENT: u32 = 1;
const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;

#[link(name = "kernel32")]
unsafe extern "system" {
    fn GenerateConsoleCtrlEvent(ctrl_event: u32, process_group_id: u32) -> i32;
    fn AllocConsole() -> i32;
}

/// Two models in two layers: `first`, then `after`, which depends on it.
fn rocky_toml() -> String {
    format!(
        r#"
[adapter]
type = "duckdb"
path = "fixture.duckdb"

[pipeline.transform]
type = "transformation"

[pipeline.transform.target]
adapter = "default"

[pipeline.transform.execution]
concurrency = 1

[hook.on_after_model_run]
command = 'echo fired>{MARKER} & ping -n 3 127.0.0.1 > nul & echo done>{DONE}'
timeout_ms = 60000
"#
    )
}

/// File names the hook writes in the fixture directory.
const MARKER: &str = "hook-fired.txt";
const DONE: &str = "hook-done.txt";

const FIRST_SQL: &str = "SELECT 1 AS id\n";
const AFTER_SQL: &str = "SELECT id FROM staging.first\n";
const FIRST_TOML: &str = r#"
[target]
catalog = "fixture"
schema = "staging"
"#;
const AFTER_TOML: &str = r#"
depends_on = ["first"]

[target]
catalog = "fixture"
schema = "staging"
"#;

struct Fixture {
    _tmp: tempfile::TempDir,
    dir: PathBuf,
    marker: PathBuf,
    /// Written when the hook's stall ends. Missing if the break ended it.
    done: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let tmp = tempfile::tempdir().expect("tempdir");
        let dir = tmp.path().to_path_buf();
        let marker = dir.join(MARKER);
        let done = dir.join(DONE);
        {
            let conn = duckdb::Connection::open(dir.join("fixture.duckdb")).expect("open duckdb");
            conn.execute_batch("CREATE SCHEMA IF NOT EXISTS staging;")
                .expect("seed sql");
        }
        fs::write(dir.join("rocky.toml"), rocky_toml()).expect("write rocky.toml");
        let models = dir.join("models");
        fs::create_dir(&models).expect("mkdir models");
        fs::write(models.join("first.sql"), FIRST_SQL).expect("write model");
        fs::write(models.join("first.toml"), FIRST_TOML).expect("write sidecar");
        fs::write(models.join("after.sql"), AFTER_SQL).expect("write model");
        fs::write(models.join("after.toml"), AFTER_TOML).expect("write sidecar");
        Fixture {
            _tmp: tmp,
            dir,
            marker,
            done,
        }
    }

    fn rocky(&self) -> Command {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_rocky"));
        cmd.arg("-c")
            .arg(self.dir.join("rocky.toml"))
            .arg("--state-path")
            .arg(self.dir.join("state.redb"))
            .arg("--output")
            .arg("json")
            .current_dir(&self.dir)
            // `warn`, so a hook that fails shows in the transcript.
            .env("RUST_LOG", "warn");
        cmd
    }
}

fn spawn_line_reader<R: Read + Send + 'static>(reader: R) -> Receiver<String> {
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

fn spawn_text_reader<R: Read + Send + 'static>(mut reader: R) -> thread::JoinHandle<String> {
    thread::spawn(move || {
        let mut text = String::new();
        let _ = reader.read_to_string(&mut text);
        text
    })
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

/// Ctrl-Break while the first model's hook runs: the run starts no further
/// model, records itself as interrupted, and exits 130.
#[test]
#[ignore = "needs a console; run by the weekly windows-settle job with --ignored"]
fn ctrl_break_during_a_transformation_run_settles_the_run() {
    // A child receives a console control event only through a console it
    // shares with the sender. A CI step may start this test with none; then
    // this gives it one, and the child inherits it. With a console already
    // attached the call fails, which is fine.
    // SAFETY: `AllocConsole` takes no arguments and only attaches a console.
    let _ = unsafe { AllocConsole() };

    let fx = Fixture::new();
    // Its own process group, so the event reaches the child and not this test.
    let mut child = fx
        .rocky()
        .args(["run", "--idempotency-key", "k-2440"])
        .creation_flags(CREATE_NEW_PROCESS_GROUP)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn rocky");
    let stderr = spawn_line_reader(child.stderr.take().expect("stderr"));
    let stdout = spawn_text_reader(child.stdout.take().expect("stdout"));

    let start = Instant::now();
    while !fx.marker.exists() {
        if start.elapsed() >= STEP_BUDGET || matches!(child.try_wait(), Ok(Some(_))) {
            let _ = child.kill();
            let status = child.wait();
            panic!(
                "the hook never fired (child: {status:?}); the run never stalled, so this test \
                 proves nothing.\nstderr:\n{}",
                drain(&stderr)
            );
        }
        thread::sleep(Duration::from_millis(20));
    }

    // SAFETY: `GenerateConsoleCtrlEvent` with a constant event and the pid of
    // a child we own, which leads its own process group.
    let sent = unsafe { GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT, child.id()) };
    if sent == 0 {
        let error = std::io::Error::last_os_error();
        let _ = child.kill();
        let _ = child.wait();
        panic!("GenerateConsoleCtrlEvent failed: {error}");
    }

    let status = wait_exit(&mut child, STEP_BUDGET);
    if status.is_none() {
        let _ = child.kill();
        let _ = child.wait();
    }
    let mut transcript = drain(&stderr);
    transcript.push_str(&format!(
        "[test] the hook's stall ran to the end: {}\n",
        fx.done.exists()
    ));
    let stdout = stdout.join().expect("stdout reader");
    let status = status.unwrap_or_else(|| panic!("rocky did not exit.\nstderr:\n{transcript}"));
    assert!(
        transcript.contains("shutdown signal received"),
        "before #2440 nothing listened for Ctrl-Break.\nstatus: {status:?}\nstderr:\n{transcript}"
    );
    assert_eq!(
        status.code(),
        Some(130),
        "a cancelled run exits 130.\nstderr:\n{transcript}\nstdout:\n{stdout}"
    );

    assert!(table_exists(&fx.dir, "first"), "stderr:\n{transcript}");
    assert!(
        !table_exists(&fx.dir, "after"),
        "a cancelled run must start no further model.\nstderr:\n{transcript}"
    );

    // An empty stdout with exit 130 means the listener's grace ran out and it
    // exited without the settle.
    let payload: serde_json::Value = serde_json::from_str(stdout.trim()).unwrap_or_else(|e| {
        panic!(
            "stdout is one JSON RunOutput ({e}); empty means the run did not settle within \
             the grace:\n{stdout}\nstderr:\n{transcript}"
        )
    });
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
    let rerun = fx
        .rocky()
        .args(["run", "--idempotency-key", "k-2440"])
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
