//! Integration test for `rocky run --watch`.
//!
//! Spawns the `rocky` binary against a tiny DuckDB replication fixture,
//! waits for the first run to complete, touches the watched config to
//! trigger a re-run, then sends SIGINT and asserts a clean exit.
//!
//! Unix-only: SIGINT delivery from a Rust test to a child process is
//! platform-specific; the watch wrapper itself is cross-platform.

#![cfg(unix)]

use std::fs;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::mpsc::{Receiver, RecvTimeoutError, channel};
use std::thread;
use std::time::{Duration, Instant};

/// Minimal pipeline: a single replication target loaded from a 1-row
/// `raw__demo` schema. No transformation models — keeps the test fast
/// and avoids any compile-step flakiness.
const ROCKY_TOML: &str = r#"
[adapter]
type = "duckdb"
path = "fixture.duckdb"

[pipeline.demo]
strategy = "full_refresh"
timestamp_column = "_updated_at"

[pipeline.demo.source.discovery]
adapter = "default"

[pipeline.demo.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.demo.target]
# DuckDB's "catalog" is the .duckdb file's stem; matching it here
# avoids needing a CREATE CATALOG, which DuckDB doesn't support.
catalog_template = "fixture"
schema_template = "stg__{source}"

[pipeline.demo.target.governance]
auto_create_schemas = true

[pipeline.demo.execution]
concurrency = 1
"#;

const SEED_SQL: &str = r#"
CREATE SCHEMA IF NOT EXISTS raw__demo;
CREATE TABLE IF NOT EXISTS raw__demo.events AS
SELECT 1 AS id, NOW() AS _updated_at;
"#;

/// Send SIGINT to a child by pid. Unix-only.
fn sigint(pid: u32) {
    // SAFETY: `libc::kill` with a valid pid + SIGINT is sound; the worst
    // case is ESRCH if the child already exited, which we ignore.
    unsafe {
        libc::kill(pid as libc::pid_t, libc::SIGINT);
    }
}

/// Best-effort terminator — used in test cleanup if a step asserts before
/// we get to the SIGINT step.
fn sigterm(pid: u32) {
    unsafe {
        libc::kill(pid as libc::pid_t, libc::SIGTERM);
    }
}

#[test]
fn run_watch_reruns_on_file_change_and_exits_clean_on_sigint() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();

    // Seed DuckDB fixture in-process (the binary is already linked
    // against duckdb via rocky-duckdb; this dev-dep just shares it).
    let db_path = dir.join("fixture.duckdb");
    {
        let conn = duckdb::Connection::open(&db_path).expect("open duckdb");
        conn.execute_batch(SEED_SQL).expect("seed sql");
    }

    let cfg_path = dir.join("rocky.toml");
    fs::write(&cfg_path, ROCKY_TOML).expect("write rocky.toml");

    let exe = env!("CARGO_BIN_EXE_rocky");

    let mut child = Command::new(exe)
        .arg("-c")
        .arg(&cfg_path)
        .arg("run")
        .arg("--watch")
        .current_dir(dir)
        // Quiet the JSON-formatted tracing logs that would otherwise
        // dominate stderr. The test asserts on `[watch] ...` lines that
        // `eprintln!` always emits regardless of `RUST_LOG`.
        .env("RUST_LOG", "error")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn rocky");

    let pid = child.id();

    // Stream stderr line-by-line into a channel so the main test thread
    // can poll with a deadline. `BufRead::read_line` blocks indefinitely
    // when the child is idle between runs, so the test thread can't
    // call it directly with a timeout.
    let stderr = child.stderr.take().expect("stderr piped");
    let stdout = child.stdout.take().expect("stdout piped");
    let stderr_rx = spawn_line_reader(stderr);
    // Drain stdout into a sink thread so the child's stdout pipe doesn't
    // block on a full buffer.
    drain_to_sink(stdout);

    // Helper: wait for a line containing `needle` to arrive on stderr,
    // up to `budget`. Returns the buffered transcript on failure for
    // diagnostics.
    let wait_for = |rx: &Receiver<String>, needle: &str, budget: Duration| -> WaitResult {
        let start = Instant::now();
        let mut buf = String::new();
        loop {
            let remaining = budget.saturating_sub(start.elapsed());
            if remaining.is_zero() {
                return WaitResult {
                    matched: false,
                    buffered: buf,
                };
            }
            match rx.recv_timeout(remaining) {
                Ok(line) => {
                    buf.push_str(&line);
                    buf.push('\n');
                    if line.contains(needle) {
                        return WaitResult {
                            matched: true,
                            buffered: buf,
                        };
                    }
                }
                Err(RecvTimeoutError::Timeout) => {
                    return WaitResult {
                        matched: false,
                        buffered: buf,
                    };
                }
                Err(RecvTimeoutError::Disconnected) => {
                    return WaitResult {
                        matched: false,
                        buffered: buf,
                    };
                }
            }
        }
    };

    let first = wait_for(&stderr_rx, "run completed in", Duration::from_secs(20));
    if !first.matched {
        sigterm(pid);
        let _ = child.wait();
        panic!(
            "first run never completed within 20s.\nstderr so far:\n{}",
            first.buffered
        );
    }

    // Touch the watched config to provoke a re-run.
    touch(&cfg_path);

    let second = wait_for(&stderr_rx, "run completed in", Duration::from_secs(8));
    if !second.matched {
        sigterm(pid);
        let _ = child.wait();
        panic!(
            "second run never triggered within 8s after touch.\n\
             stderr since first run:\n{}",
            second.buffered
        );
    }
    assert!(
        second.buffered.contains("detected change"),
        "expected 'detected change' notice in stderr; got:\n{}",
        second.buffered
    );

    // Clean shutdown: SIGINT, then await exit.
    sigint(pid);

    let exit = wait_with_timeout(&mut child, Duration::from_secs(8))
        .expect("rocky did not exit within 8s of SIGINT");

    assert!(
        exit.success(),
        "expected clean exit (status 0) after SIGINT; got {exit:?}"
    );
}

struct WaitResult {
    matched: bool,
    buffered: String,
}

/// Spawn a background thread that reads `reader` line-by-line and pushes
/// each line onto an `mpsc` channel. Returns the receiver.
fn spawn_line_reader<R: std::io::Read + Send + 'static>(reader: R) -> Receiver<String> {
    let (tx, rx) = channel::<String>();
    thread::spawn(move || {
        let buf = BufReader::new(reader);
        for line in buf.lines() {
            match line {
                Ok(s) => {
                    if tx.send(s).is_err() {
                        // Receiver dropped — the test is shutting down.
                        return;
                    }
                }
                Err(_) => return,
            }
        }
    });
    rx
}

/// Drain a Read source on a background thread so the child's pipe never
/// fills up. The test asserts on stderr; stdout content isn't checked
/// directly (it's parseable NDJSON when `--output json` is passed; the
/// stderr summaries are the primary signal here).
fn drain_to_sink<R: std::io::Read + Send + 'static>(reader: R) {
    thread::spawn(move || {
        let mut r = reader;
        let mut buf = [0u8; 4096];
        loop {
            match std::io::Read::read(&mut r, &mut buf) {
                Ok(0) | Err(_) => return,
                Ok(_) => continue,
            }
        }
    });
}

/// Wait up to `budget` for a child process to exit, polling every 50 ms.
fn wait_with_timeout(
    child: &mut std::process::Child,
    budget: Duration,
) -> Option<std::process::ExitStatus> {
    let start = Instant::now();
    while start.elapsed() < budget {
        match child.try_wait() {
            Ok(Some(status)) => return Some(status),
            Ok(None) => std::thread::sleep(Duration::from_millis(50)),
            Err(_) => return None,
        }
    }
    None
}

/// Cross-editor "touch" — opens the file, writes its existing bytes, and
/// fsyncs. A bare `utimes` doesn't reliably wake `notify::Modify` on
/// macOS FSEvents; rewriting the file always does.
fn touch(p: &Path) {
    let bytes = fs::read(p).expect("read for touch");
    fs::write(p, bytes).expect("write for touch");
}
