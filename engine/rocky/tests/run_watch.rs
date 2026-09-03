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
/// Budget for the first run, which has nothing to calibrate against.
///
/// Deliberately far larger than the ~6 s a warm run takes: it is a max-wait,
/// not a sleep, so an oversized value costs nothing when things work and only
/// removes a flake window when they do not.
const BOOTSTRAP_BUDGET: Duration = Duration::from_secs(120);

/// A re-run must arrive within this many full-run times of the touch.
const RERUN_BUDGET_RUNS: u32 = 6;

/// Shutdown must complete within this many full-run times of the SIGINT.
///
/// This is the assertion the test actually wants to make — "winding down
/// costs less than a handful of runs" is a property of rocky; "finishes
/// within 30 seconds" was a property of the machine.
const SHUTDOWN_BUDGET_RUNS: u32 = 6;

/// Floor under every scaled budget, so a suspiciously fast calibration on an
/// idle machine cannot produce a deadline too tight to be meaningful.
const MIN_BUDGET: Duration = Duration::from_secs(30);

/// Ceiling under every scaled budget.
///
/// Without one, a first run that took most of `BOOTSTRAP_BUDGET` scales to
/// `120s x 6 = 12 minutes`, so a genuine shutdown hang would sit undiagnosed
/// for that long. Nothing terminates it — the repo's nextest profile sets no
/// `terminate-after`, and the default only *marks* a test slow. Removing a
/// flake is not worth making a real hang take twelve minutes to report.
const MAX_BUDGET: Duration = Duration::from_secs(180);

/// A budget expressed in full-run times, clamped at both ends.
fn scaled_budget(one_run: Duration, runs: u32) -> Duration {
    (one_run * runs).clamp(MIN_BUDGET, MAX_BUDGET)
}

/// The scaling property itself, asserted deterministically rather than left to
/// whether a flake reproduces.
///
/// Worth stating why this test exists in this form: CPU load alone does **not**
/// reproduce #1315. Under 16 spinners at load average ~63, the pre-fix test
/// passed 3/3 in ~9 s — the reported failure needed a full
/// `cargo nextest run --all-features` alongside it, i.e. process-spawn and I/O
/// contention rather than raw CPU pressure. So "it passes under load now" would
/// have been evidence of nothing. This asserts the mechanism directly instead.
#[test]
fn budgets_scale_past_the_old_fixed_deadline() {
    // The reported numbers: 5.8 s isolated, 32.8 s inside the full suite —
    // a 5.6x slowdown against what used to be a flat 30 s deadline.
    let idle_run = Duration::from_secs(2);
    let loaded_run = idle_run.mul_f64(5.6);

    assert!(
        scaled_budget(loaded_run, SHUTDOWN_BUDGET_RUNS) > Duration::from_secs(30),
        "a machine 5.6x slower must get more headroom than the old fixed 30s, \
         got {:?}",
        scaled_budget(loaded_run, SHUTDOWN_BUDGET_RUNS)
    );

    // And the floor holds when calibration comes back implausibly fast, so an
    // idle machine cannot tighten the deadline below the old one.
    assert_eq!(
        scaled_budget(Duration::from_millis(1), SHUTDOWN_BUDGET_RUNS),
        MIN_BUDGET,
        "the floor must keep a fast calibration from producing a tight deadline"
    );

    // The ceiling holds at the other end. Without it a first run that ate most
    // of the bootstrap budget scales to twelve minutes, and since nothing
    // terminates a slow test, a genuine hang would sit undiagnosed that long.
    assert_eq!(
        scaled_budget(BOOTSTRAP_BUDGET, SHUTDOWN_BUDGET_RUNS),
        MAX_BUDGET,
        "the ceiling must bound how long a genuine hang takes to report"
    );
    assert!(
        MAX_BUDGET > MIN_BUDGET,
        "an inverted clamp would silently pin every budget to one value"
    );

    // The scaling band is real: between the two clamps the budget tracks the
    // machine, which is the whole mechanism.
    assert_eq!(
        scaled_budget(Duration::from_secs(10), SHUTDOWN_BUDGET_RUNS),
        Duration::from_secs(60),
        "inside the band the budget must follow the calibration"
    );
}

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

    // Start the calibration clock BEFORE the spawn, not after the reader
    // threads are wired up: everything between here and the first completion
    // line is part of one run's cost, and on a loaded machine the gap can be
    // most of it. Measuring from later risks a near-zero reading precisely
    // when the machine is slow (#1315).
    let calibration_start = Instant::now();

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
    // Capture stdout per-line so we can assert the NDJSON contract: one
    // compact JSON object per iteration, never pretty-printed across many
    // lines. Without this, a regression that re-introduces pretty-print
    // would silently break any tool piping `rocky run --watch -o json`.
    let stdout_collector = spawn_collecting_reader(stdout);

    // The first run doubles as a calibration: how long one full
    // spawn-compile-execute cycle costs on this machine, right now.
    //
    // What it is a proxy for matters. It is NOT a model of shutdown work —
    // by the time SIGINT is sent the run has already completed, so shutdown
    // is loop exit plus dropping the `notify` watcher, which is not
    // proportional to run duration. It is a measure of how slow this machine
    // currently is, which is the variable that turned a 5.8 s test into a
    // 32.8 s one. Budgets scale with it for that reason and no other.
    //
    // Its own budget cannot be calibrated against anything, so it is simply
    // generous. That costs nothing on the happy path: `wait_for` returns the
    // moment the line arrives.
    //
    // `calibration_start` is taken before `spawn` deliberately — see the
    // timer's definition far above. Taking it here would be the bug this
    // whole change exists to remove: on the loaded machine where the
    // calibration matters most, the test thread can be descheduled long
    // enough for the child to finish and queue its completion line first, and
    // the measured "run" would then be near-zero, collapsing every budget to
    // the floor exactly when they most needed to grow.
    let first = wait_for(&stderr_rx, "run completed in", BOOTSTRAP_BUDGET);
    if !first.matched {
        sigterm(pid);
        let _ = child.wait();
        panic!(
            "first run never completed within {BOOTSTRAP_BUDGET:?}.\nstderr so far:\n{}",
            first.buffered
        );
    }
    let one_run = calibration_start.elapsed().max(Duration::from_millis(100));

    // Touch the watched config to provoke a re-run.
    touch(&cfg_path);

    let rerun_budget = scaled_budget(one_run, RERUN_BUDGET_RUNS);
    let second = wait_for(&stderr_rx, "run completed in", rerun_budget);
    if !second.matched {
        sigterm(pid);
        let _ = child.wait();
        panic!(
            "second run never triggered within {rerun_budget:?} after touch \
             (one run measured {one_run:?} on this machine).\n\
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

    // What this actually asserts is "shutdown does not take longer than a few
    // full runs" — a claim about rocky, not about the machine. Expressing it
    // as a fixed wall-clock deadline made it a claim about the machine: at
    // load average ~26 this test took 32.8 s against a 30 s budget while
    // taking 5.8 s in isolation, so a busy laptop and a genuine shutdown hang
    // produced the same failure message (#1315).
    //
    // `wait_with_timeout` returns the moment the child exits, so a large
    // budget costs nothing on the happy path.
    let shutdown_budget = scaled_budget(one_run, SHUTDOWN_BUDGET_RUNS);
    let exit = match wait_with_timeout(&mut child, shutdown_budget) {
        WaitOutcome::Exited(status) => status,
        // Name the cause rather than the deadline. A machine slow enough to
        // starve the test and a shutdown that genuinely never completes are
        // different bugs; the old message could only describe the second.
        WaitOutcome::StillRunning => {
            let _ = child.kill();
            let _ = child.wait();
            panic!(
                "rocky was still running {shutdown_budget:?} after SIGINT. One run \
                 measured {one_run:?} on this machine and the budget is \
                 {SHUTDOWN_BUDGET_RUNS}x that, so if `one_run` is far above a few \
                 seconds this machine is loaded rather than rocky being stuck."
            );
        }
        WaitOutcome::WaitFailed(e) => {
            let _ = child.kill();
            let _ = child.wait();
            panic!("could not determine whether rocky exited after SIGINT: {e}");
        }
    };

    assert!(
        exit.success(),
        "expected clean exit (status 0) after SIGINT; got {exit:?}"
    );

    // NDJSON contract: every non-empty stdout line must be a valid JSON
    // object on its own (no pretty-printing across multiple lines), and
    // we expect at least two — one per iteration that completed before
    // SIGINT.
    let stdout_lines = stdout_collector
        .join()
        .expect("stdout collector thread panicked");
    let json_lines: Vec<&str> = stdout_lines
        .iter()
        .map(String::as_str)
        .filter(|s| !s.trim().is_empty())
        .collect();
    assert!(
        json_lines.len() >= 2,
        "expected ≥2 NDJSON lines on stdout (one per iteration); got {} lines:\n{}",
        json_lines.len(),
        stdout_lines.join("\n")
    );
    for (i, line) in json_lines.iter().enumerate() {
        let parsed: serde_json::Value = serde_json::from_str(line).unwrap_or_else(|e| {
            panic!(
                "stdout line {} is not valid single-line JSON ({e}):\n{}",
                i, line
            );
        });
        let cmd = parsed.get("command").and_then(|c| c.as_str());
        assert_eq!(
            cmd,
            Some("run"),
            "expected `command: \"run\"` on every iteration line; got {cmd:?} on line {i}"
        );
    }
}

/// Wait for a line containing `needle` to arrive on `rx`, up to `budget`.
///
/// Returns the buffered transcript either way, so a failing caller can report
/// what the child actually said instead of only that a deadline passed.
fn wait_for(rx: &Receiver<String>, needle: &str, budget: Duration) -> WaitResult {
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
            Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => {
                // Drain whatever is already queued before declaring a
                // timeout. The child may have written the line while this
                // thread was descheduled — reporting "never arrived" for a
                // line sitting in the channel is the same conflation of
                // scheduling latency with product behaviour that #1315 is
                // about, one level down.
                while let Ok(line) = rx.try_recv() {
                    buf.push_str(&line);
                    buf.push('\n');
                    if line.contains(needle) {
                        return WaitResult {
                            matched: true,
                            buffered: buf,
                        };
                    }
                }
                return WaitResult {
                    matched: false,
                    buffered: buf,
                };
            }
        }
    }
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

/// Spawn a background thread that collects the reader's lines into a
/// `Vec<String>` for later inspection. Used to assert the NDJSON
/// contract on stdout — every iteration of `rocky run --watch -o json`
/// must emit one compact JSON object per line.
fn spawn_collecting_reader<R: std::io::Read + Send + 'static>(
    reader: R,
) -> thread::JoinHandle<Vec<String>> {
    thread::spawn(move || {
        let buf = BufReader::new(reader);
        let mut lines = Vec::new();
        for line in buf.lines() {
            match line {
                Ok(s) => lines.push(s),
                Err(_) => break,
            }
        }
        lines
    })
}

/// Wait up to `budget` for a child process to exit, polling every 50 ms.
/// Why a process wait ended, kept distinct because #1315 is precisely about
/// a message that could not tell these apart.
enum WaitOutcome {
    Exited(std::process::ExitStatus),
    /// The budget elapsed with the child still running.
    StillRunning,
    /// `try_wait` itself failed — neither evidence of a hang nor of an exit.
    WaitFailed(std::io::Error),
}

fn wait_with_timeout(child: &mut std::process::Child, budget: Duration) -> WaitOutcome {
    let start = Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return WaitOutcome::Exited(status),
            Ok(None) => {}
            Err(e) => return WaitOutcome::WaitFailed(e),
        }
        if start.elapsed() >= budget {
            // One last poll on the way out. The loop above sleeps in 50 ms
            // steps, so without this a child that exits between the final
            // poll and the deadline is reported as still running — a false
            // "hang" produced by the polling interval rather than by rocky.
            return match child.try_wait() {
                Ok(Some(status)) => WaitOutcome::Exited(status),
                Ok(None) => WaitOutcome::StillRunning,
                Err(e) => WaitOutcome::WaitFailed(e),
            };
        }
        std::thread::sleep(Duration::from_millis(50).min(budget.saturating_sub(start.elapsed())));
    }
}

/// Cross-editor "touch" — opens the file, writes its existing bytes, and
/// fsyncs. A bare `utimes` doesn't reliably wake `notify::Modify` on
/// macOS FSEvents; rewriting the file always does.
fn touch(p: &Path) {
    let bytes = fs::read(p).expect("read for touch");
    fs::write(p, bytes).expect("write for touch");
}

/// SIGTERM must stop the watcher.
///
/// Before #1405 the steady-state `select!` had no SIGTERM arm at all, so once
/// the first run had replaced the default disposition the watcher could not be
/// killed by `timeout`, a CI job cancellation, or a container eviction — only
/// by SIGKILL. That is deterministic, unlike the SIGINT race in the test
/// above, which is why this is the test that pins the fix.
#[test]
fn run_watch_exits_on_sigterm() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();

    let db_path = dir.join("fixture.duckdb");
    {
        let conn = duckdb::Connection::open(&db_path).expect("open duckdb");
        conn.execute_batch(SEED_SQL).expect("seed sql");
    }
    fs::write(dir.join("rocky.toml"), ROCKY_TOML).expect("write rocky.toml");

    // Clock starts BEFORE the spawn: everything up to the first completion
    // line is part of one run's cost, and on a loaded machine the gap is most
    // of it (#1315). The shutdown deadline is then expressed in run-times, not
    // seconds — a flat wall-clock deadline asserts a property of the machine,
    // which is exactly how the first version of this test failed on CI while
    // passing locally.
    let calibration_start = Instant::now();

    let mut child = Command::new(env!("CARGO_BIN_EXE_rocky"))
        .arg("-c")
        .arg(dir.join("rocky.toml"))
        .arg("run")
        .arg("--watch")
        .current_dir(dir)
        .env("RUST_LOG", "error")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn rocky");

    let pid = child.id();
    let stderr = child.stderr.take().expect("stderr piped");
    let stderr_rx = spawn_line_reader(stderr);

    // Wait for the FIRST RUN TO FINISH, not merely for the watcher to start.
    //
    // This distinction is the whole test. Before the first run, SIGTERM still
    // has its default disposition and kills the process no matter what the
    // watch loop does — so signalling early passes with or without the fix
    // and proves nothing. The inner run installs a SIGTERM handler of its
    // own; when the run ends that handler is dropped, but tokio does NOT
    // restore the default disposition. Only from that moment does a missing
    // arm in the watch loop mean SIGTERM is swallowed.
    let mut seen = String::new();
    let mut ready = false;
    let bootstrap_deadline = Instant::now() + BOOTSTRAP_BUDGET;
    while Instant::now() < bootstrap_deadline {
        match stderr_rx.recv_timeout(Duration::from_millis(500)) {
            Ok(line) => {
                seen.push_str(&line);
                seen.push('\n');
                if line.contains("[watch] run completed") || line.contains("[watch] run failed") {
                    ready = true;
                    break;
                }
            }
            Err(RecvTimeoutError::Timeout) => continue,
            Err(RecvTimeoutError::Disconnected) => break,
        }
    }
    if !ready {
        let _ = child.kill();
        let _ = child.wait();
    }
    assert!(
        ready,
        "watcher never completed a first run; stderr so far:\n{seen}"
    );
    let one_run = calibration_start.elapsed();

    sigterm(pid);

    // Poll for exit rather than blocking: a hung watcher must fail the test,
    // not hang the suite.
    let budget = scaled_budget(one_run, SHUTDOWN_BUDGET_RUNS);
    let deadline = Instant::now() + budget;
    let mut exited = None;
    while Instant::now() < deadline {
        match child.try_wait().expect("try_wait") {
            Some(status) => {
                exited = Some(status);
                break;
            }
            None => std::thread::sleep(Duration::from_millis(50)),
        }
    }

    // Reap on every path: `try_wait` only reaps when it returns `Some`, so a
    // watcher that ignored the signal must be killed AND waited on, or the
    // test leaves a zombie behind (clippy::zombie_processes).
    if exited.is_none() {
        let _ = child.kill();
    }
    let _ = child.wait();
    assert!(
        exited.is_some(),
        "rocky run --watch ignored SIGTERM for {budget:?} (one run measured \
         {one_run:?}) — before #1405 the watch select had no SIGTERM arm, so \
         only SIGKILL could stop it. If one_run is far above a few seconds \
         this machine is loaded rather than rocky being stuck."
    );
}

/// Fixture for the mid-iteration test: a transformation pipeline with exactly
/// one model and `concurrency = 1`, so at most one warehouse statement is ever
/// in flight and "the statement" is unambiguous.
const BLOCKING_ROCKY_TOML: &str = r#"
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

/// The model reads a plain DuckDB view. The `read_csv(...)` call that makes it
/// blockable lives inside the *view*, not here, because Rocky parses model SQL
/// with sqlparser, which rejects DuckDB's struct-literal argument
/// (`columns = {'id': 'INTEGER'}`) with "Expected: an expression, found: {".
const BLOCKING_MODEL_SQL: &str = "SELECT id FROM main.blocking_source\n";

const BLOCKING_MODEL_TOML: &str = r#"
[target]
catalog = "fixture"
schema = "staging"
"#;

/// Name of the file the view reads. Deliberately `.csv`: `run_watch.rs`'s
/// `event_path_is_relevant` only forwards `.sql` / `.rocky` / `.toml`, so
/// swapping this file for a FIFO does not itself trigger a re-run. The test
/// keeps the touch of `rocky.toml` as the single, deterministic trigger.
const BLOCKING_SOURCE_FILE: &str = "blocking_source.csv";

/// Seed SQL for the blocking fixture, with an absolute path to the source file
/// so the view resolves identically from the test process (which creates it)
/// and from the `rocky` child (whose cwd is the fixture dir).
fn blocking_seed_sql(source_path: &Path) -> String {
    format!(
        "CREATE SCHEMA IF NOT EXISTS staging;\n\
         CREATE OR REPLACE VIEW main.blocking_source AS\n\
         SELECT id FROM read_csv('{}', columns = {{'id': 'INTEGER'}}, header = false);\n",
        source_path.display()
    )
}

/// Create a FIFO at `path`.
///
/// Unix-only, like the rest of this file. A FIFO is what makes the test below
/// deterministic: opening one for writing blocks until a reader opens it, which
/// is an exact handshake for "the child has the file open inside its statement".
fn mkfifo(path: &Path) {
    use std::os::unix::ffi::OsStrExt;
    let c_path = std::ffi::CString::new(path.as_os_str().as_bytes())
        .unwrap_or_else(|e| panic!("fixture path {} is not a C string: {e}", path.display()));
    // SAFETY: `libc::mkfifo` with a valid NUL-terminated path and a permission
    // mask only touches the filesystem; failure is reported through the return
    // code, which is checked below.
    let rc = unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) };
    assert_eq!(
        rc,
        0,
        "mkfifo({}) failed: {}",
        path.display(),
        std::io::Error::last_os_error()
    );
}

/// A signal landing **during** an iteration must still stop the process (#1590).
///
/// Both tests above signal *between* iterations —
/// `run_watch_reruns_on_file_change_and_exits_clean_on_sigint` says so in its
/// own comment ("by the time SIGINT is sent the run has already completed"),
/// and `run_watch_exits_on_sigterm` waits for the first run to finish first. So
/// the covered shape was:
///
/// ```text
///   iteration ends ──▶ signal ──▶ select! fires ──▶ exit          covered
///   signal mid-run ──▶ select! drops the iteration ──▶ ???        this test
/// ```
///
/// Dropping the iteration future is not the end of the work. The dropped future
/// was awaiting a `spawn_blocking` task, and a blocking task cannot be
/// cancelled: it keeps running, and `Runtime::drop` waits for every one of them
/// with no bound. Against the one-row fixtures above that wait is milliseconds,
/// which is why nothing surfaced.
///
/// # Why a FIFO and not a slow query
///
/// A "deliberately slow" statement would make the assertion a timing guess: it
/// could be slow to *start* rather than in flight when the signal lands, and a
/// loaded CI box shifts every number. A FIFO removes the guess entirely.
/// `open(O_WRONLY)` on a FIFO blocks until a reader opens it, so the moment the
/// test's writer handle materialises, DuckDB has the file open inside
/// `execute_statement`. The test then holds that write end open, so DuckDB's
/// `read()` never sees EOF and the statement stays in flight for as long as the
/// assertion needs. Nothing about the timing is load-dependent.
///
/// # What is asserted, and what deliberately is not
///
/// The assertion is *promptness*, within the same run-scaled budget the sibling
/// tests use. The exit code is asserted only as one of two known values. A
/// mid-iteration drop also leaks the run's `RemoteStateSession` (its `Drop`
/// tripwire at `rocky-core/src/state_sync.rs`), which is a `debug_assert!` — a
/// panic here, a `warn!` in the shipped release binary. So requiring `success()`
/// would assert the build profile rather than the shutdown, while accepting any
/// code would bless an unrelated failure. The release binary was measured
/// separately at exit 0.
#[test]
fn run_watch_exits_on_a_signal_during_an_iteration() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();

    // The source file starts as an ordinary one-row CSV so the first run — the
    // calibration run — completes normally and fast.
    let source_path = dir.join(BLOCKING_SOURCE_FILE);
    fs::write(&source_path, "1\n").expect("write source csv");

    {
        let conn = duckdb::Connection::open(dir.join("fixture.duckdb")).expect("open duckdb");
        conn.execute_batch(&blocking_seed_sql(&source_path))
            .expect("seed sql");
    }

    let cfg_path = dir.join("rocky.toml");
    fs::write(&cfg_path, BLOCKING_ROCKY_TOML).expect("write rocky.toml");
    let models_dir = dir.join("models");
    fs::create_dir(&models_dir).expect("mkdir models");
    fs::write(models_dir.join("blocking.sql"), BLOCKING_MODEL_SQL).expect("write model sql");
    fs::write(models_dir.join("blocking.toml"), BLOCKING_MODEL_TOML).expect("write model toml");

    // Clock starts before the spawn, for the reason the sibling tests spell
    // out: everything up to the first completion line is one run's cost.
    let calibration_start = Instant::now();

    let mut child = Command::new(env!("CARGO_BIN_EXE_rocky"))
        .arg("-c")
        .arg(&cfg_path)
        .arg("run")
        .arg("--watch")
        .current_dir(dir)
        .env("RUST_LOG", "error")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn rocky");

    let pid = child.id();
    let stderr = child.stderr.take().expect("stderr piped");
    let stderr_rx = spawn_line_reader(stderr);
    // Drain stdout too: an undrained pipe would eventually block the child.
    let stdout = child.stdout.take().expect("stdout piped");
    let _stdout_drain = spawn_collecting_reader(stdout);

    let first = wait_for(&stderr_rx, "run completed in", BOOTSTRAP_BUDGET);
    if !first.matched {
        let _ = child.kill();
        let _ = child.wait();
        panic!(
            "first run never completed within {BOOTSTRAP_BUDGET:?}.\nstderr so far:\n{}",
            first.buffered
        );
    }
    let one_run = calibration_start.elapsed().max(Duration::from_millis(100));
    let mut transcript = first.buffered;

    // Swap the one-row CSV for a FIFO. Nothing reads it until the next
    // iteration, and a `.csv` change is not itself a re-run trigger.
    fs::remove_file(&source_path).expect("remove source csv");
    mkfifo(&source_path);

    // The handshake. This thread blocks in `open(O_WRONLY)` until the child
    // opens the FIFO for reading; it then hands the write end to the test
    // thread, which owns it for the rest of the test. Sending the handle rather
    // than a bare notification is what keeps it open — if it were dropped, the
    // child would see EOF, the statement would finish, and the signal would
    // land between iterations, quietly re-testing the already-covered path.
    let (open_tx, open_rx) = channel::<fs::File>();
    let fifo_path = source_path.clone();
    thread::spawn(move || {
        if let Ok(write_end) = fs::OpenOptions::new().write(true).open(&fifo_path) {
            let _ = open_tx.send(write_end);
        }
    });

    // Single deterministic trigger for the re-run.
    touch(&cfg_path);

    let handshake_budget = scaled_budget(one_run, RERUN_BUDGET_RUNS);
    let write_end = match open_rx.recv_timeout(handshake_budget) {
        Ok(write_end) => write_end,
        // Named as a fixture failure on purpose. A fixture that never reaches
        // the statement and a shutdown that never completes are different bugs,
        // and #1315 is exactly about a message that could not tell two causes
        // apart.
        Err(e) => {
            let _ = child.kill();
            let _ = child.wait();
            panic!(
                "the re-run never opened the FIFO within {handshake_budget:?} ({e}) — the \
                 FIXTURE did not put a statement in flight, so this run proves nothing about \
                 shutdown. One run measured {one_run:?}.\nstderr so far:\n{transcript}"
            );
        }
    };

    // From here the child is provably inside `WarehouseAdapter::execute_statement`,
    // blocked on a `read()` that will never return while `write_end` lives.
    //
    // SIGTERM rather than SIGINT: the two share one `select!` shape in
    // `run_watch.rs`, and SIGTERM is the deterministic one — see
    // `run_watch_exits_on_sigterm`.
    sigterm(pid);
    let signalled = Instant::now();

    let shutdown_budget = scaled_budget(one_run, SHUTDOWN_BUDGET_RUNS);
    let outcome = wait_with_timeout(&mut child, shutdown_budget);
    let exit_latency = signalled.elapsed();

    // Reap first, so the child's stderr pipe is at EOF and the reader thread is
    // finishing, then drain to channel disconnect. A fixed short drain would
    // make a descheduled reader look like a missing line — the same conflation
    // of scheduling latency with product behaviour that #1315 is about. The
    // timeout is only a guard against blocking forever if the pipe somehow
    // stays open; the normal case returns on `Disconnected`.
    if !matches!(outcome, WaitOutcome::Exited(_)) {
        let _ = child.kill();
        let _ = child.wait();
    }
    while let Ok(line) = stderr_rx.recv_timeout(Duration::from_secs(5)) {
        transcript.push_str(&line);
        transcript.push('\n');
    }

    let status = match outcome {
        WaitOutcome::Exited(status) => status,
        WaitOutcome::StillRunning => {
            panic!(
                "rocky was still running {shutdown_budget:?} after SIGTERM landed with a \
                 DuckDB statement in flight (#1590). The statement was in flight by \
                 construction — the FIFO write end opened, which only happens once the child \
                 has it open for reading — so this is the unbounded `Runtime::drop` wait on \
                 the blocking pool, not a slow machine (one run measured \
                 {one_run:?}).\nstderr:\n{transcript}"
            );
        }
        WaitOutcome::WaitFailed(e) => {
            panic!("could not determine whether rocky exited after SIGTERM: {e}");
        }
    };

    // Two codes, and only two. `0` is the documented contract and what the
    // release binary returns. `101` is this build profile's `RemoteStateSession`
    // tripwire (see the doc comment): a `debug_assert!` that fires because the
    // dropped iteration never finalized its session. Accepting any exit code —
    // `status.code().is_some()` — would also bless a config error or an adapter
    // failure, which would say nothing about shutdown.
    let code = status.code();
    assert!(
        matches!(code, Some(0 | 101)),
        "expected exit 0 (the documented signal contract) or 101 (this profile's \
         RemoteStateSession debug tripwire); got {status:?}.\nstderr:\n{transcript}"
    );

    // The signal arm is what ended the process.
    assert!(
        transcript.contains("[watch] stopped"),
        "expected the watch loop's '[watch] stopped' notice.\nstderr:\n{transcript}"
    );

    // Positive evidence that the signal landed *during* an iteration and not
    // between two: the re-run started ("detected change") and never finished,
    // so exactly one "run completed" line exists — the calibration run's.
    assert!(
        transcript.contains("detected change"),
        "expected the re-run to have been triggered.\nstderr:\n{transcript}"
    );
    assert_eq!(
        transcript.matches("run completed in").count(),
        1,
        "expected exactly one completed run (the calibration run); a second one means the \
         iteration finished before the signal landed, which is the path the sibling tests \
         already cover.\nstderr:\n{transcript}"
    );

    // Held open until here on purpose: dropping it earlier would release the
    // statement and invalidate everything above.
    drop(write_end);

    // Reported rather than asserted: a bound this test can see but not pin,
    // since the production grace period is a deliberate budget, not a promise.
    eprintln!(
        "run_watch_exits_on_a_signal_during_an_iteration: exited {exit_latency:?} after \
         SIGTERM (budget {shutdown_budget:?}, one run {one_run:?})"
    );
}
