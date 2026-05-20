//! JSONL trace persistence (Arc 4 span retention).
//!
//! When [`crate::tracing_setup::init_tracing`] installs the JSONL layer
//! it writes one file per process under `.rocky/traces/{ts}-{pid}.jsonl`
//! where `ts` uses the same `%Y%m%d-%H%M%S-%3f` format as the existing
//! `run_id` prefix. The filename intentionally contains no colons so
//! the writer round-trips on Windows (which forbids `:` in filenames).
//!
//! This module provides:
//!
//! - [`TraceSpan`] — the deserialized shape of one JSONL line.
//! - [`make_writer`] — a [`tracing_subscriber::fmt::MakeWriter`]
//!   implementation that lazily opens the per-process file on the first
//!   emission so processes that never log (e.g. `rocky --help`) leave
//!   no trace artifacts behind.
//! - [`read_trace`] — load every span for a given `run_id` from the
//!   on-disk JSONL files. Returns the union of all lines from any file
//!   whose [`TraceSpan::spans`] chain contains the target run_id, per
//!   the file-per-process semantic (pre-mint context spans are returned
//!   alongside their run).
//! - [`sweep_traces`] — last-N-by-mtime retention sweep, mirroring the
//!   `SweepReport` shape used by `[state.retention]`.

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

/// Default number of JSONL trace files kept by [`sweep_traces`] when
/// `ROCKY_TRACE_RETAIN_RUNS` is unset.
pub const DEFAULT_TRACE_RETAIN_RUNS: usize = 20;

/// Environment variable that overrides [`DEFAULT_TRACE_RETAIN_RUNS`].
pub const TRACE_RETAIN_RUNS_ENV: &str = "ROCKY_TRACE_RETAIN_RUNS";

/// Environment variable that disables JSONL trace persistence entirely.
/// Honored by [`crate::tracing_setup::init_tracing`].
pub const TRACE_DISABLE_ENV: &str = "ROCKY_TRACE_DISABLE";

/// Directory (relative to the working directory) where Rocky writes
/// JSONL trace files. Matches the `.rocky/` convention used by plans,
/// approvals, and the catalog snapshot.
pub const TRACE_DIR: &str = ".rocky/traces";

// ---------------------------------------------------------------------------
// TraceSpan: deserialized JSONL line
// ---------------------------------------------------------------------------

/// One JSONL line as written by [`tracing_subscriber::fmt`] with `.json()`.
///
/// The fmt layer emits an open-ended object per record; this struct
/// pulls out the fields callers actually pivot on (`timestamp`, `level`,
/// `target`, `fields`, `spans`) and preserves the rest under
/// [`TraceSpan::other`] so the consumer can read additional fmt-level
/// attributes without us having to maintain a closed schema as the
/// upstream layer evolves.
///
/// `spans` is the active span chain at emission time; each entry
/// carries the span's name plus any recorded fields. For Rocky's `run`
/// span, that chain includes a `run_id` attribute once
/// `Span::record("run_id", ...)` fires at `commands/run.rs:744`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceSpan {
    /// RFC 3339 timestamp emitted by the fmt layer.
    #[serde(default)]
    pub timestamp: Option<String>,
    /// Log level (`INFO`, `WARN`, ...).
    #[serde(default)]
    pub level: Option<String>,
    /// `module_path` of the emitting site.
    #[serde(default)]
    pub target: Option<String>,
    /// Event-level fields (the `key = value` arguments to
    /// `tracing::info!` / `tracing::event!`).
    #[serde(default)]
    pub fields: serde_json::Map<String, serde_json::Value>,
    /// Active span chain. Each entry has `name` plus recorded fields
    /// (e.g. `run_id`). Order is root-first: `spans[0]` is the
    /// outermost span entered.
    #[serde(default)]
    pub spans: Vec<serde_json::Map<String, serde_json::Value>>,
    /// Catch-all for forward compatibility with new fmt layer fields.
    #[serde(flatten)]
    pub other: serde_json::Map<String, serde_json::Value>,
}

impl TraceSpan {
    /// Returns `true` when any entry in [`TraceSpan::spans`] carries a
    /// `run_id` field equal to the supplied value. Both the literal
    /// `run_id = "..."` shape (from `tracing::Span::record`) and a
    /// stringified JSON value are accepted.
    pub fn matches_run_id(&self, run_id: &str) -> bool {
        self.spans.iter().any(|s| match s.get("run_id") {
            Some(serde_json::Value::String(v)) => v == run_id,
            Some(other) => other.to_string().trim_matches('"') == run_id,
            None => false,
        })
    }
}

// ---------------------------------------------------------------------------
// MakeWriter: lazy per-process JSONL file
// ---------------------------------------------------------------------------

/// `MakeWriter` implementation that opens a single `.rocky/traces/{ts}-{pid}.jsonl`
/// file lazily on the first emission and reuses it for the lifetime of
/// the process.
///
/// Each `make_writer()` call returns a `JsonlWriter` handle backed by
/// the same `Arc<Mutex<JsonlWriterState>>`. The first write creates the
/// parent directory and opens the file in append mode; if that fails
/// (read-only filesystem, missing project root) the state flips to
/// `Disabled` so subsequent writes are silently dropped rather than
/// retrying on every record.
///
/// We write the raw `File` (no `BufWriter`): the fmt layer formats one
/// complete line per `write` call, so there's no benefit to a userland
/// buffer, and skipping it removes the "buffer was never flushed at
/// process exit" failure mode entirely. Append-mode writes are
/// atomic per-syscall up to PIPE_BUF; per-line records stay well below
/// that on every supported platform.
#[derive(Clone)]
pub struct JsonlMakeWriter {
    inner: Arc<Mutex<JsonlWriterState>>,
}

enum JsonlWriterState {
    /// First write hasn't fired yet. We hold the resolved target path
    /// so the open is hermetic w.r.t. later `chdir` calls.
    Unopened { path: PathBuf },
    /// File is open and writable. We write the raw `File` (no
    /// `BufWriter`) so per-event emissions land on disk immediately;
    /// the fmt layer formats one record per `write` call so each call
    /// is already a complete line, and skipping the buffer means there's
    /// no "BufWriter was never flushed at process exit" failure mode.
    Open(File),
    /// Open attempt failed; route subsequent writes to /dev/null so we
    /// don't repeatedly hammer the same broken state.
    Disabled,
}

impl JsonlMakeWriter {
    /// Construct a new writer targeting `{cwd}/.rocky/traces/{ts}-{pid}.jsonl`.
    ///
    /// The filename uses the supplied `started_at` formatted as
    /// `%Y%m%d-%H%M%S-%3f` — the same pattern as the `run_id` prefix and
    /// safe across every supported filesystem (no colons, no slashes).
    #[must_use]
    pub fn new(started_at: chrono::DateTime<chrono::Utc>) -> Self {
        let pid = std::process::id();
        let ts = started_at.format("%Y%m%d-%H%M%S-%3f");
        let path = PathBuf::from(TRACE_DIR).join(format!("{ts}-{pid}.jsonl"));
        Self {
            inner: Arc::new(Mutex::new(JsonlWriterState::Unopened { path })),
        }
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for JsonlMakeWriter {
    type Writer = JsonlWriter;

    fn make_writer(&'a self) -> Self::Writer {
        JsonlWriter {
            state: Arc::clone(&self.inner),
        }
    }
}

/// Per-call writer handle returned by [`JsonlMakeWriter::make_writer`].
pub struct JsonlWriter {
    state: Arc<Mutex<JsonlWriterState>>,
}

impl Write for JsonlWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut guard = match self.state.lock() {
            Ok(g) => g,
            // The mutex is only used in the writer path; a poisoned mutex
            // means an earlier writer panicked mid-flush. Recover the
            // inner state so subsequent emissions still land somewhere.
            Err(poisoned) => poisoned.into_inner(),
        };
        loop {
            match &mut *guard {
                JsonlWriterState::Open(file) => return file.write(buf),
                JsonlWriterState::Disabled => return Ok(buf.len()),
                JsonlWriterState::Unopened { path } => {
                    let path = path.clone();
                    match open_jsonl(&path) {
                        Ok(file) => {
                            *guard = JsonlWriterState::Open(file);
                            // Loop back; the next iteration writes to the
                            // newly-open file.
                        }
                        Err(err) => {
                            tracing::warn!(
                                target: "rocky::traces",
                                error = %err,
                                path = %path.display(),
                                "JSONL trace writer: open failed; spans will not be persisted"
                            );
                            *guard = JsonlWriterState::Disabled;
                            return Ok(buf.len());
                        }
                    }
                }
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        let mut guard = match self.state.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        match &mut *guard {
            JsonlWriterState::Open(file) => file.flush(),
            _ => Ok(()),
        }
    }
}

fn open_jsonl(path: &Path) -> io::Result<File> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    OpenOptions::new().create(true).append(true).open(path)
}

// ---------------------------------------------------------------------------
// read_trace
// ---------------------------------------------------------------------------

/// Load every span for `run_id` from the on-disk JSONL files.
///
/// Scans `<traces_dir>/*.jsonl` (default `.rocky/traces/*.jsonl`),
/// parses each line as a [`TraceSpan`], and returns the union of all
/// lines from any file that contains *at least one* line whose
/// [`TraceSpan::spans`] chain carries `run_id`. The file-per-process
/// semantic (D5 in the design memo) means pre-mint spans (those emitted
/// before `Span::record("run_id", ...)` fires) are returned as context
/// alongside the run's own spans.
///
/// Unreadable files and malformed JSON lines are skipped silently; this
/// API is observability and must not fail the caller's run on partial
/// corruption.
///
/// Returns an empty `Vec` when the traces directory does not exist
/// (fresh project) or no file matches.
pub fn read_trace(run_id: &str) -> Vec<TraceSpan> {
    read_trace_from(Path::new(TRACE_DIR), run_id)
}

/// Lower-level entry point used by tests to scope the scan to a tempdir.
pub fn read_trace_from(dir: &Path, run_id: &str) -> Vec<TraceSpan> {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return Vec::new(),
    };

    let mut out = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("jsonl") {
            continue;
        }
        let content = match std::fs::read_to_string(&path) {
            Ok(c) => c,
            Err(_) => continue,
        };
        let parsed: Vec<TraceSpan> = content
            .lines()
            .filter(|l| !l.trim().is_empty())
            .filter_map(|l| serde_json::from_str::<TraceSpan>(l).ok())
            .collect();
        if parsed.iter().any(|s| s.matches_run_id(run_id)) {
            out.extend(parsed);
        }
    }
    out
}

// ---------------------------------------------------------------------------
// sweep_traces: last-N-by-mtime retention
// ---------------------------------------------------------------------------

/// Resolve the retention size from `ROCKY_TRACE_RETAIN_RUNS`, falling
/// back to [`DEFAULT_TRACE_RETAIN_RUNS`] when the variable is unset or
/// fails to parse as a positive integer.
#[must_use]
pub fn retain_runs_from_env() -> usize {
    std::env::var(TRACE_RETAIN_RUNS_ENV)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(DEFAULT_TRACE_RETAIN_RUNS)
}

/// Delete all but the newest `keep` files (by mtime) under `dir`.
///
/// Returns the number of files removed. Files that fail to stat are
/// skipped (and not counted as deleted); files that fail to remove are
/// logged at WARN and not counted. Returns 0 when the directory does
/// not exist.
pub fn sweep_traces(dir: &Path, keep: usize) -> u64 {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return 0,
    };

    let mut jsonl: Vec<(PathBuf, std::time::SystemTime)> = entries
        .flatten()
        .filter_map(|e| {
            let path = e.path();
            if path.extension().and_then(|s| s.to_str()) != Some("jsonl") {
                return None;
            }
            let mtime = e.metadata().and_then(|m| m.modified()).ok()?;
            Some((path, mtime))
        })
        .collect();

    if jsonl.len() <= keep {
        return 0;
    }

    // Newest first by mtime so the first `keep` entries are the survivors.
    jsonl.sort_by_key(|entry| std::cmp::Reverse(entry.1));

    let mut deleted: u64 = 0;
    for (path, _) in jsonl.into_iter().skip(keep) {
        match std::fs::remove_file(&path) {
            Ok(()) => deleted += 1,
            Err(err) => {
                tracing::warn!(
                    target: "rocky::traces",
                    error = %err,
                    path = %path.display(),
                    "trace sweep: remove failed",
                );
            }
        }
    }
    deleted
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn write_line(path: &Path, line: &str) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        let mut f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .unwrap();
        writeln!(f, "{line}").unwrap();
    }

    /// `read_trace` returns lines whose `spans[]` chain matches the
    /// supplied `run_id`, plus every other line in the same file (the
    /// file-per-process semantic).
    #[test]
    fn read_trace_filters_by_run_id() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let file_a = dir.join("a.jsonl");
        let file_b = dir.join("b.jsonl");

        // file_a contains run_id=run-A across two lines, one pre-mint
        // (no run_id), one post-mint.
        write_line(
            &file_a,
            r#"{"timestamp":"t0","level":"INFO","fields":{"message":"pre-mint"},"spans":[{"name":"run"}]}"#,
        );
        write_line(
            &file_a,
            r#"{"timestamp":"t1","level":"INFO","fields":{"message":"post-mint"},"spans":[{"name":"run","run_id":"run-A"}]}"#,
        );
        // file_b has a different run.
        write_line(
            &file_b,
            r#"{"timestamp":"t2","level":"INFO","fields":{"message":"other"},"spans":[{"name":"run","run_id":"run-B"}]}"#,
        );

        let lines = read_trace_from(dir, "run-A");
        assert_eq!(
            lines.len(),
            2,
            "expected both lines from file_a (file-per-process semantic), got {lines:?}"
        );
        let other = read_trace_from(dir, "run-B");
        assert_eq!(other.len(), 1, "expected only file_b's line for run-B");
    }

    /// 25 files, retain=20 → 5 removed by mtime order.
    #[test]
    fn sweep_traces_keeps_newest_n() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();

        // Touch 25 files with monotonically-increasing mtimes. We
        // emulate "older" by stamping older modified-times via
        // `set_modified` so the test is deterministic on fast disks.
        let now = std::time::SystemTime::now();
        for i in 0..25_u64 {
            let p = dir.join(format!("f{i:02}.jsonl"));
            std::fs::write(&p, b"{}\n").unwrap();
            let f = File::open(&p).unwrap();
            // Older index = older mtime.
            let mtime = now
                .checked_sub(std::time::Duration::from_secs(1000 - i))
                .unwrap();
            f.set_modified(mtime).unwrap();
        }
        // Drop a non-jsonl file to confirm the filter ignores it.
        std::fs::write(dir.join("ignore.txt"), b"x").unwrap();

        let deleted = sweep_traces(dir, 20);
        assert_eq!(deleted, 5, "expected 5 of 25 to be removed");

        // The non-jsonl file must remain.
        assert!(dir.join("ignore.txt").exists());

        // 20 jsonl files remain, and they're the newest by index (f05..f24).
        let surviving: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .flatten()
            .filter_map(|e| e.path().file_name()?.to_str().map(str::to_string))
            .filter(|n| n.ends_with(".jsonl"))
            .collect();
        assert_eq!(surviving.len(), 20);
        assert!(!surviving.iter().any(|n| n == "f00.jsonl"));
        assert!(surviving.iter().any(|n| n == "f24.jsonl"));
    }

    /// `sweep_traces` is a no-op when the directory has fewer than `keep`
    /// files.
    #[test]
    fn sweep_traces_below_threshold_is_noop() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        for i in 0..3_u64 {
            std::fs::write(dir.join(format!("f{i}.jsonl")), b"{}\n").unwrap();
        }
        assert_eq!(sweep_traces(dir, 20), 0);
    }

    /// `retain_runs_from_env` honors a valid `ROCKY_TRACE_RETAIN_RUNS`
    /// value and falls back to the default otherwise. The variable is
    /// process-global, so the test serialises against itself via the
    /// well-known per-process mutex pattern.
    #[test]
    fn retain_runs_env_override() {
        // Skip env-mutation when another test holds the var (CI doesn't
        // parallelize this module heavily; this is belt-and-suspenders).
        // SAFETY: tests within this module are single-threaded against
        // the env var; tests in sibling modules don't touch
        // ROCKY_TRACE_RETAIN_RUNS.
        unsafe {
            std::env::set_var(TRACE_RETAIN_RUNS_ENV, "7");
        }
        assert_eq!(retain_runs_from_env(), 7);
        unsafe {
            std::env::remove_var(TRACE_RETAIN_RUNS_ENV);
        }
        assert_eq!(retain_runs_from_env(), DEFAULT_TRACE_RETAIN_RUNS);
    }

    /// Lazy MakeWriter only creates the file once an actual write fires.
    #[test]
    fn makewriter_does_not_open_file_eagerly() {
        let tmp = tempfile::tempdir().unwrap();
        std::env::set_current_dir(&tmp).unwrap();

        let mw = JsonlMakeWriter::new(chrono::Utc::now());
        // No file yet.
        let traces_dir = tmp.path().join(TRACE_DIR);
        assert!(!traces_dir.exists());

        // First write triggers the open.
        {
            use tracing_subscriber::fmt::MakeWriter as _;
            let mut w = mw.make_writer();
            w.write_all(b"{\"hi\":1}\n").unwrap();
            w.flush().unwrap();
        }
        assert!(traces_dir.exists(), "traces dir created on first write");
        let files: Vec<_> = std::fs::read_dir(&traces_dir).unwrap().flatten().collect();
        assert_eq!(files.len(), 1, "exactly one JSONL file written");
    }

    /// Round-trip: drive a `fmt::json` layer at the JSONL writer with a
    /// span that records a `run_id` field, then read it back via
    /// `read_trace`. Asserts the wiring used by `init_tracing` actually
    /// produces JSONL the consumer can parse.
    ///
    /// Survives the process exit by flushing the BufWriter on guard
    /// drop — the test mirrors the production teardown path.
    #[test]
    fn round_trip_emits_and_reads_back() {
        use tracing_subscriber::prelude::*;

        let tmp = tempfile::tempdir().unwrap();
        let traces_dir = tmp.path().join(TRACE_DIR);
        std::fs::create_dir_all(&traces_dir).unwrap();

        let pid = std::process::id();
        let ts = chrono::Utc::now().format("%Y%m%d-%H%M%S-%3f");
        let target = traces_dir.join(format!("{ts}-{pid}.jsonl"));

        // Custom writer that points at our chosen path so we don't have
        // to chdir the whole test process.
        let mw = JsonlMakeWriter {
            inner: Arc::new(Mutex::new(JsonlWriterState::Unopened {
                path: target.clone(),
            })),
        };

        let layer = tracing_subscriber::fmt::layer()
            .json()
            .with_target(true)
            .with_thread_ids(false)
            .with_span_list(true)
            .with_current_span(false)
            .with_writer(mw.clone());
        let subscriber = tracing_subscriber::registry().with(layer);

        let run_id = "run-roundtrip-test";
        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!("run", run_id = tracing::field::Empty);
            span.record("run_id", run_id);
            let _enter = span.enter();
            tracing::info!(target: "rocky::test", message = "inside-run");
        });

        // Force the BufWriter to flush by clearing the writer state.
        // In production this happens at process exit via the BufWriter's
        // Drop impl; the test mirrors that by explicitly flushing.
        {
            use tracing_subscriber::fmt::MakeWriter as _;
            let mut w = mw.make_writer();
            w.flush().unwrap();
        }

        let spans = read_trace_from(&traces_dir, run_id);
        assert!(
            !spans.is_empty(),
            "expected at least one span line for {run_id}, found {spans:?}"
        );
        assert!(
            spans.iter().any(|s| s.matches_run_id(run_id)),
            "no line has run_id={run_id} in spans[]: {spans:?}"
        );
    }

    /// Interleaved runs: two distinct `run_id`s in separate files; each
    /// `read_trace` must scope to its own file.
    #[test]
    fn read_trace_interleaved_runs_dont_cross_contaminate() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let file_a = dir.join("run-A.jsonl");
        let file_b = dir.join("run-B.jsonl");

        write_line(
            &file_a,
            r#"{"timestamp":"t0","spans":[{"name":"run","run_id":"run-A"}],"fields":{"message":"A1"}}"#,
        );
        write_line(
            &file_a,
            r#"{"timestamp":"t1","spans":[{"name":"run","run_id":"run-A"}],"fields":{"message":"A2"}}"#,
        );
        write_line(
            &file_b,
            r#"{"timestamp":"t2","spans":[{"name":"run","run_id":"run-B"}],"fields":{"message":"B1"}}"#,
        );

        let a = read_trace_from(dir, "run-A");
        assert_eq!(a.len(), 2);
        assert!(a.iter().all(|s| s.matches_run_id("run-A")));

        let b = read_trace_from(dir, "run-B");
        assert_eq!(b.len(), 1);
        assert!(b.iter().all(|s| s.matches_run_id("run-B")));
    }
}
