//! Run a child connector once, or again after a failed attempt.
//!
//! One attempt: make a new scratch directory, start the program in its own
//! process group with a cleared environment and resource limits, feed it the
//! request on stdin, send its output to the log, stop it if it runs too long
//! or writes too much, and check what it left behind.
//!
//! The child's stdout and stderr never reach Rocky's stdout. Both go to
//! `tracing`, a line at a time, with registered secret values replaced by
//! `${NAME}`.

use std::ffi::OsString;
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

#[cfg(unix)]
use rocky_adapter_sdk::child::GroupKillGuard;
use rocky_adapter_sdk::child::{RlimitPlan, apply_rlimits, read_bounded_line, stop_child};
#[cfg(any(target_os = "linux", target_os = "macos"))]
use rocky_adapter_sdk::child::{kill_group, leader_has_exited};
use rocky_core::connector::{EffectiveLimits, PROTOCOL_VERSION, StateEnvelope};
use serde::Serialize;
use tokio::io::{AsyncRead, BufReader};
use tokio::process::Child;
use tracing::{debug, info, warn};

use super::verify::{VerifiedBatch, VerifyError, verify_batch};

/// Longest child output line Rocky logs. A longer line is dropped.
const LOG_LINE_MAX_BYTES: usize = 64 * 1024;

/// Most lines Rocky logs per stream. The child can write more; the rest is
/// read and dropped so the child never blocks on a full pipe.
const LOG_LINES_MAX: u64 = 50_000;

/// How long a child gets to leave after `SIGTERM`.
const STOP_GRACE: Duration = Duration::from_secs(5);

/// How long Rocky waits for the log readers after the child is gone.
const READER_JOIN_TIMEOUT: Duration = Duration::from_secs(5);

/// Default gap between two checks of the bytes the child has written.
pub const DEFAULT_OUTPUT_POLL: Duration = Duration::from_secs(2);

/// Everything one run needs.
#[derive(Debug, Clone)]
pub struct ChildSpec<'a> {
    /// The connector's name, for log lines.
    pub name: &'a str,
    /// The program and its arguments. The first element must not be a
    /// relative path: the child's working directory is its scratch directory.
    pub argv: &'a [OsString],
    /// Secrets to hand over, as name and value.
    pub secrets: &'a [(String, String)],
    /// The run id: `ROCKY_RUN_ID`, and part of each scratch directory name.
    pub run_id: &'a str,
    /// The limits after the manifest and the config are merged.
    pub limits: EffectiveLimits,
    /// The saved state to hand over, if any.
    pub state: Option<&'a StateEnvelope>,
    /// The manifest's `state_schema_version`, which the result must carry.
    pub state_schema_version: u32,
    /// The directory that holds the scratch directories.
    pub landing_dir: &'a Path,
    /// Further attempts after a failed first one.
    pub retries: u32,
    /// Gap between two checks of the bytes the child has written.
    pub output_poll: Duration,
}

/// Why one attempt failed. All of them can be retried.
#[derive(Debug, thiserror::Error)]
pub enum AttemptError {
    #[error("could not set up the attempt: {0}")]
    Setup(String),
    #[error("could not start the connector: {0}")]
    Spawn(std::io::Error),
    #[error("the connector exited with {0}")]
    Exit(String),
    #[error("the connector ran past its wall_time_secs limit of {0} seconds and was stopped")]
    WallTime(u64),
    #[error(
        "the connector wrote {written} bytes, over its output_bytes limit of {limit}, and was stopped"
    )]
    OutputCap { written: u64, limit: u64 },
    #[error("the run was interrupted by a signal and the connector was stopped")]
    Interrupted,
    #[error("{0}")]
    Verify(#[from] VerifyError),
}

/// A run in which every attempt failed.
#[derive(Debug, thiserror::Error)]
#[error("connector `{name}` failed after {attempts} attempt(s): {error}")]
pub struct RunFailed {
    pub name: String,
    /// Attempts made.
    pub attempts: u32,
    /// Why the last one failed.
    #[source]
    pub error: AttemptError,
}

/// A run that produced a verified batch.
#[derive(Debug)]
pub struct RunSucceeded {
    pub batch: VerifiedBatch,
    /// Attempts made, counting the successful one.
    pub attempts: u32,
}

/// The request the child reads on stdin, as one JSON document.
#[derive(Debug, Serialize)]
struct Request<'a> {
    protocol_version: u32,
    run_id: &'a str,
    output_dir: &'a str,
    state: Option<&'a StateEnvelope>,
    limits: &'a EffectiveLimits,
}

/// Run the connector until an attempt yields a verified batch, or until
/// `retries + 1` attempts have failed.
///
/// # Errors
///
/// [`RunFailed`] with the last attempt's error. A failed attempt's scratch
/// directory is removed.
pub async fn run_with_retries(spec: &ChildSpec<'_>) -> Result<RunSucceeded, RunFailed> {
    let max_attempts = spec.retries.saturating_add(1);
    let mut attempt = 0;
    loop {
        attempt += 1;
        match run_attempt(spec, attempt).await {
            Ok(batch) => {
                return Ok(RunSucceeded {
                    batch,
                    attempts: attempt,
                });
            }
            Err(error) if attempt >= max_attempts || matches!(error, AttemptError::Interrupted) => {
                return Err(RunFailed {
                    name: spec.name.to_string(),
                    attempts: attempt,
                    error,
                });
            }
            Err(error) => {
                warn!(
                    connector = spec.name,
                    attempt,
                    max_attempts,
                    error = %error,
                    "connector attempt failed; running it again"
                );
            }
        }
    }
}

/// One attempt, in a fresh scratch directory.
async fn run_attempt(spec: &ChildSpec<'_>, attempt: u32) -> Result<VerifiedBatch, AttemptError> {
    let scratch = new_scratch_dir(spec.landing_dir, spec.run_id, attempt)?;
    let outcome = run_child_and_verify(spec, &scratch).await;
    if let Err(error) = &outcome {
        warn!(
            connector = spec.name,
            attempt,
            scratch = %scratch.display(),
            error = %error,
            "removing the scratch directory of a failed attempt"
        );
        if let Err(e) = std::fs::remove_dir_all(&scratch) {
            warn!(scratch = %scratch.display(), error = %e, "could not remove the scratch directory");
        }
    }
    outcome
}

async fn run_child_and_verify(
    spec: &ChildSpec<'_>,
    scratch: &Path,
) -> Result<VerifiedBatch, AttemptError> {
    run_child(spec, scratch).await?;
    let batch = verify_batch(scratch, spec.state_schema_version, spec.limits.output_bytes)?;
    Ok(batch)
}

/// Make `<landing>/<run_id>-<attempt>` as a new, empty directory and return
/// its absolute path.
fn new_scratch_dir(landing: &Path, run_id: &str, attempt: u32) -> Result<PathBuf, AttemptError> {
    std::fs::create_dir_all(landing)
        .map_err(|e| AttemptError::Setup(format!("{}: {e}", landing.display())))?;
    let landing = landing
        .canonicalize()
        .map_err(|e| AttemptError::Setup(format!("{}: {e}", landing.display())))?;
    let scratch = landing.join(format!("{run_id}-{attempt}"));
    // `create_dir` fails when the path exists, so a directory is never reused.
    std::fs::create_dir(&scratch)
        .map_err(|e| AttemptError::Setup(format!("{}: {e}", scratch.display())))?;
    Ok(scratch)
}

/// The request as an anonymous file, rewound for reading.
///
/// A pipe would need Rocky to write after the spawn, and a child that exits
/// without reading would then kill Rocky with `SIGPIPE`. `tempfile()` unlinks
/// the file at once on Unix, so the request never stays on disk under a
/// name. On Windows the file is deleted when its last handle closes.
fn stdin_file(json: &str) -> std::io::Result<std::fs::File> {
    let mut file = tempfile::tempfile()?;
    file.write_all(json.as_bytes())?;
    file.seek(SeekFrom::Start(0))?;
    Ok(file)
}

/// Start the child and supervise it until it has exited cleanly.
async fn run_child(spec: &ChildSpec<'_>, scratch: &Path) -> Result<(), AttemptError> {
    let Some((program, args)) = spec.argv.split_first() else {
        return Err(AttemptError::Setup("the command is empty".to_string()));
    };
    let output_dir = scratch
        .to_str()
        .ok_or_else(|| AttemptError::Setup("the scratch path is not valid UTF-8".to_string()))?;
    let request = serde_json::to_string(&Request {
        protocol_version: PROTOCOL_VERSION,
        run_id: spec.run_id,
        output_dir,
        state: spec.state,
        limits: &spec.limits,
    })
    .map_err(|e| AttemptError::Setup(e.to_string()))?;
    let stdin = stdin_file(&request)
        .map_err(|e| AttemptError::Setup(format!("cannot create the stdin file: {e}")))?;

    let mut command = tokio::process::Command::new(program);
    command.args(args).current_dir(scratch).env_clear();
    // Only these variables reach the child. HOME and TMPDIR point at the
    // scratch directory so a library that writes caches or temp files does
    // not write into the user's home.
    if let Some(path) = std::env::var_os("PATH") {
        command.env("PATH", path);
    }
    if let Some(lang) = std::env::var_os("LANG") {
        command.env("LANG", lang);
    }
    for (name, value) in spec.secrets {
        command.env(name, value);
    }
    command
        .env("ROCKY_RUN_ID", spec.run_id)
        .env("HOME", scratch)
        .env("TMPDIR", scratch);
    command
        .stdin(Stdio::from(stdin))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    #[cfg(unix)]
    command.process_group(0);
    // The plan is built before the spawn, so a bad limit fails here and not
    // in the forked child.
    apply_rlimits(
        &mut command,
        RlimitPlan {
            cpu_secs: spec.limits.cpu_time_secs,
            memory_bytes: spec.limits.memory_bytes,
            output_bytes: Some(spec.limits.output_bytes),
        },
    );

    let mut child = command.spawn().map_err(AttemptError::Spawn)?;
    // SIGKILL the whole group if this future is dropped before the child is
    // reaped (for example on Ctrl-C).
    #[cfg(unix)]
    let mut group_guard = child.id().map(GroupKillGuard::new);

    let stdout_task = child
        .stdout
        .take()
        .map(|out| spawn_log_task(out, spec.name.to_string(), LogStream::Stdout));
    let stderr_task = child
        .stderr
        .take()
        .map(|err| spawn_log_task(err, spec.name.to_string(), LogStream::Stderr));

    let finish = supervise(&mut child, scratch, &spec.limits, spec.output_poll).await;
    match finish {
        Finish::Exited => {
            // The leader is gone but has not been reaped yet, so its id still
            // names its group. Stop anything it left running before the
            // output is read.
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            if let Some(pgid) = child.id() {
                let _ = kill_group(pgid);
            }
        }
        Finish::WallTime | Finish::OutputCap(_) | Finish::Interrupted => {
            stop_child(&mut child, STOP_GRACE).await;
        }
    }
    let status = child.wait().await;
    #[cfg(unix)]
    if let Some(guard) = group_guard.as_mut() {
        guard.disarm();
    }
    for task in [stdout_task, stderr_task].into_iter().flatten() {
        join_log_task(task).await;
    }

    match finish {
        Finish::WallTime => Err(AttemptError::WallTime(spec.limits.wall_time_secs)),
        Finish::Interrupted => Err(AttemptError::Interrupted),
        Finish::OutputCap(written) => Err(AttemptError::OutputCap {
            written,
            limit: spec.limits.output_bytes,
        }),
        Finish::Exited => {
            let status = status.map_err(|e| AttemptError::Exit(format!("a wait error: {e}")))?;
            if status.success() {
                Ok(())
            } else {
                Err(AttemptError::Exit(describe_exit(status)))
            }
        }
    }
}

fn describe_exit(status: std::process::ExitStatus) -> String {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        if let Some(signal) = status.signal() {
            return format!("signal {signal}");
        }
    }
    match status.code() {
        Some(code) => format!("code {code}"),
        None => "no exit code".to_string(),
    }
}

#[derive(Debug, Clone, Copy)]
enum Finish {
    Exited,
    WallTime,
    OutputCap(u64),
    Interrupted,
}

/// Wait for the child to exit, the wall clock to run out, or the output to
/// pass its cap, whichever comes first.
async fn supervise(
    child: &mut Child,
    scratch: &Path,
    limits: &EffectiveLimits,
    poll: Duration,
) -> Finish {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(limits.wall_time_secs);
    let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + poll, poll);
    let exit = wait_for_exit(child);
    tokio::pin!(exit);
    let signalled = interrupted();
    tokio::pin!(signalled);
    loop {
        tokio::select! {
            biased;
            () = &mut exit => return Finish::Exited,
            () = &mut signalled => return Finish::Interrupted,
            () = tokio::time::sleep_until(deadline) => return Finish::WallTime,
            _ = ticker.tick() => {
                let dir = scratch.to_path_buf();
                let written = tokio::task::spawn_blocking(move || dir_size(&dir))
                    .await
                    .unwrap_or(0);
                if written > limits.output_bytes {
                    return Finish::OutputCap(written);
                }
            }
        }
    }
}

/// Resolves when Rocky is asked to stop (`SIGINT`, or `SIGTERM` on Unix).
/// The child runs in its own process group, so a Ctrl-C at the terminal
/// reaches Rocky and not the child. Rocky has to stop the child itself.
async fn interrupted() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = signal(SignalKind::terminate()).ok();
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            () = async {
                match term.as_mut() {
                    Some(term) => { term.recv().await; }
                    None => std::future::pending::<()>().await,
                }
            } => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

/// Wait until the child has exited, WITHOUT reaping it where the platform
/// allows, so its process group id stays valid for the cleanup signal.
#[cfg(any(target_os = "linux", target_os = "macos"))]
async fn wait_for_exit(child: &mut Child) {
    let Some(pid) = child.id() else {
        let _ = child.wait().await;
        return;
    };
    while !leader_has_exited(pid) {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
async fn wait_for_exit(child: &mut Child) {
    let _ = child.wait().await;
}

/// Total size of the regular files under `dir`, without following symlinks.
fn dir_size(dir: &Path) -> u64 {
    let mut total: u64 = 0;
    let mut pending = vec![dir.to_path_buf()];
    while let Some(current) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&current) else {
            continue;
        };
        for entry in entries.flatten() {
            let Ok(meta) = std::fs::symlink_metadata(entry.path()) else {
                continue;
            };
            if meta.is_dir() {
                pending.push(entry.path());
            } else if meta.is_file() {
                total = total.saturating_add(meta.len());
            }
        }
    }
    total
}

#[derive(Debug, Clone, Copy)]
enum LogStream {
    Stdout,
    Stderr,
}

fn spawn_log_task<R>(reader: R, name: String, stream: LogStream) -> tokio::task::JoinHandle<()>
where
    R: AsyncRead + Send + Unpin + 'static,
{
    tokio::spawn(async move {
        let mut reader = BufReader::new(reader);
        let mut logged: u64 = 0;
        let mut announced_cap = false;
        loop {
            match read_bounded_line(&mut reader, LOG_LINE_MAX_BYTES).await {
                Ok(Some(line)) if line.is_empty() => break,
                Ok(Some(line)) => {
                    if logged >= LOG_LINES_MAX {
                        if !announced_cap {
                            announced_cap = true;
                            warn!(connector = %name, "connector output passed {LOG_LINES_MAX} lines; the rest is not logged");
                        }
                        continue;
                    }
                    logged += 1;
                    let line = rocky_core::secret_registry::render_placeholders(
                        line.trim_end_matches(['\n', '\r']),
                    );
                    match stream {
                        LogStream::Stdout => {
                            debug!(connector = %name, stream = "stdout", "{line}");
                        }
                        LogStream::Stderr => {
                            info!(connector = %name, stream = "stderr", "{line}");
                        }
                    }
                }
                Ok(None) => {
                    warn!(connector = %name, "a connector output line was over {LOG_LINE_MAX_BYTES} bytes and was dropped");
                    if discard_rest_of_line(&mut reader).await.is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    })
}

/// Read and drop bytes up to and including the next newline.
async fn discard_rest_of_line<R: tokio::io::AsyncBufRead + Unpin>(
    reader: &mut R,
) -> std::io::Result<()> {
    use tokio::io::AsyncBufReadExt;
    loop {
        let (consumed, found) = {
            let buf = reader.fill_buf().await?;
            if buf.is_empty() {
                return Ok(());
            }
            match buf.iter().position(|b| *b == b'\n') {
                Some(i) => (i + 1, true),
                None => (buf.len(), false),
            }
        };
        reader.consume(consumed);
        if found {
            return Ok(());
        }
    }
}

async fn join_log_task(task: tokio::task::JoinHandle<()>) {
    let abort = task.abort_handle();
    if tokio::time::timeout(READER_JOIN_TIMEOUT, task)
        .await
        .is_err()
    {
        abort.abort();
    }
}
