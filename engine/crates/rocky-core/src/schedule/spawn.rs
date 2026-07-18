//! Child-process spawning for the reconciler, behind a trait.
//!
//! `tick_once` never spawns a binary directly — it goes through a [`Spawner`].
//! The real [`SubprocessSpawner`] launches `rocky run` in a child process; the
//! test [`CapturingSpawner`] records the requests and returns scripted
//! outcomes, so the entire reconciler is testable without a built binary.
//!
//! This boundary is also where all real-time waiting lives: the reconciler
//! core takes `now` as a parameter and reads no clock, while the spawner owns
//! the child's wall-clock lifetime (the timeout, the graceful-then-forced
//! termination). Keeping the wait here is what keeps the core deterministic.

use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;

/// A request to run one pipeline as a child process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpawnRequest {
    /// The pipeline to run (`--pipeline <name>`).
    pub pipeline: String,
    /// The config file to run against (`-c <config>`).
    pub config_path: PathBuf,
    /// The resolved state-store path, forwarded as `--state-path <path>` so the
    /// child opens the SAME file the reconciler read demand from. Absolute. An
    /// explicit `--state-path` disables the child's own namespacing, so parent
    /// and child converge unconditionally.
    pub state_path: PathBuf,
    /// The submission id passed as `ROCKY_SUBMISSION_ID`, stamped by the child
    /// into its run record.
    pub submission_id: String,
    /// The W3C `traceparent` for the child's `TRACEPARENT`, connecting the tick
    /// span to the run's trace. `None` when tracing is not active.
    pub traceparent: Option<String>,
    /// Scheduler-level timeout, when configured. On elapse the child is
    /// terminated gracefully, then forcibly.
    pub timeout: Option<Duration>,
}

/// The result of running a child to completion (or termination).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunOutcome {
    /// The child's exit code: 0 (success), 2 (partial), 1/other (failure). A
    /// child killed on timeout is reported as a failure exit code.
    pub exit_code: i32,
    /// The child's PID, recorded on the claim for the recovery sweep. `None`
    /// when the child could not be spawned.
    pub pid: Option<u32>,
}

/// Spawns a pipeline run as a child process and waits for it.
#[async_trait]
pub trait Spawner: Send + Sync {
    /// Run the request to completion, returning its outcome. Implementations
    /// own all real-time waiting (the timeout).
    async fn run(&self, request: &SpawnRequest) -> RunOutcome;
}

/// The grace period between a graceful termination signal and a forced kill.
const KILL_GRACE: Duration = Duration::from_secs(60);

/// The real spawner: launches `current_exe -c <config> run --pipeline <name>
/// --output json` with the tick's `TRACEPARENT`, `ROCKY_RUN_TRIGGER=schedule`,
/// and `ROCKY_SUBMISSION_ID`, honoring the scheduler-level timeout with a
/// graceful-then-forced termination.
#[derive(Debug, Default)]
pub struct SubprocessSpawner;

impl SubprocessSpawner {
    /// Construct the subprocess spawner.
    pub fn new() -> Self {
        Self
    }

    fn build_command(request: &SpawnRequest) -> tokio::process::Command {
        let exe = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("rocky"));
        let mut cmd = tokio::process::Command::new(exe);
        // Discard the child's stdout: the child runs with `--output json`, so its
        // own `RunOutput` would otherwise be inherited onto the tick's stdout and
        // corrupt the tick's `--output json` document (two JSON payloads on one
        // stream). The tick reads the child's exit code and its persisted run
        // record, never its stdout. The child's stderr (tracing/logs) is left
        // inherited so operators still see it.
        cmd.stdout(std::process::Stdio::null());
        // `--state-path` is a top-level (non-global) arg, so it must precede the
        // `run` subcommand token — same position as `-c`.
        cmd.arg("-c")
            .arg(&request.config_path)
            .arg("--state-path")
            .arg(&request.state_path)
            .arg("run")
            .arg("--pipeline")
            .arg(&request.pipeline)
            .arg("--output")
            .arg("json")
            .env("ROCKY_RUN_TRIGGER", "schedule")
            .env("ROCKY_SUBMISSION_ID", &request.submission_id);
        if let Some(tp) = &request.traceparent {
            cmd.env("TRACEPARENT", tp);
        }
        cmd
    }
}

#[async_trait]
impl Spawner for SubprocessSpawner {
    async fn run(&self, request: &SpawnRequest) -> RunOutcome {
        let mut cmd = Self::build_command(request);
        let mut child = match cmd.spawn() {
            Ok(child) => child,
            Err(_) => {
                // Could not spawn — treat as a failure with no pid.
                return RunOutcome {
                    exit_code: 1,
                    pid: None,
                };
            }
        };
        let pid = child.id();

        let status = match request.timeout {
            None => child.wait().await,
            Some(timeout) => match tokio::time::timeout(timeout, child.wait()).await {
                Ok(status) => status,
                Err(_) => {
                    // Timed out: SIGTERM (graceful), then SIGKILL after the grace.
                    terminate_gracefully(&mut child, pid);
                    match tokio::time::timeout(KILL_GRACE, child.wait()).await {
                        Ok(status) => status,
                        Err(_) => {
                            let _ = child.start_kill();
                            child.wait().await
                        }
                    }
                }
            },
        };

        let exit_code = match status {
            Ok(s) => s.code().unwrap_or(1),
            Err(_) => 1,
        };
        RunOutcome { exit_code, pid }
    }
}

/// Send a graceful termination signal to the child. On unix this is `SIGTERM`
/// (the engine handles it as a graceful shutdown); elsewhere it falls back to
/// the platform's forced kill, since no graceful signal exists.
#[cfg(unix)]
fn terminate_gracefully(_child: &mut tokio::process::Child, pid: Option<u32>) {
    if let Some(pid) = pid {
        // SAFETY: `kill` with a valid pid and a standard signal number is a
        // well-defined libc call with no memory-safety implications; a stale
        // pid simply returns ESRCH, which we ignore.
        unsafe {
            libc::kill(pid as libc::pid_t, libc::SIGTERM);
        }
    }
}

#[cfg(not(unix))]
fn terminate_gracefully(child: &mut tokio::process::Child, _pid: Option<u32>) {
    let _ = child.start_kill();
}

/// A recorded invocation captured by [`CapturingSpawner`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapturedRun {
    /// The request as it was submitted.
    pub request: SpawnRequest,
}

/// A test spawner that records every request and returns a scripted outcome.
///
/// The outcome is chosen by a per-pipeline script: each pipeline has a queue of
/// exit codes consumed in order, so a test can model "attempt 1 fails, attempt 2
/// succeeds". A pipeline with an exhausted or absent script defaults to
/// `default_exit`.
pub struct CapturingSpawner {
    captured: Mutex<Vec<CapturedRun>>,
    scripts: Mutex<std::collections::HashMap<String, std::collections::VecDeque<i32>>>,
    default_exit: i32,
    next_pid: Mutex<u32>,
}

impl CapturingSpawner {
    /// A spawner whose every run returns `default_exit`.
    pub fn new(default_exit: i32) -> Self {
        Self {
            captured: Mutex::new(Vec::new()),
            scripts: Mutex::new(std::collections::HashMap::new()),
            default_exit,
            next_pid: Mutex::new(1000),
        }
    }

    /// Script the sequence of exit codes a pipeline's successive runs return.
    pub fn script(&self, pipeline: &str, exit_codes: impl IntoIterator<Item = i32>) {
        self.scripts
            .lock()
            .unwrap()
            .insert(pipeline.to_string(), exit_codes.into_iter().collect());
    }

    /// All captured invocations, in order.
    pub fn captured(&self) -> Vec<CapturedRun> {
        self.captured.lock().unwrap().clone()
    }

    /// The number of captured invocations.
    pub fn run_count(&self) -> usize {
        self.captured.lock().unwrap().len()
    }

    /// The captured invocations for a specific pipeline.
    pub fn runs_for(&self, pipeline: &str) -> Vec<CapturedRun> {
        self.captured
            .lock()
            .unwrap()
            .iter()
            .filter(|c| c.request.pipeline == pipeline)
            .cloned()
            .collect()
    }
}

#[async_trait]
impl Spawner for CapturingSpawner {
    async fn run(&self, request: &SpawnRequest) -> RunOutcome {
        self.captured.lock().unwrap().push(CapturedRun {
            request: request.clone(),
        });
        let exit_code = {
            let mut scripts = self.scripts.lock().unwrap();
            match scripts
                .get_mut(&request.pipeline)
                .and_then(std::collections::VecDeque::pop_front)
            {
                Some(code) => code,
                None => self.default_exit,
            }
        };
        let pid = {
            let mut p = self.next_pid.lock().unwrap();
            *p += 1;
            *p
        };
        RunOutcome {
            exit_code,
            pid: Some(pid),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `--state-path` is a top-level (non-global) `rocky` arg, so it MUST appear
    /// before the `run` subcommand token, while `--pipeline` is a `run` arg and
    /// must appear after it. If this ordering regresses, the child silently opens
    /// a different state file than the reconciler read demand from.
    #[test]
    fn build_command_puts_state_path_before_run_and_pipeline_after() {
        let request = SpawnRequest {
            pipeline: "raw".to_string(),
            config_path: PathBuf::from("/proj/rocky.toml"),
            state_path: PathBuf::from("/proj/models/.rocky-state.redb"),
            submission_id: "sub-1".to_string(),
            traceparent: None,
            timeout: None,
        };
        let cmd = SubprocessSpawner::build_command(&request);
        let args: Vec<String> = cmd
            .as_std()
            .get_args()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();

        let run_pos = args.iter().position(|a| a == "run").expect("run token");
        let sp_pos = args
            .iter()
            .position(|a| a == "--state-path")
            .expect("--state-path");
        let pipe_pos = args
            .iter()
            .position(|a| a == "--pipeline")
            .expect("--pipeline");

        assert!(
            sp_pos < run_pos,
            "--state-path must precede `run`: {args:?}"
        );
        assert_eq!(
            args.get(sp_pos + 1).map(String::as_str),
            Some("/proj/models/.rocky-state.redb"),
            "--state-path value must follow the flag: {args:?}"
        );
        assert!(pipe_pos > run_pos, "--pipeline must follow `run`: {args:?}");
    }
}
