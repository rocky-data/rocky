//! The `AgentDriver` seam — bring-your-own-model workers under real
//! supervision.
//!
//! This is the engine's first process-group code (nothing else in the
//! workspace kills a process GROUP; `schedule::spawn` signals a single
//! pid and `hooks` rely on `kill_on_drop`, and grandchildren survive
//! both). The rules here, stated plainly:
//!
//! - Every worker task runs in ONE process group. The leader is spawned
//!   with `process_group(0)` (PGID = leader pid); every additional child
//!   the driver starts — a sibling worker-profile MCP server included —
//!   joins that group via `process_group(pgid)`. A sibling outside the
//!   group would survive the kill.
//! - Termination is `killpg(SIGTERM)` → a configured grace →
//!   `killpg(SIGKILL)`, and it runs after a NORMAL leader exit as well
//!   as on timeout — a leader's exit says nothing about its
//!   grandchildren.
//! - After every window the driver asserts no survivors: the group must
//!   not exist (`killpg(pgid, 0)` = ESRCH) before `run_task` returns.
//! - The worker's environment is ONLY `env_allow`: everything else is
//!   cleared before spawn.
//! - Windows is not supervised in v0: the driver refuses with a typed
//!   `unsupported driver platform` error (the crate still compiles).
//!
//! Workers have no product-spec write tool by design: an elicitation
//! task returns `{candidate_spec_bytes, questions, expected_digest}` in
//! the typed [`DriverOutcome`], and the RUNNER performs the confined
//! staged write of `products/<name>.toml`.

use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::Utc;
use rocky_core::config::FulfillDriverConfig;

/// The outbox file an elicitation worker writes its candidate spec to
/// (inside the task's outbox directory).
pub const OUTBOX_CANDIDATE: &str = "candidate_spec.toml";
/// The outbox file an elicitation worker writes its questions to
/// (a JSON array of strings). Optional.
pub const OUTBOX_QUESTIONS: &str = "questions.json";

/// What kind of task the worker is being dispatched on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskBriefKind {
    /// Propose a candidate spec from sampled sources.
    Elicitation,
    /// Draft `models/<model>.sql` until compile/test read green.
    Drafting,
    /// Repair a red verification.
    Repair,
}

impl TaskBriefKind {
    /// The stable name (session-file key, transcript name, brief file).
    pub fn as_str(self) -> &'static str {
        match self {
            TaskBriefKind::Elicitation => "elicitation",
            TaskBriefKind::Drafting => "drafting",
            TaskBriefKind::Repair => "repair",
        }
    }
}

/// One task hand-off to a driver.
#[derive(Debug, Clone)]
pub struct TaskBrief {
    /// The task kind.
    pub kind: TaskBriefKind,
    /// The rendered brief text (from [`crate::briefs`]).
    pub text: String,
    /// The product name.
    pub product: String,
    /// The project root the worker operates in (cwd of every child).
    pub project_root: PathBuf,
    /// Where the transcript file goes
    /// (`.rocky/fulfillment/<name>/transcripts/`).
    pub transcript_dir: PathBuf,
    /// The task outbox (`.rocky/fulfillment/<name>/outbox/`), cleared by
    /// the driver before dispatch. The elicitation contract: the worker
    /// writes [`OUTBOX_CANDIDATE`] (+ optionally [`OUTBOX_QUESTIONS`])
    /// here; the RUNNER does the confined spec write.
    pub outbox_dir: PathBuf,
}

/// What a driver task produced — typed, per task kind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DriverOutcome {
    /// An elicitation task's hand-off. The runner verifies
    /// `expected_digest` against the bytes BEFORE the confined write and
    /// refuses a mismatch.
    Elicitation {
        /// The candidate spec, raw bytes.
        candidate_spec_bytes: Vec<u8>,
        /// The worker's questions for the human.
        questions: Vec<String>,
        /// `sha256:<hex>` the hand-off claims for the bytes.
        expected_digest: String,
        /// The captured transcript.
        transcript_path: PathBuf,
    },
    /// A drafting / repair task ended with the worker's exit 0 and the
    /// group killed with no survivors. The runner trusts NOTHING from
    /// it — its own compile/test decide.
    Drafting {
        /// The captured transcript.
        transcript_path: PathBuf,
    },
}

/// A typed driver failure.
#[derive(Debug, thiserror::Error)]
pub enum DriverError {
    /// No supervised driver exists for this platform (Windows in v0).
    #[error("unsupported driver platform: no process-group supervision exists here in v0")]
    UnsupportedPlatform,
    /// The driver configuration is unusable.
    #[error("driver configuration invalid: {0}")]
    Config(String),
    /// The worker (or server) could not be spawned.
    #[error("failed to spawn the worker: {0}")]
    Spawn(String),
    /// The task outlived its budget; the group was killed.
    #[error("task timed out after {seconds}s (group killed); transcript: {transcript}")]
    Timeout {
        /// The configured budget.
        seconds: u64,
        /// The captured transcript path, rendered.
        transcript: String,
    },
    /// The worker exited non-zero.
    #[error("worker exited with code {exit_code}; transcript: {transcript}")]
    TaskFailed {
        /// The leader's exit code.
        exit_code: i32,
        /// The captured transcript path, rendered.
        transcript: String,
    },
    /// An elicitation task ended without the outbox hand-off.
    #[error("elicitation produced no candidate: {0}")]
    OutboxMissing(String),
    /// The recorded session is unusable or its expectations failed.
    #[error("replay session error: {0}")]
    Session(String),
    /// Survivors remained after the kill window — a supervision failure,
    /// never ignored.
    #[error("process group {pgid} still has survivors after SIGKILL: {detail}")]
    Survivors {
        /// The group that would not die.
        pgid: u32,
        /// Detail for the operator.
        detail: String,
    },
}

/// A live process group's identity, for the store's takeover stamp.
pub type GroupStamp = (u32, u64);

/// The driver seam. `on_group` fires right after the group leader
/// spawns, with `(pgid, leader_start_time)` — the runner stamps it onto
/// the record so a takeover after a crash can sweep the group.
#[async_trait::async_trait]
pub trait AgentDriver: Send + Sync {
    /// Run one task to completion under full supervision.
    async fn run_task(
        &self,
        brief: &TaskBrief,
        on_group: &mut (dyn FnMut(GroupStamp) -> anyhow::Result<()> + Send),
    ) -> Result<DriverOutcome, DriverError>;
}

/// Build the configured driver.
pub fn driver_from_config(
    config: &FulfillDriverConfig,
    project_root: &Path,
) -> Result<Box<dyn AgentDriver>, DriverError> {
    match config {
        FulfillDriverConfig::Subprocess {
            command,
            env_allow,
            timeout_seconds,
            kill_grace_seconds,
        } => Ok(Box::new(SubprocessDriver::new(
            command.clone(),
            env_allow.clone(),
            Duration::from_secs(*timeout_seconds),
            Duration::from_secs(*kill_grace_seconds),
        )?)),
        FulfillDriverConfig::Replay { session } => {
            let path = if session.is_absolute() {
                session.clone()
            } else {
                project_root.join(session)
            };
            Ok(Box::new(ReplayDriver::new(path)))
        }
    }
}

// ---------------------------------------------------------------------------
// The subprocess driver
// ---------------------------------------------------------------------------

/// The configurable-command worker driver (BYOM: the command template is
/// the whole integration).
pub struct SubprocessDriver {
    command: Vec<String>,
    env_allow: Vec<String>,
    timeout: Duration,
    kill_grace: Duration,
}

impl SubprocessDriver {
    /// Validate the template: exactly one argument must carry `{brief}`.
    pub fn new(
        command: Vec<String>,
        env_allow: Vec<String>,
        timeout: Duration,
        kill_grace: Duration,
    ) -> Result<Self, DriverError> {
        if command.is_empty() {
            return Err(DriverError::Config(
                "[fulfill.driver] command is empty".to_string(),
            ));
        }
        let brief_slots = command.iter().filter(|a| a.contains("{brief}")).count();
        if brief_slots != 1 {
            return Err(DriverError::Config(format!(
                "[fulfill.driver] command must carry the {{brief}} placeholder in exactly \
                 one argument (found {brief_slots})"
            )));
        }
        Ok(Self {
            command,
            env_allow,
            timeout,
            kill_grace,
        })
    }
}

#[async_trait::async_trait]
impl AgentDriver for SubprocessDriver {
    #[cfg(unix)]
    async fn run_task(
        &self,
        brief: &TaskBrief,
        on_group: &mut (dyn FnMut(GroupStamp) -> anyhow::Result<()> + Send),
    ) -> Result<DriverOutcome, DriverError> {
        let transcript_path = prepare_dirs(brief)?;
        let argv: Vec<String> = self
            .command
            .iter()
            .map(|a| a.replace("{brief}", &brief.text))
            .collect();

        let transcript = std::fs::File::create(&transcript_path)
            .map_err(|e| DriverError::Spawn(format!("transcript file: {e}")))?;
        let transcript_err = transcript
            .try_clone()
            .map_err(|e| DriverError::Spawn(format!("transcript file: {e}")))?;

        let mut cmd = tokio::process::Command::new(&argv[0]);
        cmd.args(&argv[1..])
            .current_dir(&brief.project_root)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::from(transcript))
            .stderr(std::process::Stdio::from(transcript_err))
            // One PGID for the whole task: 0 = the leader's own pid.
            .process_group(0)
            // The worker sees ONLY the allowlist.
            .env_clear();
        for key in &self.env_allow {
            if let Ok(value) = std::env::var(key) {
                cmd.env(key, value);
            }
        }

        let mut leader = cmd
            .spawn()
            .map_err(|e| DriverError::Spawn(format!("{}: {e}", argv[0])))?;
        let pgid = leader
            .id()
            .ok_or_else(|| DriverError::Spawn("leader exited before its pid was read".into()))?;
        let leader_start = crate::store::process_liveness(pgid)
            .ok()
            .flatten()
            .unwrap_or(0);
        on_group((pgid, leader_start)).map_err(|e| DriverError::Spawn(format!("{e:#}")))?;

        // Race the leader's natural exit against the task budget (the
        // schedule::spawn select shape).
        let waited = tokio::select! {
            status = leader.wait() => Some(status),
            () = tokio::time::sleep(self.timeout) => None,
        };

        // Group kill on EVERY path: after a normal leader exit AS WELL as
        // on timeout — leader exit does not imply grandchildren exit.
        kill_group(pgid, self.kill_grace).await?;

        let transcript_rendered = transcript_path.display().to_string();
        let status = match waited {
            None => {
                return Err(DriverError::Timeout {
                    seconds: self.timeout.as_secs(),
                    transcript: transcript_rendered,
                });
            }
            Some(Err(e)) => return Err(DriverError::Spawn(format!("wait failed: {e}"))),
            Some(Ok(status)) => status,
        };
        let exit_code = status.code().unwrap_or(1);
        if exit_code != 0 {
            return Err(DriverError::TaskFailed {
                exit_code,
                transcript: transcript_rendered,
            });
        }
        collect_outcome(brief, transcript_path)
    }

    #[cfg(not(unix))]
    async fn run_task(
        &self,
        _brief: &TaskBrief,
        _on_group: &mut (dyn FnMut(GroupStamp) -> anyhow::Result<()> + Send),
    ) -> Result<DriverOutcome, DriverError> {
        // A Windows Job Object is the sanctioned future shape; in v0 the
        // driver refuses rather than run unsupervised.
        Err(DriverError::UnsupportedPlatform)
    }
}

/// Create the transcript + outbox dirs, clear the outbox, and name this
/// task's transcript file.
fn prepare_dirs(brief: &TaskBrief) -> Result<PathBuf, DriverError> {
    std::fs::create_dir_all(&brief.transcript_dir)
        .map_err(|e| DriverError::Spawn(format!("transcript dir: {e}")))?;
    if brief.outbox_dir.exists() {
        std::fs::remove_dir_all(&brief.outbox_dir)
            .map_err(|e| DriverError::Spawn(format!("outbox clear: {e}")))?;
    }
    std::fs::create_dir_all(&brief.outbox_dir)
        .map_err(|e| DriverError::Spawn(format!("outbox dir: {e}")))?;
    let stamp = Utc::now().format("%Y%m%dT%H%M%SZ");
    Ok(brief
        .transcript_dir
        .join(format!("{stamp}-{}.log", brief.kind.as_str())))
}

/// Read the typed outcome for the task kind (elicitation: the outbox
/// hand-off; drafting/repair: the transcript alone — the runner's own
/// compile/test decide everything else).
fn collect_outcome(brief: &TaskBrief, transcript_path: PathBuf) -> Result<DriverOutcome, DriverError> {
    match brief.kind {
        TaskBriefKind::Elicitation => {
            let candidate = brief.outbox_dir.join(OUTBOX_CANDIDATE);
            let candidate_spec_bytes = std::fs::read(&candidate).map_err(|e| {
                DriverError::OutboxMissing(format!(
                    "the worker did not write {} ({e})",
                    candidate.display()
                ))
            })?;
            let questions = read_questions(&brief.outbox_dir.join(OUTBOX_QUESTIONS))?;
            let expected_digest = rocky_core::product::spec::spec_digest(&candidate_spec_bytes);
            Ok(DriverOutcome::Elicitation {
                candidate_spec_bytes,
                questions,
                expected_digest,
                transcript_path,
            })
        }
        TaskBriefKind::Drafting | TaskBriefKind::Repair => {
            Ok(DriverOutcome::Drafting { transcript_path })
        }
    }
}

fn read_questions(path: &Path) -> Result<Vec<String>, DriverError> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let raw = std::fs::read(path)
        .map_err(|e| DriverError::OutboxMissing(format!("{}: {e}", path.display())))?;
    serde_json::from_slice::<Vec<String>>(&raw).map_err(|e| {
        DriverError::OutboxMissing(format!(
            "{} is not a JSON array of strings: {e}",
            path.display()
        ))
    })
}

// ---------------------------------------------------------------------------
// Group supervision (Unix)
// ---------------------------------------------------------------------------

/// Whether the process group still has members.
#[cfg(unix)]
pub fn group_exists(pgid: u32) -> bool {
    // SAFETY: `killpg` with signal 0 performs only an existence /
    // permission check; a stale pgid returns ESRCH. No memory is touched.
    let rc = unsafe { libc::killpg(pgid as libc::pid_t, 0) };
    if rc == 0 {
        return true;
    }
    // EPERM = the group exists but is not ours (should not happen for
    // groups we created; treated as existing so the caller escalates
    // instead of declaring victory).
    std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

/// Send `signal` to the whole group. ESRCH (already gone) is success.
#[cfg(unix)]
fn signal_group(pgid: u32, signal: libc::c_int) -> Result<(), DriverError> {
    // SAFETY: `killpg` with a valid pgid and a standard signal number is
    // a well-defined libc call with no memory-safety implications; a
    // stale pgid returns ESRCH, which is the "already gone" success arm.
    let rc = unsafe { libc::killpg(pgid as libc::pid_t, signal) };
    if rc == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH) {
        Ok(())
    } else {
        Err(DriverError::Survivors {
            pgid,
            detail: format!("killpg({signal}) failed: {}", std::io::Error::last_os_error()),
        })
    }
}

/// `killpg(SIGTERM)` → grace → `killpg(SIGKILL)` → no-survivors, on the
/// whole group. Safe to call when the group is already gone (the fast
/// path costs one probe). Also safe against the kill-race where members
/// exit exactly as the signals land: ESRCH is success everywhere.
#[cfg(unix)]
pub async fn kill_group(pgid: u32, grace: Duration) -> Result<(), DriverError> {
    if !group_exists(pgid) {
        return Ok(());
    }
    signal_group(pgid, libc::SIGTERM)?;
    // Poll the group down within the grace, then escalate.
    let deadline = tokio::time::Instant::now() + grace;
    while group_exists(pgid) && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    if group_exists(pgid) {
        signal_group(pgid, libc::SIGKILL)?;
    }
    assert_no_survivors(pgid).await
}

/// Poll until the group is gone; a group that survives SIGKILL past the
/// bounded window is a supervision failure surfaced as an error.
#[cfg(unix)]
pub async fn assert_no_survivors(pgid: u32) -> Result<(), DriverError> {
    // SIGKILL is not deliverable-refusable; the window only covers the
    // kernel reaping zombies whose parent (us) hasn't waited yet plus
    // scheduler latency. 5s is generous.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        if !group_exists(pgid) {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    Err(DriverError::Survivors {
        pgid,
        detail: "the group still exists 5s after SIGKILL".to_string(),
    })
}

/// Spawn an additional child INTO an existing group (the sibling-MCP
/// shape): `process_group(pgid)` = setpgid(0, pgid), joining the
/// leader's group so the group kill covers it.
#[cfg(unix)]
pub fn spawn_sibling_in_group(
    pgid: u32,
    program: &str,
    args: &[&str],
    cwd: &Path,
) -> Result<tokio::process::Child, DriverError> {
    let mut cmd = tokio::process::Command::new(program);
    cmd.args(args)
        .current_dir(cwd)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .process_group(pgid as i32);
    cmd.spawn()
        .map_err(|e| DriverError::Spawn(format!("sibling {program}: {e}")))
}

// ---------------------------------------------------------------------------
// The replay driver
// ---------------------------------------------------------------------------

/// A recorded worker session, executed verbatim against the
/// worker-profile MCP server — deterministic, credential-free.
pub struct ReplayDriver {
    session_path: PathBuf,
}

impl ReplayDriver {
    /// A driver over the session file at `session_path`.
    pub fn new(session_path: PathBuf) -> Self {
        Self { session_path }
    }
}

/// The session file shape (`[fulfill.driver] type = "replay"`).
#[derive(Debug, serde::Deserialize)]
struct ReplaySession {
    /// Overrides the MCP server command (defaults to
    /// `current_exe mcp --profile worker`). Recorded sessions in tests
    /// point this at the built binary.
    #[serde(default)]
    mcp_command: Option<Vec<String>>,
    /// Per-task recorded calls, keyed `elicitation` / `drafting` /
    /// `repair`.
    tasks: std::collections::BTreeMap<String, ReplayTask>,
}

#[derive(Debug, serde::Deserialize)]
struct ReplayTask {
    /// The MCP `tools/call` sequence to execute, in order.
    #[serde(default)]
    calls: Vec<ReplayCall>,
    /// The elicitation hand-off (required for the elicitation task).
    #[serde(default)]
    outcome: Option<ReplayElicitation>,
}

#[derive(Debug, serde::Deserialize)]
struct ReplayCall {
    /// The tool name.
    tool: String,
    /// The tool arguments.
    #[serde(default)]
    arguments: serde_json::Value,
    /// What the recorded session expects back.
    #[serde(default)]
    expect: ReplayExpect,
}

/// The expectation for one replayed call, keyed on the STRUCTURE of the
/// JSON-RPC response, never on diagnostic prose:
/// a protocol-level `error` member = the route does not exist (the
/// worker profile removed it); `result.isError` = the tool ran and
/// failed; anything else = success.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
enum ReplayExpect {
    /// The call must succeed.
    #[default]
    Ok,
    /// The call must be refused at the protocol level (tool-not-found) —
    /// the privilege-gate assertion.
    ToolNotFound,
    /// The call must run and fail (result.isError).
    Error,
}

#[derive(Debug, serde::Deserialize)]
struct ReplayElicitation {
    /// The candidate spec, as recorded text.
    candidate_spec: String,
    /// The worker's questions.
    #[serde(default)]
    questions: Vec<String>,
    /// The digest the recorded session claims for the bytes. The RUNNER
    /// re-computes and refuses a mismatch — a session whose digest lies
    /// is the integrity drill.
    expected_digest: String,
}

#[async_trait::async_trait]
impl AgentDriver for ReplayDriver {
    #[cfg(unix)]
    async fn run_task(
        &self,
        brief: &TaskBrief,
        on_group: &mut (dyn FnMut(GroupStamp) -> anyhow::Result<()> + Send),
    ) -> Result<DriverOutcome, DriverError> {
        let transcript_path = prepare_dirs(brief)?;
        let raw = std::fs::read(&self.session_path).map_err(|e| {
            DriverError::Session(format!("{}: {e}", self.session_path.display()))
        })?;
        let session: ReplaySession = serde_json::from_slice(&raw)
            .map_err(|e| DriverError::Session(format!("{}: {e}", self.session_path.display())))?;
        let task = session.tasks.get(brief.kind.as_str()).ok_or_else(|| {
            DriverError::Session(format!(
                "session has no '{}' task recorded",
                brief.kind.as_str()
            ))
        })?;

        // The server command: recorded override, or this very binary
        // (one-binary shape: the loop runs inside `rocky`).
        let argv: Vec<String> = match &session.mcp_command {
            Some(argv) if !argv.is_empty() => argv.clone(),
            _ => {
                let exe = std::env::current_exe()
                    .map_err(|e| DriverError::Spawn(format!("current_exe: {e}")))?;
                vec![
                    exe.display().to_string(),
                    "mcp".to_string(),
                    "--profile".to_string(),
                    "worker".to_string(),
                ]
            }
        };

        let mut transcript = std::fs::File::create(&transcript_path)
            .map_err(|e| DriverError::Spawn(format!("transcript file: {e}")))?;
        let stderr_file = transcript
            .try_clone()
            .map_err(|e| DriverError::Spawn(format!("transcript file: {e}")))?;

        let mut cmd = tokio::process::Command::new(&argv[0]);
        cmd.args(&argv[1..])
            .current_dir(&brief.project_root)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::from(stderr_file))
            // Same supervision as every driver: the server is the group
            // leader and dies with the group.
            .process_group(0);
        let mut server = cmd
            .spawn()
            .map_err(|e| DriverError::Spawn(format!("{}: {e}", argv[0])))?;
        let pgid = server
            .id()
            .ok_or_else(|| DriverError::Spawn("server exited before its pid was read".into()))?;
        let leader_start = crate::store::process_liveness(pgid)
            .ok()
            .flatten()
            .unwrap_or(0);
        on_group((pgid, leader_start)).map_err(|e| DriverError::Spawn(format!("{e:#}")))?;

        let result = replay_calls(&mut server, task, &mut transcript).await;

        // Group kill + no-survivors on every path (success and failure).
        drop(server); // close our pipe handles first so the child sees EOF
        kill_group(pgid, Duration::from_secs(5)).await?;

        result?;
        match brief.kind {
            TaskBriefKind::Elicitation => {
                let outcome = task.outcome.as_ref().ok_or_else(|| {
                    DriverError::Session("elicitation task has no recorded outcome".to_string())
                })?;
                Ok(DriverOutcome::Elicitation {
                    candidate_spec_bytes: outcome.candidate_spec.clone().into_bytes(),
                    questions: outcome.questions.clone(),
                    expected_digest: outcome.expected_digest.clone(),
                    transcript_path,
                })
            }
            TaskBriefKind::Drafting | TaskBriefKind::Repair => {
                Ok(DriverOutcome::Drafting { transcript_path })
            }
        }
    }

    #[cfg(not(unix))]
    async fn run_task(
        &self,
        _brief: &TaskBrief,
        _on_group: &mut (dyn FnMut(GroupStamp) -> anyhow::Result<()> + Send),
    ) -> Result<DriverOutcome, DriverError> {
        Err(DriverError::UnsupportedPlatform)
    }
}

/// Drive the MCP handshake + the recorded `tools/call` sequence over
/// stdio (line-delimited JSON-RPC), checking each call's expectation.
#[cfg(unix)]
async fn replay_calls(
    server: &mut tokio::process::Child,
    task: &ReplayTask,
    transcript: &mut std::fs::File,
) -> Result<(), DriverError> {
    use std::io::Write as _;
    use tokio::io::{AsyncBufReadExt, BufReader};

    let mut stdin = server
        .stdin
        .take()
        .ok_or_else(|| DriverError::Session("server stdin unavailable".to_string()))?;
    let stdout = server
        .stdout
        .take()
        .ok_or_else(|| DriverError::Session("server stdout unavailable".to_string()))?;
    let mut lines = BufReader::new(stdout).lines();

    /// One JSON-RPC exchange over the stdio pair. A request without an
    /// `id` is a notification: written, no response awaited.
    async fn roundtrip(
        stdin: &mut tokio::process::ChildStdin,
        lines: &mut tokio::io::Lines<BufReader<tokio::process::ChildStdout>>,
        request: serde_json::Value,
    ) -> Result<Option<serde_json::Value>, DriverError> {
        use tokio::io::AsyncWriteExt as _;
        const CALL_BUDGET: Duration = Duration::from_secs(120);
        let payload = serde_json::to_string(&request)
            .map_err(|e| DriverError::Session(format!("encode: {e}")))?;
        stdin
            .write_all(payload.as_bytes())
            .await
            .map_err(|e| DriverError::Session(format!("write: {e}")))?;
        stdin
            .write_all(b"\n")
            .await
            .map_err(|e| DriverError::Session(format!("write: {e}")))?;
        stdin
            .flush()
            .await
            .map_err(|e| DriverError::Session(format!("flush: {e}")))?;
        let Some(want_id) = request.get("id").cloned() else {
            return Ok(None); // a notification expects no response
        };
        loop {
            let line = tokio::time::timeout(CALL_BUDGET, lines.next_line())
                .await
                .map_err(|_| DriverError::Session("server response timed out".to_string()))?
                .map_err(|e| DriverError::Session(format!("read: {e}")))?
                .ok_or_else(|| {
                    DriverError::Session("server closed stdout mid-session".to_string())
                })?;
            let value: serde_json::Value = match serde_json::from_str(&line) {
                Ok(value) => value,
                Err(_) => continue, // tolerate stray non-JSON lines
            };
            if value.get("id") == Some(&want_id) {
                return Ok(Some(value));
            }
        }
    }

    let mut next_id: u64 = 0;

    // MCP handshake.
    let init = serde_json::json!({
        "jsonrpc": "2.0", "id": next_id, "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "rocky-fulfill-replay", "version": "0"}
        }
    });
    let response = roundtrip(&mut stdin, &mut lines, init).await?;
    log_exchange(transcript, "initialize", &response);
    next_id += 1;
    roundtrip(
        &mut stdin,
        &mut lines,
        serde_json::json!({
            "jsonrpc": "2.0", "method": "notifications/initialized"
        }),
    )
    .await?;

    for call in &task.calls {
        let request = serde_json::json!({
            "jsonrpc": "2.0", "id": next_id, "method": "tools/call",
            "params": {"name": call.tool, "arguments": call.arguments}
        });
        next_id += 1;
        let response = roundtrip(&mut stdin, &mut lines, request)
            .await?
            .ok_or_else(|| DriverError::Session("call produced no response".to_string()))?;
        log_exchange(transcript, &call.tool, &Some(response.clone()));

        // Structural classification (never diagnostic prose):
        // protocol error member = the route does not exist;
        // result.isError = the tool ran and failed; else success.
        let protocol_error = response.get("error").is_some();
        let tool_error = response
            .get("result")
            .and_then(|r| r.get("isError"))
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        let verdict_ok = match call.expect {
            ReplayExpect::Ok => !protocol_error && !tool_error,
            ReplayExpect::ToolNotFound => protocol_error,
            ReplayExpect::Error => !protocol_error && tool_error,
        };
        if !verdict_ok {
            return Err(DriverError::Session(format!(
                "call '{}' expected {:?} but got protocol_error={protocol_error} \
                 tool_error={tool_error}: {response}",
                call.tool, call.expect
            )));
        }
    }
    let _ = transcript.flush();
    Ok(())
}

#[cfg(unix)]
fn log_exchange(
    transcript: &mut std::fs::File,
    label: &str,
    response: &Option<serde_json::Value>,
) {
    use std::io::Write as _;
    let rendered = response
        .as_ref()
        .map(|v| v.to_string())
        .unwrap_or_else(|| "<no response>".to_string());
    let _ = writeln!(transcript, "== {label}\n{rendered}");
}
