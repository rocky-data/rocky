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
//! - After every window the driver asserts the GROUP is gone (`killpg
//!   (pgid, 0)` = ESRCH) before `run_task` returns, and best-effort
//!   sweeps the leader's direct children captured at kill time (covers
//!   a `setpgid`-only straggler). SCOPE, stated honestly: this contains
//!   COOPERATIVE and ACCIDENTAL descendants — helpers, the sibling MCP
//!   server, backgrounded grandchildren. A descendant that DELIBERATELY
//!   `setsid()`s into its own session escapes `killpg` by design of
//!   Unix process groups; no portable userspace kill closes that. That
//!   is the D7-conceded hostile-local-worker residual, pinned by an
//!   exhibit test below and addressed only by the Phase-2 OS-sandbox /
//!   PID-namespace containment.
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
    /// group killed (cooperative-descendant scope; see the module doc).
    /// The runner trusts NOTHING from it — its own compile/test decide.
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
    /// The process GROUP outlived the kill window — a supervision
    /// failure, never ignored. (A `setsid` escapee is a different,
    /// documented residual: it has LEFT the group and this error cannot
    /// see it.)
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
        prepare_dirs(brief)?;
        let argv: Vec<String> = self
            .command
            .iter()
            .map(|a| a.replace("{brief}", &brief.text))
            .collect();

        let (transcript, transcript_path) = create_transcript(brief)?;
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
        // S1: a failed stamp must never leak the group it just spawned —
        // the child is already running, so the error path kills and
        // reaps it (no-survivors asserted) BEFORE surfacing the failure.
        if let Err(stamp_err) = on_group((pgid, leader_start)) {
            kill_group(pgid, self.kill_grace, vec![&mut leader]).await?;
            return Err(DriverError::Spawn(format!("{stamp_err:#}")));
        }

        // Race the leader's natural exit against the task budget (the
        // schedule::spawn select shape).
        let waited = tokio::select! {
            status = leader.wait() => Some(status),
            () = tokio::time::sleep(self.timeout) => None,
        };

        // Group kill on EVERY path: after a normal leader exit AS WELL as
        // on timeout — leader exit does not imply grandchildren exit.
        kill_group(pgid, self.kill_grace, vec![&mut leader]).await?;

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

/// How many stamped transcript names to try before giving up. One UTC
/// second holding this many live transcripts for one product and one task
/// kind is not a collision, it is a runaway.
const TRANSCRIPT_NAME_ATTEMPTS: u32 = 64;

/// Create the transcript + outbox dirs and clear the outbox.
///
/// Stated residual, not a leaf problem: these DIRECTORIES are not
/// ancestor-checked. A symlink planted at `.rocky/fulfillment` (or at the
/// product directory under it) is an ancestor of BOTH, so it redirects both
/// `create_dir_all` calls, the outbox `remove_dir_all`, and the transcript
/// create in [`create_transcript`]. A symlink at `transcripts` or at
/// `outbox` alone redirects only its own side — the two are siblings. That
/// is the same parent-directory class the commit protocol states as open; it
/// needs dirfd-relative traversal, not another leaf guard.
fn prepare_dirs(brief: &TaskBrief) -> Result<(), DriverError> {
    std::fs::create_dir_all(&brief.transcript_dir)
        .map_err(|e| DriverError::Spawn(format!("transcript dir: {e}")))?;
    if brief.outbox_dir.exists() {
        std::fs::remove_dir_all(&brief.outbox_dir)
            .map_err(|e| DriverError::Spawn(format!("outbox clear: {e}")))?;
    }
    std::fs::create_dir_all(&brief.outbox_dir)
        .map_err(|e| DriverError::Spawn(format!("outbox dir: {e}")))?;
    Ok(())
}

/// CREATE this task's transcript and hand back the handle with the name it
/// landed on.
///
/// Called at the exact point the old `std::fs::File::create` was, so nothing
/// that used to run before the transcript existed now runs after it — in
/// particular [`ReplayDriver`] still reads and parses its session first, and
/// a malformed session leaves no transcript behind. It is not a guarantee
/// that a failed run never leaves one: the `try_clone` for stderr comes
/// after this, and a failure there returns with the file already created,
/// exactly as it did before.
///
/// The file is created here, not merely named, because the name alone is not
/// safe to hand to `std::fs::File::create`: that is O_CREAT|O_TRUNC, which
/// follows a symlink at the leaf and truncates through a hardlink. The name
/// is a plain UTC-second stamp in a directory a lower-privilege process can
/// write, so a squatter planted there took the worker's whole stdout and
/// stderr out of the project, or emptied a file the project does not own —
/// with no race to win. `create_new_no_follow_strict` is O_EXCL: what comes
/// back is always a file this process just created (#1500).
///
/// A UTC-second stamp also collides when two tasks of the same kind for the
/// same product start inside one second. The collision is resolved by trying
/// the next name, never by unlinking: unlinking would leave the first task
/// writing to an inode with no name and lose its transcript outright, which
/// is worse than what `File::create` did (it truncated, so both tasks at
/// least still had a file).
///
/// The ancestor residual on [`prepare_dirs`] applies here too — this create
/// guards the LEAF only.
fn create_transcript(brief: &TaskBrief) -> Result<(std::fs::File, PathBuf), DriverError> {
    let stamp = Utc::now().format("%Y%m%dT%H%M%SZ");
    let kind = brief.kind.as_str();
    for attempt in 0..TRANSCRIPT_NAME_ATTEMPTS {
        let name = if attempt == 0 {
            format!("{stamp}-{kind}.log")
        } else {
            format!("{stamp}-{kind}.{}.log", attempt + 1)
        };
        let path = brief.transcript_dir.join(name);
        match rocky_core::product::commit::create_new_no_follow_strict(&path) {
            Ok(file) => return Ok((file, path)),
            // Occupied — by a live peer's transcript, or by a squatter the
            // O_EXCL create refused to open. Either way: take another name,
            // never remove what is there.
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(DriverError::Spawn(format!("transcript file: {e}"))),
        }
    }
    Err(DriverError::Spawn(format!(
        "transcript file: every one of {TRANSCRIPT_NAME_ATTEMPTS} names for this second is \
         already taken under {}",
        brief.transcript_dir.display()
    )))
}

/// Read the typed outcome for the task kind (elicitation: the outbox
/// hand-off; drafting/repair: the transcript alone — the runner's own
/// compile/test decide everything else).
fn collect_outcome(
    brief: &TaskBrief,
    transcript_path: PathBuf,
) -> Result<DriverOutcome, DriverError> {
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
            detail: format!(
                "killpg({signal}) failed: {}",
                std::io::Error::last_os_error()
            ),
        })
    }
}

/// `killpg(SIGTERM)` → reap the direct child → grace → `killpg(SIGKILL)`
/// → no-survivors, on the whole group.
///
/// `own` must name EVERY group member that is this process's direct
/// child and is still held by the caller (the leader, and any sibling
/// the driver spawned into the group): each must be reaped (waited)
/// between the signals, because an unreaped zombie of ours keeps the
/// pgid alive and — on macOS — makes `killpg` return EPERM for the
/// whole group. Members that are NOT our children (orphaned
/// grandchildren) are reaped by init/launchd once killed; the
/// escalation below retries `SIGKILL` through that reap window instead
/// of treating a transient EPERM as either success or failure.
///
/// Safe when the group is already gone (ESRCH is success everywhere)
/// and against the kill-race where members exit exactly as the signals
/// land.
#[cfg(unix)]
pub async fn kill_group(
    pgid: u32,
    grace: Duration,
    mut own: Vec<&mut tokio::process::Child>,
) -> Result<(), DriverError> {
    // Best-effort straggler snapshot: the leader's LIVE direct children
    // (pid + start time) captured before the first signal. A child that
    // left the group (`setpgid`, or a direct `setsid`) escapes `killpg`
    // but is still swept at the end, start-time-guarded so a recycled
    // pid is never killed. A DOUBLE-FORKED escapee (daemonize: the
    // middle process exits, the grandchild reparents to init) is not a
    // direct child and stays out of reach — the documented residual.
    let stragglers: Vec<(u32, u64)> = direct_children(pgid)
        .into_iter()
        .filter_map(|pid| {
            crate::store::process_liveness(pid)
                .ok()
                .flatten()
                .map(|start| (pid, start))
        })
        .collect();
    if group_exists(pgid) {
        signal_group(pgid, libc::SIGTERM)?;
    }
    // Reap our direct children within the grace: they usually exit on
    // SIGTERM, and an unreaped zombie would wedge the group probe.
    // `Child::wait` is cancel-safe, so a timeout here loses nothing.
    let reap_deadline = tokio::time::Instant::now() + grace;
    for child in own.iter_mut() {
        let left = reap_deadline.saturating_duration_since(tokio::time::Instant::now());
        let _ = tokio::time::timeout(left, child.wait()).await;
    }
    while group_exists(pgid) && tokio::time::Instant::now() < reap_deadline {
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    // Escalate + assert, retrying SIGKILL through zombie-reap windows: a
    // live member dies on a successful killpg; a zombie-only group
    // clears as its reapers run. Only a group still standing at the end
    // of the window is a supervision failure.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut last_detail = String::new();
    loop {
        if !group_exists(pgid) {
            // One final reap so a child that only died on SIGKILL never
            // outlives this call as a zombie.
            for child in own.iter_mut() {
                let _ = child.wait().await;
            }
            sweep_stragglers(&stragglers);
            return Ok(());
        }
        // SAFETY: `killpg` with a valid pgid and SIGKILL is a
        // well-defined libc call with no memory-safety implications;
        // ESRCH ("already gone") and EPERM ("only zombies remain",
        // observed on macOS) are handled by the surrounding poll.
        let rc = unsafe { libc::killpg(pgid as libc::pid_t, libc::SIGKILL) };
        if rc != 0 {
            let err = std::io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::ESRCH) {
                continue; // gone between the probe and the signal
            }
            last_detail = format!("killpg(SIGKILL) said: {err}");
        }
        for child in own.iter_mut() {
            let _ = tokio::time::timeout(Duration::from_millis(50), child.wait()).await;
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(DriverError::Survivors {
                pgid,
                detail: if last_detail.is_empty() {
                    "the group still exists 5s after SIGKILL".to_string()
                } else {
                    format!("the group still exists 5s after SIGKILL ({last_detail})")
                },
            });
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// Kill any snapshot straggler that is STILL the same process (start
/// time matches) — it left the process group, so `killpg` missed it.
/// Best-effort: every error is ignored (the straggler may have exited,
/// or the pid may have been recycled — the start-time guard refuses
/// those).
#[cfg(unix)]
fn sweep_stragglers(stragglers: &[(u32, u64)]) {
    for (pid, start) in stragglers {
        if crate::store::process_liveness(*pid) == Ok(Some(*start)) {
            // SAFETY: `kill` with a verified-identity pid and SIGKILL is
            // a well-defined libc call; ESRCH (already gone) is ignored.
            unsafe {
                libc::kill(*pid as libc::pid_t, libc::SIGKILL);
            }
        }
    }
}

/// Best-effort list of `pid`'s LIVE direct children. Empty on any
/// failure — the sweep this feeds is an extra net, never a guarantee.
#[cfg(target_os = "macos")]
fn direct_children(pid: u32) -> Vec<u32> {
    let mut buffer = vec![0i32; 512];
    // SAFETY: `proc_listchildpids` writes at most `buffersize` bytes
    // into `buffer`; the buffer is exactly the size passed and no
    // pointer outlives the call.
    let rc = unsafe {
        libc::proc_listchildpids(
            pid as libc::pid_t,
            buffer.as_mut_ptr().cast(),
            (buffer.len() * std::mem::size_of::<i32>()) as libc::c_int,
        )
    };
    if rc <= 0 {
        return Vec::new();
    }
    // The return value is the number of entries filled (capped by the
    // buffer); guard against byte-count semantics by clamping.
    let count = (rc as usize).min(buffer.len());
    buffer[..count]
        .iter()
        .filter(|child| **child > 0)
        .map(|child| *child as u32)
        .collect()
}

#[cfg(target_os = "linux")]
fn direct_children(pid: u32) -> Vec<u32> {
    // /proc/<pid>/task/<tid>/children lists direct children on modern
    // kernels; fall back to empty on any read failure.
    let path = format!("/proc/{pid}/task/{pid}/children");
    std::fs::read_to_string(path)
        .map(|list| {
            list.split_ascii_whitespace()
                .filter_map(|token| token.parse::<u32>().ok())
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(all(unix, not(any(target_os = "macos", target_os = "linux"))))]
fn direct_children(_pid: u32) -> Vec<u32> {
    Vec::new()
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
        prepare_dirs(brief)?;
        let raw = std::fs::read(&self.session_path)
            .map_err(|e| DriverError::Session(format!("{}: {e}", self.session_path.display())))?;
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

        let (mut transcript, transcript_path) = create_transcript(brief)?;
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
        // S1: same rule as the subprocess driver — a failed stamp kills
        // and reaps the just-spawned server before surfacing the error.
        if let Err(stamp_err) = on_group((pgid, leader_start)) {
            kill_group(pgid, Duration::from_secs(5), vec![&mut server]).await?;
            return Err(DriverError::Spawn(format!("{stamp_err:#}")));
        }

        let result = replay_calls(&mut server, task, &mut transcript).await;

        // Group kill + no-survivors on every path (success and failure).
        // The server's stdin handle dropped inside `replay_calls`, so it
        // already saw EOF; the kill sweeps it and anything it spawned.
        kill_group(pgid, Duration::from_secs(5), vec![&mut server]).await?;

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
fn log_exchange(transcript: &mut std::fs::File, label: &str, response: &Option<serde_json::Value>) {
    use std::io::Write as _;
    let rendered = response
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_else(|| "<no response>".to_string());
    let _ = writeln!(transcript, "== {label}\n{rendered}");
}

// ---------------------------------------------------------------------------
// The supervision battery (Unix)
// ---------------------------------------------------------------------------

#[cfg(all(test, unix))]
mod supervision_tests {
    use super::*;

    fn brief(kind: TaskBriefKind, dir: &Path) -> TaskBrief {
        TaskBrief {
            kind,
            text: "battery".to_string(),
            product: "battery".to_string(),
            project_root: dir.to_path_buf(),
            transcript_dir: dir.join("transcripts"),
            outbox_dir: dir.join("outbox"),
        }
    }

    fn subprocess(command: &[&str], timeout: Duration, grace: Duration) -> SubprocessDriver {
        SubprocessDriver::new(
            command.iter().map(ToString::to_string).collect(),
            Vec::new(),
            timeout,
            grace,
        )
        .expect("valid template")
    }

    /// Drive one task, capturing the group stamp.
    async fn run(
        driver: &SubprocessDriver,
        brief: &TaskBrief,
    ) -> (Result<DriverOutcome, DriverError>, Option<GroupStamp>) {
        let mut stamp: Option<GroupStamp> = None;
        let mut on_group = |group: GroupStamp| {
            stamp = Some(group);
            Ok(())
        };
        let outcome = driver.run_task(brief, &mut on_group).await;
        (outcome, stamp)
    }

    /// Battery case: an orphaned grandchild — backgrounded by the
    /// leader, which then exits 0 — dies with the group. The leader's
    /// NORMAL exit must still trigger the group kill.
    #[tokio::test]
    async fn orphan_grandchild_dies_with_the_group() {
        let dir = tempfile::tempdir().expect("tempdir");
        let driver = subprocess(
            &["/bin/sh", "-c", "(sleep 300 &); echo {brief}; exit 0"],
            Duration::from_secs(30),
            Duration::from_secs(2),
        );
        let brief = brief(TaskBriefKind::Drafting, dir.path());
        let (outcome, stamp) = run(&driver, &brief).await;
        let (pgid, _) = stamp.expect("the group was stamped");
        assert!(
            matches!(outcome, Ok(DriverOutcome::Drafting { .. })),
            "{outcome:?}"
        );
        // No-survivors after the window: the orphan is gone.
        assert!(
            !group_exists(pgid),
            "the backgrounded grandchild must not survive the leader's normal exit"
        );
    }

    /// Battery case: a sibling spawned INTO the leader's group (the
    /// sibling-MCP shape) dies with the group kill even when the leader
    /// is long gone.
    #[tokio::test]
    async fn sibling_in_group_dies_with_the_group() {
        let dir = tempfile::tempdir().expect("tempdir");
        // A leader that lives long enough to attach a sibling to.
        let mut leader = tokio::process::Command::new("/bin/sleep");
        leader
            .arg("300")
            .stdin(std::process::Stdio::null())
            .process_group(0);
        let mut leader = leader.spawn().expect("leader");
        let pgid = leader.id().expect("pid");

        let mut sibling = spawn_sibling_in_group(pgid, "/bin/sleep", &["300"], dir.path())
            .expect("sibling joins the group");
        assert!(group_exists(pgid));

        kill_group(
            pgid,
            Duration::from_secs(2),
            vec![&mut leader, &mut sibling],
        )
        .await
        .expect("group kill");
        assert!(
            !group_exists(pgid),
            "leader AND sibling must be gone after the group kill"
        );
    }

    /// S1: a failed ownership-stamp callback must not leak the group it
    /// just spawned — the error path kills, reaps, and asserts no
    /// survivors BEFORE returning the failure.
    #[tokio::test]
    async fn a_failed_stamp_kills_the_just_spawned_group() {
        let dir = tempfile::tempdir().expect("tempdir");
        let driver = subprocess(
            &["/bin/sh", "-c", "sleep 300 & sleep 300; echo {brief}"],
            Duration::from_secs(30),
            Duration::from_millis(300),
        );
        let brief = brief(TaskBriefKind::Drafting, dir.path());
        let mut stamp: Option<GroupStamp> = None;
        let mut on_group = |group: GroupStamp| {
            stamp = Some(group);
            anyhow::bail!("the CAS under the stamp was lost")
        };
        let outcome = driver.run_task(&brief, &mut on_group).await;
        let (pgid, _) = stamp.expect("the callback fired");
        match outcome {
            Err(DriverError::Spawn(message)) => {
                assert!(
                    message.contains("the CAS under the stamp was lost"),
                    "{message}"
                );
            }
            other => panic!("expected the stamp failure surfaced, got {other:?}"),
        }
        assert!(
            !group_exists(pgid),
            "the spawned group must be dead after a stamp failure"
        );
    }

    /// Battery case: the kill-race — children exiting exactly as the
    /// timeout fires. Every iteration must end coherently (a normal
    /// outcome or a typed timeout, never a panic or an errno surprise)
    /// with no survivors.
    #[tokio::test]
    async fn kill_race_during_child_exit_is_coherent() {
        let dir = tempfile::tempdir().expect("tempdir");
        for i in 0..10 {
            let driver = subprocess(
                &["/bin/sh", "-c", "sleep 0.1; echo {brief}"],
                Duration::from_millis(100),
                Duration::from_millis(200),
            );
            let brief = brief(TaskBriefKind::Drafting, dir.path());
            let (outcome, stamp) = run(&driver, &brief).await;
            let (pgid, _) = stamp.expect("stamped");
            match &outcome {
                Ok(DriverOutcome::Drafting { .. }) | Err(DriverError::Timeout { .. }) => {}
                other => panic!("iteration {i}: incoherent outcome {other:?}"),
            }
            assert!(!group_exists(pgid), "iteration {i}: survivors");
        }
    }

    /// Battery case: the timeout path kills the WHOLE group (leader +
    /// grandchild), reports the typed timeout, and leaves no survivors.
    #[tokio::test]
    async fn timeout_kills_the_whole_group() {
        let dir = tempfile::tempdir().expect("tempdir");
        let driver = subprocess(
            &["/bin/sh", "-c", "sleep 300 & sleep 300; echo {brief}"],
            Duration::from_millis(200),
            Duration::from_millis(300),
        );
        let brief = brief(TaskBriefKind::Drafting, dir.path());
        let (outcome, stamp) = run(&driver, &brief).await;
        let (pgid, _) = stamp.expect("stamped");
        assert!(
            matches!(outcome, Err(DriverError::Timeout { .. })),
            "{outcome:?}"
        );
        assert!(!group_exists(pgid), "survivors after the timeout kill");
    }

    /// Battery case: killing an already-dead group is success (the
    /// ESRCH arm), and probing it says gone — the PID-reuse guard's
    /// group-level counterpart.
    #[tokio::test]
    async fn killing_a_dead_group_is_a_clean_noop() {
        let mut child = tokio::process::Command::new("/bin/sh");
        child
            .args(["-c", "exit 0"])
            .stdin(std::process::Stdio::null())
            .process_group(0);
        let mut child = child.spawn().expect("spawn");
        let pgid = child.id().expect("pid");
        child.wait().await.expect("wait");
        // The group may briefly exist while the zombie is reaped; the
        // kill path handles both sides of that race.
        kill_group(pgid, Duration::from_millis(100), vec![])
            .await
            .expect("ESRCH is success");
        assert!(!group_exists(pgid));
    }

    /// The worker sees ONLY `env_allow`: an allowed variable passes
    /// through, an un-allowed one is absent.
    #[tokio::test]
    async fn env_is_only_the_allowlist() {
        let dir = tempfile::tempdir().expect("tempdir");
        let probe = dir.path().join("env-probe");
        // SAFETY: test-process env mutation is serialized by the test
        // harness convention (no other test reads these names).
        unsafe {
            std::env::set_var("ROCKY_FULFILL_BATTERY_ALLOWED", "yes");
            std::env::set_var("ROCKY_FULFILL_BATTERY_BLOCKED", "leak");
        }
        let script = format!(
            "printf '%s,%s' \"$ROCKY_FULFILL_BATTERY_ALLOWED\" \
             \"$ROCKY_FULFILL_BATTERY_BLOCKED\" > {}; echo {{brief}}",
            probe.display()
        );
        let driver = SubprocessDriver::new(
            vec!["/bin/sh".into(), "-c".into(), script],
            vec!["ROCKY_FULFILL_BATTERY_ALLOWED".into()],
            Duration::from_secs(30),
            Duration::from_secs(2),
        )
        .expect("valid");
        let brief = brief(TaskBriefKind::Drafting, dir.path());
        let (outcome, _) = run(&driver, &brief).await;
        assert!(outcome.is_ok(), "{outcome:?}");
        let seen = std::fs::read_to_string(&probe).expect("probe file");
        assert_eq!(seen, "yes,", "allowed passes; everything else is cleared");
    }

    /// The transcript captures the worker's stdout and stderr.
    #[tokio::test]
    async fn transcript_captures_stdout_and_stderr() {
        let dir = tempfile::tempdir().expect("tempdir");
        let driver = subprocess(
            &[
                "/bin/sh",
                "-c",
                "echo {brief}; echo out-line; echo err-line >&2",
            ],
            Duration::from_secs(30),
            Duration::from_secs(2),
        );
        let brief = brief(TaskBriefKind::Drafting, dir.path());
        let (outcome, _) = run(&driver, &brief).await;
        let Ok(DriverOutcome::Drafting { transcript_path }) = outcome else {
            panic!("{outcome:?}");
        };
        let transcript = std::fs::read_to_string(&transcript_path).expect("transcript");
        assert!(transcript.contains("out-line"));
        assert!(transcript.contains("err-line"));
    }

    /// The elicitation outbox contract: candidate + questions read back,
    /// digest computed over the exact bytes; a missing candidate is the
    /// typed outbox error.
    #[tokio::test]
    async fn elicitation_outbox_round_trips_and_missing_candidate_is_typed() {
        let dir = tempfile::tempdir().expect("tempdir");
        let brief = brief(TaskBriefKind::Elicitation, dir.path());
        let outbox = brief.outbox_dir.display().to_string();
        let script = format!(
            "printf '[product]\\nname = \"battery\"\\n' > {outbox}/candidate_spec.toml; \
             printf '[\"q1\",\"q2\"]' > {outbox}/questions.json; echo {{brief}}"
        );
        let driver = SubprocessDriver::new(
            vec!["/bin/sh".into(), "-c".into(), script],
            Vec::new(),
            Duration::from_secs(30),
            Duration::from_secs(2),
        )
        .expect("valid");
        let (outcome, _) = run(&driver, &brief).await;
        let Ok(DriverOutcome::Elicitation {
            candidate_spec_bytes,
            questions,
            expected_digest,
            ..
        }) = outcome
        else {
            panic!("{outcome:?}");
        };
        assert_eq!(
            String::from_utf8_lossy(&candidate_spec_bytes),
            "[product]\nname = \"battery\"\n"
        );
        assert_eq!(questions, vec!["q1".to_string(), "q2".to_string()]);
        assert_eq!(
            expected_digest,
            rocky_core::product::spec::spec_digest(&candidate_spec_bytes)
        );

        // No candidate written → the typed outbox error, not a success.
        let empty = subprocess(
            &["/bin/sh", "-c", "echo {brief}"],
            Duration::from_secs(30),
            Duration::from_secs(2),
        );
        let (outcome, _) = run(&empty, &brief).await;
        assert!(
            matches!(outcome, Err(DriverError::OutboxMissing(_))),
            "{outcome:?}"
        );
    }

    /// A non-zero worker exit is the typed task failure carrying the
    /// transcript path, and the group still dies.
    #[tokio::test]
    async fn nonzero_exit_is_a_typed_failure_with_no_survivors() {
        let dir = tempfile::tempdir().expect("tempdir");
        let driver = subprocess(
            &["/bin/sh", "-c", "sleep 300 & echo {brief}; exit 3"],
            Duration::from_secs(30),
            Duration::from_millis(300),
        );
        let brief = brief(TaskBriefKind::Drafting, dir.path());
        let (outcome, stamp) = run(&driver, &brief).await;
        let (pgid, _) = stamp.expect("stamped");
        match outcome {
            Err(DriverError::TaskFailed { exit_code, .. }) => assert_eq!(exit_code, 3),
            other => panic!("{other:?}"),
        }
        assert!(!group_exists(pgid), "survivors after a failure exit");
    }

    /// Template validation: no `{brief}` slot, two slots, and an empty
    /// command are all typed config refusals.
    #[test]
    fn template_validation_refuses_bad_shapes() {
        for command in [
            vec![],
            vec!["/bin/echo".to_string()],
            vec![
                "/bin/echo".to_string(),
                "{brief}".to_string(),
                "{brief}".to_string(),
            ],
        ] {
            let result = SubprocessDriver::new(
                command,
                Vec::new(),
                Duration::from_secs(1),
                Duration::from_secs(1),
            );
            assert!(matches!(result, Err(DriverError::Config(_))));
        }
    }

    /// The group stamp reaches the runner BEFORE the task ends, with the
    /// leader's real start time (PID-reuse-proof pair).
    #[tokio::test]
    async fn the_group_stamp_carries_the_leaders_start_time() {
        let dir = tempfile::tempdir().expect("tempdir");
        let driver = subprocess(
            &["/bin/sh", "-c", "echo {brief}"],
            Duration::from_secs(30),
            Duration::from_secs(2),
        );
        let brief = brief(TaskBriefKind::Drafting, dir.path());
        let (outcome, stamp) = run(&driver, &brief).await;
        assert!(outcome.is_ok(), "{outcome:?}");
        let (pgid, start) = stamp.expect("stamped");
        assert!(pgid > 0);
        // The leader is gone now, so its recorded start time cannot be
        // re-read — but the stamp must have carried a NONZERO probe
        // value (0 = the probe failed at spawn, which would neuter the
        // takeover sweep's reuse guard).
        assert!(start > 0, "the stamp must carry a real start time");
    }
}

#[cfg(all(test, not(unix)))]
mod windows_tests {
    use super::*;

    /// v0 refuses to run unsupervised on Windows — typed, not a panic.
    #[tokio::test]
    async fn run_task_refuses_on_windows() {
        let driver = SubprocessDriver::new(
            vec!["agent.exe".into(), "{brief}".into()],
            Vec::new(),
            Duration::from_secs(1),
            Duration::from_secs(1),
        )
        .expect("template validates on every platform");
        let dir = std::env::temp_dir().join("rocky-fulfill-win-test");
        let brief = TaskBrief {
            kind: TaskBriefKind::Drafting,
            text: "x".into(),
            product: "p".into(),
            project_root: dir.clone(),
            transcript_dir: dir.join("t"),
            outbox_dir: dir.join("o"),
        };
        let mut on_group = |_group: GroupStamp| Ok(());
        let outcome = driver.run_task(&brief, &mut on_group).await;
        assert!(matches!(outcome, Err(DriverError::UnsupportedPlatform)));
    }
}

#[cfg(all(test, unix))]
mod escape_scope_tests {
    use super::*;

    fn brief(dir: &Path) -> TaskBrief {
        TaskBrief {
            kind: TaskBriefKind::Drafting,
            text: "battery".to_string(),
            product: "battery".to_string(),
            project_root: dir.to_path_buf(),
            transcript_dir: dir.join("transcripts"),
            outbox_dir: dir.join("outbox"),
        }
    }

    async fn wait_for_pid_file(path: &Path) -> u32 {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if let Ok(raw) = std::fs::read_to_string(path)
                && let Ok(pid) = raw.trim().parse::<u32>()
            {
                return pid;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "escapee pid file never appeared at {}",
                path.display()
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// Plant a squatter at the transcript name for `seconds` consecutive UTC
    /// seconds starting from ONE reading of the clock, so a run that starts
    /// within that window lands on a planted name.
    ///
    /// One reading, not one per offset: separate readings can straddle a
    /// second boundary and leave a hole in the middle of the window for the
    /// run to fall into. The window is still finite — a run that takes longer
    /// than `seconds` to reach the create misses it — which is what the
    /// caller's `replaced_by_this_run` assertion is for: a miss FAILS the
    /// test rather than passing it having exercised nothing.
    fn plant_transcript_squatters(
        brief: &TaskBrief,
        seconds: i64,
        mut plant: impl FnMut(&Path),
    ) -> Vec<PathBuf> {
        std::fs::create_dir_all(&brief.transcript_dir).expect("mkdir");
        let now = Utc::now();
        (0..seconds)
            .map(|offset| {
                let stamp = (now + chrono::Duration::seconds(offset))
                    .format("%Y%m%dT%H%M%SZ")
                    .to_string();
                let path = brief
                    .transcript_dir
                    .join(format!("{stamp}-{}.log", brief.kind.as_str()));
                plant(&path);
                path
            })
            .collect()
    }

    /// The worker prints its brief, so a transcript this run actually wrote
    /// holds exactly these bytes.
    const WORKER_OUTPUT: &[u8] = b"battery\n";

    fn run_a_trivial_task(brief: &TaskBrief) {
        let driver = SubprocessDriver::new(
            vec!["python3".into(), "-c".into(), "print(\"{brief}\")".into()],
            vec!["PATH".into()],
            Duration::from_secs(20),
            Duration::from_millis(300),
        )
        .expect("valid");
        let mut on_group = |_group: GroupStamp| Ok(());
        let runtime = tokio::runtime::Handle::current();
        let outcome = runtime.block_on(async { driver.run_task(brief, &mut on_group).await });
        assert!(
            matches!(outcome, Ok(DriverOutcome::Drafting { .. })),
            "the trivial task must complete: {outcome:?}"
        );
    }

    /// The `.2.log` sibling of each name — where a run that found the first
    /// name occupied must have written instead.
    fn next_names(planted: &[PathBuf]) -> Vec<PathBuf> {
        planted
            .iter()
            .map(|path| {
                let name = path.file_name().expect("name").to_string_lossy();
                path.with_file_name(name.replace(".log", ".2.log"))
            })
            .collect()
    }

    /// How many of `paths` are now a REGULAR file holding this run's output.
    ///
    /// The anti-vacuity assertion. An occupied name is taken AROUND, so the
    /// proof that the run actually met the squatter is that it wrote the next
    /// name. If the run never reached a planted second this is 0, and a test
    /// that only checked the victim would pass having proved nothing.
    fn replaced_by_this_run(paths: &[PathBuf]) -> usize {
        paths
            .iter()
            .filter(|path| {
                std::fs::symlink_metadata(path).is_ok_and(|m| m.file_type().is_file())
                    && std::fs::read(path).is_ok_and(|bytes| bytes == WORKER_OUTPUT)
            })
            .count()
    }

    /// The worker's transcript is the one file this driver creates at a
    /// PREDICTABLE name (a UTC-second stamp) in a directory a lower-privilege
    /// process can write. `std::fs::File::create` truncated through a hard
    /// link planted there — no race to win — emptying a file outside the
    /// project before the worker even started (#1500).
    #[tokio::test(flavor = "multi_thread")]
    async fn a_hardlinked_transcript_name_is_never_truncated_through() {
        let dir = tempfile::tempdir().expect("tempdir");
        let victim = dir.path().join("outside-secret");
        std::fs::write(&victim, b"private bytes the transcript must not truncate").expect("write");
        let brief = brief(dir.path());
        let planted = plant_transcript_squatters(&brief, 4, |path| {
            std::fs::hard_link(&victim, path).expect("hard link");
        });

        tokio::task::block_in_place(|| run_a_trivial_task(&brief));

        assert_eq!(
            std::fs::read(&victim).expect("still there"),
            b"private bytes the transcript must not truncate",
            "the out-of-project name must be untouched"
        );
        for path in &planted {
            assert!(
                std::fs::read(path).expect("still there")
                    == b"private bytes the transcript must not truncate",
                "the planted link is left in place, not unlinked: {}",
                path.display()
            );
        }
        assert_eq!(
            replaced_by_this_run(&next_names(&planted)),
            1,
            "the run must have met a planted name and written the next one — \
             otherwise it never reached a planted second and this proves nothing"
        );
    }

    /// The same leaf, the other link kind: `File::create` FOLLOWED a symlink
    /// planted at the transcript name and redirected the worker's entire
    /// stdout and stderr through it.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_symlinked_transcript_name_is_never_written_through() {
        let dir = tempfile::tempdir().expect("tempdir");
        let victim = dir.path().join("outside-secret");
        std::fs::write(&victim, b"private bytes the transcript must not overwrite").expect("write");
        let brief = brief(dir.path());
        let planted = plant_transcript_squatters(&brief, 4, |path| {
            std::os::unix::fs::symlink(&victim, path).expect("symlink");
        });

        tokio::task::block_in_place(|| run_a_trivial_task(&brief));

        assert_eq!(
            std::fs::read(&victim).expect("still there"),
            b"private bytes the transcript must not overwrite",
            "the out-of-project target must be untouched"
        );
        for path in &planted {
            assert!(
                std::fs::symlink_metadata(path).is_ok_and(|m| m.file_type().is_symlink()),
                "the planted link is left in place, not unlinked: {}",
                path.display()
            );
        }
        assert_eq!(
            replaced_by_this_run(&next_names(&planted)),
            1,
            "the run must have met a planted name and written the next one — \
             otherwise it never reached a planted second and this proves nothing"
        );
    }

    /// The other half of the O_EXCL choice: a stamped name can legitimately
    /// be held by a LIVE peer, so the collision takes the NEXT name. Clearing
    /// it — as the commit protocol's own scratch create does for its own
    /// names — would leave that peer writing to an unlinked inode and lose
    /// its transcript, which is worse than the truncate this replaced.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_transcript_name_already_held_is_taken_around_not_unlinked() {
        let dir = tempfile::tempdir().expect("tempdir");
        let brief = brief(dir.path());
        let held = plant_transcript_squatters(&brief, 4, |path| {
            std::fs::write(path, b"a live peer's transcript").expect("write");
        });

        tokio::task::block_in_place(|| run_a_trivial_task(&brief));

        for path in &held {
            assert_eq!(
                std::fs::read(path).expect("still there"),
                b"a live peer's transcript",
                "a held name is neither unlinked nor truncated: {}",
                path.display()
            );
        }
        assert_eq!(
            replaced_by_this_run(&next_names(&held)),
            1,
            "the run must have written the next name instead"
        );
    }

    /// A DIRECT child that `setsid()`s out of the group escapes
    /// `killpg` — but it was snapshotted as a direct child at kill
    /// time, so the start-time-guarded straggler sweep still gets it.
    #[tokio::test]
    async fn a_direct_setsid_straggler_is_swept() {
        let dir = tempfile::tempdir().expect("tempdir");
        let escapee_file = dir.path().join("escapee.pid");
        let script = format!(
            "import os, time\n\
             note = \"{{brief}}\"\n\
             pid = os.fork()\n\
             if pid == 0:\n\
             \x20   os.setsid()\n\
             \x20   open(\"{}\", \"w\").write(str(os.getpid()))\n\
             \x20   time.sleep(300)\n\
             else:\n\
             \x20   time.sleep(300)\n",
            escapee_file.display()
        );
        let driver = SubprocessDriver::new(
            vec!["python3".into(), "-c".into(), script],
            vec!["PATH".into()],
            Duration::from_millis(1500),
            Duration::from_millis(300),
        )
        .expect("valid");
        let brief = brief(dir.path());
        let mut on_group = |_group: GroupStamp| Ok(());
        let outcome = driver.run_task(&brief, &mut on_group).await;
        assert!(
            matches!(outcome, Err(DriverError::Timeout { .. })),
            "{outcome:?}"
        );
        let escapee = wait_for_pid_file(&escapee_file).await;
        // The sweep ran inside run_task; the escapee must be gone (allow
        // a short reap window).
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        loop {
            match crate::store::process_liveness(escapee) {
                Ok(None) => break,
                Ok(Some(_)) if tokio::time::Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
                Ok(Some(_)) => {
                    // Cleanup before failing so the box is not littered.
                    // SAFETY: best-effort SIGKILL on the test's own child.
                    unsafe {
                        libc::kill(escapee as libc::pid_t, libc::SIGKILL);
                    }
                    panic!("a direct setsid straggler survived the sweep");
                }
                Err(_) => break,
            }
        }
    }

    /// The PINNED RESIDUAL: a double-forked escapee (daemonize — the
    /// middle process `setsid()`s, forks, and exits; the grandchild
    /// reparents to init) SURVIVES the group kill AND the direct-child
    /// sweep. This is inherent to Unix process groups: no portable
    /// userspace kill reaches it. It is the D7-conceded hostile-worker
    /// boundary; the Phase-2 OS-sandbox / PID-namespace containment is
    /// the real fix. This test EXHIBITS the escape so the limitation
    /// can never silently regress into a false "nothing survives"
    /// guarantee.
    #[tokio::test]
    async fn a_double_forked_setsid_escapee_survives_the_group_kill() {
        let dir = tempfile::tempdir().expect("tempdir");
        let escapee_file = dir.path().join("escapee.pid");
        let script = format!(
            "import os, time\n\
             note = \"{{brief}}\"\n\
             if os.fork() == 0:\n\
             \x20   os.setsid()\n\
             \x20   if os.fork() == 0:\n\
             \x20       open(\"{}\", \"w\").write(str(os.getpid()))\n\
             \x20       time.sleep(300)\n\
             \x20   os._exit(0)\n\
             time.sleep(0.5)\n",
            escapee_file.display()
        );
        let driver = SubprocessDriver::new(
            vec!["python3".into(), "-c".into(), script],
            vec!["PATH".into()],
            Duration::from_secs(30),
            Duration::from_millis(300),
        )
        .expect("valid");
        let brief = brief(dir.path());
        let mut on_group = |_group: GroupStamp| Ok(());
        let outcome = driver.run_task(&brief, &mut on_group).await;
        assert!(
            matches!(outcome, Ok(DriverOutcome::Drafting { .. })),
            "{outcome:?}"
        );
        let escapee = wait_for_pid_file(&escapee_file).await;
        let alive = crate::store::process_liveness(escapee)
            .expect("probe")
            .is_some();
        // Clean the escapee up regardless — the assertion is about what
        // the DRIVER could reach, not about littering the machine.
        // SAFETY: best-effort SIGKILL on the test's own grandchild.
        unsafe {
            libc::kill(escapee as libc::pid_t, libc::SIGKILL);
        }
        assert!(
            alive,
            "the double-forked escapee was expected to SURVIVE (the documented \
             residual); if the driver can now reach it, the containment story \
             improved — update the docs and this pin deliberately"
        );
    }
}
