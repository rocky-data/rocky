//! Child-process helpers shared by out-of-process adapters and child
//! connectors: bounded line reads, process-group stop, and resource limits.
//!
//! # Safety
//!
//! This module holds the SDK's `unsafe` calls: `killpg` and `waitid` for the
//! process-group stop, and `getrlimit` / `setrlimit` inside a `pre_exec`
//! closure for [`apply_rlimits`]. Each site carries its own `SAFETY:` comment.

use std::time::Duration;

use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt};
use tokio::process::Child;

/// Read one line of at most `max_bytes` bytes, newline excluded.
///
/// Returns `Ok(None)` when the line is longer, after reading `max_bytes + 1`
/// bytes and no more. Returns an empty string at end of file.
pub async fn read_bounded_line<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    max_bytes: usize,
) -> std::io::Result<Option<String>> {
    // `+ 1` leaves room for the newline that ends a line of exactly
    // `max_bytes` bytes.
    let limit = u64::try_from(max_bytes)
        .unwrap_or(u64::MAX)
        .saturating_add(1);
    let mut buf = Vec::new();
    (&mut *reader)
        .take(limit)
        .read_until(b'\n', &mut buf)
        .await?;
    if buf.last() != Some(&b'\n') && buf.len() > max_bytes {
        return Ok(None);
    }
    String::from_utf8(buf)
        .map(Some)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

/// Stop the child and reap it.
///
/// On Unix: `SIGTERM` to the child's process group, up to `grace` for the
/// child to exit, then `SIGKILL` to the group. The child stays unreaped
/// until after the `SIGKILL`, so its id cannot name another process group
/// when it is signalled. Elsewhere: kill the child alone.
pub async fn stop_child(child: &mut Child, grace: Duration) {
    #[cfg(unix)]
    if let Some(pgid) = child.id() {
        let _ = signal_group(pgid, libc::SIGTERM);
        let deadline = tokio::time::Instant::now() + grace;
        while tokio::time::Instant::now() < deadline && !leader_has_exited(pgid) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let _ = signal_group(pgid, libc::SIGKILL);
    }
    #[cfg(not(unix))]
    let _ = grace;
    let _ = child.start_kill();
    let _ = child.wait().await;
}

/// Send `signal` to process group `pgid`. Refuses group 0 (the caller's own)
/// and 1 (`init`).
#[cfg(unix)]
pub fn signal_group(pgid: u32, signal: libc::c_int) -> std::io::Result<()> {
    let pgid = libc::pid_t::try_from(pgid).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "process group id out of range",
        )
    })?;
    if pgid <= 1 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "refusing to signal process group 0 or 1",
        ));
    }
    // SAFETY: `killpg` takes two integers and touches no memory of this
    // process. `pgid` is above 1, so it never names this process's own group
    // or `init`. Callers pass the id of a child that is not yet reaped and
    // that leads its own group (`process_group(0)`), so the id cannot have
    // been reused. A group that no longer exists returns `ESRCH`.
    let rc = unsafe { libc::killpg(pgid, signal) };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

/// Whether the child `pid` has exited, without reaping it (`WNOWAIT`).
#[cfg(any(target_os = "linux", target_os = "macos"))]
pub fn leader_has_exited(pid: u32) -> bool {
    // `id_t` is `u32` on both Linux and macOS.
    let id = libc::id_t::from(pid);
    // SAFETY: `siginfo_t` is plain data, and all zeroes is a valid value.
    let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
    // SAFETY: `info` is a valid, writable `siginfo_t` that outlives the call.
    // `WNOWAIT` leaves the child waitable, so tokio still reaps it later.
    let rc = unsafe {
        libc::waitid(
            libc::P_PID,
            id,
            &mut info,
            libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
        )
    };
    if rc != 0 {
        // `ECHILD`: nothing left to wait for.
        return true;
    }
    // SAFETY: `waitid` succeeded and wrote `info`. With `WNOHANG`, `si_pid`
    // stays 0 when the child has not exited.
    unsafe { info.si_pid() != 0 }
}

/// Without `waitid`, wait out the full grace before `SIGKILL`.
#[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
pub fn leader_has_exited(_pid: u32) -> bool {
    false
}

/// Stops a child's whole process group with `SIGKILL` when dropped, unless
/// disarmed.
///
/// `Drop` cannot wait, so there is no `SIGTERM` grace: use [`stop_child`]
/// for a graceful stop. Disarm the guard once the child is reaped, because
/// after the reap its id may name another process group.
///
/// Nothing in Rocky constructs this guard yet. It is kept for the next slice
/// (1b), which wraps spawned children with it.
#[cfg(unix)]
#[derive(Debug)]
pub struct GroupKillGuard {
    pgid: Option<u32>,
}

#[cfg(unix)]
impl GroupKillGuard {
    /// Guard the group led by the unreaped child `pgid`. The child must lead
    /// its own group (`process_group(0)`).
    pub fn new(pgid: u32) -> Self {
        Self { pgid: Some(pgid) }
    }

    /// Do not signal the group on drop.
    pub fn disarm(&mut self) {
        self.pgid = None;
    }
}

#[cfg(unix)]
impl Drop for GroupKillGuard {
    fn drop(&mut self) {
        if let Some(pgid) = self.pgid {
            let _ = signal_group(pgid, libc::SIGKILL);
        }
    }
}

// ---------------------------------------------------------------------------
// Resource limits
// ---------------------------------------------------------------------------

/// Extra seconds between the soft CPU limit (`SIGXCPU`) and the hard one
/// (`SIGKILL`), so a child that handles `SIGXCPU` can still flush.
pub const CPU_HARD_LIMIT_MARGIN_SECS: u64 = 5;

/// Resource limits for one child. `None` leaves a limit as it is.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RlimitPlan {
    /// CPU seconds (`RLIMIT_CPU`). The soft limit is this value. The hard
    /// limit is [`CPU_HARD_LIMIT_MARGIN_SECS`] higher.
    pub cpu_secs: Option<u64>,
    /// Address-space bytes (`RLIMIT_AS`). On macOS the kernel often refuses
    /// this limit, and the refusal is ignored.
    pub memory_bytes: Option<u64>,
    /// Largest file the child may write, in bytes (`RLIMIT_FSIZE`).
    pub output_bytes: Option<u64>,
}

/// The integer type of an `RLIMIT_*` constant in this platform's `libc`.
#[cfg(all(unix, target_os = "linux", target_env = "gnu"))]
type RlimitResource = libc::__rlimit_resource_t;
#[cfg(all(unix, not(all(target_os = "linux", target_env = "gnu"))))]
type RlimitResource = libc::c_int;

/// Apply `plan` to `command`'s child, between `fork` and `exec`.
///
/// Limits are clamped to the hard limit the parent already runs under, since
/// a process without privilege cannot raise a hard limit. A refusal of
/// `RLIMIT_AS` is ignored on macOS. Any other refusal fails the spawn.
/// Does nothing on platforms without `setrlimit`.
#[cfg(unix)]
pub fn apply_rlimits(command: &mut tokio::process::Command, plan: RlimitPlan) {
    if plan == RlimitPlan::default() {
        return;
    }
    // SAFETY: the closure runs in the forked child before `exec`. It captures
    // only `Copy` integers and calls only `getrlimit` and `setrlimit`. On
    // glibc, musl and macOS libSystem those are direct syscall wrappers that
    // allocate nothing and take no locks, so they cannot deadlock on a lock
    // another thread held at `fork`. `setrlimit` is NOT on POSIX's list of
    // async-signal-safe functions, so this rests on those libc
    // implementations, not on the standard. The closure does not allocate: an
    // error is `io::Error::last_os_error()`, which stores only the errno.
    unsafe {
        command.pre_exec(move || set_rlimits(plan));
    }
}

/// Without `setrlimit`, limits are not applied.
#[cfg(not(unix))]
pub fn apply_rlimits(_command: &mut tokio::process::Command, _plan: RlimitPlan) {}

/// Set every limit in `plan`. Runs between `fork` and `exec`: no allocation,
/// no locks.
#[cfg(unix)]
fn set_rlimits(plan: RlimitPlan) -> std::io::Result<()> {
    if let Some(secs) = plan.cpu_secs {
        set_one(
            libc::RLIMIT_CPU,
            secs,
            secs.saturating_add(CPU_HARD_LIMIT_MARGIN_SECS),
            false,
        )?;
    }
    if let Some(bytes) = plan.memory_bytes {
        set_one(libc::RLIMIT_AS, bytes, bytes, cfg!(target_os = "macos"))?;
    }
    if let Some(bytes) = plan.output_bytes {
        set_one(libc::RLIMIT_FSIZE, bytes, bytes, false)?;
    }
    Ok(())
}

/// Set one limit, clamped to the current hard limit. With `ignore_failure`,
/// a refusal is not an error.
#[cfg(unix)]
fn set_one(
    resource: RlimitResource,
    soft: u64,
    hard: u64,
    ignore_failure: bool,
) -> std::io::Result<()> {
    let mut current = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `current` is a valid, writable `rlimit` that outlives the
    // call. `getrlimit` is a direct syscall wrapper: no allocation, no locks.
    let rc = unsafe { libc::getrlimit(resource, &mut current) };
    if rc != 0 {
        return if ignore_failure {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        };
    }
    let hard = (hard as libc::rlim_t).min(current.rlim_max);
    let soft = (soft as libc::rlim_t).min(hard);
    let wanted = libc::rlimit {
        rlim_cur: soft,
        rlim_max: hard,
    };
    // SAFETY: `wanted` is a valid `rlimit` that outlives the call.
    // `setrlimit` is a direct syscall wrapper: no allocation, no locks.
    let rc = unsafe { libc::setrlimit(resource, &wanted) };
    if rc != 0 && !ignore_failure {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn read_bounded_line_accepts_the_limit_and_refuses_one_byte_more() {
        let line = format!("{}\n", "x".repeat(8));
        let mut reader = tokio::io::BufReader::new(line.as_bytes());
        assert_eq!(
            read_bounded_line(&mut reader, 8).await.unwrap().as_deref(),
            Some(line.as_str())
        );

        let line = format!("{}\n", "x".repeat(9));
        let mut reader = tokio::io::BufReader::new(line.as_bytes());
        assert_eq!(read_bounded_line(&mut reader, 8).await.unwrap(), None);

        let mut reader = tokio::io::BufReader::new(&b""[..]);
        assert_eq!(
            read_bounded_line(&mut reader, 8).await.unwrap().as_deref(),
            Some("")
        );
    }

    /// Run `script` under `sh -c` with `plan` applied; return its stdout.
    #[cfg(unix)]
    async fn run_sh_with(plan: RlimitPlan, script: &str) -> String {
        let mut cmd = tokio::process::Command::new("sh");
        cmd.arg("-c").arg(script);
        apply_rlimits(&mut cmd, plan);
        let out = cmd.output().await.expect("spawn sh");
        assert!(out.status.success(), "sh failed: {out:?}");
        String::from_utf8(out.stdout).unwrap()
    }

    /// The child reads back its own limits and sees the values applied.
    #[cfg(unix)]
    #[tokio::test]
    async fn the_child_sees_the_limits_applied_to_it() {
        let plan = RlimitPlan {
            cpu_secs: Some(7),
            memory_bytes: None,
            output_bytes: Some(1_048_576),
        };
        let out = run_sh_with(plan, "ulimit -St; ulimit -Ht; ulimit -Sf").await;
        let lines: Vec<u64> = out.lines().map(|l| l.trim().parse().unwrap()).collect();
        assert_eq!(lines[0], 7, "soft CPU limit");
        assert_eq!(
            lines[1],
            7 + CPU_HARD_LIMIT_MARGIN_SECS,
            "hard CPU limit sits above the soft one"
        );
        // `ulimit -f` counts 512- or 1024-byte blocks, by shell.
        assert!(
            lines[2] == 1024 || lines[2] == 2048,
            "file size limit is 1 MiB, got {} blocks",
            lines[2]
        );
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn the_child_sees_the_address_space_limit_on_linux() {
        let plan = RlimitPlan {
            memory_bytes: Some(8 << 30),
            ..RlimitPlan::default()
        };
        let out = run_sh_with(plan, "ulimit -Sv").await;
        assert_eq!(out.trim(), (8u64 << 20).to_string(), "KiB");
    }

    /// An address-space limit is applied or ignored, never a spawn error.
    #[cfg(unix)]
    #[tokio::test]
    async fn an_address_space_limit_never_fails_the_spawn() {
        let plan = RlimitPlan {
            memory_bytes: Some(8 << 30),
            ..RlimitPlan::default()
        };
        let _ = run_sh_with(plan, "true").await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn an_empty_plan_changes_nothing() {
        let with_plan = run_sh_with(RlimitPlan::default(), "ulimit -St; ulimit -Sf").await;
        let mut cmd = tokio::process::Command::new("sh");
        cmd.arg("-c").arg("ulimit -St; ulimit -Sf");
        let direct = String::from_utf8(cmd.output().await.unwrap().stdout).unwrap();
        assert_eq!(with_plan, direct);
    }
}
