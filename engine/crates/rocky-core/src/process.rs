//! Process liveness: is a recorded pid still the process that recorded
//! it?
//!
//! Lives in `rocky-core` because two layers need the same answer and
//! neither may depend on the other. The fulfillment loop uses it to
//! decide whether a recorded owner is alive before taking a record
//! over; `rocky-cli` uses it to decide whether an owner stamp on a
//! product record is THIS process. `rocky-fulfill` sits above
//! `rocky-cli`, so the probe cannot live there and still be reachable
//! from both.
//!
//! A pid alone is not an identity — pids are recycled. Every comparison
//! pairs the pid with the start time this module returns, which is what
//! makes a stamp reuse-proof.

/// Remove an outer Dagster Pipes session from a child Rocky starts. A child
/// does not own the outer process's message channel, even when it runs `rocky`.
pub fn strip_dagster_pipes_env(command: &mut std::process::Command) {
    let keys: Vec<_> = std::env::vars_os()
        .map(|(key, _)| key)
        .chain(command.get_envs().map(|(key, _)| key.to_os_string()))
        .collect();
    for key in keys {
        if key
            .to_string_lossy()
            .to_ascii_uppercase()
            .starts_with("DAGSTER_PIPES_")
        {
            command.env_remove(key);
        }
    }
}

/// The start time of a live process, or `None` when no such pid exists.
///
/// The value's unit is platform-specific (macOS: microseconds since the
/// epoch of the process start; Linux: clock ticks since boot) and is
/// only ever compared for EQUALITY on the same machine — the
/// `fulfill_state` table is local-only, so a stamp never crosses hosts.
///
/// # Errors
///
/// A probe failure (not "no such process") is an error so callers can
/// treat it as indefinite rather than dead — a transient read failure
/// must never trigger a takeover.
pub fn process_liveness(pid: u32) -> Result<Option<u64>, String> {
    imp_process_liveness(pid)
}

#[cfg(target_os = "macos")]
fn imp_process_liveness(pid: u32) -> Result<Option<u64>, String> {
    use std::mem::MaybeUninit;

    let mut info = MaybeUninit::<libc::proc_bsdinfo>::zeroed();
    let size = std::mem::size_of::<libc::proc_bsdinfo>() as libc::c_int;
    // SAFETY: `proc_pidinfo(PROC_PIDTBSDINFO)` writes at most
    // `buffersize` bytes into `buffer`; the buffer is exactly
    // `proc_bsdinfo`-sized and zero-initialized, and no pointer is
    // retained past the call.
    let written = unsafe {
        libc::proc_pidinfo(
            pid as libc::c_int,
            libc::PROC_PIDTBSDINFO,
            0,
            info.as_mut_ptr().cast(),
            size,
        )
    };
    if written <= 0 {
        let err = std::io::Error::last_os_error();
        // ESRCH = no such process — a definitive answer.
        if err.raw_os_error() == Some(libc::ESRCH) {
            return Ok(None);
        }
        return Err(format!("proc_pidinfo({pid}) failed: {err}"));
    }
    if (written as usize) < std::mem::size_of::<libc::proc_bsdinfo>() {
        return Err(format!(
            "proc_pidinfo({pid}) wrote {written} bytes, expected {size}"
        ));
    }
    // SAFETY: the kernel reported a full `proc_bsdinfo` write, so the
    // buffer is initialized.
    let info = unsafe { info.assume_init() };
    if info.pbi_pid != pid {
        return Ok(None);
    }
    Ok(Some(
        info.pbi_start_tvsec.saturating_mul(1_000_000) + info.pbi_start_tvusec,
    ))
}

#[cfg(target_os = "linux")]
fn imp_process_liveness(pid: u32) -> Result<Option<u64>, String> {
    let stat = match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
        Ok(stat) => stat,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(format!("reading /proc/{pid}/stat failed: {err}")),
    };
    // Field 22 (1-based) is starttime, in clock ticks since boot. The
    // comm field (2) may contain spaces and parentheses, so split after
    // the LAST ')' — the documented parse for /proc/<pid>/stat.
    let after_comm = stat
        .rsplit_once(')')
        .map(|(_, rest)| rest)
        .ok_or_else(|| format!("/proc/{pid}/stat has no comm terminator"))?;
    let start = after_comm
        .split_ascii_whitespace()
        .nth(19) // after the comm split the first token is field 3 (state), so field 22 = index 19
        .ok_or_else(|| format!("/proc/{pid}/stat has no starttime field"))?;
    start
        .parse::<u64>()
        .map(Some)
        .map_err(|e| format!("/proc/{pid}/stat starttime did not parse: {e}"))
}

#[cfg(all(unix, not(any(target_os = "macos", target_os = "linux"))))]
fn imp_process_liveness(pid: u32) -> Result<Option<u64>, String> {
    let _ = pid;
    Err("no process start-time probe exists for this Unix platform".to_string())
}

#[cfg(not(unix))]
fn imp_process_liveness(pid: u32) -> Result<Option<u64>, String> {
    let _ = pid;
    Err("no process start-time probe exists for this platform".to_string())
}

/// Does an `(owner_pid, owner_start_time)` stamp name THIS process?
///
/// The one definition of "this record is mine", shared by every gate
/// that needs it, so the gates cannot drift apart.
///
/// Both halves are required. A pid alone is not an identity: a process
/// that dies leaves its stamp behind, and the operating system will
/// eventually hand that number to something unrelated. Pairing it with
/// the start time this module reads makes the answer reuse-proof.
///
/// Fails CLOSED. A stamp with no pid, no recorded start time, or one
/// whose start time cannot be confirmed is not ours — "unknown" is
/// never "mine".
pub fn stamp_is_this_process(owner_pid: Option<u32>, owner_start_time: Option<u64>) -> bool {
    let Some(pid) = owner_pid else {
        return false;
    };
    if pid != std::process::id() {
        return false;
    }
    match process_liveness(pid) {
        Ok(Some(start_time)) => owner_start_time == Some(start_time),
        Ok(None) | Err(_) => false,
    }
}

/// A signal sent to the whole process group a job child leads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupSignal {
    /// `SIGTERM`, the stop Kubernetes sends a pod. `rocky run` handles it like
    /// Ctrl-C in its replication fan-out: it stops starting copies, lets
    /// in-flight ones finish, and settles its state. A process with no handler
    /// stops at once.
    ///
    /// Not `SIGINT`: a shell starts a background command with `SIGINT`
    /// ignored, and the ignore is inherited across `exec`. A `rocky serve`
    /// started that way would hand it to every job, and a job with no handler
    /// of its own would then never stop. Nothing ignores `SIGTERM` that way.
    Terminate,
    /// `SIGKILL`. Nothing can catch it, so the process stops like a crash.
    Kill,
}

/// Send `signal` to every process in the group `pgid`.
///
/// `pgid` must be the id of a child spawned as the leader of its own group
/// (`process_group(0)`), and the caller must still hold that child unreaped:
/// an unreaped child keeps its id, so the id cannot name another process.
///
/// # Errors
///
/// A `pgid` of 0 or 1 is refused: `killpg(0, ..)` signals the CALLER's own
/// group, and 1 is `init`. Otherwise the OS error of `killpg(2)`, such as
/// `ESRCH` when the group has already exited.
#[cfg(unix)]
pub fn signal_process_group(pgid: u32, signal: GroupSignal) -> std::io::Result<()> {
    let Ok(pgid) = libc::pid_t::try_from(pgid) else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "process group id out of range",
        ));
    };
    if pgid <= 1 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "refusing to signal process group 0 or 1",
        ));
    }
    let signal = match signal {
        GroupSignal::Terminate => libc::SIGTERM,
        GroupSignal::Kill => libc::SIGKILL,
    };
    // SAFETY: `killpg` takes two integers and touches no memory of this
    // process. `pgid` is above 1 (checked above), so it never names this
    // process's own group or `init`; a group that no longer exists returns
    // `ESRCH`, which is reported, not undefined.
    let rc = unsafe { libc::killpg(pgid, signal) };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    /// Group 0 is the caller's own group, and 1 is `init`: neither is ever
    /// signalled, whatever the signal.
    #[test]
    fn signal_process_group_refuses_the_callers_group_and_init() {
        for pgid in [0, 1] {
            for signal in [GroupSignal::Terminate, GroupSignal::Kill] {
                let err = signal_process_group(pgid, signal).unwrap_err();
                assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput, "{pgid}");
            }
        }
    }

    #[test]
    fn strip_removes_inherited_pipes_variables_from_child() {
        const KEY: &str = "DAGSTER_PIPES_CORE_INHERITED_PROBE";
        struct Restore(Option<std::ffi::OsString>);
        impl Drop for Restore {
            fn drop(&mut self) {
                // SAFETY: restore this test's unique environment variable.
                unsafe {
                    match self.0.take() {
                        Some(value) => std::env::set_var(KEY, value),
                        None => std::env::remove_var(KEY),
                    }
                }
            }
        }
        let _restore = Restore(std::env::var_os(KEY));
        // SAFETY: no other test reads this probe variable.
        unsafe { std::env::set_var(KEY, "outer") };
        let mut command = std::process::Command::new("/bin/sh");
        strip_dagster_pipes_env(&mut command);
        let output = command
            .args(["-c", "printf '%s' \"$DAGSTER_PIPES_CORE_INHERITED_PROBE\""])
            .output()
            .expect("spawn probe child");
        assert!(output.status.success());
        assert!(output.stdout.is_empty(), "child inherited Pipes variable");
    }
}
