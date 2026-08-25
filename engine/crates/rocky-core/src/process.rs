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
