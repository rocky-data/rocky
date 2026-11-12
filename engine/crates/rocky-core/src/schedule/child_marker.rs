//! The spawned child's identity, recorded in a sidecar file, and the liveness
//! decision built on it.
//!
//! The tick closes the state store before it spawns a child (the child opens
//! the same file and the locks are exclusive), so the spawner cannot write the
//! child's pid into the claim. It writes a small JSON file keyed by
//! `submission_id` under `<rocky_dir>/schedule-children/` instead. The first
//! sweep of a stuck claim already holds the store, so it copies the file into
//! the claim's `child_pid` / `child_start_time` / `child_host`.
//!
//! When the recovery grace expires, [`child_may_be_alive`] decides whether the
//! child is still running. The single rule that matters: never declare a child
//! dead on doubt. A live child declared lost makes a cron occurrence a false
//! failure, or a standing claim re-run as a duplicate.
//!
//! Choices, in order:
//!
//! 1. No identity recorded (old claim, spawn without a probe, Windows): not
//!    alive. Today's grace-only behaviour.
//! 2. Recorded host differs from this host: not alive. A pid is meaningless
//!    across machines, so the grace applies as before.
//! 3. The probe says the process is gone, or its start time differs from the
//!    recorded one (the pid was reused): not alive.
//! 4. The probe confirms the same start time: alive, with no age bound. The
//!    process is positively identified, and the scheduler timeout bounds a
//!    real run.
//! 5. The probe errors, or a sidecar exists but cannot be read: alive, but
//!    only while the claim is younger than [`PROBE_ERROR_CAP`]. Past the cap
//!    the grace applies, so a wedged probe cannot hold a claim forever.
//!
//! A zombie (exited, not yet reaped) is gone: the probe is
//! [`crate::process::running_process_start_time`].

use std::io;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Duration, Utc};

use super::claim::ClaimRecord;

/// How long a claim may be held alive on a probe error alone.
pub const PROBE_ERROR_CAP: Duration = Duration::hours(24);

/// A sidecar older than this is garbage from a claim that was finalized by a
/// path that did not clean it up.
pub const STALE_MARKER_AGE: Duration = Duration::days(7);

const DIR: &str = "schedule-children";

/// The identity of a spawned child.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ChildMarker {
    /// The child's pid.
    pub pid: u32,
    /// The child's process start time, from [`crate::process::process_liveness`].
    pub start_time: u64,
    /// The host the child runs on.
    pub host: String,
}

/// This machine's identity for [`ChildMarker::host`]: the hostname, and on
/// Linux also the kernel's boot id. A pid and start time only name a process
/// within one boot of one kernel, so a reboot, or another node that shares the
/// hostname, reads as another host. A renamed host also reads as another host:
/// the claim then gets the grace-only behaviour it had before #2568.
pub fn host_identity() -> String {
    let name = hostname::get()
        .ok()
        .and_then(|h| h.into_string().ok())
        .filter(|h| !h.is_empty())
        .unwrap_or_else(|| "unknown".to_string());
    match boot_id() {
        Some(boot) => format!("{name}/{boot}"),
        None => name,
    }
}

#[cfg(target_os = "linux")]
fn boot_id() -> Option<String> {
    std::fs::read_to_string("/proc/sys/kernel/random/boot_id")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

#[cfg(not(target_os = "linux"))]
fn boot_id() -> Option<String> {
    None
}

/// What a sidecar read found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MarkerRead {
    /// No sidecar for this submission.
    Absent,
    /// A readable sidecar.
    Present(ChildMarker),
    /// A sidecar exists but could not be read or parsed. Not proof of death.
    Unreadable(String),
}

/// The child identity a liveness decision rests on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChildIdentity {
    /// Nothing recorded: the grace-only behaviour applies.
    Unknown,
    /// A recorded pid, start time and host.
    Known(ChildMarker),
    /// A sidecar exists but could not be read. Treated like a probe error.
    Unreadable(String),
}

fn dir(rocky_dir: &Path) -> PathBuf {
    rocky_dir.join(DIR)
}

fn marker_path(rocky_dir: &Path, submission_id: &str) -> PathBuf {
    // A submission id is a UUID. Anything else is hashed, so a hostile id can
    // never leave the directory.
    let plain = !submission_id.is_empty()
        && submission_id.len() <= 64
        && submission_id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
    let name = if plain {
        submission_id.to_string()
    } else {
        blake3::hash(submission_id.as_bytes()).to_hex().to_string()
    };
    dir(rocky_dir).join(format!("{name}.json"))
}

/// Record `marker` for `submission_id`, atomically (write a temp file, rename).
///
/// # Errors
///
/// Returns the I/O error. The caller warns and goes on: a missing marker only
/// means the grace-only behaviour applies.
pub fn write(rocky_dir: &Path, submission_id: &str, marker: &ChildMarker) -> io::Result<()> {
    let path = marker_path(rocky_dir, submission_id);
    std::fs::create_dir_all(dir(rocky_dir))?;
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, serde_json::to_vec(marker).map_err(io::Error::other)?)?;
    std::fs::rename(&tmp, &path)
}

/// Read the marker for `submission_id`. A missing or unreadable file is `None`;
/// [`read_marker`] tells the two apart.
pub fn read(rocky_dir: &Path, submission_id: &str) -> Option<ChildMarker> {
    match read_marker(rocky_dir, submission_id) {
        MarkerRead::Present(marker) => Some(marker),
        MarkerRead::Absent | MarkerRead::Unreadable(_) => None,
    }
}

/// Read the marker for `submission_id`, telling a missing file from one that
/// exists but cannot be read or parsed.
pub fn read_marker(rocky_dir: &Path, submission_id: &str) -> MarkerRead {
    let bytes = match std::fs::read(marker_path(rocky_dir, submission_id)) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return MarkerRead::Absent,
        Err(e) => return MarkerRead::Unreadable(e.to_string()),
    };
    match serde_json::from_slice(&bytes) {
        Ok(marker) => MarkerRead::Present(marker),
        Err(e) => MarkerRead::Unreadable(e.to_string()),
    }
}

/// Remove the marker for `submission_id`. Missing is fine.
pub fn remove(rocky_dir: &Path, submission_id: &str) {
    if let Err(e) = std::fs::remove_file(marker_path(rocky_dir, submission_id))
        && e.kind() != io::ErrorKind::NotFound
    {
        tracing::warn!(error = %e, "could not remove a scheduler child marker");
    }
}

/// Remove markers whose mtime is older than [`STALE_MARKER_AGE`] on the wall
/// clock. Returns the number removed. Errors are ignored: this is housekeeping.
pub fn sweep_stale(rocky_dir: &Path) -> usize {
    let Ok(entries) = std::fs::read_dir(dir(rocky_dir)) else {
        return 0;
    };
    let max_age = STALE_MARKER_AGE.to_std().unwrap_or_default();
    let mut removed = 0;
    for entry in entries.flatten() {
        let old = entry
            .metadata()
            .and_then(|m| m.modified())
            .ok()
            .and_then(|t| t.elapsed().ok())
            .is_some_and(|age| age > max_age);
        if old && std::fs::remove_file(entry.path()).is_ok() {
            removed += 1;
        }
    }
    removed
}

/// The child identity to use for `claim`: the claim's own fields, else the
/// sidecar.
pub fn identity_for(rocky_dir: &Path, claim: &ClaimRecord) -> ChildIdentity {
    if let (Some(pid), Some(start_time), Some(host)) = (
        claim.child_pid,
        claim.child_start_time,
        claim.child_host.clone(),
    ) {
        return ChildIdentity::Known(ChildMarker {
            pid,
            start_time,
            host,
        });
    }
    match read_marker(rocky_dir, &claim.submission_id) {
        MarkerRead::Absent => ChildIdentity::Unknown,
        MarkerRead::Present(marker) => ChildIdentity::Known(marker),
        MarkerRead::Unreadable(e) => ChildIdentity::Unreadable(e),
    }
}

/// Whether `claim` is still young enough for doubt to hold it: its last
/// transition is less than [`PROBE_ERROR_CAP`] before `now`.
fn within_doubt_cap(claim: &ClaimRecord, now: DateTime<Utc>) -> bool {
    claim
        .last_transition_at
        .as_deref()
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .is_some_and(|t| now - t.with_timezone(&Utc) < PROBE_ERROR_CAP)
}

/// Whether the child recorded for `claim` may still be running. See the module
/// docs for the rules. `probe` is [`crate::process::running_process_start_time`] in
/// production; `this_host` is [`host_identity`].
pub fn child_may_be_alive(
    identity: &ChildIdentity,
    claim: &ClaimRecord,
    this_host: &str,
    now: DateTime<Utc>,
    probe: impl Fn(u32) -> Result<Option<u64>, String>,
) -> bool {
    let child = match identity {
        ChildIdentity::Unknown => return false,
        ChildIdentity::Known(child) => child,
        ChildIdentity::Unreadable(e) => {
            let within_cap = within_doubt_cap(claim, now);
            tracing::warn!(
                submission_id = %claim.submission_id,
                error = %e,
                within_cap,
                "scheduler child sidecar unreadable"
            );
            return within_cap;
        }
    };
    if child.host != this_host {
        return false;
    }
    match probe(child.pid) {
        Ok(None) => false,
        Ok(Some(start)) => start == child.start_time,
        Err(e) => {
            let within_cap = within_doubt_cap(claim, now);
            tracing::warn!(
                pid = child.pid,
                error = %e,
                within_cap,
                "scheduler child liveness probe failed"
            );
            within_cap
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 5, 2, 12, 0, 0).unwrap()
    }

    fn marker() -> ChildMarker {
        ChildMarker {
            pid: 4242,
            start_time: 100,
            host: "h1".to_string(),
        }
    }

    fn claim_at(at: DateTime<Utc>) -> ClaimRecord {
        ClaimRecord::new_submitted("sub-1".to_string(), at)
    }

    #[test]
    fn same_start_time_is_alive_even_for_an_old_claim() {
        let c = claim_at(now() - Duration::days(3));
        assert!(child_may_be_alive(
            &ChildIdentity::Known(marker()),
            &c,
            "h1",
            now(),
            |_| Ok(Some(100))
        ));
    }

    #[test]
    fn gone_pid_is_not_alive() {
        let c = claim_at(now());
        assert!(!child_may_be_alive(
            &ChildIdentity::Known(marker()),
            &c,
            "h1",
            now(),
            |_| Ok(None)
        ));
    }

    #[test]
    fn mismatched_start_time_means_pid_reuse() {
        let c = claim_at(now());
        assert!(!child_may_be_alive(
            &ChildIdentity::Known(marker()),
            &c,
            "h1",
            now(),
            |_| Ok(Some(101))
        ));
    }

    #[test]
    fn probe_error_is_alive_inside_the_cap_and_unknown_past_it() {
        let err = |_: u32| Err("boom".to_string());
        let fresh = claim_at(now() - Duration::hours(1));
        assert!(child_may_be_alive(
            &ChildIdentity::Known(marker()),
            &fresh,
            "h1",
            now(),
            err
        ));
        let old = claim_at(now() - Duration::hours(25));
        assert!(!child_may_be_alive(
            &ChildIdentity::Known(marker()),
            &old,
            "h1",
            now(),
            err
        ));
    }

    #[test]
    fn other_host_or_no_identity_keeps_the_grace_behaviour() {
        let c = claim_at(now());
        let alive = |_: u32| Ok(Some(100));
        assert!(!child_may_be_alive(
            &ChildIdentity::Known(marker()),
            &c,
            "h2",
            now(),
            alive
        ));
        assert!(!child_may_be_alive(
            &ChildIdentity::Unknown,
            &c,
            "h1",
            now(),
            alive
        ));
    }

    #[test]
    fn an_unreadable_sidecar_is_doubt_not_death() {
        let dir = tempfile::tempdir().unwrap();
        let path = marker_path(dir.path(), "sub-1");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, b"{truncated").unwrap();
        let fresh = claim_at(now() - Duration::hours(1));
        let identity = identity_for(dir.path(), &fresh);
        assert!(matches!(identity, ChildIdentity::Unreadable(_)));
        let never_called = |_: u32| -> Result<Option<u64>, String> { panic!("no probe") };
        assert!(child_may_be_alive(
            &identity,
            &fresh,
            "h1",
            now(),
            never_called
        ));
        let old = claim_at(now() - Duration::hours(25));
        assert!(!child_may_be_alive(
            &identity,
            &old,
            "h1",
            now(),
            never_called
        ));
        assert_eq!(read_marker(dir.path(), "nope"), MarkerRead::Absent);
    }

    #[test]
    fn marker_round_trips_and_removes() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "abc-123", &marker()).unwrap();
        assert_eq!(read(dir.path(), "abc-123"), Some(marker()));
        remove(dir.path(), "abc-123");
        assert_eq!(read(dir.path(), "abc-123"), None);
        remove(dir.path(), "abc-123");
    }

    #[test]
    fn a_hostile_submission_id_stays_inside_the_directory() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "../../escape", &marker()).unwrap();
        assert_eq!(read(dir.path(), "../../escape"), Some(marker()));
        assert!(!dir.path().join("..").join("escape.json").exists());
    }
}
