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
//! 5. The probe errors: alive, but only while the claim is younger than
//!    [`PROBE_ERROR_CAP`]. Past the cap the grace applies, so a wedged probe
//!    cannot hold a claim forever.

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

/// This machine's identity for [`ChildMarker::host`].
pub fn host_identity() -> String {
    hostname::get()
        .ok()
        .and_then(|h| h.into_string().ok())
        .filter(|h| !h.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
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

/// Read the marker for `submission_id`. A missing or unreadable file is `None`.
pub fn read(rocky_dir: &Path, submission_id: &str) -> Option<ChildMarker> {
    let bytes = std::fs::read(marker_path(rocky_dir, submission_id)).ok()?;
    serde_json::from_slice(&bytes).ok()
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
/// sidecar. `None` when neither holds a complete identity.
pub fn identity_for(rocky_dir: &Path, claim: &ClaimRecord) -> Option<ChildMarker> {
    if let (Some(pid), Some(start_time), Some(host)) = (
        claim.child_pid,
        claim.child_start_time,
        claim.child_host.clone(),
    ) {
        return Some(ChildMarker {
            pid,
            start_time,
            host,
        });
    }
    read(rocky_dir, &claim.submission_id)
}

/// Whether the child recorded for `claim` may still be running. See the module
/// docs for the rules. `probe` is [`crate::process::process_liveness`] in
/// production; `this_host` is [`host_identity`].
pub fn child_may_be_alive(
    identity: Option<&ChildMarker>,
    claim: &ClaimRecord,
    this_host: &str,
    now: DateTime<Utc>,
    probe: impl Fn(u32) -> Result<Option<u64>, String>,
) -> bool {
    let Some(child) = identity else {
        return false;
    };
    if child.host != this_host {
        return false;
    }
    match probe(child.pid) {
        Ok(None) => false,
        Ok(Some(start)) => start == child.start_time,
        Err(e) => {
            let within_cap = claim
                .last_transition_at
                .as_deref()
                .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                .is_some_and(|t| now - t.with_timezone(&Utc) < PROBE_ERROR_CAP);
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
            Some(&marker()),
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
            Some(&marker()),
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
            Some(&marker()),
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
            Some(&marker()),
            &fresh,
            "h1",
            now(),
            err
        ));
        let old = claim_at(now() - Duration::hours(25));
        assert!(!child_may_be_alive(Some(&marker()), &old, "h1", now(), err));
    }

    #[test]
    fn other_host_or_no_identity_keeps_the_grace_behaviour() {
        let c = claim_at(now());
        let alive = |_: u32| Ok(Some(100));
        assert!(!child_may_be_alive(Some(&marker()), &c, "h2", now(), alive));
        assert!(!child_may_be_alive(None, &c, "h1", now(), alive));
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
