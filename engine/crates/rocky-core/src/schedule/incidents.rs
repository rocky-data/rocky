//! Incident bundles — raw, structured facts about a scheduled run that
//! terminated in failure (#P4-PR2 via the O1 plan).
//!
//! One JSON file per incident under `.rocky/incidents/`, written by the
//! reconciler at the moment a scheduler-submitted attempt finalizes as
//! `Failure` or `Partial` — and deliberately NOT when the spawner itself
//! terminated the child for a shutdown drain, or every graceful shutdown
//! would litter this directory with non-incidents. No narration: the
//! diagnosing agent brings its own reasoning; this file brings citations
//! (ids, counts, and the commands that retrieve the full context).
//!
//! Facts that cannot be known at the emission site are `None`, never a
//! fabricated zero: a recovery-path bundle (orphan sweep, stuck resolver)
//! has no child exit code, and a bundle must not claim `0` consecutive
//! failures out of a cursor it could not read.
//!
//! Single-writer by construction: bundles are written inside the tick, and
//! ticks are serialized by the tick lock — so the keep-last-N sweep needs no
//! locking of its own.

use std::io;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// How many incident files are kept; the sweep removes the oldest beyond
/// this on every write. Names begin with a UTC timestamp, so lexicographic
/// order IS chronological order (to 1-second resolution).
pub const INCIDENTS_KEPT: usize = 50;

/// The structured facts of one failed scheduled run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IncidentBundle {
    /// Bundle schema, for forward-compatible consumers.
    pub incident_version: u32,
    /// When the incident was recorded (the tick's `now`).
    pub recorded_at: DateTime<Utc>,
    pub pipeline: String,
    /// The demand source that fired the run (`cron`, `after`, `freshness`,
    /// `webhook`).
    pub source: String,
    /// The terminal outcome: `"failure"` or `"partial"`. Partial failures
    /// trip the scheduler's partial backoff exactly like full failures trip
    /// failure backoff, so both are incidents.
    pub outcome: String,
    /// The occurrence the run was for, when the emission site knows it (the
    /// orphan sweep deliberately does not reconstruct it).
    pub logical_ts: Option<DateTime<Utc>>,
    /// The failed attempt's submission id — joins to the run record, and to
    /// `/api/v1/jobs/{id}` under the resident scheduler.
    pub submission_id: String,
    /// The child's exit code, when the reconciler observed the child itself.
    /// `None` for bundles emitted by the recovery paths, which join the
    /// persisted run record and never see an exit code.
    pub exit_code: Option<i32>,
    /// Attempts consumed for this demand cycle, as recorded on the claim
    /// (`cycle_attempts`). Claims written before this field existed
    /// serde-default it to `0`.
    pub attempts: u32,
    /// The pipeline's consecutive-failure count AFTER this failure, when the
    /// cursor could be read at emission. `None` when it could not, or when
    /// the emitting path deliberately does not touch the per-pipeline
    /// scalars (the orphan sweep).
    pub consecutive_failures: Option<u32>,
    /// Retrieval pointers — commands, not prose. The diagnosing agent (or
    /// human) runs these; nothing here is narrated.
    pub pointers: Vec<String>,
}

/// Whether `name` is a filename this module itself would produce:
/// `<YYYYMMDDTHHMMSSZ>-<pipeline>-<short-id>.json` with the inert charset.
/// The retention sweep and the brief's inventory both use this so that a
/// foreign file that strays into `.rocky/incidents/` is neither deleted nor
/// counted as a bundle.
#[must_use]
pub fn is_bundle_name(name: &str) -> bool {
    let Some(stem) = name.strip_suffix(".json") else {
        return false;
    };
    let b = stem.as_bytes();
    // Timestamp prefix: 8 digits, 'T', 6 digits, 'Z'.
    if b.len() < 16 + 1 + 1 {
        return false;
    }
    let ts_ok = b[..8].iter().all(u8::is_ascii_digit)
        && b[8] == b'T'
        && b[9..15].iter().all(u8::is_ascii_digit)
        && b[15] == b'Z';
    if !ts_ok || b[16] != b'-' {
        return false;
    }
    // `<pipeline>-<short-id>`: non-empty, inert charset, at least one
    // separator between the two parts.
    let rest = &stem[17..];
    !rest.is_empty()
        && rest.contains('-')
        && rest
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

/// Write `bundle` under `<rocky_dir>/incidents/` and sweep to the retention
/// cap. Returns the path written.
///
/// Refuses to operate through a symlinked `incidents` directory — the sweep
/// deletes files, and a directory that points elsewhere would turn the
/// retention cap into deletion of files Rocky does not own.
///
/// Failures here must never fail the tick: the run's outcome is already
/// committed, and diagnostics that break the thing they diagnose are worse
/// than none — callers warn and continue.
pub fn write_incident(rocky_dir: &Path, bundle: &IncidentBundle) -> io::Result<PathBuf> {
    let dir = rocky_dir.join("incidents");
    if let Ok(meta) = std::fs::symlink_metadata(&dir)
        && meta.file_type().is_symlink()
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "{} is a symlink; refusing to write or sweep through it",
                dir.display()
            ),
        ));
    }
    std::fs::create_dir_all(&dir)?;
    let short_id: String = sanitize(&bundle.submission_id).chars().take(8).collect();
    let name = format!(
        "{}-{}-{}.json",
        bundle.recorded_at.format("%Y%m%dT%H%M%SZ"),
        sanitize(&bundle.pipeline),
        short_id
    );
    debug_assert!(is_bundle_name(&name), "{name}");
    let path = dir.join(name);
    let json = serde_json::to_vec_pretty(bundle)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    std::fs::write(&path, json)?;
    sweep(&dir)?;
    Ok(path)
}

/// Keep the newest [`INCIDENTS_KEPT`] bundles; names sort chronologically.
///
/// Deletes ONLY regular files whose names this module would itself produce
/// ([`is_bundle_name`]) — a foreign `.json`, a subdirectory, or a symlinked
/// entry is left alone and never counts against the cap. Deletion errors
/// propagate (the caller warns): a sweep that cannot delete must not report
/// the cap as enforced.
fn sweep(dir: &Path) -> io::Result<()> {
    let mut files: Vec<PathBuf> = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        // `DirEntry::file_type` does not follow symlinks: a symlinked entry
        // reports `is_symlink`, not `is_file`, and is skipped.
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if is_bundle_name(name) {
            files.push(entry.path());
        }
    }
    if files.len() <= INCIDENTS_KEPT {
        return Ok(());
    }
    files.sort();
    let excess = files.len() - INCIDENTS_KEPT;
    for old in files.into_iter().take(excess) {
        std::fs::remove_file(old)?;
    }
    Ok(())
}

/// Pipeline names and submission ids reach a filename; keep the charset
/// inert. (Config-validated names are already tame — this is defense in
/// depth, not a parser.)
fn sanitize(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bundle(pipeline: &str, at: DateTime<Utc>) -> IncidentBundle {
        IncidentBundle {
            incident_version: 1,
            recorded_at: at,
            pipeline: pipeline.to_string(),
            source: "cron".to_string(),
            outcome: "failure".to_string(),
            logical_ts: Some(at),
            submission_id: "sub-12345678-rest".to_string(),
            exit_code: Some(1),
            attempts: 1,
            consecutive_failures: Some(3),
            pointers: vec!["rocky history --output json".to_string()],
        }
    }

    #[test]
    fn writes_a_readable_bundle_and_names_it_chronologically() {
        let tmp = tempfile::tempdir().unwrap();
        let at = chrono::Utc::now();
        let path = write_incident(tmp.path(), &bundle("orders", at)).unwrap();
        assert!(path.exists());
        let read: IncidentBundle = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(read.pipeline, "orders");
        assert_eq!(read.consecutive_failures, Some(3));
        assert!(is_bundle_name(path.file_name().unwrap().to_str().unwrap()));
    }

    #[test]
    fn retention_keeps_the_newest_fifty() {
        let tmp = tempfile::tempdir().unwrap();
        let base = chrono::DateTime::parse_from_rfc3339("2026-08-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        for i in 0..(INCIDENTS_KEPT + 7) {
            let at = base + chrono::Duration::minutes(i as i64);
            write_incident(tmp.path(), &bundle("p", at)).unwrap();
        }
        let files: Vec<_> = std::fs::read_dir(tmp.path().join("incidents"))
            .unwrap()
            .filter_map(Result::ok)
            .collect();
        assert_eq!(files.len(), INCIDENTS_KEPT);
    }

    #[test]
    fn a_hostile_pipeline_name_stays_inside_the_directory() {
        let tmp = tempfile::tempdir().unwrap();
        let at = chrono::Utc::now();
        let path = write_incident(tmp.path(), &bundle("../../etc/passwd", at)).unwrap();
        assert!(path.starts_with(tmp.path().join("incidents")), "{path:?}");
    }

    /// The sweep must never delete a file this module did not name — a
    /// foreign `.json` parked in the directory survives even when bundles
    /// churn past the retention cap.
    #[test]
    fn the_sweep_leaves_foreign_files_alone() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("incidents");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("important-notes.json"), "{}").unwrap();
        std::fs::write(dir.join("00000000-not-a-ts.json"), "{}").unwrap();
        let base = chrono::DateTime::parse_from_rfc3339("2026-08-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        for i in 0..(INCIDENTS_KEPT + 5) {
            let at = base + chrono::Duration::minutes(i as i64);
            write_incident(tmp.path(), &bundle("p", at)).unwrap();
        }
        assert!(dir.join("important-notes.json").exists());
        assert!(dir.join("00000000-not-a-ts.json").exists());
        let bundles = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|e| is_bundle_name(e.file_name().to_str().unwrap()))
            .count();
        assert_eq!(bundles, INCIDENTS_KEPT);
    }

    /// A symlinked incidents directory is refused outright — following it
    /// would let the retention sweep delete files outside the project.
    #[cfg(unix)]
    #[test]
    fn a_symlinked_incidents_directory_is_refused() {
        let tmp = tempfile::tempdir().unwrap();
        let victim = tmp.path().join("victim");
        std::fs::create_dir_all(&victim).unwrap();
        std::fs::write(victim.join("20260101T000000Z-vic-aaaa.json"), "{}").unwrap();
        let project = tmp.path().join("project");
        std::fs::create_dir_all(&project).unwrap();
        std::os::unix::fs::symlink(&victim, project.join("incidents")).unwrap();

        let err = write_incident(&project, &bundle("p", chrono::Utc::now()))
            .expect_err("symlinked dir must refuse");
        assert!(err.to_string().contains("symlink"), "{err}");
        assert!(
            victim.join("20260101T000000Z-vic-aaaa.json").exists(),
            "nothing behind the symlink may be touched"
        );
    }

    #[test]
    fn bundle_name_recognition_is_strict() {
        assert!(is_bundle_name("20260801T120000Z-orders-abcd1234.json"));
        assert!(is_bundle_name("20260801T120000Z-my_pipe-line-ab.json"));
        for bad in [
            "notes.json",
            "20260801T120000Z-orders-abcd1234.txt",
            "20260801T120000Zorders-x.json",
            "2026-08-01T120000Z-orders-x.json",
            "20260801T120000Z-.json",
            "20260801T120000Z-noseparator.json",
            "20260801T120000Z-bad$char-x.json",
        ] {
            assert!(!is_bundle_name(bad), "{bad}");
        }
    }
}
