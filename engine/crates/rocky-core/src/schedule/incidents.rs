//! Incident bundles — raw, structured facts about a scheduled run that
//! terminated in failure (#P4-PR2 via the O1 plan).
//!
//! One JSON file per incident under `.rocky/incidents/`, written by the
//! reconciler at the moment a scheduler-submitted attempt finalizes as
//! `Failure` — and deliberately NOT on drain-interruptions, or every graceful
//! shutdown would litter this directory with non-incidents. No narration:
//! the diagnosing agent brings its own reasoning; this file brings citations
//! (ids, counts, and the commands that retrieve the full context).
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
/// order IS chronological order.
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
    /// The occurrence the run was for, when the source has one.
    pub logical_ts: Option<DateTime<Utc>>,
    /// The failed attempt's submission id — joins to `/api/v1/jobs/{id}` and
    /// the run record.
    pub submission_id: String,
    pub exit_code: i32,
    /// Attempts consumed for this demand cycle (the claim's audit counter).
    pub attempts: u32,
    /// The pipeline's consecutive-failure count AFTER this failure.
    pub consecutive_failures: u32,
    /// Retrieval pointers — commands, not prose. The diagnosing agent (or
    /// human) runs these; nothing here is narrated.
    pub pointers: Vec<String>,
}

/// Write `bundle` under `<rocky_dir>/incidents/` and sweep to the retention
/// cap. Returns the path written.
///
/// Failures here must never fail the tick: the run's outcome is already
/// committed, and diagnostics that break the thing they diagnose are worse
/// than none — callers warn and continue.
pub fn write_incident(rocky_dir: &Path, bundle: &IncidentBundle) -> io::Result<PathBuf> {
    let dir = rocky_dir.join("incidents");
    std::fs::create_dir_all(&dir)?;
    let short_id: String = bundle.submission_id.chars().take(8).collect();
    let name = format!(
        "{}-{}-{}.json",
        bundle.recorded_at.format("%Y%m%dT%H%M%SZ"),
        sanitize(&bundle.pipeline),
        short_id
    );
    let path = dir.join(name);
    let json = serde_json::to_vec_pretty(bundle)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    std::fs::write(&path, json)?;
    sweep(&dir)?;
    Ok(path)
}

/// Keep the newest [`INCIDENTS_KEPT`] bundles; names sort chronologically.
fn sweep(dir: &Path) -> io::Result<()> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir)?
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|x| x.to_str()) == Some("json"))
        .collect();
    if files.len() <= INCIDENTS_KEPT {
        return Ok(());
    }
    files.sort();
    let excess = files.len() - INCIDENTS_KEPT;
    for old in files.into_iter().take(excess) {
        let _ = std::fs::remove_file(old);
    }
    Ok(())
}

/// Pipeline names reach a filename; keep the charset inert. (Config-validated
/// names are already tame — this is defense in depth, not a parser.)
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
            logical_ts: Some(at),
            submission_id: "sub-12345678-rest".to_string(),
            exit_code: 1,
            attempts: 1,
            consecutive_failures: 3,
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
        assert_eq!(read.consecutive_failures, 3);
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
}
