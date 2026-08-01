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
    // Timestamp prefix: 8 digits, 'T', 6 digits, 'Z' — with the digit groups
    // required to be a plausible calendar instant, not merely digit-shaped,
    // so a foreign `00000000T000000Z-…` name cannot collide into the
    // retention sweep's deletion set.
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
    // A real calendar instant, not merely digit-shaped: chrono rejects the
    // Feb-30 class that independent range bounds cannot (the writer formats
    // with `%Y%m%dT%H%M%SZ`, so this parse accepts exactly what it emits).
    if chrono::NaiveDateTime::parse_from_str(&stem[..15], "%Y%m%dT%H%M%S").is_err() {
        return false;
    }
    // `<pipeline>-<short-id>`: inert charset, at least one separator, and a
    // non-empty segment on BOTH sides of it — `Z--id` (empty pipeline) and
    // `Z-p-` (empty id) are not names the writer produces.
    let rest = &stem[17..];
    !rest.is_empty()
        && rest.contains('-')
        && !rest.starts_with('-')
        && !rest.ends_with('-')
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
        normalize_segment(&sanitize(&bundle.pipeline)),
        normalize_segment(&short_id)
    );
    debug_assert!(is_bundle_name(&name), "{name}");
    let json = serde_json::to_vec_pretty(bundle)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    // Collision-safe: sanitize/normalize are lossy, so two DISTINCT incidents
    // can land on the same name in the same second — a numbered variant keeps
    // both instead of silently overwriting the first. Two invariants matter:
    //
    // * variants are ZERO-PADDED (`-002`…`-999`) so they sort in write order
    //   (the retention sweep sorts names; ASCII `-` < `.` means the base
    //   itself sorts after its variants, but every file in one collision
    //   group shares the same second, so intra-group order is within the
    //   name format's documented 1-second resolution either way);
    // * the slot is chosen as max-existing + 1, NEVER first-free: a slot the
    //   sweep deleted must stay dead, or a reused low slot would sort oldest
    //   and be swept again immediately — freezing retention while every
    //   NEWEST bundle vanishes (and the returned path would not exist).
    let stem = name.strip_suffix(".json").expect("constructed with .json");
    let mut k: u32 = {
        let prefix = format!("{stem}-");
        let mut max_variant = 0;
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let fname = entry.file_name();
            let Some(fname) = fname.to_str() else {
                continue;
            };
            if fname == name {
                max_variant = max_variant.max(1);
            } else if let Some(v) = fname
                .strip_prefix(&prefix)
                .and_then(|r| r.strip_suffix(".json"))
                .and_then(|d| (d.len() == 3).then(|| d.parse::<u32>().ok()).flatten())
            {
                max_variant = max_variant.max(v);
            }
        }
        max_variant
    };
    // The variant namespace is exactly `-002`…`-999` — three digits, so the
    // max-scan can parse every slot it may later need to step past. Fail
    // closed at the boundary instead of minting a 4-digit name the scan
    // cannot see and the sweep would treat as oldest-sorting (the round-4
    // freeze, one namespace up). Callers warn-never-fail.
    let exhausted = || {
        io::Error::other(format!(
            "incident collision namespace exhausted for {stem} (999 variants)"
        ))
    };
    let variant_path = |k: u32| -> io::Result<PathBuf> {
        if k > 999 {
            return Err(exhausted());
        }
        let variant = format!("{stem}-{k:03}.json");
        debug_assert!(is_bundle_name(&variant), "{variant}");
        Ok(dir.join(variant))
    };
    let mut path = dir.join(&name);
    if k > 0 {
        k += 1;
        path = variant_path(k)?;
    }
    loop {
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
        {
            Ok(mut f) => {
                use std::io::Write as _;
                f.write_all(&json)?;
                break;
            }
            // A racing writer took the slot between the scan and the open
            // (ticks are lock-serialized, so this is belt-and-braces for the
            // public API). A raced BASE name jumps to `-002` — the base
            // counts as occupancy 1, so `-001` is never minted by anyone.
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                k = if k == 0 { 2 } else { k + 1 };
                path = variant_path(k)?;
            }
            Err(e) => return Err(e),
        }
    }
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

/// A filename segment must be non-empty and must not begin or end with the
/// `-` the name format uses as its separator — otherwise a legal pipeline
/// name like `-orders` would produce a file the matcher (and therefore the
/// retention sweep and the brief inventory) refuses to recognize as Rocky's
/// own. Empty becomes `_`; boundary hyphens become `_`. Lossy on purpose:
/// the bundle JSON carries the exact names, the filename only has to be
/// unambiguous.
fn normalize_segment(s: &str) -> String {
    if s.is_empty() {
        return "_".to_string();
    }
    let mut chars: Vec<char> = s.chars().collect();
    let last = chars.len() - 1;
    if chars[0] == '-' {
        chars[0] = '_';
    }
    if chars[last] == '-' {
        chars[last] = '_';
    }
    chars.into_iter().collect()
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
            "20260801T120000Z--abcd1234.json",
            "20260801T120000Z-orders-.json",
            "00000000T000000Z-orders-x.json",
            "20261301T120000Z-orders-x.json",
            "20260801T250000Z-orders-x.json",
            "20260832T120000Z-orders-x.json",
            "20260229T120000Z-orders-x.json",
            "20260230T120000Z-orders-x.json",
            "20260431T120000Z-orders-x.json",
        ] {
            assert!(!is_bundle_name(bad), "{bad}");
        }
        // A real leap day is a real instant.
        assert!(is_bundle_name("20280229T120000Z-orders-x.json"));
    }

    /// Two DISTINCT incidents that collide on (second, pipeline segment,
    /// id prefix) — here via sanitize's lossiness — must both survive; a
    /// silent overwrite would lose one incident's facts.
    #[test]
    fn colliding_bundle_names_never_overwrite() {
        let tmp = tempfile::tempdir().unwrap();
        let at = chrono::DateTime::parse_from_rfc3339("2026-08-01T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let p1 = write_incident(tmp.path(), &bundle("a/b", at)).unwrap();
        let p2 = write_incident(tmp.path(), &bundle("a_b", at)).unwrap();
        assert_ne!(p1, p2, "the second write must pick a variant name");
        assert!(p1.exists() && p2.exists(), "both incidents survive");
        let n2 = p2.file_name().unwrap().to_str().unwrap();
        assert!(
            is_bundle_name(n2),
            "the variant is still a bundle name: {n2}"
        );
    }

    /// The round-4 review trace: a flood of DISTINCT incidents colliding on
    /// one base name must roll retention forward — every write's returned
    /// path exists, the newest bundles survive, and a swept slot is never
    /// reused (a reused low slot would sort oldest and be deleted again
    /// immediately, freezing the retained set).
    #[test]
    fn a_single_name_collision_flood_rolls_retention_forward() {
        let tmp = tempfile::tempdir().unwrap();
        let at = chrono::DateTime::parse_from_rfc3339("2026-08-01T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let mut last = None;
        for _ in 0..(INCIDENTS_KEPT + 3) {
            let path = write_incident(tmp.path(), &bundle("p", at)).unwrap();
            assert!(
                path.exists(),
                "a write must never return a path the sweep already deleted: {path:?}"
            );
            last = Some(path);
        }
        let dir = tmp.path().join("incidents");
        let count = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(Result::ok)
            .count();
        assert_eq!(count, INCIDENTS_KEPT, "retention holds at the cap");
        assert!(
            last.unwrap().exists(),
            "the newest bundle must survive its own sweep"
        );
    }

    /// A full variant namespace fails closed: the writer must never mint a
    /// 4-digit variant the max-scan cannot parse — that would recreate the
    /// round-4 freeze one namespace up (swept immediately, Ok for a path
    /// that no longer exists).
    #[test]
    fn an_exhausted_collision_namespace_fails_closed() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("incidents");
        std::fs::create_dir_all(&dir).unwrap();
        let at = chrono::DateTime::parse_from_rfc3339("2026-08-01T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let stem = "20260801T120000Z-p-sub-1234";
        std::fs::write(dir.join(format!("{stem}.json")), "{}").unwrap();
        for k in 2..=999 {
            std::fs::write(dir.join(format!("{stem}-{k:03}.json")), "{}").unwrap();
        }
        let err = write_incident(tmp.path(), &bundle("p", at)).expect_err("namespace full");
        assert!(err.to_string().contains("exhausted"), "{err}");
        assert!(
            !dir.join(format!("{stem}-1000.json")).exists(),
            "no 4-digit variant may be minted"
        );
    }

    /// Writer↔matcher agreement by construction: for ANY pipeline name and
    /// submission id — including the hostile and boundary cases — the file
    /// the writer produces is a file the matcher recognizes. Without this,
    /// a legal `-orders` pipeline would write bundles the retention sweep
    /// and the brief inventory both disown.
    #[test]
    fn every_written_bundle_is_recognized_by_the_matcher() {
        let tmp = tempfile::tempdir().unwrap();
        let at = chrono::Utc::now();
        let pipelines = [
            "orders", "-orders", "orders-", "-", "", "---", "../x", "días",
        ];
        let ids = ["abcdefg-rest", "-x", "x-", "", "sub-1", "12345678deadbeef"];
        for (i, pipeline) in pipelines.iter().enumerate() {
            for (j, id) in ids.iter().enumerate() {
                let mut b = bundle(
                    pipeline,
                    at + chrono::Duration::seconds((i * 10 + j) as i64),
                );
                b.submission_id = (*id).to_string();
                let path = write_incident(tmp.path(), &b).unwrap();
                let name = path.file_name().unwrap().to_str().unwrap();
                assert!(
                    is_bundle_name(name),
                    "writer produced {name} for pipeline {pipeline:?} id {id:?} \
                     but the matcher disowns it"
                );
            }
        }
    }
}
