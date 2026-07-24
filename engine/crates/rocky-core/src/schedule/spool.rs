//! The webhook demand spool — durable, at-most-once HTTP webhook triggers.
//!
//! A webhook `POST /api/v1/hooks/trigger/{pipeline}` is accepted only after its
//! demand is written to a durable file and `fsync`'d, so a crash between the
//! `202` and the next tick never loses an accepted demand. The resident
//! reconciler consumes each spooled demand exactly once by **claiming then
//! deleting**: it drives the demand's claim to a terminal state (spawning the
//! child), then disposes of the spool file. This is *at-most-once*: a demand is
//! attempted at most one time. If the reconciler crashes after claiming AND the
//! child also dies before recording a run, that demand is finalized as a
//! failure and not retried — the documented loss window. A child that outlives a
//! dead reconciler still records its run and is honored.
//!
//! # Crash-safe accept ordering
//!
//! [`accept`] writes a temp file, `fsync`s it, hard-links it into place at the
//! dedup key (an existing link is a duplicate — idempotently acked), `fsync`s
//! the spool directory, then removes the temp. The hard-link is the atomic
//! create-or-detect-duplicate primitive: two concurrent deliveries of the same
//! key race to a single winner, both answered `202`.
//!
//! # Dedup + disposal by [`WebhookKind`]
//!
//! - [`WebhookKind::Id`] (a caller delivery id): consumption renames the file to
//!   a `.done` tombstone kept for [`TOMBSTONE_TTL`], so a redelivery of the same
//!   id after consumption is idempotently acked without re-running.
//! - [`WebhookKind::Body`] (the body hash): consumption unlinks the file with no
//!   tombstone, so an identical body delivered again is a genuinely new demand.
//!
//! # Filename hygiene
//!
//! Raw request bytes never reach a path. The dedup key is the hex `blake3` of
//! the canonical `(pipeline, kind, token)` tuple (see [`dedup_filename`]): a
//! 64-char `[0-9a-f]` name that cannot contain a path separator, `..`, the
//! reserved control affixes (`.done`, `.corrupt-`, `.tmp-`), or any delimiter,
//! and is injective up to a `blake3` collision. So a caller-controlled delivery
//! id can neither escape the spool directory, forge a tombstone, evade the
//! pending scan, nor alias another pipeline's key. The authoritative pipeline /
//! kind / token / demand-uid live in the file's JSON content — the filename is
//! only ever used for dedup, never parsed back.

use std::io;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// How long a consumed [`WebhookKind::Id`] demand's `.done` tombstone is kept so
/// a redelivery of the same id is deduplicated instead of re-run.
pub const TOMBSTONE_TTL: chrono::Duration = chrono::Duration::hours(24);

const TMP_PREFIX: &str = ".tmp-";
const DONE_SUFFIX: &str = ".done";
const CORRUPT_INFIX: &str = ".corrupt-";

/// The webhook demand kind, which governs dedup and disposal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WebhookKind {
    /// Keyed by a caller-supplied delivery id. Consumption leaves a 24h `.done`
    /// tombstone so a redelivery of the same id is idempotently acked.
    Id,
    /// Keyed by the request body hash. No tombstone: an identical body delivered
    /// again after consumption is a new demand.
    Body,
}

impl WebhookKind {
    /// The stable token used in the dedup filename.
    fn as_str(self) -> &'static str {
        match self {
            WebhookKind::Id => "id",
            WebhookKind::Body => "body",
        }
    }
}

/// The durable record written before a webhook is acked with `202`.
///
/// The stored bytes are inert data: the pipeline is validated against config at
/// consume time and the payload is never interpreted, only used to spawn
/// `rocky run --pipeline <pipeline>`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingDemand {
    /// A fresh UUID minted at acceptance; the discriminator of the demand's
    /// claim key so each accepted delivery gets exactly one claim.
    pub demand_uid: String,
    /// The target pipeline (validated against config before acceptance).
    pub pipeline: String,
    /// The dedup kind.
    pub kind: WebhookKind,
    /// The caller-supplied dedup token (raw), stored as inert data.
    pub token: String,
    /// When the demand was accepted (RFC3339).
    pub received_at: String,
    /// `blake3` hex of the raw request body, for auditing.
    pub body_hash: String,
}

/// The outcome of an [`accept`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AcceptOutcome {
    /// A durable demand file was created; carries the minted `demand_uid`.
    Created(String),
    /// A demand with this dedup key is already pending (or, for
    /// [`WebhookKind::Id`], already consumed within the tombstone window) — the
    /// caller acks idempotently.
    Duplicate,
}

/// A pending demand read off the spool, ready to consume.
#[derive(Debug, Clone)]
pub struct PendingEntry {
    /// The spool file path.
    pub path: PathBuf,
    /// The parsed demand.
    pub demand: PendingDemand,
}

/// The ordered filesystem steps of a durable [`accept`] or [`dispose`], recorded
/// by a [`SpoolJournal`] so the crash-safety ordering can be asserted in a test
/// without an actual crash.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpoolStep {
    /// The temp file's bytes were written.
    WriteTmp,
    /// The temp file was `fsync`'d.
    FsyncTmp,
    /// The temp file was hard-linked to the dedup key.
    LinkKey,
    /// The spool directory was `fsync`'d.
    FsyncDir,
    /// The temp file was removed.
    RemoveTmp,
    /// On disposal: the consumption-stamped tombstone inode was `fsync`'d
    /// (file-level `sync_all`) so its mtime is durable before the rename.
    FsyncTombstone,
}

/// Observes the ordered steps of an [`accept`]. Production uses the no-op
/// journal; tests record the sequence.
pub trait SpoolJournal {
    /// Record that `step` is about to run.
    fn record(&self, step: SpoolStep);
}

/// The production journal: records nothing.
struct NoopJournal;

impl SpoolJournal for NoopJournal {
    fn record(&self, _step: SpoolStep) {}
}

/// The `pending-demands` directory under `.rocky`.
pub fn spool_dir(rocky_dir: &Path) -> PathBuf {
    rocky_dir.join("pending-demands")
}

/// The on-disk dedup filename: the hex `blake3` of the canonical
/// `(pipeline, kind, token)` tuple, with each field length-prefixed so the hash
/// input is unambiguous for any byte content.
///
/// Hashing — rather than embedding the caller-controlled fields — is a
/// correctness requirement, not just hygiene:
///
/// - The 64-char result is `[0-9a-f]` only, so it can never contain a path
///   separator, `..`, or **any** reserved control affix (`.done`, `.corrupt-`,
///   `.tmp-`). A caller cannot craft a delivery id (e.g. `evt.done` or
///   `x.corrupt-9`) that forges a tombstone, evades the pending scan, or trips
///   the corrupt-file doctor alarm.
/// - The hash is injective **unconditionally**: each field is length-prefixed
///   (an 8-byte little-endian length before its bytes), so the mapping from
///   `(pipeline, kind, token)` to the hash input is injective for *all* byte
///   inputs — no field can bleed into the next regardless of its contents. Two
///   distinct tuples never alias (up to a `blake3` collision), even a pipeline
///   whose name embeds a former `__` delimiter or a control byte. This does not
///   rest on any unenforced assumption about which bytes a pipeline name or
///   delivery id may contain.
///
/// The readable `{pipeline, kind, token, demand_uid, …}` fields live in the
/// file's JSON content, so debuggability is preserved; the filename is only ever
/// used for dedup and is never parsed back into its parts.
fn dedup_filename(pipeline: &str, kind: WebhookKind, token: &str) -> String {
    let mut hasher = blake3::Hasher::new();
    for field in [
        pipeline.as_bytes(),
        kind.as_str().as_bytes(),
        token.as_bytes(),
    ] {
        hasher.update(&(field.len() as u64).to_le_bytes());
        hasher.update(field);
    }
    hasher.finalize().to_hex().to_string()
}

fn tombstone_path(key_path: &Path) -> PathBuf {
    let mut name = key_path.file_name().unwrap_or_default().to_os_string();
    name.push(DONE_SUFFIX);
    key_path.with_file_name(name)
}

/// Accept a webhook demand durably, returning once it is `fsync`'d to disk.
///
/// # Errors
///
/// Returns an error if the spool directory or files cannot be created, written,
/// or `fsync`'d. A caller must not send `202` on an error — the demand is not
/// durable. This is fail-closed *by design*, including on the duplicate path: a
/// duplicate accept also `fsync`s the directory, so on a hard-failing disk
/// (`EIO`/`ENOSPC`) a duplicate returns an error (surfaced as `500`) rather than
/// the pre-durability `202` — the sender retries, and no ack is ever more
/// durable than the entry it deduplicates against.
pub fn accept(
    rocky_dir: &Path,
    pipeline: &str,
    kind: WebhookKind,
    token: &str,
    body_hash: &str,
    now: DateTime<Utc>,
) -> io::Result<AcceptOutcome> {
    accept_journaled(
        rocky_dir,
        pipeline,
        kind,
        token,
        body_hash,
        now,
        &NoopJournal,
    )
}

/// [`accept`] with an injected [`SpoolJournal`] so a test can assert the
/// tmp-write → tmp-fsync → key-link → dir-fsync ordering that makes the `202`
/// crash-safe.
pub fn accept_journaled(
    rocky_dir: &Path,
    pipeline: &str,
    kind: WebhookKind,
    token: &str,
    body_hash: &str,
    now: DateTime<Utc>,
    journal: &dyn SpoolJournal,
) -> io::Result<AcceptOutcome> {
    let dir = spool_dir(rocky_dir);
    std::fs::create_dir_all(&dir)?;

    let key_name = dedup_filename(pipeline, kind, token);
    let key_path = dir.join(&key_name);

    // Best-effort fast-path dedup for a consumed delivery id: the reconciler is
    // the authority (it also checks the tombstone before running), so a race
    // here can only cost a redundant durable write that the reconciler then
    // drops without running — never a second run. No `fsync_dir` is owed here:
    // this dedups against a tombstone that `dispose` already made durable (its
    // inode `sync_all` + a dir `fsync`), so its existence is not new state we
    // must persist before acking.
    if kind == WebhookKind::Id && tombstone_path(&key_path).exists() {
        return Ok(AcceptOutcome::Duplicate);
    }

    let demand_uid = uuid::Uuid::new_v4().to_string();
    let record = PendingDemand {
        demand_uid: demand_uid.clone(),
        pipeline: pipeline.to_string(),
        kind,
        token: token.to_string(),
        received_at: now.to_rfc3339(),
        body_hash: body_hash.to_string(),
    };
    let bytes =
        serde_json::to_vec(&record).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let tmp_path = dir.join(format!("{TMP_PREFIX}{demand_uid}"));

    journal.record(SpoolStep::WriteTmp);
    std::fs::write(&tmp_path, &bytes)?;

    journal.record(SpoolStep::FsyncTmp);
    fsync_path(&tmp_path)?;

    journal.record(SpoolStep::LinkKey);
    match std::fs::hard_link(&tmp_path, &key_path) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
            journal.record(SpoolStep::RemoveTmp);
            let _ = std::fs::remove_file(&tmp_path);
            // A prior accept may have linked this key but crashed before its own
            // dir-fsync, so the directory entry is not yet durable. `fsync` the
            // directory before we ack a `202`: a duplicate ack must never be more
            // durable than the original entry it deduplicates against.
            journal.record(SpoolStep::FsyncDir);
            fsync_dir(&dir)?;
            return Ok(AcceptOutcome::Duplicate);
        }
        Err(e) => {
            let _ = std::fs::remove_file(&tmp_path);
            return Err(e);
        }
    }

    journal.record(SpoolStep::FsyncDir);
    fsync_dir(&dir)?;

    journal.record(SpoolStep::RemoveTmp);
    let _ = std::fs::remove_file(&tmp_path);

    Ok(AcceptOutcome::Created(demand_uid))
}

/// List the pending demand files in the spool directory: regular files that are
/// not temp scratch, tombstones, or quarantined corruptions. Missing directory
/// yields an empty list.
pub fn list_pending_files(rocky_dir: &Path) -> io::Result<Vec<PathBuf>> {
    let dir = spool_dir(rocky_dir);
    let mut out = Vec::new();
    let entries = match std::fs::read_dir(&dir) {
        Ok(e) => e,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(out),
        Err(e) => return Err(e),
    };
    for entry in entries {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with(TMP_PREFIX)
            || name.ends_with(DONE_SUFFIX)
            || name.contains(CORRUPT_INFIX)
        {
            continue;
        }
        out.push(entry.path());
    }
    // Deterministic order so a tick processes demands reproducibly.
    out.sort();
    Ok(out)
}

/// Read and parse a pending demand file.
///
/// # Errors
///
/// Returns an error if the file cannot be read or its JSON is malformed — the
/// caller quarantines it via [`quarantine`] rather than acting on it.
pub fn read_pending(path: &Path) -> io::Result<PendingDemand> {
    let bytes = std::fs::read(path)?;
    serde_json::from_slice(&bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

/// Whether a consumed-tombstone exists for a pending file's dedup key.
pub fn is_tombstoned(pending: &Path) -> bool {
    tombstone_path(pending).exists()
}

/// Dispose of a consumed demand.
///
/// [`WebhookKind::Id`] is renamed to a `.done` tombstone (kept for
/// [`TOMBSTONE_TTL`]); [`WebhookKind::Body`] is unlinked. A missing file is not
/// an error — disposal is idempotent so a crash between the claim and the
/// disposal self-heals on the next tick.
///
/// # Errors
///
/// Surfaces any filesystem error — including a failure to stamp or `fsync` the
/// tombstone. A silently accept-time-stamped tombstone would reopen the F5
/// double-run window, so a failed disposal is propagated (the caller logs it and
/// the next tick retries, since the claim is already terminal).
pub fn dispose(pending: &Path, kind: WebhookKind) -> io::Result<()> {
    dispose_journaled(pending, kind, &NoopJournal)
}

/// [`dispose`] with an injected [`SpoolJournal`] so a test can assert the
/// tombstone inode is `fsync`'d before the directory entry.
pub fn dispose_journaled(
    pending: &Path,
    kind: WebhookKind,
    journal: &dyn SpoolJournal,
) -> io::Result<()> {
    match kind {
        WebhookKind::Id => {
            // The TTL window must run from CONSUMPTION, not acceptance — else a
            // demand accepted ~24h before it is consumed yields an
            // already-expired tombstone the same tick sweeps, letting the id run
            // twice. `rename` preserves the inode mtime, so stamp the consumption
            // instant onto the (soon-to-be-tombstone) inode AND make it durable
            // with a file-level `sync_all` BEFORE the rename: a crash must never
            // leave a tombstone whose on-disk mtime is still accept-time. A
            // failure to open/stamp/sync is propagated, not swallowed.
            match std::fs::File::options().write(true).open(pending) {
                Ok(f) => {
                    f.set_modified(std::time::SystemTime::now())?;
                    journal.record(SpoolStep::FsyncTombstone);
                    f.sync_all()?;
                }
                // The pending file is already gone — disposal is idempotent.
                Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
                Err(e) => return Err(e),
            }
            let done = tombstone_path(pending);
            match std::fs::rename(pending, &done) {
                Ok(()) => {}
                // Already disposed between the stamp and the rename. Idempotent.
                Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
                Err(e) => return Err(e),
            }
            // Make the tombstone's directory entry durable so a redelivery
            // arriving after a crash still deduplicates against it.
            if let Some(parent) = done.parent() {
                journal.record(SpoolStep::FsyncDir);
                fsync_dir(parent)?;
            }
            Ok(())
        }
        WebhookKind::Body => remove_ignoring_missing(pending),
    }
}

/// Drop a duplicate pending file that must not run (its delivery id was already
/// consumed within the tombstone window). Unlinks the file, leaving the existing
/// tombstone intact.
pub fn drop_pending(pending: &Path) -> io::Result<()> {
    remove_ignoring_missing(pending)
}

/// Quarantine an unparseable pending file to `<name>.corrupt-<unix_ts>` so a
/// tick never silently deletes payload it could not interpret and `rocky doctor`
/// can surface it. Returns the quarantine path.
pub fn quarantine(pending: &Path, now: DateTime<Utc>) -> io::Result<PathBuf> {
    let mut name = pending.file_name().unwrap_or_default().to_os_string();
    name.push(format!("{CORRUPT_INFIX}{}", now.timestamp()));
    let dest = pending.with_file_name(name);
    std::fs::rename(pending, &dest)?;
    Ok(dest)
}

/// Count quarantined-corrupt files in the spool directory (for a `rocky doctor`
/// warning). Missing directory yields `0`.
pub fn count_corrupt(rocky_dir: &Path) -> io::Result<usize> {
    let dir = spool_dir(rocky_dir);
    let entries = match std::fs::read_dir(&dir) {
        Ok(e) => e,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e),
    };
    let mut n = 0;
    for entry in entries {
        let entry = entry?;
        if entry.file_name().to_string_lossy().contains(CORRUPT_INFIX) {
            n += 1;
        }
    }
    Ok(n)
}

/// Sweep `.done` tombstones older than [`TOMBSTONE_TTL`], so the id-dedup window
/// is bounded and the spool directory does not grow without limit. Returns the
/// number swept. A tombstone whose age cannot be read is left in place.
pub fn sweep_tombstones(rocky_dir: &Path, now: DateTime<Utc>) -> io::Result<usize> {
    let dir = spool_dir(rocky_dir);
    let entries = match std::fs::read_dir(&dir) {
        Ok(e) => e,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e),
    };
    let mut swept = 0;
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name();
        if !name.to_string_lossy().ends_with(DONE_SUFFIX) {
            continue;
        }
        let Ok(meta) = entry.metadata() else {
            continue;
        };
        let Ok(modified) = meta.modified() else {
            continue;
        };
        let modified: DateTime<Utc> = modified.into();
        if now - modified >= TOMBSTONE_TTL && std::fs::remove_file(entry.path()).is_ok() {
            swept += 1;
        }
    }
    Ok(swept)
}

fn remove_ignoring_missing(path: &Path) -> io::Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

/// `fsync` a file so its contents are durable before it is linked into place.
fn fsync_path(path: &Path) -> io::Result<()> {
    let f = std::fs::File::open(path)?;
    f.sync_all()
}

/// `fsync` a directory so a newly created directory entry (the hard link) is
/// durable. On platforms where a directory cannot be opened for sync this is a
/// best-effort no-op.
fn fsync_dir(dir: &Path) -> io::Result<()> {
    match std::fs::File::open(dir) {
        Ok(f) => match f.sync_all() {
            Ok(()) => Ok(()),
            // A few filesystems reject `fsync` on a directory fd with EINVAL/EBADF
            // (some network/virtual filesystems). The Linux release targets
            // (ext4/xfs/btrfs) and macOS all honor it, so this fallback does NOT
            // fire there — on those the directory entry IS durable before the 202.
            // Where it does fire, the link is still written and only a
            // simultaneous power loss could drop the just-created entry: the
            // accepted, documented at-most-once bound for such a filesystem.
            Err(e) if matches!(e.raw_os_error(), Some(libc::EINVAL) | Some(libc::EBADF)) => Ok(()),
            Err(e) => Err(e),
        },
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    use chrono::TimeZone;

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 5, 1, 0, 0, 0).unwrap()
    }

    /// A journal that records the step order for the crash-safety assertion.
    #[derive(Default)]
    struct RecordingJournal {
        steps: Mutex<Vec<SpoolStep>>,
    }

    impl SpoolJournal for RecordingJournal {
        fn record(&self, step: SpoolStep) {
            self.steps.lock().unwrap().push(step);
        }
    }

    #[test]
    fn accept_orders_tmp_fsync_before_link_before_dir_fsync() {
        let dir = tempfile::tempdir().unwrap();
        let journal = RecordingJournal::default();
        let outcome = accept_journaled(
            dir.path(),
            "orders",
            WebhookKind::Id,
            "evt-1",
            "hash",
            now(),
            &journal,
        )
        .unwrap();
        assert!(matches!(outcome, AcceptOutcome::Created(_)));
        let steps = journal.steps.lock().unwrap().clone();
        // The crash-safe ordering: content is written and fsync'd BEFORE the key
        // link makes it visible, and the directory entry is fsync'd BEFORE the
        // 202 the caller sends on return.
        assert_eq!(
            steps,
            vec![
                SpoolStep::WriteTmp,
                SpoolStep::FsyncTmp,
                SpoolStep::LinkKey,
                SpoolStep::FsyncDir,
                SpoolStep::RemoveTmp,
            ]
        );
        // Exactly one durable pending file landed inside the spool dir.
        let pending = list_pending_files(dir.path()).unwrap();
        assert_eq!(pending.len(), 1);
        assert!(pending[0].starts_with(spool_dir(dir.path())));
    }

    #[test]
    fn same_key_twice_is_a_single_file_and_a_duplicate() {
        let dir = tempfile::tempdir().unwrap();
        let a = accept(dir.path(), "p", WebhookKind::Id, "evt", "h", now()).unwrap();
        let b = accept(dir.path(), "p", WebhookKind::Id, "evt", "h", now()).unwrap();
        assert!(matches!(a, AcceptOutcome::Created(_)));
        assert_eq!(b, AcceptOutcome::Duplicate);
        assert_eq!(list_pending_files(dir.path()).unwrap().len(), 1);
    }

    #[test]
    fn a_traversal_id_lands_inside_the_spool_dir() {
        let dir = tempfile::tempdir().unwrap();
        accept(
            dir.path(),
            "p",
            WebhookKind::Id,
            "../../etc/passwd",
            "h",
            now(),
        )
        .unwrap();
        let pending = list_pending_files(dir.path()).unwrap();
        assert_eq!(pending.len(), 1);
        // The file is INSIDE the spool dir — no traversal — and its name carries
        // no path separator.
        let spool = spool_dir(dir.path());
        assert_eq!(pending[0].parent().unwrap(), spool);
        let name = pending[0].file_name().unwrap().to_string_lossy();
        assert!(!name.contains('/'));
        // The dirty token was hashed, not embedded verbatim.
        assert!(!name.contains(".."));
    }

    #[test]
    fn an_oversized_id_is_hashed_and_stays_a_valid_filename() {
        let dir = tempfile::tempdir().unwrap();
        let big = "a".repeat(4096);
        accept(dir.path(), "p", WebhookKind::Id, &big, "h", now()).unwrap();
        let pending = list_pending_files(dir.path()).unwrap();
        assert_eq!(pending.len(), 1);
        let name = pending[0].file_name().unwrap().to_string_lossy();
        // `<p>__id__<64-hex-blake3>` — comfortably under NAME_MAX.
        assert!(name.len() < 255, "hashed filename must be a valid length");
    }

    #[test]
    fn id_redelivery_after_consumption_is_deduped_by_the_tombstone() {
        let dir = tempfile::tempdir().unwrap();
        accept(dir.path(), "p", WebhookKind::Id, "evt", "h", now()).unwrap();
        let pending = list_pending_files(dir.path()).unwrap();
        // Simulate consumption: dispose to a `.done` tombstone.
        dispose(&pending[0], WebhookKind::Id).unwrap();
        assert!(list_pending_files(dir.path()).unwrap().is_empty());
        // A redelivery of the same id is a duplicate (tombstone fast-path).
        let again = accept(dir.path(), "p", WebhookKind::Id, "evt", "h", now()).unwrap();
        assert_eq!(again, AcceptOutcome::Duplicate);
        assert!(list_pending_files(dir.path()).unwrap().is_empty());
    }

    #[test]
    fn body_kind_has_no_tombstone_so_it_fires_again_after_consumption() {
        let dir = tempfile::tempdir().unwrap();
        let body_hash = "bodyhash";
        accept(
            dir.path(),
            "p",
            WebhookKind::Body,
            body_hash,
            body_hash,
            now(),
        )
        .unwrap();
        let pending = list_pending_files(dir.path()).unwrap();
        dispose(&pending[0], WebhookKind::Body).unwrap();
        assert!(list_pending_files(dir.path()).unwrap().is_empty());
        // No tombstone → the identical body is a NEW demand.
        let again = accept(
            dir.path(),
            "p",
            WebhookKind::Body,
            body_hash,
            body_hash,
            now(),
        )
        .unwrap();
        assert!(matches!(again, AcceptOutcome::Created(_)));
        assert_eq!(list_pending_files(dir.path()).unwrap().len(), 1);
    }

    #[test]
    fn identical_body_to_two_pipelines_is_two_demands() {
        let dir = tempfile::tempdir().unwrap();
        let bh = "same";
        accept(dir.path(), "alpha", WebhookKind::Body, bh, bh, now()).unwrap();
        accept(dir.path(), "beta", WebhookKind::Body, bh, bh, now()).unwrap();
        assert_eq!(list_pending_files(dir.path()).unwrap().len(), 2);
    }

    #[test]
    fn a_corrupt_file_quarantines_and_is_no_longer_pending() {
        let dir = tempfile::tempdir().unwrap();
        let spool = spool_dir(dir.path());
        std::fs::create_dir_all(&spool).unwrap();
        let bad = spool.join("p__id__x");
        std::fs::write(&bad, b"not json").unwrap();
        assert!(read_pending(&bad).is_err());
        let dest = quarantine(&bad, now()).unwrap();
        assert!(dest.to_string_lossy().contains(CORRUPT_INFIX));
        // Quarantined files are excluded from the pending scan and counted.
        assert!(list_pending_files(dir.path()).unwrap().is_empty());
        assert_eq!(count_corrupt(dir.path()).unwrap(), 1);
    }

    #[test]
    fn sweep_drops_tombstones_past_the_ttl_only() {
        // The sweep compares the tombstone's real filesystem mtime against the
        // reconciler's wall clock, so this test uses real time and ages the file.
        let dir = tempfile::tempdir().unwrap();
        let real_now = Utc::now();
        accept(dir.path(), "p", WebhookKind::Id, "evt", "h", real_now).unwrap();
        let pending = list_pending_files(dir.path()).unwrap();
        let tomb = tombstone_path(&pending[0]);
        dispose(&pending[0], WebhookKind::Id).unwrap();
        // A tombstone freshly created is NOT swept (its mtime is ~now).
        assert_eq!(sweep_tombstones(dir.path(), Utc::now()).unwrap(), 0);
        // Age the tombstone past the TTL, then sweep removes exactly it.
        let old = std::time::SystemTime::now() - std::time::Duration::from_secs(25 * 3600);
        std::fs::File::options()
            .write(true)
            .open(&tomb)
            .unwrap()
            .set_modified(old)
            .unwrap();
        assert_eq!(sweep_tombstones(dir.path(), Utc::now()).unwrap(), 1);
    }

    #[test]
    fn dispose_is_idempotent_when_the_file_is_already_gone() {
        let dir = tempfile::tempdir().unwrap();
        let spool = spool_dir(dir.path());
        std::fs::create_dir_all(&spool).unwrap();
        let missing = spool.join("p__id__gone");
        // Neither disposal errors on a missing file — crash self-heal relies on it.
        dispose(&missing, WebhookKind::Id).unwrap();
        dispose(&missing, WebhookKind::Body).unwrap();
    }

    // --- filename-encoding hardening (red-team F1/F2/F3) ---------------------

    /// F1: a delivery id ending in the reserved `.done` suffix must NOT forge a
    /// tombstone for the DISTINCT sibling id `evt` — both are real demands.
    #[test]
    fn delivery_id_ending_in_done_does_not_forge_a_tombstone() {
        let dir = tempfile::tempdir().unwrap();
        accept(dir.path(), "p", WebhookKind::Id, "evt.done", "h", now()).unwrap();
        accept(dir.path(), "p", WebhookKind::Id, "evt", "h", now()).unwrap();
        // Two distinct pending demands — neither suppressed the other.
        assert_eq!(list_pending_files(dir.path()).unwrap().len(), 2);
        assert_ne!(
            dedup_filename("p", WebhookKind::Id, "evt.done"),
            dedup_filename("p", WebhookKind::Id, "evt"),
        );
        // And `evt` genuinely has no tombstone forged for it.
        let evt_key = spool_dir(dir.path()).join(dedup_filename("p", WebhookKind::Id, "evt"));
        assert!(!is_tombstoned(&evt_key));
    }

    /// F2: a token embedding a reserved control affix (`.tmp-`, `.corrupt-`,
    /// `.done`) or a separator must still be scanned as pending and never raise a
    /// false corrupt alarm — the hex key is disjoint from every affix.
    #[test]
    fn token_shaped_like_a_control_affix_is_visible_not_hidden() {
        let dir = tempfile::tempdir().unwrap();
        for token in [".tmp-x", "x.corrupt-9", "x.done", "..", "a/b__id__c"] {
            accept(dir.path(), "p", WebhookKind::Id, token, "h", now()).unwrap();
        }
        assert_eq!(
            list_pending_files(dir.path()).unwrap().len(),
            5,
            "every affix-shaped token is a visible pending demand"
        );
        assert_eq!(
            count_corrupt(dir.path()).unwrap(),
            0,
            "an affix-shaped token must not trip the corrupt alarm"
        );
    }

    /// F3: distinct `(pipeline, kind, token)` tuples never alias, even when a
    /// pipeline name embeds a former `__` delimiter, and id vs body tokens with
    /// the same bytes are distinct. The key is 64 hex chars, disjoint from every
    /// reserved affix.
    #[test]
    fn distinct_tuples_never_alias() {
        assert_ne!(
            dedup_filename("a", WebhookKind::Id, "b__id__c"),
            dedup_filename("a__id__b", WebhookKind::Id, "c"),
        );
        assert_ne!(
            dedup_filename("p", WebhookKind::Id, "abc"),
            dedup_filename("p", WebhookKind::Body, "abc"),
        );
        let k = dedup_filename("p", WebhookKind::Id, "evt");
        assert_eq!(k.len(), 64);
        assert!(k.bytes().all(|b| b.is_ascii_hexdigit()));
        assert!(!k.contains('.') && !k.contains('_') && !k.contains('-') && !k.contains('/'));
    }

    /// F4: a duplicate accept must `fsync` the directory before its `202`, so a
    /// duplicate ack is never more durable than the entry it deduplicates.
    #[test]
    fn duplicate_accept_dir_fsyncs_before_the_ack() {
        let dir = tempfile::tempdir().unwrap();
        accept(dir.path(), "p", WebhookKind::Id, "evt", "h", now()).unwrap();
        let journal = RecordingJournal::default();
        let out = accept_journaled(
            dir.path(),
            "p",
            WebhookKind::Id,
            "evt",
            "h",
            now(),
            &journal,
        )
        .unwrap();
        assert_eq!(out, AcceptOutcome::Duplicate);
        let steps = journal.steps.lock().unwrap().clone();
        assert!(
            steps.contains(&SpoolStep::FsyncDir),
            "the duplicate path must dir-fsync before returning: {steps:?}"
        );
    }

    /// F5: the tombstone TTL is measured from CONSUMPTION, not acceptance. A
    /// demand accepted ~25h before it is consumed must still leave a fresh
    /// tombstone that a same-tick sweep does not reap (else its id re-runs).
    #[test]
    fn tombstone_ttl_runs_from_consumption_not_accept() {
        let dir = tempfile::tempdir().unwrap();
        accept(dir.path(), "p", WebhookKind::Id, "evt", "h", Utc::now()).unwrap();
        let pending = list_pending_files(dir.path()).unwrap();
        let tomb = tombstone_path(&pending[0]);
        // Age the PENDING file to ~25h ago (a demand that sat before consumption).
        let old = std::time::SystemTime::now() - std::time::Duration::from_secs(25 * 3600);
        std::fs::File::options()
            .write(true)
            .open(&pending[0])
            .unwrap()
            .set_modified(old)
            .unwrap();
        // Consume it now.
        dispose(&pending[0], WebhookKind::Id).unwrap();
        // The tombstone's clock runs from consumption, so a same-instant sweep
        // leaves it — and a redelivery still deduplicates.
        assert_eq!(sweep_tombstones(dir.path(), Utc::now()).unwrap(), 0);
        assert!(tomb.exists(), "the tombstone survives a same-tick sweep");
        let again = accept(dir.path(), "p", WebhookKind::Id, "evt", "h", Utc::now()).unwrap();
        assert_eq!(again, AcceptOutcome::Duplicate);
    }

    /// R3-1: the consumption-stamped tombstone inode is made durable with a
    /// file-level `fsync` BEFORE the directory entry is fsync'd — otherwise the
    /// new mtime lives only in the inode cache and a crash reopens the F5
    /// double-run window.
    #[test]
    fn dispose_fsyncs_the_tombstone_inode_before_the_dir() {
        let dir = tempfile::tempdir().unwrap();
        accept(dir.path(), "p", WebhookKind::Id, "evt", "h", Utc::now()).unwrap();
        let pending = list_pending_files(dir.path()).unwrap();
        let journal = RecordingJournal::default();
        dispose_journaled(&pending[0], WebhookKind::Id, &journal).unwrap();
        let steps = journal.steps.lock().unwrap().clone();
        let inode = steps.iter().position(|s| *s == SpoolStep::FsyncTombstone);
        let dir_step = steps.iter().position(|s| *s == SpoolStep::FsyncDir);
        assert!(
            inode.is_some() && dir_step.is_some(),
            "both the tombstone-inode fsync and the dir fsync must run: {steps:?}"
        );
        assert!(
            inode < dir_step,
            "the tombstone inode must be fsync'd before the dir: {steps:?}"
        );
    }

    /// R3-1: a failure to stamp the tombstone (here: a read-only pending file
    /// that cannot be opened for write) is propagated, not swallowed — a silent
    /// accept-time tombstone would reopen the F5 window without a crash.
    #[cfg(unix)]
    #[test]
    fn dispose_propagates_a_stamp_failure_instead_of_swallowing_it() {
        use std::os::unix::fs::PermissionsExt;

        // SAFETY: `geteuid` is an always-safe libc call with no preconditions.
        if unsafe { libc::geteuid() } == 0 {
            return; // root bypasses the write-permission check; nothing to assert.
        }
        let dir = tempfile::tempdir().unwrap();
        accept(dir.path(), "p", WebhookKind::Id, "evt", "h", Utc::now()).unwrap();
        let pending = list_pending_files(dir.path()).unwrap();
        // Clear the write bits: a non-root owner can no longer open it for write,
        // so the mtime stamp fails.
        std::fs::set_permissions(&pending[0], std::fs::Permissions::from_mode(0o444)).unwrap();

        let result = dispose(&pending[0], WebhookKind::Id);

        // Restore writability so the tempdir can clean up regardless of outcome.
        let _ = std::fs::set_permissions(&pending[0], std::fs::Permissions::from_mode(0o644));

        assert!(
            result.is_err(),
            "a failed mtime stamp must propagate, not silently leave an accept-time tombstone"
        );
        assert!(
            pending[0].exists(),
            "a failed stamp leaves the pending file intact for the next tick to retry"
        );
    }
}
