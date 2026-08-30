//! The filesystem commit protocol for a lowering generation, and its
//! recovery.
//!
//! Every generation commits via staged same-directory tmp files with the
//! manifest renamed LAST as the commit marker; recovery treats a journal
//! without a committed manifest as an uncommitted generation and rolls
//! the staged files back. The pure lowering lives in
//! [`crate::product::lowering`]; this module owns the writes.
//!
//! # Crash boundary (v0), stated plainly
//!
//! The guarantee is PROCESS-LEVEL logical recovery — if this process dies
//! at any point in the protocol (panic, SIGKILL, OOM), the next run
//! restores the previous generation exactly. That claim is proved by
//! tests that kill a real child process between the staged renames. It is
//! NOT a durability guarantee against OS crash or power loss: nothing
//! here fsyncs staged bytes, renamed files, or their directories, so a
//! kernel-level failure can persist the rename sequence partially or out
//! of order in ways the journal cannot distinguish. v0 accepts that
//! boundary and says so rather than dressing rename ordering up as
//! fsync-grade durability.
//!
//! # The journal is input, not authority
//!
//! Any process that can write the state directory can forge the staging
//! journal, so recovery treats it as untrusted input: every path it
//! names is validated (relative, canonical, resolved-containment,
//! never a symlink at a final or in the staged/prev residue), constrained
//! to the artifact namespace the CALLER's approved spec can actually
//! stage, and refused outright on any violation — before a single
//! mutation. No on-disk manifest grants recovery authority: staged and
//! previous manifests are exactly as forgeable as the journal itself.
//!
//! Stated residuals, accepted under the v0 same-machine threat posture.
//! Path-based syscalls re-traverse the path at syscall time, so a
//! DIRECTORY swapped for a symlink in the instant between validation and
//! a rename/unlink is only fully closed by dirfd-relative APIs, which v0
//! does not use. Every LEAF the protocol writes or reads is guarded at
//! the syscall itself — O_EXCL on each write, `O_NOFOLLOW` on the backup
//! read — but `O_NOFOLLOW` is unix only: on Windows that read still
//! follows a symlink or junction planted at the leaf, and containment
//! there rests on the pre-check alone. Windows reparse-point behaviour is
//! untested; every symlink exploit test in this module is `#[cfg(unix)]`.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::fulfill::FulfillState;
use crate::product::lowering::{
    Lowering, contract_rel, lower_phase_a, lower_phase_b, manifest_rel, sidecar_rel, sql_rel,
    state_dir_rel,
};
use crate::product::manifest::{
    MANIFEST_FILENAME, Manifest, ManifestPhase, contained_artifact_path, content_digest,
    verify_artifact_hashes,
};
use crate::product::spec::{ParsedSpec, SpecRejected, SpecResult};
use crate::state::StateStore;

/// Suffix of a staged (not yet renamed) file, written next to its final.
pub const STAGED_SUFFIX: &str = ".ff-staged";
/// Suffix of the backup taken of a file about to be replaced.
pub const PREV_SUFFIX: &str = ".ff-prev";
/// File name of the staging journal inside the product's state directory.
pub const STAGING_JOURNAL: &str = "phase-staging.json";

/// What [`recover_generation`] found and did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryAction {
    /// No staging journal: nothing to recover.
    None,
    /// An uncommitted generation was rolled back to the prior tree.
    RolledBack,
    /// A committed generation's leftovers were swept.
    RolledForward,
}

/// What [`reopen_for_drafting`] found and did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReopenOutcome {
    /// No committed manifest exists, or it is already at Phase A: the
    /// drafting window is open (or there is nothing to demote) and
    /// nothing was written.
    NotNeeded,
    /// Every artifact byte-verified against the committed merged
    /// manifest, and the manifest was then demoted to Phase A through
    /// the staged commit — the drafting window is open again.
    Reopened,
    /// Artifact bytes drifted from the committed merged manifest while
    /// no write was authorized. Nothing was mutated; each entry is a
    /// [`verify_artifact_hashes`] problem rendering.
    Tampered(Vec<String>),
}

// ---------------------------------------------------------------------------
// The staging journal (strict schema — it is read back as mutation authority)
// ---------------------------------------------------------------------------

/// One staged file: its project-root-relative final path, the staged
/// content hash, and whether a prior version was backed up.
///
/// The schema is closed and uncoerced: the journal is read back at
/// recovery time as filesystem-mutation authority, so it parses under
/// exactly this shape or recovery refuses to run. (`serde_json` never
/// coerces types — a string where a bool belongs is a parse error, which
/// is the strictness this record requires.)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StagingEntry {
    /// Project-root-relative POSIX path of the final file.
    #[serde(rename = "final")]
    final_path: String,
    /// `sha256:<hex>` over the staged bytes.
    staged_sha: String,
    /// Whether a `.ff-prev` backup of a prior final was taken.
    has_prev: bool,
}

/// The staging record [`commit_generation`] writes before renaming.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StagingJournal {
    entries: Vec<StagingEntry>,
    manifest: String,
}

fn journal_path(project_root: &Path, product_name: &str) -> PathBuf {
    project_root
        .join(state_dir_rel(product_name))
        .join(STAGING_JOURNAL)
}

fn journal_reject(journal: &Path, code: &'static str, detail: &str) -> SpecRejected {
    SpecRejected::new(
        code,
        format!(
            "staging journal {} {detail} — refusing automatic recovery; inspect the \
             .ff-staged/.ff-prev files by hand",
            journal.display()
        ),
    )
}

// ---------------------------------------------------------------------------
// Path containment: journal-supplied paths are proven in-root before use
// ---------------------------------------------------------------------------

/// Resolve `path` the way a non-strict `realpath` would: canonicalize the
/// longest existing prefix (resolving symlinks), then append the
/// remaining components lexically.
///
/// The lexical tail is safe here because every caller has already
/// refused `.` and `..` components, so appending cannot walk anywhere a
/// canonical prefix did not already reach.
fn resolve_nonstrict(path: &Path) -> PathBuf {
    let mut existing = path.to_path_buf();
    let mut tail: Vec<std::ffi::OsString> = Vec::new();
    loop {
        match existing.canonicalize() {
            Ok(mut resolved) => {
                for component in tail.iter().rev() {
                    resolved.push(component);
                }
                return resolved;
            }
            Err(_) => match (existing.parent(), existing.file_name()) {
                (Some(parent), Some(name)) => {
                    tail.push(name.to_os_string());
                    existing = parent.to_path_buf();
                }
                // Nothing along the path exists at all — return the input
                // unresolved; the containment comparison below then fails
                // closed for anything outside the (existing) root.
                _ => return path.to_path_buf(),
            },
        }
    }
}

/// True when `p` is a symlink itself (never following it).
fn is_symlink(p: &Path) -> bool {
    std::fs::symlink_metadata(p).is_ok_and(|m| m.file_type().is_symlink())
}

/// True when `rel` is a canonical relative POSIX path: non-empty, no
/// empty segments (which also refuses leading/trailing/double slashes),
/// and no `.` / `..` components.
fn is_canonical_relative_posix(rel: &str) -> bool {
    !rel.is_empty()
        && rel
            .split('/')
            .all(|segment| !segment.is_empty() && segment != "." && segment != "..")
}

/// Validate one journal-supplied path and return its RESOLVED absolute.
///
/// The journal is untrusted input, so every path it names must be proven
/// to stay inside the project root BEFORE recovery mutates anything:
///
/// - relative only — an absolute path is refused;
/// - in canonical form — no `..`, `.`, or empty segments, no
///   leading/trailing slash games ([`commit_generation`] only ever writes
///   canonical root-relative POSIX paths). The lexical half reuses
///   [`contained_artifact_path`], the same primitive the manifest's
///   byte-verification walks through;
/// - directory-contained — the PARENT directory is resolved against
///   `resolved_root` (the project root, resolved ONCE by the caller), so
///   an intermediate component that is a symlink pointing outside the
///   root is refused rather than followed;
/// - **the final component is NEVER followed** — the lowering writes only
///   regular files, so a symlink sitting AT an artifact path is tampering
///   by definition; following it would let the link redirect the mutation
///   loop (restore, unlink) at any in-root target of the attacker's
///   choosing. A symlink final (dangling or not) is refused outright, and
///   so is a directory.
///
/// What comes back is the resolved parent joined with the literal final
/// name, and every later mutation must operate on it exclusively —
/// validating one spelling and then mutating through an unresolved alias
/// would reopen the window where a parent directory is swapped for a
/// symlink between the check and the use.
///
/// This is the SINGLE containment primitive — recovery, the fresh commit
/// path, and the approve snapshot seam all route through it, so a
/// leaf-only guard can never diverge from it and let an ancestor symlink
/// through. It refuses:
///
/// - an absolute or traversing (`.`/`..`/empty-segment) spelling;
/// - a parent that resolves OUTSIDE the root — this catches a static live
///   symlinked ancestor (`models -> /outside`, a malicious checkout or
///   tarball, no race) because its resolved parent escapes; an in-project
///   symlinked ancestor (`models -> models_real`, both under root) resolves
///   in-place and is allowed;
/// - a DANGLING symlinked ancestor (`models -> /nonexistent`), the one
///   escape resolution's lexical fallback would otherwise miss;
/// - a symlink or a directory at the leaf.
///
/// Reason strings on the `Err`; each caller assigns its own stable code
/// ([`contained_final_path`] → `staging-journal-unsafe-path`, the commit
/// path → `commit-symlinked-target`, approve → `approval-snapshot-tampered`).
fn contained_target(resolved_root: &Path, rel: &str) -> Result<PathBuf, String> {
    if Path::new(rel).is_absolute() || rel.starts_with('/') {
        return Err(format!("names absolute path '{rel}'"));
    }
    // The canonical-spelling check plus the component-shape check the
    // manifest verifier already applies (every component a plain name).
    if !is_canonical_relative_posix(rel) || contained_artifact_path(resolved_root, rel).is_none() {
        return Err(format!("names non-canonical or traversing path '{rel}'"));
    }
    let candidate = resolved_root.join(rel);
    // The static ancestor attack a leaf-only `is_symlink` misses, no race:
    // an attacker pre-plants `models -> /outside` and a regular file at
    // `/outside/<leaf>`; a leaf probe passes (the leaf is a regular file at
    // the resolved location) and the write truncates it out of the project.
    //
    // A LIVE escaping ancestor is caught by the parent-resolution + root
    // containment just below (its resolved parent escapes the root), while a
    // LIVE in-project symlinked ancestor (`models -> models_real`, both under
    // root) legitimately resolves and is allowed. The one case resolution
    // misses is a DANGLING symlinked ancestor: `resolve_nonstrict` falls back
    // to a lexical join when a component cannot canonicalize, which would
    // false-accept `models -> /nonexistent` — and the subsequent
    // `create_dir_all` would follow that link and create OUTSIDE the project.
    // Refuse exactly that: an ancestor that is a symlink AND does not
    // canonicalize.
    let mut ancestor = candidate.parent();
    while let Some(dir) = ancestor {
        if dir == resolved_root || !dir.starts_with(resolved_root) {
            break;
        }
        if is_symlink(dir) && dir.canonicalize().is_err() {
            return Err(format!(
                "path '{rel}' has a dangling symlinked ancestor directory '{}' — refusing \
                 rather than creating outside the project through it",
                dir.display()
            ));
        }
        ancestor = dir.parent();
    }
    let parent = candidate
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| resolved_root.to_path_buf());
    let resolved_parent = resolve_nonstrict(&parent);
    if !(resolved_parent == *resolved_root || resolved_parent.starts_with(resolved_root)) {
        return Err(format!(
            "path '{rel}' escapes the project root (parent resolves to {})",
            resolved_parent.display()
        ));
    }
    let Some(file_name) = candidate.file_name().map(std::ffi::OsStr::to_os_string) else {
        return Err(format!("names non-canonical or traversing path '{rel}'"));
    };
    let final_path = resolved_parent.join(file_name);
    if is_symlink(&final_path) {
        return Err(format!(
            "path '{rel}' is a symlink — a generation writes only regular files, and a link \
             at the target would redirect the write at its target"
        ));
    }
    if final_path.is_dir() {
        return Err(format!(
            "path '{rel}' names a directory — targets are always files"
        ));
    }
    Ok(final_path)
}

/// Recovery's wrapper over the shared [`contained_target`] primitive:
/// assign the untrusted-journal refusal code.
fn contained_final_path(resolved_root: &Path, journal: &Path, rel: &str) -> SpecResult<PathBuf> {
    contained_target(resolved_root, rel)
        .map_err(|reason| journal_reject(journal, "staging-journal-unsafe-path", &reason))
}

/// The public single-target containment check for callers outside this
/// module (the approve snapshot seam in `rocky-cli`): canonicalizes the
/// project root once, then applies the shared [`contained_target`]
/// primitive. Refuses an absolute/traversing spelling, a symlinked
/// ancestor (a symlinked state dir would redirect the snapshot temp write,
/// its `remove_file`, and its `create_new` out of the project), and a
/// symlink or directory at the target. Reason string on the `Err`.
pub fn contained_write_target(project_root: &Path, rel: &str) -> Result<PathBuf, String> {
    let resolved_root = project_root.canonicalize().map_err(|err| {
        format!(
            "project root {} is unreadable: {err}",
            project_root.display()
        )
    })?;
    contained_target(&resolved_root, rel)
}

/// Write `bytes` to a brand-new regular file, refusing to follow a symlink
/// at the leaf.
///
/// `create_new` (O_CREAT|O_EXCL) neither follows a link at the final
/// component nor clobbers an existing file, so a link swapped in after a
/// pre-check — during the race window the conceded v0 posture accepts — is
/// refused rather than followed. The only legitimate `AlreadyExists` is our
/// own stale scratch from a prior crash (the restage-over-orphans case):
/// it is removed via `remove_file`, which never follows a link either, and
/// the O_EXCL create retried once.
fn write_new_no_follow(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write as _;
    // `None` keeps the process default (0666 & umask) these staged
    // artifacts have always been created with — they are renamed into place
    // as the user's own project files.
    create_new_no_follow(path, None)?.write_all(bytes)
}

/// The open half of [`write_new_no_follow`], handing back the handle so a
/// caller can also set the mode on the descriptor rather than by path.
fn create_new_no_follow(path: &Path, mode: Option<u32>) -> std::io::Result<std::fs::File> {
    let open = || {
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        // A caller that will set the final mode itself creates restrictively
        // first, so the file is never briefly WIDER than its source: a
        // `0600` original copied under the default `0666 & umask` would be
        // world-readable for the length of the write, and stay that way if
        // the later chmod fails.
        #[cfg(unix)]
        if let Some(mode) = mode {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(mode);
        }
        #[cfg(not(unix))]
        let _ = mode;
        options.open(path)
    };
    match open() {
        Ok(file) => Ok(file),
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
            std::fs::remove_file(path)?;
            open()
        }
        Err(err) => Err(err),
    }
}

/// Read `path` whole, refusing to follow a symlink at the leaf, and hand
/// back its mode alongside the bytes.
///
/// On unix the open carries `O_NOFOLLOW`, so a link swapped in at the leaf
/// AFTER a path-based pre-check — the TOCTOU window no re-check can close,
/// because a path syscall re-traverses the path — fails instead of being
/// read through to its target. Windows `OpenOptions` has no `O_NOFOLLOW`
/// equivalent, so there the read still follows a symlink or junction: that
/// platform keeps the weaker pre-check-only guarantee, stated rather than
/// papered over.
///
/// The mode comes off the DESCRIPTOR, not a second path lookup, so it
/// describes the same file the bytes came from.
/// The largest artifact this module will back up.
///
/// The files copied here are a lowered contract, sidecar, SQL model, and
/// manifest — kilobytes. `std::fs::copy` streamed, so it could not be made
/// to exhaust memory; reading whole can be, which is why the size is
/// bounded rather than trusted.
const MAX_BACKUP_BYTES: u64 = 16 * 1024 * 1024;

fn read_no_follow(path: &Path) -> std::io::Result<(Vec<u8>, std::fs::Permissions)> {
    use std::io::Read as _;
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        // O_NONBLOCK as well as O_NOFOLLOW: opening a FIFO for reading
        // blocks until a writer appears, so a raced FIFO would hang the
        // commit here forever. With O_NONBLOCK the open returns and the
        // regular-file check below refuses it.
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = options.open(path)?;

    // Check the DESCRIPTOR, not the path — the thing already opened cannot
    // be swapped underneath this check. `std::fs::copy` refused a
    // non-regular source; reading by hand has to refuse it explicitly, or a
    // raced FIFO or device becomes an unbounded read.
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("refusing to read {} — not a regular file", path.display()),
        ));
    }
    if metadata.len() > MAX_BACKUP_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "refusing to read {} — {} bytes exceeds the {MAX_BACKUP_BYTES}-byte limit",
                path.display(),
                metadata.len()
            ),
        ));
    }

    // Bounded read even so: `len()` is a snapshot, and a writer can extend
    // the file between the check and the read. One byte over the limit is
    // enough to detect the overrun.
    let mut bytes = Vec::new();
    let mut limited = file.take(MAX_BACKUP_BYTES + 1);
    limited.read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_BACKUP_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "refusing to read {} — it grew past the {MAX_BACKUP_BYTES}-byte limit while being read",
                path.display()
            ),
        ));
    }
    Ok((bytes, metadata.permissions()))
}

/// Copy `src` to `dst` without following a symlink at EITHER end.
///
/// `std::fs::copy` follows both: a link at the source is read through to
/// its target, and a link at the destination is written through to its.
/// Both are TOCTOU against the containment pre-check, so each end takes a
/// syscall-level guard instead — `O_NOFOLLOW` on the read (unix only, see
/// [`read_no_follow`]) and O_EXCL on the create. The source's mode is
/// carried across, as `std::fs::copy` did, and is set on the destination
/// DESCRIPTOR so a link swapped in at `dst` afterwards cannot catch a
/// path-based `set_permissions`.
///
/// The files copied here are a lowered contract, sidecar, SQL model, and
/// manifest — kilobytes — so reading one whole is cheaper than streaming.
/// [`read_no_follow`] bounds that read rather than trusting the size.
///
/// The mode carried across is masked to `0o777`. `std::fs::copy` propagates
/// the source mode verbatim, which is safe when the destination is the
/// caller's own file — but here the source is attacker-influenceable and the
/// destination is created as Rocky. Racing in a `04755` source would
/// otherwise produce a ROCKY-OWNED SETUID backup, which a rollback then
/// renames over the final. Set-user-ID and set-group-ID are dropped;
/// nothing this module backs up is executable, let alone privileged.
///
/// A failed write leaves the partial `.ff-prev` in place, deliberately.
///
/// Deleting it would be the obvious cleanup, and it is the wrong call here:
/// `O_EXCL` grants exclusivity at open time and nothing after it, so ANY
/// path-based unlink can remove a file this copy never created if a racer
/// swapped the name — including one of the user's. An `(dev, ino)` check
/// before the unlink narrows that window but cannot close it, and doing
/// nothing has no deletion window at all. Between a stale backup and
/// deleting a bystander's file, the stale backup is the recoverable one.
///
/// The orphan is therefore handled where it is consumed, and that half has
/// landed: [`recover_generation`] restores only what the journal's
/// `has_prev` records, and refuses a backup it does not know about
/// (`commit-unexpected-backup`, #1502). So a partial `.ff-prev` left here
/// is inert rather than dangerous — this function's job is only to not
/// make it worse.
fn copy_no_follow(src: &Path, dst: &Path) -> std::io::Result<()> {
    let (bytes, permissions) = read_no_follow(src)?;
    let backup = create_new_no_follow(dst, Some(0o600))?;
    write_backup(backup, &bytes, &permissions)
}

/// Write the backup body and apply its mode, both on the DESCRIPTOR so a
/// link swapped in at the destination afterwards cannot catch a path-based
/// operation. Split out so [`copy_no_follow`] has one cleanup path.
fn write_backup(
    mut backup: std::fs::File,
    bytes: &[u8],
    permissions: &std::fs::Permissions,
) -> std::io::Result<()> {
    use std::io::Write as _;
    backup.write_all(bytes)?;
    #[cfg(unix)]
    let permissions = {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::Permissions::from_mode(permissions.mode() & 0o777)
    };
    #[cfg(not(unix))]
    let permissions = permissions.clone();
    backup.set_permissions(permissions)
}

/// Refuse a symlink sitting at any path the fresh commit is about to
/// WRITE THROUGH, before the first mutation.
///
/// The staging loop stages into `<final>.ff-staged`, backs finals up into
/// `<final>.ff-prev`, and journals into `<journal>.ff-staged`. Written the
/// plain way (`std::fs::write`, `std::fs::copy`) all three FOLLOW a symlink
/// at the destination — which is why they go through the no-follow helpers
/// above, and why this pre-check exists at all. A crash recovery path
/// already refuses symlinked residue, but that check lives past
/// [`recover_generation`]'s no-journal early return, so on the FRESH
/// commit path (the common case: no prior crash) nothing guarded these
/// writes. An attacker who can place a file in the models directory or the
/// state dir (a malicious spec repo, a lower-privilege process) could park
/// a symlink at `<contract>.ff-staged` pointing at `~/.ssh/authorized_keys`
/// and the commit would write engine bytes through it — an out-of-project
/// write. This closes that class on the fresh path.
///
/// Routes through the SHARED [`contained_target`] primitive so it refuses
/// a symlinked ANCESTOR directory (the static `models -> /outside` attack,
/// no race) as well as a symlink at the leaf — a leaf-only `is_symlink`
/// misses the ancestor case, the stronger blocker. The project root is
/// canonicalized once; each final is run through the primitive (ancestor
/// containment + final-symlink + final-dir refusal), and the `.ff-staged`
/// / `.ff-prev` residue — which shares the final's now-contained parent —
/// is additionally refused when its leaf is a symlink.
///
/// Stated residual, the conceded v0 boundary: a check-then-write is TOCTOU
/// against a DIRECTORY swapped for a symlink between validation and the
/// syscall, which only dirfd-relative APIs close. Every LEAF this pre-check
/// covers is guarded a second time at the syscall itself: the staged,
/// journal-temp and `.ff-prev` writes use O_EXCL ([`write_new_no_follow`]),
/// and the `.ff-prev` backup reads its source with `O_NOFOLLOW`
/// ([`copy_no_follow`]) — the read guard on unix only (see
/// [`commit_generation`]).
fn refuse_symlinked_write_targets<'a>(
    project_root: &Path,
    relpaths: impl IntoIterator<Item = &'a str>,
) -> SpecResult<()> {
    let resolved_root = project_root.canonicalize().map_err(|err| {
        SpecRejected::new(
            "commit-io",
            format!(
                "resolving project root {} failed: {err}",
                project_root.display()
            ),
        )
    })?;
    for relpath in relpaths {
        // Ancestor + leaf containment for the final (the shared primitive).
        contained_target(&resolved_root, relpath).map_err(|reason| {
            SpecRejected::new(
                "commit-symlinked-target",
                format!("refusing to commit: {reason}"),
            )
        })?;
        // The residue shares the final's (now-contained) parent — refuse a
        // symlink at each residue leaf too.
        let final_path = project_root.join(relpath);
        for probe in [staged_sibling(&final_path), prev_sibling(&final_path)] {
            if is_symlink(&probe) {
                return Err(SpecRejected::new(
                    "commit-symlinked-target",
                    format!(
                        "refusing to commit: a staging residue for '{relpath}' is a symlink — \
                         a link at a staging path would redirect the write out of the project"
                    ),
                ));
            }
        }
    }
    Ok(())
}

/// The staged sibling of a final path (same directory, fixed suffix).
fn staged_sibling(final_path: &Path) -> PathBuf {
    sibling_with_suffix(final_path, STAGED_SUFFIX)
}

/// The backup sibling of a final path.
fn prev_sibling(final_path: &Path) -> PathBuf {
    sibling_with_suffix(final_path, PREV_SUFFIX)
}

fn sibling_with_suffix(final_path: &Path, suffix: &str) -> PathBuf {
    let mut name = final_path
        .file_name()
        .map(std::ffi::OsStr::to_os_string)
        .unwrap_or_default();
    name.push(suffix);
    final_path.with_file_name(name)
}

// ---------------------------------------------------------------------------
// Commit: stage, journal, rename (manifest LAST), clean up
// ---------------------------------------------------------------------------

/// The two filesystem mutations the crash drills interpose on. Production
/// code passes the real operations; a test injects a failure at the N-th
/// rename (or at a cleanup removal) to freeze the protocol mid-flight —
/// the same seam the answer key's `os.replace` monkeypatch exercised.
struct CommitOps<'a> {
    rename: &'a mut dyn FnMut(&Path, &Path) -> std::io::Result<()>,
    remove: &'a mut dyn FnMut(&Path) -> std::io::Result<()>,
}

fn io_reject(action: &str, path: &Path, err: &std::io::Error) -> SpecRejected {
    SpecRejected::new(
        "commit-io",
        format!("{action} {} failed: {err}", path.display()),
    )
}

/// Commit a lowering generation with the manifest rename as the marker.
///
/// Protocol: stage every file as a same-directory `.ff-staged` tmp; back
/// up every file about to be replaced as `.ff-prev`; journal the staging
/// (atomic tmp+rename); rename staged → final for the artifacts, then
/// the manifest LAST; clean up. A crash anywhere before the manifest
/// rename leaves a journal whose manifest is uncommitted —
/// [`recover_generation`] rolls the whole generation back.
///
/// A prior generation may have crashed mid-commit; it is recovered first
/// so this run stages on a consistent tree.
///
/// # Symlink containment
///
/// Every write target is proven inside the project root through the shared
/// [`contained_target`] primitive BEFORE the first mutation, refusing a
/// symlinked ancestor directory (the static `models -> /outside` escape, no
/// race) as well as a symlink at the leaf. Each leaf is then guarded a
/// second time at the syscall, so a link swapped in AFTER the pre-check is
/// refused rather than followed: the staged, journal-temp and `.ff-prev`
/// writes use O_EXCL, and the `.ff-prev` backup reads its source with
/// `O_NOFOLLOW` ([`copy_no_follow`]). The backup copy has TWO symlink-follow
/// ends — the SOURCE read and the DESTINATION write — and both are covered.
///
/// Two residuals remain, the conceded v0 boundary. A DIRECTORY swapped for
/// a symlink between validation and a rename/unlink is closed only by
/// dirfd-relative APIs, which v0 does not use. And `O_NOFOLLOW` is unix
/// only: on Windows the backup read still follows a symlink or junction
/// planted at the final, so containment there rests on the pre-check alone
/// — and that platform is untested, because every symlink exploit test in
/// this module is `#[cfg(unix)]`.
///
/// # Errors
///
/// Propagates any recovery refusal (the journal is validated before
/// anything mutates), a `commit-symlinked-target` refusal for a symlinked
/// ancestor or leaf, and any I/O failure as `commit-io`.
pub fn commit_generation(
    project_root: &Path,
    parsed: &ParsedSpec,
    lowering: &Lowering,
) -> SpecResult<()> {
    commit_generation_with_ops(
        project_root,
        parsed,
        lowering,
        &mut CommitOps {
            rename: &mut |src, dst| std::fs::rename(src, dst),
            remove: &mut |p| std::fs::remove_file(p),
        },
    )
}

fn commit_generation_with_ops(
    project_root: &Path,
    parsed: &ParsedSpec,
    lowering: &Lowering,
    ops: &mut CommitOps<'_>,
) -> SpecResult<()> {
    let product_name = &parsed.product().name;
    recover_generation(project_root, parsed)?;

    let manifest_relpath = manifest_rel(product_name);
    let mut contents: Vec<(String, Vec<u8>)> = lowering
        .artifacts
        .iter()
        .map(|artifact| (artifact.relpath.clone(), artifact.content.clone()))
        .collect();
    contents.push((manifest_relpath.clone(), lowering.manifest.to_json_bytes()));

    // Refuse a symlink at any write target BEFORE the first mutation: the
    // fresh commit path stages/backs-up/journals with write+copy, all of
    // which follow a link at the destination. `recover_generation` above
    // guards the recovery path's residue, but returns early with no
    // journal, so this is the fresh path's only guard. The journal and its
    // `.ff-staged` tmp are covered alongside every artifact's siblings.
    let journal_relpath = format!("{}/{STAGING_JOURNAL}", state_dir_rel(product_name));
    refuse_symlinked_write_targets(
        project_root,
        contents
            .iter()
            .map(|(relpath, _)| relpath.as_str())
            .chain(std::iter::once(journal_relpath.as_str())),
    )?;

    // 1. Stage every file (same dir, fixed suffix) and back up finals.
    let mut entries: Vec<StagingEntry> = Vec::with_capacity(contents.len());
    for (relpath, bytes) in &contents {
        let final_path = project_root.join(relpath);
        if let Some(parent) = final_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|err| io_reject("creating directory", parent, &err))?;
        }
        let staged = staged_sibling(&final_path);
        // O_EXCL (via `write_new_no_follow`): a link swapped in at the
        // staged leaf after the pre-check is refused, never followed.
        write_new_no_follow(&staged, bytes).map_err(|err| io_reject("staging", &staged, &err))?;
        let has_prev = final_path.exists();
        if has_prev {
            let prev = prev_sibling(&final_path);
            // Neither end of this copy follows a link swapped in after the
            // pre-check: `O_NOFOLLOW` on the source read (unix) and O_EXCL
            // on the backup create. The failure is named against the SOURCE
            // — the racy end — not the destination it writes.
            copy_no_follow(&final_path, &prev)
                .map_err(|err| io_reject("backing up", &final_path, &err))?;
        }
        entries.push(StagingEntry {
            final_path: relpath.clone(),
            staged_sha: content_digest(bytes),
            has_prev,
        });
    }

    // 2. Journal the staging (atomic write) — from here recovery is defined.
    let record = StagingJournal {
        entries: entries.clone(),
        manifest: manifest_relpath,
    };
    let journal = journal_path(project_root, product_name);
    if let Some(parent) = journal.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|err| io_reject("creating directory", parent, &err))?;
    }
    let journal_tmp = sibling_with_suffix(&journal, STAGED_SUFFIX);
    let journal_bytes =
        serde_json::to_vec_pretty(&record).expect("the staging journal serializes to JSON");
    write_new_no_follow(&journal_tmp, &journal_bytes)
        .map_err(|err| io_reject("writing", &journal_tmp, &err))?;
    (ops.rename)(&journal_tmp, &journal).map_err(|err| io_reject("renaming", &journal, &err))?;

    // 3. Rename staged → final: artifacts first, the manifest LAST.
    for entry in &entries {
        let final_path = project_root.join(&entry.final_path);
        (ops.rename)(&staged_sibling(&final_path), &final_path)
            .map_err(|err| io_reject("renaming", &final_path, &err))?;
    }

    // 4. Committed — drop the backups and the journal.
    for entry in &entries {
        let prev = prev_sibling(&project_root.join(&entry.final_path));
        if prev.exists() {
            (ops.remove)(&prev).map_err(|err| io_reject("removing", &prev, &err))?;
        }
    }
    (ops.remove)(&journal).map_err(|err| io_reject("removing", &journal, &err))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Recovery: resolve a crashed commit, refusing anything forged
// ---------------------------------------------------------------------------

/// Resolve a crashed commit, if any. Idempotent for genuine crashes.
///
/// The manifest is the commit marker: the generation is committed iff the
/// manifest file exists with exactly the journaled staged content hash.
/// (A plain existence check is not enough — a re-lowering stages over the
/// PREVIOUS generation's committed manifest, which also "exists".)
/// Uncommitted → restore every `.ff-prev` backup, remove any renamed new
/// file and every staged tmp. Committed → drop the leftovers.
///
/// The journal is UNTRUSTED input, never filesystem authority. Before any
/// mutation, the whole record is validated and every check failure is a
/// refusal that touches nothing:
///
/// - it parses under the strict [`StagingJournal`] schema;
/// - it names this product's own manifest path, exactly once, and no
///   final twice — compared casefolded, because on a case-insensitive
///   filesystem two case-aliased spellings are one file and duplicate
///   entries could direct conflicting mutations at it;
/// - no entry names a staging journal or any manifest path other than
///   this generation's own commit marker — those files ARE the recovery
///   machinery and are never legitimate staging targets;
/// - every path is relative, canonical, and directory-contained under the
///   project root, whose real path is resolved exactly ONCE
///   ([`contained_final_path`]); final components — and the staged/prev
///   residue the mutation loop touches — are refused when they are
///   symlinks (the lowering writes only regular files, so a link at an
///   artifact path is tampering); every mutation below operates on the
///   validated resolved absolutes and nothing is re-resolved;
/// - every entry is constrained to the artifact namespace of the CURRENT
///   generation, derived from the caller-supplied `parsed` spec — the
///   approved snapshot, the only authenticated identity source. The
///   lowering of this spec can only ever stage the contract, the sidecar,
///   and the manifest, so those three paths are the whole set. No on-disk
///   manifest grants anything: staged and previous manifests are exactly
///   as forgeable as the journal itself, and a committed manifest whose
///   identity fields merely match is still attacker-writable content —
///   matching fields authenticate nothing.
///
/// A rare corollary of failing closed: a recovery that itself died
/// half-way can leave a journal whose staged manifest is already gone —
/// that state refuses automatic handling and asks for a human, it is
/// never guessed through.
///
/// # Errors
///
/// Returns `staging-journal-corrupt`, `staging-journal-forbidden-entry`,
/// `staging-journal-unsafe-path`, or `staging-journal-foreign-entry` per
/// the rules above, plus `commit-io` for a filesystem failure during the
/// mutation phase.
pub fn recover_generation(project_root: &Path, parsed: &ParsedSpec) -> SpecResult<RecoveryAction> {
    let product_name = &parsed.product().name;
    let journal = journal_path(project_root, product_name);
    if !journal.is_file() {
        return Ok(RecoveryAction::None);
    }
    let raw = std::fs::read(&journal).map_err(|err| io_reject("reading", &journal, &err))?;
    let record: StagingJournal = serde_json::from_slice(&raw).map_err(|err| {
        journal_reject(
            &journal,
            "staging-journal-corrupt",
            &format!("does not parse as a staging journal ({err})"),
        )
    })?;

    let manifest_relpath = manifest_rel(product_name);
    if record.manifest != manifest_relpath {
        return Err(journal_reject(
            &journal,
            "staging-journal-corrupt",
            &format!(
                "names manifest '{}' instead of this product's '{manifest_relpath}'",
                record.manifest
            ),
        ));
    }
    let finals: Vec<&str> = record
        .entries
        .iter()
        .map(|entry| entry.final_path.as_str())
        .collect();
    let folded: std::collections::BTreeSet<String> =
        finals.iter().map(|f| f.to_lowercase()).collect();
    if folded.len() != finals.len() {
        return Err(journal_reject(
            &journal,
            "staging-journal-corrupt",
            "lists a final path twice (case-insensitive aliases collide on case-insensitive \
             filesystems)",
        ));
    }
    let manifest_entries: Vec<&StagingEntry> = record
        .entries
        .iter()
        .filter(|entry| entry.final_path == record.manifest)
        .collect();
    let [manifest_entry] = manifest_entries.as_slice() else {
        return Err(journal_reject(
            &journal,
            "staging-journal-corrupt",
            "does not name its manifest entry exactly once",
        ));
    };
    let manifest_entry: StagingEntry = (*manifest_entry).clone();

    // The recovery machinery itself is never a staging target: an entry
    // naming a staging journal, or a manifest path other than this
    // generation's own commit marker, is a forgery however it resolves.
    for final_rel in &finals {
        let basename = final_rel
            .rsplit('/')
            .next()
            .unwrap_or(final_rel)
            .to_lowercase();
        if basename == STAGING_JOURNAL.to_lowercase() {
            return Err(journal_reject(
                &journal,
                "staging-journal-forbidden-entry",
                &format!(
                    "lists '{final_rel}', which names a staging journal — recovery never \
                     stages its own machinery"
                ),
            ));
        }
        if basename == MANIFEST_FILENAME.to_lowercase() && *final_rel != manifest_relpath {
            return Err(journal_reject(
                &journal,
                "staging-journal-forbidden-entry",
                &format!(
                    "lists '{final_rel}', which names a manifest other than this generation's \
                     own commit marker '{manifest_relpath}'"
                ),
            ));
        }
    }

    // Path containment for EVERY entry, before anything is touched. The
    // project root is resolved ONCE; the resolved absolutes that come back
    // are the only paths the mutation loop below is allowed to touch.
    let resolved_root = project_root
        .canonicalize()
        .map_err(|err| io_reject("resolving project root", project_root, &err))?;
    let mut resolved_finals: Vec<(StagingEntry, PathBuf)> =
        Vec::with_capacity(record.entries.len());
    for entry in &record.entries {
        let resolved = contained_final_path(&resolved_root, &journal, &entry.final_path)?;
        resolved_finals.push((entry.clone(), resolved));
    }

    // The staged/prev siblings the mutation loop will touch are validated
    // here too, BEFORE anything mutates: a symlink parked at
    // `<final>.ff-staged` or `<final>.ff-prev` (dangling or not) would
    // otherwise be renamed into place or restored over a final — recovery
    // refuses symlinked residue.
    for (entry, final_path) in &resolved_finals {
        for probe in [staged_sibling(final_path), prev_sibling(final_path)] {
            if is_symlink(&probe) {
                let name = probe
                    .file_name()
                    .map(|n| n.to_string_lossy().into_owned())
                    .unwrap_or_default();
                return Err(journal_reject(
                    &journal,
                    "staging-journal-unsafe-path",
                    &format!(
                        "'{name}' (residue of '{}') is a symlink — recovery refuses symlinked \
                         staging residue",
                        entry.final_path
                    ),
                ));
            }
        }
    }

    // Constrain mutations to the CURRENT generation's artifact namespace,
    // derived from the approved spec snapshot the caller supplied — never
    // from any on-disk manifest (staged, previous, or committed), all of
    // which are attacker-writable exactly like the journal.
    let permissible = [
        manifest_relpath.clone(),
        contract_rel(parsed),
        sidecar_rel(parsed),
    ];
    let mut foreign: Vec<&str> = finals
        .iter()
        .filter(|f| !permissible.iter().any(|p| p == *f))
        .copied()
        .collect();
    foreign.sort_unstable();
    if !foreign.is_empty() {
        let listed: Vec<String> = foreign.iter().map(|f| format!("'{f}'")).collect();
        return Err(journal_reject(
            &journal,
            "staging-journal-foreign-entry",
            &format!(
                "lists {} which the current generation of product '{product_name}' (spec {}) \
                 can never stage — its lowering emits only the contract, the sidecar, and the \
                 manifest",
                listed.join(", "),
                parsed.digest
            ),
        ));
    }

    let manifest_final = resolved_finals
        .iter()
        .find(|(entry, _)| entry.final_path == manifest_entry.final_path)
        .map(|(_, path)| path.clone())
        .expect("the manifest entry was proven present above");

    let committed = manifest_final.is_file()
        && std::fs::read(&manifest_final)
            .map(|bytes| content_digest(&bytes) == manifest_entry.staged_sha)
            .unwrap_or(false);

    for (entry, final_path) in &resolved_finals {
        let staged = staged_sibling(final_path);
        let prev = prev_sibling(final_path);
        if committed {
            if staged.exists() {
                std::fs::remove_file(&staged)
                    .map_err(|err| io_reject("removing", &staged, &err))?;
            }
            if prev.exists() {
                std::fs::remove_file(&prev).map_err(|err| io_reject("removing", &prev, &err))?;
            }
            continue;
        }
        // The JOURNAL decides whether a backup should exist — not the
        // filesystem. This record is documented as "filesystem-mutation
        // authority" at recovery time, and restoring on `prev.exists()`
        // alone contradicted that: a partial `.ff-prev` left by an earlier
        // failed attempt would be renamed over the final, undoing a commit
        // the journal never recorded a backup for (#1502).
        if entry.has_prev && prev.exists() {
            std::fs::rename(&prev, final_path)
                .map_err(|err| io_reject("restoring", final_path, &err))?;
        } else if !entry.has_prev && prev.exists() {
            // A backup the journal does not know about. Refusing is the
            // conservative half of the trade this module already made: the
            // producer deliberately leaves a failed backup in place rather
            // than unlink under a race, so recovery must not treat one as
            // trustworthy content. Nothing is deleted here either — the
            // operator is told what to look at.
            return Err(SpecRejected::new(
                "commit-unexpected-backup",
                format!(
                    "{} exists but the staging journal records no backup for {}. \
                     Recovery will not restore a backup it did not create. \
                     Inspect that file and remove it once you have confirmed \
                     it is not needed, then re-run.",
                    prev.display(),
                    final_path.display()
                ),
            ));
        } else if !entry.has_prev
            && final_path.is_file()
            && std::fs::read(final_path)
                .map(|bytes| content_digest(&bytes) == entry.staged_sha)
                .unwrap_or(false)
        {
            // A brand-new file that already renamed into place: uncommitted
            // generation content with nothing underneath — remove it.
            std::fs::remove_file(final_path)
                .map_err(|err| io_reject("removing", final_path, &err))?;
        }
        if staged.exists() {
            std::fs::remove_file(&staged).map_err(|err| io_reject("removing", &staged, &err))?;
        }
    }
    std::fs::remove_file(&journal).map_err(|err| io_reject("removing", &journal, &err))?;
    Ok(if committed {
        RecoveryAction::RolledForward
    } else {
        RecoveryAction::RolledBack
    })
}

// ---------------------------------------------------------------------------
// Orchestration: run a phase against a project directory
// ---------------------------------------------------------------------------

/// The committed lowering manifest, or `None` when this product has none.
///
/// # Errors
///
/// Returns `manifest-unreadable` when a file exists but does not parse.
pub fn committed_manifest(project_root: &Path, product_name: &str) -> SpecResult<Option<Manifest>> {
    let manifest_path = project_root.join(manifest_rel(product_name));
    if !manifest_path.is_file() {
        return Ok(None);
    }
    let raw =
        std::fs::read(&manifest_path).map_err(|err| io_reject("reading", &manifest_path, &err))?;
    Manifest::from_json_bytes(&raw).map(Some)
}

/// Verify draft-path collisions, lower Phase A, and commit it.
///
/// Cold-start rule: refuse when `models/<model>.sql` or
/// `models/<model>.toml` already exists, unless this product's committed
/// manifest already claims the model (resuming).
///
/// # Errors
///
/// Returns `model-collision` on the cold-start rule, plus anything
/// [`recover_generation`], [`lower_phase_a`], or [`commit_generation`]
/// refuses.
pub fn run_phase_a(
    project_root: &Path,
    spec_path: &str,
    parsed: &ParsedSpec,
) -> SpecResult<Lowering> {
    recover_generation(project_root, parsed)?;
    let committed = committed_manifest(project_root, &parsed.product().name)?;
    let resuming = committed.as_ref().is_some_and(|manifest| {
        manifest.product_id == parsed.product_id() && manifest.output_model == parsed.output_model()
    });
    if !resuming {
        for rel in [sql_rel(parsed), sidecar_rel(parsed)] {
            if project_root.join(&rel).exists() {
                return Err(SpecRejected::new(
                    "model-collision",
                    format!(
                        "{rel} already exists and this product has no committed lowering \
                         claiming it — refusing the cold start (rename output.model or remove \
                         the stray file)"
                    ),
                ));
            }
        }
    }
    let lowering = lower_phase_a(parsed, spec_path)?;
    commit_generation(project_root, parsed, &lowering)?;
    Ok(lowering)
}

/// Byte-verify Phase A, merge the drafted sidecar, and commit Phase B.
///
/// Generation identity binding is enforced by [`lower_phase_b`] itself —
/// this orchestrator hands it the committed manifest OBJECT and the pure
/// boundary refuses a foreign identity or a superseded digest (the spec
/// moved after Phase A) rather than committing a manifest that mixes
/// spec B's metadata with spec A's contract. The verified Phase-A hashes
/// are carried forward from that object into the merged manifest.
///
/// # Errors
///
/// Returns `phase-a-missing` when no committed manifest exists,
/// `phase-a-tampered` when a lowered artifact's bytes drifted from the
/// committed manifest, `sidecar-missing` before drafting, plus anything
/// [`lower_phase_b`] or [`commit_generation`] refuses.
pub fn run_phase_b(
    project_root: &Path,
    spec_path: &str,
    parsed: &ParsedSpec,
) -> SpecResult<Lowering> {
    run_phase_b_with_ops(
        project_root,
        spec_path,
        parsed,
        &mut CommitOps {
            rename: &mut |src, dst| std::fs::rename(src, dst),
            remove: &mut |p| std::fs::remove_file(p),
        },
    )
}

fn run_phase_b_with_ops(
    project_root: &Path,
    spec_path: &str,
    parsed: &ParsedSpec,
    ops: &mut CommitOps<'_>,
) -> SpecResult<Lowering> {
    recover_generation(project_root, parsed)?;
    let Some(committed) = committed_manifest(project_root, &parsed.product().name)? else {
        return Err(SpecRejected::new(
            "phase-a-missing",
            "no committed lowering manifest found — run Phase A before the metadata merge",
        ));
    };
    let problems = verify_artifact_hashes(project_root, &committed);
    if !problems.is_empty() {
        return Err(SpecRejected::new(
            "phase-a-tampered",
            format!(
                "lowered artifact bytes no longer match the committed manifest: {}",
                problems.join("; ")
            ),
        ));
    }
    let sidecar_path = project_root.join(sidecar_rel(parsed));
    if !sidecar_path.is_file() {
        return Err(SpecRejected::new(
            "sidecar-missing",
            format!(
                "{} does not exist — Phase B merges metadata into the drafted sidecar and \
                 cannot run before drafting",
                sidecar_rel(parsed)
            ),
        ));
    }
    // NO-FOLLOW. A plain read follows a symlink, and Phase B's merge preserves
    // every key it does not own (`lowering.rs`) — so a process that can write in
    // the models directory could point the sidecar at any Rocky-readable TOML,
    // let this read pull it in, and swap a regular file back before the commit
    // pre-check. Whatever was read would be merged into the project sidecar and
    // committed as project content (#1501). A hardlink needs no timing at all.
    //
    // `read_no_follow` checks the DESCRIPTOR's metadata rather than the path, so
    // what it validated is what it read. The commit below works from `lowering`,
    // built out of these bytes — it never re-resolves the path for content, so
    // there is no second read to race.
    let (sidecar_bytes, _perms) =
        read_no_follow(&sidecar_path).map_err(|err| io_reject("reading", &sidecar_path, &err))?;
    let sidecar_text = String::from_utf8(sidecar_bytes).map_err(|err| {
        io_reject(
            "reading",
            &sidecar_path,
            &std::io::Error::new(std::io::ErrorKind::InvalidData, err),
        )
    })?;
    let lowering = lower_phase_b(parsed, spec_path, &sidecar_text, &committed)?;
    commit_generation_with_ops(project_root, parsed, &lowering, ops)?;
    Ok(lowering)
}

/// (Re)open the drafting window over a committed MERGED generation, on
/// the evidence that the fulfillment loop DECIDED this drafting round
/// (#1493).
///
/// # Why this needs evidence
///
/// The demotion below is an authority transition: it takes a generation
/// whose sidecar bytes are pinned by a committed manifest and returns
/// that sidecar to the writable drafting namespace. Reached without the
/// loop, it would open the Phase-A window on a legitimate merged
/// generation with no round to fill it — so the raw transition is
/// [`demote_merged_manifest_to_phase_a`], which is `pub(crate)` and
/// unreachable from any other crate (pinned by the out-of-crate
/// compile-fail proof in `rocky-core-compiletest`). This is the only
/// public way in, and it reads the decision out of `store` rather than
/// taking it as an argument. Every condition below is checked against
/// that stored record:
///
/// - the record is THIS product's (`product_id` matches the spec), so a
///   record fetched for another product grants nothing here;
/// - the machine is at [`FulfillState::Drafting`] — the one state in
///   which a drafting or repair worker is dispatched. Every other state
///   (`Merged`, `Verifying`, `Applied`, …) has no round to open a window
///   for;
/// - the owner stamp names THIS process. Stamping it means winning the
///   record's CAS, which is what "the loop decided" reduces to on disk.
///
/// # Exactly what this gate is worth
///
/// It prevents a demotion the loop did not decide: an accidental one, or
/// one from another code path in this binary that reaches for the
/// transition without a round behind it. That is the failure it exists
/// for, and it holds — the conditions are answered by persisted state,
/// not by anything the caller says about itself.
///
/// It is NOT a defense against a deliberate in-process caller. `store`
/// is a caller-chosen [`StateStore`], and opening one at an arbitrary
/// path and writing a record into it are both public operations, so code
/// running inside this process can construct a store that satisfies
/// every condition. Nothing in-process can prevent that: such a caller
/// already holds every capability the process holds, including simply
/// rewriting the files this function would have written. It is outside
/// the boundary, not a hole in it.
///
/// The owner check pairs the pid with the process start time
/// ([`crate::process::stamp_is_this_process`]), so a dead owner's
/// recycled pid is not mistaken for ours, and an unconfirmable stamp
/// fails closed.
///
/// # Errors
///
/// Returns `reopen-undecided` when the record does not carry the loop's
/// decision, plus everything
/// [`demote_merged_manifest_to_phase_a`] refuses.
pub fn reopen_for_drafting(
    project_root: &Path,
    spec_path: &str,
    parsed: &ParsedSpec,
    store: &StateStore,
) -> SpecResult<ReopenOutcome> {
    let product_id = parsed.product_id();
    let undecided = |why: String| {
        SpecRejected::new(
            "reopen-undecided",
            format!(
                "refusing to reopen the drafting window for {product_id}: {why}. The drafting \
                 window is opened by the fulfillment loop's own decided round — run \
                 `rocky fulfill {}` instead of demoting a committed generation by hand",
                parsed.product().name
            ),
        )
    };
    // The evidence is READ HERE, from the state store, and never
    // accepted as an argument. A `FulfillStateRecord` is publicly
    // constructible, so taking one from the caller would let any caller
    // mint its own permission slip; taking the store makes the caller
    // prove the claim against the record that actually won the CAS.
    let decided = store
        .fulfill_state_get(&parsed.product().name)
        .map_err(|err| {
            SpecRejected::new(
                "reopen-state-unreadable",
                format!("could not read the fulfillment record for {product_id}: {err}"),
            )
        })?;
    let Some(decided) = decided else {
        return Err(undecided(
            "no fulfillment record exists, so nothing decided a drafting round".to_string(),
        ));
    };
    if decided.product_id != product_id {
        return Err(undecided(format!(
            "the stored fulfillment record belongs to {} ",
            decided.product_id
        )));
    }
    if decided.state != FulfillState::Drafting {
        return Err(undecided(format!(
            "the loop is at '{}', not 'drafting' — no drafting round is open",
            decided.state.tag()
        )));
    }
    if !crate::process::stamp_is_this_process(decided.owner_pid, decided.owner_start_time) {
        let me = std::process::id();
        // Say which of the two cases this is. "owned by pid 43917, not
        // this process (pid 43917)" reads as a contradiction and tells
        // an operator nothing.
        let why = match decided.owner_pid {
            None => "no process owns the record — nothing decided a drafting round".to_string(),
            Some(pid) if pid == me => format!(
                "the record's owner stamp carries this pid ({pid}) but a different process \
                 start time, so it was left by an earlier process that has since died and \
                 whose pid the system reused — not by this one. Re-run `rocky fulfill` to \
                 take the stale record over"
            ),
            Some(pid) => format!(
                "the record is owned by another process (pid {pid}), not this one (pid {me})"
            ),
        };
        return Err(undecided(why));
    }
    demote_merged_manifest_to_phase_a(project_root, spec_path, parsed)
}

/// The raw demotion of a committed MERGED generation back to Phase A —
/// the loop's authorized transition before it dispatches a repair (or a
/// resumed drafting) worker (#1493).
///
/// `pub(crate)`: reaching this without the loop's decided round would
/// open the Phase-A window on a legitimate merged generation. The one
/// public route in is [`reopen_for_drafting`], which demands the
/// compare-and-swapped record first.
///
/// A merged manifest pins the sidecar's exact bytes, which is right for
/// every window where no write is authorized — but a repair round the
/// loop itself dispatched rewrites the sidecar legitimately, and
/// verifying it against the previous round's hash would mis-classify
/// the loop's own work as tamper. This is the E12 authority transition
/// that resolves it, with the prepare / commit / recovery protocol
/// stated:
///
/// - **prepare** — byte-verify EVERY artifact hash in the committed
///   merged manifest first. Any drift happened while no write was
///   authorized and is reported as [`ReopenOutcome::Tampered`], nothing
///   mutated — the reopen never blesses dirty state into a fresh
///   window.
/// - **commit** — re-lower Phase A from the caller's spec (byte-stable:
///   the contract render is deterministic and the verified on-disk
///   contract already matches it) and commit it through
///   [`commit_generation`], manifest-renamed-last. The sidecar leaves
///   the manifest's artifact set, returning it to the drafting
///   namespace exactly as in round 1; the very next Phase B re-records
///   its hash transactionally from what it merges.
/// - **recovery** — [`recover_generation`] (run first here and by the
///   commit) rolls a crashed reopen back to the merged manifest, and
///   the next drafting dispatch simply reopens again.
///
/// Only the staged commit protocol ever updates hashes; out-of-band
/// edits between gates stay detected because every other verification
/// surface is unchanged.
///
/// # Errors
///
/// Returns `spec-superseded` when the merged manifest was generated
/// from a different spec digest (the supersession fence should have
/// re-entered the loop before any reopen), and
/// `reopen-foreign-generation` when its identity fields name another
/// product, model, or spec path — a foreign generation is never
/// demoted. Plus anything [`recover_generation`], [`lower_phase_a`], or
/// [`commit_generation`] refuses.
pub(crate) fn demote_merged_manifest_to_phase_a(
    project_root: &Path,
    spec_path: &str,
    parsed: &ParsedSpec,
) -> SpecResult<ReopenOutcome> {
    recover_generation(project_root, parsed)?;
    let Some(committed) = committed_manifest(project_root, &parsed.product().name)? else {
        return Ok(ReopenOutcome::NotNeeded);
    };
    if committed.phase == ManifestPhase::LoweredContract {
        return Ok(ReopenOutcome::NotNeeded);
    }
    let output_model = parsed.output_model().to_string();
    let product_id = parsed.product_id();
    let mismatches: Vec<String> = [
        ("product_id", &committed.product_id, &product_id),
        ("output_model", &committed.output_model, &output_model),
        ("spec_path", &committed.spec_path, &spec_path.to_string()),
    ]
    .into_iter()
    .filter(|(_, recorded, current)| recorded != current)
    .map(|(name, recorded, current)| {
        format!("{name}: manifest {recorded:?} vs current {current:?}")
    })
    .collect();
    if !mismatches.is_empty() {
        return Err(SpecRejected::new(
            "reopen-foreign-generation",
            format!(
                "the committed merged manifest belongs to a different generation identity \
                 and is never demoted: {}",
                mismatches.join("; ")
            ),
        ));
    }
    if committed.spec_digest != parsed.digest {
        return Err(SpecRejected::new(
            "spec-superseded",
            format!(
                "the committed merged manifest was generated from spec {}, but the current \
                 spec digests to {} — re-approve to restart the generation instead of \
                 reopening a superseded one",
                committed.spec_digest, parsed.digest
            ),
        ));
    }
    let problems = verify_artifact_hashes(project_root, &committed);
    if !problems.is_empty() {
        return Ok(ReopenOutcome::Tampered(problems));
    }
    let lowering = lower_phase_a(parsed, spec_path)?;
    commit_generation(project_root, parsed, &lowering)?;
    Ok(ReopenOutcome::Reopened)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    // Only the reopen's evidence tests build a record; production code
    // reads one out of the store and never names the type.
    use crate::fulfill::FulfillStateRecord;
    use crate::product::manifest::{ManifestPhase, assert_total};
    use crate::product::spec::parse_spec_bytes;

    const SPEC_PATH: &str = "products/revenue_daily.toml";
    const SPEC_FIXTURE: &[u8] = include_bytes!("testdata/revenue_daily.spec.toml");

    /// A sidecar exactly as the worker's drafting tool leaves it, with one
    /// worker-appended check — the same fixture the lowering tests use.
    const DRAFTED_SIDECAR: &str = concat!(
        "# Draft authored via the Rocky MCP `draft_model` tool. Target and strategy resolve\n",
        "# from the project's conventions (rocky.toml pipeline + _defaults.toml).\n",
        "name = \"revenue_daily\"\n",
        "intent = \"Daily gross revenue per client in EUR, refunds excluded\"\n",
        "\n",
        "[[tests]]\n",
        "type = \"expression\"\n",
        "expression = \"client_id > 0\"\n",
    );

    fn parsed_d3() -> ParsedSpec {
        parse_spec_bytes(SPEC_FIXTURE, SPEC_PATH).expect("the fixture parses")
    }

    fn seeded_project(root: &Path) -> PathBuf {
        let project = root.join("project");
        std::fs::create_dir_all(project.join("models")).expect("mkdir");
        project
    }

    fn write_file(path: &Path, bytes: &[u8]) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("mkdir");
        }
        std::fs::write(path, bytes).expect("write");
    }

    fn manifest_path(project: &Path) -> PathBuf {
        project.join(".rocky/fulfillment/revenue_daily/lowering-manifest.json")
    }

    fn committed(project: &Path) -> Manifest {
        Manifest::from_json_bytes(&std::fs::read(manifest_path(project)).expect("manifest"))
            .expect("parses")
    }

    /// Every regular file under `project`, keyed by its relative path.
    fn snapshot(project: &Path) -> BTreeMap<String, Vec<u8>> {
        fn walk(dir: &Path, root: &Path, out: &mut BTreeMap<String, Vec<u8>>) {
            let Ok(entries) = std::fs::read_dir(dir) else {
                return;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() && !is_symlink(&path) {
                    walk(&path, root, out);
                } else if path.is_file() {
                    let rel = path
                        .strip_prefix(root)
                        .expect("under root")
                        .to_string_lossy()
                        .replace('\\', "/");
                    out.insert(rel, std::fs::read(&path).expect("read"));
                }
            }
        }
        let mut out = BTreeMap::new();
        walk(project, project, &mut out);
        out
    }

    fn leftovers(project: &Path) -> Vec<String> {
        snapshot(project)
            .keys()
            .filter(|name| name.ends_with(STAGED_SUFFIX) || name.ends_with(PREV_SUFFIX))
            .cloned()
            .collect()
    }

    fn full_flow(project: &Path, parsed: &ParsedSpec) {
        run_phase_a(project, SPEC_PATH, parsed).expect("phase A");
        write_file(&project.join("models/revenue_daily.sql"), b"SELECT 1\n");
        write_file(
            &project.join("models/revenue_daily.toml"),
            DRAFTED_SIDECAR.as_bytes(),
        );
        run_phase_b(project, SPEC_PATH, parsed).expect("phase B");
    }

    fn project_with_phase_a_and_draft(root: &Path, parsed: &ParsedSpec) -> PathBuf {
        let project = seeded_project(root);
        run_phase_a(&project, SPEC_PATH, parsed).expect("phase A");
        write_file(&project.join("models/revenue_daily.sql"), b"SELECT 1\n");
        write_file(
            &project.join("models/revenue_daily.toml"),
            DRAFTED_SIDECAR.as_bytes(),
        );
        project
    }

    /// Fail the N-th rename with an injected error, mirroring the answer
    /// key's `os.replace` bomb. Phase B commits 3 renames: the journal
    /// swap (1), the sidecar rename (2), the manifest rename (3 — the
    /// commit marker).
    fn phase_b_with_rename_bomb(
        project: &Path,
        parsed: &ParsedSpec,
        fail_on_call: usize,
    ) -> SpecRejected {
        let mut calls = 0usize;
        let mut rename = |src: &Path, dst: &Path| {
            calls += 1;
            if calls == fail_on_call {
                return Err(std::io::Error::other(
                    "injected crash between staged renames",
                ));
            }
            std::fs::rename(src, dst)
        };
        let mut remove = |p: &Path| std::fs::remove_file(p);
        let error = run_phase_b_with_ops(
            project,
            SPEC_PATH,
            parsed,
            &mut CommitOps {
                rename: &mut rename,
                remove: &mut remove,
            },
        )
        .expect_err("the injected crash must surface");
        assert!(
            error.message.contains("injected crash"),
            "unexpected failure: {error}"
        );
        error
    }

    // ------------------------ two-phase orchestration ------------------------

    #[test]
    fn phase_a_refuses_cold_start_over_existing_model_files() {
        let dir = tempfile::tempdir().expect("tempdir");
        let project = seeded_project(dir.path());
        write_file(&project.join("models/revenue_daily.sql"), b"SELECT 1\n");
        let error = run_phase_a(&project, SPEC_PATH, &parsed_d3()).expect_err("collision");
        assert_eq!(error.code, "model-collision");
    }

    #[test]
    fn phase_a_resumes_over_its_own_committed_lowering() {
        let dir = tempfile::tempdir().expect("tempdir");
        let project = seeded_project(dir.path());
        let parsed = parsed_d3();
        run_phase_a(&project, SPEC_PATH, &parsed).expect("phase A");
        write_file(&project.join("models/revenue_daily.sql"), b"SELECT 1\n");
        run_phase_a(&project, SPEC_PATH, &parsed).expect("resuming: no refusal");
    }

    #[test]
    fn phase_b_requires_phase_a() {
        let dir = tempfile::tempdir().expect("tempdir");
        let project = seeded_project(dir.path());
        let error = run_phase_b(&project, SPEC_PATH, &parsed_d3()).expect_err("no phase A");
        assert_eq!(error.code, "phase-a-missing");
    }

    #[test]
    fn phase_b_requires_the_drafted_sidecar() {
        let dir = tempfile::tempdir().expect("tempdir");
        let project = seeded_project(dir.path());
        let parsed = parsed_d3();
        run_phase_a(&project, SPEC_PATH, &parsed).expect("phase A");
        let error = run_phase_b(&project, SPEC_PATH, &parsed).expect_err("no sidecar");
        assert_eq!(error.code, "sidecar-missing");
    }

    #[test]
    fn phase_b_refuses_after_a_spec_edit_supersedes_phase_a() {
        // Cross-generation refusal: Phase A under spec A, then Phase B
        // attempted under an edited spec B → refused as superseded. The
        // committed manifest never mixes spec B's metadata with spec A's
        // contract.
        let dir = tempfile::tempdir().expect("tempdir");
        let parsed = parsed_d3();
        let project = project_with_phase_a_and_draft(dir.path(), &parsed);
        let edited_text = String::from_utf8(SPEC_FIXTURE.to_vec())
            .expect("utf-8")
            .replace(
                r#"checks = ["revenue_eur >= 0"]"#,
                r#"checks = ["revenue_eur > 0"]"#,
            );
        let edited = parse_spec_bytes(edited_text.as_bytes(), SPEC_PATH).expect("valid");
        assert_ne!(edited.digest, parsed.digest);
        let error = run_phase_b(&project, SPEC_PATH, &edited).expect_err("superseded");
        assert_eq!(error.code, "spec-superseded");
        assert!(error.message.contains(&parsed.digest), "{error}");
        assert!(error.message.contains(&edited.digest), "{error}");
        let manifest = committed(&project);
        assert_eq!(manifest.phase, ManifestPhase::LoweredContract);
        assert_eq!(manifest.spec_digest, parsed.digest);
    }

    #[test]
    fn phase_b_refuses_a_foreign_generation_identity() {
        // A committed manifest recorded from a different spec path is a
        // different generation identity, not a resumable Phase A.
        let dir = tempfile::tempdir().expect("tempdir");
        let project = seeded_project(dir.path());
        let parsed = parsed_d3();
        run_phase_a(&project, "products/elsewhere.toml", &parsed).expect("phase A");
        write_file(&project.join("models/revenue_daily.sql"), b"SELECT 1\n");
        write_file(
            &project.join("models/revenue_daily.toml"),
            DRAFTED_SIDECAR.as_bytes(),
        );
        let error = run_phase_b(&project, SPEC_PATH, &parsed).expect_err("foreign");
        assert_eq!(error.code, "phase-a-identity-mismatch");
        assert!(error.message.contains("spec_path"), "{error}");
    }

    #[test]
    fn phase_b_detects_phase_a_tampering() {
        // The worker mutated a spec-owned file through some route: caught
        // by the manifest hash, treated as tampering, never trusted.
        let dir = tempfile::tempdir().expect("tempdir");
        let project = seeded_project(dir.path());
        let parsed = parsed_d3();
        run_phase_a(&project, SPEC_PATH, &parsed).expect("phase A");
        let contract = project.join("models/revenue_daily.contract.toml");
        let tampered = std::fs::read_to_string(&contract)
            .expect("contract")
            .replace("Int64", "String");
        write_file(&contract, tampered.as_bytes());
        write_file(
            &project.join("models/revenue_daily.toml"),
            DRAFTED_SIDECAR.as_bytes(),
        );
        let error = run_phase_b(&project, SPEC_PATH, &parsed).expect_err("tampered");
        assert_eq!(error.code, "phase-a-tampered");
    }

    #[test]
    fn full_two_phase_flow_commits_everything() {
        let dir = tempfile::tempdir().expect("tempdir");
        let project = seeded_project(dir.path());
        let parsed = parsed_d3();
        full_flow(&project, &parsed);
        let manifest = committed(&project);
        assert_eq!(manifest.phase, ManifestPhase::Merged);
        assert_total(&parsed.spec, &manifest).expect("total");
        for (rel_path, expected) in &manifest.artifacts {
            let bytes = std::fs::read(project.join(rel_path)).expect("artifact");
            assert_eq!(&content_digest(&bytes), expected, "{rel_path}");
        }
        assert_eq!(leftovers(&project), Vec::<String>::new());
    }

    // ------------------- the drafting-window reopen (#1493) -----------------

    #[test]
    fn reopen_demotes_a_verified_merged_generation_to_phase_a() {
        let dir = tempfile::tempdir().expect("tempdir");
        let project = seeded_project(dir.path());
        let parsed = parsed_d3();
        full_flow(&project, &parsed);
        let before = snapshot(&project);

        let outcome =
            demote_merged_manifest_to_phase_a(&project, SPEC_PATH, &parsed).expect("reopens");
        assert_eq!(outcome, ReopenOutcome::Reopened);

        // The manifest is Phase A again: contract-only artifact set, so
        // the sidecar is back in the drafting namespace.
        let manifest = committed(&project);
        assert_eq!(manifest.phase, ManifestPhase::LoweredContract);
        assert_eq!(
            manifest.artifacts.keys().collect::<Vec<_>>(),
            vec!["models/revenue_daily.contract.toml"]
        );

        // ONLY the manifest changed: every model file kept its exact
        // bytes, nothing appeared, nothing vanished.
        let after = snapshot(&project);
        assert_eq!(
            before.keys().collect::<Vec<_>>(),
            after.keys().collect::<Vec<_>>(),
            "no files created or removed"
        );
        let changed: Vec<&String> = before
            .iter()
            .filter(|(name, bytes)| after.get(*name) != Some(bytes))
            .map(|(name, _)| name)
            .collect();
        assert_eq!(
            changed,
            vec![".rocky/fulfillment/revenue_daily/lowering-manifest.json"],
            "the reopen writes the manifest and nothing else"
        );

        // Round-trip: the next Phase B re-merges and re-records the
        // sidecar hash transactionally.
        run_phase_b(&project, SPEC_PATH, &parsed).expect("phase B re-commits");
        assert_eq!(committed(&project).phase, ManifestPhase::Merged);
        assert_eq!(leftovers(&project), Vec::<String>::new());
    }

    #[test]
    fn reopen_refuses_a_drifted_artifact_without_mutating() {
        // Drift in EITHER committed artifact between the merge and the
        // reopen had no authorized writer: tamper, refused, untouched.
        for tampered_rel in [
            "models/revenue_daily.toml",
            "models/revenue_daily.contract.toml",
        ] {
            let dir = tempfile::tempdir().expect("tempdir");
            let project = seeded_project(dir.path());
            let parsed = parsed_d3();
            full_flow(&project, &parsed);
            let path = project.join(tampered_rel);
            let mut text = std::fs::read_to_string(&path).expect("read");
            text.push_str("\n# out-of-band edit\n");
            write_file(&path, text.as_bytes());
            let before = snapshot(&project);

            let outcome =
                demote_merged_manifest_to_phase_a(&project, SPEC_PATH, &parsed).expect("runs");
            let ReopenOutcome::Tampered(problems) = outcome else {
                panic!("expected Tampered for {tampered_rel}, got {outcome:?}");
            };
            assert!(
                problems
                    .iter()
                    .any(|p| p.contains(tampered_rel) && p.contains("content drift")),
                "{tampered_rel}: {problems:?}"
            );
            assert_eq!(snapshot(&project), before, "a refusal mutates nothing");
            assert_eq!(
                committed(&project).phase,
                ManifestPhase::Merged,
                "the merged manifest is never demoted over drifted bytes"
            );
        }
    }

    #[test]
    fn reopen_is_not_needed_before_the_merge() {
        let dir = tempfile::tempdir().expect("tempdir");
        let project = seeded_project(dir.path());
        let parsed = parsed_d3();

        // No committed manifest at all: nothing to demote.
        assert_eq!(
            demote_merged_manifest_to_phase_a(&project, SPEC_PATH, &parsed).expect("runs"),
            ReopenOutcome::NotNeeded
        );

        // A committed Phase-A manifest: the window is already open.
        run_phase_a(&project, SPEC_PATH, &parsed).expect("phase A");
        let before = snapshot(&project);
        assert_eq!(
            demote_merged_manifest_to_phase_a(&project, SPEC_PATH, &parsed).expect("runs"),
            ReopenOutcome::NotNeeded
        );
        assert_eq!(snapshot(&project), before, "not-needed writes nothing");
    }

    #[test]
    fn reopen_refuses_a_superseded_or_foreign_generation() {
        let dir = tempfile::tempdir().expect("tempdir");
        let project = seeded_project(dir.path());
        let parsed = parsed_d3();
        full_flow(&project, &parsed);
        let before = snapshot(&project);

        // The spec moved after the merge: refuse, never demote spec A's
        // generation under spec B.
        let edited_text = String::from_utf8(SPEC_FIXTURE.to_vec())
            .expect("utf-8")
            .replace(
                r#"checks = ["revenue_eur >= 0"]"#,
                r#"checks = ["revenue_eur > 0"]"#,
            );
        let edited = parse_spec_bytes(edited_text.as_bytes(), SPEC_PATH).expect("valid");
        assert_ne!(edited.digest, parsed.digest);
        let error = demote_merged_manifest_to_phase_a(&project, SPEC_PATH, &edited)
            .expect_err("superseded");
        assert_eq!(error.code, "spec-superseded");
        assert!(error.message.contains(&parsed.digest), "{error}");
        assert!(error.message.contains(&edited.digest), "{error}");

        // A manifest recorded under another spec path is a foreign
        // generation identity.
        let error = demote_merged_manifest_to_phase_a(&project, "products/elsewhere.toml", &parsed)
            .expect_err("foreign");
        assert_eq!(error.code, "reopen-foreign-generation");
        assert!(error.message.contains("spec_path"), "{error}");

        assert_eq!(snapshot(&project), before, "refusals mutate nothing");
        assert_eq!(committed(&project).phase, ManifestPhase::Merged);
    }

    // ---------------- the reopen's evidence gate (F1, #1493) ----------------

    /// The record a loop that DECIDED a drafting round leaves on disk:
    /// this product, at `drafting`, owner stamp = this process.
    ///
    /// The stamp carries this process's REAL start time, because the
    /// gate pairs the pid with it — a pid alone no longer passes.
    fn decided_record(parsed: &ParsedSpec) -> FulfillStateRecord {
        let mut record = FulfillStateRecord::new(
            FulfillState::Drafting,
            parsed.product_id(),
            Some(parsed.digest.clone()),
            None,
        );
        let pid = std::process::id();
        record.owner_pid = Some(pid);
        record.owner_start_time = Some(
            crate::process::process_liveness(pid)
                .expect("probe this process")
                .expect("this process is alive"),
        );
        record
    }

    /// A state store at `dir/state.redb` holding exactly `record` for
    /// `revenue_daily` — the reopen reads the store, so a test that
    /// wants a record observed has to actually persist it.
    fn store_holding(dir: &Path, record: Option<&FulfillStateRecord>) -> StateStore {
        let store = StateStore::open(&dir.join("state.redb")).expect("state store");
        if let Some(record) = record {
            let row = crate::fulfill::FulfillJournalRow {
                seq: 0,
                at: None,
                event: "seeded".to_string(),
                from_state: None,
                to_state: record.state.tag().to_string(),
                spec_digest: None,
                plan_id: None,
                idempotency_key: None,
            };
            let outcome = store
                .fulfill_state_cas("revenue_daily", None, record, &row)
                .expect("seed the record");
            assert_eq!(outcome, crate::fulfill::FulfillCas::Won);
        }
        store
    }

    #[test]
    fn the_reopen_demotes_only_on_the_loops_decided_record() {
        let dir = tempfile::tempdir().expect("tempdir");
        let project = seeded_project(dir.path());
        let parsed = parsed_d3();
        full_flow(&project, &parsed);
        let store = store_holding(dir.path(), Some(&decided_record(&parsed)));

        let outcome = reopen_for_drafting(&project, SPEC_PATH, &parsed, &store)
            .expect("the decided record opens the window");
        assert_eq!(outcome, ReopenOutcome::Reopened);
        assert_eq!(committed(&project).phase, ManifestPhase::LoweredContract);
    }

    #[test]
    fn the_reopen_refuses_when_the_store_holds_no_record_at_all() {
        // The forgery case the evidence gate exists for: a caller that
        // never won a CAS has nothing in the store to point at, and
        // cannot supply a record of its own because the entry does not
        // take one.
        let dir = tempfile::tempdir().expect("tempdir");
        let project = seeded_project(dir.path());
        let parsed = parsed_d3();
        full_flow(&project, &parsed);
        let before = snapshot(&project);
        let store = store_holding(dir.path(), None);

        let error =
            reopen_for_drafting(&project, SPEC_PATH, &parsed, &store).expect_err("no record");
        assert_eq!(error.code, "reopen-undecided");
        assert!(
            error.message.contains("no fulfillment record exists"),
            "{error}"
        );
        assert_eq!(snapshot(&project), before, "a refusal mutates nothing");
        assert_eq!(committed(&project).phase, ManifestPhase::Merged);
    }

    /// One refusal case: why it must refuse, how to build the record
    /// that triggers it, and the fragment the message must name.
    type UndecidedCase = (
        &'static str,
        fn(&ParsedSpec) -> FulfillStateRecord,
        &'static str,
    );

    #[test]
    fn the_reopen_refuses_every_record_that_is_not_the_loops_decision() {
        // Each case removes exactly ONE element of the decision, so a
        // passing case cannot be carried by the others.
        let cases: [UndecidedCase; 6] = [
            (
                "no owner stamp at all — nobody won the record's CAS",
                |parsed: &ParsedSpec| {
                    let mut record = decided_record(parsed);
                    record.owner_pid = None;
                    record
                },
                "nothing decided a drafting round",
            ),
            (
                "owned by a DIFFERENT process — a concurrent caller",
                |parsed: &ParsedSpec| {
                    let mut record = decided_record(parsed);
                    record.owner_pid = Some(std::process::id().wrapping_add(1));
                    record
                },
                "owned by another process",
            ),
            (
                "OUR pid, but a dead owner's start time — a recycled pid",
                |parsed: &ParsedSpec| {
                    let mut record = decided_record(parsed);
                    record.owner_start_time = Some(1);
                    record
                },
                "pid the system reused",
            ),
            (
                "our pid with NO recorded start time — unconfirmable, so not ours",
                |parsed: &ParsedSpec| {
                    let mut record = decided_record(parsed);
                    record.owner_start_time = None;
                    record
                },
                "pid the system reused",
            ),
            (
                "the loop is not in a drafting round",
                |parsed: &ParsedSpec| {
                    let mut record = decided_record(parsed);
                    record.state = FulfillState::Merged;
                    record
                },
                "not 'drafting'",
            ),
            (
                "a record fetched for ANOTHER product",
                |parsed: &ParsedSpec| {
                    let mut record = decided_record(parsed);
                    record.product_id = "product:elsewhere".to_string();
                    record
                },
                "belongs to product:elsewhere",
            ),
        ];

        for (why, build, expected_fragment) in cases {
            let dir = tempfile::tempdir().expect("tempdir");
            let project = seeded_project(dir.path());
            let parsed = parsed_d3();
            full_flow(&project, &parsed);
            let before = snapshot(&project);
            // The record has to be PERSISTED to be observed — that is
            // the point of reading the store rather than an argument.
            let store = store_holding(dir.path(), Some(&build(&parsed)));

            let error = reopen_for_drafting(&project, SPEC_PATH, &parsed, &store).expect_err(why);
            assert_eq!(error.code, "reopen-undecided", "{why}: {error}");
            assert!(
                error.message.contains(expected_fragment),
                "{why}: the refusal must name the missing evidence \
                 ({expected_fragment:?}), got: {error}"
            );
            assert!(
                error.message.contains("rocky fulfill revenue_daily"),
                "{why}: the refusal must name the decided route: {error}"
            );
            assert_eq!(
                snapshot(&project),
                before,
                "{why}: a refusal mutates nothing"
            );
            assert_eq!(
                committed(&project).phase,
                ManifestPhase::Merged,
                "{why}: the merged manifest is never demoted without the decision"
            );
        }
    }

    // ------------------------- crash and recovery ---------------------------

    #[test]
    fn crash_between_staged_renames_rolls_back() {
        // Phase B commits 3 renames: journal swap (1), sidecar (2),
        // manifest (3 — the commit marker). Failing at 2 and at 3 are the
        // two distinct mid-protocol shapes.
        for fail_on_call in [2usize, 3] {
            let dir = tempfile::tempdir().expect("tempdir");
            let parsed = parsed_d3();
            let project = project_with_phase_a_and_draft(dir.path(), &parsed);
            let before = snapshot(&project);

            phase_b_with_rename_bomb(&project, &parsed, fail_on_call);

            let action = recover_generation(&project, &parsed).expect("recovers");
            assert_eq!(action, RecoveryAction::RolledBack, "call {fail_on_call}");
            assert_eq!(
                snapshot(&project),
                before,
                "recovery must restore the pre-generation tree exactly (call {fail_on_call})"
            );
            // The committed manifest is still Phase A's — the uncommitted
            // merge generation left no trace.
            assert_eq!(committed(&project).phase, ManifestPhase::LoweredContract);
        }
    }

    #[test]
    fn crash_before_journal_leaves_priors_untouched() {
        // A crash before the journal swap (call 1) predates recovery's
        // remit: nothing was renamed, the priors are untouched, and the
        // next commit restages over the orphaned tmps and backups.
        let dir = tempfile::tempdir().expect("tempdir");
        let parsed = parsed_d3();
        let project = project_with_phase_a_and_draft(dir.path(), &parsed);
        let without_orphans = |tree: BTreeMap<String, Vec<u8>>| -> BTreeMap<String, Vec<u8>> {
            tree.into_iter()
                .filter(|(name, _)| !name.ends_with(STAGED_SUFFIX) && !name.ends_with(PREV_SUFFIX))
                .collect()
        };
        let before = without_orphans(snapshot(&project));

        phase_b_with_rename_bomb(&project, &parsed, 1);

        assert_eq!(
            recover_generation(&project, &parsed).expect("no journal"),
            RecoveryAction::None
        );
        assert_eq!(without_orphans(snapshot(&project)), before);
        run_phase_b(&project, SPEC_PATH, &parsed).expect("restages cleanly");
        assert_eq!(committed(&project).phase, ManifestPhase::Merged);
    }

    /// Recovery must not restore a backup the journal never recorded.
    ///
    /// The producer deliberately leaves a failed `.ff-prev` in place rather
    /// than unlink it under a race (#1482's review rejected both a plain and
    /// an inode-checked unlink). So an orphan from an earlier failed attempt
    /// can sit next to a final whose journal entry says `has_prev: false`.
    /// Renaming it over the final would undo a commit using content the
    /// journal never vouched for (#1502).
    #[test]
    fn recovery_refuses_a_backup_the_journal_does_not_know_about() {
        let dir = tempfile::tempdir().expect("tempdir");
        let project = dir.path().join("project");
        let models = project.join("models");
        std::fs::create_dir_all(&models).expect("mkdir");

        // A committed final, and beside it an orphan `.ff-prev` — the shape a
        // failed backup write leaves, since the producer deliberately does
        // not unlink under a race (#1482).
        let final_path = models.join("revenue_daily.contract.toml");
        write_file(&final_path, b"the committed contract");
        let orphan = prev_sibling(&final_path);
        write_file(&orphan, b"orphan bytes from a failed earlier attempt");

        // A journal that records NO backup for that final.
        write_journal(
            &project,
            "revenue_daily",
            &forged_journal_payload(&[serde_json::json!({
                "final": "models/revenue_daily.contract.toml",
                "staged_sha": content_digest(b"the committed contract"),
                "has_prev": false,
            })]),
        );

        let error = recover_generation(&project, &parsed_d3())
            .expect_err("an unrecorded backup must refuse recovery");
        assert_eq!(error.code, "commit-unexpected-backup");

        // Nothing restored, nothing deleted: the final keeps its own bytes
        // and the orphan is left for the operator to inspect.
        assert_eq!(
            std::fs::read(&final_path).expect("final intact"),
            b"the committed contract",
            "the orphan must not have been renamed over the final"
        );
        assert_eq!(
            std::fs::read(&orphan).expect("orphan intact"),
            b"orphan bytes from a failed earlier attempt",
            "recovery must not delete a file it did not create"
        );
    }

    /// The refusal must be a PAUSE, not a dead end.
    ///
    /// `recover_generation` refuses mid-loop, so entries handled before the
    /// orphan may already be rolled back when it returns — the tree is left
    /// partially restored. That is only acceptable if the operator can act
    /// on the message and finish the job, so this asserts the whole
    /// sequence: refuse, clear the named file, re-run, complete.
    ///
    /// The journal is removed only AFTER the loop, which is what makes the
    /// re-run possible.
    #[test]
    fn the_unexpected_backup_refusal_is_recoverable_by_re_running() {
        let dir = tempfile::tempdir().expect("tempdir");
        let project = dir.path().join("project");
        let models = project.join("models");
        std::fs::create_dir_all(&models).expect("mkdir");

        let final_path = models.join("revenue_daily.contract.toml");
        write_file(&final_path, b"the committed contract");
        let orphan = prev_sibling(&final_path);
        write_file(&orphan, b"orphan bytes from a failed earlier attempt");

        write_journal(
            &project,
            "revenue_daily",
            &forged_journal_payload(&[serde_json::json!({
                "final": "models/revenue_daily.contract.toml",
                "staged_sha": content_digest(b"the committed contract"),
                "has_prev": false,
            })]),
        );

        let error = recover_generation(&project, &parsed_d3()).expect_err("refuses on the orphan");
        assert_eq!(error.code, "commit-unexpected-backup");

        // The journal survives the refusal — without it the operator would
        // be stranded, because recovery would then report "nothing to do".
        assert!(
            journal_path(&project, "revenue_daily").is_file(),
            "the journal must survive a refusal or the rollback cannot be finished"
        );

        // The operator does what the message says.
        std::fs::remove_file(&orphan).expect("operator clears the named file");

        // And the re-run completes.
        let action = recover_generation(&project, &parsed_d3()).expect("re-run completes");
        assert_eq!(action, RecoveryAction::RolledBack);
        assert!(
            !journal_path(&project, "revenue_daily").exists(),
            "a completed recovery consumes its journal"
        );
    }

    #[test]
    fn recovery_is_idempotent() {
        let dir = tempfile::tempdir().expect("tempdir");
        let parsed = parsed_d3();
        let project = project_with_phase_a_and_draft(dir.path(), &parsed);
        let before = snapshot(&project);
        phase_b_with_rename_bomb(&project, &parsed, 3);
        assert_eq!(
            recover_generation(&project, &parsed).expect("recovers"),
            RecoveryAction::RolledBack
        );
        assert_eq!(
            recover_generation(&project, &parsed).expect("no-op"),
            RecoveryAction::None
        );
        assert_eq!(snapshot(&project), before);
    }

    #[test]
    fn crash_after_commit_marker_rolls_forward() {
        // Crash AFTER the manifest rename (during cleanup): the generation
        // is committed; recovery only sweeps the leftovers, never rolls
        // back.
        let dir = tempfile::tempdir().expect("tempdir");
        let parsed = parsed_d3();
        let project = project_with_phase_a_and_draft(dir.path(), &parsed);

        let mut rename = |src: &Path, dst: &Path| std::fs::rename(src, dst);
        let mut remove = |p: &Path| -> std::io::Result<()> {
            if p.to_string_lossy().ends_with(PREV_SUFFIX) {
                return Err(std::io::Error::other("injected crash during cleanup"));
            }
            std::fs::remove_file(p)
        };
        let error = run_phase_b_with_ops(
            &project,
            SPEC_PATH,
            &parsed,
            &mut CommitOps {
                rename: &mut rename,
                remove: &mut remove,
            },
        )
        .expect_err("the injected cleanup crash must surface");
        assert!(error.message.contains("injected crash"), "{error}");

        assert_eq!(
            recover_generation(&project, &parsed).expect("recovers"),
            RecoveryAction::RolledForward
        );
        assert_eq!(committed(&project).phase, ManifestPhase::Merged);
        assert_eq!(leftovers(&project), Vec::<String>::new());
    }

    #[test]
    fn recovery_runs_automatically_on_the_next_phase() {
        // A crashed commit does not need a manual recovery call — the next
        // lowering run resolves it before staging.
        let dir = tempfile::tempdir().expect("tempdir");
        let parsed = parsed_d3();
        let project = project_with_phase_a_and_draft(dir.path(), &parsed);
        phase_b_with_rename_bomb(&project, &parsed, 3);

        run_phase_b(&project, SPEC_PATH, &parsed).expect("recovers then commits");
        assert_eq!(committed(&project).phase, ManifestPhase::Merged);
        assert!(!journal_path(&project, "revenue_daily").exists());
    }

    // ----- fresh-path symlink refusals (no journal → recovery is a no-op) ---
    //
    // The residue-symlink guard inside `recover_generation` runs only when a
    // journal already exists. On the FRESH commit path — the common case, no
    // prior crash — the staging writes (`write` into `.ff-staged`, `copy`
    // into `.ff-prev`, `write` into `<journal>.ff-staged`) would otherwise
    // follow an attacker-planted symlink out of the project. These pin the
    // pre-mutation refusal that closes that class.

    #[cfg(unix)]
    fn plant_symlink(link: &Path, target: &Path) {
        if let Some(parent) = link.parent() {
            std::fs::create_dir_all(parent).expect("mkdir");
        }
        std::os::unix::fs::symlink(target, link).expect("symlink");
    }

    /// Every `.ff-staged` / `.ff-prev` REGULAR-FILE residue under the
    /// project (the mutations the staging loop and backup copy produce).
    /// Symlinks (a planted exploit) are excluded via `symlink_metadata`, so
    /// a clean PRE-mutation refusal returns `[]` — this is what proves the
    /// guard ran before the first `write`/`copy`, not merely somewhere.
    #[cfg(unix)]
    fn staging_residue(project: &Path) -> Vec<String> {
        fn walk(dir: &Path, root: &Path, out: &mut Vec<String>) {
            let Ok(entries) = std::fs::read_dir(dir) else {
                return;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                let Ok(meta) = std::fs::symlink_metadata(&path) else {
                    continue;
                };
                if meta.file_type().is_dir() {
                    walk(&path, root, out);
                } else if meta.file_type().is_file() {
                    let rel = path
                        .strip_prefix(root)
                        .expect("under root")
                        .to_string_lossy()
                        .replace('\\', "/");
                    if rel.ends_with(STAGED_SUFFIX) || rel.ends_with(PREV_SUFFIX) {
                        out.push(rel);
                    }
                }
            }
        }
        let mut out = Vec::new();
        walk(project, project, &mut out);
        out.sort();
        out
    }

    #[cfg(unix)]
    #[test]
    fn fresh_commit_refuses_a_symlinked_staged_target_and_leaves_it_untouched() {
        // THE leaf exploit: a first `rocky product compile` (no journal) with
        // a symlink pre-planted at `<contract>.ff-staged` pointing at a file
        // OUTSIDE the project. Without the guard, the staged write follows it
        // and writes the contract bytes through to the target.
        let dir = tempfile::tempdir().expect("tempdir");
        let secret = dir.path().join("outside-secret");
        std::fs::write(&secret, b"a developer's private bytes outside the project").expect("write");
        let project = seeded_project(dir.path());
        let parsed = parsed_d3();
        plant_symlink(
            &project.join("models/revenue_daily.contract.toml.ff-staged"),
            &secret,
        );

        let error = run_phase_a(&project, SPEC_PATH, &parsed).expect_err("symlinked staged target");
        assert_eq!(error.code, "commit-symlinked-target");
        assert_eq!(
            std::fs::read(&secret).expect("still there"),
            b"a developer's private bytes outside the project",
            "the out-of-project target must be untouched"
        );
        // PRE-mutation: nothing was staged, backed up, or committed — the
        // guard ran before the first write, not after artifact staging.
        assert_eq!(staging_residue(&project), Vec::<String>::new());
        assert!(!manifest_path(&project).exists());
        assert!(!project.join("models/revenue_daily.contract.toml").exists());
    }

    #[cfg(unix)]
    #[test]
    fn fresh_commit_refuses_a_symlinked_prev_target_and_leaves_it_untouched() {
        // The backup `copy` vector: on a re-commit the existing final is
        // copied to `<final>.ff-prev` — a symlink there is written through.
        let dir = tempfile::tempdir().expect("tempdir");
        let secret = dir.path().join("outside-secret");
        std::fs::write(&secret, b"private bytes the backup copy must not overwrite")
            .expect("write");
        let project = seeded_project(dir.path());
        let parsed = parsed_d3();
        // First commit: the contract final now exists, so the next commit
        // will back it up.
        run_phase_a(&project, SPEC_PATH, &parsed).expect("phase A");
        plant_symlink(
            &project.join("models/revenue_daily.contract.toml.ff-prev"),
            &secret,
        );
        // Resume (re-commit): would copy the existing contract to .ff-prev.
        let error = run_phase_a(&project, SPEC_PATH, &parsed).expect_err("symlinked prev target");
        assert_eq!(error.code, "commit-symlinked-target");
        assert_eq!(
            std::fs::read(&secret).expect("still there"),
            b"private bytes the backup copy must not overwrite",
            "the out-of-project backup target must be untouched"
        );
        // PRE-mutation: the refused re-commit added no staging residue, and
        // the committed generation is still the first commit's Phase A.
        assert_eq!(staging_residue(&project), Vec::<String>::new());
        assert_eq!(committed(&project).phase, ManifestPhase::LoweredContract);
    }

    // ----- the backup copy's own syscall-level guards ----------------------
    //
    // The pre-check above refuses a symlink PLANTED before the commit. It
    // cannot refuse one swapped in AFTER it validates and before the copy
    // runs — a path-based check re-traverses the path, so the window is real
    // for any local process that can write the models directory. These pin
    // the syscall-level guard at each end of the copy, which is what closes
    // it: `O_NOFOLLOW` on the source read, O_EXCL on the destination create.
    // They exercise the helper directly because no end-to-end path can reach
    // the copy with a symlinked source — the pre-check refuses that first.

    #[cfg(unix)]
    #[test]
    fn the_backup_copy_refuses_a_source_swapped_for_a_symlink_after_the_check() {
        // A local racer swaps the final for a link at an out-of-project file
        // between the pre-check and the backup. `std::fs::copy` reads THROUGH
        // that link and stamps the secret into `<final>.ff-prev`, from which
        // a rollback renames it over the final — the secret becomes the
        // committed artifact. The `O_NOFOLLOW` open refuses instead.
        let dir = tempfile::tempdir().expect("tempdir");
        let secret = dir.path().join("outside-secret");
        std::fs::write(&secret, b"a developer's private bytes outside the project").expect("write");
        let project = seeded_project(dir.path());
        let final_path = project.join("models/revenue_daily.contract.toml");
        plant_symlink(&final_path, &secret);
        let prev = prev_sibling(&final_path);

        let error = copy_no_follow(&final_path, &prev).expect_err("a symlinked source is refused");

        // The security property, asserted platform-independently: no backup
        // exists at all, so nothing read through the link reached the
        // project. (The errno differs across unixes — Linux reports ELOOP,
        // some BSDs EMLINK — so the bytes are the assertion, not the code.)
        assert!(
            !prev.exists(),
            "no backup may be produced from a symlinked source (error was {error})"
        );
        assert_eq!(
            std::fs::read(&secret).expect("still there"),
            b"a developer's private bytes outside the project",
            "the out-of-project source must be untouched"
        );
    }

    /// The wiring, not the helper.
    ///
    /// The two tests around this one call `copy_no_follow` directly, because
    /// the vector is a TOCTOU race: the pre-check refuses a symlinked final,
    /// so only a swap landing between that check and the copy reaches it, and
    /// staging that deterministically needs a fault-injection hook this
    /// module does not have. That leaves the call site itself unpinned —
    /// reverting it to `std::fs::copy` keeps every one of those tests green,
    /// which is exactly the regression this guard catches.
    ///
    /// The needle is assembled from fragments so this assertion cannot match
    /// its own source text.
    /// Phase B's sidecar read must not follow a symlink.
    ///
    /// The merge preserves every key it does not own, so a sidecar pointed at
    /// an external TOML would have its contents merged into the project sidecar
    /// and committed as project content — credentials included (#1501). A
    /// hardlink needs no timing at all.
    #[cfg(unix)]
    #[test]
    fn phase_b_refuses_a_sidecar_swapped_for_a_symlink() {
        let dir = tempfile::tempdir().expect("tempdir");
        let parsed = parsed_d3();
        let project = project_with_phase_a_and_draft(dir.path(), &parsed);

        // Something outside the project that Rocky can read.
        let secret = dir.path().join("outside-secret.toml");
        std::fs::write(&secret, b"exfiltrated_key = \"s3cret\"\n").expect("write secret");

        let sidecar = project.join("models/revenue_daily.toml");
        assert!(
            sidecar.is_file(),
            "fixture: the drafted sidecar must exist before it is swapped — \
             otherwise this test proves nothing"
        );
        std::fs::remove_file(&sidecar).expect("remove drafted sidecar");
        std::os::unix::fs::symlink(&secret, &sidecar).expect("symlink");

        let error = run_phase_b(&project, SPEC_PATH, &parsed)
            .expect_err("a symlinked sidecar must be refused");
        assert_eq!(
            error.code, "commit-io",
            "expected the no-follow read to refuse it, got {error:?}"
        );

        // The plant is untouched: Rocky neither followed it nor wrote through it.
        assert!(
            std::fs::symlink_metadata(&sidecar)
                .expect("sidecar still present")
                .file_type()
                .is_symlink(),
            "the symlink was replaced, so something wrote through the sidecar path"
        );
        assert_eq!(
            std::fs::read_to_string(&secret).expect("secret readable"),
            "exfiltrated_key = \"s3cret\"\n",
            "the out-of-project file was modified"
        );

        // And no REGULAR file in the project carries the external key. Symlinks
        // are skipped: the only one is the plant above, and following it would
        // just re-read the secret — measuring the fixture, not the behaviour.
        for entry in std::fs::read_dir(project.join("models")).expect("read models") {
            let path = entry.expect("entry").path();
            let is_regular = std::fs::symlink_metadata(&path)
                .map(|m| m.file_type().is_file())
                .unwrap_or(false);
            if is_regular && let Ok(text) = std::fs::read_to_string(&path) {
                assert!(
                    !text.contains("exfiltrated_key"),
                    "{} carries a key from outside the project",
                    path.display()
                );
            }
        }
    }

    /// The Phase B read must go through `read_no_follow`.
    ///
    /// The banned and required strings are assembled at runtime so this test's
    /// own source cannot satisfy the search it performs.
    #[test]
    fn the_phase_b_sidecar_read_uses_the_no_follow_helper() {
        let source = include_str!("commit.rs");
        let banned = format!("std::fs::read_to_{}(&sidecar_path)", "string");
        assert!(
            !source.contains(&banned),
            "Phase B's sidecar read must go through read_no_follow: a plain read \
             follows a symlink, and the merge keeps every key it does not own"
        );
        let required = format!("read_no_{}(&sidecar_path)", "follow");
        assert!(
            source.contains(&required),
            "expected the Phase B sidecar read to call read_no_follow"
        );
    }

    #[test]
    fn the_backup_call_site_uses_the_no_follow_copy() {
        let source = include_str!("commit.rs");
        let banned = format!("std::fs::{}(&final_path", "copy");
        assert!(
            !source.contains(&banned),
            "the .ff-prev backup must go through copy_no_follow: a plain \
             copy follows a symlinked source swapped in after the pre-check"
        );
        let required = format!("copy_no_{}(&final_path, &prev)", "follow");
        assert!(
            source.contains(&required),
            "expected the backup call site to call copy_no_follow"
        );
    }

    #[cfg(unix)]
    #[test]
    fn the_backup_copy_refuses_a_destination_swapped_for_a_symlink_after_the_check() {
        // The other end of the same copy: a link parked at `<final>.ff-prev`
        // after the pre-check. O_EXCL neither follows it nor clobbers
        // through it — the link is unlinked and a real file created in its
        // place, so the out-of-project target keeps its bytes.
        let dir = tempfile::tempdir().expect("tempdir");
        let secret = dir.path().join("outside-secret");
        std::fs::write(&secret, b"private bytes the backup copy must not overwrite")
            .expect("write");
        let project = seeded_project(dir.path());
        let final_path = project.join("models/revenue_daily.contract.toml");
        std::fs::write(&final_path, b"the previous generation's contract").expect("write");
        let prev = prev_sibling(&final_path);
        plant_symlink(&prev, &secret);

        copy_no_follow(&final_path, &prev).expect("the backup is taken beside the link");

        assert_eq!(
            std::fs::read(&secret).expect("still there"),
            b"private bytes the backup copy must not overwrite",
            "the out-of-project destination target must be untouched"
        );
        assert!(
            !is_symlink(&prev),
            "the link must not survive as the backup"
        );
        assert_eq!(
            std::fs::read(&prev).expect("backup"),
            b"the previous generation's contract"
        );
    }

    #[cfg(unix)]
    #[test]
    fn the_backup_copy_carries_the_source_mode_across() {
        // `std::fs::copy` preserved the source's mode, and a rollback renames
        // the backup back over the final — so dropping the mode here would
        // silently widen a locked-down artifact on every recovery.
        use std::os::unix::fs::PermissionsExt as _;

        let dir = tempfile::tempdir().expect("tempdir");
        let project = seeded_project(dir.path());
        let final_path = project.join("models/revenue_daily.contract.toml");
        std::fs::write(&final_path, b"owner-only contract bytes").expect("write");
        std::fs::set_permissions(&final_path, std::fs::Permissions::from_mode(0o600))
            .expect("chmod");
        let prev = prev_sibling(&final_path);

        copy_no_follow(&final_path, &prev).expect("copy");

        assert_eq!(
            std::fs::read(&prev).expect("backup"),
            b"owner-only contract bytes"
        );
        assert_eq!(
            std::fs::metadata(&prev).expect("meta").permissions().mode() & 0o777,
            0o600
        );
    }

    /// Set-user-ID must never survive the copy.
    ///
    /// `std::fs::copy` propagates the source mode verbatim. That is safe
    /// when the destination is the caller's own file, but here the source is
    /// attacker-influenceable and the destination is created as Rocky: a
    /// raced `04755` source would produce a ROCKY-OWNED SETUID backup, which
    /// a rollback then renames over the final.
    ///
    /// The mode assertion in the test above masks `& 0o777`, so it cannot
    /// see these bits at all — this one asserts the UNMASKED mode.
    #[cfg(unix)]
    #[test]
    fn the_backup_copy_drops_setuid_and_setgid_from_the_source_mode() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = tempfile::tempdir().expect("tempdir");
        let project = seeded_project(dir.path());
        let final_path = project.join("models/revenue_daily.contract.toml");
        std::fs::write(&final_path, b"contract bytes").expect("write");
        // setuid + setgid + sticky, all three, plus 0755.
        std::fs::set_permissions(&final_path, std::fs::Permissions::from_mode(0o7755))
            .expect("chmod");
        let prev = prev_sibling(&final_path);

        copy_no_follow(&final_path, &prev).expect("copy");

        let mode = std::fs::metadata(&prev).expect("meta").permissions().mode();
        assert_eq!(
            mode & 0o7000,
            0,
            "setuid/setgid/sticky must not be carried onto a Rocky-created \
             backup (mode was {mode:o})"
        );
        assert_eq!(mode & 0o777, 0o755, "the ordinary permission bits carry");
    }

    /// A non-regular source is refused, and refused WITHOUT blocking.
    ///
    /// `std::fs::copy` rejected non-regular sources; reading by hand has to
    /// refuse them explicitly. A FIFO raced into place would otherwise block
    /// the open until a writer appeared — the commit hangs forever, with no
    /// timeout anywhere above it.
    #[cfg(unix)]
    #[test]
    fn the_backup_copy_refuses_a_fifo_source_instead_of_blocking() {
        let dir = tempfile::tempdir().expect("tempdir");
        let project = seeded_project(dir.path());
        let final_path = project.join("models/revenue_daily.contract.toml");
        std::fs::remove_file(&final_path).ok();
        let c_path = std::ffi::CString::new(final_path.as_os_str().as_encoded_bytes())
            .expect("path has no interior nul");
        // SAFETY: `mkfifo` with a valid NUL-terminated path and a mode is
        // sound; the worst case is a failure return, asserted below.
        let made = unsafe { libc::mkfifo(c_path.as_ptr(), 0o644) };
        assert_eq!(
            made,
            0,
            "mkfifo failed: {}",
            std::io::Error::last_os_error()
        );
        let prev = prev_sibling(&final_path);

        // The assertion is that this RETURNS. Before O_NONBLOCK it would
        // block here until a writer opened the FIFO, which never happens.
        let error = copy_no_follow(&final_path, &prev).expect_err("a FIFO source is refused");

        assert!(
            error.to_string().contains("not a regular file"),
            "expected the regular-file refusal, got: {error}"
        );
        assert!(!prev.exists(), "a refused copy must leave no backup behind");
    }

    #[cfg(unix)]
    #[test]
    fn fresh_commit_refuses_a_symlinked_journal_temp_and_leaves_it_untouched() {
        // The journal's own `.ff-staged` tmp is written too, in the state dir
        // — an attacker-writable location under the same threat model.
        let dir = tempfile::tempdir().expect("tempdir");
        let secret = dir.path().join("outside-secret");
        std::fs::write(&secret, b"journal-temp target bytes").expect("write");
        let project = seeded_project(dir.path());
        let parsed = parsed_d3();
        plant_symlink(
            &project
                .join(state_dir_rel("revenue_daily"))
                .join(format!("{STAGING_JOURNAL}{STAGED_SUFFIX}")),
            &secret,
        );
        let error = run_phase_a(&project, SPEC_PATH, &parsed).expect_err("symlinked journal temp");
        assert_eq!(error.code, "commit-symlinked-target");
        assert_eq!(
            std::fs::read(&secret).expect("still there"),
            b"journal-temp target bytes"
        );
        // PRE-mutation: the guard covers the journal temp up front, so no
        // artifact was staged before the refusal either.
        assert_eq!(staging_residue(&project), Vec::<String>::new());
        assert!(!manifest_path(&project).exists());
    }

    #[cfg(unix)]
    #[test]
    fn a_symlinked_final_is_refused_before_any_staging_residue() {
        // A symlink AT the final is refused too. The assertion is NOT the
        // (vacuous) external bytes — an unguarded final is only a `copy`
        // SOURCE, then replaced by `rename`, so its target is never written.
        // What the vulnerability WOULD produce is staging residue: the staged
        // write creates `<contract>.ff-staged`, and the backup copies the
        // secret's bytes into `<contract>.ff-prev`. Asserting NO residue is
        // therefore the non-vacuous proof of pre-mutation refusal.
        let dir = tempfile::tempdir().expect("tempdir");
        let secret = dir.path().join("outside-secret");
        std::fs::write(&secret, b"final-target bytes").expect("write");
        let project = seeded_project(dir.path());
        let parsed = parsed_d3();
        plant_symlink(&project.join("models/revenue_daily.contract.toml"), &secret);
        let error = run_phase_a(&project, SPEC_PATH, &parsed).expect_err("symlinked final");
        assert_eq!(error.code, "commit-symlinked-target");
        assert_eq!(
            staging_residue(&project),
            Vec::<String>::new(),
            "a symlinked final must be refused before any staged/prev residue is created"
        );
        assert!(!manifest_path(&project).exists());
    }

    #[cfg(unix)]
    #[test]
    fn fresh_commit_refuses_a_symlinked_parent_dir_and_leaves_the_out_of_tree_target_untouched() {
        // BLOCKER #2 — the ANCESTOR attack a leaf-only check misses, no race:
        // the attacker pre-plants `models -> /outside` (a malicious checkout
        // or tarball) with a regular file at `/outside/<leaf>.ff-staged`.
        // Every LEAF probe passes (the leaf is a regular file at the resolved
        // location); the unguarded staged write then truncates it out of the
        // project THROUGH the symlinked parent.
        let dir = tempfile::tempdir().expect("tempdir");
        let outside = dir.path().join("outside");
        std::fs::create_dir_all(&outside).expect("mkdir");
        let victim = outside.join("revenue_daily.contract.toml.ff-staged");
        std::fs::write(
            &victim,
            b"a real file the developer keeps outside the project",
        )
        .expect("write");

        let project = dir.path().join("project");
        std::fs::create_dir_all(&project).expect("mkdir");
        // `models` itself is a symlink out of the project — a REGULAR file at
        // each leaf, so a leaf `is_symlink` check would pass.
        std::os::unix::fs::symlink(&outside, project.join("models")).expect("symlink");
        let parsed = parsed_d3();

        let error =
            run_phase_a(&project, SPEC_PATH, &parsed).expect_err("symlinked parent directory");
        assert_eq!(error.code, "commit-symlinked-target");
        assert!(
            error.message.contains("escapes the project root"),
            "{error}"
        );
        assert_eq!(
            std::fs::read(&victim).expect("still there"),
            b"a real file the developer keeps outside the project",
            "the out-of-project file behind the symlinked parent must be untouched"
        );
        // Nothing committed inside or outside.
        assert!(!project.join(".rocky").exists() || !manifest_path(&project).exists());
    }

    #[cfg(unix)]
    #[test]
    fn fresh_commit_refuses_a_dangling_symlinked_parent_dir() {
        // The one ancestor escape parent-resolution's lexical fallback would
        // otherwise miss: `models -> /nonexistent` (dangling). `resolve_nonstrict`
        // cannot canonicalize it, falls back to a lexical `<root>/models`, and
        // the containment check would then PASS — but the subsequent
        // `create_dir_all` would follow the dangling link and create OUTSIDE
        // the project. The explicit dangling-ancestor guard refuses it.
        let dir = tempfile::tempdir().expect("tempdir");
        let nonexistent = dir.path().join("nonexistent-outside");
        let project = dir.path().join("project");
        std::fs::create_dir_all(&project).expect("mkdir");
        std::os::unix::fs::symlink(&nonexistent, project.join("models")).expect("symlink");
        let parsed = parsed_d3();

        let error =
            run_phase_a(&project, SPEC_PATH, &parsed).expect_err("dangling symlinked parent");
        assert_eq!(error.code, "commit-symlinked-target");
        assert!(
            error.message.contains("dangling symlinked ancestor"),
            "{error}"
        );
        // The guard refused before `create_dir_all` could create the target.
        assert!(
            !nonexistent.exists(),
            "nothing was created outside the project"
        );
    }

    // --------------- the journal is untrusted: forgeries refused ------------

    fn write_journal(project: &Path, product_name: &str, payload: &serde_json::Value) -> PathBuf {
        let journal = journal_path(project, product_name);
        write_file(
            &journal,
            serde_json::to_vec(payload).expect("payload").as_slice(),
        );
        journal
    }

    fn forged_journal_payload(entries: &[serde_json::Value]) -> serde_json::Value {
        let manifest = manifest_rel("revenue_daily");
        let mut all: Vec<serde_json::Value> = entries.to_vec();
        all.push(serde_json::json!({
            "final": manifest,
            "staged_sha": format!("sha256:{}", "0".repeat(64)),
            "has_prev": false,
        }));
        serde_json::json!({ "entries": all, "manifest": manifest })
    }

    #[test]
    fn forged_journal_with_traversal_path_is_refused_and_target_untouched() {
        // The traversal probe: a `../escape` entry must refuse recovery
        // outright — the file outside the project stays exactly as it was.
        let dir = tempfile::tempdir().expect("tempdir");
        let outside = dir.path().join("escape");
        write_file(&outside, b"precious bytes outside the project");
        let project = dir.path().join("project");
        std::fs::create_dir_all(&project).expect("mkdir");
        let journal = write_journal(
            &project,
            "revenue_daily",
            &forged_journal_payload(&[serde_json::json!({
                "final": "../escape",
                "staged_sha": content_digest(b"x"),
                "has_prev": false,
            })]),
        );
        let error = recover_generation(&project, &parsed_d3()).expect_err("refused");
        assert_eq!(error.code, "staging-journal-unsafe-path");
        assert_eq!(
            std::fs::read(&outside).expect("still there"),
            b"precious bytes outside the project"
        );
        assert!(journal.is_file(), "a refused journal is left for the human");
    }

    #[test]
    fn forged_journal_with_absolute_path_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let victim = dir.path().join("victim");
        write_file(&victim, b"absolute-path victim");
        let project = dir.path().join("project");
        std::fs::create_dir_all(&project).expect("mkdir");
        write_journal(
            &project,
            "revenue_daily",
            &forged_journal_payload(&[serde_json::json!({
                "final": victim.to_string_lossy(),
                "staged_sha": content_digest(b"x"),
                "has_prev": false,
            })]),
        );
        let error = recover_generation(&project, &parsed_d3()).expect_err("refused");
        assert_eq!(error.code, "staging-journal-unsafe-path");
        assert_eq!(
            std::fs::read(&victim).expect("intact"),
            b"absolute-path victim"
        );
    }

    #[cfg(unix)]
    #[test]
    fn symlink_at_an_allowed_final_path_is_refused_and_target_untouched() {
        // A symlink parked AT an artifact path is refused outright: the
        // final component is never followed, so the link cannot aim the
        // mutation loop (restore, unlink) at another in-root file.
        let dir = tempfile::tempdir().expect("tempdir");
        let project = dir.path().join("project");
        std::fs::create_dir_all(&project).expect("mkdir");
        let target = project.join("unrelated.sql");
        write_file(&target, b"in-root file the link aims at");
        let parsed = parsed_d3();
        let contract = contract_rel(&parsed);
        let link = project.join(&contract);
        std::fs::create_dir_all(link.parent().expect("parent")).expect("mkdir");
        std::os::unix::fs::symlink(&target, &link).expect("symlink");
        let journal = write_journal(
            &project,
            "revenue_daily",
            &forged_journal_payload(&[serde_json::json!({
                "final": contract,
                "staged_sha": content_digest(b"x"),
                "has_prev": false,
            })]),
        );
        let error = recover_generation(&project, &parsed).expect_err("refused");
        assert_eq!(error.code, "staging-journal-unsafe-path");
        assert!(error.message.contains("symlink"), "{error}");
        assert_eq!(
            std::fs::read(&target).expect("intact"),
            b"in-root file the link aims at"
        );
        assert!(is_symlink(&link), "the refused link is left for the human");
        assert!(journal.is_file());
    }

    #[cfg(unix)]
    #[test]
    fn symlinked_staging_residue_is_refused_before_any_mutation() {
        // A symlink parked at `<final>.ff-prev` (or `.ff-staged`) must
        // refuse recovery before the mutation loop renames anything into
        // place.
        let dir = tempfile::tempdir().expect("tempdir");
        let project = dir.path().join("project");
        std::fs::create_dir_all(&project).expect("mkdir");
        let target = project.join("unrelated.sql");
        write_file(&target, b"in-root file the link aims at");
        let parsed = parsed_d3();
        let contract = contract_rel(&parsed);
        let final_path = project.join(&contract);
        std::fs::create_dir_all(final_path.parent().expect("parent")).expect("mkdir");
        write_file(&final_path, b"a real contract final");
        let prev_link = prev_sibling(&final_path);
        std::os::unix::fs::symlink(&target, &prev_link).expect("symlink");
        write_journal(
            &project,
            "revenue_daily",
            &forged_journal_payload(&[serde_json::json!({
                "final": contract,
                "staged_sha": content_digest(b"x"),
                "has_prev": true,
            })]),
        );
        let error = recover_generation(&project, &parsed).expect_err("refused");
        assert_eq!(error.code, "staging-journal-unsafe-path");
        assert!(error.message.contains("residue"), "{error}");
        assert_eq!(
            std::fs::read(&final_path).expect("intact"),
            b"a real contract final"
        );
        assert_eq!(
            std::fs::read(&target).expect("intact"),
            b"in-root file the link aims at"
        );
    }

    #[cfg(unix)]
    #[test]
    fn forged_journal_with_symlink_escape_is_refused() {
        // A relative, `..`-free path whose parent is a symlink out of the
        // project must be refused too — containment is checked on the
        // RESOLVED path, not the spelling.
        let dir = tempfile::tempdir().expect("tempdir");
        let outside_dir = dir.path().join("outside");
        std::fs::create_dir_all(&outside_dir).expect("mkdir");
        let target = outside_dir.join("revenue_daily.contract.toml");
        write_file(&target, b"outside contract");
        let project = dir.path().join("project");
        std::fs::create_dir_all(&project).expect("mkdir");
        std::os::unix::fs::symlink(&outside_dir, project.join("models")).expect("symlink");
        write_journal(
            &project,
            "revenue_daily",
            &forged_journal_payload(&[serde_json::json!({
                "final": "models/revenue_daily.contract.toml",
                "staged_sha": content_digest(b"x"),
                "has_prev": false,
            })]),
        );
        let error = recover_generation(&project, &parsed_d3()).expect_err("refused");
        assert_eq!(error.code, "staging-journal-unsafe-path");
        assert_eq!(std::fs::read(&target).expect("intact"), b"outside contract");
    }

    #[test]
    fn forged_journal_entry_outside_the_generation_namespace_is_refused() {
        // An IN-ROOT path the generation can never stage (here the
        // agent-owned SQL) must still be refused: recovery mutations are
        // constrained to the artifact namespace derived from the approved
        // spec snapshot.
        let dir = tempfile::tempdir().expect("tempdir");
        let project = seeded_project(dir.path());
        let parsed = parsed_d3();
        full_flow(&project, &parsed);
        let sql = project.join("models/revenue_daily.sql");
        let committed_manifest_bytes =
            std::fs::read(manifest_path(&project)).expect("committed manifest");
        write_journal(
            &project,
            "revenue_daily",
            &serde_json::json!({
                "entries": [
                    {
                        "final": "models/revenue_daily.sql",
                        "staged_sha": content_digest(b"forged"),
                        "has_prev": false,
                    },
                    {
                        "final": manifest_rel("revenue_daily"),
                        "staged_sha": content_digest(&committed_manifest_bytes),
                        "has_prev": true,
                    },
                ],
                "manifest": manifest_rel("revenue_daily"),
            }),
        );
        let error = recover_generation(&project, &parsed).expect_err("refused");
        assert_eq!(error.code, "staging-journal-foreign-entry");
        assert_eq!(
            std::fs::read(&sql).expect("intact"),
            b"SELECT 1\n",
            "the agent's SQL must be untouched"
        );
    }

    #[test]
    fn journal_entry_naming_the_journal_itself_is_refused() {
        // An entry naming a staging journal (this product's own here) is a
        // forgery outright — recovery never stages its own machinery.
        let dir = tempfile::tempdir().expect("tempdir");
        let project = seeded_project(dir.path());
        let journal_rel = format!("{}/{STAGING_JOURNAL}", state_dir_rel("revenue_daily"));
        let journal = write_journal(
            &project,
            "revenue_daily",
            &forged_journal_payload(&[serde_json::json!({
                "final": journal_rel,
                "staged_sha": content_digest(b"x"),
                "has_prev": false,
            })]),
        );
        let error = recover_generation(&project, &parsed_d3()).expect_err("refused");
        assert_eq!(error.code, "staging-journal-forbidden-entry");
        assert!(error.message.contains("staging journal"), "{error}");
        assert!(journal.is_file(), "a refused journal is left for the human");
    }

    #[test]
    fn journal_entry_naming_a_foreign_manifest_path_is_refused() {
        // An entry naming any manifest path other than this generation's
        // own commit marker (another product's here) is refused before any
        // mutation.
        let dir = tempfile::tempdir().expect("tempdir");
        let project = seeded_project(dir.path());
        let other_manifest = project.join(manifest_rel("other_product"));
        write_file(&other_manifest, b"other product's committed manifest");
        write_journal(
            &project,
            "revenue_daily",
            &forged_journal_payload(&[serde_json::json!({
                "final": manifest_rel("other_product"),
                "staged_sha": content_digest(b"x"),
                "has_prev": false,
            })]),
        );
        let error = recover_generation(&project, &parsed_d3()).expect_err("refused");
        assert_eq!(error.code, "staging-journal-forbidden-entry");
        assert_eq!(
            std::fs::read(&other_manifest).expect("intact"),
            b"other product's committed manifest"
        );
    }

    #[test]
    fn case_aliased_duplicate_finals_are_refused() {
        // Two spellings differing only in case are one file on a
        // case-insensitive filesystem; duplicate detection casefolds so
        // the alias pair is refused exactly like a literal duplicate.
        let dir = tempfile::tempdir().expect("tempdir");
        let project = seeded_project(dir.path());
        write_file(
            &project.join("models/revenue_daily.toml"),
            b"drafted sidecar",
        );
        write_journal(
            &project,
            "revenue_daily",
            &forged_journal_payload(&[
                serde_json::json!({
                    "final": "models/revenue_daily.toml",
                    "staged_sha": content_digest(b"drafted sidecar"),
                    "has_prev": false,
                }),
                serde_json::json!({
                    "final": "models/REVENUE_DAILY.toml",
                    "staged_sha": content_digest(b"x"),
                    "has_prev": true,
                }),
            ]),
        );
        let error = recover_generation(&project, &parsed_d3()).expect_err("refused");
        assert_eq!(error.code, "staging-journal-corrupt");
        assert!(error.message.contains("case-insensitive"), "{error}");
        assert_eq!(
            std::fs::read(project.join("models/revenue_daily.toml")).expect("intact"),
            b"drafted sidecar"
        );
    }

    #[test]
    fn forged_staged_manifest_grants_no_recovery_authority() {
        // The laundering probe: an attacker who can write the state dir
        // forges a STAGED (uncommitted) manifest — correct product_id,
        // correct CURRENT spec digest, an artifacts map claiming an
        // arbitrary in-root file — plus a journal entry whose staged_sha
        // matches that file's current bytes (the shape that would make
        // rollback DELETE it). On-disk manifests grant nothing: the entry
        // is foreign to the snapshot-derived namespace, recovery refuses,
        // and the file is untouched.
        let dir = tempfile::tempdir().expect("tempdir");
        let project = seeded_project(dir.path());
        let parsed = parsed_d3();
        let precious = project.join("precious.txt");
        write_file(&precious, b"user bytes recovery must never touch");
        let manifest_relpath = manifest_rel("revenue_daily");
        let forged_manifest = Manifest {
            product_id: parsed.product_id(),
            spec_digest: parsed.digest.clone(),
            spec_path: SPEC_PATH.to_string(),
            output_model: "revenue_daily".to_string(),
            phase: ManifestPhase::LoweredContract,
            fields: BTreeMap::new(),
            artifacts: [(
                "precious.txt".to_string(),
                content_digest(b"user bytes recovery must never touch"),
            )]
            .into_iter()
            .collect(),
        }
        .to_json_bytes();
        write_file(
            &project.join(format!("{manifest_relpath}{STAGED_SUFFIX}")),
            &forged_manifest,
        );
        write_journal(
            &project,
            "revenue_daily",
            &serde_json::json!({
                "entries": [
                    {
                        "final": "precious.txt",
                        "staged_sha": content_digest(b"user bytes recovery must never touch"),
                        "has_prev": false,
                    },
                    {
                        "final": manifest_relpath,
                        "staged_sha": content_digest(&forged_manifest),
                        "has_prev": false,
                    },
                ],
                "manifest": manifest_relpath,
            }),
        );
        let error = recover_generation(&project, &parsed).expect_err("refused");
        assert_eq!(error.code, "staging-journal-foreign-entry");
        assert!(error.message.contains("precious.txt"), "{error}");
        assert_eq!(
            std::fs::read(&precious).expect("intact"),
            b"user bytes recovery must never touch"
        );
    }

    #[cfg(unix)]
    #[test]
    fn contained_final_path_returns_the_resolved_absolute() {
        // The validated path that comes back is the RESOLVED absolute —
        // the mutation loop must never operate on an unresolved alias, or
        // a parent symlink swapped in after validation would be followed
        // at use time.
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().join("project");
        std::fs::create_dir_all(root.join("models_real")).expect("mkdir");
        std::os::unix::fs::symlink(root.join("models_real"), root.join("models")).expect("symlink");
        write_file(&root.join("models_real/x.toml"), b"content");
        let resolved_root = root.canonicalize().expect("resolves");
        let validated = contained_final_path(
            &resolved_root,
            &root.join("whatever-journal"),
            "models/x.toml",
        )
        .expect("contained");
        assert_eq!(validated, validated.canonicalize().expect("resolves"));
        assert!(validated.iter().any(|part| part == "models_real"));
        assert!(!validated.iter().any(|part| part == "models"));
    }

    #[test]
    fn malformed_journal_is_refused_without_mutation() {
        // Schema violations (wrong types, missing keys, unknown keys)
        // refuse under the strict journal schema instead of being
        // half-interpreted.
        //
        // The coerced-bool case is built to be OTHERWISE VALID — correct
        // manifest entry, permissible finals, matching hashes — so it only
        // refuses because the schema is strict: a lax parser would coerce
        // "yes" → true and run the recovery to completion.
        let dir = tempfile::tempdir().expect("tempdir");
        let project = seeded_project(dir.path());
        let parsed = parsed_d3();
        full_flow(&project, &parsed);
        let before = snapshot(&project);
        let manifest_relpath = manifest_rel("revenue_daily");
        let committed_sha =
            content_digest(&std::fs::read(manifest_path(&project)).expect("manifest"));
        let contract_sha = content_digest(
            &std::fs::read(project.join("models/revenue_daily.contract.toml")).expect("contract"),
        );
        let coerced_bool_otherwise_valid = serde_json::json!({
            "entries": [
                {
                    "final": "models/revenue_daily.contract.toml",
                    "staged_sha": contract_sha,
                    "has_prev": "yes",
                },
                {
                    "final": manifest_relpath,
                    "staged_sha": committed_sha,
                    "has_prev": true,
                },
            ],
            "manifest": manifest_relpath,
        });
        for payload in [
            serde_json::json!({ "entries": "not-a-list", "manifest": manifest_relpath }),
            serde_json::json!({ "entries": [], "manifest": 7 }),
            coerced_bool_otherwise_valid,
            serde_json::json!({ "entries": [], "manifest": manifest_relpath, "extra": true }),
        ] {
            let journal = write_journal(&project, "revenue_daily", &payload);
            let error = recover_generation(&project, &parsed).expect_err("refused");
            assert_eq!(error.code, "staging-journal-corrupt", "{payload}");
            std::fs::remove_file(&journal).expect("cleanup");
        }
        assert_eq!(snapshot(&project), before);
    }

    #[test]
    fn journal_naming_a_foreign_manifest_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let project = seeded_project(dir.path());
        write_journal(
            &project,
            "revenue_daily",
            &serde_json::json!({
                "entries": [],
                "manifest": manifest_rel("another_product"),
            }),
        );
        let error = recover_generation(&project, &parsed_d3()).expect_err("refused");
        assert_eq!(error.code, "staging-journal-corrupt");
    }

    #[test]
    fn crash_during_a_cold_phase_a_removes_the_renamed_new_files() {
        // Added coverage (no answer-key counterpart): on a COLD project
        // every staged file is brand-new (`has_prev = false`), so rollback
        // has no backup to restore — it must instead REMOVE a renamed new
        // file whose bytes still match the journaled staged hash. A
        // mutation pass showed no ported test reached that branch: every
        // Phase-B drill replaces files that exist, so `has_prev` is always
        // true there.
        let dir = tempfile::tempdir().expect("tempdir");
        let project = seeded_project(dir.path());
        let parsed = parsed_d3();
        let lowering = lower_phase_a(&parsed, SPEC_PATH).expect("lowers");

        // Phase A commits 3 renames: journal (1), contract (2), manifest
        // (3). Fail at 3: the brand-new contract has already renamed into
        // place; the manifest (the marker) has not.
        let mut calls = 0usize;
        let mut rename = |src: &Path, dst: &Path| {
            calls += 1;
            if calls == 3 {
                return Err(std::io::Error::other(
                    "injected crash between staged renames",
                ));
            }
            std::fs::rename(src, dst)
        };
        let mut remove = |p: &Path| std::fs::remove_file(p);
        commit_generation_with_ops(
            &project,
            &parsed,
            &lowering,
            &mut CommitOps {
                rename: &mut rename,
                remove: &mut remove,
            },
        )
        .expect_err("the injected crash must surface");

        let contract = project.join("models/revenue_daily.contract.toml");
        assert!(contract.is_file(), "the new file renamed before the crash");
        assert_eq!(
            recover_generation(&project, &parsed).expect("recovers"),
            RecoveryAction::RolledBack
        );
        assert!(
            !contract.exists(),
            "an uncommitted brand-new file must be removed, not left behind"
        );
        assert_eq!(leftovers(&project), Vec::<String>::new());
        assert!(!manifest_path(&project).exists());
    }

    #[test]
    fn half_canonical_path_aliases_are_refused_as_unsafe() {
        // Added coverage (no answer-key counterpart): `a//b`, `a/./b`, and
        // `a/b/` are aliases `Path::components` silently normalizes, so
        // without the canonical-spelling gate they would flow onward and be
        // refused later under a DIFFERENT rule (the namespace check) — the
        // half-canonical-identity trap. The spelling gate refuses them as
        // unsafe paths before any resolution. A mutation pass showed no
        // ported test pinned this: the answer key's traversal cases also
        // trip the component check.
        let parsed = parsed_d3();
        for alias in [
            "models//revenue_daily.contract.toml",
            "models/./revenue_daily.contract.toml",
            "models/revenue_daily.contract.toml/",
        ] {
            let dir = tempfile::tempdir().expect("tempdir");
            let project = seeded_project(dir.path());
            write_journal(
                &project,
                "revenue_daily",
                &forged_journal_payload(&[serde_json::json!({
                    "final": alias,
                    "staged_sha": content_digest(b"x"),
                    "has_prev": false,
                })]),
            );
            let error = recover_generation(&project, &parsed).expect_err("refused");
            assert_eq!(error.code, "staging-journal-unsafe-path", "{alias}");
        }
    }

    // -------- the crash claim is process death, proved with a dead child ----

    /// Child half of the SIGKILL drill. Not a test: it runs only when the
    /// parent test spawns this binary with the env vars set, performs the
    /// real Phase-B protocol, and stalls immediately before the manifest
    /// rename (the commit marker) so the parent can SIGKILL a genuinely
    /// mid-protocol process. SIGKILL means no unwinding, no cleanup — the
    /// exact boundary the recovery protocol claims to cover.
    #[cfg(unix)]
    #[test]
    #[ignore = "helper for sigkilled_child_between_staged_renames_rolls_back; runs only when spawned"]
    fn sigkill_stall_child_helper() {
        let (Ok(project), Ok(spec_file), Ok(marker)) = (
            std::env::var("ROCKY_E1B_KILL_CHILD_PROJECT"),
            std::env::var("ROCKY_E1B_KILL_CHILD_SPEC"),
            std::env::var("ROCKY_E1B_KILL_CHILD_MARKER"),
        ) else {
            return;
        };
        let parsed = crate::product::spec::parse_spec_file(Path::new(&spec_file))
            .expect("the child reads the spec file the parent wrote");
        let marker = PathBuf::from(marker);
        let mut rename = |src: &Path, dst: &Path| {
            if dst.file_name().is_some_and(|n| n == MANIFEST_FILENAME) {
                std::fs::write(&marker, b"mid-protocol").expect("marker");
                std::thread::sleep(std::time::Duration::from_secs(300));
            }
            std::fs::rename(src, dst)
        };
        let mut remove = |p: &Path| std::fs::remove_file(p);
        let _ = run_phase_b_with_ops(
            Path::new(&project),
            SPEC_PATH,
            &parsed,
            &mut CommitOps {
                rename: &mut rename,
                remove: &mut remove,
            },
        );
    }

    #[cfg(unix)]
    #[test]
    fn sigkilled_child_between_staged_renames_rolls_back() {
        use std::os::unix::process::ExitStatusExt as _;

        // The crash-boundary honesty test: a REAL child process performing
        // the staged writes is SIGKILLed between the artifact renames and
        // the manifest rename; recovery in this (parent) process must roll
        // the uncommitted generation back to the exact pre-generation tree.
        let dir = tempfile::tempdir().expect("tempdir");
        let parsed = parsed_d3();
        let project = project_with_phase_a_and_draft(dir.path(), &parsed);
        let before = snapshot(&project);

        let spec_file = dir.path().join("revenue_daily.spec.toml");
        write_file(&spec_file, SPEC_FIXTURE);
        let marker = dir.path().join("mid-protocol");

        let exe = std::env::current_exe().expect("test binary");
        let mut child = std::process::Command::new(exe)
            .args([
                "product::commit::tests::sigkill_stall_child_helper",
                "--exact",
                "--ignored",
                "--nocapture",
            ])
            .env("ROCKY_E1B_KILL_CHILD_PROJECT", &project)
            .env("ROCKY_E1B_KILL_CHILD_SPEC", &spec_file)
            .env("ROCKY_E1B_KILL_CHILD_MARKER", &marker)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("spawn child");

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            if marker.exists() {
                break;
            }
            if let Some(status) = child.try_wait().expect("try_wait") {
                panic!("child exited before the stall point: {status:?}");
            }
            assert!(
                std::time::Instant::now() < deadline,
                "child never reached the stall point"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        child.kill().expect("SIGKILL");
        let status = child.wait().expect("wait");
        assert_eq!(status.signal(), Some(libc_sigkill()), "died by SIGKILL");

        let journal = journal_path(&project, "revenue_daily");
        assert!(journal.is_file(), "the dead child left its staging journal");
        assert_eq!(
            recover_generation(&project, &parsed).expect("recovers"),
            RecoveryAction::RolledBack
        );
        assert_eq!(
            snapshot(&project),
            before,
            "recovery must restore the pre-generation tree exactly"
        );
        assert_eq!(committed(&project).phase, ManifestPhase::LoweredContract);
    }

    /// SIGKILL's number without a libc dependency: the value is fixed (9)
    /// on every Unix Rocky targets.
    #[cfg(unix)]
    fn libc_sigkill() -> i32 {
        9
    }
}
