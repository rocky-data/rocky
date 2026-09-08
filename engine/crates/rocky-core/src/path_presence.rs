//! Tell "nothing is at this path" apart from "an entry is here that cannot be
//! read".
//!
//! [`std::io::ErrorKind::NotFound`] covers both worlds and names neither. A
//! read of a symlink whose target is gone fails exactly like a read of a path
//! that was never created, so any caller that maps `NotFound` straight onto
//! "absent" reports a broken link as an empty world (#1668, #1707).
//!
//! [`std::fs::symlink_metadata`] stats the link itself instead of following
//! it, so it still succeeds for a dangling link. That is the discriminator.
//!
//! It is a discriminator for the LEAF. A dangling link one directory up makes
//! the leaf stat fail with `NotFound` too — nothing is at `proj/rocky.toml`
//! when `proj -> /gone` — and a leaf-only probe reports that as honest absence
//! (#1817). So a `NotFound` leaf is not the end of the question: the ancestors
//! are walked, and the first one that IS there decides. A missing ancestor is
//! the never-created chain and stays absent; a dangling or unreadable one is
//! not missing, it is broken, and the answer is a refusal.

use std::path::Path;

/// What a `NotFound` from an operation on a path actually means.
pub enum PathPresence {
    /// Nothing is at this path. The `NotFound` meant what it said, and the
    /// caller's ordinary "there is none of this yet" answer is correct.
    Absent,
    /// An entry IS at this path, so absence is unproven and the caller must
    /// refuse. `detail` says what was found, phrased to read after a
    /// "`<path>` cannot be read: " prefix.
    Present { detail: String },
}

/// Decide what a `NotFound` on `path` actually means.
///
/// ```text
///   read/read_dir(path) -> NotFound
///            |
///            +-- symlink_metadata(leaf) FAILS with NotFound
///            |        |
///            |        +-- walk UP: first ancestor that is there
///            |               a directory, or a link that resolves
///            |                   nothing below it was created -> Absent
///            |               a link that does NOT resolve, or unstatable
///            |                   the path is broken, not missing -> Present
///            |
///            +-- symlink_metadata(leaf) SUCCEEDS, or fails another way
///                     an entry IS at this path -> Present (refuse)
/// ```
///
/// Fail-closed by construction: every outcome other than a proven absence is
/// `Present`. A path whose parent directory is MISSING stays `Absent` — the
/// walk passes through a `NotFound` ancestor and keeps going, because a chain
/// that was never created is the healthy "not created yet" case. A parent that
/// is a dangling symlink is a different thing: an entry IS there, and the
/// leaf's `NotFound` came from following it into nothing (#1817).
///
/// The walk stops at the first ancestor that stats, so on the ordinary absent
/// path it costs one extra `symlink_metadata` on the parent directory.
///
/// The detail sentences are deliberately entry-neutral: the same helper backs
/// a file read and a directory read, so nothing here says "file".
pub fn classify_not_found(path: &Path) -> PathPresence {
    match std::fs::symlink_metadata(path) {
        // Nothing at this path itself. Whether that is ABSENCE depends on what
        // the path runs through — a dangling ancestor produces exactly this
        // `NotFound`, and it is not absence.
        Err(stat_error) if stat_error.kind() == std::io::ErrorKind::NotFound => {
            classify_ancestors(path)
        }
        // The path could not even be stat-ed, so absence is unproven.
        // Refusing is the fail-closed answer.
        Err(stat_error) => PathPresence::Present {
            detail: format!("the path could not be inspected: {stat_error}"),
        },
        // A symlink is here and the read followed it into nothing: dangling.
        // `read_link` names the immediate hop — the link as written, which is
        // what the operator has to go fix — so the message says the target
        // cannot be RESOLVED rather than claiming that one name is missing.
        Ok(metadata) if metadata.is_symlink() => PathPresence::Present {
            detail: match std::fs::read_link(path) {
                Ok(target) => format!(
                    "it is a symlink to '{}', which cannot be resolved",
                    target.display()
                ),
                Err(link_error) => {
                    format!("it is a symlink that cannot be resolved ({link_error})")
                }
            },
        },
        // An entry that is not a symlink is here, yet the read said it was
        // missing — it was replaced under us between the two calls. Not
        // absence either way.
        Ok(_) => PathPresence::Present {
            detail: "an entry exists at this path, but reading it reported nothing there"
                .to_string(),
        },
    }
}

/// The leaf is proven `NotFound`. Walk up until an ancestor answers.
///
/// The same walk `product::commit::contained_target` does to refuse a
/// dangling symlinked ancestor before a write, generalised: no project root
/// to stop at, and an ancestor that cannot be stat-ed at all is treated the
/// way the leaf is — absence unproven, so refuse.
///
/// ```text
///   ancestor symlink_metadata          verdict
///   ----------------------------       -------
///   Err(NotFound)                      keep walking: never-created chain
///   Err(other)                         Present: could not be inspected
///   Ok, symlink, canonicalize fails    Present: dangling ancestor
///   Ok, symlink, resolves              Absent:  a live link, nothing below it
///   Ok, directory                      Absent:  nothing below it was created
///   Ok, anything else                  Present: a non-directory where a
///                                               directory belongs
///   no ancestor left                   Absent
/// ```
///
/// The "anything else" row is belt and braces: reading through a regular file
/// reports `NotADirectory`, not `NotFound`, so a caller rarely reaches here
/// with one. It is kept because "rarely" is not "never", and the fail-closed
/// answer costs nothing.
fn classify_ancestors(path: &Path) -> PathPresence {
    let mut ancestor = path.parent();
    while let Some(dir) = ancestor {
        match std::fs::symlink_metadata(dir) {
            Err(stat_error) if stat_error.kind() == std::io::ErrorKind::NotFound => {
                ancestor = dir.parent();
            }
            Err(stat_error) => {
                return PathPresence::Present {
                    detail: format!(
                        "its ancestor directory '{}' could not be inspected: {stat_error}",
                        dir.display()
                    ),
                };
            }
            Ok(metadata) if metadata.is_symlink() => {
                return if dir.canonicalize().is_ok() {
                    PathPresence::Absent
                } else {
                    PathPresence::Present {
                        detail: match std::fs::read_link(dir) {
                            Ok(target) => format!(
                                "its ancestor directory '{}' is a symlink to '{}', which cannot \
                                 be resolved",
                                dir.display(),
                                target.display()
                            ),
                            Err(link_error) => format!(
                                "its ancestor directory '{}' is a symlink that cannot be \
                                 resolved ({link_error})",
                                dir.display()
                            ),
                        },
                    }
                };
            }
            Ok(metadata) if metadata.is_dir() => return PathPresence::Absent,
            Ok(_) => {
                return PathPresence::Present {
                    detail: format!("its ancestor '{}' is not a directory", dir.display()),
                };
            }
        }
    }
    PathPresence::Absent
}

/// Is there an entry at `path`, of any kind?
///
/// The replacement for [`Path::exists`] wherever a `false` would make a caller
/// treat a present file as absent and carry on. `exists()` is
/// `metadata(..).is_ok()`, and `metadata` FOLLOWS a symlink — so a file that is
/// a symlink to a deleted target answers `false`, and the caller silently takes
/// its "there is none of this" branch (#1738).
///
/// `symlink_metadata` stats the link itself, so a dangling link is `true` here.
/// The caller then attempts its read and gets the honest I/O error, instead of
/// compiling a model against `_defaults.toml` or writing through a broken link
/// that a guard was supposed to refuse.
///
/// Not for the `state.redb` gates: those treat an absent store as "no state
/// yet" and then do nothing, which is the CI-safe ephemeral-runner contract.
///
/// "Present" means **anything but a proven absence**. `symlink_metadata` can
/// fail for reasons other than `NotFound` — a parent that denies search — and
/// `.is_ok()` folded every one of those into `false`, which is the same
/// fail-open this helper exists to replace. A `NotFound` leaf goes through
/// [`classify_not_found`], so a dangling ancestor answers `true` here too and
/// the caller's read surfaces the honest error (#1817). Every caller of this
/// function propagates that read error rather than folding it, which is what
/// makes `true` the safe direction.
pub fn entry_is_present(path: &Path) -> bool {
    match std::fs::symlink_metadata(path) {
        Ok(_) => true,
        Err(stat_error) if stat_error.kind() == std::io::ErrorKind::NotFound => {
            matches!(classify_not_found(path), PathPresence::Present { .. })
        }
        Err(_) => true,
    }
}

#[cfg(test)]
mod classify_tests {
    use super::{PathPresence, classify_not_found};

    fn is_present(p: &std::path::Path) -> Option<String> {
        match classify_not_found(p) {
            PathPresence::Present { detail } => Some(detail),
            PathPresence::Absent => None,
        }
    }

    /// The #1817 case. `proj -> gone` and the leaf is `proj/rocky.toml`. A
    /// leaf-only probe answers `Absent`: the leaf stat fails with `NotFound`
    /// exactly as it would for a path nobody ever created.
    #[cfg(unix)]
    #[test]
    fn a_dangling_ancestor_is_present_not_absent() {
        let dir = tempfile::tempdir().unwrap();
        let proj = dir.path().join("proj");
        std::os::unix::fs::symlink(dir.path().join("gone"), &proj).unwrap();
        let leaf = proj.join("rocky.toml");
        assert!(
            std::fs::symlink_metadata(&leaf).is_err(),
            "precondition: the leaf stat fails, which is what the old probe trusted"
        );

        let detail = is_present(&leaf).expect("a broken path is not an absent one");
        assert!(
            detail.contains(&proj.display().to_string()) && detail.contains("cannot be resolved"),
            "the detail names the ancestor the operator has to fix: {detail}"
        );
    }

    /// Two levels up is the same defect; the walk must not stop at the parent.
    #[cfg(unix)]
    #[test]
    fn a_dangling_grandparent_is_present_too() {
        let dir = tempfile::tempdir().unwrap();
        let proj = dir.path().join("proj");
        std::os::unix::fs::symlink(dir.path().join("gone"), &proj).unwrap();
        let leaf = proj.join("products").join("revenue_daily.toml");
        let detail = is_present(&leaf).expect("present");
        assert!(detail.contains(&proj.display().to_string()), "{detail}");
    }

    /// The promise the module doc makes and every caller depends on: a chain
    /// that was never created is absence. Passing through a `NotFound`
    /// ancestor must keep walking, not refuse.
    #[test]
    fn a_never_created_chain_is_still_absent() {
        let dir = tempfile::tempdir().unwrap();
        let leaf = dir.path().join("never").join("created").join("here.toml");
        assert!(
            is_present(&leaf).is_none(),
            "a missing parent is the healthy case"
        );
    }

    /// A LIVE symlinked ancestor with nothing below it is honest absence — the
    /// link resolves, the file simply is not there. `models -> models_real`
    /// is the in-project shape this must keep working.
    #[cfg(unix)]
    #[test]
    fn a_live_symlinked_ancestor_with_an_absent_leaf_is_absent() {
        let dir = tempfile::tempdir().unwrap();
        let real = dir.path().join("models_real");
        std::fs::create_dir(&real).unwrap();
        let link = dir.path().join("models");
        std::os::unix::fs::symlink(&real, &link).unwrap();
        assert!(is_present(&link.join("orders.toml")).is_none());
    }

    /// An ancestor the process may not search. On Linux and macOS the LEAF
    /// stat already reports `PermissionDenied` rather than `NotFound`, so the
    /// existing "fails another way" arm refuses — but that mapping was the
    /// reviewer's least-confident point, so it is pinned here rather than
    /// assumed, and the walk's own EACCES arm covers a filesystem that masks
    /// it as `NotFound`. Skips when the process can stat through mode 000
    /// (root), because then the condition was never built.
    #[cfg(unix)]
    #[test]
    fn an_unsearchable_ancestor_is_present() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let locked = dir.path().join("locked");
        std::fs::create_dir(&locked).unwrap();
        let leaf = locked.join("rocky.toml");
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o000)).unwrap();

        let reproduced = std::fs::symlink_metadata(&leaf).is_err();
        let verdict = is_present(&leaf);
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755)).ok();

        if !reproduced {
            eprintln!("skipping: this process can stat through a mode-000 directory");
            return;
        }
        assert!(
            verdict.is_some(),
            "an ancestor that denies search is not absence"
        );
    }
}

#[cfg(test)]
mod present_tests {
    use super::entry_is_present;

    /// #1817: the sidecar probe must answer `true` under a dangling ancestor,
    /// so the loader's read surfaces the error instead of compiling against
    /// `_defaults.toml`.
    #[cfg(unix)]
    #[test]
    fn a_leaf_under_a_dangling_ancestor_is_present() {
        let dir = tempfile::tempdir().unwrap();
        let proj = dir.path().join("proj");
        std::os::unix::fs::symlink(dir.path().join("gone"), &proj).unwrap();
        assert!(entry_is_present(&proj.join("orders.toml")));
    }

    #[test]
    fn a_dangling_symlink_is_present_where_exists_says_otherwise() {
        let dir = tempfile::tempdir().unwrap();
        let link = dir.path().join("orders.toml");
        std::os::unix::fs::symlink(dir.path().join("gone"), &link).unwrap();

        assert!(
            !link.exists(),
            "precondition: Path::exists follows the link and answers false"
        );
        assert!(
            entry_is_present(&link),
            "a file IS at that path; treating it as absent is the defect"
        );
    }

    #[test]
    fn a_path_with_nothing_at_it_is_absent() {
        let dir = tempfile::tempdir().unwrap();
        assert!(!entry_is_present(&dir.path().join("never-created.toml")));
    }

    #[test]
    fn an_ordinary_file_is_present() {
        let dir = tempfile::tempdir().unwrap();
        let f = dir.path().join("orders.toml");
        std::fs::write(&f, "name = \"orders\"\n").unwrap();
        assert!(entry_is_present(&f));
    }
}
