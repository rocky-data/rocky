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
///            +-- symlink_metadata FAILS with NotFound
///            |        nothing is at this path -> Absent  (ordinary case)
///            |
///            +-- symlink_metadata SUCCEEDS, or fails another way
///                     an entry IS at this path -> Present (refuse)
/// ```
///
/// Fail-closed by construction: every outcome other than a proven absence is
/// `Present`. A path whose parent directory is missing stays `Absent` — both
/// calls report `NotFound`, which is the healthy "not created yet" case.
///
/// The detail sentences are deliberately entry-neutral: the same helper backs
/// a file read and a directory read, so nothing here says "file".
pub fn classify_not_found(path: &Path) -> PathPresence {
    match std::fs::symlink_metadata(path) {
        // Nothing at this path. Genuinely absent — unchanged behaviour, and
        // the case every caller depends on.
        Err(stat_error) if stat_error.kind() == std::io::ErrorKind::NotFound => {
            PathPresence::Absent
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
pub fn entry_is_present(path: &Path) -> bool {
    std::fs::symlink_metadata(path).is_ok()
}

#[cfg(test)]
mod present_tests {
    use super::entry_is_present;

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
