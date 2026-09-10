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

use std::path::{Path, PathBuf};

/// What one filesystem entry is, as far as this module needs to know.
///
/// Not [`std::fs::Metadata`]: that type has no public constructor, so a test
/// double cannot produce one. Three variants is everything the classification
/// below branches on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryKind {
    /// A symlink, inspected without following it.
    Symlink,
    /// A directory.
    Directory,
    /// Anything else — a regular file, a socket, a device.
    Other,
}

/// The four filesystem questions this module asks, behind a seam.
///
/// Every review of the path-probe class was read-only static tracing, because
/// the states it exists for cannot be produced on a CI runner: an ancestor
/// that denies search on a filesystem reporting it as `NotFound`, and an entry
/// replaced between two probes. Both are now expressible against a scripted
/// implementation (#1827).
///
/// The methods are deliberately one-to-one with a `std::fs` call, so
/// [`RealFs`] is a transcription and the double cannot drift from it by
/// modelling something the real one does not do.
pub trait FsProbe {
    /// [`std::fs::symlink_metadata`] — stats the entry itself, never the
    /// target of a link.
    fn entry_kind(&self, path: &Path) -> std::io::Result<EntryKind>;
    /// [`std::fs::read_link`] — the immediate hop a link names.
    fn read_link(&self, path: &Path) -> std::io::Result<PathBuf>;
    /// Whether [`Path::canonicalize`] succeeds. The question is only ever
    /// "does this resolve", so the resolved path is not returned.
    fn resolves(&self, path: &Path) -> bool;
    /// `symlink_metadata(dir/".")` — succeeds iff this process may look inside
    /// `dir`. See [`searchable_or_present`] for why `dir/.` is the probe.
    fn is_searchable(&self, dir: &Path) -> std::io::Result<()>;
}

/// The real filesystem. The only implementation outside tests.
pub struct RealFs;

impl FsProbe for RealFs {
    fn entry_kind(&self, path: &Path) -> std::io::Result<EntryKind> {
        let metadata = std::fs::symlink_metadata(path)?;
        Ok(if metadata.is_symlink() {
            EntryKind::Symlink
        } else if metadata.is_dir() {
            EntryKind::Directory
        } else {
            EntryKind::Other
        })
    }

    fn read_link(&self, path: &Path) -> std::io::Result<PathBuf> {
        std::fs::read_link(path)
    }

    fn resolves(&self, path: &Path) -> bool {
        path.canonicalize().is_ok()
    }

    fn is_searchable(&self, dir: &Path) -> std::io::Result<()> {
        std::fs::symlink_metadata(dir.join(".")).map(|_| ())
    }
}

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
    classify_not_found_with(path, &RealFs)
}

/// [`classify_not_found`] against a supplied [`FsProbe`].
///
/// The whole classification runs on the seam, so a scripted probe can produce
/// the states a CI runner cannot — including a different answer on a later
/// call, which is how the replace-between-probes race becomes testable.
pub fn classify_not_found_with(path: &Path, fs: &dyn FsProbe) -> PathPresence {
    // A trailing separator makes the OS resolve the last component AS A
    // DIRECTORY, so `symlink_metadata("models/")` follows the link it was
    // supposed to inspect and reports a dangling one as `NotFound` — the
    // exact answer this function exists to second-guess. `components()`
    // drops the trailing separator (and any `.` segments) without touching
    // the filesystem, so the stat below lands on the entry itself.
    let path = path.components().as_path();
    match fs.entry_kind(path) {
        // Nothing at this path itself. Whether that is ABSENCE depends on what
        // the path runs through — a dangling ancestor produces exactly this
        // `NotFound`, and it is not absence.
        Err(stat_error) if stat_error.kind() == std::io::ErrorKind::NotFound => {
            classify_ancestors(path, fs)
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
        Ok(EntryKind::Symlink) => PathPresence::Present {
            detail: match fs.read_link(path) {
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
        Ok(EntryKind::Directory | EntryKind::Other) => PathPresence::Present {
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
///   Ok, symlink, resolves              Absent if searchable, else Present
///   Ok, directory                      Absent if searchable, else Present
///   Ok, anything else                  Present: a non-directory where a
///                                               directory belongs
///   no ancestor left                   Absent
/// ```
///
/// The "anything else" row is belt and braces: reading through a regular file
/// reports `NotADirectory`, not `NotFound`, so a caller rarely reaches here
/// with one. It is kept because "rarely" is not "never", and the fail-closed
/// answer costs nothing.
fn classify_ancestors(path: &Path, fs: &dyn FsProbe) -> PathPresence {
    let mut ancestor = path.parent();
    while let Some(dir) = ancestor {
        // `Path::parent("rocky.toml")` is `Some("")`, and a stat of `""` is
        // `NotFound` — so a bare relative leaf walked straight off the end
        // and was called absent without the working directory ever being
        // probed. The empty parent IS the working directory; name it.
        let dir = if dir.as_os_str().is_empty() {
            Path::new(".")
        } else {
            dir
        };
        match fs.entry_kind(dir) {
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
            Ok(EntryKind::Symlink) => {
                return if fs.resolves(dir) {
                    searchable_or_present(dir, fs)
                } else {
                    PathPresence::Present {
                        detail: match fs.read_link(dir) {
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
            Ok(EntryKind::Directory) => return searchable_or_present(dir, fs),
            Ok(EntryKind::Other) => {
                return PathPresence::Present {
                    detail: format!("its ancestor '{}' is not a directory", dir.display()),
                };
            }
        }
    }
    PathPresence::Absent
}

/// The first live ancestor is a directory. Nothing below it stats — but is
/// that because nothing is there, or because this process cannot look?
///
/// Statting the directory ENTRY only needs search permission on its parent.
/// Statting `dir/.` needs search permission on `dir` itself, which is the
/// permission a lookup inside it needs. So `dir/.` is the portable probe for
/// "can this process see into here": it succeeds on every healthy directory
/// and fails — however the platform spells the failure — on one that denies
/// search. On Linux and macOS the leaf stat already reports `PermissionDenied`
/// for that case and never reaches the walk; this probe is for a filesystem
/// that masks the lookup failure as `NotFound`, which is the one shape the
/// walk would otherwise read as absence.
///
/// What this does not close: a platform whose `dir/.` normalises back to
/// `dir` before the check (Windows does) answers "searchable" for any
/// directory that exists. There the masked case stays open; it is recorded
/// here rather than claimed closed.
fn searchable_or_present(dir: &Path, fs: &dyn FsProbe) -> PathPresence {
    match fs.is_searchable(dir) {
        Ok(()) => PathPresence::Absent,
        Err(probe_error) => PathPresence::Present {
            detail: format!(
                "its ancestor directory '{}' exists but cannot be searched: {probe_error}",
                dir.display()
            ),
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
///
/// "Present" means **anything but a proven absence**. `symlink_metadata` can
/// fail for reasons other than `NotFound` — a parent that denies search — and
/// `.is_ok()` folded every one of those into `false`, which is the same
/// fail-open this helper exists to replace. A `NotFound` leaf goes through
/// [`classify_not_found`], so a dangling ancestor answers `true` here too and
/// the caller's read surfaces the honest error (#1817). Every caller of this
/// function either propagates that read error or refuses outright on `true`
/// (`rocky init` refuses to scaffold over it) — none folds it — which is what
/// makes `true` the safe direction.
pub fn entry_is_present(path: &Path) -> bool {
    entry_is_present_with(path, &RealFs)
}

/// [`entry_is_present`] against a supplied [`FsProbe`] (#1827).
pub fn entry_is_present_with(path: &Path, fs: &dyn FsProbe) -> bool {
    // No trailing-separator normalisation here, on purpose: a dangling link
    // spelled `models/` stats as `NotFound` and falls through to
    // `classify_not_found`, which normalises before it decides. Doing it
    // twice would let a reviewer believe this function guards something it
    // does not.
    match fs.entry_kind(path) {
        Ok(_) => true,
        Err(stat_error) if stat_error.kind() == std::io::ErrorKind::NotFound => {
            matches!(
                classify_not_found_with(path, fs),
                PathPresence::Present { .. }
            )
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

    /// A bare relative leaf — `rocky.toml`, the CLI default — has `parent()`
    /// `Some("")`. A stat of `""` is `NotFound`, so the walk used to run off
    /// the end and call the leaf absent without probing the working directory
    /// at all. It cannot chmod the test's own cwd, so this pins the reachable
    /// half: the walk lands on `.`, which is searchable, and answers Absent
    /// for a leaf that is genuinely not there — rather than tripping over the
    /// empty parent. The mode-000 half is the same probe the absolute-path
    /// test exercises.
    #[test]
    fn a_bare_relative_leaf_walks_to_the_working_directory() {
        let leaf = std::path::Path::new("rocky-never-created-9f1c.toml");
        assert!(
            std::path::Path::new("").parent().is_none()
                && leaf.parent() == Some(std::path::Path::new("")),
            "precondition: the parent of a bare leaf is the empty path"
        );
        assert!(
            is_present(leaf).is_none(),
            "a leaf that is not in a searchable working directory is absent"
        );
    }

    /// The fix-sensitive half of the bare-relative case, in a CHILD PROCESS
    /// so it can own its working directory: the child chdirs into a temp
    /// directory, removes search permission from it, and asks the walk about
    /// a bare `rocky.toml`. Old code stat-ed the empty parent, got NotFound,
    /// and answered Absent; the walk now lands on `.`, whose stat fails with
    /// PermissionDenied, and refuses. The parent asserts on the child's
    /// printed verdict. Skips (via the child) under root, which can search a
    /// mode-000 directory.
    #[cfg(unix)]
    #[test]
    fn a_bare_relative_leaf_under_an_unsearchable_cwd_is_present() {
        use std::os::unix::fs::PermissionsExt;

        const CHILD: &str = "ROCKY_PATH_PRESENCE_CHILD";
        if std::env::var_os(CHILD).is_some() {
            // ---- child ----
            let dir = tempfile::tempdir().unwrap();
            let home = std::env::current_dir().unwrap();
            std::env::set_current_dir(dir.path()).unwrap();
            std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o000)).unwrap();
            // The gate is the PRODUCTION probe, not a look-alike: `stat(".")`
            // needs no search permission on Linux (nothing leads to it), so
            // gating on it skipped the whole test on the CI runner. `./.`
            // puts the working directory in the path prefix, which is what
            // `searchable_or_present` relies on; it fails without search
            // permission on every Unix, and succeeds under root — a skip.
            let reproduced = std::fs::symlink_metadata("./.").is_err();
            let verdict =
                super::classify_ancestors(std::path::Path::new("rocky.toml"), &super::RealFs);
            std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755)).ok();
            std::env::set_current_dir(home).ok();
            if !reproduced {
                println!("CHILD_VERDICT=skip");
                return;
            }
            match verdict {
                PathPresence::Present { .. } => println!("CHILD_VERDICT=present"),
                PathPresence::Absent => println!("CHILD_VERDICT=absent"),
            }
            return;
        }
        // ---- parent ----
        let exe = std::env::current_exe().unwrap();
        let output = std::process::Command::new(exe)
            .args([
                "--exact",
                "path_presence::classify_tests::a_bare_relative_leaf_under_an_unsearchable_cwd_is_present",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .output()
            .expect("spawn the child test");
        let stdout = String::from_utf8_lossy(&output.stdout);
        let verdict = stdout
            .lines()
            .find_map(|l| l.strip_prefix("CHILD_VERDICT="))
            .unwrap_or_else(|| panic!("the child printed no verdict; stdout:\n{stdout}"));
        match verdict {
            "skip" => eprintln!("skipping: the child can search a mode-000 directory (root)"),
            "present" => {}
            other => panic!(
                "a bare leaf under an unsearchable working directory must be PRESENT, the \
                 walk answered {other} — the empty-parent hole"
            ),
        }
    }

    /// `models/` — the spelling a glob base produces. With the trailing
    /// separator the OS resolves the last component as a directory, so a
    /// leaf stat FOLLOWS the link and a dangling one reads as `NotFound`; the
    /// walk then finds a healthy parent and calls it absent. Found by
    /// `locate_models_dir`'s test, where the fix was invisible until the
    /// separator was stripped.
    #[cfg(unix)]
    #[test]
    fn a_dangling_symlink_spelled_with_a_trailing_separator_is_present() {
        let dir = tempfile::tempdir().unwrap();
        let link = dir.path().join("models");
        std::os::unix::fs::symlink(dir.path().join("gone"), &link).unwrap();
        let mut spelled = link.into_os_string();
        spelled.push("/");
        let with_slash = std::path::PathBuf::from(spelled);
        assert!(
            std::fs::symlink_metadata(&with_slash).is_err(),
            "precondition: the trailing separator makes the stat follow the link"
        );
        assert!(is_present(&with_slash).is_some(), "the entry IS there");
        assert!(super::entry_is_present(&with_slash));
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

    /// The walk's own answer for an unsearchable ancestor, reached DIRECTLY —
    /// bypassing the leaf stat that, on Linux and macOS, reports
    /// `PermissionDenied` and never lets the walk run. This is the shape a
    /// filesystem that masks the lookup failure as `NotFound` would produce:
    /// the leaf says "nothing here", the walk stats `locked` fine (that needs
    /// search on its PARENT), and only the `locked/.` probe can tell that the
    /// process cannot see inside. Reverting the probe makes this fail.
    /// Skips when the process can stat through mode 000 (root).
    #[cfg(unix)]
    #[test]
    fn the_walk_refuses_an_unsearchable_ancestor_even_when_the_leaf_says_not_found() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let locked = dir.path().join("locked");
        std::fs::create_dir(&locked).unwrap();
        let leaf = locked.join("rocky.toml");
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o000)).unwrap();

        let reproduced = std::fs::symlink_metadata(locked.join(".")).is_err();
        let verdict = super::classify_ancestors(&leaf, &super::RealFs);
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755)).ok();

        if !reproduced {
            eprintln!("skipping: this process can search a mode-000 directory");
            return;
        }
        match verdict {
            PathPresence::Present { detail } => assert!(
                detail.contains("cannot be searched"),
                "the walk must say WHY it refused: {detail}"
            ),
            PathPresence::Absent => panic!(
                "the walk stat-ed `locked` and called everything below it absent — the \
                 masked-NotFound hole (#1822 review, finding 1)"
            ),
        }
    }

    /// The leaf-stat path for the same condition, kept as a pin: on Linux and
    /// macOS the LEAF reports `PermissionDenied`, and the "fails another way"
    /// arm refuses before the walk. Not fix-sensitive for the walk — the test
    /// above is — but it pins the mapping the walk's comment relies on.
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

    // Unix-only: the fixture is a dangling SYMLINK, and
    // `std::os::unix::fs::symlink` does not exist on Windows — where
    // `cargo check --all-targets` compiles this test and fails on it.
    #[cfg(unix)]
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

/// The states a CI runner cannot produce, produced (#1827).
///
/// Every review of this class — four original lanes and nine rounds on #1822 —
/// was read-only static tracing, and every one of them asked for this. The
/// scripted probe below answers from a table the test writes, so a filesystem
/// that masks `EACCES` as `NotFound`, and an entry that changes between two
/// probes, are both ordinary test inputs.
#[cfg(test)]
mod scripted_probe_tests {
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::io::{Error, ErrorKind};
    use std::path::{Path, PathBuf};

    use super::{EntryKind, FsProbe, PathPresence, classify_not_found_with, entry_is_present_with};

    /// One entry's scripted answers. `None` for `kind` means the path is not
    /// there — the `NotFound` the whole module exists to second-guess.
    #[derive(Clone, Default)]
    struct Entry {
        kind: Option<EntryKind>,
        link_target: Option<PathBuf>,
        resolves: bool,
        searchable: bool,
        /// When set, `entry_kind` fails with this kind instead of answering.
        stat_error: Option<ErrorKind>,
    }

    /// A filesystem the test writes out in full.
    ///
    /// `swap_after` is the instrument the round-nine review asked for and could
    /// not build: after `entry_kind` has been asked about a path N times, the
    /// answer changes. That is the walk-then-swap race, made deterministic.
    #[derive(Default)]
    struct ScriptedFs {
        entries: HashMap<PathBuf, Entry>,
        swap_after: RefCell<HashMap<PathBuf, (usize, Entry)>>,
        seen: RefCell<HashMap<PathBuf, usize>>,
    }

    impl ScriptedFs {
        fn with(mut self, path: &str, entry: Entry) -> Self {
            self.entries.insert(PathBuf::from(path), entry);
            self
        }

        /// After `path` has been stat-ed `after` times, it becomes `entry`.
        fn swapping(self, path: &str, after: usize, entry: Entry) -> Self {
            self.swap_after
                .borrow_mut()
                .insert(PathBuf::from(path), (after, entry));
            self
        }

        fn lookup(&self, path: &Path) -> Entry {
            let count = {
                let mut seen = self.seen.borrow_mut();
                let c = seen.entry(path.to_path_buf()).or_insert(0);
                *c += 1;
                *c
            };
            if let Some((after, replacement)) = self.swap_after.borrow().get(path)
                && count > *after
            {
                return replacement.clone();
            }
            self.entries.get(path).cloned().unwrap_or_default()
        }
    }

    impl FsProbe for ScriptedFs {
        fn entry_kind(&self, path: &Path) -> std::io::Result<EntryKind> {
            let entry = self.lookup(path);
            if let Some(kind) = entry.stat_error {
                return Err(Error::new(kind, "scripted"));
            }
            entry
                .kind
                .ok_or_else(|| Error::new(ErrorKind::NotFound, "scripted"))
        }

        fn read_link(&self, path: &Path) -> std::io::Result<PathBuf> {
            self.lookup(path)
                .link_target
                .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "not a link"))
        }

        fn resolves(&self, path: &Path) -> bool {
            self.lookup(path).resolves
        }

        fn is_searchable(&self, dir: &Path) -> std::io::Result<()> {
            if self.lookup(dir).searchable {
                Ok(())
            } else {
                Err(Error::new(ErrorKind::PermissionDenied, "scripted"))
            }
        }
    }

    fn detail(verdict: PathPresence) -> Option<String> {
        match verdict {
            PathPresence::Present { detail } => Some(detail),
            PathPresence::Absent => None,
        }
    }

    /// The shape the module's own doc records as the one it cannot close on a
    /// real runner: a directory that EXISTS but denies search, on a filesystem
    /// that reports the masked lookup as `NotFound` rather than
    /// `PermissionDenied`.
    ///
    /// Without the `dir/.` probe this reads as honest absence, which is the
    /// fail-open this module exists to replace.
    #[test]
    fn an_ancestor_that_denies_search_is_present_not_absent() {
        let fs = ScriptedFs::default().with(
            "/p/models",
            Entry {
                kind: Some(EntryKind::Directory),
                searchable: false,
                ..Entry::default()
            },
        );

        let d = detail(classify_not_found_with(
            Path::new("/p/models/orders.sql"),
            &fs,
        ))
        .expect("a directory that cannot be searched does not prove absence");
        assert!(
            d.contains("/p/models") && d.contains("cannot be searched"),
            "the detail names the directory and the reason: {d}"
        );
    }

    /// The control for the test above. Same directory, same `NotFound` leaf —
    /// searchable this time, so the answer must be `Absent`. Without this, an
    /// implementation that refused everything would pass.
    #[test]
    fn a_searchable_ancestor_still_proves_absence() {
        let fs = ScriptedFs::default().with(
            "/p/models",
            Entry {
                kind: Some(EntryKind::Directory),
                searchable: true,
                ..Entry::default()
            },
        );

        assert!(
            detail(classify_not_found_with(
                Path::new("/p/models/orders.sql"),
                &fs
            ))
            .is_none(),
            "a healthy directory with nothing in it IS absence"
        );
    }

    /// **The gap the round-nine reviewer named verbatim**: *"There is no
    /// deterministic hook testing replacement after the walk but before the
    /// pre-check."*
    ///
    /// `classify_ancestors` stats the ancestor, sees a healthy directory, and
    /// then asks `is_searchable` about it. Between those two calls the entry is
    /// replaced. The module must not answer `Absent` on the strength of a stat
    /// that is already stale.
    ///
    /// It does not, and now that is tested rather than argued. This is the
    /// whole reason the seam exists — the swap is one call apart, so no real
    /// filesystem can be driven into it on a CI runner.
    #[test]
    fn an_ancestor_replaced_after_the_walk_but_before_the_search_probe_refuses() {
        let fs = ScriptedFs::default()
            .with(
                "/p/models",
                Entry {
                    kind: Some(EntryKind::Directory),
                    searchable: true,
                    ..Entry::default()
                },
            )
            // Lookup 1 is the ancestor stat; lookup 2 is the search probe.
            .swapping(
                "/p/models",
                1,
                Entry {
                    kind: Some(EntryKind::Symlink),
                    link_target: Some(PathBuf::from("/gone")),
                    resolves: false,
                    searchable: false,
                    ..Entry::default()
                },
            );

        let d = detail(classify_not_found_with(
            Path::new("/p/models/orders.sql"),
            &fs,
        ))
        .expect("a stat that is already stale must not prove absence");
        assert!(
            d.contains("/p/models") && d.contains("cannot be searched"),
            "the refusal comes from the probe that saw the NEW state: {d}"
        );
    }

    /// The same swap one pass later, which is the ordinary TOCTOU shape: a
    /// classification completes cleanly, the entry is replaced, and the next
    /// classification sees the replacement.
    ///
    /// The precondition matters — without it, a swap landing early would make
    /// the second assertion pass for the wrong reason, which is exactly what
    /// happened while writing this.
    #[test]
    fn an_ancestor_replaced_between_two_classifications_refuses_the_second() {
        let fs = ScriptedFs::default()
            .with(
                "/p/models",
                Entry {
                    kind: Some(EntryKind::Directory),
                    searchable: true,
                    ..Entry::default()
                },
            )
            // Two lookups per pass (ancestor stat + search probe), so the
            // first pass completes on the healthy entry.
            .swapping(
                "/p/models",
                2,
                Entry {
                    kind: Some(EntryKind::Symlink),
                    link_target: Some(PathBuf::from("/gone")),
                    resolves: false,
                    ..Entry::default()
                },
            );

        assert!(
            detail(classify_not_found_with(
                Path::new("/p/models/orders.sql"),
                &fs
            ))
            .is_none(),
            "PRECONDITION: the first pass must see only the healthy entry, \
             else the swap lands early and the assertion below proves nothing"
        );

        let d = detail(classify_not_found_with(
            Path::new("/p/models/orders.sql"),
            &fs,
        ))
        .expect("the replaced ancestor is a broken path, not an absent one");
        assert!(
            d.contains("/gone") && d.contains("cannot be resolved"),
            "the detail names the link's target: {d}"
        );
    }

    /// An ancestor that cannot be stat-ed for a reason other than `NotFound`
    /// is unproven absence, and refusing is the fail-closed answer.
    #[test]
    fn an_unstatable_ancestor_is_present() {
        let fs = ScriptedFs::default().with(
            "/p/models",
            Entry {
                stat_error: Some(ErrorKind::PermissionDenied),
                ..Entry::default()
            },
        );

        let d = detail(classify_not_found_with(
            Path::new("/p/models/orders.sql"),
            &fs,
        ))
        .expect("a directory that cannot be inspected does not prove absence");
        assert!(d.contains("could not be inspected"), "{d}");
    }

    /// A chain nobody ever created stays `Absent` — the walk passes THROUGH a
    /// `NotFound` ancestor rather than stopping at it. This is the healthy
    /// case, and the one an over-strict fix would break.
    #[test]
    fn a_never_created_chain_is_absent() {
        let fs = ScriptedFs::default().with(
            "/p",
            Entry {
                kind: Some(EntryKind::Directory),
                searchable: true,
                ..Entry::default()
            },
        );

        assert!(
            detail(classify_not_found_with(
                Path::new("/p/models/orders.sql"),
                &fs
            ))
            .is_none(),
            "`/p/models` was never created, so nothing below it exists"
        );
    }

    /// `entry_is_present` runs on the same seam, so the masked-search case
    /// answers `true` there too — which is what makes the caller surface the
    /// honest read error instead of taking its "there is none of this" branch.
    #[test]
    fn entry_is_present_follows_the_same_verdict() {
        let fs = ScriptedFs::default().with(
            "/p/models",
            Entry {
                kind: Some(EntryKind::Directory),
                searchable: false,
                ..Entry::default()
            },
        );

        assert!(entry_is_present_with(
            Path::new("/p/models/_defaults.toml"),
            &fs
        ));
    }
}
