//! The one recursive walk over a models tree (#1262).
//!
//! Every scanner that enumerates a models directory must see the same set of
//! directories, or the surfaces disagree silently — `list models` showing a
//! model `run` will not build was exactly the defect family this closes. The
//! traversal therefore lives here, below every consumer: the CLI loader,
//! the compiler's model loading, and the per-directory sidecar loaders in
//! this file all walk through this function rather than each reading one
//! level (or none) on their own.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

/// How deep below the models root the walk descends.
///
/// Cycles are broken by the visited set, so this is not what makes the walk
/// terminate — it is a ceiling on nesting no real project reaches. Exceeding
/// it is an **error**, never a silent stop: quietly truncating the walk is
/// the exact failure recursing exists to close (#1262).
pub const MAX_MODELS_TREE_DEPTH: usize = 64;

/// One directory the walk could not fully read, or a subtree it refused.
///
/// Never silently dropped: a caller that ignores these reintroduces the
/// silent-drop family one seam over. Strict callers fail on the first one;
/// tolerant callers surface them beside the partial result.
#[derive(Debug, thiserror::Error)]
pub enum ModelWalkError {
    #[error("failed to read models directory {dir}: {source}")]
    ReadDir {
        dir: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to read an entry of models directory {dir}: {source}")]
    DirEntry {
        dir: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error(
        "models directory '{dir}' nests deeper than the {limit}-level limit; \
         models below it were not loaded"
    )]
    DepthCeiling { dir: PathBuf, limit: usize },
}

/// Every directory of the models tree under `root`, pre-order.
///
/// Pre-order depth-first over an explicit stack — not a recursive function,
/// so a pathologically deep tree cannot overflow the thread stack. Children
/// are pushed in reverse sorted order so they pop in sorted order, making
/// both the eventual model order and the "which broken directory is reported
/// first" answer independent of `read_dir`'s arbitrary ordering.
///
/// `is_dir` follows symlinks, and always has: a `models/staging` symlinked at
/// a shared directory loads today and must keep loading. That is also how a
/// cycle gets in, so each directory is entered at most once, keyed on its
/// resolved identity; a directory that cannot be canonicalized falls back to
/// its own path — it is loaded today, so failing to resolve it must not drop
/// it. Hidden (dot) directories were never excluded and are not excluded now;
/// adding an exclusion here would be a fresh silent drop of exactly the kind
/// this walk closes.
///
/// The root is always yielded first, even when absent — per-directory loaders
/// treat an absent directory as empty, and several callers rely on that
/// (`rocky docs` on a project with no models, for one).
pub fn walk_model_dirs(root: &Path) -> (Vec<PathBuf>, Vec<ModelWalkError>) {
    let mut dirs = Vec::new();
    let mut errors = Vec::new();
    let mut visited: HashSet<PathBuf> = HashSet::new();
    visited.insert(visit_key(root));

    let mut stack: Vec<(PathBuf, usize)> = vec![(root.to_path_buf(), 0)];
    while let Some((dir, depth)) = stack.pop() {
        dirs.push(dir.clone());

        // Only descend when there is something to descend into, so an absent
        // root stays a non-error rather than failing `read_dir`.
        if !dir.is_dir() {
            continue;
        }

        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(source) => {
                errors.push(ModelWalkError::ReadDir { dir, source });
                continue;
            }
        };
        let mut subdirs: Vec<PathBuf> = Vec::new();
        for entry in entries {
            // NOT `entries.flatten()`. A per-entry `io::Error` dropped there
            // is a directory that vanishes with nothing said about it — the
            // silent drop this walk exists to close, reintroduced one level
            // down.
            match entry {
                Ok(entry) if entry.path().is_dir() => subdirs.push(entry.path()),
                Ok(_) => {}
                Err(source) => errors.push(ModelWalkError::DirEntry {
                    dir: dir.clone(),
                    source,
                }),
            }
        }
        subdirs.sort();

        // Selected WITHOUT marking anything visited, so the depth check below
        // sees only genuinely-unvisited children. Marking first would both
        // report a ceiling breach for a subtree that is nothing but a cycle
        // back to an already-loaded directory, and mark directories visited
        // that are then abandoned — a later, legitimately shallow path to one
        // of them would be skipped with no error at all. `batch` dedups two
        // siblings that alias one directory, which the mark-as-you-go form
        // got for free.
        let mut batch: HashSet<PathBuf> = HashSet::new();
        let fresh: Vec<(PathBuf, PathBuf)> = subdirs
            .into_iter()
            .filter_map(|subdir| {
                let key = visit_key(&subdir);
                (!visited.contains(&key) && batch.insert(key.clone())).then_some((subdir, key))
            })
            .collect();
        if fresh.is_empty() {
            continue;
        }

        if depth + 1 > MAX_MODELS_TREE_DEPTH {
            errors.push(ModelWalkError::DepthCeiling {
                dir,
                limit: MAX_MODELS_TREE_DEPTH,
            });
            continue;
        }

        visited.extend(fresh.iter().map(|(_, key)| key.clone()));
        stack.extend(
            fresh
                .into_iter()
                .rev()
                .map(|(subdir, _)| (subdir, depth + 1)),
        );
    }
    (dirs, errors)
}

/// The identity a directory is deduplicated on while walking.
///
/// Canonical where possible so two paths reaching one directory — a symlink
/// and its target, or a symlink pointing back at an ancestor — are entered
/// once.
fn visit_key(dir: &Path) -> PathBuf {
    dir.canonicalize().unwrap_or_else(|_| dir.to_path_buf())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk(root: &Path, rel: &str) {
        std::fs::create_dir_all(root.join(rel)).unwrap();
    }

    /// Depths 0 through 3 are all enumerated, pre-order, children sorted —
    /// so the order is a contract, not an accident of `read_dir`.
    #[test]
    fn walks_every_depth_in_sorted_pre_order() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("models");
        mk(&root, "b/deep/deeper");
        mk(&root, "a");
        let (dirs, errors) = walk_model_dirs(&root);
        assert!(errors.is_empty(), "{errors:?}");
        let rel: Vec<String> = dirs
            .iter()
            .map(|d| {
                d.strip_prefix(&root)
                    .unwrap()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        assert_eq!(rel, ["", "a", "b", "b/deep", "b/deep/deeper"]);
    }

    /// An absent root is yielded (loaders treat it as empty) with no error.
    #[test]
    fn an_absent_root_is_yielded_without_error() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("does-not-exist");
        let (dirs, errors) = walk_model_dirs(&root);
        assert_eq!(dirs, vec![root]);
        assert!(errors.is_empty(), "{errors:?}");
    }

    /// Hidden directories were never excluded and stay walked — an exclusion
    /// here would be a fresh silent drop of exactly the kind this closes.
    #[test]
    fn hidden_directories_are_walked_at_every_depth() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("models");
        mk(&root, ".archive/nested");
        let (dirs, errors) = walk_model_dirs(&root);
        assert!(errors.is_empty(), "{errors:?}");
        assert_eq!(dirs.len(), 3, "{dirs:?}");
    }

    /// A symlinked subdirectory is followed — it always was — and a symlink
    /// cycle back to an ancestor terminates, entering each directory once.
    #[cfg(unix)]
    #[test]
    fn a_symlink_cycle_terminates_and_enters_each_directory_once() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("models");
        mk(&root, "staging");
        std::os::unix::fs::symlink(&root, root.join("staging").join("loop")).unwrap();
        let (dirs, errors) = walk_model_dirs(&root);
        assert!(errors.is_empty(), "{errors:?}");
        assert_eq!(dirs.len(), 2, "cycle must not re-enter: {dirs:?}");
    }

    /// Two sibling names aliasing one directory (a symlink beside its target)
    /// enter it once.
    #[cfg(unix)]
    #[test]
    fn sibling_aliases_of_one_directory_enter_it_once() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("models");
        mk(&root, "real");
        std::os::unix::fs::symlink(root.join("real"), root.join("alias")).unwrap();
        let (dirs, errors) = walk_model_dirs(&root);
        assert!(errors.is_empty(), "{errors:?}");
        assert_eq!(dirs.len(), 2, "alias must not double-enter: {dirs:?}");
    }

    /// Nesting past the ceiling is an error naming the directory — never a
    /// silent stop.
    #[test]
    fn nesting_past_the_ceiling_is_an_error() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("models");
        let mut deep = root.clone();
        for i in 0..=MAX_MODELS_TREE_DEPTH {
            deep = deep.join(format!("d{i}"));
        }
        std::fs::create_dir_all(&deep).unwrap();
        let (dirs, errors) = walk_model_dirs(&root);
        assert_eq!(
            dirs.len(),
            MAX_MODELS_TREE_DEPTH + 1,
            "everything above the ceiling loads"
        );
        assert_eq!(errors.len(), 1, "{errors:?}");
        assert!(matches!(errors[0], ModelWalkError::DepthCeiling { .. }));
    }
}
