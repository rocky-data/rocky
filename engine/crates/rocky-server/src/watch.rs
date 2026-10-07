//! Filesystem watcher for auto-recompilation.
//!
//! Watches the models directory, and the bound `rocky.toml`, for changes and
//! triggers recompilation. The config matters as much as a model: a pipeline
//! target change moves the per-model-target checks.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::state::ServerState;

/// Whether a filesystem event should recompile the project.
///
/// A create, modify or remove of a `.sql`, `.rocky` or `.toml` file, or of
/// the bound config file whatever its extension.
fn event_triggers_recompile(
    kind: &EventKind,
    paths: &[PathBuf],
    config_file: Option<&Path>,
) -> bool {
    let relevant_kind = matches!(
        kind,
        EventKind::Create(_) | EventKind::Modify(_) | EventKind::Remove(_)
    );
    relevant_kind
        && paths.iter().any(|p| {
            p.extension()
                .is_some_and(|ext| ext == "sql" || ext == "rocky" || ext == "toml")
                || config_file.is_some_and(|c| is_same_file(p, c))
        })
}

/// Whether `event_path` names `config`. The two can differ in spelling (a
/// relative `--config`, `/var` against `/private/var`), so compare the file
/// name and the canonical parent directory.
fn is_same_file(event_path: &Path, config: &Path) -> bool {
    if event_path == config {
        return true;
    }
    if event_path.file_name() != config.file_name() {
        return false;
    }
    let parent = |p: &Path| {
        let dir = match p.parent() {
            Some(d) if !d.as_os_str().is_empty() => d,
            _ => Path::new("."),
        };
        dir.canonicalize().ok()
    };
    matches!((parent(event_path), parent(config)), (Some(a), Some(b)) if a == b)
}

/// Start watching a directory for file changes, and the state's bound
/// `rocky.toml` too (`ServerState::config_path`).
/// Returns a handle that keeps the watcher alive.
pub fn start_watcher(
    state: Arc<ServerState>,
    watch_dir: &Path,
) -> Result<RecommendedWatcher, notify::Error> {
    let (tx, mut rx) = mpsc::channel::<()>(1);
    let watch_dir_display = watch_dir.display().to_string();
    let config_file: Option<PathBuf> = state.config_path.clone();
    let config_for_filter = config_file.clone();

    // Debounced recompilation task
    tokio::spawn(async move {
        let mut debounce = tokio::time::interval(Duration::from_millis(500));
        debounce.tick().await; // skip first tick

        loop {
            tokio::select! {
                Some(()) = rx.recv() => {
                    // Drain any pending notifications (debounce)
                    while rx.try_recv().is_ok() {}
                    // Small delay to let writes finish
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    state.recompile().await;
                }
                _ = debounce.tick() => {
                    // Keep interval alive
                }
            }
        }
    });

    let mut watcher =
        notify::recommended_watcher(move |res: Result<Event, notify::Error>| match res {
            Ok(event) => {
                if event_triggers_recompile(&event.kind, &event.paths, config_for_filter.as_deref())
                {
                    debug!(paths = ?event.paths, "file change detected");
                    let _ = tx.try_send(());
                }
            }
            Err(e) => warn!(error = %e, "watch error"),
        })?;

    watcher.watch(watch_dir, RecursiveMode::Recursive)?;
    // The config usually sits next to the models directory, not in it. Watch
    // its directory (not the file: editors replace a file on save, which
    // drops a watch on the old inode) unless the models watch covers it.
    if let Some(config) = config_file.as_deref() {
        let config_dir = match config.parent() {
            Some(d) if !d.as_os_str().is_empty() => d.to_path_buf(),
            _ => PathBuf::from("."),
        };
        let covered = match (config_dir.canonicalize(), watch_dir.canonicalize()) {
            (Ok(c), Ok(w)) => c.starts_with(w),
            _ => false,
        };
        if !covered {
            match watcher.watch(&config_dir, RecursiveMode::NonRecursive) {
                Ok(()) => info!(file = %config.display(), "watching the config file"),
                Err(e) => {
                    warn!(error = %e, file = %config.display(), "cannot watch the config file")
                }
            }
        }
    }
    info!(dir = watch_dir_display, "watching for file changes");

    Ok(watcher)
}

#[cfg(test)]
mod tests {
    use super::*;
    use notify::event::{CreateKind, ModifyKind};

    fn modify() -> EventKind {
        EventKind::Modify(ModifyKind::Any)
    }

    /// A pipeline target change edits `rocky.toml`, which is outside the
    /// models directory and not always named `*.toml`.
    #[test]
    fn a_change_to_the_bound_config_recompiles() {
        let config = PathBuf::from("/proj/rocky.toml");
        assert!(event_triggers_recompile(
            &modify(),
            &[config.clone()],
            Some(&config)
        ));
        // A config with another extension is still the config.
        let odd = PathBuf::from("/proj/rocky.conf");
        assert!(event_triggers_recompile(
            &modify(),
            &[odd.clone()],
            Some(&odd)
        ));
        assert!(!event_triggers_recompile(&modify(), &[odd], None));
    }

    #[test]
    fn unrelated_files_and_events_do_not_recompile() {
        let config = PathBuf::from("/proj/rocky.conf");
        let notes = PathBuf::from("/proj/notes.txt");
        assert!(!event_triggers_recompile(
            &modify(),
            &[notes],
            Some(&config)
        ));
        assert!(!event_triggers_recompile(
            &EventKind::Access(notify::event::AccessKind::Any),
            &[config.clone()],
            Some(&config)
        ));
        assert!(event_triggers_recompile(
            &EventKind::Create(CreateKind::File),
            &[PathBuf::from("/proj/models/m.sql")],
            None
        ));
    }

    #[test]
    fn config_spellings_match() {
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("rocky.toml");
        std::fs::write(&config, "").unwrap();
        let canonical = config.canonicalize().unwrap();
        assert!(is_same_file(&canonical, &config));
        assert!(!is_same_file(&dir.path().join("other.toml"), &config));
    }
}
