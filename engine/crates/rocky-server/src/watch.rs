//! Filesystem watcher for auto-recompilation.
//!
//! Watches the models directory, the `functions/` directory beside it, and the
//! bound `rocky.toml`, for changes and triggers recompilation. The config
//! matters as much as a model: a pipeline target change moves the
//! per-model-target checks, and a function definition moves E051.

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

/// Send a recompile signal when `res` is an event that should recompile.
fn handle_event(
    res: Result<Event, notify::Error>,
    tx: &mpsc::Sender<()>,
    config_file: Option<&Path>,
) {
    match res {
        Ok(event) => {
            if event_triggers_recompile(&event.kind, &event.paths, config_file) {
                debug!(paths = ?event.paths, "file change detected");
                let _ = tx.try_send(());
            }
        }
        Err(e) => warn!(error = %e, "watch error"),
    }
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

    let functions_dir = rocky_core::functions::functions_dir_for(watch_dir);
    // The watcher on a `functions/` directory that appeared (or reappeared)
    // after startup. The main watcher's handler owns it, so it lives as long
    // as the watcher the caller holds.
    let late_functions: Arc<std::sync::Mutex<Option<RecommendedWatcher>>> = Arc::default();
    let late_slot = late_functions.clone();
    let functions_for_handler = functions_dir.clone();
    let tx_for_late = tx.clone();
    let config_for_late = config_file.clone();

    let mut watcher =
        notify::recommended_watcher(move |res: Result<Event, notify::Error>| match res {
            Ok(event) => {
                if let Some(functions) = functions_for_handler.as_deref()
                    && matches!(event.kind, EventKind::Create(_))
                    && event.paths.iter().any(|p| is_same_file(p, functions))
                {
                    // The directory was created after startup: watch it now,
                    // and recompile, since files may already be inside it.
                    let tx = tx_for_late.clone();
                    let config = config_for_late.clone();
                    let made =
                        notify::recommended_watcher(move |res: Result<Event, notify::Error>| {
                            handle_event(res, &tx, config.as_deref());
                        })
                        .and_then(|mut w| {
                            w.watch(functions, RecursiveMode::NonRecursive)?;
                            Ok(w)
                        });
                    match made {
                        Ok(w) => {
                            info!(dir = %functions.display(), "watching the functions directory");
                            if let Ok(mut slot) = late_slot.lock() {
                                *slot = Some(w);
                            }
                            let _ = tx_for_late.try_send(());
                        }
                        Err(e) => warn!(
                            error = %e,
                            dir = %functions.display(),
                            "cannot watch the functions directory"
                        ),
                    }
                    return;
                }
                handle_event(Ok(event), &tx, config_for_filter.as_deref());
            }
            Err(e) => warn!(error = %e, "watch error"),
        })?;

    watcher.watch(watch_dir, RecursiveMode::Recursive)?;
    // UDF definitions live beside the models directory, not in it
    // (`<models>/../functions/*.toml`, the directory the compiler reads). A
    // change there moves E051 and the types of UDF calls, so it recompiles
    // too. When the directory does not exist yet, its parent is watched so
    // the handler above can start watching it once it appears.
    if let Some(functions_dir) = functions_dir.as_deref() {
        let covered = match (functions_dir.canonicalize(), watch_dir.canonicalize()) {
            (Ok(f), Ok(w)) => f.starts_with(w),
            _ => false,
        };
        if !covered {
            if functions_dir.is_dir() {
                match watcher.watch(functions_dir, RecursiveMode::NonRecursive) {
                    Ok(()) => {
                        info!(dir = %functions_dir.display(), "watching the functions directory")
                    }
                    Err(e) => warn!(
                        error = %e,
                        dir = %functions_dir.display(),
                        "cannot watch the functions directory"
                    ),
                }
            }
            // The parent also reports a `functions/` created, or replaced
            // after a delete, later on.
            if let Some(parent) = functions_dir.parent()
                && let Err(e) = watcher.watch(parent, RecursiveMode::NonRecursive)
            {
                warn!(
                    error = %e,
                    dir = %parent.display(),
                    "cannot watch for a functions directory appearing"
                );
            }
        }
    }
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
            std::slice::from_ref(&config),
            Some(&config)
        ));
        // A config with another extension is still the config.
        let odd = PathBuf::from("/proj/rocky.conf");
        assert!(event_triggers_recompile(
            &modify(),
            std::slice::from_ref(&odd),
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
            std::slice::from_ref(&config),
            Some(&config)
        ));
        assert!(event_triggers_recompile(
            &EventKind::Create(CreateKind::File),
            &[PathBuf::from("/proj/models/m.sql")],
            None
        ));
    }

    /// A change under `functions/` (beside the models directory, not in it)
    /// recompiles. The watcher used to watch only the models directory, so
    /// a broken function definition showed no E051 until a model changed.
    #[tokio::test]
    async fn a_change_under_functions_recompiles() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().canonicalize().unwrap();
        let models = root.join("models");
        let functions = root.join("functions");
        std::fs::create_dir_all(&models).unwrap();
        std::fs::create_dir_all(&functions).unwrap();
        std::fs::write(
            functions.join("dbl.toml"),
            "returns = \"DOUBLE\"\n\n[[arguments]]\nname = \"x\"\ntype = \"DOUBLE\"\n",
        )
        .unwrap();
        std::fs::write(functions.join("dbl.sql"), "x * 2\n").unwrap();
        std::fs::write(models.join("m.sql"), "SELECT dbl(1.0) AS a2").unwrap();
        std::fs::write(
            models.join("m.toml"),
            "[strategy]\ntype = \"full_refresh\"\n\n\
             [target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"m\"\n",
        )
        .unwrap();

        let state = ServerState::new(models.clone(), None, None);
        let has_e051 = |state: &ServerState| {
            let state = state.compile_result.try_read().ok()?;
            let result = state.as_ref()?;
            Some(result.diagnostics.iter().any(|d| &*d.code == "E051"))
        };
        let wait_for = |want: bool| {
            let state = state.clone();
            async move {
                let deadline = std::time::Instant::now() + Duration::from_secs(15);
                while std::time::Instant::now() < deadline {
                    if has_e051(&state) == Some(want) {
                        return true;
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                false
            }
        };
        assert!(wait_for(false).await, "the initial compile is clean");

        let _watcher = start_watcher(state.clone(), &models).unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;
        std::fs::write(
            functions.join("dbl.toml"),
            "returns = \"DOUBLE\"\nbogus = 1\n",
        )
        .unwrap();
        assert!(
            wait_for(true).await,
            "editing functions/dbl.toml must recompile and surface E051"
        );
    }

    /// A `functions/` directory created after the server starts is watched
    /// from then on (#2292): its creation recompiles, and so does a later
    /// edit inside it.
    #[tokio::test]
    async fn a_functions_directory_created_after_start_is_watched() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().canonicalize().unwrap();
        let models = root.join("models");
        let functions = root.join("functions");
        std::fs::create_dir_all(&models).unwrap();
        std::fs::write(models.join("m.sql"), "SELECT 1 AS a").unwrap();
        std::fs::write(
            models.join("m.toml"),
            "[strategy]\ntype = \"full_refresh\"\n\n\
             [target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"m\"\n",
        )
        .unwrap();

        let state = ServerState::new(models.clone(), None, None);
        let has_e051 = |state: &ServerState| {
            let state = state.compile_result.try_read().ok()?;
            let result = state.as_ref()?;
            Some(result.diagnostics.iter().any(|d| &*d.code == "E051"))
        };
        let wait_for = |want: bool| {
            let state = state.clone();
            async move {
                let deadline = std::time::Instant::now() + Duration::from_secs(15);
                while std::time::Instant::now() < deadline {
                    if has_e051(&state) == Some(want) {
                        return true;
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                false
            }
        };
        assert!(wait_for(false).await, "the initial compile is clean");

        let _watcher = start_watcher(state.clone(), &models).unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;
        std::fs::create_dir_all(&functions).unwrap();
        std::fs::write(
            functions.join("dbl.toml"),
            "returns = \"DOUBLE\"\nbogus = 1\n",
        )
        .unwrap();
        std::fs::write(functions.join("dbl.sql"), "x * 2\n").unwrap();
        assert!(
            wait_for(true).await,
            "creating functions/ after start must recompile and surface E051"
        );

        // The new directory is watched, not only noticed once.
        tokio::time::sleep(Duration::from_millis(500)).await;
        std::fs::write(
            functions.join("dbl.toml"),
            "returns = \"DOUBLE\"\n\n[[arguments]]\nname = \"x\"\ntype = \"DOUBLE\"\n",
        )
        .unwrap();
        assert!(
            wait_for(false).await,
            "an edit inside the new functions/ must recompile"
        );
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
