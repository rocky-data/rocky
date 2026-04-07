//! Filesystem watcher for auto-recompilation.
//!
//! Watches the models directory for changes and triggers recompilation.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::state::ServerState;

/// Start watching a directory for file changes.
/// Returns a handle that keeps the watcher alive.
pub fn start_watcher(
    state: Arc<ServerState>,
    watch_dir: &Path,
) -> Result<RecommendedWatcher, notify::Error> {
    let (tx, mut rx) = mpsc::channel::<()>(1);
    let watch_dir_display = watch_dir.display().to_string();

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
                let dominated = matches!(
                    event.kind,
                    EventKind::Create(_) | EventKind::Modify(_) | EventKind::Remove(_)
                );
                let is_model_file = event.paths.iter().any(|p| {
                    p.extension()
                        .is_some_and(|ext| ext == "sql" || ext == "rocky" || ext == "toml")
                });

                if dominated && is_model_file {
                    debug!(paths = ?event.paths, "file change detected");
                    let _ = tx.try_send(());
                }
            }
            Err(e) => warn!(error = %e, "watch error"),
        })?;

    watcher.watch(watch_dir, RecursiveMode::Recursive)?;
    info!(dir = watch_dir_display, "watching for file changes");

    Ok(watcher)
}
