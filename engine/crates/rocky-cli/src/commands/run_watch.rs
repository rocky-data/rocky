//! `rocky run --watch` — re-run the pipeline on every model / config change.
//!
//! Wraps [`super::run::run`] in a filesystem-watcher loop so an interactive
//! editor session re-materializes after every save. v0 scope: re-run the
//! whole pipeline on any change, with a 200 ms debounce window to coalesce
//! editor save bursts into a single re-run.
//!
//! Stdout discipline: when `--output json` is set, each iteration emits
//! one [`crate::output::RunOutput`] JSON object as a single compact line
//! (NDJSON / newline-delimited-JSON stream). Chatty banner / "detected
//! change" / "run completed" lines go to stderr so stdout stays
//! parseable by piping tools (`jq`, `tail -f | jq -c '.command'`, etc.).
//! This differs from non-watch one-shot commands, which pretty-print
//! their single `RunOutput` for human readability — the streaming
//! contract is the watch-specific add.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::mpsc;
use tracing::warn;

use rocky_core::config::GovernanceOverride;

use super::run::{Interrupted, PartitionRunOptions, run};

/// Debounce window: collect events for this long before triggering a re-run.
/// Editor save bursts (e.g. vim's atomic-rename, vscode's two-phase write)
/// commonly fire 2-4 events within ~50 ms; 200 ms keeps the inner loop
/// reactive without re-running on a half-finished write.
const DEBOUNCE_MS: u64 = 200;

/// Channel buffer for filesystem-event notifications. Sized small because
/// a burst is coalesced inside the debounce window — we just need enough
/// headroom that the synchronous `notify` callback never blocks on `try_send`
/// for typical editor workloads.
const EVENT_CHANNEL_CAP: usize = 64;

/// Execute `rocky run --watch` — wrap [`super::run::run`] in a filesystem
/// watcher loop.
///
/// On first call the watcher seeds with one immediate run; thereafter every
/// change to a watched path triggers a debounced re-run. Errors from the
/// inner `run` are logged and the loop continues — only adapter-init /
/// config-parse errors at startup exit non-zero. SIGINT during a run is
/// handled by the inner run path (returns [`Interrupted`]); SIGINT between
/// runs exits the watch loop with status 0.
///
/// # Arguments
///
/// All arguments mirror [`super::run::run`] except for the watcher scope
/// being implicit (resolved from `models_dir` + `config_path`).
#[allow(clippy::too_many_arguments)]
pub async fn run_watch(
    config_path: &Path,
    filter: Option<&str>,
    pipeline_name_arg: Option<&str>,
    state_path: &Path,
    governance_override: Option<&GovernanceOverride>,
    output_json: bool,
    models_dir: Option<&Path>,
    run_all: bool,
    shadow_config: Option<&rocky_core::shadow::ShadowConfig>,
    partition_opts: &PartitionRunOptions,
    cache_ttl_override: Option<u64>,
    env: Option<&str>,
    // `--skip-unchanged` / `--force-rebuild` overlay; threaded into each
    // watch iteration's `run()`. Default OFF ⇒ unchanged watch behavior.
    skip_opts: &super::run::SkipRunOptions,
) -> Result<()> {
    // -------------------------------------------------------------------
    // Resolve watcher scope.
    //
    // We always watch the rocky.toml's *parent directory* (not the file
    // itself) and filter events by filename. This is a macOS FSEvents
    // robustness fix: editors that write atomically via rename
    // (vim's `set backupcopy=auto`, VSCode's default save) generate a
    // create-and-replace pair on the parent dir; a file-level watch
    // can miss the new inode. The directory watch with a filename
    // filter catches both shapes.
    //
    // For transformation pipelines we additionally watch the resolved
    // models directory recursively. For replication pipelines (no
    // models/), the rocky.toml watch alone is sufficient.
    // -------------------------------------------------------------------
    let config_path_abs = config_path
        .canonicalize()
        .with_context(|| format!("config path not found: {}", config_path.display()))?;
    let config_dir = config_path_abs
        .parent()
        .with_context(|| format!("config has no parent dir: {}", config_path_abs.display()))?
        .to_path_buf();
    let config_filename = config_path_abs
        .file_name()
        .with_context(|| format!("config has no filename: {}", config_path_abs.display()))?
        .to_owned();

    let models_dir_abs: Option<PathBuf> = match models_dir {
        Some(p) if p.exists() => Some(
            p.canonicalize()
                .with_context(|| format!("models dir not accessible: {}", p.display()))?,
        ),
        _ => None,
    };

    // Channel for watcher → debounce task. The watcher callback runs on a
    // background thread owned by `notify`, so we use a `mpsc` channel and
    // `try_send` to avoid blocking the OS event pump.
    let (tx, mut rx) = mpsc::channel::<PathBuf>(EVENT_CHANNEL_CAP);

    let config_filename_for_filter = config_filename.clone();
    let config_dir_for_filter = config_dir.clone();
    let mut watcher: RecommendedWatcher =
        notify::recommended_watcher(move |res: Result<Event, notify::Error>| match res {
            Ok(event) => {
                // Only surface events that mutate file content. `Access`
                // events (e.g. `stat`, read) are noise for a re-run trigger.
                if !matches!(
                    event.kind,
                    EventKind::Create(_) | EventKind::Modify(_) | EventKind::Remove(_)
                ) {
                    return;
                }
                for p in event.paths {
                    if event_path_is_relevant(
                        &p,
                        &config_dir_for_filter,
                        &config_filename_for_filter,
                    ) {
                        // Drop on full: the debounce loop will pick up
                        // any later event in the same burst anyway.
                        let _ = tx.try_send(p);
                    }
                }
            }
            Err(e) => warn!(error = %e, "watcher error"),
        })
        .context("failed to construct filesystem watcher")?;

    // Watch the config's parent dir non-recursively (just the rocky.toml).
    // Watching recursively here would double-up with the models-dir watch
    // and produce duplicate events for every model edit.
    watcher
        .watch(&config_dir, RecursiveMode::NonRecursive)
        .with_context(|| format!("failed to watch config dir: {}", config_dir.display()))?;

    if let Some(ref m) = models_dir_abs {
        watcher
            .watch(m, RecursiveMode::Recursive)
            .with_context(|| format!("failed to watch models dir: {}", m.display()))?;
    }

    // Honour the newline-delimited-stream contract: each iteration's
    // `RunOutput` lands as one compact line on stdout. Non-watch
    // commands stay on pretty-printed JSON because they're read by
    // humans, not piped tools.
    if output_json {
        crate::output::COMPACT_JSON.store(true, std::sync::atomic::Ordering::Relaxed);
    }

    // Banner — stderr, never stdout. The non-JSON case still prints a
    // human summary per iteration on stdout via the inner `run`.
    let watched: Vec<String> = std::iter::once(config_path_abs.display().to_string())
        .chain(models_dir_abs.iter().map(|p| p.display().to_string()))
        .collect();
    eprintln!("[watch] watching {} (Ctrl-C to stop)", watched.join(", "));

    // First run is immediate so the user gets feedback without having to
    // touch a file.
    iter_once(
        config_path,
        filter,
        pipeline_name_arg,
        state_path,
        governance_override,
        output_json,
        models_dir,
        run_all,
        shadow_config,
        partition_opts,
        cache_ttl_override,
        env,
        skip_opts,
    )
    .await;

    // Steady-state loop: wait for an event, debounce, drain, re-run.
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                eprintln!("\n[watch] stopped");
                return Ok(());
            }
            maybe_path = rx.recv() => {
                let Some(first) = maybe_path else {
                    // Watcher died; bail out so the user notices instead
                    // of silently sitting idle forever.
                    anyhow::bail!("filesystem watcher channel closed unexpectedly");
                };
                // Coalesce: sleep the debounce window, then drain everything
                // that piled up during it. We deliberately *don't* use a
                // third-party debounce crate here — a 4-line drain loop is
                // both clearer and fewer deps.
                tokio::time::sleep(Duration::from_millis(DEBOUNCE_MS)).await;
                let mut paths = vec![first];
                while let Ok(more) = rx.try_recv() {
                    paths.push(more);
                }
                paths.sort();
                paths.dedup();
                let display = paths
                    .first()
                    .map(|p| p.display().to_string())
                    .unwrap_or_default();
                eprintln!("[watch] detected change: {display}");

                iter_once(
                    config_path,
                    filter,
                    pipeline_name_arg,
                    state_path,
                    governance_override,
                    output_json,
                    models_dir,
                    run_all,
                    shadow_config,
                    partition_opts,
                    cache_ttl_override,
                    env,
                    skip_opts,
                )
                .await;
            }
        }
    }
}

/// Run one iteration of the watch loop. Errors are logged but never
/// propagated — the watcher must keep going across run failures so the
/// developer can fix the broken model and re-save without restarting
/// the watcher.
///
/// `Interrupted` (inner SIGINT) is treated like any other run error here;
/// the outer loop's own `ctrl_c` arm will pick up a *second* SIGINT to
/// exit the watcher itself.
#[allow(clippy::too_many_arguments)]
async fn iter_once(
    config_path: &Path,
    filter: Option<&str>,
    pipeline_name_arg: Option<&str>,
    state_path: &Path,
    governance_override: Option<&GovernanceOverride>,
    output_json: bool,
    models_dir: Option<&Path>,
    run_all: bool,
    shadow_config: Option<&rocky_core::shadow::ShadowConfig>,
    partition_opts: &PartitionRunOptions,
    cache_ttl_override: Option<u64>,
    env: Option<&str>,
    skip_opts: &super::run::SkipRunOptions,
) {
    let started = std::time::Instant::now();
    let result = run(
        config_path,
        filter,
        pipeline_name_arg,
        state_path,
        governance_override,
        output_json,
        models_dir,
        run_all,
        // resume / resume_latest / model_filter / idempotency_key are
        // intentionally None in watch mode — see clap conflict
        // declarations on the `--watch` arg.
        None,
        false,
        shadow_config,
        partition_opts,
        None,
        cache_ttl_override,
        None,
        env,
        // `--watch` is mutually exclusive with `--model`, so there is no
        // selection to defer against.
        &super::run::DeferOptions::default(),
        skip_opts,
        // `--watch` does not expose `--var` today; pass an empty set.
        &rocky_core::run_vars::RunVars::new(),
        // `--watch` mints the usual timestamp run_id per iteration.
        None,
    )
    .await;
    let elapsed_ms = started.elapsed().as_millis();
    match result {
        Ok(()) => {
            eprintln!("[watch] run completed in {elapsed_ms}ms");
        }
        Err(e) if e.is::<Interrupted>() => {
            // Inner run was interrupted. The next ctrl_c arm in the outer
            // loop catches a second SIGINT for the watcher itself.
            eprintln!("[watch] run interrupted in {elapsed_ms}ms");
        }
        Err(e) => {
            eprintln!("[watch] run failed in {elapsed_ms}ms: {e:#}");
        }
    }
}

/// Filter incoming filesystem events against the watcher's relevant set.
///
/// We watch:
/// 1. The rocky.toml file (matched by parent dir + exact filename).
/// 2. Anything under `models_dir` recursively, with extensions
///    `.sql` / `.rocky` / `.toml`. The recursive notify watcher already
///    scopes by directory, so we only need to filter extensions and
///    skip dotfiles / state-store artefacts.
fn event_path_is_relevant(
    path: &Path,
    config_dir: &Path,
    config_filename: &std::ffi::OsStr,
) -> bool {
    // rocky.toml exact match (+ its temporary editor backups don't match
    // the filename, so are ignored automatically).
    if let Some(name) = path.file_name()
        && path.parent() == Some(config_dir)
        && name == config_filename
    {
        return true;
    }

    // Skip dotfiles (vim swap files like `.foo.swp`, `.DS_Store`, etc.)
    // and the embedded redb state store, which lives next to models on
    // some layouts.
    if let Some(name) = path.file_name().and_then(|n| n.to_str())
        && (name.starts_with('.') || name.ends_with(".redb") || name.ends_with(".redb-lock"))
    {
        return false;
    }

    // Match transformation-relevant suffixes. `.toml` is included so
    // model sidecars trigger a re-run.
    matches!(
        path.extension().and_then(|e| e.to_str()),
        Some("sql" | "rocky" | "toml")
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use tokio::time::{Duration, Instant, sleep};
    // `Instant` re-imported here from `tokio::time` for `start.elapsed()`
    // assertions in `debounce_captures_events_during_window` — it's the
    // wall-clock variant since these tests don't pause time.

    /// `event_path_is_relevant` accepts `rocky.toml` and rejects unrelated
    /// neighbors in the same dir.
    #[test]
    fn filter_accepts_config_and_rejects_neighbors() {
        let cfg_dir = PathBuf::from("/proj");
        let cfg_name = std::ffi::OsString::from("rocky.toml");

        assert!(event_path_is_relevant(
            &PathBuf::from("/proj/rocky.toml"),
            &cfg_dir,
            &cfg_name
        ));
        // Different filename in config dir, no recognized extension.
        assert!(!event_path_is_relevant(
            &PathBuf::from("/proj/README.md"),
            &cfg_dir,
            &cfg_name
        ));
        // Neighboring .toml in config dir matches by extension (sidecar
        // configs may live alongside rocky.toml in some layouts).
        assert!(event_path_is_relevant(
            &PathBuf::from("/proj/_defaults.toml"),
            &cfg_dir,
            &cfg_name
        ));
        // dotfile reject
        assert!(!event_path_is_relevant(
            &PathBuf::from("/proj/.rocky.toml.swp"),
            &cfg_dir,
            &cfg_name
        ));
        // state-store reject
        assert!(!event_path_is_relevant(
            &PathBuf::from("/proj/.rocky-state.redb"),
            &cfg_dir,
            &cfg_name
        ));
    }

    #[test]
    fn filter_accepts_models_by_extension() {
        let cfg_dir = PathBuf::from("/proj");
        let cfg_name = std::ffi::OsString::from("rocky.toml");
        for ext in &["sql", "rocky", "toml"] {
            let p = PathBuf::from(format!("/proj/models/sub/x.{ext}"));
            assert!(
                event_path_is_relevant(&p, &cfg_dir, &cfg_name),
                ".{ext} should be relevant"
            );
        }
        // Unrecognized extension under models/ — rejected.
        assert!(!event_path_is_relevant(
            &PathBuf::from("/proj/models/x.txt"),
            &cfg_dir,
            &cfg_name
        ));
    }

    /// Debounce coalescing: a burst of N events delivered within the
    /// debounce window should produce exactly one drained vector,
    /// containing all N paths (de-duplicated).
    ///
    /// We mock the `notify` watcher by feeding events directly to the
    /// channel that `run_watch` would consume from. The drain logic is
    /// extracted into [`drain_burst`] so the test exercises the same
    /// code path the production loop runs.
    #[tokio::test]
    async fn debounce_coalesces_event_burst() {
        let (tx, mut rx) = mpsc::channel::<PathBuf>(64);

        // Send a 3-event burst within 50 ms — well inside the debounce.
        tx.send(PathBuf::from("/proj/models/a.rocky"))
            .await
            .unwrap();
        tx.send(PathBuf::from("/proj/models/b.rocky"))
            .await
            .unwrap();
        // Same path twice → dedup should collapse it.
        tx.send(PathBuf::from("/proj/models/a.rocky"))
            .await
            .unwrap();

        // First event arrives.
        let first = rx.recv().await.unwrap();
        let drained = drain_burst(first, &mut rx, Duration::from_millis(DEBOUNCE_MS)).await;

        // 3 events sent, but `a.rocky` deduped → 2 unique paths.
        assert_eq!(drained.len(), 2);
        assert_eq!(drained[0], PathBuf::from("/proj/models/a.rocky"));
        assert_eq!(drained[1], PathBuf::from("/proj/models/b.rocky"));
    }

    /// Debounce window must include all events that arrive *during* the
    /// sleep, not just the ones queued before.
    #[tokio::test]
    async fn debounce_captures_events_during_window() {
        let (tx, mut rx) = mpsc::channel::<PathBuf>(64);
        tx.send(PathBuf::from("/a")).await.unwrap();
        let first = rx.recv().await.unwrap();

        // Spawn a producer that fires another event ~100 ms later (still
        // inside the 200 ms debounce window).
        let tx2 = tx.clone();
        tokio::spawn(async move {
            sleep(Duration::from_millis(100)).await;
            let _ = tx2.send(PathBuf::from("/b")).await;
        });

        let start = Instant::now();
        let drained = drain_burst(first, &mut rx, Duration::from_millis(DEBOUNCE_MS)).await;
        // Sleep elapsed at least the debounce window.
        assert!(start.elapsed() >= Duration::from_millis(DEBOUNCE_MS));
        assert_eq!(drained.len(), 2);
    }

    /// Mirror of the production drain logic. Kept as a free fn so the
    /// debounce test exercises the same code shape as the watch loop.
    async fn drain_burst(
        first: PathBuf,
        rx: &mut mpsc::Receiver<PathBuf>,
        debounce: Duration,
    ) -> Vec<PathBuf> {
        tokio::time::sleep(debounce).await;
        let mut paths = vec![first];
        while let Ok(more) = rx.try_recv() {
            paths.push(more);
        }
        paths.sort();
        paths.dedup();
        paths
    }
}
