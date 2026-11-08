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
use rocky_core::state_sync::DroppedSessionSink;

use tokio_util::sync::CancellationToken;

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
/// config-parse errors at startup exit non-zero. A single SIGINT always exits
/// the watch loop with status 0, whether it lands between runs or during one.
/// Requiring a second signal in the second case was #1405.
///
/// A signal during an iteration cancels that iteration's token (#1606). The
/// run stops starting warehouse work and settles its record, claim and state
/// session itself, within [`SIGNAL_SETTLE_GRACE`]. An iteration that does not
/// settle in time (a stalled statement) is dropped, and the loop then settles
/// the remote-state session it held — its terminal upload, under the session's
/// own durability rules (#1603). The exit is non-zero only when that upload
/// fails and `[state] on_upload_failure = "fail"` says it must not be lost;
/// see `stop_interrupted_iteration`.
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
    // `--var name=value`, parsed once by the caller and handed to every
    // iteration's `run()` (#2379).
    run_vars: &rocky_core::run_vars::RunVars,
    // Who is running (RV4-P1), handed to each iteration's `run()`.
    actor: &rocky_core::config::PrincipalRef,
) -> Result<()> {
    // The watch loop logs iteration errors and keeps running. Refuse an
    // unusable Pipes channel before entering that loop.
    crate::pipes::PipesEmitter::validate_requested()?;
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
    // We additionally watch model directories recursively. An explicit
    // `--models` directory is watched when it exists. Without `--models`,
    // each selected transformation pipeline's models glob base is watched
    // (`default_watch_set`): the directories the run reads. A replication
    // pipeline reads model files only with `--all` or `--models`
    // (`replication_models_dir`), and then that directory is watched. A
    // selected pipeline that reads no watched file re-runs only on a
    // rocky.toml edit; the banner says so (`config_only_pipelines`).
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

    let replication_models = replication_models_dir(models_dir, run_all);
    let watch_set = resolve_watch_set(
        models_dir,
        &config_path_abs,
        &config_dir,
        pipeline_name_arg,
        replication_models.as_deref(),
    )?;

    // Channel for watcher → debounce task. The watcher callback runs on a
    // background thread owned by `notify`, so we use a `mpsc` channel and
    // `try_send` to avoid blocking the OS event pump.
    let (tx, mut rx) = mpsc::channel::<PathBuf>(EVENT_CHANNEL_CAP);

    let config_filename_for_filter = config_filename.clone();
    let config_dir_for_filter = config_dir.clone();
    let roots_for_filter = watch_set.model_roots.clone();
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
                    if event_triggers_rerun(
                        &p,
                        &config_dir_for_filter,
                        &config_filename_for_filter,
                        &roots_for_filter,
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
    // and produce duplicate events for every model edit. The one exception
    // is a default models base that IS the config dir: then the config dir
    // is watched once, recursively, and covers both. A recursive watch of the
    // config dir that fails (on Linux, the inotify watch limit on a large
    // project) falls back to the non-recursive config watch instead of
    // refusing to start: the config still re-runs, model edits do not.
    let mut config_dir_recursive = watch_set.config_dir_recursive;
    for (dir, mode) in watch_registrations(&config_dir, &watch_set) {
        let is_config_dir = dir == config_dir;
        match watcher.watch(&dir, mode) {
            Ok(()) => {}
            Err(e) if is_config_dir && mode == RecursiveMode::Recursive => {
                warn!(
                    error = %e,
                    dir = %dir.display(),
                    "cannot watch the config dir recursively; watching rocky.toml only, \
                     so a model edit does not re-run"
                );
                config_dir_recursive = false;
                // A failed recursive watch can leave the subdirectories it
                // registered before the error; drop them so only rocky.toml
                // is watched, as the warning says.
                let _ = watcher.unwatch(&dir);
                watcher
                    .watch(&dir, RecursiveMode::NonRecursive)
                    .with_context(|| format!("failed to watch config dir: {}", dir.display()))?;
            }
            Err(e) if is_config_dir => {
                return Err(e)
                    .with_context(|| format!("failed to watch config dir: {}", dir.display()));
            }
            Err(e) => {
                return Err(e)
                    .with_context(|| format!("failed to watch models dir: {}", dir.display()));
            }
        }
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
    let config_only = config_only_pipelines(
        &config_path_abs,
        pipeline_name_arg,
        replication_models.is_some(),
    );
    for line in watch_banner(
        &config_path_abs,
        &config_dir,
        config_dir_recursive,
        &watch_set,
        &config_only,
    ) {
        eprintln!("{line}");
    }

    // ONE long-lived registration per signal, built BEFORE the first run and
    // never dropped.
    //
    // Two windows exist where a signal has zero listeners, and tokio
    // DISCARDS a signal that arrives while zero listeners are registered
    // (the process-level disposition stays tokio's once any listener ever
    // existed, so the default terminate behavior does not come back):
    //
    //   1. Inside the loop, when the file-change arm wins and a per-
    //      iteration future would be dropped — closed by #1490 (hoist the
    //      futures out of the loop).
    //   2. Between the inner run's own listener dropping (when the first
    //      run ends) and the loop's registrations being created. A SIGTERM
    //      landing there was swallowed forever: the watcher then ignored
    //      every later SIGTERM because the discarded one never latched.
    //      This is not theoretical — `run_watch_exits_on_sigterm` signals
    //      the instant the first run completes, and lost exactly this race
    //      on CI once the schedule shifted (#1405's residual).
    //
    // Window 2 is why the registrations sit ABOVE the first `iter_once`:
    // `tokio::signal::unix::signal()` registers eagerly at creation and a
    // `Signal` stream LATCHES a pending signal even while nothing polls it,
    // so a signal arriving during (or right after) the first run fires the
    // select arm at loop entry. `ctrl_c()` registers only on first poll
    // (`pin!` alone registers nothing), which is why the unix SIGINT arm
    // uses an eager `SignalKind::interrupt()` stream too; the `ctrl_c()`
    // future stays as the portable arm for non-unix.
    //
    // SIGTERM previously had no arm at all, so after the first run the
    // watcher could not be killed by `timeout`, CI cancellation or a
    // container eviction. Same shape as the run loop's own handling
    // (`run.rs`).
    let ctrl_c_signal = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c_signal);
    #[cfg(unix)]
    let mut sigint_stream =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()).ok();
    #[cfg(unix)]
    let mut sigterm_stream =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).ok();
    // Windows (#2440): Ctrl-Break, console close, logoff and shutdown take the
    // SIGTERM arm's place; `ctrl_c_signal` above keeps Ctrl-C.
    #[cfg(windows)]
    let mut windows_stops = super::run_cancel::WindowsStopEvents::register(false);

    // First run is immediate so the user gets feedback without having to
    // touch a file.
    //
    // The run is RACED against the signal arms rather than plainly awaited.
    // With the streams registered above, a signal arriving mid-run is only
    // latched — nothing polls the streams while `iter_once` is in flight. An
    // un-raced await would therefore defer shutdown to the end of the
    // iteration and make a stalled run unkillable by SIGTERM, where the
    // pre-registration code died instantly on the default disposition.
    //
    // When an arm fires, `stop_iteration` cancels the iteration's token and
    // waits at most `SIGNAL_SETTLE_GRACE` for the run to settle itself (#1606).
    // That bound is what keeps SIGTERM prompt: a stalled iteration is then
    // dropped, and the process exits. Dropping mid-run is crash-parity — the
    // state store's commit protocol and `InProgress` breadcrumbs are designed
    // for exactly that — and is strictly cleaner than the old mid-run
    // default-disposition kill.
    {
        // The sink catches any remote-state session the iteration still holds
        // if a signal makes us drop it, so its terminal upload is settled
        // rather than skipped (#1603). See `stop_interrupted_iteration`.
        let sink = DroppedSessionSink::new();
        // The iteration's cancellation token (#1606). The run inside adopts it
        // instead of listening for signals itself, so the arms below can ask
        // it to stop and settle rather than only drop it.
        let cancel = CancellationToken::new();
        let mut first_run = Box::pin(sink.scope(super::run_cancel::scope(
            cancel.clone(),
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
                run_vars,
                actor,
            ),
        )));
        let stop = tokio::select! {
            outcome = &mut first_run => IterStop::Finished { interrupted: outcome.interrupted },
            _ = &mut ctrl_c_signal => IterStop::Signal,
            Some(()) = async {
                #[cfg(unix)]
                {
                    match sigint_stream.as_mut() {
                        Some(stream) => stream.recv().await,
                        None => std::future::pending().await,
                    }
                }
                #[cfg(not(unix))]
                {
                    std::future::pending().await
                }
            } => IterStop::Signal,
            Some(()) = async {
                #[cfg(unix)]
                {
                    match sigterm_stream.as_mut() {
                        Some(stream) => stream.recv().await,
                        None => std::future::pending().await,
                    }
                }
                #[cfg(windows)]
                {
                    windows_stops.recv().await
                }
                #[cfg(not(any(unix, windows)))]
                {
                    std::future::pending().await
                }
            } => IterStop::Signal,
        };
        if stop_iteration(stop, &cancel, first_run, &sink).await? {
            eprintln!("\n[watch] stopped");
            return Ok(());
        }
    }

    // Steady-state loop: wait for an event, debounce, drain, re-run.
    loop {
        tokio::select! {
            _ = &mut ctrl_c_signal => {
                eprintln!("\n[watch] stopped");
                return Ok(());
            }
            Some(()) = async {
                #[cfg(unix)]
                {
                    match sigint_stream.as_mut() {
                        Some(stream) => stream.recv().await,
                        None => std::future::pending().await,
                    }
                }
                #[cfg(not(unix))]
                {
                    std::future::pending().await
                }
            } => {
                eprintln!("\n[watch] stopped");
                return Ok(());
            }
            Some(()) = async {
                #[cfg(unix)]
                {
                    match sigterm_stream.as_mut() {
                        Some(stream) => stream.recv().await,
                        None => std::future::pending().await,
                    }
                }
                #[cfg(windows)]
                {
                    windows_stops.recv().await
                }
                #[cfg(not(any(unix, windows)))]
                {
                    std::future::pending().await
                }
            } => {
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

                // Same race as the first run, for the same reason: a signal
                // during the debounce sleep or the re-run is only latched by
                // the streams above, and an un-raced await would defer
                // shutdown to the end of the iteration (or forever, for a
                // stalled one). A signal latched during the debounce fires
                // here immediately, before the re-run starts.
                let sink = DroppedSessionSink::new();
                let cancel = CancellationToken::new();
                let mut rerun = Box::pin(sink.scope(super::run_cancel::scope(
                    cancel.clone(),
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
                    run_vars,
                    actor,
                ),
                )));
                let stop = tokio::select! {
                    outcome = &mut rerun => IterStop::Finished { interrupted: outcome.interrupted },
                    _ = &mut ctrl_c_signal => IterStop::Signal,
                    Some(()) = async {
                        #[cfg(unix)]
                        {
                            match sigint_stream.as_mut() {
                                Some(stream) => stream.recv().await,
                                None => std::future::pending().await,
                            }
                        }
                        #[cfg(not(unix))]
                        {
                            std::future::pending().await
                        }
                    } => IterStop::Signal,
                    Some(()) = async {
                        #[cfg(unix)]
                        {
                            match sigterm_stream.as_mut() {
                                Some(stream) => stream.recv().await,
                                None => std::future::pending().await,
                            }
                        }
                        #[cfg(windows)]
                        {
                            windows_stops.recv().await
                        }
                        #[cfg(not(any(unix, windows)))]
                        {
                            std::future::pending().await
                        }
                    } => IterStop::Signal,
                };
                if stop_iteration(stop, &cancel, rerun, &sink).await? {
                    eprintln!("\n[watch] stopped");
                    return Ok(());
                }
            }
        }
    }
}

/// The grace a signalled iteration gets to stop and settle on its own before
/// the loop drops it (#1606). The same constant as plain `rocky run`; see it
/// for why the wait is bounded. Past it the iteration is dropped as before —
/// crash parity, settled by [`stop_interrupted_iteration`] — and `main`'s
/// five-second blocking-pool grace follows, so a stalled iteration exits
/// within ten seconds.
use super::run_cancel::SIGNAL_SETTLE_GRACE;

/// What ended the wait on one watch iteration.
enum IterStop {
    /// The iteration returned by itself; `interrupted` when its run reported
    /// [`Interrupted`].
    Finished { interrupted: bool },
    /// A signal arm fired while the iteration was still running.
    Signal,
}

/// Act on how an iteration's wait ended. Returns `true` when the watch loop
/// must stop.
///
/// On a signal, cancel the iteration's token and give it
/// [`SIGNAL_SETTLE_GRACE`] to stop and settle through its own terminal path
/// (#1606). Past the grace, drop it as before. A second signal during the
/// grace exits 130 at once.
///
/// ```text
///   signal ──▶ cancel.cancel() ──▶ wait ≤ grace for the iteration
///                                     │ settled            │ grace elapsed
///                                     ▼                    ▼
///                      stop_interrupted_iteration (no-op)  drop it (crash
///                                                          parity) + settle
///                                                          its session
/// ```
async fn stop_iteration<F: std::future::Future>(
    stop: IterStop,
    cancel: &CancellationToken,
    mut iteration: std::pin::Pin<Box<F>>,
    sink: &DroppedSessionSink,
) -> Result<bool> {
    match stop {
        IterStop::Finished { interrupted: false } => return Ok(false),
        IterStop::Finished { interrupted: true } => {}
        IterStop::Signal => {
            // Registered before the notice, so a second SIGINT sent after the
            // operator sees it is never lost. A fresh stream sees only signals
            // delivered after it exists, not the one that brought us here.
            #[cfg(unix)]
            let mut second_sigint =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()).ok();
            #[cfg(unix)]
            let mut second_sigterm =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).ok();
            #[cfg(windows)]
            let mut second_stop = super::run_cancel::WindowsStopEvents::register(true);
            cancel.cancel();
            eprintln!(
                "\n[watch] shutdown signal received — letting the run settle (up to {}s); press \
                 Ctrl-C again to terminate",
                SIGNAL_SETTLE_GRACE.as_secs()
            );
            let second = async {
                // Either kind of signal counts as the second one.
                #[cfg(unix)]
                {
                    let sigint = async {
                        match second_sigint.as_mut() {
                            Some(stream) => stream.recv().await,
                            None => std::future::pending().await,
                        }
                    };
                    let sigterm = async {
                        match second_sigterm.as_mut() {
                            Some(stream) => stream.recv().await,
                            None => std::future::pending().await,
                        }
                    };
                    tokio::select! {
                        Some(()) = sigint => Some(()),
                        Some(()) = sigterm => Some(()),
                        else => None,
                    }
                }
                #[cfg(windows)]
                {
                    second_stop.recv().await
                }
                #[cfg(not(any(unix, windows)))]
                {
                    tokio::signal::ctrl_c().await.ok()
                }
            };
            tokio::select! {
                settled = tokio::time::timeout(SIGNAL_SETTLE_GRACE, iteration.as_mut()) => {
                    if settled.is_err() {
                        eprintln!(
                            "[watch] the run did not settle within {}s; stopping it",
                            SIGNAL_SETTLE_GRACE.as_secs()
                        );
                    }
                }
                Some(()) = second => {
                    eprintln!("\n[watch] second signal — terminating");
                    std::process::exit(130);
                }
            }
        }
    }
    stop_interrupted_iteration(iteration, sink).await?;
    Ok(true)
}

/// Stop an iteration a signal interrupted, and settle what it owed its remote
/// state (#1603).
///
/// The `select!` above stops a run by dropping its future. A
/// `RemoteStateSession` inside it then never reaches `finalize`, and `Drop`
/// cannot await an upload, so the run's terminal state upload used to be
/// skipped — silently in release builds, and with a `debug_assert!` panic
/// (exit 101) in debug ones. Arming `sink` before the drop makes each such
/// session hand itself over instead, and this function performs its terminal
/// upload under the session's own durability rules:
///
/// ```text
///   signal ──▶ arm sink ──▶ drop iteration ──▶ sessions handed over
///                                                    │
///                     settle_all: join periodic, wait for writers, upload
///                                                    │
///                          Ok ──▶ "[watch] stopped", exit 0
///                          Err ──▶ exit non-zero (fail closed)
/// ```
///
/// A settlement failure is returned, so the watcher exits non-zero: under
/// `on_upload_failure = "fail"` the upload must not be lost behind a clean
/// exit (watch runs are never governed, so `Durable` does not arise here). The default `skip` posture warns and still exits 0,
/// exactly as `finalize` would. A second Ctrl-C during settlement terminates
/// at once with 130, the same escape hatch the run's own shutdown offers.
///
/// When the iteration had already returned (the inner run handled the signal
/// itself and settled its own session), dropping it hands over nothing and
/// this is a no-op.
async fn stop_interrupted_iteration<F: std::future::Future>(
    iteration: std::pin::Pin<Box<F>>,
    sink: &DroppedSessionSink,
) -> Result<()> {
    sink.arm();
    drop(iteration);
    if sink.pending() == 0 {
        return Ok(());
    }
    if !sink.owes_upload() {
        // Local backend, suppressed, or never downloaded: settling only
        // consumes the sessions, so there is nothing to wait for.
        sink.settle_all().await?;
        return Ok(());
    }
    eprintln!(
        "[watch] settling the interrupted run's state upload; press Ctrl-C again to terminate \
         without it"
    );
    // Either kind of signal escapes, as in `stop_iteration` (#1606). Streams
    // registered now see only later signals.
    #[cfg(unix)]
    let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()).ok();
    #[cfg(unix)]
    let mut sigterm =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).ok();
    #[cfg(windows)]
    let mut stops = super::run_cancel::WindowsStopEvents::register(true);
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            let sigint = async {
                match sigint.as_mut() {
                    Some(stream) => stream.recv().await,
                    None => std::future::pending().await,
                }
            };
            let sigterm = async {
                match sigterm.as_mut() {
                    Some(stream) => stream.recv().await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                Some(()) = sigint => {}
                Some(()) = sigterm => {}
                else => std::future::pending::<()>().await,
            }
        }
        #[cfg(windows)]
        {
            if stops.recv().await.is_none() {
                std::future::pending::<()>().await;
            }
        }
        #[cfg(not(any(unix, windows)))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
        eprintln!("\n[watch] second signal — terminating without the state upload");
        std::process::exit(130);
    });
    sink.settle_all().await.context(
        "the interrupted run's terminal state upload failed, so the remote state ledger does \
         not hold what the run committed",
    )?;
    Ok(())
}

/// What one watch iteration observed.
///
/// `interrupted` exists because a swallowed SIGINT is indistinguishable from a
/// completed run at the call site, and the watcher has to tell them apart to
/// honour a single Ctrl-C (#1405).
struct IterOutcome {
    /// Fingerprint of the config snapshot this iteration loaded; `None` when
    /// the load itself failed.
    ///
    /// Read only by the per-iteration-reload test — production callers act on
    /// `interrupted` alone. It was previously `iter_once`'s return value, where
    /// an ignored result is not dead code; as a struct field an unread one is,
    /// so a non-test build trips `-D dead-code` without this. Kept rather than
    /// dropped because the reload contract it pins (`iter_once` never hoists
    /// the config load above the loop) is worth an executable assertion.
    #[allow(dead_code)]
    fingerprint: Option<String>,
    /// The inner run was interrupted by SIGINT — the watcher must stop.
    interrupted: bool,
}

/// Run one iteration of the watch loop. Errors are logged but never
/// propagated — the watcher must keep going across run failures so the
/// developer can fix the broken model and re-save without restarting
/// the watcher.
///
/// `Interrupted` (inner SIGINT) is the one error that DOES stop the watcher:
/// it is reported on [`IterOutcome::interrupted`] so the caller can exit.
/// Previously it was logged like any other run failure and the loop went back
/// to waiting, which meant a Ctrl-C landing *during* a run was consumed by the
/// inner run and the watcher sat there until a second one arrived. Whether one
/// SIGINT sufficed therefore depended on whether a run happened to be in
/// flight — the race behind the intermittent shutdown hang in #1405.
///
/// Returns the fingerprint of the config snapshot this iteration loaded
/// (`None` when the load failed), so the per-iteration-reload contract is
/// directly testable, alongside whether the run was interrupted.
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
    run_vars: &rocky_core::run_vars::RunVars,
    actor: &rocky_core::config::PrincipalRef,
) -> IterOutcome {
    let started = std::time::Instant::now();
    // Load the config FRESH for THIS iteration — deliberately NEVER hoisted
    // above the watch loop into a single load (#1120 threads ONE snapshot per
    // invocation, and for `--watch` the invocation is the iteration): editing
    // `rocky.toml` mid-watch must take effect on the next iteration
    // (live-reload, the pre-existing behavior of `run()`'s internal load),
    // and a broken config must fail only this iteration — logged via the
    // same "[watch] run failed" line the inner run's load error used to
    // produce — with the watcher continuing.
    let loaded = match rocky_core::config::load_rocky_config_fingerprinted(config_path) {
        Ok(loaded) => std::sync::Arc::new(loaded),
        Err(e) => {
            let elapsed_ms = started.elapsed().as_millis();
            let e = anyhow::Error::new(e).context(format!(
                "failed to load config from {}",
                config_path.display()
            ));
            eprintln!("[watch] run failed in {elapsed_ms}ms: {e:#}");
            return IterOutcome {
                fingerprint: None,
                interrupted: false,
            };
        }
    };
    let fingerprint = loaded.fingerprint.clone();
    let result = run(
        config_path,
        loaded,
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
        // The same `--var` map on every iteration (#2379).
        run_vars,
        // `--watch` mints the usual timestamp run_id per iteration.
        None,
        // `--watch` is a dev loop, never a governed two-step apply — no gate.
        None,
        // `--assume-fresh-state` is not surfaced on the watch path (clap
        // rejects the combination at parse time).
        false,
        None, // #1460: dev watch loop, no persisted plan
        actor,
    )
    .await;
    let elapsed_ms = started.elapsed().as_millis();
    let mut interrupted = false;
    match result {
        Ok(_) => {
            eprintln!("[watch] run completed in {elapsed_ms}ms");
        }
        Err(e) if e.is::<Interrupted>() => {
            // The user pressed Ctrl-C while this run was executing. Report it
            // so the watcher stops now rather than waiting for a second one.
            eprintln!("[watch] run interrupted in {elapsed_ms}ms");
            interrupted = true;
        }
        Err(e) => {
            eprintln!("[watch] run failed in {elapsed_ms}ms: {e:#}");
        }
    }
    IterOutcome {
        fingerprint: Some(fingerprint),
        interrupted,
    }
}

/// The directories one `rocky run --watch` watches, beside `rocky.toml`.
#[derive(Debug, PartialEq, Eq)]
struct WatchSet {
    /// Watch the config directory recursively instead of non-recursively.
    /// Set when a default models base is the config directory itself, so
    /// that directory is watched once and covers the config and the models.
    config_dir_recursive: bool,
    /// Directories watched recursively. No entry is equal to or inside
    /// another entry, or inside the config directory when that is recursive.
    model_roots: Vec<PathBuf>,
}

/// The watch set for `rocky run --watch`.
///
/// An explicit `--models` keeps the behaviour it always had: that directory
/// is watched when it exists, next to the non-recursive config watch, and no
/// pipeline's directory is added. Without `--models`, see
/// [`default_watch_set`].
fn resolve_watch_set(
    models_dir: Option<&Path>,
    config_path: &Path,
    config_dir: &Path,
    pipeline_name_arg: Option<&str>,
    replication_models: Option<&Path>,
) -> Result<WatchSet> {
    let Some(p) = models_dir else {
        return Ok(default_watch_set(
            config_path,
            config_dir,
            pipeline_name_arg,
            replication_models,
        ));
    };
    let model_roots = if p.exists() {
        vec![
            p.canonicalize()
                .with_context(|| format!("models dir not accessible: {}", p.display()))?,
        ]
    } else {
        Vec::new()
    };
    Ok(WatchSet {
        config_dir_recursive: false,
        model_roots,
    })
}

/// The watch set for `rocky run --watch` without `--models`.
///
/// Watches the models glob base of each selected transformation pipeline:
/// the pipeline named by `--pipeline`, or every pipeline when none is named.
/// The base is the directory part of the glob before its first wildcard, from
/// the same `locate_models_dir` derivation the run uses. A base that does not
/// exist, does not resolve, or escapes the project is not watched; the run
/// reports that itself.
///
/// A selected replication pipeline adds `replication_models`, the directory
/// its run reads compiled models from ([`replication_models_dir`]), when
/// there is one.
///
/// Read once, when the watcher starts. A config that does not load gives no
/// model directory, because each iteration reloads the config and reports the
/// error, and a fix to `rocky.toml` must still re-run. A pipeline added, or a
/// base created, after start is watched only after a restart.
fn default_watch_set(
    config_path: &Path,
    config_dir: &Path,
    pipeline_name_arg: Option<&str>,
    replication_models: Option<&Path>,
) -> WatchSet {
    let roots = match rocky_core::config::load_rocky_config(config_path) {
        Ok(config) => config
            .pipelines
            .iter()
            .filter(|(name, _)| pipeline_name_arg.is_none_or(|selected| selected == name.as_str()))
            .filter_map(|(_, pipeline)| {
                if pipeline.as_replication().is_some() {
                    return replication_models.map(Path::to_path_buf);
                }
                let tx = pipeline.as_transformation()?;
                match crate::models_loader::locate_models_dir(&tx.models, config_path) {
                    Ok(crate::models_loader::ModelsDir::Present(dir)) => dir.canonicalize().ok(),
                    Ok(crate::models_loader::ModelsDir::Absent(_)) | Err(_) => None,
                }
            })
            .collect(),
        Err(_) => Vec::new(),
    };
    dedupe_watch_roots(config_dir, roots)
}

/// The directory a replication pipeline's run reads compiled models from,
/// canonical, or `None` when it reads none.
///
/// The run's own rule (`run.rs`, the compiled-model phase of the replication
/// path): with `--all` or `--models`, it runs the models in `--models`, else
/// in `models` under the working directory, when that directory exists.
/// Without either flag it runs no model.
fn replication_models_dir(models_dir: Option<&Path>, run_all: bool) -> Option<PathBuf> {
    let dir = match models_dir {
        Some(dir) => dir,
        None if run_all => Path::new("models"),
        None => return None,
    };
    if !dir.exists() {
        return None;
    }
    dir.canonicalize().ok()
}

/// Remove duplicate watches from `roots` (canonical paths).
///
/// A root equal to or inside another root is dropped: the outer recursive
/// watch already covers it. A root equal to `config_dir` turns the config
/// directory's watch recursive instead of adding a second watch on it, and
/// then every root inside `config_dir` is covered by it too. Otherwise a root
/// inside `config_dir` is kept: the config watch is non-recursive and does
/// not see it.
fn dedupe_watch_roots(config_dir: &Path, mut roots: Vec<PathBuf>) -> WatchSet {
    let config_dir_recursive = roots.iter().any(|r| r == config_dir);
    if config_dir_recursive {
        roots.retain(|r| !r.starts_with(config_dir));
    }
    // Sorted, a parent comes before everything inside it.
    roots.sort();
    let mut model_roots: Vec<PathBuf> = Vec::new();
    for root in roots {
        if !model_roots.iter().any(|kept| root.starts_with(kept)) {
            model_roots.push(root);
        }
    }
    WatchSet {
        config_dir_recursive,
        model_roots,
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
///
/// The filter checks names only: a run that writes a `.sql`, `.rocky` or
/// `.toml` file inside a watched directory would re-run itself in a loop.
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

/// Whether an event re-runs the pipeline: [`event_path_is_relevant`], and
/// not under a directory [`under_ignored_dir`] skips.
fn event_triggers_rerun(
    path: &Path,
    config_dir: &Path,
    config_filename: &std::ffi::OsStr,
    watched_roots: &[PathBuf],
) -> bool {
    !under_ignored_dir(path, config_dir, watched_roots)
        && event_path_is_relevant(path, config_dir, config_filename)
}

/// Whether `path` sits below a directory no model lives in: one whose name
/// starts with `.` (`.git`, `.venv`), or is `target` or `node_modules`.
///
/// Only the components between the watched directory that holds `path` (the
/// deepest of `config_dir` and `watched_roots`) and the file itself are
/// checked, so a watched directory may itself be named that way. A recursive
/// watch of the project root sees all of these; without this, a
/// `pyproject.toml` or a vendored `Cargo.toml` anywhere would re-run.
fn under_ignored_dir(path: &Path, config_dir: &Path, watched_roots: &[PathBuf]) -> bool {
    let Some(base) = std::iter::once(config_dir)
        .chain(watched_roots.iter().map(PathBuf::as_path))
        .filter(|root| path.starts_with(root))
        .max_by_key(|root| root.components().count())
    else {
        return false;
    };
    let Ok(relative) = path.strip_prefix(base) else {
        return false;
    };
    let Some(parent) = relative.parent() else {
        return false;
    };
    parent.components().any(|component| match component {
        std::path::Component::Normal(name) => name
            .to_str()
            .is_some_and(|n| n.starts_with('.') || n == "target" || n == "node_modules"),
        std::path::Component::Prefix(_)
        | std::path::Component::RootDir
        | std::path::Component::CurDir
        | std::path::Component::ParentDir => false,
    })
}

/// The `(directory, mode)` pairs the watcher registers for `watch_set`: the
/// config directory first (recursive only when a models base is that
/// directory), then each models directory recursively.
fn watch_registrations(config_dir: &Path, watch_set: &WatchSet) -> Vec<(PathBuf, RecursiveMode)> {
    let config_dir_mode = if watch_set.config_dir_recursive {
        RecursiveMode::Recursive
    } else {
        RecursiveMode::NonRecursive
    };
    std::iter::once((config_dir.to_path_buf(), config_dir_mode))
        .chain(
            watch_set
                .model_roots
                .iter()
                .map(|root| (root.clone(), RecursiveMode::Recursive)),
        )
        .collect()
}

/// A selected pipeline that reads no file `--watch` watches, so only an edit
/// to `rocky.toml` re-runs it.
#[derive(Debug, PartialEq, Eq)]
struct ConfigOnlyPipeline {
    name: String,
    kind: ConfigOnlyKind,
}

/// Why a [`ConfigOnlyPipeline`] has no file to watch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConfigOnlyKind {
    /// Replication (without `--all` or `--models`), quality or snapshot:
    /// everything it reads is in the warehouse or the source system, and its
    /// setup is in `rocky.toml`.
    NoModelFiles(&'static str),
    /// Load: it reads files from its `source_dir`, which `--watch` does not
    /// watch.
    SourceFilesNotWatched,
}

/// The pipeline the run selects, when it reads no file `--watch` watches.
///
/// The selection is the run's own (`registry::resolve_pipeline`): the
/// pipeline `--pipeline` names, or the only pipeline in the config. Without
/// `--pipeline`, a config with several pipelines gives none, because every
/// iteration refuses to run and says so.
///
/// A replication pipeline reads model files only when
/// `replication_reads_models` (see [`replication_models_dir`]). It has no
/// other input file to watch: its source is a Fivetran or Airbyte
/// connector, a manual table list in `rocky.toml`, or a warehouse schema.
/// A file-backed DuckDB database can be that warehouse, but the run writes
/// to it, so watching it would re-run the pipeline on its own writes. A
/// config that does not load gives none; each iteration reports the error.
fn config_only_pipelines(
    config_path: &Path,
    pipeline_name_arg: Option<&str>,
    replication_reads_models: bool,
) -> Vec<ConfigOnlyPipeline> {
    use rocky_core::config::PipelineConfig;

    let Ok(config) = rocky_core::config::load_rocky_config(config_path) else {
        return Vec::new();
    };
    if pipeline_name_arg.is_none() && config.pipelines.len() != 1 {
        return Vec::new();
    }
    config
        .pipelines
        .iter()
        .filter(|(name, _)| pipeline_name_arg.is_none_or(|selected| selected == name.as_str()))
        .filter_map(|(name, pipeline)| {
            let kind = match pipeline {
                PipelineConfig::Transformation(_) => return None,
                PipelineConfig::Replication(_) if replication_reads_models => return None,
                PipelineConfig::Load(_) => ConfigOnlyKind::SourceFilesNotWatched,
                PipelineConfig::Replication(_)
                | PipelineConfig::Quality(_)
                | PipelineConfig::Snapshot(_) => {
                    ConfigOnlyKind::NoModelFiles(pipeline.pipeline_type_str())
                }
            };
            Some(ConfigOnlyPipeline {
                name: name.clone(),
                kind,
            })
        })
        .collect()
}

/// The startup lines `rocky run --watch` prints to stderr: every path it
/// watches, then one line for each selected pipeline only a `rocky.toml`
/// edit re-runs.
fn watch_banner(
    config_path: &Path,
    config_dir: &Path,
    config_dir_recursive: bool,
    watch_set: &WatchSet,
    config_only: &[ConfigOnlyPipeline],
) -> Vec<String> {
    let watched: Vec<String> = std::iter::once(config_path.display().to_string())
        .chain(config_dir_recursive.then(|| config_dir.display().to_string()))
        .chain(
            watch_set
                .model_roots
                .iter()
                .map(|p| p.display().to_string()),
        )
        .collect();
    std::iter::once(format!(
        "[watch] watching {} (Ctrl-C to stop)",
        watched.join(", ")
    ))
    .chain(config_only.iter().map(|pipeline| match pipeline.kind {
        ConfigOnlyKind::NoModelFiles(kind) => format!(
            "[watch] pipeline '{}' is a {kind} pipeline and has no model files to watch: \
             only an edit to {} re-runs it",
            pipeline.name,
            config_path.display()
        ),
        ConfigOnlyKind::SourceFilesNotWatched => format!(
            "[watch] pipeline '{}' is a load pipeline; its source files are not watched: \
             only an edit to {} re-runs it",
            pipeline.name,
            config_path.display()
        ),
    }))
    .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use tokio::time::{Duration, Instant, sleep};
    // `Instant` re-imported here from `tokio::time` for `start.elapsed()`
    // assertions in `debounce_captures_events_during_window` — it's the
    // wall-clock variant since these tests don't pause time.

    /// A project with one replication pipeline and transformation pipelines
    /// whose models globs are `pipelines` (name, glob). Returns the canonical
    /// project root and the config path.
    fn watch_project(
        pipelines: &[(&str, &str)],
        dirs: &[&str],
    ) -> (tempfile::TempDir, PathBuf, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();
        for d in dirs {
            std::fs::create_dir_all(root.join(d)).unwrap();
        }
        let mut toml = String::from(
            "[adapter]\ntype = \"duckdb\"\npath = \"test.duckdb\"\n\n\
             [pipeline.raw]\ntype = \"replication\"\nstrategy = \"full_refresh\"\n\n\
             [pipeline.raw.source.schema_pattern]\nprefix = \"src__\"\nseparator = \"__\"\n\
             components = [\"source\"]\n\n\
             [pipeline.raw.target]\nadapter = \"default\"\n\
             catalog_template = \"wh\"\nschema_template = \"raw__{source}\"\n\n",
        );
        for (name, glob) in pipelines {
            toml.push_str(&format!(
                "[pipeline.{name}]\ntype = \"transformation\"\nmodels = \"{glob}\"\n\n\
                 [pipeline.{name}.target.governance]\nauto_create_schemas = true\n\n"
            ));
        }
        let config = root.join("rocky.toml");
        std::fs::write(&config, toml).unwrap();
        (dir, root, config)
    }

    /// Without `--models`, every transformation pipeline's models glob base
    /// is watched when no pipeline is selected. A base that does not exist
    /// and the replication pipeline add nothing.
    #[test]
    fn default_watch_set_watches_every_pipelines_models_base() {
        let (_dir, root, config) = watch_project(
            &[
                ("core", "models/**"),
                ("reporting", "reporting/*.sql"),
                ("gone", "gone/**"),
            ],
            &["models", "reporting"],
        );
        let set = default_watch_set(&config, &root, None, None);
        assert_eq!(
            set,
            WatchSet {
                config_dir_recursive: false,
                model_roots: vec![root.join("models"), root.join("reporting")],
            }
        );
    }

    /// `--pipeline` limits the watch set to that pipeline's base. An unknown
    /// or non-transformation pipeline watches only `rocky.toml`.
    #[test]
    fn default_watch_set_watches_only_the_selected_pipeline() {
        let (_dir, root, config) = watch_project(
            &[("core", "models/**"), ("reporting", "reporting/*.sql")],
            &["models", "reporting"],
        );
        assert_eq!(
            default_watch_set(&config, &root, Some("reporting"), None).model_roots,
            vec![root.join("reporting")]
        );
        assert!(
            default_watch_set(&config, &root, Some("raw"), None)
                .model_roots
                .is_empty()
        );
        assert!(
            default_watch_set(&config, &root, Some("nope"), None)
                .model_roots
                .is_empty()
        );
    }

    /// A base shared by two pipelines, or inside another pipeline's base, is
    /// watched once.
    #[test]
    fn default_watch_set_drops_duplicate_and_nested_bases() {
        let (_dir, root, config) = watch_project(
            &[
                ("a", "models/**"),
                ("b", "models/*.sql"),
                ("c", "models/staging/**"),
            ],
            &["models/staging"],
        );
        let set = default_watch_set(&config, &root, None, None);
        assert_eq!(set.model_roots, vec![root.join("models")]);
        assert!(!set.config_dir_recursive);
        // The nested base is still watched when it is the only one selected.
        assert_eq!(
            default_watch_set(&config, &root, Some("c"), None).model_roots,
            vec![root.join("models").join("staging")]
        );
    }

    /// A models base equal to the config directory does not add a second
    /// watch on it: the config directory is watched once, recursively, and
    /// covers every other base inside it.
    #[test]
    fn default_watch_set_does_not_watch_the_config_dir_twice() {
        let (_dir, root, config) =
            watch_project(&[("flat", "./*.sql"), ("core", "models/**")], &["models"]);
        let set = default_watch_set(&config, &root, None, None);
        assert_eq!(
            set,
            WatchSet {
                config_dir_recursive: true,
                model_roots: Vec::new(),
            }
        );
        // A base inside the config dir is kept while that watch is
        // non-recursive: it is the normal layout.
        assert_eq!(
            default_watch_set(&config, &root, Some("core"), None),
            WatchSet {
                config_dir_recursive: false,
                model_roots: vec![root.join("models")],
            }
        );
    }

    /// An explicit `--models` watches that directory only, as before: no
    /// pipeline's directory is added. A `--models` that does not exist
    /// watches only `rocky.toml`. Without `--models`, the pipelines' bases.
    #[test]
    fn explicit_models_keeps_todays_watch_set() {
        let (_dir, root, config) = watch_project(
            &[("core", "models/**"), ("reporting", "reporting/*.sql")],
            &["models", "reporting", "elsewhere"],
        );
        let explicit = root.join("elsewhere");
        assert_eq!(
            resolve_watch_set(Some(&explicit), &config, &root, None, None).unwrap(),
            WatchSet {
                config_dir_recursive: false,
                model_roots: vec![explicit],
            }
        );
        // `--models` naming the config dir keeps today's two watches.
        assert_eq!(
            resolve_watch_set(Some(&root), &config, &root, None, None).unwrap(),
            WatchSet {
                config_dir_recursive: false,
                model_roots: vec![root.clone()],
            }
        );
        assert_eq!(
            resolve_watch_set(Some(&root.join("missing")), &config, &root, None, None).unwrap(),
            WatchSet {
                config_dir_recursive: false,
                model_roots: Vec::new(),
            }
        );
        assert_eq!(
            resolve_watch_set(None, &config, &root, None, None).unwrap(),
            default_watch_set(&config, &root, None, None)
        );
    }

    /// A replication pipeline is named as config-only, so the banner says
    /// that only a `rocky.toml` edit re-runs it. A transformation pipeline is
    /// not. The selection is the run's: the pipeline `--pipeline` names, or
    /// the only one. With several pipelines and no `--pipeline` the run
    /// refuses, so none is named.
    #[test]
    fn config_only_pipelines_names_the_replication_pipeline() {
        let (_dir, root, config) = watch_project(&[("core", "models/**")], &["models"]);
        let raw = vec![ConfigOnlyPipeline {
            name: "raw".to_string(),
            kind: ConfigOnlyKind::NoModelFiles("replication"),
        }];
        assert!(config_only_pipelines(&config, None, false).is_empty());
        assert_eq!(config_only_pipelines(&config, Some("raw"), false), raw);
        let (_only, _, only_raw) = watch_project(&[], &[]);
        assert_eq!(config_only_pipelines(&only_raw, None, false), raw);
        assert!(config_only_pipelines(&config, Some("core"), false).is_empty());
        assert!(config_only_pipelines(&config, Some("nope"), false).is_empty());
        // A config that does not load names nothing: the run reports it.
        assert!(config_only_pipelines(&root.join("missing.toml"), None, false).is_empty());
    }

    /// `--watch --all` over a project with only a replication pipeline
    /// watches `rocky.toml` alone, and the banner says why: the pipeline has
    /// no model files. Before, it listed `rocky.toml` and said nothing more.
    #[test]
    fn the_banner_says_a_replication_pipeline_has_no_model_files() {
        let (_dir, root, config) = watch_project(&[], &[]);
        let set = resolve_watch_set(None, &config, &root, None, None).unwrap();
        assert!(set.model_roots.is_empty());
        assert!(!set.config_dir_recursive);
        let lines = watch_banner(
            &config,
            &root,
            set.config_dir_recursive,
            &set,
            &config_only_pipelines(&config, None, false),
        );
        assert_eq!(
            lines,
            vec![
                format!("[watch] watching {} (Ctrl-C to stop)", config.display()),
                format!(
                    "[watch] pipeline 'raw' is a replication pipeline and has no model files \
                     to watch: only an edit to {} re-runs it",
                    config.display()
                ),
            ]
        );
    }

    /// With `--all` (or `--models`), a replication run also runs the compiled
    /// models in that directory, so `--watch` watches it, and the pipeline is
    /// not named as config-only. Without either flag it reads no model file.
    #[test]
    fn a_replication_run_with_models_watches_its_models_dir() {
        let (_dir, root, config) = watch_project(&[], &["models"]);
        let models = root.join("models");
        assert_eq!(
            default_watch_set(&config, &root, None, Some(&models)),
            WatchSet {
                config_dir_recursive: false,
                model_roots: vec![models.clone()],
            }
        );
        assert!(config_only_pipelines(&config, None, true).is_empty());
        assert!(
            default_watch_set(&config, &root, None, None)
                .model_roots
                .is_empty()
        );
        assert_eq!(config_only_pipelines(&config, None, false).len(), 1);
        // Through the entry point the watcher calls, as `--models` would.
        assert_eq!(
            resolve_watch_set(Some(&models), &config, &root, None, Some(&models))
                .unwrap()
                .model_roots,
            vec![models.clone()]
        );
    }

    /// `replication_models_dir` follows the run's rule: `--models` when it
    /// exists, else `models` from the working directory with `--all`, else
    /// nothing. A `--models` that does not exist reads nothing.
    #[test]
    fn replication_models_dir_follows_the_runs_rule() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();
        assert_eq!(
            replication_models_dir(Some(&root), false),
            Some(root.clone())
        );
        assert_eq!(
            replication_models_dir(Some(&root), true),
            Some(root.clone())
        );
        assert_eq!(
            replication_models_dir(Some(&root.join("missing")), true),
            None
        );
        assert_eq!(replication_models_dir(None, false), None);
    }

    /// A load pipeline reads files the watcher does not see; the banner says
    /// so rather than calling it a pipeline without input files.
    #[test]
    fn the_banner_says_a_load_pipelines_source_files_are_not_watched() {
        let config = PathBuf::from("/proj/rocky.toml");
        let set = WatchSet {
            config_dir_recursive: false,
            model_roots: vec![PathBuf::from("/proj/models")],
        };
        let lines = watch_banner(
            &config,
            Path::new("/proj"),
            false,
            &set,
            &[ConfigOnlyPipeline {
                name: "seeds".to_string(),
                kind: ConfigOnlyKind::SourceFilesNotWatched,
            }],
        );
        assert_eq!(
            lines,
            vec![
                "[watch] watching /proj/rocky.toml, /proj/models (Ctrl-C to stop)".to_string(),
                "[watch] pipeline 'seeds' is a load pipeline; its source files are not \
                 watched: only an edit to /proj/rocky.toml re-runs it"
                    .to_string(),
            ]
        );
    }

    /// A recursive watch of the project root ignores files under `.git/`,
    /// `.venv/`, `target/` and `node_modules/`, at any depth, but still
    /// accepts a model under the root. A watched directory may itself carry
    /// such a name: only components below it are checked.
    #[test]
    fn rerun_filter_ignores_tool_and_hidden_dirs() {
        let root = PathBuf::from("/proj");
        let name = std::ffi::OsString::from("rocky.toml");
        let roots = vec![
            PathBuf::from("/proj/models"),
            PathBuf::from("/proj/.models"),
            PathBuf::from("/srv/target"),
        ];
        let triggers = |p: &str| event_triggers_rerun(&PathBuf::from(p), &root, &name, &roots);
        for ignored in [
            "/proj/.git/config.toml",
            "/proj/.venv/lib/pyproject.toml",
            "/proj/target/debug/build/Cargo.toml",
            "/proj/node_modules/pkg/x.sql",
            "/proj/vendor/node_modules/pkg/x.sql",
            "/proj/models/.cache/a.sql",
            "/srv/target/sub/target/a.sql",
        ] {
            assert!(!triggers(ignored), "{ignored} must not re-run");
        }
        for accepted in [
            "/proj/rocky.toml",
            "/proj/a.sql",
            "/proj/models/a.sql",
            "/proj/models/staging/a.toml",
            "/srv/target/a.sql",
            "/srv/target/sub/a.rocky",
            // A watched root inside the config dir is the base, not the
            // config dir, so its own leading `.` is not checked.
            "/proj/.models/a.sql",
        ] {
            assert!(triggers(accepted), "{accepted} must re-run");
        }
    }

    /// The watcher registers the config dir first, non-recursively unless a
    /// models base is that directory, then each models root recursively.
    #[test]
    fn watch_registrations_match_the_watch_set() {
        let root = PathBuf::from("/proj");
        assert_eq!(
            watch_registrations(
                &root,
                &WatchSet {
                    config_dir_recursive: false,
                    model_roots: vec![root.join("models"), root.join("reporting")],
                }
            ),
            vec![
                (root.clone(), RecursiveMode::NonRecursive),
                (root.join("models"), RecursiveMode::Recursive),
                (root.join("reporting"), RecursiveMode::Recursive),
            ]
        );
        assert_eq!(
            watch_registrations(
                &root,
                &WatchSet {
                    config_dir_recursive: true,
                    model_roots: Vec::new(),
                }
            ),
            vec![(root.clone(), RecursiveMode::Recursive)]
        );
    }

    /// A config that does not load still starts the watcher: only
    /// `rocky.toml` is watched, so a fix re-runs.
    #[test]
    fn default_watch_set_is_empty_for_a_config_that_does_not_load() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();
        std::fs::create_dir_all(root.join("models")).unwrap();
        let config = root.join("rocky.toml");
        std::fs::write(&config, "[pipeline.core\n").unwrap();
        assert_eq!(
            default_watch_set(&config, &root, None, None),
            WatchSet {
                config_dir_recursive: false,
                model_roots: Vec::new(),
            }
        );
    }

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

    /// WP-01 PR-B (#1120 / watch live-reload): `iter_once` loads the config
    /// FRESH each iteration — two iterations straddling a `rocky.toml` edit
    /// observe different fingerprints, and a broken config fails only its own
    /// iteration (returns `None`, never panics/propagates). This is the
    /// executable pin for the "never hoist one load above the loop" comment
    /// in `iter_once`.
    ///
    /// The inner `run()` fails fast here (no pipeline is configured) — that's
    /// fine: `iter_once` swallows run errors by contract, and the property
    /// under test is the per-iteration config load, which happens before it.
    /// Write `body` to the watched config and drive one watch iteration,
    /// returning the fingerprint that iteration loaded (`iter_once`'s own
    /// return).
    async fn drive_iteration(config_path: &Path, state_path: &Path, body: &str) -> Option<String> {
        std::fs::write(config_path, body).unwrap();
        iter_once(
            config_path,
            None,
            None,
            state_path,
            None,
            false,
            None,
            false,
            None,
            &PartitionRunOptions::default(),
            None,
            None,
            &super::super::run::SkipRunOptions::default(),
            &rocky_core::run_vars::RunVars::new(),
            &rocky_core::config::PrincipalRef::unnamed(),
        )
        .await
        .fingerprint
    }

    /// `--var` reaches the run a watch iteration drives (#2379). This calls
    /// `iter_once` directly, so one cycle runs with no filesystem events or
    /// sleeps. The `--watch` loop hands the same `run_vars` to every
    /// iteration; the loop itself is covered by `rocky/tests/run_watch.rs`.
    ///
    /// Mutation that must turn this red: pass `RunVars::new()` instead of
    /// `run_vars` to `run()` in `iter_once`.
    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn iter_once_passes_var_to_the_run() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::create_dir_all(root.join("models")).unwrap();
        let db_path = root.join("db.duckdb");
        let config_path = root.join("rocky.toml");
        std::fs::write(
            &config_path,
            format!(
                "[adapter.local]\ntype = \"duckdb\"\npath = \"{}\"\n\n\
                 [pipeline.t]\ntype = \"transformation\"\nmodels = \"models/**\"\n\n\
                 [pipeline.t.target]\nadapter = \"local\"\n",
                db_path.display()
            ),
        )
        .unwrap();
        std::fs::write(
            root.join("models/m.sql"),
            "SELECT '@var(region)' AS region, '@var(tag, dflt)' AS tag\n",
        )
        .unwrap();
        std::fs::write(
            root.join("models/m.toml"),
            "name = \"m\"\n\n[strategy]\ntype = \"full_refresh\"\n\n\
             [target]\ncatalog = \"db\"\nschema = \"main\"\ntable = \"m\"\n",
        )
        .unwrap();

        let vars = rocky_core::run_vars::RunVars::parse_pairs(&[
            "region=emea".to_string(),
            "tag=supplied".to_string(),
        ])
        .unwrap();
        let outcome = iter_once(
            &config_path,
            None,
            None,
            &root.join(".rocky-state.redb"),
            None,
            false,
            Some(&root.join("models")),
            false,
            None,
            &PartitionRunOptions::default(),
            None,
            None,
            &super::super::run::SkipRunOptions::default(),
            &vars,
            &rocky_core::config::PrincipalRef::unnamed(),
        )
        .await;
        assert!(!outcome.interrupted);

        let adapter = rocky_duckdb::adapter::DuckDbWarehouseAdapter::open(&db_path).unwrap();
        let conn = adapter.shared_connector();
        let guard = conn.lock().unwrap();
        let rows = guard
            .execute_sql("SELECT region, tag FROM db.main.m")
            .unwrap();
        assert_eq!(rows.rows.len(), 1);
        assert_eq!(rows.rows[0][0], "emea");
        assert_eq!(rows.rows[0][1], "supplied");
    }

    #[tokio::test]
    async fn iter_once_reloads_the_config_every_iteration() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("rocky.toml");
        let state_path = dir.path().join(".rocky-state.redb");

        let fp1 = drive_iteration(
            &config_path,
            &state_path,
            "[adapter.db]\ntype = \"duckdb\"\npath = \"wh.duckdb\"\n# v1\n",
        )
        .await;
        let fp2 = drive_iteration(
            &config_path,
            &state_path,
            "[adapter.db]\ntype = \"duckdb\"\npath = \"wh.duckdb\"\n# v2\n",
        )
        .await;
        assert!(
            fp1.is_some() && fp2.is_some(),
            "both iterations must load their config snapshot"
        );
        assert_ne!(
            fp1, fp2,
            "an edit between iterations must be observed by the next iteration \
             (fresh load per iteration — the load is never hoisted above the loop)"
        );

        // A broken config fails only this iteration: no panic, no propagation.
        let fp3 = drive_iteration(&config_path, &state_path, "this is [not toml").await;
        assert_eq!(fp3, None, "a broken config yields no snapshot");

        // ...and the next iteration recovers with a fresh, successful load.
        let fp4 = drive_iteration(
            &config_path,
            &state_path,
            "[adapter.db]\ntype = \"duckdb\"\npath = \"wh.duckdb\"\n# v1\n",
        )
        .await;
        assert_eq!(
            fp4, fp1,
            "identical bytes reproduce the identical fingerprint"
        );
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
