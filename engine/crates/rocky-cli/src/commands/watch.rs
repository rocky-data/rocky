//! `rocky watch` — monitor models directory and auto-recompile on changes.

use std::path::Path;
use std::time::Duration;

use anyhow::Result;
use notify::{Event, EventKind, RecursiveMode, Watcher};
use tokio::sync::mpsc;
use tracing::{info, warn};

use super::compile::run_compile;

/// Execute `rocky watch`.
///
/// Watches `models_dir` for `.sql`, `.rocky`, and `.toml` file changes,
/// debounces for 200 ms, then runs `rocky compile`. Blocks until Ctrl-C.
///
/// `state_namespace` is the resolved `--state-namespace` (or
/// `[state] namespacing`) value threaded from `main.rs`, so the compile-time
/// schema-cache read sees the same namespaced state file that `rocky run`
/// targets. `None` (the default) keeps the global file, byte-identical to
/// before.
pub async fn run_watch(
    models_dir: &Path,
    contracts_dir: Option<&Path>,
    state_namespace: Option<&str>,
    output_json: bool,
) -> Result<()> {
    // Validate the models directory exists before starting the watcher.
    if !models_dir.is_dir() {
        anyhow::bail!("models directory does not exist: {}", models_dir.display());
    }

    // Initial compile so the user gets immediate feedback.
    println!("[watch] compiling...");
    print_compile_result(models_dir, contracts_dir, state_namespace, output_json);
    println!("[watch] waiting for changes...");

    // Channel for watcher → debounce task communication.
    let (tx, mut rx) = mpsc::channel::<Vec<String>>(16);

    // Set up the filesystem watcher.
    let mut watcher =
        notify::recommended_watcher(move |res: Result<Event, notify::Error>| match res {
            Ok(event) => {
                let dominated = matches!(
                    event.kind,
                    EventKind::Create(_) | EventKind::Modify(_) | EventKind::Remove(_)
                );
                let model_paths: Vec<String> = event
                    .paths
                    .iter()
                    .filter(|p| {
                        p.extension()
                            .is_some_and(|ext| ext == "sql" || ext == "rocky" || ext == "toml")
                    })
                    .map(|p| p.display().to_string())
                    .collect();

                if dominated && !model_paths.is_empty() {
                    let _ = tx.try_send(model_paths);
                }
            }
            Err(e) => warn!(error = %e, "watch error"),
        })?;

    watcher.watch(models_dir, RecursiveMode::Recursive)?;

    info!(
        dir = %models_dir.display(),
        "watching for file changes (Ctrl-C to stop)"
    );

    // Debounce + recompile loop.
    loop {
        tokio::select! {
            Some(paths) = rx.recv() => {
                // Debounce: wait 200 ms, then drain any additional events
                // that arrived during that window.
                tokio::time::sleep(Duration::from_millis(200)).await;

                let mut all_paths = paths;
                while let Ok(more) = rx.try_recv() {
                    all_paths.extend(more);
                }

                // Deduplicate for cleaner output.
                all_paths.sort();
                all_paths.dedup();

                for p in &all_paths {
                    println!("[watch] file changed: {p}");
                }

                println!("[watch] compiling...");
                print_compile_result(models_dir, contracts_dir, state_namespace, output_json);
                println!("[watch] waiting for changes...");
            }
            _ = tokio::signal::ctrl_c() => {
                println!("\n[watch] stopped");
                return Ok(());
            }
        }
    }
}

/// Run compile and print the result. Errors are printed, not propagated,
/// so the watch loop continues after a failed compilation.
fn print_compile_result(
    models_dir: &Path,
    contracts_dir: Option<&Path>,
    state_namespace: Option<&str>,
    output_json: bool,
) {
    // Watch doesn't take a `--state-path` arg today; resolve via the
    // namespace-aware helper so the compile-time schema-cache read sees the
    // same file that `rocky run` and the LSP see. The resolver is keyed off
    // this command's `models_dir` (which honors `--models`), not the global
    // `"models"` default, so threading the namespace here — rather than the
    // already-resolved global state_path — preserves `--models`-relative
    // resolution. Fresh projects with no runs yet still land on the default
    // (`<models>/.rocky-state.redb` or `<models>/.rocky-state/<ns>.redb`) and
    // the cache loader gracefully returns an empty map.
    let resolved = rocky_core::state::resolve_state_path_ns(None, models_dir, state_namespace);
    if let Some(ref w) = resolved.warning {
        warn!(target: "rocky::state_path", "{w}");
    }
    match run_compile(
        None,
        &resolved.path,
        models_dir,
        contracts_dir,
        None,
        output_json,
        false,
        None,
        false,
        // Watch uses the config-derived / default TTL. Wiring a CLI
        // `--cache-ttl` through a background recompile loop would tie
        // the override's lifetime to the loop; users who want stricter
        // freshness in a watch session should set `[cache.schemas]
        // ttl_seconds` in `rocky.toml` instead.
        None,
        // `rocky compile --watch` does not expose `--var`.
        &rocky_core::run_vars::RunVars::new(),
    ) {
        Ok(()) => {
            if !output_json {
                println!("[watch] compilation succeeded");
            }
        }
        Err(e) => {
            // `run_compile` already printed diagnostics; just note the failure.
            println!("[watch] compilation failed: {e}");
        }
    }
}
