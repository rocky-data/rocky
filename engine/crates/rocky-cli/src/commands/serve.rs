//! `rocky serve` — HTTP API server exposing the compiler's semantic graph.
//!
//! # Security defaults
//!
//! The server binds `127.0.0.1:8080` by default. Binding a non-loopback
//! host (e.g. `--host 0.0.0.0`) requires a Bearer token — otherwise the
//! server refuses to start so model SQL, file paths, and run history
//! don't leak to the LAN. Token sources, in priority order:
//!
//! 1. `--token <secret>` flag
//! 2. `ROCKY_SERVE_TOKEN` env var
//!
//! Cross-origin clients must be enumerated via `--allowed-origin`. The
//! default allowlist is empty (same-origin only); the dashboard at `/`
//! is server-rendered HTML and doesn't need cross-origin XHR.

use std::path::Path;

use anyhow::Result;

/// Execute `rocky serve`.
///
/// When `scheduler` is set, a resident reconciler loop runs alongside the HTTP
/// server (see [`crate::commands::scheduler`]): both share one shutdown signal,
/// so a SIGTERM/ctrl-c gracefully drains in-flight HTTP requests AND a running
/// scheduled child before the process exits.
#[allow(clippy::too_many_arguments)]
pub async fn run_serve(
    models_dir: &Path,
    contracts_dir: Option<&Path>,
    config_path: Option<&Path>,
    host: String,
    port: u16,
    watch: bool,
    auth_token: Option<String>,
    allowed_origins: Vec<String>,
    scheduler: bool,
    poll_interval_seconds: Option<u64>,
    drain_timeout_seconds: Option<u64>,
) -> Result<()> {
    // Token resolution: --token takes precedence over the env var so
    // CI / scripts can override an inherited environment.
    let token = auth_token.or_else(|| std::env::var("ROCKY_SERVE_TOKEN").ok());

    let state = rocky_server::state::ServerState::with_auth(
        models_dir.to_path_buf(),
        contracts_dir.map(std::path::Path::to_path_buf),
        config_path.map(std::path::Path::to_path_buf),
        token,
        allowed_origins,
    );

    // Start filesystem watcher if requested
    let _watcher = if watch {
        Some(rocky_server::watch::start_watcher(
            state.clone(),
            models_dir,
        )?)
    } else {
        None
    };

    // Wait for initial compilation
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // One shutdown/drain signal shared by the HTTP server and the scheduler loop.
    // SIGTERM/ctrl-c raises it; axum drains connections and the reconciler drains
    // its in-flight child before either returns.
    let shutdown = rocky_core::schedule::Drain::new();
    {
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            wait_for_shutdown().await;
            tracing::info!("shutdown signal received; draining");
            shutdown.signal();
        });
    }

    // Spawn the resident reconciler alongside the server, if requested.
    let scheduler_task = if scheduler {
        let sched_cfg = crate::commands::scheduler::SchedulerConfig {
            poll_interval: poll_interval_seconds
                .map(std::time::Duration::from_secs)
                .unwrap_or(crate::commands::scheduler::DEFAULT_POLL_INTERVAL),
            drain_timeout: drain_timeout_seconds
                .map(std::time::Duration::from_secs)
                .unwrap_or(crate::commands::scheduler::DEFAULT_DRAIN_TIMEOUT),
        };
        // Read schedules from the config file the server bound (falls back to the
        // conventional `rocky.toml`); a missing/invalid file just idles the loop.
        let resolved_config = config_path
            .map(std::path::Path::to_path_buf)
            .unwrap_or_else(|| std::path::PathBuf::from("rocky.toml"));
        Some(crate::commands::scheduler::spawn_scheduler(
            state.clone(),
            resolved_config,
            sched_cfg,
            shutdown.clone(),
        ))
    } else {
        None
    };

    let result = crate::api::serve(state, crate::api::ServeConfig { host, port }, shutdown.clone()).await;

    // The server has stopped (graceful shutdown, or a bind/runtime error). Ensure
    // the drain is raised so the scheduler stops evaluating, then wait for it to
    // finish draining any in-flight child before returning.
    shutdown.signal();
    if let Some(task) = scheduler_task
        && let Err(e) = task.await
    {
        tracing::warn!(error = %e, "scheduler task did not shut down cleanly");
    }
    result
}

/// Resolve when the process should begin draining: a SIGTERM (unix) or a ctrl-c
/// (SIGINT, all platforms). Mirrors the two-signal handling `rocky run` uses.
async fn wait_for_shutdown() {
    #[cfg(unix)]
    {
        let mut term = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            Ok(s) => s,
            Err(_) => {
                // Fall back to ctrl-c only if SIGTERM can't be registered.
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
