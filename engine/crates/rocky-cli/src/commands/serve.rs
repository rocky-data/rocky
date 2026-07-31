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

/// The fixed webhook-ingress request rate (requests/second), with an equal
/// burst. A flood guard shared across all callers, not a per-sender quota.
const WEBHOOK_RATE_LIMIT_RPS: f64 = 10.0;

/// Execute `rocky serve`.
///
/// When `scheduler` is set, a resident reconciler loop runs alongside the HTTP
/// server (see [`crate::commands::scheduler`]): both share one shutdown signal,
/// so a SIGTERM/ctrl-c gracefully drains in-flight HTTP requests AND a running
/// scheduled child before the process exits.
#[allow(clippy::too_many_arguments)]
pub async fn run_serve(
    models_dir: &Path,
    // Whether `models_dir` came from an explicit `--models`, as opposed to the
    // conventional default. Only `GET /api/v1/dag` distinguishes them; see
    // `rocky_server::state::ServerState::models_dir_is_explicit`.
    models_dir_is_explicit: bool,
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
    state_path: Option<&Path>,
) -> Result<()> {
    // Token resolution: --token takes precedence over the env var so
    // CI / scripts can override an inherited environment.
    let token = auth_token.or_else(|| std::env::var("ROCKY_SERVE_TOKEN").ok());

    // The config file the scheduler reads (falls back to the conventional
    // `rocky.toml`); the webhook spool is anchored under its `.rocky` directory,
    // so the accept path and the reconciler agree on one spool.
    let resolved_config = config_path
        .map(std::path::Path::to_path_buf)
        .unwrap_or_else(|| std::path::PathBuf::from("rocky.toml"));

    // Webhook ingress is live only alongside a resident reconciler (`--scheduler`)
    // — nothing else would consume a spooled demand. The secret comes from
    // `ROCKY_WEBHOOK_SECRET`; without one the route stays dark unless the bind is
    // loopback (dev convenience).
    let webhook = if scheduler {
        Some(rocky_server::webhook_ingress::WebhookIngress {
            secret: std::env::var("ROCKY_WEBHOOK_SECRET")
                .ok()
                .filter(|s| !s.is_empty()),
            bind_is_loopback: crate::api::is_loopback(&host),
            rocky_dir: crate::commands::scheduler::rocky_dir_for_config(&resolved_config),
            rate_limiter: rocky_server::webhook_ingress::WebhookRateLimiter::new(
                WEBHOOK_RATE_LIMIT_RPS,
            ),
        })
    } else {
        None
    };

    let state = rocky_server::state::ServerState::with_auth_and_webhook(
        models_dir.to_path_buf(),
        models_dir_is_explicit,
        contracts_dir.map(std::path::Path::to_path_buf),
        config_path.map(std::path::Path::to_path_buf),
        token,
        allowed_origins,
        state_path.map(std::path::Path::to_path_buf),
        webhook,
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
    // A readiness latch the server raises once its startup job sweep is done and
    // the listener is bound; the scheduler awaits it before its first tick so a
    // scheduled run never precedes (or outlives a failed) server startup.
    let server_ready = rocky_core::schedule::Drain::new();
    {
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            wait_for_shutdown().await;
            tracing::info!("shutdown signal received; draining");
            shutdown.signal();
        });
    }

    // Process-lifetime scheduler metrics. Stood up only under `--scheduler`, and
    // only actually exporting when `OTEL_EXPORTER_OTLP_ENDPOINT` is set — a no-op
    // guard otherwise, installing no meter provider. Bound at function scope so
    // its `Drop` (a final flush + shutdown) runs AFTER the scheduler task is
    // awaited below, letting the last tick's metrics reach the collector before
    // the provider closes.
    let meter_guard = if scheduler {
        rocky_observe::scheduler_metrics::SchedulerMeterGuard::init_if_enabled()
    } else {
        rocky_observe::scheduler_metrics::SchedulerMeterGuard::disabled()
    };

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
        Some(crate::commands::scheduler::spawn_scheduler(
            state.clone(),
            resolved_config.clone(),
            sched_cfg,
            shutdown.clone(),
            server_ready.clone(),
            meter_guard.metrics(),
        ))
    } else {
        None
    };

    let result = crate::api::serve(
        state,
        crate::api::ServeConfig { host, port },
        shutdown.clone(),
        server_ready,
    )
    .await;

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
        let mut term =
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
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
