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
//! The token's **scope** comes from the same shape, so an operator configures
//! it the way they already configure the secret:
//!
//! 1. `--token-scope <full|read-only>` flag
//! 2. `ROCKY_SERVE_TOKEN_SCOPE` env var
//! 3. `full` when neither is set — the historical all-or-nothing token, so
//!    existing deployments are unchanged.
//!
//! `read-only` authenticates exactly like `full` but is refused `403` on any
//! request whose HTTP method is not safe (`GET`, `HEAD`, `OPTIONS`). That is
//! the token a browser UI should hold: a leak of it cannot reach
//! `POST /api/v1/jobs/run` and through it the warehouse.
//!
//! It does not gate the webhook ingress, which is Bearer-exempt. Under
//! `--scheduler` on a loopback bind with no `ROCKY_WEBHOOK_SECRET` that route
//! accepts unsigned `POST`s by dev convenience, so same-origin script can
//! spool work with no token at all. Set `ROCKY_WEBHOOK_SECRET` when a browser
//! can reach this server.
//!
//! A scope with no token is an **error**, not a silent no-op: the operator
//! asked to restrict something that would otherwise stay fully mutable.
//!
//! There is no `[serve]` section in `rocky.toml` — `rocky-core/src/config.rs`
//! defines none — so flag-plus-env is the idiom `serve` already uses for every
//! one of its knobs (`--token`, `--allowed-origin`, `--host`). A new config
//! shape for one field would be net-new surface, and would split where an
//! operator looks for the secret and where they look for its scope.
//!
//! Cross-origin clients must be enumerated via `--allowed-origin`. The
//! default allowlist is empty (same-origin only); the dashboard at `/`
//! is server-rendered HTML and doesn't need cross-origin XHR.

use std::path::Path;

use anyhow::Result;

use rocky_server::auth::{ServeToken, TokenScope};

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
    // The raw `--token-scope` value, unparsed. `None` falls back to
    // `ROCKY_SERVE_TOKEN_SCOPE`, then to `TokenScope::Full`.
    token_scope: Option<String>,
    allowed_origins: Vec<String>,
    scheduler: bool,
    poll_interval_seconds: Option<u64>,
    drain_timeout_seconds: Option<u64>,
    state_path: Option<&Path>,
) -> Result<()> {
    // Token resolution: --token takes precedence over the env var so
    // CI / scripts can override an inherited environment.
    let secret = auth_token.or_else(|| std::env::var("ROCKY_SERVE_TOKEN").ok());
    let token = resolve_serve_token(secret, token_scope)?;

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

/// Pair the resolved Bearer secret with its [`TokenScope`].
///
/// Mirrors the secret's own resolution order — flag, then env var, then a
/// default — so there is one idiom to learn. Two things fail closed here:
///
/// - An unparseable scope is an error, never a fall back to `Full`. clap
///   validates `--token-scope`, but nothing validates the env var, and a typo
///   there ("readonly", "read_only") must not silently hand out full access.
/// - A scope with no secret is an error. The operator asked to restrict a
///   token that does not exist; accepting it would leave the server fully
///   mutable while looking configured. This fires for the env var too: a
///   globally exported `ROCKY_SERVE_TOKEN_SCOPE` with no token is exactly the
///   confusion worth refusing rather than ignoring, and the message names the
///   two ways out.
fn resolve_serve_token(
    secret: Option<String>,
    token_scope: Option<String>,
) -> Result<Option<ServeToken>> {
    let scope = match token_scope.or_else(|| std::env::var("ROCKY_SERVE_TOKEN_SCOPE").ok()) {
        Some(raw) => Some(raw.parse::<TokenScope>()?),
        None => None,
    };
    pair_token_with_scope(secret, scope)
}

/// The decision half of [`resolve_serve_token`], with the environment read
/// already done. Split out so its four cases are testable without mutating
/// process-global env vars from a parallel test binary.
fn pair_token_with_scope(
    secret: Option<String>,
    scope: Option<TokenScope>,
) -> Result<Option<ServeToken>> {
    match (secret, scope) {
        (Some(secret), scope) => Ok(Some(ServeToken {
            secret,
            // No scope named → `Full`, the historical all-or-nothing token.
            scope: scope.unwrap_or_default(),
        })),
        (None, Some(_)) => anyhow::bail!(
            "--token-scope (or ROCKY_SERVE_TOKEN_SCOPE) was set but no token was. \
             A scope only restricts a configured token. Pass --token <secret> \
             (or set ROCKY_SERVE_TOKEN), or bind to 127.0.0.1 and drop the scope."
        ),
        (None, None) => Ok(None),
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A token with no scope named keeps the historical behaviour.
    #[test]
    fn a_token_without_a_scope_is_full() {
        let token = pair_token_with_scope(Some("s3cret".into()), None)
            .unwrap()
            .expect("a secret yields a token");
        assert_eq!(token.secret, "s3cret");
        assert_eq!(token.scope, TokenScope::Full);
    }

    #[test]
    fn a_named_scope_is_carried_onto_the_token() {
        let token = pair_token_with_scope(Some("s3cret".into()), Some(TokenScope::ReadOnly))
            .unwrap()
            .expect("a secret yields a token");
        assert_eq!(token.scope, TokenScope::ReadOnly);
    }

    /// A scope with no token is refused, not ignored. Accepting it would leave
    /// the server fully mutable while looking configured — the operator asked
    /// to restrict something and would get no restriction and no warning.
    #[test]
    fn a_scope_without_a_token_is_an_error() {
        let err = pair_token_with_scope(None, Some(TokenScope::ReadOnly))
            .expect_err("a scope with no token must not be silently dropped");
        let msg = err.to_string();
        assert!(msg.contains("--token-scope"), "{msg}");
        assert!(msg.contains("ROCKY_SERVE_TOKEN_SCOPE"), "{msg}");
    }

    /// Neither set → loopback-only mode, exactly as before.
    #[test]
    fn neither_token_nor_scope_is_no_auth() {
        assert!(pair_token_with_scope(None, None).unwrap().is_none());
    }

    /// An unparseable scope is an error rather than a fall back to `Full`.
    /// This is the case that matters on the env path: clap validates the flag,
    /// nothing validates `ROCKY_SERVE_TOKEN_SCOPE`.
    #[test]
    fn an_unparseable_scope_is_an_error() {
        let err = resolve_serve_token(Some("s3cret".into()), Some("readonly".into()))
            .expect_err("a typo must not resolve to a full-scope token");
        assert!(err.to_string().contains("unknown token scope"), "{err}");
    }
}
