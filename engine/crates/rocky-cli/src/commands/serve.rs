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
    // The whole flags -> token -> ServerState segment lives in
    // `build_serve_state` so a test can cross the SAME code production runs.
    // Previously the wire test called `resolve_serve_token` and then built its
    // own `ServerState`, which left this handoff unobserved: replacing the
    // token with `ServeToken::full(t.secret)` right here survived the entire
    // suite (the helper tests still saw `ReadOnly`, the router tests still
    // built read-only tokens by hand, and production always installed `Full`).
    // Also needed below by the scheduler; `build_serve_state` derives its own
    // copy for the webhook spool. Same expression, deliberately — see the
    // note on `rocky_dir_for_config`.
    let resolved_config = config_path
        .map(std::path::Path::to_path_buf)
        .unwrap_or_else(|| std::path::PathBuf::from("rocky.toml"));

    let state = build_serve_state(
        models_dir,
        models_dir_is_explicit,
        contracts_dir,
        config_path,
        &host,
        auth_token,
        token_scope,
        allowed_origins,
        scheduler,
        state_path,
    )?;

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

/// Read `name` from the environment, treating a value that is *set but not
/// valid Unicode* as an error rather than as absent.
///
/// The obvious spelling — `std::env::var(name).ok()` — collapses
/// `NotPresent` and `NotUnicode` into `None`, and both of this command's
/// security-relevant env vars fail **open** on that collapse: a mangled
/// `ROCKY_SERVE_TOKEN` would serve with no auth at all on a loopback bind,
/// a mangled `ROCKY_SERVE_TOKEN_SCOPE` would silently grant `Full`, and a
/// mangled `ROCKY_WEBHOOK_SECRET` would re-open the unsigned-webhook path. In
/// both cases the operator set the variable and gets less protection than
/// they asked for, with no diagnostic. Refusing to start is the only honest
/// answer: the variable is set, and Rocky cannot tell what it says.
/// `ROCKY_WEBHOOK_SECRET`, refusing every value that cannot authenticate.
///
/// Separate from [`env_var_fail_closed`] only because an empty secret is a
/// distinct error worth its own message: unlike a bearer token, an empty HMAC
/// key silently re-opens the unsigned-webhook path on a loopback bind.
fn webhook_secret_fail_closed() -> Result<Option<String>> {
    match env_var_fail_closed("ROCKY_WEBHOOK_SECRET")? {
        Some(s) if s.trim().is_empty() => anyhow::bail!(
            "ROCKY_WEBHOOK_SECRET is set but empty, so it cannot sign or verify \
             anything. Refusing to start: on a loopback bind an absent secret \
             makes the webhook accept UNSIGNED requests, which is not what \
             setting the variable asked for. Give it a value or unset it."
        ),
        other => Ok(other),
    }
}

/// A bearer secret that is present but blank cannot authenticate anyone, and
/// the non-loopback startup gate only asks whether auth is `None` — so an
/// empty token would start a public-bound server holding a zero-length
/// full-scope credential. Refused here instead.
fn reject_blank_secret(name: &str, secret: Option<String>) -> Result<Option<String>> {
    match secret {
        Some(s) if s.trim().is_empty() => anyhow::bail!(
            "{name} is set but empty, so it cannot authenticate a request. \
             Refusing to start rather than serving with a zero-length \
             credential. Give it a value or unset it."
        ),
        other => Ok(other),
    }
}

fn env_var_fail_closed(name: &str) -> Result<Option<String>> {
    match std::env::var(name) {
        Ok(v) => Ok(Some(v)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => anyhow::bail!(
            "{name} is set but is not valid Unicode, so Rocky cannot read it. \
             Refusing to start rather than silently ignoring it — an ignored \
             {name} is less protection than you configured. Fix the value or \
             unset it."
        ),
    }
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
    let raw = match token_scope {
        Some(v) => Some(v),
        None => env_var_fail_closed("ROCKY_SERVE_TOKEN_SCOPE")?,
    };
    let scope = match raw {
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

/// Everything between the raw CLI flags and the live [`ServerState`]: token
/// resolution, scope pairing, webhook wiring, state construction.
///
/// Extracted so the producer-to-consumer test can call the SAME function
/// `run_serve` calls. A test that resolves a token and then constructs its own
/// state proves only that the helper works — it cannot see a regression in the
/// handoff between them, which is exactly where a `ServeToken::full(..)` slip
/// would live.
#[allow(clippy::too_many_arguments)]
fn build_serve_state(
    models_dir: &Path,
    models_dir_is_explicit: bool,
    contracts_dir: Option<&Path>,
    config_path: Option<&Path>,
    host: &str,
    auth_token: Option<String>,
    token_scope: Option<String>,
    allowed_origins: Vec<String>,
    scheduler: bool,
    state_path: Option<&Path>,
) -> Result<std::sync::Arc<rocky_server::state::ServerState>> {
    // Token resolution: --token takes precedence over the env var so
    // CI / scripts can override an inherited environment.
    let secret = match auth_token {
        Some(t) => Some(t),
        None => env_var_fail_closed("ROCKY_SERVE_TOKEN")?,
    };
    // Blank from either source — the flag or the env var — is refused.
    let secret = reject_blank_secret("ROCKY_SERVE_TOKEN", secret)?;
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
            // THE THIRD ALIGNED READ. `ROCKY_SERVE_TOKEN` and
            // `ROCKY_SERVE_TOKEN_SCOPE` were made fail-closed and this one was
            // left on `.ok()` — the read with the WORST consequence of the
            // three. A mangled secret collapsed to `None` means "no secret
            // configured", and on a loopback bind that is the documented
            // dev-convenience path: the webhook accepts an UNSIGNED POST,
            // spools a demand, and the resident scheduler runs it. The
            // operator set the variable and got no HMAC at all.
            //
            // An EMPTY value is refused for the same reason rather than
            // filtered to `None`: "" is a configured secret that cannot
            // authenticate anything, so treating it as absent silently opens
            // the same path.
            secret: webhook_secret_fail_closed()?,
            bind_is_loopback: crate::api::is_loopback(host),
            rocky_dir: crate::commands::scheduler::rocky_dir_for_config(&resolved_config),
            rate_limiter: rocky_server::webhook_ingress::WebhookRateLimiter::new(
                WEBHOOK_RATE_LIMIT_RPS,
            ),
        })
    } else {
        None
    };

    Ok(rocky_server::state::ServerState::with_auth_and_webhook(
        models_dir.to_path_buf(),
        models_dir_is_explicit,
        contracts_dir.map(std::path::Path::to_path_buf),
        config_path.map(std::path::Path::to_path_buf),
        token,
        allowed_origins,
        state_path.map(std::path::Path::to_path_buf),
        webhook,
    ))
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

    /// A set-but-unreadable env var must be an error, not silence. This is the
    /// difference between `std::env::var(..).ok()` (fail open — the operator's
    /// token or scope is discarded without a word) and refusing to start.
    ///
    /// The env var is set and restored inside one test, and the assertions do
    /// not depend on any other variable, so a parallel test binary is unharmed.
    #[test]
    fn a_set_but_unreadable_env_var_refuses_to_start() {
        use std::ffi::OsString;
        #[cfg(unix)]
        use std::os::unix::ffi::OsStringExt;

        // A lone 0x80 byte is not valid UTF-8, so `env::var` reports
        // `NotUnicode` rather than `NotPresent`.
        #[cfg(unix)]
        {
            let name = "ROCKY_TEST_NOT_UNICODE_PROBE";
            // SAFETY: single-threaded test body; the variable is unique to this
            // test and removed before it returns.
            unsafe { std::env::set_var(name, OsString::from_vec(vec![0x80])) };
            let result = env_var_fail_closed(name);
            unsafe { std::env::remove_var(name) };

            let err = result.expect_err("an unreadable value must not read as unset");
            assert!(err.to_string().contains("not valid Unicode"), "{err}");
        }

        // An absent variable is still simply absent.
        assert!(
            env_var_fail_closed("ROCKY_TEST_DEFINITELY_UNSET_PROBE")
                .unwrap()
                .is_none()
        );
    }

    /// **The producer-to-consumer wire.** Everything else here tests one half:
    /// the router tests build a `ServeToken::read_only` by hand, and the
    /// pairing tests call the private helper with an already-parsed
    /// `TokenScope`. Neither would notice if `run_serve` ignored its
    /// `token_scope` argument, or always built a full-scope token — the field
    /// would be written by tests and never by the CLI.
    ///
    /// So this walks the real path: the raw flag string `--token-scope
    /// read-only` goes through `resolve_serve_token`, and the resulting token
    /// is installed on a `ServerState` exactly as `run_serve` installs it. The
    /// assertion is on `state.auth`, the field the middleware actually reads.
    ///
    /// The one link still not covered in-process is clap itself (`Cli` lives
    /// in the `rocky` binary crate, which this crate cannot import). That link
    /// is covered by running the built binary: `rocky serve --token-scope
    /// read-only` with a token answers `403` on `POST /api/v1/jobs/run` and
    /// `200` on `GET /api/v1/meta`.
    #[tokio::test]
    async fn the_raw_flag_value_reaches_the_state_the_middleware_reads() {
        let models = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../rocky-compiler/tests/fixtures/simple_project/models");

        for (raw, expected) in [
            (Some("read-only".to_string()), TokenScope::ReadOnly),
            (Some("full".to_string()), TokenScope::Full),
            // No scope named at all → the historical full-scope token.
            (None, TokenScope::Full),
        ] {
            // Crosses `build_serve_state` — the SAME function `run_serve`
            // calls — rather than resolving a token and hand-building a
            // state. That is the difference between proving the helper works
            // and proving the handoff does: replacing the token with
            // `ServeToken::full(..)` between resolution and state
            // construction survived the old shape of this test.
            let state = build_serve_state(
                &models,
                false,
                None,
                None,
                "127.0.0.1",
                Some("s3cret".to_string()),
                raw.clone(),
                Vec::new(),
                false,
                None,
            )
            .expect("a well-formed scope builds a state");
            let installed = state.auth.as_ref().expect("the token reached the state");
            assert_eq!(
                installed.scope, expected,
                "--token-scope {raw:?} must install {expected:?}"
            );
            assert_eq!(installed.secret, "s3cret");
        }
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
