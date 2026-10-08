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
//! the token a shared browser UI must hold: a leak of it cannot reach
//! `POST /api/v1/jobs/run` and through it the warehouse. `--read-only` is the
//! same as `--token-scope read-only`, and conflicts with `--token-scope full`.
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
//! # `--ui` tokens and operator mode
//!
//! "Fronted" means `--allowed-host` or `--allowed-origin` was given (any
//! entry): another name or page reaches this server, so it is shared.
//!
//! ```text
//!   bind          fronted  secret  scope         -> result
//!   loopback      no       none    none or full  -> generated, FULL (operator mode)
//!   loopback      no       none    read-only     -> generated, read-only
//!   loopback      yes      none    none or r-o   -> generated, read-only
//!   loopback      yes      none    full          -> refused: a fronted server is shared
//!   loopback      no       given   full          -> accepted (operator mode)
//!   loopback      yes      given   full          -> refused: a fronted server is shared
//!   loopback      any      given   read-only     -> accepted
//!   non-loopback  any      any     full          -> refused: the UI token must be read-only
//!   non-loopback  any      none    -             -> refused: a token is required
//! ```
//!
//! Operator mode is a full-scope UI token: the browser can run, plan, approve
//! and apply, acting as the OS user who started the server, as the VS Code
//! extension does. A process that can read that token could already run
//! `rocky` as that user. A foreign browser tab cannot use it: the token
//! travels in an `Authorization` header the browser never adds by itself, and
//! the `Host`/`Origin` guard refuses a foreign origin. The server cannot
//! detect an SSH `-L` tunnel or a port-forward to its loopback port, so
//! `--read-only` is the answer whenever someone else can reach the port.
//!
//! A generated token has 244 random bits. It is valid for every request
//! until the process exits, and each start makes a new one. Once the
//! listener is bound, the server prints the `Rocky UI:` address with the
//! token on stdout, for every scope, and notes on stderr (a generated token;
//! operator mode). `--open` hands that address to the system opener, whose
//! command line other local users can read: at full scope another local OS
//! user could act with it, so a generated token is for a single-user
//! machine. A non-loopback bind never generates. Without `--ui` nothing is
//! generated, so a loopback server with no token still asks no request for one.
//!
//! There is no `[serve]` section in `rocky.toml` — `rocky-core/src/config.rs`
//! defines none — so flag-plus-env is the idiom `serve` already uses for every
//! one of its knobs (`--token`, `--allowed-origin`, `--host`). A new config
//! shape for one field would be net-new surface, and would split where an
//! operator looks for the secret and where they look for its scope.
//!
//! Cross-origin clients must be enumerated via `--allowed-origin`. The
//! default allowlist is empty (same-origin only); the browser UI is served
//! from this origin and needs none.

use std::path::Path;

use anyhow::Result;

use rocky_server::auth::{ServeToken, TokenScope};

/// The fixed webhook-ingress request rate (requests/second), with an equal
/// burst. A flood guard shared across all callers, not a per-sender quota.
const WEBHOOK_RATE_LIMIT_RPS: f64 = 10.0;

/// Which `rocky.toml`, if any, this `rocky serve` binds — the decision behind
/// `run_serve`'s `config_path` argument.
///
/// ```text
///   nothing at `config`               -> Ok(None)          config-less serve
///   a config that stats               -> Ok(Some(config))  bind it
///   an entry that cannot be stat-ed   -> Err              refuse to start
/// ```
///
/// `None` is a legitimate answer: `rocky serve --models models/` over a
/// directory with no project around it is the documented standalone flow, and
/// it must keep starting. Only a path that HAS an entry and cannot be read
/// becomes a refusal.
///
/// # Why this is not a config load
///
/// A `rocky.toml` that is present and malformed still binds. The server
/// reloads the config on every compile and degrades on a load error there
/// (`rocky_server::state::ServerState::recompile`), so refusing here would
/// change what a malformed config does to `serve` — a different question from
/// the one #1729 asks. What changes is only the answer to "is a config here
/// at all", which `Path::exists()` got wrong for a dangling symlink: it
/// follows the link, answered `false`, and `serve` then ran as if the
/// operator had passed no `--config` at all. That silently emptied `[mask]`,
/// `[classifications.allow_unmasked]` and `[freshness]` so W004 and W005 went
/// quiet on every compile, defaulted the `[cache.schemas]` posture, and — via
/// the `PathBuf::from("rocky.toml")` fallback below — moved the scheduler's
/// webhook spool from the project the operator named to `./.rocky` in the
/// current directory.
///
/// Lives here, not in `main.rs`, so a test crosses the same code production
/// runs — the reason `build_serve_state` exists.
pub fn resolve_serve_config_path(config: &Path) -> Result<Option<&Path>> {
    rocky_core::config::config_path_if_present(config).map_err(|e| {
        anyhow::Error::new(e).context(
            "refusing to start: `rocky serve` cannot tell whether this project has a config, \
             and starting without one would compile with no [mask], no [freshness] and the \
             default schema-cache posture, and spool webhook demands to ./.rocky",
        )
    })
}
/// The resident scheduler's poll cadence: the flag, then the project's own
/// `[schedule] poll_interval_seconds`, then the built-in default (#1620).
///
/// The key parsed and validated while nothing read it, so a project that set
/// it got the built-in cadence and no warning. The reference page said the
/// one-shot `rocky tick` did not consume it — true, and beside the point: the
/// resident loop did not either.
///
/// Read once, not per tick. `spawn_scheduler` fixes the interval for the
/// process's life (it clamps to `MIN_POLL_INTERVAL` and then sleeps on it), so
/// re-reading would change nothing; making the cadence live under a running
/// loop is a separate feature.
///
/// A config that does not load leaves the built-in default rather than
/// refusing. That matches the tolerance the tick loop already applies — it
/// re-reads the config every iteration and carries on when the read fails — so
/// an unparseable config must not stop the server from starting. The config is
/// reported through the ordinary serve path either way.
fn resolved_poll_interval(
    flag_seconds: Option<u64>,
    config_path: &std::path::Path,
) -> std::time::Duration {
    if let Some(seconds) = flag_seconds {
        return std::time::Duration::from_secs(seconds);
    }
    match rocky_core::config::load_rocky_config(config_path) {
        Ok(config) => std::time::Duration::from_secs(config.schedule.poll_interval_seconds),
        Err(_) => crate::commands::scheduler::DEFAULT_POLL_INTERVAL,
    }
}

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
    // conventional default. `GET /api/v1/dag` and the resident compile
    // distinguish them; see
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
    // `--read-only`: `--token-scope read-only` by another name.
    read_only: bool,
    allowed_origins: Vec<String>,
    // `--ui`: serve the embedded browser UI at `/ui/`. Validated in
    // `build_serve_state` (the token matrix in `validate_ui_flags`; webhook
    // secret with `--scheduler`; the feature compiled in).
    ui: bool,
    // `--allowed-host`: extra `Host` values the `--ui` guard accepts.
    allowed_hosts: Vec<String>,
    // `--open`: hand the printed page address to the system browser once the
    // listener is bound. Refused without `--ui` (`validate_open_flag`).
    open: bool,
    scheduler: bool,
    poll_interval_seconds: Option<u64>,
    drain_timeout_seconds: Option<u64>,
    state_path: Option<&Path>,
) -> Result<()> {
    validate_open_flag(open, ui)?;
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

    // Built BEFORE the state so the settings snapshot can borrow the very
    // `String` the listener binds. Reporting a bind host the server is not
    // using would be a lie on a field an operator reads to decide whether the
    // server is exposed; lending it from one owner makes that unrepresentable
    // rather than merely untrue-today.
    let serve_config = crate::api::ServeConfig { host, port };

    let (state, token_origin) = build_serve_state(
        models_dir,
        models_dir_is_explicit,
        contracts_dir,
        config_path,
        &serve_config.host,
        auth_token,
        token_scope,
        read_only,
        allowed_origins,
        ui,
        allowed_hosts,
        scheduler,
        state_path,
        UiBuild::current(),
    )?;

    // The one address a person needs: the login link, token in the query.
    // `GET /login` trades it for a session cookie and redirects to the page,
    // which never holds the token. Printed for every scope, full included,
    // on stdout, once the listener is bound
    // (`announce_when_ready`, below), so a script can capture it. The same
    // string is what `--open` hands the opener, so the two cannot drift: the
    // address that opens is the address that printed.
    let announcement = ui_announcement(
        ui,
        &serve_config.host,
        port,
        state.auth.as_ref(),
        token_origin,
    );

    // Start filesystem watcher if requested. Without `--models` the compile
    // reads every transformation pipeline's own models directory (#2011), so
    // each of those is watched too.
    let _watcher = if watch {
        let pipeline_roots = if models_dir_is_explicit {
            Vec::new()
        } else {
            pipeline_model_roots(state.config_path.as_deref())
        };
        Some(rocky_server::watch::start_watcher(
            state.clone(),
            models_dir,
            &pipeline_roots,
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

    // The address, the generated-token note and `--open` all wait on the
    // readiness latch `api::serve` raises after the startup sweep and the
    // bind. So nothing prints a token for a port that is not listening: a
    // failed bind (a port in use) returns from `api::serve` without raising
    // the latch, and `shutdown` (raised below) ends the task. A missing or
    // failing opener is a warning, never a reason not to serve.
    if let Some(announcement) = announcement {
        let opener: Option<std::sync::Arc<dyn BrowserOpener>> = if open {
            Some(std::sync::Arc::new(SystemOpener))
        } else {
            None
        };
        announce_when_ready(
            server_ready.clone(),
            shutdown.clone(),
            announcement,
            std::sync::Arc::new(StdAnnouncer),
            opener,
        );
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
            poll_interval: resolved_poll_interval(poll_interval_seconds, &resolved_config),
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

    let result = crate::api::serve(state, serve_config, shutdown.clone(), server_ready).await;

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
    webhook_secret_fail_closed_named(WEBHOOK_SECRET_ENV)
}

/// The env var both the startup gate and [`webhook_secret_posture`] read.
///
/// Named once so `the_gate_and_the_probe_read_one_variable` can pin that they
/// cannot drift onto different variables — the parity test below is worthless
/// if the two agree about different inputs.
const WEBHOOK_SECRET_ENV: &str = "ROCKY_WEBHOOK_SECRET";

/// [`webhook_secret_fail_closed`] over an arbitrary variable, so a test can use
/// a name unique to itself rather than mutating the real one in a parallel test
/// binary.
fn webhook_secret_fail_closed_named(name: &str) -> Result<Option<String>> {
    match env_var_fail_closed(name)? {
        Some(s) if s.trim().is_empty() => anyhow::bail!(
            "{name} is set but empty, so it cannot sign or verify \
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

/// Whether `ROCKY_WEBHOOK_SECRET` could sign a webhook — a **report**, not a
/// decision. Never bails, so it is safe to run with the scheduler off, which is
/// the case the settings route exists to answer: an operator needs to know the
/// secret is usable *before* turning the scheduler on.
///
/// This deliberately mirrors the startup gate rather than re-deciding anything.
/// The gate is two reads deep — [`webhook_secret_fail_closed`] refuses a blank
/// value, and [`env_var_fail_closed`] beneath it refuses `NotUnicode` — so both
/// collapse to `SetButUnusable` here. `std::env::var(..).ok()` would report a
/// non-UTF-8 secret as absent, which is the display contradicting the gate it
/// describes; `var_os` keeps the two apart.
///
/// `webhook_secret_posture_matches_the_startup_gate` pins the agreement.
fn webhook_secret_posture() -> rocky_server::state::WebhookSecret {
    webhook_secret_posture_named(WEBHOOK_SECRET_ENV)
}

/// [`webhook_secret_posture`] over an arbitrary variable — see
/// [`webhook_secret_fail_closed_named`] for why.
fn webhook_secret_posture_named(name: &str) -> rocky_server::state::WebhookSecret {
    use rocky_server::state::WebhookSecret;
    match std::env::var_os(name) {
        None => WebhookSecret::Absent,
        // Not valid UTF-8: set, and unreadable — `env_var_fail_closed` bails.
        Some(raw) => match raw.to_str() {
            None => WebhookSecret::SetButUnusable,
            // Blank: set, and cannot authenticate — `webhook_secret_fail_closed` bails.
            Some(value) if value.trim().is_empty() => WebhookSecret::SetButUnusable,
            Some(_) => WebhookSecret::Present,
        },
    }
}

/// The two `[state]` labels the settings route reports, read once at startup.
///
/// Non-fatal by construction. `rocky serve` starts today against an absent or
/// malformed `rocky.toml` — the scheduler re-reads the file every tick and skips
/// the tick on a parse error (`scheduler/mod.rs`), and `resolved_poll_interval`
/// already falls back to a default the same way. A read-only settings route must
/// not be the thing that newly refuses to start a server.
///
/// Only two fieldless enum labels are taken; the `RockyConfig` is dropped here
/// so nothing downstream can reach `AdapterConfig`'s unbounded `.extra` map.
pub(crate) fn config_posture(config_path: Option<&Path>) -> rocky_server::state::ConfigLabels {
    use rocky_server::state::{ConfigLabels, ConfigStatus};
    // The same loader the recompile path uses (`ServerState::recompile`), so a
    // broken config means one thing in this process rather than one thing per
    // caller — the defect #1625 is about.
    match rocky_core::config::load_optional_project_config(config_path) {
        Ok(Some(config)) => ConfigLabels {
            state_backend: Some(config.state.backend),
            // The resolved mode, as far as it resolves without I/O: `cas` only
            // when it is requested (explicitly or as the backend default) AND
            // the backend can do it; an explicit `cas` on `local` or `valkey`
            // runs as `off`, so it reports `off`. No probe runs here — this is
            // a label read at startup, not a writer — so a store that turns out
            // not to honour conditional writes is reported by `rocky doctor`.
            concurrency_control: Some(if rocky_core::state_sync::cas_effective(&config.state) {
                rocky_core::config::ConcurrencyControl::Cas
            } else {
                rocky_core::config::ConcurrencyControl::Off
            }),
            config_status: ConfigStatus::Loaded,
        },
        Ok(None) => ConfigLabels {
            config_status: ConfigStatus::Absent,
            ..ConfigLabels::default()
        },
        Err(_) => ConfigLabels {
            config_status: ConfigStatus::Unreadable,
            ..ConfigLabels::default()
        },
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
    read_only: bool,
    exposure: UiExposure,
) -> Result<(Option<ServeToken>, TokenOrigin)> {
    let raw = merge_read_only_flag(read_only, token_scope)?;
    let raw = match raw {
        Some(v) => Some(v),
        None => env_var_fail_closed("ROCKY_SERVE_TOKEN_SCOPE")?,
    };
    let scope = match raw {
        Some(raw) => Some(raw.parse::<TokenScope>()?),
        None => None,
    };
    resolve_token_for_bind(secret, scope, exposure)
}

/// `--read-only` is `--token-scope read-only` by another name. It folds into
/// the flag value before the env var is consulted, so like `--token-scope` it
/// overrides `ROCKY_SERVE_TOKEN_SCOPE`. With `--token-scope full` it is a
/// contradiction and refused; with `--token-scope read-only` it is redundant
/// and accepted.
fn merge_read_only_flag(read_only: bool, token_scope: Option<String>) -> Result<Option<String>> {
    if !read_only {
        return Ok(token_scope);
    }
    match token_scope.as_deref() {
        None | Some("read-only") => Ok(Some("read-only".to_string())),
        Some(other) => anyhow::bail!(
            "--read-only conflicts with --token-scope {other}. Pass one of them: \
             --read-only is the same as --token-scope read-only."
        ),
    }
}

/// How the `--ui` page is exposed, which decides what token `--ui` may hold
/// and whether one is generated.
///
/// "Fronted" means `--allowed-host` or `--allowed-origin` was given: some
/// other name or page reaches this server, a reverse proxy or a second app.
/// A fronted server is shared, so the UI stays read-only there until writes
/// carry per-person identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UiExposure {
    /// No `--ui`. Nothing is generated and nothing here applies: a loopback
    /// server with no token serves every route with no auth, as it always has.
    NoUi,
    /// `--ui` on a loopback bind with no `--allowed-host` / `--allowed-origin`.
    /// Operator mode: a generated token is full scope unless the operator
    /// asks for `read-only`.
    Local,
    /// `--ui` on a loopback bind with `--allowed-host` or `--allowed-origin`.
    /// A generated token is read-only, and a full token is refused.
    Fronted,
    /// `--ui` on a non-loopback bind. Never generates; the token must be
    /// given and read-only.
    Remote,
}

impl UiExposure {
    pub(crate) fn for_bind(ui: bool, host: &str, fronted: bool) -> Self {
        match (ui, crate::api::is_loopback(host), fronted) {
            (false, _, _) => Self::NoUi,
            (true, true, false) => Self::Local,
            (true, true, true) => Self::Fronted,
            (true, false, _) => Self::Remote,
        }
    }
}

/// Where the installed token came from, so `run_serve` can say when it made
/// one up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TokenOrigin {
    /// The operator configured it (`--token` or `ROCKY_SERVE_TOKEN`), or there
    /// is no token at all.
    Operator,
    /// Made by [`generate_ui_token`] for this process. It changes on every
    /// restart.
    Generated,
}

/// The refusal for a full-scope UI token on a fronted loopback server.
const FRONTED_FULL_REFUSAL: &str = "rocky serve --ui with --allowed-host or --allowed-origin \
     refuses a full-scope token: a server reached through a proxy or another page is shared, \
     and writes from a shared UI need per-person tokens, which Rocky does not have yet. Use \
     --read-only (or --token-scope read-only), or drop --allowed-host / --allowed-origin to run \
     the UI in operator mode on your own machine.";

/// [`pair_token_with_scope`], plus the cases where `--ui` on a loopback bind
/// generates a token.
///
/// ```text
///   secret  scope       exposure  -> outcome
///   None    none/full   Local     -> generated, FULL (operator mode)
///   None    read-only   Local     -> generated, read-only
///   None    none/r-o    Fronted   -> generated, read-only
///   None    full        Fronted   -> refused: a fronted server is shared
///   any     any         other     -> pair_token_with_scope, unchanged
/// ```
///
/// A given secret is paired here and judged by [`validate_ui_flags`].
fn resolve_token_for_bind(
    secret: Option<String>,
    scope: Option<TokenScope>,
    exposure: UiExposure,
) -> Result<(Option<ServeToken>, TokenOrigin)> {
    let generated = |scope| {
        Ok((
            Some(ServeToken {
                secret: generate_ui_token(),
                scope,
            }),
            TokenOrigin::Generated,
        ))
    };
    match (secret, scope, exposure) {
        (None, None | Some(TokenScope::Full), UiExposure::Local) => generated(TokenScope::Full),
        (None, Some(TokenScope::ReadOnly), UiExposure::Local)
        | (None, None | Some(TokenScope::ReadOnly), UiExposure::Fronted) => {
            generated(TokenScope::ReadOnly)
        }
        (None, Some(TokenScope::Full), UiExposure::Fronted) => {
            anyhow::bail!("{FRONTED_FULL_REFUSAL}")
        }
        (secret @ Some(_), scope, UiExposure::Local | UiExposure::Fronted)
        | (secret, scope, UiExposure::NoUi | UiExposure::Remote) => {
            Ok((pair_token_with_scope(secret, scope)?, TokenOrigin::Operator))
        }
    }
}

/// A fresh secret for one `rocky serve --ui` process: 64 lowercase hex
/// characters from two random (v4) UUIDs, so 244 random bits.
///
/// The bits come from `rand`'s thread RNG through the workspace `uuid`
/// dependency (feature `fast-rng`): ChaCha12, seeded and reseeded from the
/// operating system's RNG. Hex needs no escaping in the login URL that
/// carries it. Never logged through tracing; the printed address carries it,
/// and `--open` passes that address to the opener.
fn generate_ui_token() -> String {
    format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    )
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

/// What this binary carries for `--ui`. A seam: production passes
/// [`UiBuild::current`], and a test passes a page, so the `--ui` path of
/// [`build_serve_state`] runs in a build without the `ui` feature.
pub(crate) enum UiBuild {
    /// Built without the `ui` feature.
    NoFeature,
    /// Built with the feature, but `engine/ui/dist` had no page at build time.
    NoPage,
    /// A page to serve.
    Page(std::sync::Arc<dyn rocky_server::ui::UiAssetSource>),
}

impl UiBuild {
    /// What this binary was built with.
    pub(crate) fn current() -> Self {
        if !crate::ui::built_with_ui() {
            return Self::NoFeature;
        }
        match crate::ui::embedded_assets() {
            Some(assets) => Self::Page(assets),
            None => Self::NoPage,
        }
    }

    fn has_feature(&self) -> bool {
        match self {
            Self::NoFeature => false,
            Self::NoPage | Self::Page(_) => true,
        }
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
    read_only: bool,
    allowed_origins: Vec<String>,
    ui: bool,
    allowed_hosts: Vec<String>,
    scheduler: bool,
    state_path: Option<&Path>,
    ui_build: UiBuild,
) -> Result<(
    std::sync::Arc<rocky_server::state::ServerState>,
    TokenOrigin,
)> {
    // Token resolution: --token takes precedence over the env var so
    // CI / scripts can override an inherited environment.
    let secret = match auth_token {
        Some(t) => Some(t),
        None => env_var_fail_closed("ROCKY_SERVE_TOKEN")?,
    };
    // Blank from either source — the flag or the env var — is refused.
    let secret = reject_blank_secret("ROCKY_SERVE_TOKEN", secret)?;
    // Fronted: some other name or page reaches this server. Any entry counts.
    let fronted = !allowed_hosts.is_empty() || !allowed_origins.is_empty();
    let exposure = UiExposure::for_bind(ui, host, fronted);
    let (token, token_origin) = resolve_serve_token(secret, token_scope, read_only, exposure)?;

    // The webhook secret is read once, here, because two decisions hang on
    // it: the ingress below, and the `--ui --scheduler` refusal.
    let webhook_secret = if scheduler {
        webhook_secret_fail_closed()?
    } else {
        None
    };

    validate_ui_flags(
        exposure,
        token.as_ref(),
        scheduler,
        webhook_secret.is_some(),
        ui_build.has_feature(),
    )?;
    let ui_config = if ui {
        let assets = match ui_build {
            UiBuild::Page(assets) => assets,
            UiBuild::NoPage | UiBuild::NoFeature => anyhow::bail!(
                "this build of rocky was made with the `ui` feature but embeds no page: \
                 engine/ui/dist had no index.html at build time. Run `npm ci && npm run \
                 build` in engine/ui and rebuild with `cargo build --features ui`."
            ),
        };
        Some(rocky_server::ui::UiConfig {
            bind_host: host.to_string(),
            allowed_hosts,
            assets,
        })
    } else {
        None
    };

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
            secret: webhook_secret,
            bind_is_loopback: crate::api::is_loopback(host),
            rocky_dir: crate::commands::scheduler::rocky_dir_for_config(&resolved_config),
            rate_limiter: rocky_server::webhook_ingress::WebhookRateLimiter::new(
                WEBHOOK_RATE_LIMIT_RPS,
            ),
        })
    } else {
        None
    };

    // The posture `GET /api/v1/settings` reports. Built field by field from
    // primitives that are already in scope here, which is what keeps the
    // allowlist honest: there is no `RockyConfig` and no `Debug` on the path
    // from a config file to the response body.
    let settings = rocky_server::state::SettingsSnapshot {
        // The SAME `String` the listener binds -- `serve` builds `ServeConfig`
        // first and lends this from it, so the reported host cannot drift from
        // the bound one.
        bind_host: host.to_string(),
        scheduler,
        // Probed unconditionally, INCLUDING with the scheduler off: the branch
        // above only reads the secret under `--scheduler`, and presence is
        // exactly what an operator needs before turning the scheduler on.
        webhook_secret: webhook_secret_posture(),
        // Left unresolved on purpose. Reading `rocky.toml` here would put a
        // blocking full-file read on the path to `TcpListener::bind`, which on
        // a plain `rocky serve` reads no file, so a FIFO or a stalled mount
        // would stop the server binding at all. (The initial compile reads the
        // config on its own spawned task, so it never gates the listener;
        // `--scheduler` without an explicit poll interval already reads it
        // before binding.) The settings route resolves it on first ask, under a
        // permit and a deadline.
        config_labels: std::sync::OnceLock::new(),
    };

    // Before the state starts its first compile: `serve` shows the same
    // per-model-target checks `rocky compile` does.
    rocky_server::project_gates::install_project_gates(super::apply_model_target_gates);

    let state = rocky_server::state::ServerState::with_auth_and_webhook(
        models_dir.to_path_buf(),
        models_dir_is_explicit,
        contracts_dir.map(std::path::Path::to_path_buf),
        config_path.map(std::path::Path::to_path_buf),
        token,
        allowed_origins,
        state_path.map(std::path::Path::to_path_buf),
        webhook,
        ui_config,
        settings,
    );
    Ok((state, token_origin))
}

/// The models directory of every transformation pipeline that exists now:
/// the directories `ServerState::recompile` reads without `--models` (#2011).
///
/// Read once, when the watcher starts. A config that cannot be read gives no
/// extra directory; the compile reports that config error itself. A pipeline
/// added to `rocky.toml` later is compiled, but its directory is only watched
/// after a restart.
fn pipeline_model_roots(config_path: Option<&Path>) -> Vec<std::path::PathBuf> {
    let Some(config_path) = config_path else {
        return Vec::new();
    };
    let Ok(Some(config)) = rocky_core::config::load_optional_project_config(Some(config_path))
    else {
        return Vec::new();
    };
    config
        .pipelines
        .values()
        .filter_map(|pipeline| pipeline.as_transformation())
        .filter_map(
            |tx| match crate::models_loader::locate_models_dir(&tx.models, config_path) {
                Ok(crate::models_loader::ModelsDir::Present(dir)) => Some(dir),
                Ok(crate::models_loader::ModelsDir::Absent(_)) | Err(_) => None,
            },
        )
        .collect()
}

/// `--open` needs `--ui`: without the UI there is no page to open. Checked
/// before anything binds, like the `--ui` rules, so the refusal names the fix
/// and costs nothing.
pub(crate) fn validate_open_flag(open: bool, ui: bool) -> Result<()> {
    if open && !ui {
        anyhow::bail!(
            "rocky serve --open needs --ui: without the UI there is no page to open. \
             Add `--ui`, or drop --open."
        );
    }
    Ok(())
}

/// The login link `rocky serve --ui` prints: `/login?t=<token>`.
///
/// One function for the print and for `--open`, so what opens is exactly what
/// printed. A wildcard bind (`0.0.0.0`, `::`) is shown as `localhost`: the one
/// name a wildcard bind promises a browser on the same machine can reach. Any
/// other IPv6 literal is bracketed, as a URL requires. The token is
/// form-encoded, so a supplied token with `&`, `#` or `+` survives the trip.
/// `GET /login` answers `Cache-Control: no-store` and `Referrer-Policy:
/// no-referrer` and redirects, so the token does not stay in the address bar.
pub(crate) fn ui_address(bind_host: &str, port: u16, token_secret: &str) -> String {
    let shown_host = match bind_host {
        "0.0.0.0" | "::" => "localhost".to_string(),
        host if host.contains(':') && !host.starts_with('[') => format!("[{host}]"),
        host => host.to_string(),
    };
    let token: String = url::form_urlencoded::byte_serialize(token_secret.as_bytes()).collect();
    format!("http://{shown_host}:{port}/login?t={token}")
}

/// How `--open` reaches a browser: a seam, so a test can substitute one and
/// assert the address it receives. Production uses [`SystemOpener`].
pub(crate) trait BrowserOpener: Send + Sync {
    fn open(&self, url: &str) -> std::io::Result<()>;
}

/// The platform's URL opener — `open` on macOS, `xdg-open` elsewhere, and on
/// Windows `rundll32 url.dll,FileProtocolHandler`, which takes the URL as a
/// plain argument (never `cmd /C start`: `cmd` re-parses its line, so a token
/// carrying `&` or `%` would cut the URL and run the rest). Spawned, and
/// reaped on a thread rather than awaited: some `xdg-open` handlers stay in
/// the foreground until the browser exits, and that must not hold the server.
/// Its output is discarded; the address it was given is already on stdout. An
/// opener that exits non-zero is logged by its status — never by its argument,
/// which carries the token.
pub(crate) struct SystemOpener;

impl BrowserOpener for SystemOpener {
    fn open(&self, url: &str) -> std::io::Result<()> {
        let mut command = if cfg!(target_os = "macos") {
            let mut c = std::process::Command::new("open");
            c.arg(url);
            c
        } else if cfg!(target_os = "windows") {
            let mut c = std::process::Command::new("rundll32");
            c.args(["url.dll,FileProtocolHandler", url]);
            c
        } else {
            let mut c = std::process::Command::new("xdg-open");
            c.arg(url);
            c
        };
        rocky_core::process::strip_dagster_pipes_env(&mut command);
        let mut child = command
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()?;
        std::thread::spawn(move || match child.wait() {
            Ok(status) if status.success() => {}
            Ok(status) => tracing::warn!(
                %status,
                "the browser opener exited without opening the UI; the address is printed above"
            ),
            Err(error) => tracing::warn!(%error, "could not wait for the browser opener"),
        });
        Ok(())
    }
}

/// What `rocky serve --ui` tells the person once the listener is bound.
pub(crate) struct Announcement {
    /// The page address, token in the fragment ([`ui_address`]). Printed for
    /// every token, full scope included: it is the one address a person needs.
    pub(crate) address: String,
    /// Whether the token in it was generated for this process.
    pub(crate) token_origin: TokenOrigin,
    /// Whether the token is full scope, so the UI can make changes.
    pub(crate) operator_mode: bool,
}

/// What `run_serve` announces for the installed token: the login link for
/// every `--ui` token, full scope included, and operator mode when the token
/// is full scope. `None` without `--ui` or without a token.
pub(crate) fn ui_announcement(
    ui: bool,
    bind_host: &str,
    port: u16,
    token: Option<&ServeToken>,
    token_origin: TokenOrigin,
) -> Option<Announcement> {
    let token = token.filter(|_| ui)?;
    Some(Announcement {
        address: ui_address(bind_host, port, &token.secret),
        token_origin,
        operator_mode: match token.scope {
            TokenScope::Full => true,
            TokenScope::ReadOnly => false,
        },
    })
}

/// Where the announcement goes: a seam, so a test can assert what is printed
/// and when. Production uses [`StdAnnouncer`].
pub(crate) trait Announcer: Send + Sync {
    /// The `Rocky UI: <address>` line, on stdout so a script can capture it.
    fn address_line(&self, line: &str);
    /// A note for the person, on stderr. Never carries the token.
    fn note_line(&self, line: &str);
}

/// stdout for the address, stderr for the note.
pub(crate) struct StdAnnouncer;

impl Announcer for StdAnnouncer {
    fn address_line(&self, line: &str) {
        println!("{line}");
    }

    fn note_line(&self, line: &str) {
        eprintln!("{line}");
    }
}

/// The stderr note for a generated token. It names no secret.
const GENERATED_TOKEN_NOTE: &str = "rocky serve --ui: no token was configured, so a per-process \
     token was generated. Each start makes a new one, so the address stops working after a \
     restart. To keep one token, or on a shared machine, pass --token (or set \
     ROCKY_SERVE_TOKEN).";

/// The stderr note for operator mode: a full-scope UI token. It names no
/// secret.
pub(crate) const OPERATOR_MODE_NOTE: &str = "rocky serve --ui: operator mode. This UI can make \
     changes (run, plan, approve, apply) as the user running this server. Use --read-only for \
     a view-only UI, and whenever someone else can reach this port (an SSH tunnel, a \
     port-forward).";

/// Once `ready` is raised, unless `shutdown` comes first: print the address,
/// then the generated-token note, then the operator-mode note, then hand the
/// address to `opener`.
///
/// Waiting on `ready` means a token is never printed for a port the server
/// failed to bind. Its own task, so a slow or failing opener never delays the
/// server; an opener error is logged and the address stays on stdout.
pub(crate) fn announce_when_ready(
    ready: rocky_core::schedule::Drain,
    shutdown: rocky_core::schedule::Drain,
    announcement: Announcement,
    out: std::sync::Arc<dyn Announcer>,
    opener: Option<std::sync::Arc<dyn BrowserOpener>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        // `biased`, shutdown first: a Ctrl-C that lands in the same instant as
        // the bind must not announce, or open a browser at, a server that is
        // going away.
        tokio::select! {
            biased;
            () = shutdown.signalled() => {}
            () = ready.signalled() => {
                out.address_line(&format!("Rocky UI: {}", announcement.address));
                match announcement.token_origin {
                    TokenOrigin::Generated => out.note_line(GENERATED_TOKEN_NOTE),
                    TokenOrigin::Operator => {}
                }
                if announcement.operator_mode {
                    out.note_line(OPERATOR_MODE_NOTE);
                }
                let Some(opener) = opener else { return };
                if let Err(error) = opener.open(&announcement.address) {
                    tracing::warn!(
                        %error,
                        "could not open a browser for the UI; the address is printed above"
                    );
                }
            }
        }
    })
}

/// The `--ui` rules, checked before anything binds. Each refusal names the
/// fix. Without `--ui` nothing here applies.
///
/// - The build must carry the UI (cargo feature `ui`).
/// - A token must be present: every UI request presents one. On a loopback
///   bind with none configured, [`resolve_token_for_bind`] has already
///   generated one, so this refusal is the non-loopback case.
/// - A full-scope token is operator mode, and only a [`UiExposure::Local`]
///   server may hold one: loopback, with no `--allowed-host` and no
///   `--allowed-origin`. A fronted server is shared, and a non-loopback bind
///   is reachable from the network; both need a read-only token until writes
///   carry per-person identity.
/// - With `--scheduler`, `ROCKY_WEBHOOK_SECRET` must be set: a browser can
///   reach the webhook route, and without a secret a loopback bind accepts
///   unsigned requests.
pub(crate) fn validate_ui_flags(
    exposure: UiExposure,
    token: Option<&ServeToken>,
    scheduler: bool,
    webhook_secret_present: bool,
    built_with_ui: bool,
) -> Result<()> {
    if exposure == UiExposure::NoUi {
        return Ok(());
    }
    if !built_with_ui {
        anyhow::bail!(
            "this build of rocky carries no browser UI (built without the `ui` feature). \
             Release binaries carry it; from source, run `npm ci && npm run build` in \
             engine/ui and build with `cargo build --features ui`."
        );
    }
    let Some(token) = token else {
        anyhow::bail!(
            "rocky serve --ui refuses to start without a token: every UI request presents \
             one. Pass `--token <secret> --read-only`, or set ROCKY_SERVE_TOKEN and \
             ROCKY_SERVE_TOKEN_SCOPE=read-only. Only a loopback bind (127.0.0.1) generates a \
             per-process token by itself."
        );
    };
    match (token.scope, exposure) {
        (TokenScope::ReadOnly, _) | (TokenScope::Full, UiExposure::Local | UiExposure::NoUi) => {}
        (TokenScope::Full, UiExposure::Fronted) => anyhow::bail!("{FRONTED_FULL_REFUSAL}"),
        (TokenScope::Full, UiExposure::Remote) => anyhow::bail!(
            "rocky serve --ui on a non-loopback bind requires a read-only token \
             (`--read-only`, or `--token-scope read-only`): the server is reachable from the \
             network, and writes from a shared UI need per-person tokens, which Rocky does not \
             have yet. For job submissions run a second sidecar without --ui, or use the CLI. \
             Operator mode (a UI that can make changes) is for a loopback bind only."
        ),
    }
    if scheduler && !webhook_secret_present {
        anyhow::bail!(
            "rocky serve --ui --scheduler refuses to start without ROCKY_WEBHOOK_SECRET: a \
             browser can reach the webhook route, and without a secret a loopback bind \
             accepts unsigned requests. Set the variable, or drop --scheduler."
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `serve --watch` without `--models` watches every transformation
    /// pipeline's existing models directory: the set the compile reads
    /// (#2011). A missing root and a non-transformation pipeline add nothing.
    #[test]
    fn pipeline_model_roots_are_every_transformation_pipelines_existing_root() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::create_dir_all(root.join("models")).unwrap();
        std::fs::create_dir_all(root.join("reporting")).unwrap();
        let config = root.join("rocky.toml");
        std::fs::write(
            &config,
            "[adapter]\ntype = \"duckdb\"\npath = \"test.duckdb\"\n\n\
             [pipeline.core]\ntype = \"transformation\"\nmodels = \"models/**\"\n\n\
             [pipeline.core.target.governance]\nauto_create_schemas = true\n\n\
             [pipeline.reporting]\ntype = \"transformation\"\nmodels = \"reporting/**\"\n\n\
             [pipeline.reporting.target.governance]\nauto_create_schemas = true\n\n\
             [pipeline.gone]\ntype = \"transformation\"\nmodels = \"gone/**\"\n\n\
             [pipeline.gone.target.governance]\nauto_create_schemas = true\n",
        )
        .unwrap();
        let mut roots = pipeline_model_roots(Some(&config));
        roots.sort();
        assert_eq!(roots, [root.join("models"), root.join("reporting")]);
        assert!(pipeline_model_roots(None).is_empty());
    }

    /// `--open` without `--ui` is refused before anything binds, naming the
    /// fix; every other combination passes.
    #[test]
    fn open_without_ui_is_refused_and_names_the_fix() {
        assert!(validate_open_flag(false, false).is_ok());
        assert!(validate_open_flag(false, true).is_ok());
        assert!(validate_open_flag(true, true).is_ok());
        let err = validate_open_flag(true, false).unwrap_err();
        assert!(err.to_string().contains("needs --ui"), "{err}");
    }

    /// The printed shape, with a host a browser on this machine can reach: a
    /// wildcard bind shows as `localhost`, an IPv6 literal is bracketed, a
    /// name or IPv4 address passes through.
    #[test]
    fn ui_address_is_the_printed_shape_with_a_reachable_host() {
        assert_eq!(
            ui_address("127.0.0.1", 8080, "t"),
            "http://127.0.0.1:8080/login?t=t"
        );
        assert_eq!(
            ui_address("0.0.0.0", 9000, "s3cret"),
            "http://localhost:9000/login?t=s3cret"
        );
        assert_eq!(
            ui_address("::", 8080, "t"),
            "http://localhost:8080/login?t=t"
        );
        assert_eq!(ui_address("::1", 8080, "t"), "http://[::1]:8080/login?t=t");
        assert_eq!(
            ui_address("rocky.internal", 8080, "t"),
            "http://rocky.internal:8080/login?t=t"
        );
        assert_eq!(
            ui_address("fd00::1", 8080, "t"),
            "http://[fd00::1]:8080/login?t=t"
        );
        // A supplied token is form-encoded, so it reaches `/login` intact.
        assert_eq!(
            ui_address("127.0.0.1", 8080, "a&b#c+d e"),
            "http://127.0.0.1:8080/login?t=a%26b%23c%2Bd+e"
        );
    }

    /// The address that prints is an address this server will serve (#1993).
    ///
    /// The two halves live in different crates and spell an IPv6 literal
    /// differently — the URL brackets it, `--host` does not — so each was
    /// individually right while the pair was broken. This asserts the pair:
    /// for every bind host, the authority in the printed URL passes the host
    /// guard that bind produces.
    #[test]
    fn every_printed_ui_address_passes_the_host_guard_for_that_bind() {
        for bind in [
            "127.0.0.1",
            "0.0.0.0",
            "::",
            "::1",
            "fd00::1",
            "rocky.internal",
        ] {
            let url = ui_address(bind, 8080, "t");
            let authority = url
                .strip_prefix("http://")
                .and_then(|rest| rest.split('/').next())
                .expect("the printed address is an http URL with a path");
            let ui = rocky_server::ui::UiConfig {
                bind_host: bind.to_string(),
                allowed_hosts: Vec::new(),
                assets: std::sync::Arc::new(rocky_server::ui::InMemoryAssets(
                    std::collections::BTreeMap::new(),
                )),
            };
            assert!(
                ui.host_allowed(authority),
                "bind {bind} advertises {authority}, which its own guard refuses"
            );
        }
    }

    struct RecordingOpener(std::sync::Mutex<Vec<String>>);

    impl BrowserOpener for RecordingOpener {
        fn open(&self, url: &str) -> std::io::Result<()> {
            self.0.lock().unwrap().push(url.to_string());
            Ok(())
        }
    }

    struct FailingOpener;

    impl BrowserOpener for FailingOpener {
        fn open(&self, _url: &str) -> std::io::Result<()> {
            Err(std::io::Error::other("no browser on this machine"))
        }
    }

    /// Records the announcement: `out:` for stdout, `err:` for stderr.
    #[derive(Default)]
    struct RecordingAnnouncer(std::sync::Mutex<Vec<String>>);

    impl Announcer for RecordingAnnouncer {
        fn address_line(&self, line: &str) {
            self.0.lock().unwrap().push(format!("out:{line}"));
        }

        fn note_line(&self, line: &str) {
            self.0.lock().unwrap().push(format!("err:{line}"));
        }
    }

    fn announcement(address: &str, token_origin: TokenOrigin) -> Announcement {
        Announcement {
            address: address.to_string(),
            token_origin,
            operator_mode: false,
        }
    }

    /// Operator mode (a generated FULL-scope token) still prints the login
    /// link with the token on stdout, then the generated-token note, then the
    /// operator-mode note. Neither note carries the token.
    #[tokio::test]
    async fn operator_mode_prints_the_address_with_the_token_then_both_notes() {
        let ready = rocky_core::schedule::Drain::new();
        let shutdown = rocky_core::schedule::Drain::new();
        let out = std::sync::Arc::new(RecordingAnnouncer::default());
        let address = ui_address("127.0.0.1", 8080, "fullscopesecret");
        let handle = announce_when_ready(
            ready.clone(),
            shutdown,
            Announcement {
                address: address.clone(),
                token_origin: TokenOrigin::Generated,
                operator_mode: true,
            },
            out.clone(),
            None,
        );
        ready.signal();
        handle.await.unwrap();

        let lines = out.0.lock().unwrap();
        assert_eq!(lines.len(), 3, "{lines:?}");
        assert_eq!(
            lines[0],
            "out:Rocky UI: http://127.0.0.1:8080/login?t=fullscopesecret"
        );
        assert_eq!(lines[2], format!("err:{OPERATOR_MODE_NOTE}"));
        assert!(lines[2].contains("--read-only"), "{lines:?}");
        assert!(
            lines[1..].iter().all(|l| !l.contains("fullscopesecret")),
            "{lines:?}"
        );
    }

    /// Nothing is printed and nothing opens until the listener is bound. Then
    /// the address prints on stdout, and the opener receives EXACTLY that
    /// address — the same `ui_address` string, token fragment included.
    #[tokio::test]
    async fn the_address_prints_and_opens_only_after_ready() {
        let ready = rocky_core::schedule::Drain::new();
        let shutdown = rocky_core::schedule::Drain::new();
        let out = std::sync::Arc::new(RecordingAnnouncer::default());
        let opener = std::sync::Arc::new(RecordingOpener(std::sync::Mutex::new(Vec::new())));
        let address = ui_address("0.0.0.0", 8080, "s3cret");
        let handle = announce_when_ready(
            ready.clone(),
            shutdown.clone(),
            announcement(&address, TokenOrigin::Operator),
            out.clone(),
            Some(opener.clone()),
        );

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            out.0.lock().unwrap().is_empty(),
            "the address was printed before the listener was bound"
        );
        assert!(
            opener.0.lock().unwrap().is_empty(),
            "the browser was opened before the listener was bound"
        );

        ready.signal();
        handle.await.unwrap();
        assert_eq!(
            *out.0.lock().unwrap(),
            vec![format!("out:Rocky UI: {address}")],
            "an operator's token gets no note"
        );
        assert_eq!(*opener.0.lock().unwrap(), vec![address]);
    }

    /// A generated token: the address, then a note on stderr that says it is
    /// per-process. The note never carries the token. No `--open`, no opener.
    #[tokio::test]
    async fn a_generated_token_prints_the_address_then_a_note_without_the_secret() {
        let ready = rocky_core::schedule::Drain::new();
        let shutdown = rocky_core::schedule::Drain::new();
        let out = std::sync::Arc::new(RecordingAnnouncer::default());
        let address = ui_address("127.0.0.1", 8080, "abc123secret");
        let handle = announce_when_ready(
            ready.clone(),
            shutdown,
            announcement(&address, TokenOrigin::Generated),
            out.clone(),
            None,
        );
        ready.signal();
        handle.await.unwrap();

        let lines = out.0.lock().unwrap();
        assert_eq!(lines.len(), 2, "{lines:?}");
        assert_eq!(lines[0], format!("out:Rocky UI: {address}"));
        assert!(lines[1].starts_with("err:"), "{lines:?}");
        assert!(lines[1].contains("per-process"), "{lines:?}");
        assert!(!lines[1].contains("abc123secret"), "{lines:?}");
    }

    /// A shutdown before readiness — a bind that failed, such as a port in
    /// use — prints no token and opens nothing.
    #[tokio::test]
    async fn a_shutdown_before_ready_prints_and_opens_nothing() {
        let ready = rocky_core::schedule::Drain::new();
        let shutdown = rocky_core::schedule::Drain::new();
        let out = std::sync::Arc::new(RecordingAnnouncer::default());
        let opener = std::sync::Arc::new(RecordingOpener(std::sync::Mutex::new(Vec::new())));
        let handle = announce_when_ready(
            ready.clone(),
            shutdown.clone(),
            announcement(&ui_address("127.0.0.1", 8080, "t"), TokenOrigin::Generated),
            out.clone(),
            Some(opener.clone()),
        );
        shutdown.signal();
        handle.await.unwrap();
        assert!(out.0.lock().unwrap().is_empty());
        assert!(opener.0.lock().unwrap().is_empty());
    }

    /// A failing opener is a warning: the task completes, the address was
    /// printed, and nothing propagates to the server.
    #[tokio::test]
    async fn a_failing_opener_is_not_fatal() {
        let ready = rocky_core::schedule::Drain::new();
        let shutdown = rocky_core::schedule::Drain::new();
        let out = std::sync::Arc::new(RecordingAnnouncer::default());
        let handle = announce_when_ready(
            ready.clone(),
            shutdown,
            announcement(&ui_address("127.0.0.1", 8080, "t"), TokenOrigin::Operator),
            out.clone(),
            Some(std::sync::Arc::new(FailingOpener)),
        );
        ready.signal();
        handle
            .await
            .expect("the opener task must not panic on a failed open");
        assert_eq!(out.0.lock().unwrap().len(), 1);
    }

    /// The four `--ui` refusals, each naming its fix, and the one shape that
    /// starts. Without `--ui` every combination passes.
    #[test]
    fn ui_flags_refuse_a_missing_or_full_token_a_bare_scheduler_and_a_build_without_the_ui() {
        let read_only = ServeToken {
            secret: "s3cret".into(),
            scope: TokenScope::ReadOnly,
        };
        let full = ServeToken::full("s3cret");

        use UiExposure::{Fronted, Local, NoUi, Remote};
        assert!(validate_ui_flags(NoUi, None, true, false, false).is_ok());
        assert!(validate_ui_flags(NoUi, Some(&full), false, false, true).is_ok());

        let err = validate_ui_flags(Local, Some(&read_only), false, false, false).unwrap_err();
        assert!(err.to_string().contains("--features ui"), "{err}");

        let err = validate_ui_flags(Remote, None, false, false, true).unwrap_err();
        assert!(err.to_string().contains("without a token"), "{err}");

        // Full scope: operator mode on a local loopback server only.
        assert!(validate_ui_flags(Local, Some(&full), false, false, true).is_ok());
        let err = validate_ui_flags(Fronted, Some(&full), false, false, true).unwrap_err();
        assert!(err.to_string().contains("per-person tokens"), "{err}");
        let err = validate_ui_flags(Remote, Some(&full), false, false, true).unwrap_err();
        assert!(err.to_string().contains("read-only"), "{err}");

        let err = validate_ui_flags(Local, Some(&read_only), true, false, true).unwrap_err();
        assert!(err.to_string().contains("ROCKY_WEBHOOK_SECRET"), "{err}");
        let err = validate_ui_flags(Local, Some(&full), true, false, true).unwrap_err();
        assert!(err.to_string().contains("ROCKY_WEBHOOK_SECRET"), "{err}");

        for exposure in [Local, Fronted, Remote] {
            assert!(validate_ui_flags(exposure, Some(&read_only), true, true, true).is_ok());
            assert!(validate_ui_flags(exposure, Some(&read_only), false, false, true).is_ok());
        }
    }

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

    /// **The parity test.** A settings route that reports a secret's presence
    /// is a display producer for the startup gate, so the two must never
    /// disagree about the same value. Asserting the probe alone would pass for
    /// a probe that is simply wrong in the same direction as itself.
    ///
    /// The gate is two reads deep — `webhook_secret_fail_closed` refuses a
    /// blank value, `env_var_fail_closed` beneath it refuses `NotUnicode` — so
    /// the interesting case is the one a naive `env::var(..).ok()` probe gets
    /// wrong: a non-UTF-8 secret is SET, and must not read as absent.
    ///
    /// ```text
    ///   gate Ok(Some) <-> Present          gate Err <-> SetButUnusable
    ///   gate Ok(None) <-> Absent
    /// ```
    ///
    /// Uses a variable unique to this test, so a parallel test binary is
    /// unharmed; `the_gate_and_the_probe_read_one_variable` pins that the two
    /// production spellings still name the same real variable.
    #[test]
    fn webhook_secret_posture_matches_the_startup_gate() {
        use rocky_server::state::WebhookSecret;
        use std::ffi::OsString;

        let name = "ROCKY_TEST_WEBHOOK_SECRET_PARITY_PROBE";

        // (what the variable holds, the posture we expect)
        let mut cases: Vec<(Option<OsString>, WebhookSecret)> = vec![
            (None, WebhookSecret::Absent),
            (Some(OsString::from("s3cret")), WebhookSecret::Present),
            (Some(OsString::from("   ")), WebhookSecret::SetButUnusable),
            (Some(OsString::from("")), WebhookSecret::SetButUnusable),
        ];
        // A lone 0x80 byte is set, and unreadable. This is the case the obvious
        // probe spelling reports as `Absent`.
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStringExt;
            cases.push((
                Some(OsString::from_vec(vec![0x80])),
                WebhookSecret::SetButUnusable,
            ));
        }

        for (value, expected) in cases {
            // SAFETY: single-threaded test body; the variable is unique to this
            // test and cleared on every iteration.
            match &value {
                Some(v) => unsafe { std::env::set_var(name, v) },
                None => unsafe { std::env::remove_var(name) },
            }

            let posture = webhook_secret_posture_named(name);
            let gate = webhook_secret_fail_closed_named(name);

            unsafe { std::env::remove_var(name) };

            assert_eq!(posture, expected, "posture for {value:?}");

            // The agreement, which is the actual claim.
            match (&posture, &gate) {
                (WebhookSecret::Present, Ok(Some(_))) => {}
                (WebhookSecret::Absent, Ok(None)) => {}
                (WebhookSecret::SetButUnusable, Err(_)) => {}
                (p, g) => panic!(
                    "the settings route and the startup gate disagree about \
                     {value:?}: route says {p:?}, gate says {}",
                    match g {
                        Ok(Some(_)) => "a usable secret".to_string(),
                        Ok(None) => "no secret".to_string(),
                        Err(e) => format!("refuse to start ({e})"),
                    }
                ),
            }
        }
    }

    /// The parity above proves the two agree about ONE variable. This proves it
    /// is the variable that matters — a gate reading `ROCKY_WEBHOOK_SECRET`
    /// while the route reports something else would satisfy every assertion in
    /// that test and still be a lie.
    #[test]
    fn the_gate_and_the_probe_read_one_variable() {
        assert_eq!(WEBHOOK_SECRET_ENV, "ROCKY_WEBHOOK_SECRET");
    }

    /// A settings route must never be the reason a server stops starting.
    ///
    /// `rocky serve` starts today against a malformed `rocky.toml` — the
    /// scheduler re-reads that file each tick and skips the tick on a parse
    /// error. Reading it at startup to fill two labels must keep that true, and
    /// must say WHY the labels are missing rather than reporting a default that
    /// looks like a real answer.
    #[test]
    fn a_malformed_config_is_reported_not_fatal() {
        use rocky_server::state::ConfigStatus;

        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("rocky.toml");
        std::fs::write(&config, "this is not = [valid toml").unwrap();

        let labels = config_posture(Some(&config));

        assert_eq!(labels.config_status, ConfigStatus::Unreadable);
        assert!(
            labels.state_backend.is_none() && labels.concurrency_control.is_none(),
            "an unparsable config must not yield a default that reads as configured"
        );
    }

    /// An absent config is an ordinary fact, and a DIFFERENT one from a broken
    /// config. Collapsing the two would leave `state_backend: null` unexplained
    /// — no other HTTP route distinguishes them.
    #[test]
    fn an_absent_config_is_not_an_unreadable_one() {
        use rocky_server::state::ConfigStatus;

        let dir = tempfile::tempdir().unwrap();
        let labels = config_posture(Some(&dir.path().join("rocky.toml")));

        assert_eq!(labels.config_status, ConfigStatus::Absent);
        assert!(labels.state_backend.is_none() && labels.concurrency_control.is_none());
    }

    /// A readable config yields the real labels.
    #[test]
    fn a_readable_config_reports_its_state_backend() {
        use rocky_core::config::{ConcurrencyControl, StateBackend};
        use rocky_server::state::ConfigStatus;

        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("rocky.toml");
        std::fs::write(
            &config,
            "[adapter]\ntype = \"duckdb\"\n\n\
             [pipeline.p]\ntype = \"transformation\"\nmodels = \"models/**\"\n\n\
             [pipeline.p.target]\nadapter = \"default\"\n\n\
             [state]\nbackend = \"s3\"\ns3_bucket = \"example\"\n\
             concurrency_control = \"cas\"\n",
        )
        .unwrap();

        let labels = config_posture(Some(&config));

        assert_eq!(labels.config_status, ConfigStatus::Loaded);
        assert_eq!(labels.state_backend, Some(StateBackend::S3));
        assert_eq!(labels.concurrency_control, Some(ConcurrencyControl::Cas));
    }

    /// An unset `concurrency_control` reports the backend default, not `off`:
    /// `cas` on a conditional-write backend (#1228).
    #[test]
    fn an_unset_concurrency_control_reports_the_backend_default() {
        use rocky_core::config::ConcurrencyControl;

        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("rocky.toml");
        for (state, expected) in [
            (
                "backend = \"s3\"\ns3_bucket = \"example\"\n",
                ConcurrencyControl::Cas,
            ),
            ("backend = \"local\"\n", ConcurrencyControl::Off),
        ] {
            std::fs::write(
                &config,
                format!(
                    "[adapter]\ntype = \"duckdb\"\n\n\
                     [pipeline.p]\ntype = \"transformation\"\nmodels = \"models/**\"\n\n\
                     [pipeline.p.target]\nadapter = \"default\"\n\n\
                     [state]\n{state}"
                ),
            )
            .unwrap();
            assert_eq!(
                config_posture(Some(&config)).concurrency_control,
                Some(expected),
                "{state}"
            );
        }
    }

    /// P3-8: the label reports the mode the writers resolve to, not the
    /// request. An explicit `cas` on a backend with no conditional-write tier
    /// runs as `off`, so it reports `off`.
    #[test]
    fn an_explicit_cas_without_a_cas_tier_reports_off() {
        use rocky_core::config::ConcurrencyControl;

        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("rocky.toml");
        std::fs::write(
            &config,
            "[adapter]\ntype = \"duckdb\"\n\n\
             [pipeline.p]\ntype = \"transformation\"\nmodels = \"models/**\"\n\n\
             [pipeline.p.target]\nadapter = \"default\"\n\n\
             [state]\nbackend = \"local\"\nconcurrency_control = \"cas\"\n",
        )
        .unwrap();
        assert_eq!(
            config_posture(Some(&config)).concurrency_control,
            Some(ConcurrencyControl::Off)
        );
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

    /// **Red team.** `build_serve_state` must not read `rocky.toml`.
    ///
    /// It sits on the path to `TcpListener::bind`, and on a plain `rocky serve`
    /// nothing on that path reads a file. (`--scheduler` without an explicit
    /// `--poll-interval` does, via `resolved_poll_interval` — inherited, and
    /// not something this changes.) An eager read for two report fields would let a
    /// `rocky.toml` that is a FIFO or sits on a stalled mount stop the server
    /// binding at all. Loader ERRORS are tolerated; a read that never returns
    /// is not something tolerance catches.
    ///
    /// **What this does and does not prove.** It pins that THIS function leaves
    /// the cell unresolved. It does not prove the listener binds, and it is not
    /// a claim that nothing anywhere reads the config first: the initial
    /// compile does, on its own spawned task, which is why it does not gate the
    /// bind. Asserting on the cell is what makes the narrow property a fact
    /// rather than an intention.
    #[tokio::test]
    async fn build_serve_state_does_not_read_the_config() {
        let models = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../rocky-compiler/tests/fixtures/simple_project/models");
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("rocky.toml");
        std::fs::write(&config, "[adapter]\ntype = \"duckdb\"\n").unwrap();

        let (state, _) = build_serve_state(
            &models,
            false,
            None,
            Some(&config),
            "127.0.0.1",
            Some("s3cret".to_string()),
            None,
            false,
            Vec::new(),
            false,
            Vec::new(),
            false,
            None,
            UiBuild::NoFeature,
        )
        .expect("builds");

        assert!(
            state.settings.config_labels.get().is_none(),
            "build_serve_state read the config; that read sits on the path to bind"
        );
    }

    /// `rocky serve` judges each model against the warehouse of the pipeline
    /// that loads it, as `rocky compile` does. Two pipelines load a merge
    /// model each; only the one on ClickHouse (no upsert, E053) is refused.
    /// Goes through `build_serve_state`, so it also covers the installation of
    /// the checks, and reads the diagnostics the API serves from. The
    /// ClickHouse model lives outside `models/`, so it is only compiled, and
    /// only judged, because the compile reads every pipeline's root (#2011).
    #[tokio::test]
    async fn serve_resolves_per_pipeline_model_targets_like_compile() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let write_merge_model = |sub: &str, name: &str| {
            let models = root.join(sub);
            std::fs::create_dir_all(&models).unwrap();
            std::fs::write(models.join(format!("{name}.sql")), "SELECT 1 AS id").unwrap();
            std::fs::write(
                models.join(format!("{name}.toml")),
                "[strategy]\ntype = \"merge\"\nunique_key = [\"id\"]\n\n\
                 [target]\ncatalog = \"\"\nschema = \"s\"\n",
            )
            .unwrap();
        };
        write_merge_model("reporting", "on_clickhouse");
        write_merge_model("models/duck", "on_duckdb");
        let config = root.join("rocky.toml");
        std::fs::write(
            &config,
            "[adapter.ch]\ntype = \"clickhouse\"\nhost = \"localhost\"\n\n\
             [adapter.local]\ntype = \"duckdb\"\npath = \":memory:\"\n\n\
             [pipeline.a]\ntype = \"transformation\"\nmodels = \"reporting/**\"\n\
             target = { adapter = \"ch\" }\n\n\
             [pipeline.b]\ntype = \"transformation\"\nmodels = \"models/duck/**\"\n\
             target = { adapter = \"local\" }\n",
        )
        .unwrap();

        let (state, _) = build_serve_state(
            &root.join("models"),
            false,
            None,
            Some(&config),
            "127.0.0.1",
            Some("s3cret".to_string()),
            None,
            false,
            Vec::new(),
            false,
            Vec::new(),
            false,
            None,
            UiBuild::NoFeature,
        )
        .expect("builds");
        let outcome = state.recompile().await;
        assert!(outcome.compile_error.is_none(), "{outcome:?}");

        let guard = state.compile_result.read().await;
        let result = guard.as_ref().expect("compiled");
        let refused: Vec<&str> = result
            .diagnostics
            .iter()
            .filter(|d| &*d.code == "E053")
            .map(|d| d.model.as_str())
            .collect();
        assert_eq!(refused, vec!["on_clickhouse"], "{:?}", result.diagnostics);
        // `/project` reports the flag, not the diagnostics: an added E053 must set it.
        assert!(result.has_errors, "{:?}", result.diagnostics);
    }

    /// **The producer-to-consumer wire for the settings snapshot.** The route
    /// tests build a `SettingsSnapshot` by hand, so none of them would notice
    /// if `build_serve_state` ignored its `host` argument or hard-coded a
    /// posture — the fields would be written by tests and never by the CLI.
    ///
    /// A settings route that names a host the server is not bound to is worse
    /// than no route: an operator reads `bind_host` to decide whether the
    /// server is exposed. So this crosses the real function and asserts on the
    /// snapshot the handler actually projects.
    #[tokio::test]
    async fn the_flags_reach_the_settings_snapshot() {
        let models = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../rocky-compiler/tests/fixtures/simple_project/models");

        for (host, scheduler) in [("127.0.0.1", false), ("0.0.0.0", false)] {
            let (state, _) = build_serve_state(
                &models,
                false,
                None,
                None,
                host,
                Some("s3cret".to_string()),
                None,
                false,
                Vec::new(),
                false,
                // Passed WITHOUT `--ui`, so the guard never exists and the
                // reported list must stay empty.
                vec!["example.test".to_string()],
                scheduler,
                None,
                UiBuild::NoFeature,
            )
            .expect("a well-formed serve builds a state");

            assert_eq!(
                state.settings.bind_host, host,
                "the snapshot must report the host the listener binds"
            );
            assert_eq!(state.settings.scheduler, scheduler);
            assert!(
                state.ui.is_none(),
                "no --ui, so there is no host guard to report"
            );
        }
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
            let (state, _) = build_serve_state(
                &models,
                false,
                None,
                None,
                "127.0.0.1",
                Some("s3cret".to_string()),
                raw.clone(),
                false,
                Vec::new(),
                false,
                Vec::new(),
                false,
                None,
                UiBuild::NoFeature,
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
        let err = resolve_serve_token(
            Some("s3cret".into()),
            Some("readonly".into()),
            false,
            UiExposure::NoUi,
        )
        .expect_err("a typo must not resolve to a full-scope token");
        assert!(err.to_string().contains("unknown token scope"), "{err}");
    }

    /// `--read-only` is `--token-scope read-only`; with `--token-scope full`
    /// it is a contradiction and refused.
    #[test]
    fn read_only_flag_folds_into_the_scope_and_conflicts_with_full() {
        assert_eq!(merge_read_only_flag(false, None).unwrap(), None);
        assert_eq!(
            merge_read_only_flag(false, Some("full".into())).unwrap(),
            Some("full".into())
        );
        assert_eq!(
            merge_read_only_flag(true, None).unwrap(),
            Some("read-only".into())
        );
        assert_eq!(
            merge_read_only_flag(true, Some("read-only".into())).unwrap(),
            Some("read-only".into())
        );
        let err = merge_read_only_flag(true, Some("full".into())).unwrap_err();
        assert!(err.to_string().contains("conflicts"), "{err}");
    }

    #[test]
    fn exposure_follows_ui_bind_and_fronting() {
        use UiExposure::{Fronted, Local, NoUi, Remote};
        for host in ["127.0.0.1", "::1", "localhost"] {
            assert_eq!(UiExposure::for_bind(true, host, false), Local, "{host}");
            assert_eq!(UiExposure::for_bind(true, host, true), Fronted, "{host}");
            assert_eq!(UiExposure::for_bind(false, host, true), NoUi, "{host}");
        }
        for host in ["0.0.0.0", "::", "192.168.1.10", "example.test"] {
            assert_eq!(UiExposure::for_bind(true, host, false), Remote, "{host}");
            assert_eq!(UiExposure::for_bind(true, host, true), Remote, "{host}");
        }
    }

    /// Each generated token is 64 lowercase hex characters (two v4 UUIDs, 244
    /// random bits), and two calls never agree.
    #[test]
    fn generated_tokens_are_long_hex_and_differ() {
        let tokens: Vec<String> = (0..16).map(|_| generate_ui_token()).collect();
        for token in &tokens {
            assert_eq!(token.len(), 64, "{token}");
            assert!(
                token
                    .chars()
                    .all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c)),
                "{token}"
            );
        }
        let distinct: std::collections::BTreeSet<&String> = tokens.iter().collect();
        assert_eq!(distinct.len(), tokens.len());
    }

    /// The wire, for the case that can run without the `ui` feature: through
    /// `build_serve_state`, a loopback server with no `--ui` and no token
    /// installs no auth and reports no generated token.
    #[tokio::test]
    async fn build_serve_state_without_ui_generates_nothing() {
        if serve_token_env_is_set() {
            return;
        }
        let (state, origin) = build_ui_matrix_state(UiCase {
            ui: false,
            ..UiCase::local()
        })
        .expect("a loopback serve with no token builds");
        assert!(state.auth.is_none());
        assert_eq!(origin, TokenOrigin::Operator);
        assert!(
            ui_announcement(false, "127.0.0.1", 8080, state.auth.as_ref(), origin).is_none(),
            "no --ui, nothing to announce"
        );
    }

    fn serve_token_env_is_set() -> bool {
        let set = std::env::var_os("ROCKY_SERVE_TOKEN").is_some()
            || std::env::var_os("ROCKY_SERVE_TOKEN_SCOPE").is_some();
        if set {
            eprintln!("skipping: a ROCKY_SERVE_TOKEN* variable is set in this environment");
        }
        set
    }

    /// One row of the `--ui` token matrix, as raw flags.
    #[derive(Clone)]
    struct UiCase {
        ui: bool,
        host: &'static str,
        secret: Option<&'static str>,
        scope: Option<&'static str>,
        read_only: bool,
        allowed_hosts: Vec<String>,
        allowed_origins: Vec<String>,
    }

    impl UiCase {
        /// `rocky serve --ui` on loopback with no other flag.
        fn local() -> Self {
            Self {
                ui: true,
                host: "127.0.0.1",
                secret: None,
                scope: None,
                read_only: false,
                allowed_hosts: Vec::new(),
                allowed_origins: Vec::new(),
            }
        }

        fn fronted_by_host() -> Self {
            Self {
                allowed_hosts: vec!["rocky.example.test".to_string()],
                ..Self::local()
            }
        }

        fn fronted_by_origin() -> Self {
            Self {
                allowed_origins: vec!["https://portal.example.test".to_string()],
                ..Self::local()
            }
        }
    }

    /// The SAME function `run_serve` calls, with the raw flags, through the
    /// [`UiBuild`] seam so it runs without the `ui` feature.
    fn build_ui_matrix_state(
        case: UiCase,
    ) -> Result<(
        std::sync::Arc<rocky_server::state::ServerState>,
        TokenOrigin,
    )> {
        let models = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../rocky-compiler/tests/fixtures/simple_project/models");
        build_serve_state(
            &models,
            false,
            None,
            None,
            case.host,
            case.secret.map(str::to_string),
            case.scope.map(str::to_string),
            case.read_only,
            case.allowed_origins,
            case.ui,
            case.allowed_hosts,
            false,
            None,
            UiBuild::Page(std::sync::Arc::new(rocky_server::ui::InMemoryAssets(
                std::collections::BTreeMap::new(),
            ))),
        )
    }

    /// What a matrix row installs: the token's scope and origin, or the
    /// refusal text.
    fn matrix_outcome(case: UiCase) -> Result<(TokenScope, TokenOrigin), String> {
        match build_ui_matrix_state(case) {
            Ok((state, origin)) => {
                let token = state.auth.as_ref().expect("--ui always installs a token");
                assert!(state.ui.is_some(), "--ui installs the UI");
                if origin == TokenOrigin::Generated {
                    assert_eq!(token.secret.len(), 64);
                }
                Ok((token.scope, origin))
            }
            Err(e) => Err(e.to_string()),
        }
    }

    /// **The `--ui` token matrix, row by row, through `build_serve_state`.**
    ///
    /// ```text
    ///   bind          fronted  secret  scope         -> result
    ///   loopback      no       none    none or full  -> generated, FULL (operator mode)
    ///   loopback      no       none    read-only     -> generated, read-only
    ///   loopback      yes      none    none or r-o   -> generated, read-only
    ///   loopback      yes      none    full          -> refused
    ///   loopback      no       given   full          -> accepted
    ///   loopback      yes      given   full          -> refused
    ///   loopback      any      given   read-only     -> accepted
    ///   non-loopback  any      any     full          -> refused
    ///   non-loopback  any      none    -             -> refused
    /// ```
    #[tokio::test]
    async fn the_ui_token_matrix_through_build_serve_state() {
        if serve_token_env_is_set() {
            return;
        }
        use TokenOrigin::{Generated, Operator};
        use TokenScope::{Full, ReadOnly};
        let fronted = [UiCase::fronted_by_host(), UiCase::fronted_by_origin()];

        // loopback, not fronted, no secret, none or full -> generated FULL.
        for host in ["127.0.0.1", "::1", "localhost"] {
            for scope in [None, Some("full")] {
                let case = UiCase {
                    host,
                    scope,
                    ..UiCase::local()
                };
                assert_eq!(
                    matrix_outcome(case),
                    Ok((Full, Generated)),
                    "{host} {scope:?}"
                );
            }
        }
        // loopback, not fronted, no secret, read-only (either spelling).
        for (scope, read_only) in [(Some("read-only"), false), (None, true)] {
            let case = UiCase {
                scope,
                read_only,
                ..UiCase::local()
            };
            assert_eq!(matrix_outcome(case), Ok((ReadOnly, Generated)));
        }
        for base in fronted.clone() {
            // fronted, no secret, none or read-only -> generated read-only.
            for (scope, read_only) in [(None, false), (Some("read-only"), false), (None, true)] {
                let case = UiCase {
                    scope,
                    read_only,
                    ..base.clone()
                };
                assert_eq!(matrix_outcome(case), Ok((ReadOnly, Generated)));
            }
            // fronted, no secret, full -> refused.
            let err = matrix_outcome(UiCase {
                scope: Some("full"),
                ..base.clone()
            })
            .unwrap_err();
            assert!(err.contains("per-person tokens"), "{err}");
            // fronted, given, full (named or defaulted) -> refused.
            for scope in [None, Some("full")] {
                let err = matrix_outcome(UiCase {
                    secret: Some("s3cret"),
                    scope,
                    ..base.clone()
                })
                .unwrap_err();
                assert!(err.contains("per-person tokens"), "{err}");
            }
            // fronted, given, read-only -> accepted.
            let case = UiCase {
                secret: Some("s3cret"),
                read_only: true,
                ..base.clone()
            };
            assert_eq!(matrix_outcome(case), Ok((ReadOnly, Operator)));
        }
        // loopback, not fronted, given, full (named or defaulted) -> accepted.
        for scope in [None, Some("full")] {
            let case = UiCase {
                secret: Some("s3cret"),
                scope,
                ..UiCase::local()
            };
            assert_eq!(matrix_outcome(case), Ok((Full, Operator)), "{scope:?}");
        }
        // loopback, not fronted, given, read-only -> accepted.
        let case = UiCase {
            secret: Some("s3cret"),
            scope: Some("read-only"),
            ..UiCase::local()
        };
        assert_eq!(matrix_outcome(case), Ok((ReadOnly, Operator)));

        for host in ["0.0.0.0", "::", "192.168.1.10"] {
            let remote = UiCase {
                host,
                ..UiCase::local()
            };
            // non-loopback, given, full -> refused.
            for scope in [None, Some("full")] {
                let err = matrix_outcome(UiCase {
                    secret: Some("s3cret"),
                    scope,
                    ..remote.clone()
                })
                .unwrap_err();
                assert!(
                    err.contains("non-loopback bind requires a read-only"),
                    "{err}"
                );
            }
            // non-loopback, no secret -> refused (no scope: the --ui rule;
            // a scope: the scope-without-token rule).
            let err = matrix_outcome(remote.clone()).unwrap_err();
            assert!(err.contains("refuses to start without a token"), "{err}");
            let err = matrix_outcome(UiCase {
                read_only: true,
                ..remote.clone()
            })
            .unwrap_err();
            assert!(err.contains("no token was"), "{err}");
            // non-loopback, given, read-only -> accepted, as before.
            let case = UiCase {
                secret: Some("s3cret"),
                read_only: true,
                ..remote
            };
            assert_eq!(matrix_outcome(case), Ok((ReadOnly, Operator)));
        }

        // `--read-only` with `--token-scope full` is a contradiction.
        let err = matrix_outcome(UiCase {
            scope: Some("full"),
            read_only: true,
            ..UiCase::local()
        })
        .unwrap_err();
        assert!(err.contains("conflicts"), "{err}");
    }

    /// Without `--ui` nothing changes: `--read-only` is a scope like any
    /// other, a full token on a non-loopback bind is fine, and a scope with
    /// no token is still an error.
    #[tokio::test]
    async fn without_ui_the_matrix_does_not_apply() {
        if serve_token_env_is_set() {
            return;
        }
        let no_ui = UiCase {
            ui: false,
            ..UiCase::local()
        };
        let (state, _) = build_ui_matrix_state(UiCase {
            host: "0.0.0.0",
            secret: Some("s3cret"),
            allowed_origins: vec!["https://portal.example.test".to_string()],
            ..no_ui.clone()
        })
        .expect("a full token, fronted, without --ui builds as before");
        assert_eq!(state.auth.as_ref().unwrap().scope, TokenScope::Full);
        let (state, _) = build_ui_matrix_state(UiCase {
            secret: Some("s3cret"),
            read_only: true,
            ..no_ui.clone()
        })
        .expect("--read-only without --ui is a scope");
        assert_eq!(state.auth.as_ref().unwrap().scope, TokenScope::ReadOnly);
        let err = build_ui_matrix_state(UiCase {
            read_only: true,
            ..no_ui
        })
        .err()
        .expect("a scope without a token is still an error without --ui");
        assert!(err.to_string().contains("no token was"), "{err}");
    }

    /// `--ui --scheduler` with a generated token still needs
    /// `ROCKY_WEBHOOK_SECRET`, in operator mode too.
    #[test]
    fn a_generated_token_does_not_lift_the_scheduler_rule() {
        for exposure in [UiExposure::Local, UiExposure::Fronted] {
            let (token, _) = resolve_token_for_bind(None, None, exposure).unwrap();
            let err = validate_ui_flags(exposure, token.as_ref(), true, false, true).unwrap_err();
            assert!(err.to_string().contains("ROCKY_WEBHOOK_SECRET"), "{err}");
        }
    }

    /// **Operator mode announces its link, token included** — through the
    /// state `build_serve_state` installs and the `ui_announcement`
    /// `run_serve` calls. A read-only UI gets the link and no operator note.
    #[tokio::test]
    async fn operator_mode_announces_the_login_link_with_the_token() {
        if serve_token_env_is_set() {
            return;
        }
        let (state, origin) = build_ui_matrix_state(UiCase::local()).expect("operator mode");
        let token = state.auth.as_ref().unwrap();
        let announcement =
            ui_announcement(true, "127.0.0.1", 8080, Some(token), origin).expect("announced");
        assert_eq!(
            announcement.address,
            format!("http://127.0.0.1:8080/login?t={}", token.secret)
        );
        assert!(announcement.operator_mode);
        assert_eq!(announcement.token_origin, TokenOrigin::Generated);

        let (state, origin) = build_ui_matrix_state(UiCase {
            read_only: true,
            ..UiCase::local()
        })
        .expect("read-only");
        let announcement =
            ui_announcement(true, "127.0.0.1", 8080, state.auth.as_ref(), origin).unwrap();
        assert!(!announcement.operator_mode);
        assert!(announcement.address.contains("/login?t="));
    }
}

#[cfg(test)]
mod serve_config_presence_tests {
    //! `rocky serve` binds one `rocky.toml` for the lifetime of the process.
    //! Getting "is there a config here" wrong does not fail — it starts a
    //! server that answers a different project (#1729).
    //!
    //! The symlink cases are `#[cfg(unix)]`: creating a symlink on Windows
    //! needs Developer Mode or `SeCreateSymbolicLinkPrivilege`, so the test
    //! would fail for a reason unrelated to the discriminator. The fix itself
    //! is portable.

    use super::resolve_serve_config_path;

    /// The #1729 defect. A dangling `rocky.toml` answered `false` to
    /// `Path::exists()`, so `serve` started as though no `--config` had been
    /// given: empty `[mask]` / `allow_unmasked` / `[freshness]` on every
    /// compile, the default schema-cache posture, and the scheduler's webhook
    /// spool moved from the named project to `./.rocky` in the current
    /// directory. It must refuse to start and name the link.
    #[cfg(unix)]
    #[test]
    fn a_dangling_config_symlink_refuses_to_start() {
        let tmp = tempfile::tempdir().unwrap();
        let target = tmp.path().join("prod.toml");
        let config = tmp.path().join("rocky.toml");
        std::os::unix::fs::symlink(&target, &config).unwrap();
        assert!(
            !config.exists(),
            "precondition: the probe this replaces reports the config as absent"
        );

        let err = resolve_serve_config_path(&config)
            .expect_err("a config that is present but unreadable must refuse");
        let rendered = format!("{err:#}");
        assert!(
            rendered.contains(&config.display().to_string())
                && rendered.contains(&target.display().to_string()),
            "the refusal must name the config path and the link target, got: {rendered}"
        );
        assert!(
            rendered.contains("refusing to start"),
            "and say the server did not start, got: {rendered}"
        );
    }

    /// The control. `rocky serve --models models/` over a directory with no
    /// project around it is a documented flow, and `None` is its honest
    /// answer. It must keep starting.
    #[test]
    fn no_config_at_all_still_starts_config_less() {
        let tmp = tempfile::tempdir().unwrap();
        let absent = tmp.path().join("rocky.toml");
        assert_eq!(
            resolve_serve_config_path(&absent).expect("absence is not an error"),
            None,
            "no rocky.toml must still resolve to the config-less serve"
        );
    }

    /// A config that is there binds, and — the boundary this fix deliberately
    /// does NOT move — a config that is there and does not parse binds too.
    /// The server reloads the config per compile and degrades on a load error
    /// there; refusing here would answer a different question from #1729's.
    #[test]
    fn a_present_config_binds_even_when_it_does_not_parse() {
        let tmp = tempfile::tempdir().unwrap();

        let good = tmp.path().join("rocky.toml");
        std::fs::write(&good, "[adapter]\ntype = \"duckdb\"\npath = \"x.duckdb\"\n").unwrap();
        assert_eq!(
            resolve_serve_config_path(&good).expect("a readable config binds"),
            Some(good.as_path())
        );

        let bad = tmp.path().join("broken.toml");
        std::fs::write(&bad, "not = = toml\n").unwrap();
        assert_eq!(
            resolve_serve_config_path(&bad).expect("a malformed config still binds"),
            Some(bad.as_path()),
            "serve must not start refusing malformed configs it accepted before"
        );
    }

    /// A symlink that resolves is an ordinary config. The discriminator is
    /// presence, never "is this path a link".
    #[cfg(unix)]
    #[test]
    fn a_resolvable_config_symlink_binds() {
        let tmp = tempfile::tempdir().unwrap();
        let target = tmp.path().join("prod.toml");
        std::fs::write(
            &target,
            "[adapter]\ntype = \"duckdb\"\npath = \"x.duckdb\"\n",
        )
        .unwrap();
        let config = tmp.path().join("rocky.toml");
        std::os::unix::fs::symlink(&target, &config).unwrap();
        assert_eq!(
            resolve_serve_config_path(&config).expect("a resolvable symlink binds"),
            Some(config.as_path()),
            "and it binds the path the operator typed, not the resolved target"
        );
    }
}

#[cfg(test)]
mod poll_interval_tests {
    use super::resolved_poll_interval;
    use crate::commands::scheduler::DEFAULT_POLL_INTERVAL;
    use std::time::Duration;

    fn config_with(body: &str) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rocky.toml");
        std::fs::write(
            &path,
            format!(
                "[adapter]\ntype = \"duckdb\"\n\n\
                 [pipeline.p]\ntype = \"transformation\"\nmodels = \"models/**\"\n\n\
                 [pipeline.p.target]\nadapter = \"default\"\n{body}"
            ),
        )
        .unwrap();
        (dir, path)
    }

    /// The whole point of #1620: a project that sets the key gets that cadence.
    /// It parsed and validated while nothing read it, so the value was inert.
    #[test]
    fn the_projects_configured_cadence_is_used() {
        let (_dir, path) = config_with("\n[schedule]\npoll_interval_seconds = 45\n");
        assert_eq!(resolved_poll_interval(None, &path), Duration::from_secs(45));
    }

    /// The flag still wins, so the documented override keeps working.
    #[test]
    fn the_flag_overrides_the_configured_cadence() {
        let (_dir, path) = config_with("\n[schedule]\npoll_interval_seconds = 45\n");
        assert_eq!(
            resolved_poll_interval(Some(7), &path),
            Duration::from_secs(7)
        );
    }

    /// A project that declares no `[schedule]` gets the built-in, unchanged.
    #[test]
    fn no_schedule_block_falls_back_to_the_default() {
        let (_dir, path) = config_with("");
        assert_eq!(resolved_poll_interval(None, &path), DEFAULT_POLL_INTERVAL);
    }

    /// A config that cannot be read must not stop the server from starting —
    /// the same tolerance the tick loop applies when it re-reads per iteration.
    /// Asserted for both a missing file and an unparseable one, because the
    /// two take different paths through `load_rocky_config`.
    #[test]
    fn an_unreadable_config_leaves_the_default_rather_than_refusing() {
        let dir = tempfile::tempdir().unwrap();

        let missing = dir.path().join("nope.toml");
        assert_eq!(
            resolved_poll_interval(None, &missing),
            DEFAULT_POLL_INTERVAL
        );

        let broken = dir.path().join("broken.toml");
        std::fs::write(&broken, "this is not = valid toml [[[").unwrap();
        assert_eq!(resolved_poll_interval(None, &broken), DEFAULT_POLL_INTERVAL);

        // And the flag still wins over an unreadable config, so an operator
        // can always force a cadence.
        assert_eq!(
            resolved_poll_interval(Some(3), &broken),
            Duration::from_secs(3)
        );
    }
}
