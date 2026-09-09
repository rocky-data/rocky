//! Shared compiler state for the server and LSP.
//!
//! Holds the latest `CompileResult` behind a `RwLock`, recompiled
//! on file changes when watch mode is active.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use rocky_compiler::compile::{CompileResult, CompilerConfig};
use rocky_core::dag_status::DagStatusStore;

use crate::auth::ServeToken;
use crate::schema_cache_throttle::SchemaCacheThrottle;

/// How long `GET /api/v1/models/{name}/rows` waits for a sample before
/// answering `504 sample_timeout`, unless [`ServerState::set_sample_timeout`]
/// changed it. The query itself may keep running past it: cancelling one is
/// adapter-specific and is not in this package, which the guide says plainly.
pub const DEFAULT_SAMPLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Shared server state holding the latest compilation result.
pub struct ServerState {
    pub models_dir: PathBuf,
    /// Whether [`Self::models_dir`] was named explicitly (`serve --models`) or
    /// is just the conventional default.
    ///
    /// Only the DAG projection cares. `GET /api/v1/dag` treats an explicit
    /// directory as a whole-project override, reading every transformation
    /// pipeline from it — the HTTP analogue of `rocky dag --models`. Applying
    /// that override to a *defaulted* path is what reproduced #1261 over HTTP:
    /// pipelines that declare their own model directories had them silently
    /// replaced by `models`, collapsing the graph. Defaults to `false`, so a
    /// state built by any constructor that does not name it keeps each
    /// pipeline's own directory — the non-overriding, more conservative read.
    pub models_dir_is_explicit: bool,
    pub contracts_dir: Option<PathBuf>,
    pub config_path: Option<PathBuf>,
    pub compile_result: RwLock<Option<CompileResult>>,
    /// Why the LAST compile produced no result, when it did not (#1823).
    ///
    /// `recompile` runs in the background and used to log a failed compile
    /// and carry on; `compile_result` then held either nothing or an earlier
    /// compile's result, and every reader of "no result" — `GET
    /// /api/v1/project`, the SPA — presented a project whose models cannot
    /// be read as clean: no diagnostics, `has_errors: false`. This is the
    /// third state beside "compiled clean" and "compiled with errors":
    /// "the compile did not complete", with its reason. Set by every
    /// failing exit of `recompile` (a compile error, a panicked compile
    /// task); cleared by a compile that produces a result. A stale
    /// `compile_result` may sit beside it, for the LSP's sake; a status
    /// reader must not project that result's counts while this stands.
    pub compile_failure: RwLock<Option<String>>,
    /// Latest DAG execution status, exposed at `GET /api/v1/dag/status`.
    pub dag_status: DagStatusStore,
    /// App-level single-mutating-job guard for the HTTP job model. A second
    /// `run`/`apply` submission while one holds it is rejected with
    /// `409 mutation_in_progress`. In-memory only — does not survive a restart
    /// (the redb advisory lock is the correctness backstop).
    pub mutation_permit: crate::jobs::MutationPermit,
    /// Fast in-memory cache of job records fronting the durable `jobs` state
    /// table, serving `GET /api/v1/jobs/{id}` without touching redb on the hot
    /// path. Repopulated lazily from redb after a restart.
    pub jobs: crate::jobs::JobRegistry,
    /// Bearer token required by the HTTP API auth middleware, together with
    /// the [`crate::auth::TokenScope`] it grants. `None` means "no auth"; in
    /// that mode `rocky_cli::api::serve` refuses to bind a non-loopback host.
    /// See [`crate::auth::require_bearer_token`].
    ///
    /// Secret and scope travel as one value so a scope can never be set
    /// without a token to attach it to.
    pub auth: Option<ServeToken>,
    /// CORS allowlist passed to [`crate::auth::build_cors_layer`]. An
    /// empty list means same-origin only.
    pub allowed_origins: Vec<String>,
    /// An explicit `--state-path` override for this process, when one was given.
    /// `None` means "derive the conventional path from `models_dir`" — the
    /// historical behavior. Callers that resolve a state path themselves (the
    /// CLI's global flag) pass it here so every state-backed surface of the
    /// server — the job records AND the resident scheduler's cursors, claims,
    /// and child run history — agrees on one file instead of silently splitting
    /// across two.
    pub state_path: Option<PathBuf>,
    /// Webhook-ingress configuration, present only when `serve --scheduler` is
    /// active. `None` means the ingress route (`POST /api/v1/hooks/trigger/…`)
    /// answers `404` — a webhook can only be consumed by a resident reconciler,
    /// so ingress without one is disabled.
    pub webhook: Option<crate::webhook_ingress::WebhookIngress>,
    /// The browser UI (`rocky serve --ui`): its files, the `Host` values it
    /// accepts. `None` means no UI routes and no host guard.
    pub ui: Option<crate::ui::UiConfig>,
    /// Per-session throttle for the "N sources hit" info log so it
    /// emits once per server start, not once per recompile. Keyed on
    /// `models_dir`, which stays constant.
    schema_cache_throttle: SchemaCacheThrottle,
    /// The one permit every state-store open in this process takes: the
    /// request-local reads, the job records, and the resident scheduler's tick.
    ///
    /// redb holds an exclusive `flock` on the store file for the life of a
    /// handle, and a concurrent open polls that lock a few times before it
    /// gives up with a busy error. Two of this process's own reads racing for
    /// the file therefore turned into `503 engine_busy` at two concurrent
    /// clients, and a scheduler tick under sustained reads gave up as
    /// `state_busy` (43 of 67 ticks at eight polling clients), before any
    /// other process was involved. Opens that go through this permit take
    /// turns instead of racing; the busy error is left to mean what it says,
    /// another process holding the store.
    pub store_access: Arc<tokio::sync::Semaphore>,
    /// Admission for `GET /api/v1/review/{plan_id}`: one review diff at a time.
    ///
    /// A diff is two compiles and a git read — local, bounded work, hundreds of
    /// milliseconds on a large project. A caller that finds it busy waits
    /// briefly rather than being refused, because a refusal for work that short
    /// is noise.
    pub review_diffs: Arc<tokio::sync::Semaphore>,
    /// Admission for `GET /api/v1/models/{name}/rows`: one warehouse sample at
    /// a time.
    ///
    /// Deliberately **separate** from [`Self::review_diffs`]. A sample is a
    /// warehouse round trip bounded only by a 30 second timeout; a diff is
    /// local and ends in milliseconds. Sharing one permit would let the slow
    /// class block the fast one — the shape that starved the scheduler behind
    /// the store queue. A caller that finds this one busy is refused at once
    /// with `Retry-After`, because queueing behind a possible 30 seconds is
    /// worse for it than a fast refusal.
    pub warehouse_samples: Arc<tokio::sync::Semaphore>,
    /// The sample deadline, in milliseconds: [`DEFAULT_SAMPLE_TIMEOUT`]
    /// unless [`ServerState::set_sample_timeout`] changed it. An atomic
    /// rather than a constructor argument because the state is handed out as
    /// an `Arc` (and cloned into the initial compile) before a test can reach
    /// it (#1816).
    sample_timeout_ms: std::sync::atomic::AtomicU64,
}

/// A duration as whole milliseconds, saturating rather than truncating.
fn millis(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

impl ServerState {
    /// How long the samples route waits before answering `504 sample_timeout`.
    pub fn sample_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_millis(
            self.sample_timeout_ms
                .load(std::sync::atomic::Ordering::Relaxed),
        )
    }

    /// Change the sample deadline for every later request. Tests shorten it
    /// to watch the deadline fire; nothing in the server itself calls this.
    pub fn set_sample_timeout(&self, timeout: std::time::Duration) {
        self.sample_timeout_ms
            .store(millis(timeout), std::sync::atomic::Ordering::Relaxed);
    }

    /// Create new server state and perform initial compilation.
    ///
    /// Defaults to the LSP-style configuration (no token, empty CORS
    /// allowlist). Use [`ServerState::with_auth`] to attach a Bearer
    /// token + CORS allowlist before starting the HTTP server.
    pub fn new(
        models_dir: PathBuf,
        contracts_dir: Option<PathBuf>,
        config_path: Option<PathBuf>,
    ) -> Arc<Self> {
        Self::with_auth(
            models_dir,
            contracts_dir,
            config_path,
            None,
            Vec::new(),
            None,
        )
    }

    /// Create new server state with explicit auth + CORS configuration.
    ///
    /// `state_path` is an explicit `--state-path` override; `None` derives the
    /// conventional path from `models_dir` (see [`ServerState::state_path`]).
    pub fn with_auth(
        models_dir: PathBuf,
        contracts_dir: Option<PathBuf>,
        config_path: Option<PathBuf>,
        auth: Option<ServeToken>,
        allowed_origins: Vec<String>,
        state_path: Option<PathBuf>,
    ) -> Arc<Self> {
        Self::with_auth_and_webhook(
            models_dir,
            // Not an explicit `--models`: this constructor's callers (the LSP,
            // the scheduler, tests) point at a directory they derived, not one
            // a user named, so pipelines keep their own model directories.
            false,
            contracts_dir,
            config_path,
            auth,
            allowed_origins,
            state_path,
            None,
            None,
        )
    }

    /// [`ServerState::with_auth`] plus webhook-ingress configuration.
    ///
    /// `webhook` is `Some` only when `serve --scheduler` is active; without a
    /// resident reconciler there is nothing to consume a spooled demand, so the
    /// ingress route stays disabled (`404`).
    #[allow(clippy::too_many_arguments)]
    pub fn with_auth_and_webhook(
        models_dir: PathBuf,
        models_dir_is_explicit: bool,
        contracts_dir: Option<PathBuf>,
        config_path: Option<PathBuf>,
        auth: Option<ServeToken>,
        allowed_origins: Vec<String>,
        state_path: Option<PathBuf>,
        webhook: Option<crate::webhook_ingress::WebhookIngress>,
        ui: Option<crate::ui::UiConfig>,
    ) -> Arc<Self> {
        let state = Arc::new(Self {
            models_dir,
            models_dir_is_explicit,
            contracts_dir,
            config_path,
            state_path,
            webhook,
            ui,
            compile_result: RwLock::new(None),
            compile_failure: RwLock::new(None),
            dag_status: DagStatusStore::new(),
            mutation_permit: crate::jobs::MutationPermit::new(),
            jobs: crate::jobs::JobRegistry::new(),
            auth,
            allowed_origins,
            schema_cache_throttle: SchemaCacheThrottle::new(),
            store_access: Arc::new(tokio::sync::Semaphore::new(1)),
            review_diffs: Arc::new(tokio::sync::Semaphore::new(1)),
            warehouse_samples: Arc::new(tokio::sync::Semaphore::new(1)),
            sample_timeout_ms: std::sync::atomic::AtomicU64::new(millis(DEFAULT_SAMPLE_TIMEOUT)),
        });

        // Initial compile
        let rt_state = state.clone();
        tokio::spawn(async move {
            rt_state.recompile().await;
        });

        state
    }

    /// Recompile the project and update the stored result.
    ///
    /// Returns the reason the project config could not be READ, when that is
    /// what happened. `None` covers both "it loaded" and "there is none" —
    /// a project with no `rocky.toml` is an ordinary project, not a failure.
    ///
    /// `serve` keeps compiling on an unreadable config, which is a contract
    /// rather than an accident (#1625): a resident server watching a
    /// directory should not go dark because someone is mid-edit in
    /// `rocky.toml`. What it must not do is what it did — carry on silently,
    /// so a caller cannot tell "this project declares no masks" from "the
    /// file that declares them could not be parsed". The reason now rides
    /// out on a W013 diagnostic and on this return value.
    pub async fn recompile(&self) -> Option<String> {
        info!(models_dir = %self.models_dir.display(), "compiling project");

        // ONE read of `rocky.toml` for the whole recompile. It used to be
        // loaded twice — here and again inside the schema-cache loader —
        // and each copy decided independently what a broken config meant.
        // That per-caller decision is the defect #1625 is about, so there
        // is now one snapshot and one decision.
        let project_config =
            rocky_core::config::load_optional_project_config(self.config_path.as_deref());

        let config_unreadable = match &project_config {
            // `Ok(None)` is "no rocky.toml", which is an ordinary fact
            // about the project rather than a failure to read one.
            Ok(_) => None,
            Err(e) => {
                let path = self
                    .config_path
                    .as_ref()
                    .map(|p| p.display().to_string())
                    .unwrap_or_else(|| "rocky.toml".to_string());
                warn!(error = %e, config = %path, "rocky.toml could not be read");
                Some(format!("{path} could not be read: {e}"))
            }
        };
        let project_config = project_config.ok().flatten();

        // Load cached source schemas so the server's hover/inlay-hint
        // surfaces typecheck against real warehouse types when the cache
        // is warm. Degrades to empty on cold cache, missing state.redb,
        // or `[cache.schemas] enabled = false`. See
        // `rocky-cli::source_schemas` for the CLI equivalent.
        let schema_cache_config = project_config
            .as_ref()
            .map(|c| c.cache.schemas.clone())
            .unwrap_or_default();
        let source_schemas = self.load_cached_source_schemas(schema_cache_config).await;

        // `[mask]` + `[classifications.allow_unmasked]` (W004) and the
        // `[freshness]` default bit (W005), mirroring the CLI compile path.
        // With no rocky.toml these come through empty and the checks stay
        // silent — matching standalone `rocky compile --models models/`.
        // With an UNREADABLE one they also come through empty, but that is
        // now reported rather than assumed equivalent.
        let (mask, allow_unmasked, project_freshness) = match &project_config {
            Some(cfg) => (
                cfg.mask.clone(),
                cfg.classifications.allow_unmasked.clone(),
                cfg.freshness.clone(),
            ),
            None => (Default::default(), Vec::new(), Default::default()),
        };

        let config = CompilerConfig {
            models_dir: self.models_dir.clone(),
            contracts_dir: self.contracts_dir.clone(),
            source_schemas,
            mask,
            allow_unmasked,
            project_freshness,
            run_vars: rocky_core::run_vars::RunVars::new(),
        };

        // The compile pass walks the model directory, parses every
        // `.rocky` / `.sql` file, and runs type-checking. On a non-trivial
        // project this is hundreds of milliseconds of CPU-bound work; if
        // we run it directly on the async runtime it stalls every other
        // task on this worker thread (HTTP handlers, the file watcher,
        // the LSP). Move it to the blocking pool. Mirrors the pattern at
        // `lsp.rs:468` (PR #263).
        let compile_result =
            match tokio::task::spawn_blocking(move || rocky_compiler::compile::compile(&config))
                .await
            {
                Ok(r) => r,
                Err(join_err) => {
                    warn!(error = %join_err, "compile task join failed");
                    *self.compile_failure.write().await =
                        Some(format!("the compile task did not complete: {join_err}"));
                    return config_unreadable;
                }
            };

        match compile_result {
            Ok(mut result) => {
                // The compiler never saw the config, so it cannot raise
                // this itself. Attach W013 to the stored result so every
                // reader of `compile_result` — the diagnostics counts on
                // `GET /api/v1/meta`, the browser UI — sees that the
                // project-level checks were silent because the file could
                // not be parsed, not because the project declares nothing.
                if let Some(ref reason) = config_unreadable {
                    result
                        .diagnostics
                        .push(rocky_compiler::diagnostic::Diagnostic::warning(
                            rocky_compiler::diagnostic::W013,
                            "rocky.toml",
                            format!(
                                "{reason}. The models still compile, but the project-level \
                                 checks are silent: masking (W004), the project [freshness] \
                                 default (W005), and the cached warehouse schemas all came \
                                 through empty."
                            ),
                        ));
                }
                let model_count = result.project.model_count();
                let diag_count = result.diagnostics.len();
                let has_errors = result.has_errors;
                *self.compile_result.write().await = Some(result);
                *self.compile_failure.write().await = None;
                info!(
                    models = model_count,
                    diagnostics = diag_count,
                    has_errors,
                    "compilation complete"
                );
            }
            Err(e) => {
                // Logged, as before — and RECORDED (#1823), so a reader of
                // the state can tell "the compile failed" from "nothing to
                // report". The warning alone left `GET /api/v1/project` and
                // the SPA describing a project whose models cannot be read as
                // clean.
                warn!(error = %e, "compilation failed");
                *self.compile_failure.write().await = Some(e.to_string());
            }
        }

        config_unreadable
    }

    /// Load the schema-cache-backed `source_schemas` map for this
    /// server's project. Gated on `[cache.schemas] enabled`; resolves the
    /// state file via [`rocky_core::state::resolve_state_path`] so the
    /// server observes exactly the same file that `rocky run` writes to
    /// (unified default — `<models>/.rocky-state.redb` — with the legacy
    /// CWD fallback for existing projects).
    ///
    /// `schema_cache_config` comes from the caller's single `rocky.toml`
    /// snapshot rather than a second read of the file. This used to load the
    /// config again and `unwrap_or_default()` the error, making it the second
    /// place in one function that independently decided a broken config meant
    /// "defaults" (#1625). Absent a config it still falls back to the
    /// defaults (enabled + 24h TTL), so zero-config projects mirror the CLI.
    async fn load_cached_source_schemas(
        &self,
        schema_cache_config: rocky_core::config::SchemaCacheConfig,
    ) -> HashMap<String, Vec<rocky_compiler::types::TypedColumn>> {
        if !schema_cache_config.enabled {
            return HashMap::new();
        }

        let resolved = rocky_core::state::resolve_state_path(None, &self.models_dir);
        if let Some(ref w) = resolved.warning {
            debug!(target: "rocky::state_path", "{w}");
        }
        let state_path = resolved.path;
        if !state_path.exists() {
            return HashMap::new();
        }

        // Mirror the LSP path at `lsp.rs:468` (PR #263): the redb open +
        // scan are sync work that can sleep up to ~250ms when contending
        // with a CLI process for the state-file flock (see
        // `StateStore::open_redb_with_retry`). Doing that on a Tokio
        // worker would intermittently starve HTTP handlers; move it to
        // the blocking pool.
        let ttl = schema_cache_config.ttl();
        // Take the process's store gate like every other open here. Without
        // it this read raced the HTTP reads and the scheduler's tick for
        // redb's file lock, and a loss returns an empty map below — a compile
        // that silently drops its cached warehouse types, at `debug!`, under
        // load. A closed gate (shutdown) is treated as no gate: the read then
        // contends exactly as it did before, which is the safe direction.
        let permit = Arc::clone(&self.store_access).acquire_owned().await.ok();
        let map = match tokio::task::spawn_blocking(move || {
            let _held = permit;
            let store = rocky_core::state::StateStore::open_read_only(&state_path)
                .map_err(|e| ("state open", e.to_string()))?;
            rocky_compiler::schema_cache::load_source_schemas_from_cache(
                &store,
                chrono::Utc::now(),
                ttl,
            )
            .map_err(|e| ("scan", e.to_string()))
        })
        .await
        {
            Ok(Ok(m)) => m,
            Ok(Err((stage, e))) => {
                debug!(error = %e, stage, "schema cache: {stage} failed in server path");
                return HashMap::new();
            }
            Err(join_err) => {
                debug!(error = %join_err, "schema cache: blocking task join failed");
                return HashMap::new();
            }
        };

        if !map.is_empty()
            && self
                .schema_cache_throttle
                .mark_logged(&self.models_dir.display().to_string())
                .await
        {
            info!(
                target: "rocky::schema_cache",
                sources_hit = map.len(),
                "schema cache: {} source(s) hit — run `rocky run` (write tap in PR 2) or \
                 `rocky discover --with-schemas` (PR 3) to warm-cache more sources",
                map.len(),
            );
        }

        map
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// Write a one-model project whose only column carries a `pii`
    /// classification tag. Returns the temp dir (kept alive by the caller)
    /// and the models dir + rocky.toml path.
    fn pii_project(rocky_toml: &str) -> (tempfile::TempDir, PathBuf, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let models_dir = dir.path().join("models");
        std::fs::create_dir(&models_dir).unwrap();

        let mut sql = std::fs::File::create(models_dir.join("users.sql")).unwrap();
        write!(sql, "SELECT 'a@b.com' AS email").unwrap();

        let mut toml = std::fs::File::create(models_dir.join("users.toml")).unwrap();
        write!(
            toml,
            "name = \"users\"\n\n[target]\ncatalog = \"demo\"\nschema = \"main\"\ntable = \"users\"\n\n[classification]\nemail = \"pii\"\n"
        )
        .unwrap();

        let config_path = dir.path().join("rocky.toml");
        let mut cfg = std::fs::File::create(&config_path).unwrap();
        write!(cfg, "{rocky_toml}").unwrap();

        (dir, models_dir, config_path)
    }

    fn w004_count(result: &CompileResult) -> usize {
        result
            .diagnostics
            .iter()
            .filter(|d| &*d.code == "W004")
            .count()
    }

    /// The server compile path must consult rocky.toml: an
    /// `allow_unmasked = ["pii"]` entry suppresses W004, which only
    /// happens if `[classifications]` was actually loaded (previously the
    /// server passed `..Default::default()` and the check was a no-op).
    #[tokio::test]
    async fn server_compile_honours_allow_unmasked_from_config() {
        // No mask, but pii is explicitly allowed unmasked → W004 suppressed.
        let (_dir, models_dir, config_path) =
            pii_project("[classifications]\nallow_unmasked = [\"pii\"]\n");
        let state = ServerState::new(models_dir, None, Some(config_path));
        state.recompile().await;
        let guard = state.compile_result.read().await;
        let result = guard.as_ref().expect("compile result");
        assert_eq!(
            w004_count(result),
            0,
            "allow_unmasked must suppress W004 in the server compile path"
        );
    }

    /// #1823. A compile that FAILS — here the `models` entry is a dangling
    /// symlink, which the walker refuses since #1817 — was logged and
    /// dropped: `compile_result` stayed `None`, and every reader of `None`
    /// took it for "nothing to report". The failure is recorded now, with
    /// its reason, and a compile that produces a result clears it.
    #[tokio::test]
    async fn a_failed_compile_is_recorded_and_a_later_success_clears_it() {
        let dir = tempfile::tempdir().unwrap();
        let models_dir = dir.path().join("models");
        std::os::unix::fs::symlink(dir.path().join("gone"), &models_dir).unwrap();
        let state = ServerState::new(models_dir.clone(), None, None);

        state.recompile().await;
        let failure = state
            .compile_failure
            .read()
            .await
            .clone()
            .expect("a compile that produced no result is recorded, not just logged");
        assert!(
            failure.contains("models"),
            "the reason names what could not be read: {failure}"
        );
        assert!(
            state.compile_result.read().await.is_none(),
            "precondition: there is no result to read"
        );

        // Repair the project: the link becomes a directory with one model.
        std::fs::remove_file(&models_dir).unwrap();
        std::fs::create_dir(&models_dir).unwrap();
        std::fs::write(models_dir.join("users.sql"), "SELECT 1 AS id").unwrap();
        std::fs::write(
            models_dir.join("users.toml"),
            "name = \"users\"\n\n[target]\ncatalog = \"demo\"\nschema = \"main\"\ntable = \"users\"\n",
        )
        .unwrap();
        state.recompile().await;
        assert!(
            state.compile_failure.read().await.is_none(),
            "a compile that produced a result clears the recorded failure"
        );
        assert!(state.compile_result.read().await.is_some());
    }

    /// The case #1625 is about, on the `serve` side.
    ///
    /// A `rocky.toml` that is present and unparseable used to be swallowed
    /// by `debug!` + defaults. The compile then ran with an empty `[mask]`,
    /// an empty `allow_unmasked` and no project `[freshness]`, and every
    /// reader — `POST /api/v1/compile`, the diagnostics counts, the UI —
    /// was told the same thing as a project that genuinely declares none of
    /// those. `serve` still compiles (a resident server must not go dark
    /// mid-edit), but it now says WHY the project-level checks are silent.
    #[tokio::test]
    async fn an_unreadable_config_is_reported_rather_than_treated_as_absent() {
        // Valid enough to exist, invalid as TOML.
        let (_dir, models_dir, config_path) = pii_project("[classifications\nnot = toml\n");
        let state = ServerState::new(models_dir, None, Some(config_path.clone()));

        let reason = state.recompile().await.expect(
            "a present-but-unparseable rocky.toml must be reported; returning None would \
             make POST /api/v1/compile answer plain success after degrading",
        );
        assert!(
            reason.contains(&config_path.display().to_string()),
            "the reason must name the file to fix, got: {reason}"
        );

        let guard = state.compile_result.read().await;
        let result = guard.as_ref().expect("compile result");

        // The contract: still usable. Serve does not refuse.
        assert!(
            result.project.model_count() > 0,
            "serve must keep compiling the models on a broken config"
        );

        // And the silence is visible to every reader of `compile_result`.
        let w013: Vec<&str> = result
            .diagnostics
            .iter()
            .filter(|d| &*d.code == "W013")
            .map(|d| &*d.message)
            .collect();
        assert_eq!(
            w013.len(),
            1,
            "exactly one W013 must reach the stored result; without it the compile \
             is indistinguishable from a project that declares nothing"
        );
    }

    /// The other half, and the reason W013 cannot simply fire whenever the
    /// project inputs are empty: a project with NO `rocky.toml` is an
    /// ordinary project. Squiggling it would make the warning noise.
    #[tokio::test]
    async fn a_project_with_no_config_is_not_reported_as_unreadable() {
        let dir = tempfile::tempdir().unwrap();
        let models_dir = dir.path().join("models");
        std::fs::create_dir(&models_dir).unwrap();
        let mut sql = std::fs::File::create(models_dir.join("users.sql")).unwrap();
        write!(sql, "SELECT 1 AS id").unwrap();
        let mut sidecar = std::fs::File::create(models_dir.join("users.toml")).unwrap();
        write!(
            sidecar,
            "name = \"users\"\n\n[target]\ncatalog = \"demo\"\nschema = \"main\"\ntable = \"users\"\n"
        )
        .unwrap();

        // No `rocky.toml` written at this path.
        let state = ServerState::new(models_dir, None, Some(dir.path().join("rocky.toml")));
        assert!(
            state.recompile().await.is_none(),
            "an ABSENT rocky.toml is a fact about the project, not a failure to read one"
        );

        let guard = state.compile_result.read().await;
        let result = guard.as_ref().expect("compile result");
        assert!(
            !result.diagnostics.iter().any(|d| &*d.code == "W013"),
            "W013 must not fire on a project that simply has no config"
        );
    }

    /// Conversely, with a config that does not mask or allow the `pii`
    /// tag, W004 fires — confirming the check is live (not just always
    /// silent) once the config is wired.
    #[tokio::test]
    async fn server_compile_fires_w004_when_tag_unmasked() {
        let (_dir, models_dir, config_path) = pii_project("# empty config\n");
        let state = ServerState::new(models_dir, None, Some(config_path));
        state.recompile().await;
        let guard = state.compile_result.read().await;
        let result = guard.as_ref().expect("compile result");
        assert_eq!(
            w004_count(result),
            1,
            "an unmasked, unallowed classification tag must raise W004"
        );
    }
}
