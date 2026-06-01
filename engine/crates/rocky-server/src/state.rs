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

use crate::schema_cache_throttle::SchemaCacheThrottle;

/// Shared server state holding the latest compilation result.
pub struct ServerState {
    pub models_dir: PathBuf,
    pub contracts_dir: Option<PathBuf>,
    pub config_path: Option<PathBuf>,
    pub compile_result: RwLock<Option<CompileResult>>,
    /// Latest DAG execution status, exposed at `GET /api/v1/dag/status`.
    pub dag_status: DagStatusStore,
    /// Bearer token required by the HTTP API auth middleware. `None`
    /// means "no auth"; in that mode [`crate::api::serve`] refuses to
    /// bind a non-loopback host. See [`crate::auth::require_bearer_token`].
    pub auth_token: Option<String>,
    /// CORS allowlist passed to [`crate::auth::build_cors_layer`]. An
    /// empty list means same-origin only.
    pub allowed_origins: Vec<String>,
    /// Per-session throttle for the "N sources hit" info log so it
    /// emits once per server start, not once per recompile. Keyed on
    /// `models_dir`, which stays constant.
    schema_cache_throttle: SchemaCacheThrottle,
}

impl ServerState {
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
        Self::with_auth(models_dir, contracts_dir, config_path, None, Vec::new())
    }

    /// Create new server state with explicit auth + CORS configuration.
    pub fn with_auth(
        models_dir: PathBuf,
        contracts_dir: Option<PathBuf>,
        config_path: Option<PathBuf>,
        auth_token: Option<String>,
        allowed_origins: Vec<String>,
    ) -> Arc<Self> {
        let state = Arc::new(Self {
            models_dir,
            contracts_dir,
            config_path,
            compile_result: RwLock::new(None),
            dag_status: DagStatusStore::new(),
            auth_token,
            allowed_origins,
            schema_cache_throttle: SchemaCacheThrottle::new(),
        });

        // Initial compile
        let rt_state = state.clone();
        tokio::spawn(async move {
            rt_state.recompile().await;
        });

        state
    }

    /// Recompile the project and update the stored result.
    pub async fn recompile(&self) {
        info!(models_dir = %self.models_dir.display(), "compiling project");

        // Load cached source schemas so the server's hover/inlay-hint
        // surfaces typecheck against real warehouse types when the cache
        // is warm. Degrades to empty on cold cache, missing state.redb,
        // or `[cache.schemas] enabled = false`. See
        // `rocky-cli::source_schemas` for the CLI equivalent.
        let source_schemas = self.load_cached_source_schemas().await;

        // Load `[mask]` + `[classifications.allow_unmasked]` (W004) and the
        // `[freshness]` default bit (W005) from rocky.toml, mirroring the
        // CLI compile path. Without a config_path (or on a parse error) both
        // come through empty and the checks stay silent — matching standalone
        // `rocky compile --models models/`.
        let (mask, allow_unmasked, project_freshness_default) = match &self.config_path {
            Some(path) => match rocky_core::config::load_rocky_config(path) {
                Ok(cfg) => (
                    cfg.mask.clone(),
                    cfg.classifications.allow_unmasked.clone(),
                    cfg.freshness.has_default(),
                ),
                Err(e) => {
                    debug!(error = %e, "rocky.toml load failed; W004/W005 will be silent");
                    (Default::default(), Vec::new(), false)
                }
            },
            None => (Default::default(), Vec::new(), false),
        };

        let config = CompilerConfig {
            models_dir: self.models_dir.clone(),
            contracts_dir: self.contracts_dir.clone(),
            source_schemas,
            source_column_info: HashMap::new(),
            mask,
            allow_unmasked,
            project_freshness_default,
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
                    return;
                }
            };

        match compile_result {
            Ok(result) => {
                let model_count = result.project.model_count();
                let diag_count = result.diagnostics.len();
                let has_errors = result.has_errors;
                *self.compile_result.write().await = Some(result);
                info!(
                    models = model_count,
                    diagnostics = diag_count,
                    has_errors,
                    "compilation complete"
                );
            }
            Err(e) => {
                warn!(error = %e, "compilation failed");
            }
        }
    }

    /// Load the schema-cache-backed `source_schemas` map for this
    /// server's project. Gated on `[cache.schemas] enabled`; resolves the
    /// state file via [`rocky_core::state::resolve_state_path`] so the
    /// server observes exactly the same file that `rocky run` writes to
    /// (unified default — `<models>/.rocky-state.redb` — with the legacy
    /// CWD fallback for existing projects).
    async fn load_cached_source_schemas(
        &self,
    ) -> HashMap<String, Vec<rocky_compiler::types::TypedColumn>> {
        // Config lookup: fall back to defaults (enabled + 24h TTL) when
        // no rocky.toml is wired in, so LSP/server behaviour mirrors the
        // CLI for zero-config projects.
        let schema_cache_config = match &self.config_path {
            Some(path) => rocky_core::config::load_rocky_config(path)
                .map(|c| c.cache.schemas)
                .unwrap_or_default(),
            None => rocky_core::config::SchemaCacheConfig::default(),
        };

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
        let map = match tokio::task::spawn_blocking(move || {
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
