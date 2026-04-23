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
    /// Arc 7 wave 2 wave-2: per-session throttle for the "N sources hit"
    /// info log so it emits once per server start, not once per recompile.
    /// PR 2 will key it on `(document_uri, cache_version)` — the only LSP
    /// analogue today is `models_dir`, which stays constant.
    schema_cache_throttle: SchemaCacheThrottle,
}

impl ServerState {
    /// Create new server state and perform initial compilation.
    pub fn new(
        models_dir: PathBuf,
        contracts_dir: Option<PathBuf>,
        config_path: Option<PathBuf>,
    ) -> Arc<Self> {
        let state = Arc::new(Self {
            models_dir,
            contracts_dir,
            config_path,
            compile_result: RwLock::new(None),
            dag_status: DagStatusStore::new(),
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

        // Wave-2 of Arc 7 wave 2: load cached source schemas so the
        // server's hover/inlay-hint surfaces typecheck against real
        // warehouse types when the cache is warm. Degrades to empty on
        // cold cache, missing state.redb, or `[cache.schemas] enabled =
        // false`. See `rocky-cli::source_schemas` for the CLI equivalent.
        let source_schemas = self.load_cached_source_schemas().await;

        let config = CompilerConfig {
            models_dir: self.models_dir.clone(),
            contracts_dir: self.contracts_dir.clone(),
            source_schemas,
            source_column_info: HashMap::new(),
        };

        match rocky_compiler::compile::compile(&config) {
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

        let store = match rocky_core::state::StateStore::open_read_only(&state_path) {
            Ok(s) => s,
            Err(e) => {
                debug!(error = %e, "schema cache: state open failed in server path");
                return HashMap::new();
            }
        };

        let map = match rocky_compiler::schema_cache::load_source_schemas_from_cache(
            &store,
            chrono::Utc::now(),
            schema_cache_config.ttl(),
        ) {
            Ok(m) => m,
            Err(e) => {
                debug!(error = %e, "schema cache: scan failed in server path");
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
