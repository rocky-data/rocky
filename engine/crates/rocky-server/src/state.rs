//! Shared compiler state for the server and LSP.
//!
//! Holds the latest `CompileResult` behind a `RwLock`, recompiled
//! on file changes when watch mode is active.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::RwLock;
use tracing::{info, warn};

use rocky_compiler::compile::{CompileResult, CompilerConfig};
use rocky_core::dag_status::DagStatusStore;

/// Shared server state holding the latest compilation result.
pub struct ServerState {
    pub models_dir: PathBuf,
    pub contracts_dir: Option<PathBuf>,
    pub config_path: Option<PathBuf>,
    pub compile_result: RwLock<Option<CompileResult>>,
    /// Latest DAG execution status, exposed at `GET /api/v1/dag/status`.
    pub dag_status: DagStatusStore,
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

        let config = CompilerConfig {
            models_dir: self.models_dir.clone(),
            contracts_dir: self.contracts_dir.clone(),
            source_schemas: HashMap::new(),
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
}
