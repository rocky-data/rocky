//! Project loading and DAG construction.
//!
//! A `Project` represents a collection of models with resolved dependencies
//! and a computed execution order. It loads models from a directory, resolves
//! inter-model dependencies from SQL, and builds the execution DAG.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use rayon::prelude::*;
use rocky_core::dag::{self, DagNode};
use rocky_core::models::{self, Model, ModelConfig, TargetConfig};
use rocky_core::unified_dag::UnifiedDag;
use rocky_sql::lineage;
use thiserror::Error;
use tracing::info;

use crate::resolve;

/// A loaded and resolved project ready for compilation.
#[derive(Debug)]
pub struct Project {
    /// All models in the project.
    pub models: Vec<Model>,
    /// DAG nodes with resolved dependencies.
    pub dag_nodes: Vec<DagNode>,
    /// Models in topological execution order.
    pub execution_order: Vec<String>,
    /// Parallel execution layers (each layer can run concurrently).
    pub layers: Vec<Vec<String>>,
    /// Cached lineage results from dependency resolution, keyed by model name.
    /// Populated during `resolve_dependencies` and consumed by `build_semantic_graph`.
    pub lineage_cache: HashMap<String, lineage::LineageResult>,
    /// Diagnostics from dependency resolution (e.g., D011 mismatch warnings).
    pub resolve_diagnostics: Vec<crate::diagnostic::Diagnostic>,
    /// Optional unified DAG spanning all pipeline types, seeds, and tests.
    ///
    /// This is populated by the CLI when full project context is available
    /// (config + models + seeds). `None` when only model-level compilation
    /// is performed.
    pub unified_dag: Option<UnifiedDag>,
}

/// Errors during project loading.
#[derive(Debug, Error)]
pub enum ProjectError {
    #[error("failed to load models: {0}")]
    ModelLoad(#[from] models::ModelError),

    #[error("dependency resolution failed: {0}")]
    Resolve(#[from] resolve::ResolveError),

    #[error("DAG error: {0}")]
    Dag(#[from] dag::DagError),

    #[error("no models found in {path}")]
    NoModels { path: String },

    #[error("failed to parse .rocky file '{path}': {reason}")]
    RockyParse { path: String, reason: String },

    #[error("failed to lower .rocky file '{path}': {reason}")]
    RockyLower { path: String, reason: String },
}

impl Project {
    /// Load a project from a models directory.
    ///
    /// Supports both `.sql` (with sidecar `.toml`) and `.rocky` files.
    /// Both formats produce `Model` structs and are unified from this point.
    pub fn load(models_dir: &Path) -> Result<Self, ProjectError> {
        let mut models = models::load_models_from_dir(models_dir)?;

        // Also load .rocky files
        let rocky_models = load_rocky_models(models_dir)?;
        if !rocky_models.is_empty() {
            info!(count = rocky_models.len(), "loaded .rocky models");
            models.extend(rocky_models);
        }

        if models.is_empty() {
            return Err(ProjectError::NoModels {
                path: models_dir.display().to_string(),
            });
        }

        Self::from_models(models)
    }

    /// Build a project from pre-loaded models.
    ///
    /// Useful when models come from sources other than a directory
    /// (e.g., DSL lowering, dbt import).
    pub fn from_models(models: Vec<Model>) -> Result<Self, ProjectError> {
        let (dag_nodes, lineage_cache, resolve_diagnostics) =
            resolve::resolve_dependencies(&models)?;
        let execution_order = dag::topological_sort(&dag_nodes)?;
        let layers = dag::execution_layers(&dag_nodes)?;

        Ok(Project {
            models,
            dag_nodes,
            execution_order,
            layers,
            lineage_cache,
            resolve_diagnostics,
            unified_dag: None,
        })
    }

    /// Get a model by name.
    pub fn model(&self, name: &str) -> Option<&Model> {
        self.models.iter().find(|m| m.config.name == name)
    }

    /// Number of models in the project.
    pub fn model_count(&self) -> usize {
        self.models.len()
    }

    /// Attach a unified DAG built from the full project context.
    ///
    /// Called by the CLI after loading config, models, and seeds. The
    /// unified DAG is a read-only view used for display (`rocky plan`)
    /// and lineage queries.
    pub fn set_unified_dag(&mut self, dag: UnifiedDag) {
        self.unified_dag = Some(dag);
    }
}

/// Load `.rocky` files from a directory and lower them to `Model` structs.
///
/// Respects `_defaults.toml` and sidecar inference, same as
/// [`models::load_models_from_dir`] does for `.sql` files.
///
/// File discovery is sequential (single `read_dir` scan), but parsing and
/// lowering are parallelized with rayon — each `.rocky` file is parsed,
/// lowered to SQL, and resolved independently.
fn load_rocky_models(dir: &Path) -> Result<Vec<Model>, ProjectError> {
    if !dir.exists() {
        return Ok(Vec::new());
    }

    // Load optional directory defaults (shared with .sql model loader)
    let defaults_path = dir.join("_defaults.toml");
    let defaults = if defaults_path.exists() {
        Some(models::load_dir_defaults(&defaults_path)?)
    } else {
        None
    };

    // 1. Collect .rocky file paths (lightweight sequential scan)
    let rocky_paths: Vec<PathBuf> = std::fs::read_dir(dir)
        .map_err(models::ModelError::ReadFile)?
        .filter_map(|entry| {
            let path = entry.ok()?.path();
            if path.extension().is_some_and(|ext| ext == "rocky") {
                Some(path)
            } else {
                None
            }
        })
        .collect();

    if rocky_paths.is_empty() {
        return Ok(Vec::new());
    }

    // 2. Parse + lower in parallel (CPU-bound per file)
    let mut models: Vec<Model> = rocky_paths
        .par_iter()
        .map(|path| load_single_rocky_model(path, defaults.as_ref()))
        .collect::<Result<Vec<_>, _>>()?;

    // Sort by name for deterministic ordering
    models.sort_by(|a, b| a.config.name.cmp(&b.config.name));
    Ok(models)
}

/// Parse, lower, and configure a single `.rocky` file into a `Model`.
fn load_single_rocky_model(
    path: &Path,
    defaults: Option<&models::DirDefaults>,
) -> Result<Model, ProjectError> {
    let name = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("unknown")
        .to_string();

    let content = std::fs::read_to_string(path).map_err(models::ModelError::ReadFile)?;

    // Parse the .rocky file
    let rocky_file = rocky_lang::parse(&content).map_err(|e| ProjectError::RockyParse {
        path: path.display().to_string(),
        reason: e.to_string(),
    })?;

    // Lower to SQL
    let sql =
        rocky_lang::lower::lower_to_sql(&rocky_file).map_err(|e| ProjectError::RockyLower {
            path: path.display().to_string(),
            reason: e,
        })?;

    // Check for sidecar .toml config
    let toml_path = path.with_extension("toml");
    let config = if toml_path.exists() {
        // Use the sidecar loader which handles inference + defaults
        models::load_model_pair(path, &toml_path, defaults)?.config
    } else {
        // No sidecar — use defaults or hardcoded fallback
        let catalog = defaults
            .as_ref()
            .and_then(|d| d.target.as_ref())
            .and_then(|t| t.catalog.clone())
            .unwrap_or_else(|| "warehouse".to_string());
        let schema = defaults
            .as_ref()
            .and_then(|d| d.target.as_ref())
            .and_then(|t| t.schema.clone())
            .unwrap_or_else(|| "default".to_string());
        let strategy = defaults
            .as_ref()
            .and_then(|d| d.strategy.clone())
            .unwrap_or_default();

        ModelConfig {
            name: name.clone(),
            depends_on: vec![],
            strategy,
            target: TargetConfig {
                catalog,
                schema,
                table: name.clone(),
            },
            sources: vec![],
            adapter: None,
            intent: defaults.as_ref().and_then(|d| d.intent.clone()),
            freshness: defaults.as_ref().and_then(|d| d.freshness.clone()),
            tests: vec![],
            format: None,
            format_options: None,
            classification: Default::default(),
            retention: None,
            budget: None,
        }
    };

    // Check for sibling contract file
    let contract_file = path.with_extension("contract.toml");
    let contract_path = if contract_file.exists() {
        Some(contract_file)
    } else {
        None
    };

    Ok(Model {
        config,
        sql,
        file_path: path.display().to_string(),
        contract_path,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocky_core::models::{ModelConfig, StrategyConfig, TargetConfig};

    fn make_model(name: &str, sql: &str) -> Model {
        Model {
            config: ModelConfig {
                name: name.to_string(),
                depends_on: vec![],
                strategy: StrategyConfig::default(),
                target: TargetConfig {
                    catalog: "warehouse".to_string(),
                    schema: "silver".to_string(),
                    table: name.to_string(),
                },
                sources: vec![],
                adapter: None,
                intent: None,
                freshness: None,
                tests: vec![],
                format: None,
                format_options: None,
                classification: Default::default(),
                retention: None,
                budget: None,
            },
            sql: sql.to_string(),
            file_path: format!("models/{name}.sql"),
            contract_path: None,
        }
    }

    #[test]
    fn test_project_from_models_linear() {
        let models = vec![
            make_model("c", "SELECT * FROM b"),
            make_model("b", "SELECT * FROM a"),
            make_model("a", "SELECT 1 AS id"),
        ];

        let project = Project::from_models(models).unwrap();

        // a must come before b, b before c
        let a_pos = project
            .execution_order
            .iter()
            .position(|n| n == "a")
            .unwrap();
        let b_pos = project
            .execution_order
            .iter()
            .position(|n| n == "b")
            .unwrap();
        let c_pos = project
            .execution_order
            .iter()
            .position(|n| n == "c")
            .unwrap();
        assert!(a_pos < b_pos);
        assert!(b_pos < c_pos);
    }

    #[test]
    fn test_project_from_models_diamond() {
        let models = vec![
            make_model("d", "SELECT * FROM b JOIN c ON b.id = c.id"),
            make_model("b", "SELECT * FROM a"),
            make_model("c", "SELECT * FROM a"),
            make_model("a", "SELECT 1 AS id"),
        ];

        let project = Project::from_models(models).unwrap();

        assert_eq!(project.model_count(), 4);

        // a before b and c, b and c before d
        let a_pos = project
            .execution_order
            .iter()
            .position(|n| n == "a")
            .unwrap();
        let b_pos = project
            .execution_order
            .iter()
            .position(|n| n == "b")
            .unwrap();
        let c_pos = project
            .execution_order
            .iter()
            .position(|n| n == "c")
            .unwrap();
        let d_pos = project
            .execution_order
            .iter()
            .position(|n| n == "d")
            .unwrap();
        assert!(a_pos < b_pos);
        assert!(a_pos < c_pos);
        assert!(b_pos < d_pos);
        assert!(c_pos < d_pos);

        // Layers: [a], [b, c], [d]
        assert_eq!(project.layers.len(), 3);
        assert_eq!(project.layers[0], vec!["a"]);
        assert!(project.layers[1].contains(&"b".to_string()));
        assert!(project.layers[1].contains(&"c".to_string()));
        assert_eq!(project.layers[2], vec!["d"]);
    }

    #[test]
    fn test_project_model_lookup() {
        let models = vec![
            make_model("orders", "SELECT 1 AS id"),
            make_model("customers", "SELECT 1 AS id"),
        ];

        let project = Project::from_models(models).unwrap();
        assert!(project.model("orders").is_some());
        assert!(project.model("customers").is_some());
        assert!(project.model("nonexistent").is_none());
    }

    #[test]
    fn test_project_cycle_detected() {
        let models = vec![
            make_model("a", "SELECT * FROM b"),
            make_model("b", "SELECT * FROM a"),
        ];

        let result = Project::from_models(models);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("circular"));
    }
}
