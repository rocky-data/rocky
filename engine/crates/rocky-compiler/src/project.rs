//! Project loading and DAG construction.
//!
//! A `Project` represents a collection of models with resolved dependencies
//! and a computed execution order. It loads models from a directory, resolves
//! inter-model dependencies from SQL, and builds the execution DAG.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use rocky_core::models::{self, Model, ModelConfig, TargetConfig};
use rocky_core::unified_dag::UnifiedDag;
use rocky_ir::dag::{self, DagNode};
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

    // Transparent: the leaf `DagError` already carries a fully
    // self-describing message (incl. the `did you mean?` hint). Re-
    // prefixing it here only produced a duplicated `Caused by` layer in
    // the rendered anyhow chain.
    #[error(transparent)]
    Dag(#[from] dag::DagError),

    #[error("no models found in {path}")]
    NoModels { path: String },

    #[error(
        "duplicate model name '{name}': two or more models share this name. \
         Model names must be unique within a project — rename one."
    )]
    DuplicateModel { name: String },

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
    ///
    /// This is the legacy entry point — it constructs a transient salsa
    /// database internally for the `.rocky` parse pipeline, so repeated
    /// calls re-parse from disk. Use [`Project::load_with_db`] when the
    /// caller already owns a `RockyDatabase` and wants the per-file
    /// parse cache to persist across compiles (LSP, watch-mode, tests).
    pub fn load(models_dir: &Path) -> Result<Self, ProjectError> {
        let mut db = crate::salsa_compile::RockyDatabase::default();
        Self::load_with_db(models_dir, &mut db)
    }

    /// Load a project from a models directory, reusing the caller's
    /// salsa database for the `.rocky` parse + lower pipeline.
    ///
    /// `.rocky` files are loaded through
    /// [`crate::salsa_compile::read_source`] +
    /// [`crate::salsa_compile::file_typecheck`], so a second call against
    /// an unchanged file returns the cached lowered SQL without
    /// re-invoking the parser or lowerer. `.sql` files still load
    /// through the existing non-tracked path —
    /// `models::load_models_from_dir` reads them directly from disk —
    /// because they have no entry on the salsa side yet.
    pub fn load_with_db(
        models_dir: &Path,
        db: &mut crate::salsa_compile::RockyDatabase,
    ) -> Result<Self, ProjectError> {
        let mut models = models::load_models_from_dir(models_dir)?;

        // Also load .rocky files via the salsa pipeline
        let rocky_models = load_rocky_models_with_db(models_dir, db)?;
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
        // Reject duplicate model names up front. `model()` is first-wins and
        // `resolve` collapses names into a HashSet, so a second model with the
        // same name would silently shadow the first — a real source of "my edit
        // had no effect" confusion. Fail loudly instead.
        let mut seen = std::collections::HashSet::with_capacity(models.len());
        for m in &models {
            if !seen.insert(m.config.name.as_str()) {
                return Err(ProjectError::DuplicateModel {
                    name: m.config.name.clone(),
                });
            }
        }

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

/// Load every model in a single directory — both `.sql` (with sidecar
/// `.toml`) and `.rocky` DSL files — without resolving the DAG.
///
/// This is the loader CLI commands should use when they need the model list
/// but not the dependency graph (`list`, `dag`, `validate`'s count, `docs`,
/// `optimize`, `compliance`, `preview`, `estimate`, branch scoping, …).
/// [`rocky_core::models::load_models_from_dir`] alone collects only `.sql`
/// files, so any command on that path silently drops `.rocky` DSL models —
/// this includes them. Unlike [`Project::load`] it never fails on dependency
/// resolution (it does none), so a partial or broken DAG still yields the
/// models that parsed.
pub fn load_dir_models(dir: &Path) -> Result<Vec<Model>, ProjectError> {
    let mut models = models::load_models_from_dir(dir)?;
    let mut db = crate::salsa_compile::RockyDatabase::default();
    models.extend(load_rocky_models_with_db(dir, &mut db)?);
    Ok(models)
}

/// Load `.rocky` files from a directory through the salsa database.
///
/// Sequential rather than rayon-parallel: salsa's `RockyDatabase` is
/// `&mut`-bounded for input loading (`read_source` mutates the dedup
/// map), so per-file parses run on the calling thread. The expected
/// payoff is the *second* compile, which hits the salsa cache for
/// every unchanged file and serves the lowered SQL without touching
/// the parser at all.
fn load_rocky_models_with_db(
    dir: &Path,
    db: &mut crate::salsa_compile::RockyDatabase,
) -> Result<Vec<Model>, ProjectError> {
    if !dir.exists() {
        return Ok(Vec::new());
    }

    let defaults_path = dir.join("_defaults.toml");
    let defaults = if defaults_path.exists() {
        Some(models::load_dir_defaults(&defaults_path)?)
    } else {
        None
    };
    // Config groups apply to `.rocky` DSL models too — they resolve their
    // sidecar config through the same `load_model_pair` path, so load the
    // groups here and thread them in (the `.sql` loader does this in
    // `load_models_from_dir`).
    let groups = models::load_groups_from_dir(dir)?;
    let groups = if groups.is_empty() {
        None
    } else {
        Some(&groups)
    };

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

    let mut models: Vec<Model> = Vec::with_capacity(rocky_paths.len());
    for path in &rocky_paths {
        models.push(load_single_rocky_model_with_db(
            path,
            defaults.as_ref(),
            groups,
            db,
        )?);
    }

    models.sort_by(|a, b| a.config.name.cmp(&b.config.name));
    Ok(models)
}

/// Salsa-driven loader for a single `.rocky` file. Uses
/// [`crate::salsa_compile::read_source`] +
/// [`crate::salsa_compile::file_typecheck`] to get the lowered SQL
/// through the database cache; the surrounding sidecar / defaults
/// logic mirrors the `.sql` model loader.
fn load_single_rocky_model_with_db(
    path: &Path,
    defaults: Option<&models::DirDefaults>,
    groups: Option<&std::collections::HashMap<String, models::GroupConfig>>,
    db: &mut crate::salsa_compile::RockyDatabase,
) -> Result<Model, ProjectError> {
    let name = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("unknown")
        .to_string();

    // Tracked-query route: read_source loads the file via the salsa
    // dedup map; file_typecheck parses + lowers (or returns the cached
    // result if neither input nor AST has changed).
    let src = crate::salsa_compile::read_source(db, path.to_path_buf()).map_err(|e| {
        ProjectError::RockyParse {
            path: path.display().to_string(),
            reason: e.to_string(),
        }
    })?;
    let ft = crate::salsa_compile::file_typecheck(db, src);

    // Diagnostics are accumulated by `file_typecheck` via the
    // [`CompileDiagnostic`] salsa accumulator — on a cache hit they're
    // returned straight from the cache without re-running the body, on
    // a token-equivalent re-parse they backdate, and on a real AST
    // change they re-emit fresh.
    let diagnostics = crate::salsa_compile::file_typecheck::accumulated::<
        crate::salsa_compile::CompileDiagnostic,
    >(db, src);
    if let Some(first) = diagnostics.first() {
        // Treat the leading parse / lower diagnostic as the
        // ProjectError. Distinguishing parse vs lower errors by
        // string-prefix matches the historical structure.
        let msg = &first.0;
        if msg.starts_with("lower error: ") {
            return Err(ProjectError::RockyLower {
                path: path.display().to_string(),
                reason: msg.trim_start_matches("lower error: ").to_string(),
            });
        }
        return Err(ProjectError::RockyParse {
            path: path.display().to_string(),
            reason: msg.trim_start_matches("parse error: ").to_string(),
        });
    }

    let sql = ft.sql.clone();

    // Sidecar config + contract resolution — unchanged from the
    // non-salsa path.
    let toml_path = path.with_extension("toml");
    let config = if toml_path.exists() {
        models::load_model_pair_with_groups(path, &toml_path, defaults, groups)?.config
    } else {
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
            skip: None,
            name_declared: String::new(),
            target_table_declared: String::new(),
        }
    };

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
                skip: None,
                name_declared: String::new(),
                target_table_declared: String::new(),
            },
            sql: sql.to_string(),
            file_path: format!("models/{name}.sql"),
            contract_path: None,
        }
    }

    #[test]
    fn duplicate_model_name_is_rejected() {
        let models = vec![
            make_model("orders", "SELECT 1 AS id"),
            make_model("orders", "SELECT 2 AS id"),
        ];
        let err =
            Project::from_models(models).expect_err("two models sharing a name must be rejected");
        match err {
            ProjectError::DuplicateModel { name } => assert_eq!(name, "orders"),
            other => panic!("expected DuplicateModel, got {other:?}"),
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

    /// `load_dir_models` must pick up BOTH `.sql` (sidecar) and `.rocky` DSL
    /// files — the gap that made `load_models_from_dir` (sql-only) silently
    /// drop DSL models from `validate` / `list` / `dag` / etc.
    #[test]
    fn load_dir_models_includes_sql_and_rocky() {
        // Loading a `.rocky` file runs the salsa-tracked `file_typecheck`,
        // which bumps the process-global invocation counter the salsa tests
        // assert on — hold their lock so we don't race those assertions.
        let _guard = crate::salsa_compile::tests::TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path();
        std::fs::write(d.join("stg.sql"), "SELECT 1 AS id FROM raw__x.y").unwrap();
        std::fs::write(
            d.join("stg.toml"),
            "[strategy]\ntype = \"full_refresh\"\n[target]\ncatalog = \"c\"\nschema = \"s\"\n",
        )
        .unwrap();
        std::fs::write(
            d.join("agg.rocky"),
            "from stg\ngroup id {\n    n: count()\n}\n",
        )
        .unwrap();
        std::fs::write(
            d.join("agg.toml"),
            "[strategy]\ntype = \"full_refresh\"\n[target]\ncatalog = \"c\"\nschema = \"s\"\n",
        )
        .unwrap();

        let models = load_dir_models(d).unwrap();
        let names: Vec<_> = models.iter().map(|m| m.config.name.as_str()).collect();
        assert!(names.contains(&"stg"), "sql model loaded: {names:?}");
        assert!(names.contains(&"agg"), "rocky DSL model loaded: {names:?}");
    }

    /// Regression: a `.rocky` DSL model resolves a config group through the
    /// salsa loader, just like a `.sql` model does via `load_models_from_dir`.
    /// Guards the wiring gap where the `.rocky` path used the bare
    /// `load_model_pair` and silently ignored groups.
    #[test]
    fn rocky_model_resolves_config_group() {
        let _guard = crate::salsa_compile::tests::TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path();
        std::fs::create_dir_all(d.join("groups")).unwrap();
        std::fs::write(
            d.join("groups/daily_marts.toml"),
            "schema_template = \"mart_{region}\"\n\n[strategy]\ntype = \"full_refresh\"\n",
        )
        .unwrap();
        std::fs::write(d.join("flow.rocky"), "from orders\nselect { id }\n").unwrap();
        std::fs::write(
            d.join("flow.toml"),
            "group = \"daily_marts\"\n\n[target]\ncatalog = \"wh\"\n\n[args]\nregion = \"emea\"\n",
        )
        .unwrap();

        let models = load_dir_models(d).unwrap();
        let flow = models
            .iter()
            .find(|m| m.config.name == "flow")
            .expect("rocky model loaded");
        assert_eq!(
            flow.config.target.schema, "mart_emea",
            "the group's schema_template must route the .rocky model"
        );
    }
}
