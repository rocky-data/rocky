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
    /// Groups of models that resolve to the SAME physical target (#1291).
    ///
    /// Deliberately not a [`ProjectError`]. A hard construction failure is
    /// swallowed into silence by every tolerant caller — `ci-diff` logs at
    /// `debug!` and falls back to filename-stem classification, and the LSP's
    /// `Err` arm publishes no diagnostic at all — and it would make any
    /// historical ref containing a collision unloadable, foreclosing the
    /// base-∪-head target-occupancy fix #1236 wants. So construction records
    /// the collision, `compile` renders it as an E036 error diagnostic, and
    /// the *execution* boundary refuses, which is where two concurrent
    /// writers to one table actually do damage.
    pub target_collisions: Vec<TargetCollision>,
    /// Optional unified DAG spanning all pipeline types, seeds, and tests.
    ///
    /// This is populated by the CLI when full project context is available
    /// (config + models + seeds). `None` when only model-level compilation
    /// is performed.
    pub unified_dag: Option<UnifiedDag>,
}

/// Two or more models resolving to one physical `catalog.schema.table`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetCollision {
    /// The shared target as the user spelled it, for the message.
    pub target: String,
    /// Colliding model names, sorted — so the message is deterministic.
    pub models: Vec<String>,
}

/// Group models by the physical object their target names.
///
/// Identity is [`rocky_sql::defer::CollisionIdentity`], which case-folds with
/// Unicode `to_lowercase` and deliberately takes no dialect rules: answering
/// "different" for two spellings that are one object would let both models
/// write the same table with no error at all, which is the failure this is
/// here to prevent.
///
/// Note the grouping order below follows the folded key, so a change to the
/// fold reorders these diagnostics even where it merges nothing.
///
/// **Scope.** The key is `catalog.schema.table` only — not adapter routing
/// identity, which #1291 also asks for. `from_models` receives `Vec<Model>`
/// and no `RockyConfig`, so it cannot resolve an adapter name to a physical
/// destination. That is sound *today* only because the per-model adapter
/// override is coded but not wired: `rocky_core::cross_engine::effective_model_adapter`
/// has no production call sites, and execution resolves exactly one warehouse
/// adapter per *pipeline*. The day a model-level adapter is honored, this key
/// becomes unsound and must grow the adapter's routing identity. The
/// cross-*pipeline* half belongs next to `reject_models_claimed_twice` in
/// `unified_dag`, which does have the config.
///
/// `Ephemeral` models are excluded: they carry a fully populated but phantom
/// target and are never materialized, so two of them — or one beside a real
/// model — are not two writers to one table.
fn collide_on_target(models: &[Model]) -> Vec<TargetCollision> {
    let mut by_identity: std::collections::BTreeMap<String, (String, Vec<String>)> =
        std::collections::BTreeMap::new();

    for m in models {
        if matches!(
            m.config.strategy,
            rocky_core::models::StrategyConfig::Ephemeral
        ) {
            continue;
        }
        let t = &m.config.target;
        let identity =
            rocky_sql::defer::CollisionIdentity::of(&t.catalog, &t.schema, &t.table).to_string();
        let spelled = if t.catalog.is_empty() {
            format!("{}.{}", t.schema, t.table)
        } else {
            format!("{}.{}.{}", t.catalog, t.schema, t.table)
        };
        by_identity
            .entry(identity)
            .or_insert_with(|| (spelled, Vec::new()))
            .1
            .push(m.config.name.clone());
    }

    by_identity
        .into_values()
        .filter(|(_, names)| names.len() > 1)
        .map(|(target, mut models)| {
            models.sort();
            TargetCollision { target, models }
        })
        .collect()
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

    #[error("invalid models glob '{pattern}': {reason}")]
    InvalidModelsGlob { pattern: String, reason: String },

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
    pub fn load(
        models_dir: &Path,
        project_freshness: Option<&rocky_core::config::ProjectFreshnessConfig>,
    ) -> Result<Self, ProjectError> {
        let mut db = crate::salsa_compile::RockyDatabase::default();
        Self::load_with_db(models_dir, &mut db, project_freshness)
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
        project_freshness: Option<&rocky_core::config::ProjectFreshnessConfig>,
    ) -> Result<Self, ProjectError> {
        Self::from_models(Self::load_models_with_db(
            models_dir,
            db,
            project_freshness,
        )?)
    }

    /// Load every model from a directory **without** resolving dependencies.
    ///
    /// Returns the raw `Model` set (`.sql` sidecars + `.rocky` files) before
    /// `resolve_dependencies` / lineage extraction has run. Callers that need
    /// to mutate model SQL ahead of resolution — e.g. substituting per-run
    /// `@var()` variables so a bare marker never reaches the SQL parser — load
    /// here, transform, then call [`Project::from_models`]. Most callers want
    /// [`Project::load_with_db`], which chains the two.
    pub fn load_models_with_db(
        models_dir: &Path,
        db: &mut crate::salsa_compile::RockyDatabase,
        project_freshness: Option<&rocky_core::config::ProjectFreshnessConfig>,
    ) -> Result<Vec<Model>, ProjectError> {
        Self::load_models_with_db_filtered(models_dir, db, &|_| true, project_freshness)
    }

    /// Load only model source files matching `models_glob`, without resolving
    /// dependencies.
    ///
    /// The glob must use the same path form as `models_dir` (both absolute or
    /// both relative). Filtering precedes parsing, so a non-matching malformed
    /// model cannot fail the selected project.
    pub fn load_models_matching_with_db(
        models_dir: &Path,
        models_glob: &str,
        db: &mut crate::salsa_compile::RockyDatabase,
        project_freshness: Option<&rocky_core::config::ProjectFreshnessConfig>,
    ) -> Result<Vec<Model>, ProjectError> {
        let pattern = compile_models_glob(models_glob)?;
        if !has_matching_model_source(models_dir, &pattern)? {
            return Err(ProjectError::NoModels {
                path: models_dir.display().to_string(),
            });
        }
        Self::load_models_with_db_filtered(
            models_dir,
            db,
            &|path| model_path_matches(&pattern, path),
            project_freshness,
        )
    }

    fn load_models_with_db_filtered(
        models_dir: &Path,
        db: &mut crate::salsa_compile::RockyDatabase,
        include: &impl Fn(&Path) -> bool,
        project_freshness: Option<&rocky_core::config::ProjectFreshnessConfig>,
    ) -> Result<Vec<Model>, ProjectError> {
        // The whole tree, through the one shared walk (#1262) — the same
        // directory set every other scanner sees, so `rocky list models`
        // can never show a model this compile will not build. Compile is a
        // strict consumer: the first walk error fails the load, because an
        // unreadable subtree silently missing from a compile is the
        // silent-drop family this closes.
        let (dirs, walk_errors) = rocky_core::model_walk::walk_model_dirs(models_dir);
        if let Some(walk_error) = walk_errors.into_iter().next() {
            return Err(models::ModelError::from(std::io::Error::other(walk_error)).into());
        }

        let mut models = Vec::new();
        for dir in &dirs {
            // Participation rule (#1262, shared with the CLI matching loader):
            // a directory contributing no model under the active filter has
            // its metadata skipped — malformed defaults beside only
            // non-matching sources must fail NEITHER loader, or the surfaces
            // disagree.
            if !dir_contributes_models(dir, include) {
                continue;
            }
            models.extend(models::load_models_from_dir_filtered(
                dir,
                include,
                project_freshness,
            )?);

            // Also load matching .rocky files via the salsa pipeline.
            let rocky_models =
                load_rocky_models_with_db_filtered(dir, db, include, project_freshness)?;
            if !rocky_models.is_empty() {
                info!(count = rocky_models.len(), dir = %dir.display(), "loaded .rocky models");
                models.extend(rocky_models);
            }
        }

        if models.is_empty() {
            return Err(ProjectError::NoModels {
                path: models_dir.display().to_string(),
            });
        }

        Ok(models)
    }

    /// Load every model from a directory **without** resolving dependencies,
    /// using a transient salsa database for the `.rocky` parse pipeline.
    ///
    /// The non-`db` counterpart to [`Project::load_models_with_db`] — mirrors
    /// [`Project::load`]'s relationship to [`Project::load_with_db`].
    pub fn load_models(
        models_dir: &Path,
        project_freshness: Option<&rocky_core::config::ProjectFreshnessConfig>,
    ) -> Result<Vec<Model>, ProjectError> {
        let mut db = crate::salsa_compile::RockyDatabase::default();
        Self::load_models_with_db(models_dir, &mut db, project_freshness)
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

        // Duplicate *targets*, unlike duplicate names, are recorded rather than
        // rejected — see the field doc on `Project::target_collisions`.
        let target_collisions = collide_on_target(&models);

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
            target_collisions,
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
pub fn load_dir_models(
    dir: &Path,
    project_freshness: Option<&rocky_core::config::ProjectFreshnessConfig>,
) -> Result<Vec<Model>, ProjectError> {
    let mut models = models::load_models_from_dir(dir, project_freshness)?;
    let mut db = crate::salsa_compile::RockyDatabase::default();
    models.extend(load_rocky_models_with_db(dir, &mut db, project_freshness)?);
    Ok(models)
}

/// Load matching `.sql` and `.rocky` models without resolving dependencies.
pub fn load_dir_models_matching(
    dir: &Path,
    models_glob: &str,
    project_freshness: Option<&rocky_core::config::ProjectFreshnessConfig>,
) -> Result<Vec<Model>, ProjectError> {
    let pattern = compile_models_glob(models_glob)?;
    // Per-DIRECTORY on purpose: callers walk the tree themselves and invoke
    // this once per visited directory, so the participation question here is
    // "does THIS directory contribute", never "does anything below it" — the
    // tree-wide answer is [`has_matching_model_source`]'s job, behind the
    // NoModels pre-check only.
    if !dir_has_matching_model_source(dir, &pattern)? {
        return Ok(Vec::new());
    }
    let include = |path: &Path| model_path_matches(&pattern, path);
    let mut models = models::load_models_from_dir_filtered(dir, include, project_freshness)?;
    let mut db = crate::salsa_compile::RockyDatabase::default();
    models.extend(load_rocky_models_with_db_filtered(
        dir,
        &mut db,
        &include,
        project_freshness,
    )?);
    Ok(models)
}

fn compile_models_glob(models_glob: &str) -> Result<glob::Pattern, ProjectError> {
    glob::Pattern::new(models_glob).map_err(|error| ProjectError::InvalidModelsGlob {
        pattern: models_glob.to_string(),
        reason: error.to_string(),
    })
}

fn model_path_matches(pattern: &glob::Pattern, path: &Path) -> bool {
    pattern.matches_path_with(
        path,
        glob::MatchOptions {
            case_sensitive: true,
            require_literal_separator: true,
            require_literal_leading_dot: false,
        },
    )
}

fn has_matching_model_source(root: &Path, pattern: &glob::Pattern) -> Result<bool, ProjectError> {
    // Walks the same tree the loader walks (#1262): a project whose only
    // matching sources sit below the first level must not be reported as
    // having no models. Walk errors are ignored HERE on purpose — this is a
    // pre-check answering "is there anything at all"; the loader immediately
    // behind it surfaces the same errors strictly.
    let (dirs, _) = rocky_core::model_walk::walk_model_dirs(root);
    for dir in dirs {
        if dir_has_matching_model_source(&dir, pattern)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn dir_has_matching_model_source(
    dir: &Path,
    pattern: &glob::Pattern,
) -> Result<bool, ProjectError> {
    if !dir.exists() {
        return Ok(false);
    }
    let entries = std::fs::read_dir(dir).map_err(models::ModelError::from)?;
    for entry in entries {
        let entry = entry.map_err(models::ModelError::from)?;
        let path = entry.path();
        if matches!(
            path.extension().and_then(|extension| extension.to_str()),
            Some("sql" | "rocky")
        ) && model_path_matches(pattern, &path)
        {
            return Ok(true);
        }
    }
    Ok(false)
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
    project_freshness: Option<&rocky_core::config::ProjectFreshnessConfig>,
) -> Result<Vec<Model>, ProjectError> {
    load_rocky_models_with_db_filtered(dir, db, &|_| true, project_freshness)
}

/// Whether `dir` contributes any model under `include` — the participation
/// rule both walks share (#1262). A directory that contributes nothing has
/// its metadata (`_defaults.toml`, groups, test definitions) skipped
/// entirely, mirroring the #1305 contract: "directories with no matching
/// source skip their defaults, groups, and test definitions, so unrelated
/// malformed metadata cannot fail the selected run." Without this rule the
/// two loaders disagree on malformed metadata in non-participating deep
/// directories — listing succeeds while compiling fails, the exact
/// surface-disagreement family this fix exists to close.
fn dir_contributes_models(dir: &Path, include: &impl Fn(&Path) -> bool) -> bool {
    let Ok(entries) = std::fs::read_dir(dir) else {
        // Unreadable directories are the walk's problem (it surfaces them);
        // for participation purposes they contribute nothing.
        return false;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if matches!(
            path.extension().and_then(|extension| extension.to_str()),
            Some("sql" | "rocky")
        ) && include(&path)
        {
            return true;
        }
    }
    false
}

fn load_rocky_models_with_db_filtered(
    dir: &Path,
    db: &mut crate::salsa_compile::RockyDatabase,
    include: &impl Fn(&Path) -> bool,
    project_freshness: Option<&rocky_core::config::ProjectFreshnessConfig>,
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
    // Config groups and named test definitions apply to `.rocky` DSL models
    // too — they resolve their sidecar config through the same `load_model_pair`
    // path, so load them here and thread them in (the `.sql` loader does this in
    // `load_models_from_dir`).
    let groups = models::load_groups_from_dir(dir)?;
    let test_defs = models::load_test_definitions_from_dir(dir)?;
    let ctx = models::ModelLoadContext {
        groups: (!groups.is_empty()).then_some(&groups),
        test_defs: (!test_defs.is_empty()).then_some(&test_defs),
        project_freshness,
    };

    let rocky_paths: Vec<PathBuf> = std::fs::read_dir(dir)
        .map_err(models::ModelError::ReadFile)?
        .filter_map(|entry| {
            let path = entry.ok()?.path();
            if path.extension().is_some_and(|ext| ext == "rocky") && include(&path) {
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
            &ctx,
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
    ctx: &models::ModelLoadContext,
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
        models::load_model_pair_with_context(path, &toml_path, defaults, ctx)?.config
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
            // Same precedence as the sidecar path in
            // `rocky_core::models::resolve_model_config`: directory
            // `_defaults.toml` first, then the project `[freshness]` block
            // (#1435). A `.rocky` model with no sidecar builds its config
            // here instead of going through that function, so the project
            // rung has to be repeated — without it, a DSL model would
            // report no freshness where its `.sql` neighbour reports the
            // inherited block.
            freshness: defaults
                .as_ref()
                .and_then(|d| d.freshness.clone())
                .or_else(|| {
                    ctx.project_freshness
                        .and_then(models::ModelFreshnessConfig::from_project_default)
                }),
            tests: vec![],
            format: None,
            format_options: None,
            classification: Default::default(),
            tags: Default::default(),
            governance: Default::default(),
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
                tags: Default::default(),
                governance: Default::default(),
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

    /// Give a model an explicit target, so two differently-named models can be
    /// pointed at one physical table (the #1291 shape). `make_model` defaults
    /// `table` to the model name, which is already unique via the
    /// duplicate-name check.
    fn with_target(mut m: Model, catalog: &str, schema: &str, table: &str) -> Model {
        m.config.target = TargetConfig {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
        };
        m
    }

    /// #1291: two models resolving to one physical table are RECORDED, not
    /// rejected — construction must still succeed so tolerant readers
    /// (`ci-diff` across two refs, the LSP) keep loading the project.
    #[test]
    fn duplicate_target_is_recorded_not_rejected() {
        let models = vec![
            with_target(make_model("a", "SELECT 1 AS id"), "c", "s", "shared"),
            with_target(make_model("b", "SELECT 2 AS id"), "c", "s", "shared"),
        ];
        let project =
            Project::from_models(models).expect("a duplicate TARGET must not fail construction");
        assert_eq!(project.target_collisions.len(), 1);
        assert_eq!(project.target_collisions[0].models, vec!["a", "b"]);
        assert_eq!(project.target_collisions[0].target, "c.s.shared");
    }

    /// Identity folds case, because answering "different" for two spellings
    /// that are one warehouse object is the fail-open this exists to prevent.
    #[test]
    fn duplicate_target_identity_folds_case() {
        let models = vec![
            with_target(make_model("a", "SELECT 1 AS id"), "c", "s", "Shared"),
            with_target(make_model("b", "SELECT 2 AS id"), "C", "S", "shared"),
        ];
        let project = Project::from_models(models).unwrap();
        assert_eq!(
            project.target_collisions.len(),
            1,
            "case-only differences name one object"
        );
    }

    /// Ephemeral models carry a fully populated but phantom target and are
    /// never materialized, so they are not a second writer.
    #[test]
    fn ephemeral_models_do_not_collide() {
        let mut eph = with_target(make_model("a", "SELECT 1 AS id"), "c", "s", "shared");
        eph.config.strategy = StrategyConfig::Ephemeral;
        let models = vec![
            eph,
            with_target(make_model("b", "SELECT 2 AS id"), "c", "s", "shared"),
        ];
        let project = Project::from_models(models).unwrap();
        assert!(
            project.target_collisions.is_empty(),
            "an ephemeral model materializes nothing, so it cannot be the second writer"
        );
    }

    /// Distinct targets stay clean — guards against the check firing on every
    /// project.
    #[test]
    fn distinct_targets_do_not_collide() {
        let models = vec![
            with_target(make_model("a", "SELECT 1 AS id"), "c", "s", "a"),
            with_target(make_model("b", "SELECT 2 AS id"), "c", "s", "b"),
        ];
        let project = Project::from_models(models).unwrap();
        assert!(project.target_collisions.is_empty());
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

        let models = load_dir_models(d, None).unwrap();
        let names: Vec<_> = models.iter().map(|m| m.config.name.as_str()).collect();
        assert!(names.contains(&"stg"), "sql model loaded: {names:?}");
        assert!(names.contains(&"agg"), "rocky DSL model loaded: {names:?}");
    }

    #[test]
    fn matching_loader_filters_before_parsing_sql_and_keeps_rocky() {
        let _guard = crate::salsa_compile::tests::TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path();
        std::fs::write(d.join("broken.sql"), "SELECT 1").unwrap();
        std::fs::write(d.join("broken.toml"), "name = [").unwrap();
        std::fs::write(d.join("agg.rocky"), "from source\nselect { id }\n").unwrap();
        std::fs::write(
            d.join("agg.toml"),
            "[target]\ncatalog = \"c\"\nschema = \"s\"\n",
        )
        .unwrap();

        let glob = d.join("agg*.rocky").to_string_lossy().into_owned();
        let models = load_dir_models_matching(d, &glob, None).expect("load matching model");
        assert_eq!(models.len(), 1);
        assert_eq!(models[0].config.name, "agg");
    }

    /// A `.rocky` DSL model inherits the project `[freshness]` block, with
    /// and without a `.toml` sidecar.
    ///
    /// The two shapes take different code paths: with a sidecar the config
    /// comes from `resolve_model_config`; without one it is built inline in
    /// `load_single_rocky_model_with_db`. Both must reach the project rung,
    /// or a DSL model reports no freshness where its `.sql` neighbour
    /// reports the inherited block (#1435).
    #[test]
    fn rocky_models_inherit_the_project_freshness_block() {
        let _guard = crate::salsa_compile::tests::TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path();
        // With a sidecar that declares no freshness.
        std::fs::write(d.join("with_sidecar.rocky"), "from orders\nselect { id }\n").unwrap();
        std::fs::write(
            d.join("with_sidecar.toml"),
            "[target]\ncatalog = \"wh\"\nschema = \"s\"\n",
        )
        .unwrap();
        // With no sidecar at all.
        std::fs::write(d.join("bare.rocky"), "from orders\nselect { id }\n").unwrap();

        let project = rocky_core::config::ProjectFreshnessConfig {
            expected_lag_seconds: Some(1800),
            time_column: Some("updated_at".to_string()),
            severity: None,
        };
        let models = load_dir_models(d, Some(&project)).unwrap();

        for name in ["with_sidecar", "bare"] {
            let m = models
                .iter()
                .find(|m| m.config.name == name)
                .unwrap_or_else(|| panic!("{name} loaded"));
            let f = m
                .config
                .freshness
                .as_ref()
                .unwrap_or_else(|| panic!("{name} must inherit the project [freshness] block"));
            assert_eq!(f.max_lag_seconds, 1800);
            assert_eq!(f.time_column.as_deref(), Some("updated_at"));
        }

        // Baseline: with no project block, neither shape gains freshness.
        let none = load_dir_models(d, None).unwrap();
        assert!(none.iter().all(|m| m.config.freshness.is_none()));
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

        let models = load_dir_models(d, None).unwrap();
        let flow = models
            .iter()
            .find(|m| m.config.name == "flow")
            .expect("rocky model loaded");
        assert_eq!(
            flow.config.target.schema, "mart_emea",
            "the group's schema_template must route the .rocky model"
        );
    }

    /// Regression: a `.rocky` DSL model resolves a `[[use_test]]` reference
    /// against `test_definitions.toml` through the salsa loader, like a `.sql`
    /// model does via `load_models_from_dir`.
    #[test]
    fn rocky_model_resolves_named_test() {
        let _guard = crate::salsa_compile::tests::TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path();
        std::fs::write(
            d.join("test_definitions.toml"),
            "[positive]\ntype = \"expression\"\nexpression = \"amount > 0\"\n",
        )
        .unwrap();
        std::fs::write(d.join("flow.rocky"), "from orders\nselect { id }\n").unwrap();
        std::fs::write(
            d.join("flow.toml"),
            "[target]\ncatalog = \"wh\"\nschema = \"s\"\n\n[[use_test]]\nname = \"positive\"\n",
        )
        .unwrap();

        let models = load_dir_models(d, None).unwrap();
        let flow = models
            .iter()
            .find(|m| m.config.name == "flow")
            .expect("rocky model loaded");
        assert_eq!(
            flow.config.tests.len(),
            1,
            "the named test must resolve onto the .rocky model"
        );
    }

    /// Regression: a `.rocky` DSL model inherits its config group's `[tags]`
    /// and merges its own `[tags]` over them (sidecar > group) through the
    /// salsa loader — the same merge a `.sql` model gets via
    /// `load_models_from_dir`. Guards the #917-class wiring gap where a
    /// model-load feature reaches only the `.sql` path.
    #[test]
    fn rocky_model_resolves_governance_tags() {
        let _guard = crate::salsa_compile::tests::TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path();
        std::fs::create_dir_all(d.join("groups")).unwrap();
        std::fs::write(
            d.join("groups/finance.toml"),
            "[tags]\ndomain = \"finance\"\ntier = \"gold\"\n",
        )
        .unwrap();
        std::fs::write(d.join("flow.rocky"), "from orders\nselect { id }\n").unwrap();
        std::fs::write(
            d.join("flow.toml"),
            "group = \"finance\"\n\n[target]\ncatalog = \"wh\"\nschema = \"s\"\n\n\
             [tags]\ntier = \"silver\"\nowner = \"data-eng\"\n",
        )
        .unwrap();

        let models = load_dir_models(d, None).unwrap();
        let flow = models
            .iter()
            .find(|m| m.config.name == "flow")
            .expect("rocky model loaded");
        // group baseline inherited
        assert_eq!(
            flow.config.tags.get("domain").map(String::as_str),
            Some("finance")
        );
        // model overrides the group's value for a shared key
        assert_eq!(
            flow.config.tags.get("tier").map(String::as_str),
            Some("silver")
        );
        // model-only key present alongside the group baseline
        assert_eq!(
            flow.config.tags.get("owner").map(String::as_str),
            Some("data-eng")
        );
    }

    /// Regression: the C5 misplacement guard (a group member that supplies
    /// `[args]` AND pins its own `target.schema`) fires for a `.rocky` DSL model
    /// too, since it rides the shared `resolve_model_config` chokepoint — not
    /// only the `.sql` `load_models_from_dir` path.
    #[test]
    fn rocky_model_args_with_pinned_schema_is_rejected() {
        let _guard = crate::salsa_compile::tests::TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path();
        std::fs::create_dir_all(d.join("groups")).unwrap();
        std::fs::write(
            d.join("groups/marts.toml"),
            "schema_template = \"mart_{region}\"\n",
        )
        .unwrap();
        std::fs::write(d.join("flow.rocky"), "from orders\nselect { id }\n").unwrap();
        std::fs::write(
            d.join("flow.toml"),
            "group = \"marts\"\n\n[target]\ncatalog = \"wh\"\nschema = \"escape\"\n\n[args]\nregion = \"emea\"\n",
        )
        .unwrap();

        let err = load_dir_models(d, None).unwrap_err();
        assert!(
            matches!(
                &err,
                ProjectError::ModelLoad(rocky_core::models::ModelError::GroupArgsWithPinnedSchema {
                    model,
                    group,
                }) if model == "flow" && group == "marts"
            ),
            "got {err:?}"
        );
    }
}

#[cfg(test)]
mod recursive_load_tests {
    use super::*;

    fn write(path: &Path, contents: &str) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(path, contents).unwrap();
    }

    fn write_model(dir: &Path, name: &str) {
        write(&dir.join(format!("{name}.sql")), "SELECT 1 AS id\n");
        write(
            &dir.join(format!("{name}.toml")),
            &format!(
                "name = \"{name}\"\n[target]\ncatalog = \"w\"\nschema = \"s\"\ntable = \"{name}\"\n"
            ),
        );
    }

    /// The compile path sees the same tree every other scanner sees (#1262):
    /// a model two levels down loads, where it used to be silently absent —
    /// which is what made `rocky list models` show a model `run` would then
    /// fail to find.
    #[test]
    fn compile_path_loads_models_at_every_depth() {
        let tmp = tempfile::tempdir().unwrap();
        let models = tmp.path().join("models");
        write_model(&models, "top");
        write_model(&models.join("a").join("b"), "lvl2");

        let loaded = Project::load_models(&models, None).expect("load");
        let mut names: Vec<&str> = loaded.iter().map(|m| m.config.name.as_str()).collect();
        names.sort_unstable();
        assert_eq!(names, ["lvl2", "top"]);
    }

    /// A project whose ONLY matching sources sit below the first level is not
    /// "no models" — the glob pre-check walks the same tree the loader walks.
    #[test]
    fn a_deep_only_project_is_not_no_models() {
        let tmp = tempfile::tempdir().unwrap();
        let models = tmp.path().join("models");
        write_model(&models.join("staging").join("deep"), "only");

        let mut db = crate::salsa_compile::RockyDatabase::default();
        let glob = format!("{}/**", models.display());
        let loaded = Project::load_models_matching_with_db(&models, &glob, &mut db, None)
            .expect("a deep-only project must load, not report NoModels");
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].config.name, "only");
    }

    /// The red-team repro (#1328 review, F1): malformed `_defaults.toml` in a
    /// deep directory whose sources all fail the filter. The participation
    /// rule must make BOTH loaders skip that directory's metadata — listing
    /// and compiling must agree, and both must succeed.
    #[test]
    fn malformed_metadata_in_a_non_participating_directory_fails_neither_loader() {
        let tmp = tempfile::tempdir().unwrap();
        let models = tmp.path().join("models");
        write_model(&models, "orders");
        let deep = models.join("a").join("b");
        std::fs::create_dir_all(&deep).unwrap();
        // Non-matching source + malformed defaults beside it.
        write(&deep.join("customers.sql"), "SELECT 1 AS id\n");
        write(
            &deep.join("customers.toml"),
            "name = \"customers\"\n[target]\ncatalog = \"w\"\nschema = \"s\"\ntable = \"customers\"\n",
        );
        write(&deep.join("_defaults.toml"), "this is not toml [\n");

        let glob = format!("{}/orders*.sql", models.display());
        let mut db = crate::salsa_compile::RockyDatabase::default();
        let compiled = Project::load_models_matching_with_db(&models, &glob, &mut db, None)
            .expect("the compiler must skip a non-participating directory's metadata");
        assert_eq!(compiled.len(), 1);
        assert_eq!(compiled[0].config.name, "orders");

        let listed = load_dir_models_matching(&models, &glob, None)
            .expect("the matching loader must skip it identically");
        assert_eq!(listed.len(), 1, "both surfaces agree: orders only");
    }

    /// A malformed sidecar two levels down fails the STRICT compile load —
    /// proving the file is read — where the flat loader never opened it.
    #[test]
    fn a_broken_deep_sidecar_fails_the_compile_load() {
        let tmp = tempfile::tempdir().unwrap();
        let models = tmp.path().join("models");
        write_model(&models, "top");
        write(&models.join("a").join("b").join("broken.sql"), "SELECT 1\n");
        write(
            &models.join("a").join("b").join("broken.toml"),
            "name = [\n",
        );

        Project::load_models(&models, None)
            .expect_err("a deep malformed sidecar must fail the load");
    }
}
