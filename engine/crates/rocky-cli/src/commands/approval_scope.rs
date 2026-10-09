//! The model set a reviewable run plan's approval covers (#2239).
//!
//! A run plan's approval binds three things to the models apply will execute:
//! the breaking-change findings, the conditional DROP disclosure, and the
//! execution fingerprint. All three must be computed over the SAME set, and
//! that set must be the one apply executes:
//!
//! - A plain run plan executes one models directory through one file glob,
//!   chosen by [`super::apply::run_model_selection`]. Its scope is one unit.
//! - A `--dag` plan executes every transformation pipeline's models, each from
//!   that pipeline's own directory and glob (`run_dag_exec`). Its scope is one
//!   unit per transformation pipeline.
//!
//! Plan time fingerprints the scope, review recomputes it from the disclosed
//! snapshot before writing a marker, and apply recomputes it before a `--dag`
//! plan executes. Any missing, extra or changed model moves the fingerprint.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use rocky_core::config::{PipelineConfig, RockyConfig};
use rocky_core::models::Model;

use crate::output::RunPlan;

/// One directory and file glob whose compiled models a plan executes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ScopeUnit {
    /// The transformation pipeline this unit belongs to, for a `--dag` plan.
    /// `None` for a plain run plan, whose single unit is not pipeline-keyed.
    pub pipeline: Option<String>,
    pub models_dir: PathBuf,
    pub models_glob: Option<String>,
}

/// Every unit a run plan executes, and whether it is a `--dag` plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ApprovalScope {
    pub dag: bool,
    pub units: Vec<ScopeUnit>,
    /// The project seeds directory a `--dag` run loads (its CSVs, sidecars and
    /// seed hooks run as DAG nodes). `None` for a plain run plan, which runs
    /// no seeds.
    pub seeds_dir: Option<PathBuf>,
}

/// A unit with its compile. `head` is `None` for a `--dag` unit that runs
/// nothing (absent directory, or a glob that matches no model).
pub(crate) struct CompiledUnit {
    pub unit: ScopeUnit,
    pub head: Option<rocky_compiler::compile::CompileResult>,
}

impl CompiledUnit {
    pub(crate) fn models(&self) -> &[Model] {
        self.head
            .as_ref()
            .map_or(&[], |head| head.project.models.as_slice())
    }
}

/// Resolve the scope a run plan executes, using the executors' own resolvers.
///
/// A `--dag` plan needs a project config: the DAG runner reads its pipelines
/// from it, so a scope without one would cover nothing the DAG runs. That is
/// an error, never an empty scope.
pub(crate) fn approval_scope(
    config: Option<&RockyConfig>,
    config_path: &Path,
    run_plan: &RunPlan,
) -> Result<ApprovalScope> {
    if run_plan.dag {
        let cfg = config.context(
            "a --dag plan needs a rocky.toml: the DAG runs the models of every pipeline it \
             declares",
        )?;
        return Ok(ApprovalScope {
            dag: true,
            units: dag_units(cfg, config_path)?,
            // The same directory `run_dag_exec::plan_runtime_dag` discovers.
            seeds_dir: Some(config_path.parent().unwrap_or(Path::new(".")).join("seeds")),
        });
    }
    let (models_dir, models_glob) = match config {
        Some(cfg) => super::apply::run_model_selection(cfg, config_path, run_plan)?,
        None => (
            PathBuf::from(run_plan.models_dir.as_deref().unwrap_or("models")),
            None,
        ),
    };
    Ok(ApprovalScope {
        dag: false,
        units: vec![ScopeUnit {
            pipeline: None,
            models_dir,
            models_glob,
        }],
        seeds_dir: None,
    })
}

/// [`approval_scope`] for a project at `root`, anchoring every directory at
/// `root` exactly once.
///
/// `config_path` is already resolved against `root` (`root.join(path)`; an
/// absolute path is kept), so the directories and globs a config declares
/// come out anchored by it. Only a directory taken from the plan itself
/// (`--models`, or the `models` default when no transformation pipeline
/// applies) is relative to the project root, and only those are joined to
/// `root`. Joining a config-derived directory again would double a relative
/// root (`proj/proj/models`).
pub(crate) fn approval_scope_at(
    config: Option<&RockyConfig>,
    root: &Path,
    config_path: &Path,
    run_plan: &RunPlan,
) -> Result<ApprovalScope> {
    let mut scope = approval_scope(config, config_path, run_plan)?;
    if !scope.dag {
        for unit in &mut scope.units {
            // `run_model_selection` returns a glob exactly when it resolved a
            // pipeline from the config; a unit without one is the plan's own
            // directory.
            if unit.models_glob.is_none() {
                unit.models_dir = root.join(&unit.models_dir);
            }
        }
    }
    Ok(scope)
}

/// One unit per transformation pipeline, name-sorted, resolved exactly as
/// `run_dag_exec::load_transformation_models` and the DAG's model-only
/// sub-runs resolve them. A pipeline whose directory is absent is kept: it
/// compiles to nothing now, and a directory created after the plan then
/// moves the fingerprint.
fn dag_units(cfg: &RockyConfig, config_path: &Path) -> Result<Vec<ScopeUnit>> {
    let mut units = Vec::new();
    for (name, pipeline) in &cfg.pipelines {
        let PipelineConfig::Transformation(t) = pipeline else {
            continue;
        };
        let models_dir = match crate::models_loader::locate_models_dir(&t.models, config_path)? {
            crate::models_loader::ModelsDir::Present(dir)
            | crate::models_loader::ModelsDir::Absent(dir) => dir,
        };
        units.push(ScopeUnit {
            pipeline: Some(name.clone()),
            models_dir,
            models_glob: Some(crate::models_loader::resolved_models_glob(
                &t.models,
                config_path,
            )),
        });
    }
    units.sort_by(|a, b| a.pipeline.cmp(&b.pipeline));
    Ok(units)
}

impl ScopeUnit {
    /// `run_plan` as this unit's pipeline runs it: a `--dag` unit resolves
    /// its target adapter from its own pipeline, as the DAG sub-run does.
    pub(crate) fn run_plan_for(&self, run_plan: &RunPlan) -> RunPlan {
        let mut unit_plan = run_plan.clone();
        if let Some(pipeline) = &self.pipeline {
            unit_plan.pipeline = Some(pipeline.clone());
        }
        unit_plan
    }
}

impl ApprovalScope {
    /// The units whose directory exists. A `--dag` run skips a pipeline whose
    /// directory is absent, so review has nothing to compare there.
    pub(crate) fn present_units(&self) -> impl Iterator<Item = &ScopeUnit> {
        self.units.iter().filter(|unit| unit.models_dir.is_dir())
    }

    /// Compile every unit with `source_schemas`.
    ///
    /// Fails closed: a plain plan's missing directory, or any unit that does
    /// not compile, is an error. A `--dag` unit may be empty (its directory is
    /// absent, or its glob matches no model), because the DAG runs nothing for
    /// that pipeline. A plain plan's unit is empty only under
    /// [`NoModels::Empty`].
    pub(crate) fn compile(
        &self,
        source_schemas: &HashMap<String, Vec<rocky_compiler::types::TypedColumn>>,
        no_models: NoModels,
    ) -> Result<Vec<CompiledUnit>> {
        use rocky_compiler::compile::{self, CompileError, CompilerConfig};
        use rocky_compiler::project::ProjectError;

        let mut compiled = Vec::with_capacity(self.units.len());
        for unit in &self.units {
            if !unit.models_dir.is_dir() {
                anyhow::ensure!(
                    self.dag,
                    "models directory '{}' not found",
                    unit.models_dir.display()
                );
                compiled.push(CompiledUnit {
                    unit: unit.clone(),
                    head: None,
                });
                continue;
            }
            let config = CompilerConfig {
                models_dir: unit.models_dir.clone(),
                source_schemas: source_schemas.clone(),
                ..Default::default()
            };
            let result = match unit.models_glob.as_deref() {
                Some(glob) => compile::compile_matching(&config, glob),
                None => compile::compile(&config),
            };
            let head = match result {
                Ok(result) => Some(result),
                Err(CompileError::Project(ProjectError::NoModels { .. }))
                    if self.dag || no_models == NoModels::Empty =>
                {
                    None
                }
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!(
                            "failed to compile models in '{}'",
                            unit.models_dir.display()
                        )
                    });
                }
            };
            compiled.push(CompiledUnit {
                unit: unit.clone(),
                head,
            });
        }
        Ok(compiled)
    }
}

/// What a plain (non-`--dag`) unit that compiles to no model yields.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NoModels {
    /// An error: plan time writes no fingerprint for it.
    Error,
    /// An empty unit: review fingerprints the empty set, which a plan with
    /// models can never match, so a removed model refuses the approval.
    Empty,
}

/// The policy and routing identities a scope fingerprint binds.
pub(crate) struct ScopeIdentities<'a> {
    pub config: &'a str,
    pub governance: &'a str,
    pub exec_control: &'a str,
    /// The env-resolved mask, bound only where apply reconciles masks. Always
    /// empty for a `--dag` plan: its model-only sub-runs reconcile none.
    pub resolved_mask: &'a BTreeMap<String, rocky_ir::MaskStrategy>,
}

/// The execution fingerprint of a compiled scope.
///
/// A plain plan's single unit hashes exactly as
/// [`super::apply::execution_ir_fingerprint`] does, so the in-run governed
/// gate (which recompiles that one unit) compares against the same value.
///
/// A `--dag` plan hashes each pipeline's unit the same way, then hashes the
/// pipeline-keyed list. A model added to, removed from, changed in, or moved
/// between any pipeline's set moves the result.
///
/// `Ok(None)` when a model config does not serialize (the caller refuses).
/// `Err` when a surrogate-key spec is malformed (#1730).
pub(crate) fn scope_fingerprint(
    scope: &ApprovalScope,
    compiled: &[CompiledUnit],
    ids: &ScopeIdentities<'_>,
) -> Result<Option<String>> {
    let dag = scope.dag;
    let empty_mask = BTreeMap::new();
    let mask = if dag { &empty_mask } else { ids.resolved_mask };
    let unit_fingerprint = |unit: &CompiledUnit| -> Result<Option<String>> {
        let models = unit.models();
        let keys = if models.is_empty() {
            HashMap::new()
        } else {
            super::apply::resolved_surrogate_keys(&unit.unit.models_dir, models)?
        };
        let empty_contracts = BTreeMap::new();
        let contract_files = unit
            .head
            .as_ref()
            .map_or(&empty_contracts, |head| &head.contract_files);
        let extras = super::apply::ExecutionExtras::build(&keys, models, contract_files, mask);
        Ok(super::apply::execution_ir_fingerprint(
            models,
            ids.config,
            ids.governance,
            ids.exec_control,
            &extras,
        ))
    };
    if !dag {
        let [unit] = compiled else {
            anyhow::bail!(
                "a plan without --dag executes one models directory, found {}",
                compiled.len()
            );
        };
        return unit_fingerprint(unit);
    }
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"rocky-dag-scope-v1\x00");
    for unit in compiled {
        let Some(fingerprint) = unit_fingerprint(unit)? else {
            return Ok(None);
        };
        hasher.update(unit.unit.pipeline.as_deref().unwrap_or("").as_bytes());
        hasher.update(b"\x00");
        hasher.update(fingerprint.as_bytes());
        hasher.update(b"\x00");
    }
    if let Some(seeds_dir) = &scope.seeds_dir {
        hash_seeds(&mut hasher, seeds_dir)?;
    }
    Ok(Some(hasher.finalize().to_hex().to_string()))
}

/// Fold every seed a `--dag` run loads into the scope fingerprint: its name,
/// its merged sidecar config (target, strategy, column types, and the
/// pre/post hook SQL it executes) and the bytes of its data file. A seed
/// added, removed or edited after the plan moves the fingerprint.
///
/// An absent directory hashes as "no seeds", as the DAG loads none. A
/// directory that fails discovery is an error, as it fails the DAG run.
fn hash_seeds(hasher: &mut blake3::Hasher, seeds_dir: &Path) -> Result<()> {
    hasher.update(b"seeds\x00");
    if !seeds_dir.is_dir() {
        return Ok(());
    }
    let mut seeds = rocky_core::seeds::discover_seeds(seeds_dir)
        .map_err(|e| anyhow::anyhow!("{e}"))
        .with_context(|| format!("failed to discover seeds in {}", seeds_dir.display()))?;
    seeds.sort_by(|a, b| a.name.cmp(&b.name));
    for seed in &seeds {
        // `to_value` sorts the sidecar's map keys, so the bytes are stable.
        let config = serde_json::to_vec(&serde_json::to_value(&seed.config)?)?;
        let data = std::fs::read(&seed.file_path)
            .with_context(|| format!("failed to read seed {}", seed.file_path.display()))?;
        hasher.update(seed.name.as_bytes());
        hasher.update(b"\x00");
        hasher.update(&config);
        hasher.update(b"\x00");
        hasher.update(blake3::hash(&data).to_hex().as_bytes());
        hasher.update(b"\x00");
    }
    Ok(())
}

/// The identities a scope fingerprint binds, read from the project config.
/// `bind_masks` says whether apply reconciles masks for this plan.
pub(crate) struct OwnedScopeIdentities {
    pub config: String,
    pub governance: String,
    pub exec_control: String,
    pub resolved_mask: BTreeMap<String, rocky_ir::MaskStrategy>,
}

impl OwnedScopeIdentities {
    pub(crate) fn from_config(cfg: Option<&RockyConfig>, mask_env: Option<Option<&str>>) -> Self {
        Self {
            config: cfg
                .map(super::apply::config_policy_identity)
                .unwrap_or_default(),
            governance: cfg
                .map(super::apply::governance_policy_identity)
                .unwrap_or_default(),
            exec_control: cfg
                .map(super::apply::execution_control_identity)
                .unwrap_or_default(),
            resolved_mask: match (cfg, mask_env) {
                (Some(cfg), Some(env)) => cfg.resolve_mask_for_env(env),
                _ => BTreeMap::new(),
            },
        }
    }

    pub(crate) fn borrowed(&self) -> ScopeIdentities<'_> {
        ScopeIdentities {
            config: &self.config,
            governance: &self.governance,
            exec_control: &self.exec_control,
            resolved_mask: &self.resolved_mask,
        }
    }
}

/// Apply-time check for a reviewable `--dag` plan: recompile every pipeline's
/// models from the reviewed source-schema snapshot and refuse unless the scope
/// fingerprint equals the one the plan (and its approval) recorded.
///
/// Runs whoever applies the plan. A governed (agent) apply has its own
/// in-run gate, but the DAG's sub-runs carry no governance context, so this
/// is the only check that covers every pipeline's models.
pub(crate) fn verify_dag_scope_for_apply(
    plan: &crate::plan_store::PersistedPlan,
    plan_id: &str,
    cfg: &RockyConfig,
    config_path: &Path,
    run_plan: &RunPlan,
) -> Result<()> {
    let refuse = |why: &str| {
        anyhow::anyhow!(
            "refusing to apply plan '{plan_id}': {why}. A reviewed --dag plan's approval covers \
             every model the DAG runs, across every pipeline's models directory, so apply \
             re-checks that set before running anything. Re-run `rocky plan --dag` and review \
             the new plan."
        )
    };
    let capabilities = plan.embedded_capabilities();
    let expected = capabilities
        .models_fingerprint
        .as_deref()
        .ok_or_else(|| refuse("the plan has no execution fingerprint"))?;
    let source_schemas: HashMap<_, _> = capabilities
        .reviewed_source_schemas
        .ok_or_else(|| refuse("the plan has no reviewed source-schema snapshot"))?
        .into_iter()
        .collect();
    let scope = approval_scope(Some(cfg), config_path, run_plan)?;
    let compiled = scope
        .compile(&source_schemas, NoModels::Error)
        .map_err(|e| refuse(&format!("the DAG's models no longer compile ({e:#})")))?;
    let ids = OwnedScopeIdentities::from_config(Some(cfg), None);
    let actual = scope_fingerprint(&scope, &compiled, &ids.borrowed())
        .map_err(|e| refuse(&format!("its fingerprint cannot be recomputed ({e:#})")))?;
    if actual.as_deref() == Some(expected) {
        return Ok(());
    }
    // The full fingerprint also binds the config the DAG runs under. When the
    // models still match, say so, so the refusal does not blame a model
    // change that did not happen (#2326). Either way the apply refuses.
    let models_only = scope_models_only_fingerprint(&scope, &compiled, &ids.resolved_mask)
        .map_err(|e| refuse(&format!("its fingerprint cannot be recomputed ({e:#})")))?;
    if let Some(expected_models_only) = capabilities.models_only_fingerprint.as_deref()
        && models_only.as_deref() == Some(expected_models_only)
    {
        return Err(anyhow::anyhow!(
            "{PLAN_CONFIG_CHANGED}: refusing to apply plan '{plan_id}': the models the DAG \
             runs are unchanged, but the config they run under changed since this plan was \
             written (adapters, pipelines, governance or run settings, as resolved in this \
             environment). A reviewed --dag plan applies only under the config it was planned \
             under. Re-run `rocky plan --dag` in this environment and review the new \
             plan."
        ));
    }
    Err(refuse(
        "a model the DAG runs was added, removed or changed since the plan was written",
    ))
}

/// Whether apply reconciles masks for this plan, so its fingerprint binds the
/// env-resolved mask. The same predicate plan time computes (`bind_masks` in
/// `plan.rs`): a Replication pipeline whose model leg runs (`--all` /
/// `--models`) on a full, `--model`-less, non-`--dag` run. A backfill
/// reconciles no masks.
pub(crate) fn plan_binds_mask(
    plan: &crate::plan_store::PersistedPlan,
    scope: &ApprovalScope,
    cfg: Option<&RockyConfig>,
    run_plan: &RunPlan,
) -> bool {
    cfg.is_some_and(|cfg| {
        plan.kind != crate::plan_store::PlanKind::Backfill
            && !scope.dag
            && super::apply::pipeline_is_replication(cfg, run_plan.pipeline.as_deref())
            && (run_plan.run_all || run_plan.models_dir.is_some())
            && run_plan.model.is_none()
    })
}

/// The identities a plan's scope fingerprint binds: the config's routing,
/// governance and execution-control identities, plus the mask where
/// [`plan_binds_mask`] says apply reconciles one. Review and apply both read
/// this, so the two recompute the fingerprint the same way.
pub(crate) fn plan_scope_identities(
    plan: &crate::plan_store::PersistedPlan,
    scope: &ApprovalScope,
    cfg: Option<&RockyConfig>,
    run_plan: &RunPlan,
) -> OwnedScopeIdentities {
    let binds_mask = plan_binds_mask(plan, scope, cfg, run_plan);
    OwnedScopeIdentities::from_config(cfg, binds_mask.then_some(run_plan.env.as_deref()))
}

/// The fingerprint of a compiled scope over its models and the masks they
/// use: no config, governance or execution-control identity. Seeds still
/// count for a `--dag` scope, because the DAG runs them.
///
/// `resolved_mask` is the mask the full fingerprint binds: the plan's `--env`
/// resolution where [`plan_binds_mask`] says apply reconciles masks, else
/// empty. Only the strategies of tags the models classify with are hashed,
/// and a `--dag` scope hashes none. It does not depend on the process
/// environment, so a person's apply still refuses a `[mask]` strategy change
/// for a tag the models use.
///
/// A person's apply, and every approval (#2326), compare this one. The
/// identities hash adapters and pipelines with their `${VAR}` values
/// resolved, so the full fingerprint moves when the same plan is applied or
/// approved from another environment, such as a `rocky serve` job child, or
/// after an edit to an unrelated pipeline.
pub(crate) fn scope_models_only_fingerprint(
    scope: &ApprovalScope,
    compiled: &[CompiledUnit],
    resolved_mask: &BTreeMap<String, rocky_ir::MaskStrategy>,
) -> Result<Option<String>> {
    scope_fingerprint(
        scope,
        compiled,
        &ScopeIdentities {
            config: "",
            governance: "",
            exec_control: "",
            resolved_mask,
        },
    )
}

/// The stable code `rocky apply` refuses with when the models a plan
/// fingerprinted no longer match the models on disk. It is in the error text,
/// so a failed HTTP apply job's `error` carries it.
pub(crate) const PLAN_MODELS_CHANGED: &str = "plan_models_changed";

/// The stable code an agent's apply refuses with when the plan's models (and
/// the masks they use) are unchanged but the config they run under is not
/// (adapters, pipelines, governance or run settings, as resolved in this
/// environment).
pub(crate) const PLAN_CONFIG_CHANGED: &str = "plan_config_changed";

/// The stable code `rocky apply` refuses with when a plan carries a
/// fingerprint but not what the check needs to recompute it: a plan written
/// before the check existed.
pub(crate) const PLAN_SNAPSHOT_MISSING: &str = "plan_snapshot_missing";

/// Apply-time check: a plan that carries a models fingerprint executes only
/// if the models it would run still match it.
///
/// `rocky apply` does not replay stored SQL: `run` recompiles the models on
/// disk. This recomputes the fingerprint with the scope and compile review
/// uses, seeded from the plan's reviewed source-schema snapshot.
///
/// ```text
///   principal  compares                       on mismatch
///   agent      full fingerprint               plan_config_changed if the
///              (models + masks + config       models and masks still
///              identities)                    match, else plan_models_changed
///   other      models-only fingerprint        plan_models_changed
///              (models + masks)
/// ```
///
/// "Masks" is the `[mask]` strategy, resolved for the plan's `--env`, of each
/// tag the models classify with, where the plan binds masks at all.
///
/// An agent's apply is also re-checked inside `run`. A person's is not, so
/// for a person the models can still change between this check and the
/// run's own compile.
///
/// A plan with no fingerprint (a legacy plan, or one whose models did not
/// compile at plan time) is not checked here: a review-gated apply already
/// refuses it elsewhere. A plan with a fingerprint but no source-schema
/// snapshot, or (for a person) no models-only fingerprint, predates this
/// check and refuses with [`PLAN_SNAPSHOT_MISSING`].
///
/// `config_path` is already resolved against `root`; `root` anchors only a
/// directory the plan names itself (a backfill's, or `--models`).
pub(crate) fn verify_plan_models_for_apply(
    plan: &crate::plan_store::PersistedPlan,
    plan_id: &str,
    cfg: Option<&RockyConfig>,
    config_path: &Path,
    root: &Path,
    run_plan: &RunPlan,
    principal: rocky_core::config::PolicyPrincipal,
) -> Result<()> {
    let capabilities = plan.embedded_capabilities();
    let Some(expected) = capabilities.models_fingerprint.as_deref() else {
        return Ok(());
    };
    if capabilities.fingerprint_version == 0 {
        return Ok(());
    }
    let agent = principal == rocky_core::config::PolicyPrincipal::Agent;
    let models_changed = |why: &str| {
        anyhow::anyhow!(
            "{PLAN_MODELS_CHANGED}: refusing to apply plan '{plan_id}': models changed since \
             this plan was made; plan again with `rocky plan` (and review the new plan if it \
             needs approval). {why}."
        )
    };
    let snapshot_missing = |what: &str| {
        anyhow::anyhow!(
            "{PLAN_SNAPSHOT_MISSING}: refusing to apply plan '{plan_id}': the plan predates the \
             check apply runs on a plan's models (it has no {what}), so apply cannot compare \
             them with the models on disk. Plan again with `rocky plan` (and review the new \
             plan if it needs approval)."
        )
    };
    let Some(source_schemas) = capabilities.reviewed_source_schemas else {
        return Err(snapshot_missing("source-schema snapshot"));
    };
    let expected_models_only = capabilities.models_only_fingerprint.as_deref();
    if !agent && expected_models_only.is_none() {
        return Err(snapshot_missing("models-only fingerprint"));
    }
    let source_schemas: HashMap<_, _> = source_schemas.into_iter().collect();
    let scope = if plan.kind == crate::plan_store::PlanKind::Backfill {
        // A backfill executes its persisted directory, without a glob.
        ApprovalScope {
            dag: false,
            units: vec![ScopeUnit {
                pipeline: None,
                models_dir: super::apply::backfill_models_dir(root, run_plan),
                models_glob: None,
            }],
            seeds_dir: None,
        }
    } else {
        // `config_path` is resolved against `root` by the apply entry point,
        // as propose and review resolve it, so the directory and the glob
        // are both anchored at the project root and not at the process cwd.
        approval_scope_at(cfg, root, config_path, run_plan)?
    };
    let compiled = scope
        .compile(&source_schemas, NoModels::Empty)
        .map_err(|e| models_changed(&format!("Its models no longer compile ({e:#})")))?;
    // The mask the plan's full fingerprint binds, resolved for the plan's
    // stored `--env`. The models-only fingerprint binds the same mask.
    let ids = plan_scope_identities(plan, &scope, cfg, run_plan);
    let models_only = scope_models_only_fingerprint(&scope, &compiled, &ids.resolved_mask)
        .map_err(|e| models_changed(&format!("Its fingerprint cannot be recomputed ({e:#})")))?;
    let models_match =
        expected_models_only.is_some() && models_only.as_deref() == expected_models_only;
    if !agent {
        if !models_match {
            return Err(models_changed(
                "A model it runs was added, removed or changed, or the `[mask]` strategy of a \
                 tag those models classify with changed",
            ));
        }
        return Ok(());
    }
    let actual = scope_fingerprint(&scope, &compiled, &ids.borrowed())
        .map_err(|e| models_changed(&format!("Its fingerprint cannot be recomputed ({e:#})")))?;
    if actual.as_deref() == Some(expected) {
        return Ok(());
    }
    if models_match {
        return Err(anyhow::anyhow!(
            "{PLAN_CONFIG_CHANGED}: refusing to apply plan '{plan_id}': its models are \
             unchanged, but the config they run under changed since this plan was made \
             (adapters, pipelines, governance or run settings, as resolved in this \
             environment). An agent's apply runs only under the config its plan was made \
             with; plan again with `rocky plan` in this environment."
        ));
    }
    Err(models_changed(
        "A model it runs was added, removed or changed, the `[mask]` strategy of a tag those \
         models classify with changed, or the config those models run under changed",
    ))
}
