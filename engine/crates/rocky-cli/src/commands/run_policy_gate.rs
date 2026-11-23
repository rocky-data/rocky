//! The `[policy]` gate on a one-step `rocky run` (#2610).
//!
//! `rocky run` is plan and apply fused, with no persisted plan. Before this
//! gate it read no principal, so an agent under `default_agent_effect =
//! "deny"` wrote production and left no decision in the ledger. The gate
//! evaluates each subject the run writes under [`PolicyCapability::Run`] and
//! records every verdict under the ledger id `run:<run_id>`.
//!
//! ```text
//!   rocky run (agent, [policy] set)
//!     pre-run   compiled models / model-less writes ── evaluate(run) ── record
//!     in-run    replication targets after discovery ── evaluate(run) ── record
//!   verdict     allow → run │ require_review → refuse │ deny → refuse
//! ```
//!
//! Scope, as ruled for this change:
//! - Only an `agent` with a `[policy]` block is evaluated. A human, or a
//!   project with no `[policy]`, never reaches the evaluator, so those runs
//!   are byte-identical to before.
//! - `run` is a sibling verb. Only a rule naming `run` matches it; an `apply`
//!   rule never grants a one-step write.
//! - A `--branch` run writes branch schemas, so the default posture is waived
//!   for it: only an explicit `run` rule governs it. The documented agent
//!   flow (`run --branch`, then a gated `plan promote`) keeps working under
//!   `default_agent_effect = "deny"`.
//! - `require_review` refuses: a one-step run has no plan to review, so the
//!   verdict cannot be satisfied here.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use anyhow::{Result, bail};
use rocky_core::config::{
    PolicyCapability, PolicyEffect, PolicyPrincipal, PrincipalRef, RockyConfig,
};
use rocky_core::state::StateStore;

use super::apply::{
    EmptyTouched, GateSubjects, PolicyGate, evaluate_apply_policy_durable,
    evaluate_apply_policy_with_store, is_replication_only, marker_freezes_before_gate,
    modelless_pipeline_writes, pipeline_is_replication, resolve_config_models_glob,
    resolve_confined_config_models_dir, run_model_selection,
};
use crate::output::RunPlan;

/// Who is running a direct `rocky run`, for the policy gate.
///
/// `None` in place of this struct means "not a direct run": `rocky apply`
/// (gated by its own plan gate), and every internal or test caller.
#[derive(Debug, Clone)]
pub struct DirectRunGate {
    /// The runtime principal class (`--principal` / `ROCKY_PRINCIPAL`).
    pub principal: PolicyPrincipal,
    /// Who is running. A label stamped on the decision rows, never an
    /// enforcement input.
    pub actor: PrincipalRef,
    /// `true` when the caller already evaluated this run's model and
    /// model-less subjects (the `--dag` runner gates its whole graph once).
    /// The in-run replication gate still runs.
    pub pregated: bool,
}

impl DirectRunGate {
    /// A gate for a run the caller has not evaluated yet.
    pub fn new(principal: PolicyPrincipal, actor: PrincipalRef) -> Self {
        Self {
            principal,
            actor,
            pregated: false,
        }
    }

    /// The same gate, marked as evaluated by the caller.
    pub(crate) fn pregated(&self) -> Self {
        Self {
            pregated: true,
            ..self.clone()
        }
    }

    /// Whether this run reaches the evaluator at all.
    fn applies(&self, cfg: &RockyConfig) -> bool {
        self.principal == PolicyPrincipal::Agent && cfg.policy.is_some()
    }
}

/// The ledger id a direct run's decision rows carry. The `run:` prefix never
/// names a plan file, so the review queue counts these rows as non-plan rows
/// instead of offering them for approval.
pub(crate) fn ledger_id(run_id: &str) -> String {
    format!("run:{run_id}")
}

/// What a direct run selected, in the terms the subject derivation needs.
pub(crate) struct RunSelection<'a> {
    pub pipeline: Option<&'a str>,
    /// `--model <name>`, or a one-model `--select`.
    pub model: Option<&'a str>,
    /// A `--select` of two or more models. Like `model`, it takes the
    /// model-only arm of the run, whatever the pipeline type.
    pub selected: Option<&'a BTreeSet<String>>,
    pub models_dir: Option<&'a Path>,
    pub run_all: bool,
    /// `--var` values: the gate's compile must resolve `@var()` as the run's
    /// does, or it fails where the run would succeed.
    pub run_vars: &'a rocky_core::run_vars::RunVars,
}

impl RunSelection<'_> {
    /// The run takes the model-only arm (`--model`, or `--select`).
    fn model_only(&self) -> bool {
        self.model.is_some() || self.selected.is_some()
    }

    /// Whether a compiled model is one this run executes.
    fn selects(&self, name: &str) -> bool {
        match (self.model, self.selected) {
            (Some(model), _) => model == name,
            (None, Some(selected)) => selected.contains(name),
            (None, None) => true,
        }
    }
}

/// The subjects a run will write before discovery: the compiled models it
/// executes, or a model-less pipeline's write set. Replication targets are
/// known only after discovery and are gated in-run by
/// [`enforce_replication_targets`].
///
/// Follows the run's own dispatch order: a model selection takes the
/// model-only arm first, then the pipeline type decides. The directory and
/// glob come from the same helper the governed apply uses
/// ([`run_model_selection`]), so both gate what the run reads.
fn pre_run_subjects(
    cfg: &RockyConfig,
    config_path: &Path,
    selection: &RunSelection<'_>,
    branch: bool,
) -> Result<(BTreeMap<String, PolicyCapability>, EmptyTouched, PathBuf)> {
    let plan = selection_plan(selection);
    let (models_dir, models_glob) = run_model_selection(cfg, config_path, &plan)?;
    if !selection.model_only() {
        if let Some((writes, empty_touched)) = modelless_pipeline_writes(cfg, selection.pipeline) {
            return Ok((run_subjects(writes), empty_touched, models_dir));
        }
        if pipeline_is_replication(cfg, selection.pipeline) {
            // The tables are known only after discovery, and the run starts
            // a vendor sync and claims its idempotency key before that. When
            // no `run` rule could allow any table, refuse here, before any
            // of it, under the pipeline's name.
            let mut touched = BTreeMap::new();
            if !run_could_be_allowed(cfg, branch) {
                let label = crate::registry::resolve_pipeline(cfg, selection.pipeline)
                    .map(|(name, _)| name.to_string())
                    .unwrap_or_else(|_| "replication".to_string());
                touched.insert(label, PolicyCapability::Run);
            }
            if is_replication_only(cfg, &plan) {
                return Ok((touched, EmptyTouched::NoOp, models_dir));
            }
            // `--all` / `--models`: the model leg runs too.
            let (models, empty_touched) =
                selected_models(&models_dir, models_glob.as_deref(), selection);
            touched.extend(run_subjects(models));
            return Ok((touched, empty_touched, models_dir));
        }
    }
    let (models, empty_touched) = selected_models(&models_dir, models_glob.as_deref(), selection);
    Ok((run_subjects(models), empty_touched, models_dir))
}

/// The compiled models the run executes, or `Refuse` when the compile fails
/// and the set is unknown.
fn selected_models(
    models_dir: &Path,
    models_glob: Option<&str>,
    selection: &RunSelection<'_>,
) -> (Vec<String>, EmptyTouched) {
    use rocky_compiler::compile::{self, CompilerConfig};
    let config = CompilerConfig {
        models_dir: models_dir.to_path_buf(),
        run_vars: selection.run_vars.clone(),
        ..Default::default()
    };
    let result = match models_glob {
        Some(glob) => compile::compile_matching(&config, glob),
        None => compile::compile(&config),
    };
    match result {
        Ok(result) => (
            result
                .project
                .models
                .iter()
                .map(|m| m.config.name.clone())
                .filter(|name| selection.selects(name))
                .collect(),
            EmptyTouched::NoOp,
        ),
        Err(_) => (Vec::new(), EmptyTouched::Refuse),
    }
}

/// Whether any `run` verdict for an agent could be `allow`: an `allow`
/// default posture (or a waived one), or an `allow` `run` rule. A run
/// refuses under both `deny` and `require_review`, so neither counts.
fn run_could_be_allowed(cfg: &RockyConfig, branch: bool) -> bool {
    let Some(policy) = cfg.policy.as_ref() else {
        return true;
    };
    branch
        || policy.default_agent_effect == PolicyEffect::Allow
        || policy.rules.iter().any(|rule| {
            rule.principal == PolicyPrincipal::Agent
                && rule.capability.matches_input(PolicyCapability::Run)
                && rule.effect == PolicyEffect::Allow
        })
}

/// A `RunPlan` carrying only the selection fields the shared helpers read.
fn selection_plan(selection: &RunSelection<'_>) -> RunPlan {
    RunPlan {
        filter: None,
        pipeline: selection.pipeline.map(str::to_string),
        // A multi-model `--select` resolves its directory as `--model` does.
        model: selection
            .model
            .map(str::to_string)
            .or_else(|| selection.selected.and_then(|s| s.iter().next().cloned())),
        branch: None,
        partition: None,
        partition_from: None,
        partition_to: None,
        latest: false,
        missing: false,
        lookback: None,
        parallel: 1,
        run_all: selection.run_all,
        env: None,
        models_dir: selection
            .models_dir
            .map(|dir| dir.to_string_lossy().into_owned()),
        resume: None,
        resume_latest: false,
        shadow: false,
        shadow_suffix: None,
        shadow_schema: None,
        dag: false,
        idempotency_key: None,
        governance_override: None,
        models: Vec::new(),
        execution_layers: Vec::new(),
        product_id: None,
        spec_digest: None,
        intent: None,
    }
}

fn run_subjects(names: impl IntoIterator<Item = String>) -> BTreeMap<String, PolicyCapability> {
    names
        .into_iter()
        .map(|name| (name, PolicyCapability::Run))
        .collect()
}

/// The config the evaluator reads: the run's own snapshot, with the default
/// posture waived for a branch-scoped run. Borrowed when nothing changes.
fn evaluation_config(cfg: &RockyConfig, branch: bool) -> std::borrow::Cow<'_, RockyConfig> {
    if !branch {
        return std::borrow::Cow::Borrowed(cfg);
    }
    let mut owned = cfg.clone();
    if let Some(policy) = owned.policy.as_mut() {
        policy.waive_default_posture = true;
    }
    std::borrow::Cow::Owned(owned)
}

/// Gate a direct run's pre-discovery subjects, before any state or warehouse
/// write. A no-op for a human, a project with no `[policy]`, or a run the
/// caller already gated.
///
/// Every verdict is recorded durably (published to a remote state backend
/// when one is configured), so the deny survives the next run's ledger
/// download and `rocky audit` lists it.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn gate_pre_run(
    gate: Option<&DirectRunGate>,
    cfg: &RockyConfig,
    config_path: &Path,
    state_path: &Path,
    run_id: &str,
    selection: &RunSelection<'_>,
    branch: bool,
) -> Result<()> {
    let Some(gate) = gate.filter(|g| !g.pregated && g.applies(cfg)) else {
        return Ok(());
    };
    let (touched, empty_touched, models_dir) =
        pre_run_subjects(cfg, config_path, selection, branch)?;
    gate_subjects(
        gate,
        cfg,
        state_path,
        run_id,
        &touched,
        empty_touched,
        &models_dir,
        branch,
    )
    .await
}

/// Gate an explicit subject set (the `--dag` runner's whole graph).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn gate_subjects(
    gate: &DirectRunGate,
    cfg: &RockyConfig,
    state_path: &Path,
    run_id: &str,
    touched: &BTreeMap<String, PolicyCapability>,
    empty_touched: EmptyTouched,
    models_dir: &Path,
    branch: bool,
) -> Result<()> {
    if !gate.applies(cfg) {
        return Ok(());
    }
    let plan_id = ledger_id(run_id);
    let eval_cfg = evaluation_config(cfg, branch);
    let marker_freezes = marker_freezes_before_gate(&eval_cfg, touched).await?;
    let verdict = evaluate_apply_policy_durable(
        &eval_cfg,
        &plan_id,
        gate.principal,
        &gate.actor,
        touched,
        empty_touched,
        models_dir,
        state_path,
        &marker_freezes,
        None,
        None,
    )
    .await?;
    enforce(verdict, &plan_id, branch)
}

/// Gate the replication targets a direct run discovered, before any
/// warehouse statement. Records through the state handle the run holds,
/// as the governed in-run gate does.
#[allow(clippy::too_many_arguments)]
pub(crate) fn enforce_replication_targets(
    gate: Option<&DirectRunGate>,
    cfg: &RockyConfig,
    config_path: &Path,
    ledger: &StateStore,
    run_id: &str,
    targets: &BTreeSet<String>,
    marker_freezes: &[rocky_core::freeze_marker::ActiveMarkerFreeze],
    branch: bool,
) -> Result<()> {
    let Some(gate) = gate.filter(|g| g.applies(cfg)) else {
        return Ok(());
    };
    if targets.is_empty() {
        return Ok(());
    }
    let touched = run_subjects(targets.iter().cloned());
    let models_dir = resolve_confined_config_models_dir(config_path, Some(cfg))?;
    let models_glob = resolve_config_models_glob(config_path, Some(cfg));
    let plan_id = ledger_id(run_id);
    let eval_cfg = evaluation_config(cfg, branch);
    let verdict = evaluate_apply_policy_with_store(
        eval_cfg.policy.as_ref(),
        &plan_id,
        gate.principal,
        &gate.actor,
        &touched,
        // Never empty: an empty target set returned above.
        EmptyTouched::Refuse,
        &models_dir,
        models_glob.as_deref(),
        ledger,
        marker_freezes,
        GateSubjects::ReplicationTargets,
    );
    enforce(verdict, &plan_id, branch)
}

/// Turn a recorded verdict into the run's outcome.
fn enforce(verdict: PolicyGate, plan_id: &str, branch: bool) -> Result<()> {
    match verdict {
        PolicyGate::NotConfigured | PolicyGate::Allow => Ok(()),
        PolicyGate::Unloadable { reason } => bail!(
            "refusing agent `rocky run` ({plan_id}): the [policy] plane could not be evaluated, \
             so its rules cannot be enforced (fail-closed). Cause: {reason}"
        ),
        PolicyGate::RequireReview {
            model,
            rule_id,
            reason,
        } => {
            // A matched `run` rule governs a branch run too, so only a
            // verdict from the default posture (no rule) can point at one.
            let alternatives = match (branch, rule_id) {
                (true, _) => "Have a human run it, or change the `run` rule",
                (false, Some(_)) => {
                    "Use a reviewed plan (`rocky plan`, `rocky review <plan-id> --approve`, \
                     `rocky apply <plan-id>`), have a human run it, or change the `run` rule"
                }
                (false, None) => {
                    "Build it on a branch (`rocky run --branch <name>`, then `rocky plan \
                     promote`), use a reviewed plan (`rocky plan`, `rocky review <plan-id> \
                     --approve`, `rocky apply <plan-id>`), or have a human run it"
                }
            };
            bail!(
                "policy requires review for agent `rocky run` ({plan_id}): model '{model}'{} — \
                 {reason}. A one-step run has no plan to review. {alternatives}.",
                rule_suffix(rule_id)
            )
        }
        PolicyGate::Deny {
            model,
            rule_id,
            reason,
        } => {
            let alternatives = if branch {
                "Have a human run it, or change the `run` rule"
            } else {
                "Run it on a branch (`rocky run --branch <name>`, then `rocky plan promote`), \
                 or have a human run it"
            };
            bail!(
                "policy DENIES agent `rocky run` ({plan_id}): model '{model}'{} — {reason}. A deny \
                 cannot be satisfied by review. {alternatives}.",
                rule_suffix(rule_id)
            )
        }
    }
}

fn rule_suffix(rule_id: Option<usize>) -> String {
    rule_id
        .map(|id| format!(" (rule {id})"))
        .unwrap_or_default()
}

/// Gate a `rocky run --dag` graph once, before any node runs, under the
/// ledger id `run:<dag_id>`.
///
/// Every node that writes before discovery is a subject: each transformation
/// model and seed by name, each model-less pipeline by its write set.
/// Transformation models are evaluated per pipeline, against that pipeline's
/// own models directory, so an attribute-scoped rule (tag, layer,
/// classification, contract) sees each model's real attributes. Replication
/// pipelines are gated in their sub-runs, after discovery.
pub(crate) async fn gate_dag<'a>(
    gate: &DirectRunGate,
    cfg: &RockyConfig,
    config_path: &Path,
    state_path: &Path,
    dag_id: &str,
    nodes: impl IntoIterator<Item = (rocky_core::unified_dag::NodeKind, &'a str, Option<&'a str>)>,
) -> Result<()> {
    if !gate.applies(cfg) {
        return Ok(());
    }
    for (pipeline, touched) in dag_subjects(cfg, nodes) {
        let models_dir = match pipeline.as_deref() {
            Some(name) => {
                let plan = selection_plan(&RunSelection {
                    pipeline: Some(name),
                    model: None,
                    selected: None,
                    models_dir: None,
                    run_all: false,
                    run_vars: &rocky_core::run_vars::RunVars::new(),
                });
                run_model_selection(cfg, config_path, &plan)?.0
            }
            None => resolve_confined_config_models_dir(config_path, Some(cfg))?,
        };
        gate_subjects(
            gate,
            cfg,
            state_path,
            dag_id,
            &touched,
            EmptyTouched::NoOp,
            &models_dir,
            // `--dag` refuses `--branch`.
            false,
        )
        .await?;
    }
    Ok(())
}

/// The DAG graph's pre-discovery writes, grouped by the transformation
/// pipeline whose models directory holds their attributes (`None` for seeds
/// and model-less writes).
///
/// A model-less node whose write set cannot be derived is gated under its
/// own label, so an unknown write is evaluated (and denied under a deny)
/// instead of skipped.
fn dag_subjects<'a>(
    cfg: &RockyConfig,
    nodes: impl IntoIterator<Item = (rocky_core::unified_dag::NodeKind, &'a str, Option<&'a str>)>,
) -> BTreeMap<Option<String>, BTreeMap<String, PolicyCapability>> {
    use rocky_core::unified_dag::NodeKind;
    let mut groups: BTreeMap<Option<String>, BTreeSet<String>> = BTreeMap::new();
    for (kind, label, pipeline) in nodes {
        match kind {
            NodeKind::Transformation => {
                groups
                    .entry(pipeline.map(str::to_string))
                    .or_default()
                    .insert(label.to_string());
            }
            NodeKind::Seed => {
                groups.entry(None).or_default().insert(label.to_string());
            }
            // A replication pipeline expands to `Source` + `Load` nodes; its
            // sub-run gates the discovered tables.
            NodeKind::Load if pipeline_is_replication(cfg, pipeline) => {}
            NodeKind::Quality | NodeKind::Snapshot | NodeKind::Load => {
                let entry = groups.entry(None).or_default();
                match pipeline.and_then(|p| modelless_pipeline_writes(cfg, Some(p))) {
                    Some((writes, EmptyTouched::Refuse)) if writes.is_empty() => {
                        entry.insert(label.to_string());
                    }
                    Some((writes, _)) => entry.extend(writes),
                    None => {
                        entry.insert(label.to_string());
                    }
                }
            }
            // Read-only, or gated in-run after discovery.
            NodeKind::Source | NodeKind::Replication | NodeKind::Test => {}
        }
    }
    groups
        .into_iter()
        .map(|(pipeline, names)| (pipeline, run_subjects(names)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(policy: &str) -> RockyConfig {
        toml::from_str(policy).expect("policy config parses")
    }

    const RULE: &str = "\n[[policy.rules]]\nprincipal = \"agent\"\ncapability = \"run\"\n\
                        scope = { any = true }\neffect = ";

    /// Only an `allow` can let a run through, so only an `allow` default or
    /// an `allow` `run` rule defers a replication run to its in-run gate.
    /// `require_review` refuses a run as `deny` does, at the same point.
    #[test]
    fn only_an_allow_could_allow_a_run() {
        let cases = [
            ("[policy]\nversion = 1\n".to_string(), false),
            (
                "[policy]\nversion = 1\ndefault_agent_effect = \"require_review\"\n".to_string(),
                false,
            ),
            (
                "[policy]\nversion = 1\ndefault_agent_effect = \"deny\"\n".to_string(),
                false,
            ),
            (
                "[policy]\nversion = 1\ndefault_agent_effect = \"allow\"\n".to_string(),
                true,
            ),
            (
                format!("[policy]\nversion = 1\n{RULE}\"require_review\"\n"),
                false,
            ),
            (
                format!(
                    "[policy]\nversion = 1\ndefault_agent_effect = \"deny\"\n{RULE}\"allow\"\n"
                ),
                true,
            ),
        ];
        for (policy, expected) in &cases {
            assert_eq!(
                run_could_be_allowed(&config(policy), false),
                *expected,
                "{policy}"
            );
            assert!(
                run_could_be_allowed(&config(policy), true),
                "a branch run defers to the in-run gate: {policy}"
            );
        }
        assert!(run_could_be_allowed(&config(""), false));
    }
}
