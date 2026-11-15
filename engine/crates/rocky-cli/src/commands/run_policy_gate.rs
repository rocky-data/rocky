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
//!   verdict     allow → run │ require_review → warn, run │ deny → refuse
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
//! - `require_review` is recorded and warned about but not enforced: a
//!   one-step run has no plan to review. Refusing it is a separate change
//!   that waits on the owner.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use anyhow::{Result, bail};
use rocky_core::config::{PolicyCapability, PolicyPrincipal, PrincipalRef, RockyConfig};
use rocky_core::state::StateStore;

use super::apply::{
    EmptyTouched, GateSubjects, PolicyGate, evaluate_apply_policy_durable,
    evaluate_apply_policy_with_store, is_replication_only, marker_freezes_before_gate,
    modelless_pipeline_writes, resolve_config_models_glob, resolve_confined_config_models_dir,
    run_executable_models, run_model_selection,
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
    pub model: Option<&'a str>,
    pub models_dir: Option<&'a Path>,
    pub run_all: bool,
}

/// The subjects a run will write before discovery: the compiled models it
/// executes, or a model-less pipeline's write set. Replication targets are
/// known only after discovery and are gated in-run by
/// [`enforce_replication_targets`].
///
/// Mirrors the governed apply's derivation (`apply::execute_governed_run`)
/// through the same helpers, so a one-step run and a two-step apply of the
/// same selection gate the same set.
fn pre_run_subjects(
    cfg: &RockyConfig,
    config_path: &Path,
    selection: &RunSelection<'_>,
) -> Result<(BTreeMap<String, PolicyCapability>, EmptyTouched, PathBuf)> {
    let plan = selection_plan(selection);
    let (models_dir, models_glob) = run_model_selection(cfg, config_path, &plan)?;
    if let Some((writes, empty_touched)) = modelless_pipeline_writes(cfg, selection.pipeline) {
        return Ok((run_subjects(writes), empty_touched, models_dir));
    }
    if is_replication_only(cfg, &plan) {
        return Ok((BTreeMap::new(), EmptyTouched::NoOp, models_dir));
    }
    let (models, empty_touched) =
        match run_executable_models(&models_dir, models_glob.as_deref(), &plan) {
            Some(models) => (models, EmptyTouched::NoOp),
            // The compile failed, so what the run executes is unknown.
            None => (Vec::new(), EmptyTouched::Refuse),
        };
    Ok((run_subjects(models), empty_touched, models_dir))
}

/// A `RunPlan` carrying only the selection fields the shared helpers read.
fn selection_plan(selection: &RunSelection<'_>) -> RunPlan {
    RunPlan {
        filter: None,
        pipeline: selection.pipeline.map(str::to_string),
        model: selection.model.map(str::to_string),
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
    let (touched, empty_touched, models_dir) = pre_run_subjects(cfg, config_path, selection)?;
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
            tracing::warn!(
                plan_id,
                model = model.as_str(),
                "policy resolved require_review for agent `rocky run` on model '{model}'{} — \
                 {reason}. A one-step run has no plan to review, so this verdict is recorded \
                 but not enforced; the run proceeds",
                rule_suffix(rule_id)
            );
            Ok(())
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

/// The DAG graph's writes, keyed for the gate: each transformation model and
/// seed by name, each model-less pipeline by its write set. Replication
/// pipelines are gated in their sub-runs, after discovery.
///
/// A model-less node whose write set cannot be derived is gated under its own
/// label, so an unknown write is evaluated (and denied under a deny) instead
/// of skipped.
pub(crate) fn dag_subjects<'a>(
    cfg: &RockyConfig,
    nodes: impl IntoIterator<Item = (rocky_core::unified_dag::NodeKind, &'a str, Option<&'a str>)>,
) -> BTreeMap<String, PolicyCapability> {
    use rocky_core::unified_dag::NodeKind;
    let mut names: BTreeSet<String> = BTreeSet::new();
    for (kind, label, pipeline) in nodes {
        match kind {
            NodeKind::Transformation | NodeKind::Seed => {
                names.insert(label.to_string());
            }
            NodeKind::Quality | NodeKind::Snapshot | NodeKind::Load => {
                match pipeline.and_then(|p| modelless_pipeline_writes(cfg, Some(p))) {
                    Some((writes, _)) => names.extend(writes),
                    None => {
                        names.insert(label.to_string());
                    }
                }
            }
            // Read-only, or gated in-run after discovery.
            NodeKind::Source | NodeKind::Replication | NodeKind::Test => {}
        }
    }
    run_subjects(names)
}
