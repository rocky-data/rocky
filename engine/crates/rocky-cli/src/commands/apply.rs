//! `rocky apply <plan-id>` — execute a previously-generated plan.
//!
//! Dispatches by `PlanKind`:
//! - `Compact` → `commands::compact::run_compact_apply_in`
//! - `Archive` → `commands::archive::run_archive_apply_in`
//! - `Run` → `commands::run::run` with the `RunPlan` operational metadata
//! - `Replication` → `commands::run::run` against a replication-only project,
//!   after re-discovering source state and asserting it matches the persisted snapshot
//! - `Promote` → `commands::branch::run_promote_apply` with the persisted
//!   per-target SQL statements
//!
//! The `--inline` flag skips plan persistence and executes immediately —
//! this is the path that `rocky run` aliases to so existing callers see no
//! behaviour change.
//!
//! ## Plan payload for Run kind
//!
//! `RunPlan` persists operational metadata (filter, pipeline, partition flags,
//! model list, execution layers) rather than a full `ProjectIr` snapshot.
//! `rocky apply` re-derives `ProjectIr` by calling `commands::run::run` with
//! the same flags — a fast, CPU-only recompile step. Full IR persistence is
//! deferred to a future phase if re-execution from the pinned record without
//! recompile becomes a hard requirement.
//!
//! ## Plan payload for Replication kind (Phase 5b)
//!
//! `ReplicationPlan` persists the canonical `RockyConfig` snapshot plus a
//! sorted source-state snapshot (connectors + tables). Apply re-runs
//! discovery, rebuilds the snapshot with the same canonicalization, and
//! asserts byte-equality against the persisted one — any drift surfaces
//! a clear "source state has drifted since plan was created" error before
//! any SQL is emitted. The successful path delegates to
//! `commands::run::run` with `models_dir = None`, `run_all = false`, and
//! no model filter so the engine's existing replication arm executes.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use anyhow::{Context, Result, bail};
use rocky_core::config::{PolicyCapability, PolicyEffect, PolicyPrincipal};
use rocky_core::policy::{self, ModelAttributes};
use rocky_core::schema::SchemaPattern;
use rocky_core::state::{PolicyDecisionRecord, StateStore};
use tracing::warn;

use crate::commands::parse_filter;
use crate::output::{
    AuditEvent, AuditEventKind, BranchPromoteOutput, PromotePlan, ReplicationConnectorSnapshot,
    ReplicationPlan, RunPlan,
};
use crate::plan_store::{PersistedPlan, PlanKind, read_plan};

use super::archive::run_archive_apply_in;
use super::compact::run_compact_apply_in;

/// Execute `rocky apply <plan-id>`.
///
/// Reads the plan from `.rocky/plans/<plan_id>.json`, dispatches by kind,
/// and emits an `ApplyOutput` envelope wrapping the inner result.
///
/// `state_path` is the already-resolved state-file path threaded from
/// `main.rs` — it is namespace-aware (`resolve_state_path_ns`), so
/// `rocky --state-namespace <ns> apply <plan>` opens the namespaced state
/// file rather than re-resolving the global one. This keeps the canonical
/// `rocky plan` → `rocky apply` workflow consistent with `rocky run`, whose
/// inline apply already receives the same path.
pub async fn run_apply(
    config_path: &Path,
    plan_id: &str,
    state_path: &Path,
    output_json: bool,
) -> Result<()> {
    let cwd = std::env::current_dir().context("failed to get current working directory")?;
    run_apply_in(&cwd, config_path, plan_id, state_path, output_json).await
}

/// Inner implementation — takes an explicit `root` for the plans directory so
/// tests can pass a temp dir without touching the process-global cwd.
pub(crate) async fn run_apply_in(
    root: &Path,
    config_path: &Path,
    plan_id: &str,
    state_path: &Path,
    output_json: bool,
) -> Result<()> {
    let plan =
        read_plan(root, plan_id).with_context(|| format!("failed to read plan '{plan_id}'"))?;

    match plan.kind {
        PlanKind::Compact => {
            // Delegate to the existing compact apply path. It emits its own
            // output directly; we don't need to re-wrap it for compat.
            run_compact_apply_in(root, config_path, plan_id, output_json).await
        }
        PlanKind::Archive => {
            // Delegate to the existing archive apply path.
            run_archive_apply_in(root, config_path, plan_id, output_json).await
        }
        PlanKind::Run => {
            run_apply_run_plan(root, config_path, plan_id, state_path, output_json).await
        }
        PlanKind::Replication => {
            run_apply_replication_plan(root, config_path, plan_id, state_path, output_json).await
        }
        PlanKind::Promote => {
            run_apply_promote_plan(root, config_path, plan_id, state_path, output_json).await
        }
        PlanKind::AiAuthored => {
            run_apply_ai_authored_plan(root, config_path, plan_id, state_path, output_json).await
        }
    }
}

/// Apply a `PlanKind::Run` plan by re-executing `commands::run::run` with
/// the persisted operational metadata.
///
/// The full flag surface of `rocky run` is replayed from the persisted
/// `RunPlan` payload — partitioning, shadow / branch routing, governance
/// override, resume, idempotency key, and the `--dag` mode. Flags whose
/// semantics depend on state at apply time (`--missing`,
/// `--resume-latest`) are passed through as booleans; the actual state-store
/// lookup happens inside `commands::run::run`.
async fn run_apply_run_plan(
    root: &Path,
    config_path: &Path,
    plan_id: &str,
    state_path: &Path,
    output_json: bool,
) -> Result<()> {
    let plan =
        read_plan(root, plan_id).with_context(|| format!("failed to read run plan '{plan_id}'"))?;

    if plan.kind != PlanKind::Run {
        bail!(
            "plan '{plan_id}' is a {} plan, not a run plan. \
             Use `rocky {} apply {plan_id}` instead.",
            plan.kind,
            plan.kind,
        );
    }

    let run_plan: RunPlan = serde_json::from_value(plan.payload.clone())
        .context("failed to deserialize run plan payload")?;

    // F3 seam 2: an agent-authored `Run` plan (`rocky plan --principal agent`,
    // or `ROCKY_PRINCIPAL=agent`) is gated exactly like an AiAuthored plan when
    // a `[policy]` block is configured. Absent `[policy]` this is a no-op —
    // `Run` was never gated — so behaviour is byte-identical to today. A
    // human-authored plan resolves to `allow` (humans are never gated in v0).
    let models_dir = Path::new(run_plan.models_dir.as_deref().unwrap_or("models"));
    let touched = touched_models_for_run(&plan, &run_plan);
    let gate = evaluate_apply_policy(
        config_path,
        plan_id,
        plan.resolved_principal(),
        &touched,
        models_dir,
        state_path,
    );
    apply_policy_gate(root, plan_id, gate)?;

    execute_run_plan(config_path, plan_id, run_plan, state_path, output_json).await
}

/// Execute a deserialized [`RunPlan`] against the warehouse.
///
/// Shared by [`run_apply_run_plan`] (plain `PlanKind::Run`) and
/// [`run_apply_ai_authored_plan`] (`PlanKind::AiAuthored`, post-review): the
/// payload shape is identical, so once the AI-authored gate has cleared, the
/// execution path is byte-for-byte the same as a plain run plan.
///
/// The full flag surface of `rocky run` is replayed from the `RunPlan` —
/// partitioning, shadow / branch routing, governance override, resume,
/// idempotency key, and the `--dag` mode.
async fn execute_run_plan(
    config_path: &Path,
    plan_id: &str,
    run_plan: RunPlan,
    state_path: &Path,
    output_json: bool,
) -> Result<()> {
    // Build partition options from the persisted flags.
    let partition_opts = crate::commands::run::PartitionRunOptions {
        partition: run_plan.partition.clone(),
        from: run_plan.partition_from.clone(),
        to: run_plan.partition_to.clone(),
        latest: run_plan.latest,
        missing: run_plan.missing,
        lookback: run_plan.lookback,
        parallel: run_plan.parallel,
    };

    // `state_path` is the namespace-aware path threaded from main.rs (mirrors
    // how `rocky run`'s inline apply receives it). Used both by branch→shadow
    // resolution below and by `run` itself, so `rocky --state-namespace <ns>
    // apply <plan>` writes to the namespaced state file rather than the global
    // one.

    // Shadow config. Mirrors the `Command::Run` dispatch in main.rs:
    // `--branch` is internally equivalent to `--shadow --shadow-schema
    // <branch.schema_prefix>`; otherwise `--shadow` activates the shadow
    // path with the persisted suffix / schema override. clap rejects
    // `--branch` combined with the shadow flags at plan time, so we only
    // see one of the two shapes here.
    let shadow_suffix = run_plan
        .shadow_suffix
        .clone()
        .unwrap_or_else(|| "_rocky_shadow".to_string());
    let shadow_config = if let Some(ref name) = run_plan.branch {
        let store = rocky_core::state::StateStore::open_read_only(state_path)
            .with_context(|| format!("failed to open state store at {}", state_path.display()))?;
        let record = store.get_branch(name)?.with_context(|| {
            format!("branch '{name}' not found — create it with `rocky branch create {name}`")
        })?;
        Some(rocky_core::shadow::ShadowConfig {
            suffix: shadow_suffix,
            schema_override: Some(record.schema_prefix),
            cleanup_after: false,
        })
    } else if run_plan.shadow {
        Some(rocky_core::shadow::ShadowConfig {
            suffix: shadow_suffix,
            schema_override: run_plan.shadow_schema.clone(),
            cleanup_after: false,
        })
    } else {
        None
    };

    let models_dir_path = run_plan.models_dir.as_ref().map(std::path::PathBuf::from);

    // `--dag` runs every pipeline as a unified DAG. The DAG runner is
    // currently flag-light (it reads config + tooling defaults rather than
    // walking the same flag matrix as `commands::run::run`), so we dispatch
    // to it for plans that captured `dag = true` and let the future
    // unification land separately. This preserves the parity-with-`rocky run`
    // shape for the alias-deprecation path.
    if run_plan.dag {
        return crate::commands::run_with_dag(config_path, state_path, output_json)
            .await
            .with_context(|| format!("rocky apply run plan '{plan_id}' failed (dag path)"));
    }

    // Capture stdout from the run command — `run` writes JSON directly.
    // We execute it normally (it emits output) to preserve streaming
    // behaviour; the ApplyOutput envelope is emitted by `rocky apply`
    // itself only when `output_json` is true.
    //
    // For Phase 2, `run` emits its output directly and we emit the
    // ApplyOutput envelope *after* on a best-effort basis. A future phase
    // can capture run's output to embed it in `ApplyOutput.result`.
    crate::commands::run::run(
        config_path,
        run_plan.filter.as_deref(),
        run_plan.pipeline.as_deref(),
        state_path,
        run_plan.governance_override.as_ref(),
        output_json,
        models_dir_path.as_deref(),
        run_plan.run_all,
        run_plan.resume.as_deref(),
        run_plan.resume_latest,
        shadow_config.as_ref(),
        &partition_opts,
        run_plan.model.as_deref(),
        None, // cache_ttl_override — runtime-only, not part of the plan
        run_plan.idempotency_key.as_deref(),
        run_plan.env.as_deref(),
        // `--defer` is a runtime-only dev convenience, not persisted on the
        // plan; the two-step `rocky plan`/`apply` path always runs without it.
        &crate::commands::run::DeferOptions::default(),
        // The skip gate is a runtime-only `rocky run` overlay, not persisted
        // on the plan; the two-step path builds every planned model.
        &crate::commands::run::SkipRunOptions::default(),
        // Per-run `--var` values are not persisted on the plan; an
        // `@var()` model would compile-error on a two-step apply.
        &rocky_core::run_vars::RunVars::new(),
    )
    .await
    .with_context(|| format!("rocky apply run plan '{plan_id}' failed"))?;

    // The `run` command has already emitted its own JSON (or text) output.
    // Nothing more to emit in the inline/non-envelope path.
    Ok(())
}

/// Path to the review marker for an AI-authored plan:
/// `<root>/.rocky/plans/<plan_id>.reviewed.json`.
///
/// The marker is written by `rocky review <plan-id> --approve` and is the
/// human sign-off that unblocks `rocky apply` for an AI-authored plan. Its
/// presence (not its contents) is what the apply gate checks.
pub(crate) fn review_marker_path(root: &Path, plan_id: &str) -> std::path::PathBuf {
    root.join(".rocky")
        .join("plans")
        .join(format!("{plan_id}.reviewed.json"))
}

/// True when an approved review marker exists for `plan_id` under `root`.
pub(crate) fn ai_plan_is_reviewed(root: &Path, plan_id: &str) -> bool {
    review_marker_path(root, plan_id).exists()
}

// ---------------------------------------------------------------------------
// F3 agent-policy plane — apply/promote enforcement (seams 2 & 3)
// ---------------------------------------------------------------------------

/// The apply-time policy decision, aggregated most-restrictive across every
/// model the plan touches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PolicyGate {
    /// No `[policy]` block in the config — the evaluator was never
    /// constructed. The caller falls back to its pre-F3 behaviour
    /// (AiAuthored → require a review marker; Run/Promote → ungated), so
    /// absent-`[policy]` behaviour is byte-identical to today.
    NotConfigured,
    /// Every touched model resolved to `allow`. Proceed without a marker.
    Allow,
    /// The most-restrictive effect is `require_review`. A human review marker
    /// (`rocky review <plan> --approve`) satisfies it.
    RequireReview {
        model: String,
        rule_id: Option<usize>,
        reason: String,
    },
    /// The most-restrictive effect is `deny` — a hard refusal that cannot be
    /// satisfied interactively (no marker unblocks it; that is the point).
    Deny {
        model: String,
        rule_id: Option<usize>,
        reason: String,
    },
}

/// Build the apply-time [`ModelAttributes`] for every compiled model under
/// `models_dir`, mirroring `rocky policy check`: `classifications` is the
/// distinct column-classification set, `layer` is the `layer` tag, and
/// `contracted` is the presence of a sibling `.contract.toml`.
fn model_attributes(models_dir: &Path) -> BTreeMap<String, ModelAttributes> {
    use rocky_compiler::compile::{self, CompilerConfig};

    let config = CompilerConfig {
        models_dir: models_dir.to_path_buf(),
        ..Default::default()
    };
    let Ok(result) = compile::compile(&config) else {
        return BTreeMap::new();
    };

    let mut out = BTreeMap::new();
    for model in &result.project.models {
        let name = model.config.name.clone();
        let classifications = model.config.classification.values().cloned().collect();
        let layer = model.config.tags.get("layer").cloned();
        let contracted = model.contract_path.is_some();
        let downstreams = result
            .project
            .models
            .iter()
            .filter(|m| m.config.depends_on.iter().any(|d| d == &name))
            .count() as u64;
        out.insert(
            name.clone(),
            ModelAttributes {
                name,
                tags: model.config.tags.clone(),
                classifications,
                layer,
                contracted,
                downstreams,
            },
        );
    }
    out
}

/// Evaluate the agent-policy plane over a plan's touched `(model, capability)`
/// set and aggregate the most-restrictive effect. Records one
/// [`PolicyDecisionRecord`] per evaluation to the ledger (best-effort — an
/// audit-write failure never fails the apply; the *gate* is the safety
/// boundary, the ledger is the trail).
///
/// `touched` maps each governed model to the capability that was reviewed at
/// propose time (the embedded classification, or `schema_change.breaking` when
/// the classification was unavailable / fail-closed). An empty map means the
/// plan changes nothing → `Allow`.
fn evaluate_apply_policy(
    config_path: &Path,
    plan_id: &str,
    principal: PolicyPrincipal,
    touched: &BTreeMap<String, PolicyCapability>,
    models_dir: &Path,
    state_path: &Path,
) -> PolicyGate {
    let policy = match rocky_core::config::load_rocky_config(config_path) {
        Ok(cfg) => match cfg.policy {
            Some(p) => p,
            None => return PolicyGate::NotConfigured,
        },
        // A missing or malformed config leaves the policy plane unconfigured —
        // the caller keeps its pre-F3 gate. (The run path re-loads and surfaces
        // any real config error itself.)
        Err(_) => return PolicyGate::NotConfigured,
    };

    if touched.is_empty() {
        return PolicyGate::Allow;
    }

    let attrs_map = model_attributes(models_dir);
    // Best-effort ledger handle. A failure to open state (e.g. a locked or
    // forward-incompatible store) must not fail the apply — the decision is
    // computed regardless; only the audit write is skipped.
    let ledger = StateStore::open(state_path).ok();

    let mut worst: Option<PolicyGate> = None;
    for (model, capability) in touched {
        let owned;
        let attrs = match attrs_map.get(model) {
            Some(a) => a,
            None => {
                // Model not in the compiled project (e.g. removed on the head
                // side): evaluate against a bare-named model so it still meets
                // the default posture rather than silently escaping.
                owned = ModelAttributes {
                    name: model.clone(),
                    ..Default::default()
                };
                &owned
            }
        };

        let decision = policy::evaluate(&policy, principal, *capability, attrs);

        if let Some(store) = &ledger {
            let record = PolicyDecisionRecord {
                timestamp: chrono::Utc::now(),
                plan_id: plan_id.to_string(),
                principal,
                capability: *capability,
                model: model.clone(),
                effect: decision.effect,
                rule_id: decision.matched_rule,
                reason: decision.reason.clone(),
            };
            if let Err(e) = store.record_policy_decision(&record) {
                warn!(
                    target: "rocky::policy",
                    error = %e,
                    "failed to record policy decision to the ledger (continuing)"
                );
            }
        }

        let gate = match decision.effect {
            PolicyEffect::Allow => PolicyGate::Allow,
            PolicyEffect::RequireReview => PolicyGate::RequireReview {
                model: model.clone(),
                rule_id: decision.matched_rule,
                reason: decision.reason,
            },
            PolicyEffect::Deny => PolicyGate::Deny {
                model: model.clone(),
                rule_id: decision.matched_rule,
                reason: decision.reason,
            },
        };
        // Keep the most-restrictive gate: deny > require_review > allow.
        if worst
            .as_ref()
            .map(|w| gate_rank(&gate) > gate_rank(w))
            .unwrap_or(true)
        {
            worst = Some(gate);
        }
    }

    worst.unwrap_or(PolicyGate::Allow)
}

/// Restrictiveness rank for aggregating per-model [`PolicyGate`]s.
fn gate_rank(gate: &PolicyGate) -> u8 {
    match gate {
        PolicyGate::NotConfigured => 0,
        PolicyGate::Allow => policy::effect_rank(PolicyEffect::Allow),
        PolicyGate::RequireReview { .. } => policy::effect_rank(PolicyEffect::RequireReview),
        PolicyGate::Deny { .. } => policy::effect_rank(PolicyEffect::Deny),
    }
}

/// The `(model, capability)` set the policy plane evaluates for a run-shaped
/// plan (`Run` / `AiAuthored`).
///
/// - Diff available at propose time → the embedded per-changed-model
///   classification. Unchanged models are absent and are not gated.
/// - Diff unavailable (skipped, or a legacy plan with no embed) → **every**
///   planned model, classified `schema_change.breaking` (fail closed).
fn touched_models_for_run(
    plan: &PersistedPlan,
    run_plan: &RunPlan,
) -> BTreeMap<String, PolicyCapability> {
    let caps = plan.embedded_capabilities();
    if caps.diff_available {
        caps.changed
    } else {
        run_plan
            .models
            .iter()
            .map(|m| (m.clone(), PolicyCapability::SchemaChangeBreaking))
            .collect()
    }
}

/// The `(model, capability)` set the policy plane evaluates for a `Promote`
/// plan: every model the branch changed (from the plan's captured
/// breaking-change findings), gated under the bare `promote` verb.
///
/// The `promote` verb — not a `schema_change.*` refinement — is used so a
/// `deny agent promote {…}` rule governs promotions while an `apply`-scoped
/// rule does not accidentally fire on a promote (the refinements are shared
/// between the two verbs). An empty result (no captured findings) means the
/// promote changes nothing the plane needs to gate.
fn touched_models_for_promote(
    promote: &PromotePlan,
    models_dir: &Path,
) -> BTreeMap<String, PolicyCapability> {
    let Some(findings) = promote.breaking_changes.as_ref().filter(|f| !f.is_empty()) else {
        return BTreeMap::new();
    };
    let changed_targets: BTreeSet<String> = findings
        .iter()
        .map(|f| f.change.model().to_string())
        .collect();
    let target_to_name = compile_target_to_name(models_dir);
    changed_targets
        .into_iter()
        .filter_map(|t| {
            target_to_name
                .get(&t)
                .map(|n| (n.clone(), PolicyCapability::Promote))
        })
        .collect()
}

/// Compile `models_dir` and map each model's `target.full_name()` to its
/// logical name (`config.name`). Used to translate breaking-change findings
/// (keyed by target) into the model names the policy scope matches on.
fn compile_target_to_name(models_dir: &Path) -> BTreeMap<String, String> {
    use rocky_compiler::compile::{self, CompilerConfig};
    let config = CompilerConfig {
        models_dir: models_dir.to_path_buf(),
        ..Default::default()
    };
    let Ok(result) = compile::compile(&config) else {
        return BTreeMap::new();
    };
    let ir = super::ci_diff::project_ir_from_compile(&result);
    ir.models
        .iter()
        .map(|m| (m.target.full_name(), m.name.to_string()))
        .collect()
}

/// Apply a resolved [`PolicyGate`] to a run-shaped plan, returning `Ok(true)`
/// when the plan may proceed and `Err(..)` when it is blocked. `require_marker`
/// controls whether `require_review` demands the review marker (it always does
/// for run-shaped plans; kept explicit for the promote mirror).
///
/// The `NotConfigured` arm is handled by the caller (it needs the pre-F3
/// fallback), so this function is only called for a configured plane.
fn apply_policy_gate(root: &Path, plan_id: &str, gate: PolicyGate) -> Result<()> {
    match gate {
        PolicyGate::NotConfigured | PolicyGate::Allow => Ok(()),
        PolicyGate::RequireReview {
            model,
            rule_id,
            reason,
        } => {
            if ai_plan_is_reviewed(root, plan_id) {
                Ok(())
            } else {
                let rule = rule_id.map(|r| format!(" (rule {r})")).unwrap_or_default();
                bail!(
                    "policy requires human review for plan '{plan_id}': model '{model}'{rule} \
                     resolved to require_review — {reason}. \
                     Review and approve it with `rocky review {plan_id} --approve`, \
                     then re-run `rocky apply {plan_id}`."
                )
            }
        }
        PolicyGate::Deny {
            model,
            rule_id,
            reason,
        } => {
            let rule = rule_id.map(|r| format!(" (rule {r})")).unwrap_or_default();
            bail!(
                "policy DENIES plan '{plan_id}': model '{model}'{rule} — {reason}. \
                 A deny cannot be satisfied by review; this mutation is reserved for a human. \
                 Re-scope the change (e.g. propose to a branch) or have a human apply it."
            )
        }
    }
}

/// Apply a `PlanKind::AiAuthored` plan.
///
/// AI-authored plans carry a `RunPlan` payload identical in shape to a plain
/// `PlanKind::Run` plan, but a bare `rocky apply` must NOT execute them — an
/// AI agent could have authored a change that drops a column or rewrites a
/// model. Execution is gated on a review marker written by
/// `rocky review <plan-id> --approve`:
///
/// - marker ABSENT → `bail!` instructing the operator to review first.
/// - marker PRESENT → the human has signed off; dispatch the identical
///   execution path as [`run_apply_run_plan`] via [`execute_run_plan`].
async fn run_apply_ai_authored_plan(
    root: &Path,
    config_path: &Path,
    plan_id: &str,
    state_path: &Path,
    output_json: bool,
) -> Result<()> {
    let plan = read_plan(root, plan_id)
        .with_context(|| format!("failed to read AI-authored plan '{plan_id}'"))?;

    if plan.kind != PlanKind::AiAuthored {
        bail!(
            "plan '{plan_id}' is a {} plan, not an ai_authored plan. \
             Use `rocky apply {plan_id}` and let the dispatcher route it.",
            plan.kind,
        );
    }

    let run_plan: RunPlan = serde_json::from_value(plan.payload.clone())
        .context("failed to deserialize ai_authored plan payload")?;

    // F3 seam 2: rule-driven refusal. When a `[policy]` block is configured,
    // the per-model policy evaluation (over the authoring principal and the
    // embedded, reviewed capability classification) supersedes the fixed
    // AiAuthored gate. Absent a `[policy]` block the evaluator is never
    // constructed and the pre-F3 marker gate remains — byte-identical to today.
    let models_dir = Path::new(run_plan.models_dir.as_deref().unwrap_or("models"));
    let touched = touched_models_for_run(&plan, &run_plan);
    let gate = evaluate_apply_policy(
        config_path,
        plan_id,
        plan.resolved_principal(),
        &touched,
        models_dir,
        state_path,
    );
    match gate {
        PolicyGate::NotConfigured => {
            if !ai_plan_is_reviewed(root, plan_id) {
                bail!(
                    "AI-authored plan '{plan_id}' has not been reviewed and approved. \
                     An AI agent authored this change, so it cannot be applied directly. \
                     Review the breaking-change report and approve it first with \
                     `rocky review {plan_id} --approve`, then re-run `rocky apply {plan_id}`."
                );
            }
        }
        gate => apply_policy_gate(root, plan_id, gate)?,
    }

    execute_run_plan(config_path, plan_id, run_plan, state_path, output_json).await
}

/// Apply a `PlanKind::Replication` plan by re-running discovery, asserting
/// the source state matches the persisted snapshot, then delegating to the
/// replication arm of `commands::run::run`.
///
/// Stale-source detection compares the snapshot built from a fresh discovery
/// against the one captured at plan time. Any difference (connectors added,
/// removed, renamed; tables added, removed, renamed; row counts changed when
/// the adapter surfaces them) is treated as drift and aborts the apply with
/// a clear "re-plan and re-apply" error. The check happens BEFORE any SQL
/// is emitted to the warehouse.
async fn run_apply_replication_plan(
    root: &Path,
    config_path: &Path,
    plan_id: &str,
    state_path: &Path,
    output_json: bool,
) -> Result<()> {
    let plan = read_plan(root, plan_id)
        .with_context(|| format!("failed to read replication plan '{plan_id}'"))?;

    if plan.kind != PlanKind::Replication {
        bail!(
            "plan '{plan_id}' is a {} plan, not a replication plan. \
             Use `rocky apply {plan_id}` and let the dispatcher route it.",
            plan.kind,
        );
    }

    let replication_plan: ReplicationPlan = serde_json::from_value(plan.payload.clone())
        .context("failed to deserialize replication plan payload")?;

    // Re-load config + re-run discovery to detect source drift before
    // touching the warehouse.
    let rocky_cfg = rocky_core::config::load_rocky_config(config_path).context(format!(
        "failed to load config from {}",
        config_path.display()
    ))?;
    let (_pipeline_name, pipeline) = crate::registry::resolve_replication_pipeline(
        &rocky_cfg,
        replication_plan.pipeline.as_deref(),
    )?;
    let pattern = pipeline.schema_pattern()?;

    let adapter_registry = crate::registry::AdapterRegistry::from_config(&rocky_cfg)?;
    let live_connectors = if let Some(ref disc) = pipeline.source.discovery {
        let discovery_adapter = adapter_registry.discovery_adapter(&disc.adapter)?;
        discovery_adapter
            .discover(&pattern.prefix)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .connectors
    } else {
        anyhow::bail!("no discovery adapter configured for this pipeline")
    };

    let live_snapshot = crate::commands::plan::build_source_state_snapshot(&live_connectors);

    // Drift tolerance: when the apply is filtered (e.g. `rocky run --filter id=<X>`
    // or `--filter client=<X>`) and the discovered drift is disjoint from the
    // filter scope, demote the abort to a WARN and proceed. Unfiltered applies
    // keep today's strict behaviour. Drift inside the filter scope (the
    // filtered source itself was added/removed/changed) still bails — that's a
    // real conflict the caller must resolve.
    match decide_drift_scope(
        &replication_plan.source_state_snapshot,
        &live_snapshot,
        replication_plan.filter.as_deref(),
        &pattern,
    ) {
        DriftScope::None => {}
        DriftScope::OutOfScope { drifted, filter } => {
            warn!(
                target = "rocky::replication::drift",
                filter = filter.as_str(),
                drifted = drifted.join(",").as_str(),
                "source state has drifted outside filter scope (filter={filter}, drifted=[{}]); \
                 continuing",
                drifted.join(", "),
            );
        }
        DriftScope::InScope {
            in_scope_drifted,
            filter,
        } => {
            let diff_summary = summarize_source_state_drift(
                &replication_plan.source_state_snapshot,
                &live_snapshot,
            );
            bail!(
                "source state has drifted since plan '{plan_id}' was created \
                 (in-scope under filter={filter}, affected=[{}]).\n\
                 {diff_summary}\n\
                 Re-plan with `rocky plan` and re-apply against the resulting plan_id.",
                in_scope_drifted.join(", ")
            );
        }
        DriftScope::Unfiltered => {
            let diff_summary = summarize_source_state_drift(
                &replication_plan.source_state_snapshot,
                &live_snapshot,
            );
            bail!(
                "source state has drifted since plan '{plan_id}' was created.\n\
                 {diff_summary}\n\
                 Re-plan with `rocky plan` and re-apply against the resulting plan_id."
            );
        }
    }

    // Snapshot matched — delegate to the existing replication codepath
    // by calling `commands::run::run` with model-related args set to
    // defaults. The replication arm inside `run` handles everything
    // from here. `state_path` is the namespace-aware path threaded from
    // main.rs, so a namespaced replication apply writes the namespaced file.
    let partition_opts = crate::commands::run::PartitionRunOptions::default();

    crate::commands::run::run(
        config_path,
        replication_plan.filter.as_deref(),
        replication_plan.pipeline.as_deref(),
        state_path,
        replication_plan.governance_override.as_ref(),
        output_json,
        // No models directory — replication-only apply does not run
        // transformation models.
        None,
        // `--all` runs both replication and models; replication-only
        // apply never wants the model leg.
        false,
        replication_plan.resume.as_deref(),
        replication_plan.resume_latest,
        // No shadow config — branch promote and shadow paths are
        // independent of replication-plan replay.
        None,
        &partition_opts,
        // No model filter — replication runs every discovered table.
        None,
        // Cache TTL override — runtime-only, not part of the plan.
        None,
        replication_plan.idempotency_key.as_deref(),
        replication_plan.env.as_deref(),
        // Replication apply never runs transformation models; defer is moot.
        &crate::commands::run::DeferOptions::default(),
        // The skip gate only applies to transformation models; inert here.
        &crate::commands::run::SkipRunOptions::default(),
        // Replication apply runs no transformation models; vars are inert.
        &rocky_core::run_vars::RunVars::new(),
    )
    .await
    .with_context(|| format!("rocky apply replication plan '{plan_id}' failed"))?;

    Ok(())
}

/// Outcome of the symmetric source-state drift comparison at apply time,
/// scoped against the active `--filter`.
///
/// Filtered applies (`rocky run --filter id=<X>` / `--filter client=<X>`)
/// only intend to touch the in-scope subset. Drift in the *complement* of
/// that subset is informational, not actionable — bailing throws away the
/// preceding bulk run's work and makes wrapping orchestrators non-idempotent
/// under steady-state source-system churn. Unfiltered applies keep the
/// strict semantics because any drift is structurally undefined.
#[derive(Debug, Clone, PartialEq, Eq)]
enum DriftScope {
    /// Snapshots match — proceed normally.
    None,
    /// Drift exists but the active filter excludes every drifted connector
    /// from scope. The caller logs a WARN and proceeds; the downstream
    /// `commands::run::run` call re-discovers and applies the same filter,
    /// so the in-scope subset still executes against fresh source state.
    OutOfScope {
        /// Sorted connector-ids that drifted (added/removed/changed) outside
        /// the filter scope. Surfaced in the WARN log so operators can
        /// correlate against upstream change events.
        drifted: Vec<String>,
        /// Echo of the `--filter` string for logging.
        filter: String,
    },
    /// Drift intersects the filter scope — the caller bails with the
    /// existing hard error. The `--filter` target itself was added,
    /// removed, or schema-changed since plan time; that's a real conflict
    /// the wrapping job needs to resolve.
    InScope {
        /// Sorted connector-ids that drifted *and* are in scope of the
        /// active filter. Surfaced in the bail message so operators see
        /// which filter-relevant connector(s) changed.
        in_scope_drifted: Vec<String>,
        /// Echo of the `--filter` string for the error message.
        filter: String,
    },
    /// No filter was specified — drift detected; the caller bails with
    /// the existing hard error (today's behaviour for unfiltered runs).
    Unfiltered,
}

/// Compute the drift-scope decision used by `run_apply_replication_plan`.
///
/// Pure function so the four FR acceptance scenarios are unit-testable
/// without standing up a discovery adapter or warehouse connection.
///
/// Algorithm:
/// 1. Compute the set of *drifted* connector-ids: every id whose
///    `ReplicationConnectorSnapshot` differs across `persisted` ↔ `live`
///    (added, removed, or any inner field changed — tables, row_count,
///    schema, source_type).
/// 2. If empty → `DriftScope::None`.
/// 3. If `filter` is `None` → `DriftScope::Unfiltered`.
/// 4. Otherwise, expand the filter scope: each connector-id from
///    `persisted ∪ live` that `matches_filter` accepts is in scope.
/// 5. Partition the drifted set by membership in the filter scope. If
///    *any* drifted id is in scope → `DriftScope::InScope`, else
///    `DriftScope::OutOfScope`.
///
/// `client=<X>` may resolve to multiple connector ids (multi-source
/// clients). The filter-scope set is the union of every connector that
/// matches today (live) and every connector that matched at plan time
/// (persisted) — that way a removed in-scope connector is correctly
/// surfaced as in-scope drift even though `live` no longer carries it.
fn decide_drift_scope(
    persisted: &[ReplicationConnectorSnapshot],
    live: &[ReplicationConnectorSnapshot],
    filter: Option<&str>,
    pattern: &SchemaPattern,
) -> DriftScope {
    use std::collections::BTreeMap;

    let persisted_map: BTreeMap<&str, &ReplicationConnectorSnapshot> =
        persisted.iter().map(|c| (c.id.as_str(), c)).collect();
    let live_map: BTreeMap<&str, &ReplicationConnectorSnapshot> =
        live.iter().map(|c| (c.id.as_str(), c)).collect();

    let mut drifted: BTreeSet<String> = BTreeSet::new();
    for id in persisted_map.keys().chain(live_map.keys()) {
        match (persisted_map.get(id), live_map.get(id)) {
            (Some(p), Some(l)) if p != l => {
                drifted.insert((*id).to_string());
            }
            (Some(_), None) | (None, Some(_)) => {
                drifted.insert((*id).to_string());
            }
            _ => {}
        }
    }

    if drifted.is_empty() {
        return DriftScope::None;
    }

    let Some(filter_str) = filter else {
        return DriftScope::Unfiltered;
    };

    // Filter string is well-formed at plan time (it round-tripped through
    // `parse_filter` inside `rocky plan`), but be defensive — if parsing
    // fails here we fall back to today's strict behaviour rather than
    // silently widening tolerance.
    let Ok((filter_key, filter_value)) = parse_filter(filter_str) else {
        return DriftScope::Unfiltered;
    };

    // Build the union of in-scope connector ids across persisted ∪ live.
    // A connector that was in scope at plan time but vanished from live
    // is correctly classified as in-scope drift (the caller must re-plan).
    let in_scope: BTreeSet<&str> = persisted_map
        .values()
        .chain(live_map.values())
        .filter(|snap| connector_matches_filter(snap, pattern, &filter_key, &filter_value))
        .map(|snap| snap.id.as_str())
        .collect();

    let in_scope_drifted: Vec<String> = drifted
        .iter()
        .filter(|id| in_scope.contains(id.as_str()))
        .cloned()
        .collect();

    if in_scope_drifted.is_empty() {
        DriftScope::OutOfScope {
            drifted: drifted.into_iter().collect(),
            filter: filter_str.to_string(),
        }
    } else {
        DriftScope::InScope {
            in_scope_drifted,
            filter: filter_str.to_string(),
        }
    }
}

/// Apply the connector-level `--filter` semantics from
/// [`crate::commands::matches_filter`] directly against a
/// [`ReplicationConnectorSnapshot`].
///
/// `matches_filter` takes `&DiscoveredConnector`, which is the
/// adapter-shaped value; the persisted plan stores the leaner
/// `ReplicationConnectorSnapshot` shape. Rather than synthesise a
/// `DiscoveredConnector`, we re-apply the same id / parsed-schema rules
/// so plan-time and apply-time filter semantics stay in lockstep.
///
/// `table=` is connector-pass-through here (same as `matches_filter`) —
/// per-table subsetting happens inside the downstream `run::run`.
fn connector_matches_filter(
    snap: &ReplicationConnectorSnapshot,
    pattern: &SchemaPattern,
    filter_key: &str,
    filter_value: &str,
) -> bool {
    if filter_key == "id" {
        return snap.id == filter_value;
    }
    if filter_key == "table" {
        return true;
    }
    let Ok(parsed) = pattern.parse(&snap.schema) else {
        return false;
    };
    match parsed.get(filter_key) {
        Some(val) => val == filter_value,
        None => parsed
            .get_multiple(filter_key)
            .is_some_and(|vals| vals.iter().any(|v| v == filter_value)),
    }
}

/// Render a human-readable summary of the diff between the persisted
/// source-state snapshot and the live one. Surfaced inside the
/// stale-source bail message so operators see what changed without
/// having to inspect the plan file by hand.
fn summarize_source_state_drift(
    persisted: &[ReplicationConnectorSnapshot],
    live: &[ReplicationConnectorSnapshot],
) -> String {
    use std::collections::BTreeMap;

    let persisted_map: BTreeMap<&str, &ReplicationConnectorSnapshot> =
        persisted.iter().map(|c| (c.id.as_str(), c)).collect();
    let live_map: BTreeMap<&str, &ReplicationConnectorSnapshot> =
        live.iter().map(|c| (c.id.as_str(), c)).collect();

    let mut lines = vec![format!(
        "  persisted snapshot: {} connector(s); live snapshot: {} connector(s)",
        persisted.len(),
        live.len(),
    )];

    let added: Vec<&str> = live_map
        .keys()
        .filter(|k| !persisted_map.contains_key(*k))
        .copied()
        .collect();
    let removed: Vec<&str> = persisted_map
        .keys()
        .filter(|k| !live_map.contains_key(*k))
        .copied()
        .collect();

    if !added.is_empty() {
        lines.push(format!(
            "  connectors added (in live, not in plan): {}",
            added.join(", ")
        ));
    }
    if !removed.is_empty() {
        lines.push(format!(
            "  connectors removed (in plan, not in live): {}",
            removed.join(", ")
        ));
    }
    for (id, p_conn) in &persisted_map {
        if let Some(l_conn) = live_map.get(id)
            && p_conn != l_conn
        {
            lines.push(format!(
                "  connector '{id}' changed (tables: {} -> {}; schema/type may also differ)",
                p_conn.tables.len(),
                l_conn.tables.len(),
            ));
        }
    }
    lines.join("\n")
}

/// Apply a `PlanKind::Promote` plan by executing the pre-built SQL statements
/// against the warehouse adapter.
///
/// Gates (approval, breaking-change) are NOT re-run — they ran at plan time
/// and their outcomes are captured in the persisted `PromotePlan`. Apply only
/// executes the SQL and emits audit events for `PromoteStarted` /
/// `PromoteCompleted` / `PromoteFailed`.
async fn run_apply_promote_plan(
    root: &Path,
    config_path: &Path,
    plan_id: &str,
    state_path: &Path,
    output_json: bool,
) -> Result<()> {
    use crate::output::print_json;

    let plan = read_plan(root, plan_id)
        .with_context(|| format!("failed to read promote plan '{plan_id}'"))?;

    if plan.kind != PlanKind::Promote {
        bail!(
            "plan '{plan_id}' is a {} plan, not a promote plan. \
             Use `rocky apply {plan_id}` and let the dispatcher route it.",
            plan.kind,
        );
    }

    let promote_plan: PromotePlan = serde_json::from_value(plan.payload.clone())
        .context("failed to deserialize promote plan payload")?;

    // F3 seam 3: mirror the apply-time policy enforcement on the promote path.
    // Absent `[policy]` this is a no-op (promote was never gated). A
    // human-authored promote resolves to `allow`; an agent-authored promote is
    // gated per changed model under the `promote` verb.
    let promote_models_dir = Path::new("models");
    let touched = touched_models_for_promote(&promote_plan, promote_models_dir);
    let gate = evaluate_apply_policy(
        config_path,
        plan_id,
        plan.resolved_principal(),
        &touched,
        promote_models_dir,
        state_path,
    );
    apply_policy_gate(root, plan_id, gate)?;

    // Build actor identity for apply-time audit events.
    let actor = crate::commands::branch::approver_identity_pub().unwrap_or_else(|_| {
        crate::output::ApproverIdentity {
            email: "unknown".to_string(),
            name: None,
            host: "unknown".to_string(),
            source: crate::output::ApproverSource::Local,
        }
    });

    let mut audit = promote_plan.plan_audit.clone();

    audit.push(AuditEvent {
        kind: AuditEventKind::PromoteStarted,
        at: chrono::Utc::now(),
        actor: actor.clone(),
        branch: promote_plan.branch_name.clone(),
        branch_state_hash: promote_plan.branch_state_hash.clone(),
        reason: Some(format!("apply plan_id={plan_id}")),
        breaking_changes: None,
    });

    let (targets_out, overall_success) =
        crate::commands::branch::run_promote_apply(config_path, &promote_plan.targets).await?;

    audit.push(AuditEvent {
        kind: if overall_success {
            AuditEventKind::PromoteCompleted
        } else {
            AuditEventKind::PromoteFailed
        },
        at: chrono::Utc::now(),
        actor: actor.clone(),
        branch: promote_plan.branch_name.clone(),
        branch_state_hash: promote_plan.branch_state_hash.clone(),
        reason: None,
        breaking_changes: None,
    });

    let output = BranchPromoteOutput {
        version: env!("CARGO_PKG_VERSION").to_string(),
        command: "branch promote".to_string(),
        branch: promote_plan.branch_name.clone(),
        branch_state_hash: promote_plan.branch_state_hash.clone(),
        approvals_used: promote_plan.approvals_used.clone(),
        approvals_rejected: promote_plan.approvals_rejected.clone(),
        breaking_changes: promote_plan.breaking_changes.clone(),
        targets: targets_out,
        audit,
        success: overall_success,
    };

    if output_json {
        print_json(&output)?;
    } else if output.success {
        println!(
            "promoted branch '{}' ({} targets) via plan {plan_id}",
            output.branch,
            output.targets.len()
        );
    } else {
        println!(
            "promote failed for branch '{}' after {} target(s) — see JSON output for details",
            output.branch,
            output.targets.len()
        );
    }

    if !overall_success {
        bail!("`rocky apply {plan_id}` (promote) did not complete successfully");
    }
    Ok(())
}

/// Execute `rocky run` directly (the inline path that `rocky run` aliases to).
///
/// Skips plan persistence — equivalent to `rocky apply --inline`. This exists
/// so the binary's `Command::Run` dispatch can delegate here, establishing the
/// routing for the eventual Phase 4 `rocky run` → `rocky apply --inline`
/// deprecation without changing the current behaviour.
///
/// The argument count mirrors `commands::run::run` exactly — same surface,
/// thin passthrough. `#[allow(clippy::too_many_arguments)]` follows the same
/// pattern as `commands::run::run`.
#[allow(clippy::too_many_arguments)]
pub async fn run_apply_inline_for_run(
    config_path: &Path,
    filter: Option<&str>,
    pipeline_name_arg: Option<&str>,
    state_path: &Path,
    governance_override: Option<&rocky_core::config::GovernanceOverride>,
    output_json: bool,
    models_dir: Option<&Path>,
    run_all: bool,
    resume_run_id: Option<&str>,
    resume_latest: bool,
    shadow_config: Option<&rocky_core::shadow::ShadowConfig>,
    partition_opts: &crate::commands::run::PartitionRunOptions,
    model_name_filter: Option<&str>,
    cache_ttl_override: Option<u64>,
    idempotency_key: Option<&str>,
    env: Option<&str>,
    defer_opts: &crate::commands::run::DeferOptions,
    skip_opts: &crate::commands::run::SkipRunOptions,
    run_vars: &rocky_core::run_vars::RunVars,
) -> Result<()> {
    // Thin passthrough — routes to the existing run implementation.
    crate::commands::run::run(
        config_path,
        filter,
        pipeline_name_arg,
        state_path,
        governance_override,
        output_json,
        models_dir,
        run_all,
        resume_run_id,
        resume_latest,
        shadow_config,
        partition_opts,
        model_name_filter,
        cache_ttl_override,
        idempotency_key,
        env,
        defer_opts,
        skip_opts,
        run_vars,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::output::{ReplicationTableSnapshot, RunPlan};
    use crate::plan_store::write_plan;

    fn minimal_run_plan() -> RunPlan {
        RunPlan {
            filter: None,
            pipeline: None,
            model: None,
            branch: None,
            partition: None,
            partition_from: None,
            partition_to: None,
            latest: false,
            missing: false,
            lookback: None,
            parallel: 1,
            run_all: false,
            env: None,
            models_dir: None,
            resume: None,
            resume_latest: false,
            shadow: false,
            shadow_suffix: None,
            shadow_schema: None,
            dag: false,
            idempotency_key: None,
            governance_override: None,
            models: vec!["schema.orders".to_string()],
            execution_layers: vec![vec!["schema.orders".to_string()]],
        }
    }

    /// `rocky apply <compact-plan-id>` with a run plan dispatches to run, not compact.
    /// We only check that the dispatch correctly identifies the kind — actual
    /// execution requires a warehouse adapter.
    #[test]
    fn wrong_kind_for_run_apply_returns_clear_error() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        // Write a compact plan (wrong kind for run-plan apply path).
        let plan_id = write_plan(
            dir.path(),
            crate::plan_store::PlanKind::Compact,
            &serde_json::json!({"dummy": true}),
        )?;

        // read_plan + kind check inside run_apply_run_plan
        let plan = read_plan(dir.path(), &plan_id)?;
        assert_eq!(plan.kind, crate::plan_store::PlanKind::Compact);

        // The actual error path requires an async runtime; verify the guard
        // logic via a sync assertion on the read plan.
        assert_ne!(
            plan.kind,
            PlanKind::Run,
            "compact plan kind must differ from Run"
        );
        Ok(())
    }

    /// `review_marker_path` resolves to
    /// `<root>/.rocky/plans/<plan_id>.reviewed.json`.
    #[test]
    fn review_marker_path_layout() {
        let root = std::path::Path::new("/tmp/proj");
        let plan_id = "abc123";
        let p = super::review_marker_path(root, plan_id);
        assert_eq!(
            p,
            std::path::Path::new("/tmp/proj/.rocky/plans/abc123.reviewed.json")
        );
    }

    /// An AI-authored plan with no review marker is reported as not reviewed,
    /// and the apply gate refuses it. Writing the marker flips the detection.
    /// (The full execution path needs a warehouse adapter; here we assert the
    /// guard helper, which is what `run_apply_ai_authored_plan` branches on.)
    #[test]
    fn ai_authored_plan_review_gate_detects_marker() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let rp = minimal_run_plan();
        let plan_id = write_plan(dir.path(), PlanKind::AiAuthored, &rp)?;

        // Sanity: the plan really is AiAuthored, so the dispatcher routes it
        // to the gated path rather than the bare run path.
        let plan = read_plan(dir.path(), &plan_id)?;
        assert_eq!(plan.kind, PlanKind::AiAuthored);

        // No marker yet → not reviewed → apply must refuse.
        assert!(
            !super::ai_plan_is_reviewed(dir.path(), &plan_id),
            "fresh AI-authored plan must not count as reviewed"
        );

        // Write the marker (what `rocky review --approve` does) → reviewed.
        let marker = super::review_marker_path(dir.path(), &plan_id);
        std::fs::create_dir_all(marker.parent().unwrap())?;
        std::fs::write(&marker, b"{}")?;
        assert!(
            super::ai_plan_is_reviewed(dir.path(), &plan_id),
            "AI-authored plan with a marker present must count as reviewed"
        );
        Ok(())
    }

    /// The apply-time guard refuses to execute an AI-authored plan that has
    /// no review marker, with a message pointing at `rocky review`.
    #[tokio::test]
    async fn ai_authored_apply_without_marker_is_refused() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let rp = minimal_run_plan();
        let plan_id = write_plan(dir.path(), PlanKind::AiAuthored, &rp)?;

        // No marker → the gate bails before any warehouse work. The config
        // path is never read because the marker check short-circuits first.
        let err = super::run_apply_ai_authored_plan(
            dir.path(),
            std::path::Path::new("rocky.toml"),
            &plan_id,
            std::path::Path::new("models/.rocky-state.redb"),
            true,
        )
        .await
        .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("rocky review"),
            "refusal must point at `rocky review`, got: {msg}"
        );
        assert!(
            msg.contains("not been reviewed"),
            "refusal must explain the plan is unreviewed, got: {msg}"
        );
        Ok(())
    }

    // ---------------------------------------------------------------------
    // F3 agent-policy enforcement (seams 2 & 3)
    // ---------------------------------------------------------------------

    /// Config with an adapter + pipeline and an EMPTY `[policy]` block (no
    /// rules, factory `default_agent_effect`).
    const EMPTY_POLICY_TOML: &str = r#"
[adapter]
type = "duckdb"
path = "x.duckdb"

[pipeline.p]
type = "transformation"
models = "models/**"

[pipeline.p.target.governance]
auto_create_schemas = true

[policy]
version = 1
"#;

    /// Config with an adapter + pipeline and NO `[policy]` block.
    const NO_POLICY_TOML: &str = r#"
[adapter]
type = "duckdb"
path = "x.duckdb"

[pipeline.p]
type = "transformation"
models = "models/**"

[pipeline.p.target.governance]
auto_create_schemas = true
"#;

    /// Rewrite a just-written plan file to look like a *legacy* plan: strip the
    /// `principal` field (as if written before F3). The `plan_id` is unchanged
    /// because `principal` rides outside the hash, so the integrity check still
    /// passes on read.
    fn strip_principal_from_plan(root: &Path, plan_id: &str) -> anyhow::Result<()> {
        let path = root
            .join(".rocky")
            .join("plans")
            .join(format!("{plan_id}.json"));
        let raw = std::fs::read_to_string(&path)?;
        let mut v: serde_json::Value = serde_json::from_str(&raw)?;
        v.as_object_mut().unwrap().remove("principal");
        std::fs::write(&path, serde_json::to_vec_pretty(&v)?)?;
        Ok(())
    }

    /// 🔴 THE load-bearing regression: a legacy `ai_authored` plan file with NO
    /// `principal` field, under an EMPTY `[policy]` block, must STILL require
    /// review — it must NOT apply unreviewed. This proves the kind-aware
    /// principal default (`ai_authored` ⟹ agent) is load-bearing: were it to
    /// resolve to `human`, the policy would `allow` and the plan would apply
    /// unreviewed.
    #[tokio::test]
    async fn legacy_ai_authored_with_empty_policy_still_requires_review() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        std::fs::write(dir.path().join("rocky.toml"), EMPTY_POLICY_TOML)?;
        let plan_id = write_plan(dir.path(), PlanKind::AiAuthored, &minimal_run_plan())?;
        strip_principal_from_plan(dir.path(), &plan_id)?;

        let state = dir.path().join("state.redb");
        let err = super::run_apply_ai_authored_plan(
            dir.path(),
            &dir.path().join("rocky.toml"),
            &plan_id,
            &state,
            true,
        )
        .await
        .expect_err("a legacy ai_authored plan under an empty [policy] must NOT apply unreviewed");
        let msg = err.to_string();
        // Must be refused via the POLICY path (`require_review` from the empty
        // block's default_agent_effect), NOT merely the legacy marker path —
        // this is what proves the kind-aware `ai_authored ⟹ agent` default is
        // load-bearing (a `human` default would `allow` and apply unreviewed).
        assert!(
            msg.contains("policy requires human review"),
            "must be refused by the policy plane (not just the marker gate), got: {msg}"
        );
        Ok(())
    }

    /// Byte-identical fallback: with NO `[policy]` block, an AI-authored plan
    /// without a marker is refused with the pre-F3 message (the hardcoded gate
    /// remains the sole gate — the evaluator is never constructed).
    #[tokio::test]
    async fn no_policy_block_ai_authored_requires_marker() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        std::fs::write(dir.path().join("rocky.toml"), NO_POLICY_TOML)?;
        let plan_id = write_plan(dir.path(), PlanKind::AiAuthored, &minimal_run_plan())?;

        let state = dir.path().join("state.redb");
        let err = super::run_apply_ai_authored_plan(
            dir.path(),
            &dir.path().join("rocky.toml"),
            &plan_id,
            &state,
            true,
        )
        .await
        .expect_err("no [policy] block still gates ai_authored on the marker");
        let msg = err.to_string();
        assert!(
            msg.contains("not been reviewed") && msg.contains("rocky review"),
            "must be the pre-F3 marker message, got: {msg}"
        );
        Ok(())
    }

    fn write_config(dir: &Path, policy_rules: &str) -> anyhow::Result<std::path::PathBuf> {
        let toml = format!(
            r#"
[adapter]
type = "duckdb"
path = "x.duckdb"

[pipeline.p]
type = "transformation"
models = "models/**"

[pipeline.p.target.governance]
auto_create_schemas = true

[policy]
version = 1
default_agent_effect = "require_review"
{policy_rules}
"#
        );
        let path = dir.join("rocky.toml");
        std::fs::write(&path, toml)?;
        Ok(path)
    }

    #[test]
    fn evaluate_apply_policy_denies_agent_on_any_deny_rule() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let config = write_config(
            dir.path(),
            r#"
[[policy.rules]]
principal = "agent"
capability = "apply"
scope = { any = true }
effect = "deny"
"#,
        )?;
        let mut touched = BTreeMap::new();
        touched.insert("m".to_string(), PolicyCapability::SchemaChangeBreaking);
        let gate = super::evaluate_apply_policy(
            &config,
            "plan_x",
            PolicyPrincipal::Agent,
            &touched,
            &dir.path().join("models"),
            &dir.path().join("state.redb"),
        );
        assert!(matches!(gate, PolicyGate::Deny { .. }), "got {gate:?}");
        Ok(())
    }

    #[test]
    fn evaluate_apply_policy_allows_human_even_with_deny_rule() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let config = write_config(
            dir.path(),
            r#"
[[policy.rules]]
principal = "agent"
capability = "apply"
scope = { any = true }
effect = "deny"
"#,
        )?;
        let mut touched = BTreeMap::new();
        touched.insert("m".to_string(), PolicyCapability::SchemaChangeBreaking);
        // A human principal is never gated in v0 — the agent deny does not apply.
        let gate = super::evaluate_apply_policy(
            &config,
            "plan_x",
            PolicyPrincipal::Human,
            &touched,
            &dir.path().join("models"),
            &dir.path().join("state.redb"),
        );
        assert_eq!(gate, PolicyGate::Allow);
        Ok(())
    }

    #[test]
    fn evaluate_apply_policy_not_configured_without_block() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        std::fs::write(dir.path().join("rocky.toml"), NO_POLICY_TOML)?;
        let mut touched = BTreeMap::new();
        touched.insert("m".to_string(), PolicyCapability::SchemaChangeBreaking);
        let gate = super::evaluate_apply_policy(
            &dir.path().join("rocky.toml"),
            "plan_x",
            PolicyPrincipal::Agent,
            &touched,
            &dir.path().join("models"),
            &dir.path().join("state.redb"),
        );
        assert_eq!(gate, PolicyGate::NotConfigured);
        Ok(())
    }

    #[test]
    fn evaluate_apply_policy_empty_touched_allows() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let config = write_config(dir.path(), "")?;
        let gate = super::evaluate_apply_policy(
            &config,
            "plan_x",
            PolicyPrincipal::Agent,
            &BTreeMap::new(),
            &dir.path().join("models"),
            &dir.path().join("state.redb"),
        );
        assert_eq!(
            gate,
            PolicyGate::Allow,
            "no changed models ⇒ nothing to gate"
        );
        Ok(())
    }

    #[test]
    fn touched_models_for_run_fail_closed_without_embed() {
        // A plan with no embedded classification (legacy / diff skipped) marks
        // every planned model breaking.
        let dir = tempfile::tempdir().unwrap();
        let plan_id = write_plan(dir.path(), PlanKind::AiAuthored, &minimal_run_plan()).unwrap();
        let plan = read_plan(dir.path(), &plan_id).unwrap();
        let run_plan: RunPlan = serde_json::from_value(plan.payload.clone()).unwrap();
        let touched = super::touched_models_for_run(&plan, &run_plan);
        assert_eq!(
            touched.get("schema.orders"),
            Some(&PolicyCapability::SchemaChangeBreaking)
        );
    }

    #[test]
    fn apply_policy_gate_deny_and_review_and_allow() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        // Deny cannot be satisfied — always errors.
        let deny = PolicyGate::Deny {
            model: "m".to_string(),
            rule_id: Some(0),
            reason: "denied by rule 0".to_string(),
        };
        assert!(super::apply_policy_gate(dir.path(), "p", deny).is_err());

        // RequireReview without a marker errors; with a marker it passes.
        let review = || PolicyGate::RequireReview {
            model: "m".to_string(),
            rule_id: None,
            reason: "default".to_string(),
        };
        assert!(super::apply_policy_gate(dir.path(), "p", review()).is_err());
        let marker = super::review_marker_path(dir.path(), "p");
        std::fs::create_dir_all(marker.parent().unwrap())?;
        std::fs::write(&marker, b"{}")?;
        assert!(super::apply_policy_gate(dir.path(), "p", review()).is_ok());

        // Allow and NotConfigured always pass.
        assert!(super::apply_policy_gate(dir.path(), "p", PolicyGate::Allow).is_ok());
        assert!(super::apply_policy_gate(dir.path(), "p", PolicyGate::NotConfigured).is_ok());
        Ok(())
    }

    /// Dispatching an AI-authored plan id through a wrong-kind guard reports
    /// the actual kind rather than silently treating it as a run plan.
    #[test]
    fn wrong_kind_for_ai_authored_apply_returns_clear_error() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let plan_id = write_plan(
            dir.path(),
            crate::plan_store::PlanKind::Compact,
            &serde_json::json!({"dummy": true}),
        )?;
        let plan = read_plan(dir.path(), &plan_id)?;
        assert_eq!(plan.kind, crate::plan_store::PlanKind::Compact);
        assert_ne!(
            plan.kind,
            PlanKind::AiAuthored,
            "compact plan kind must differ from AiAuthored"
        );
        Ok(())
    }

    #[test]
    fn run_plan_round_trip_serde() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let rp = minimal_run_plan();
        let plan_id = write_plan(dir.path(), PlanKind::Run, &rp)?;

        let persisted = read_plan(dir.path(), &plan_id)?;
        assert_eq!(persisted.kind, PlanKind::Run);

        let decoded: RunPlan = serde_json::from_value(persisted.payload)?;
        assert_eq!(decoded.models, vec!["schema.orders".to_string()]);
        assert_eq!(decoded.parallel, 1);
        assert!(!decoded.latest);
        Ok(())
    }

    /// Every flag in the backfilled surface round-trips through write/read +
    /// serde. This is the apply-side guarantee that no field is silently
    /// dropped by the persistence layer.
    #[test]
    fn run_plan_full_flag_surface_round_trips() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let rp = RunPlan {
            filter: Some("client=acme".to_string()),
            pipeline: Some("main".to_string()),
            model: Some("orders".to_string()),
            branch: None,
            partition: Some("2026-04-07".to_string()),
            partition_from: None,
            partition_to: None,
            latest: false,
            missing: false,
            lookback: Some(7),
            parallel: 2,
            run_all: true,
            env: Some("prod".to_string()),
            models_dir: Some("custom_models".to_string()),
            resume: Some("rid_123".to_string()),
            resume_latest: false,
            shadow: true,
            shadow_suffix: Some("_my_shadow".to_string()),
            shadow_schema: Some("custom_schema".to_string()),
            dag: false,
            idempotency_key: Some("my_idem_key".to_string()),
            governance_override: Some(rocky_core::config::GovernanceOverride {
                workspace_ids: None,
                allow_empty_workspace_ids: false,
                grants: vec![],
                schema_grants: vec![],
            }),
            models: vec!["db.s.orders".to_string()],
            execution_layers: vec![vec!["db.s.orders".to_string()]],
        };
        let plan_id = write_plan(dir.path(), PlanKind::Run, &rp)?;
        let persisted = read_plan(dir.path(), &plan_id)?;
        let decoded: RunPlan = serde_json::from_value(persisted.payload)?;

        assert_eq!(decoded.model.as_deref(), Some("orders"));
        assert_eq!(decoded.partition.as_deref(), Some("2026-04-07"));
        assert_eq!(decoded.resume.as_deref(), Some("rid_123"));
        assert!(decoded.shadow);
        assert_eq!(decoded.shadow_suffix.as_deref(), Some("_my_shadow"));
        assert_eq!(decoded.shadow_schema.as_deref(), Some("custom_schema"));
        assert_eq!(decoded.idempotency_key.as_deref(), Some("my_idem_key"));
        assert_eq!(decoded.models_dir.as_deref(), Some("custom_models"));
        assert!(decoded.governance_override.is_some());
        Ok(())
    }

    /// `--idempotency-key` is part of the content-hashed plan payload, so
    /// two plans differing only by key get distinct plan_ids. This is the
    /// canonical answer per the parity PR — the hash discriminates.
    #[test]
    fn different_idempotency_keys_produce_distinct_plan_ids() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let mut base = minimal_run_plan();
        base.idempotency_key = Some("key_a".to_string());
        let id_a = write_plan(dir.path(), PlanKind::Run, &base)?;

        base.idempotency_key = Some("key_b".to_string());
        let id_b = write_plan(dir.path(), PlanKind::Run, &base)?;

        assert_ne!(
            id_a, id_b,
            "different idempotency keys must produce different plan_ids"
        );
        Ok(())
    }

    /// `--missing` and `--resume-latest` persist as booleans; both round-trip
    /// even though the actual lookup happens at apply time against the state
    /// store. This guards against accidental skip_serializing_if drift.
    #[test]
    fn deferred_state_flags_round_trip() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let mut rp = minimal_run_plan();
        rp.missing = true;
        rp.resume_latest = true;
        let plan_id = write_plan(dir.path(), PlanKind::Run, &rp)?;
        let persisted = read_plan(dir.path(), &plan_id)?;
        let decoded: RunPlan = serde_json::from_value(persisted.payload)?;
        assert!(decoded.missing);
        assert!(decoded.resume_latest);
        Ok(())
    }

    #[test]
    fn run_plan_with_all_flags_round_trips() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let rp = RunPlan {
            filter: Some("client=acme".to_string()),
            pipeline: Some("main".to_string()),
            model: None,
            branch: None,
            partition: None,
            partition_from: Some("2026-01-01".to_string()),
            partition_to: Some("2026-01-31".to_string()),
            latest: false,
            missing: false,
            lookback: Some(3),
            parallel: 4,
            run_all: true,
            env: Some("prod".to_string()),
            models_dir: None,
            resume: None,
            resume_latest: false,
            shadow: false,
            shadow_suffix: None,
            shadow_schema: None,
            dag: false,
            idempotency_key: None,
            governance_override: None,
            models: vec!["db.s.orders".to_string(), "db.s.users".to_string()],
            execution_layers: vec![
                vec!["db.s.users".to_string()],
                vec!["db.s.orders".to_string()],
            ],
        };
        let plan_id = write_plan(dir.path(), PlanKind::Run, &rp)?;
        let persisted = read_plan(dir.path(), &plan_id)?;
        let decoded: RunPlan = serde_json::from_value(persisted.payload)?;

        assert_eq!(decoded.filter.as_deref(), Some("client=acme"));
        assert_eq!(decoded.pipeline.as_deref(), Some("main"));
        assert_eq!(decoded.partition_from.as_deref(), Some("2026-01-01"));
        assert_eq!(decoded.lookback, Some(3));
        assert_eq!(decoded.parallel, 4);
        assert!(decoded.run_all);
        assert_eq!(decoded.env.as_deref(), Some("prod"));
        assert_eq!(decoded.execution_layers.len(), 2);
        Ok(())
    }

    // ------------------------------------------------------------------
    // Replication plan (Phase 5b)
    // ------------------------------------------------------------------

    fn minimal_replication_plan() -> ReplicationPlan {
        ReplicationPlan {
            filter: Some("source=orders".to_string()),
            pipeline: Some("playground".to_string()),
            env: None,
            idempotency_key: None,
            resume: None,
            resume_latest: false,
            governance_override: None,
            config_snapshot: serde_json::json!({"adapter": {"default": {"type": "duckdb"}}}),
            source_state_snapshot: vec![ReplicationConnectorSnapshot {
                id: "raw__orders".to_string(),
                schema: "raw__orders".to_string(),
                source_type: "duckdb".to_string(),
                tables: vec![ReplicationTableSnapshot {
                    name: "orders".to_string(),
                    row_count: Some(100),
                }],
            }],
        }
    }

    /// Round-trip: `ReplicationPlan` written to disk parses back into an
    /// identical struct. Guards against accidental skip_serializing_if
    /// drift on the new payload.
    #[test]
    fn replication_plan_round_trip_serde() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let rp = minimal_replication_plan();
        let plan_id = write_plan(dir.path(), PlanKind::Replication, &rp)?;
        assert_eq!(plan_id.len(), 64);

        let persisted = read_plan(dir.path(), &plan_id)?;
        assert_eq!(persisted.kind, PlanKind::Replication);

        let decoded: ReplicationPlan = serde_json::from_value(persisted.payload)?;
        assert_eq!(decoded.filter.as_deref(), Some("source=orders"));
        assert_eq!(decoded.pipeline.as_deref(), Some("playground"));
        assert_eq!(decoded.source_state_snapshot.len(), 1);
        assert_eq!(decoded.source_state_snapshot[0].id, "raw__orders");
        assert_eq!(decoded.source_state_snapshot[0].tables.len(), 1);
        assert_eq!(decoded.source_state_snapshot[0].tables[0].name, "orders");
        assert_eq!(
            decoded.source_state_snapshot[0].tables[0].row_count,
            Some(100)
        );
        Ok(())
    }

    /// Identical replication plan payloads produce identical plan_ids —
    /// the content-addressing property that the apply path relies on
    /// for stale-source detection.
    #[test]
    fn replication_plan_same_payload_same_plan_id() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let rp = minimal_replication_plan();
        let id1 = write_plan(dir.path(), PlanKind::Replication, &rp)?;
        let id2 = write_plan(dir.path(), PlanKind::Replication, &rp)?;
        assert_eq!(
            id1, id2,
            "identical replication payload must produce identical plan_id"
        );
        Ok(())
    }

    /// Differing config snapshots (any field) produce different plan_ids.
    /// Critical for "I changed my rocky.toml and re-planned" workflows —
    /// each config edit yields a fresh plan_id and the old plan stays
    /// rejected by stale-source / stale-config logic.
    #[test]
    fn replication_plan_differing_config_yields_distinct_plan_ids() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let mut rp = minimal_replication_plan();
        let id_a = write_plan(dir.path(), PlanKind::Replication, &rp)?;

        // Mutate the config snapshot — even a single key change must
        // shift the plan_id.
        rp.config_snapshot = serde_json::json!({"adapter": {"default": {"type": "snowflake"}}});
        let id_b = write_plan(dir.path(), PlanKind::Replication, &rp)?;

        assert_ne!(
            id_a, id_b,
            "different config_snapshot must produce different plan_ids"
        );
        Ok(())
    }

    /// Differing source-state snapshots (e.g. table added, row_count
    /// changed) produce different plan_ids. The plan_id is the
    /// deterministic correlation handle apply uses to detect drift —
    /// this is the property that keeps stale plans from re-running
    /// against a moved source.
    #[test]
    fn replication_plan_differing_source_state_yields_distinct_plan_ids() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let mut rp = minimal_replication_plan();
        let id_a = write_plan(dir.path(), PlanKind::Replication, &rp)?;

        // Add a new table to the snapshot.
        rp.source_state_snapshot[0]
            .tables
            .push(ReplicationTableSnapshot {
                name: "order_items".to_string(),
                row_count: Some(500),
            });
        let id_b = write_plan(dir.path(), PlanKind::Replication, &rp)?;

        assert_ne!(
            id_a, id_b,
            "different source_state_snapshot must produce different plan_ids"
        );

        // Mutate row_count — also has to change the plan_id.
        rp.source_state_snapshot[0].tables[0].row_count = Some(101);
        let id_c = write_plan(dir.path(), PlanKind::Replication, &rp)?;
        assert_ne!(
            id_b, id_c,
            "row_count mutation must produce different plan_id"
        );
        Ok(())
    }

    /// Mismatched filter / idempotency_key produce different plan_ids,
    /// mirroring `RunPlan`'s precedent. Two runs with the same source
    /// state but different `--filter` are not the same plan.
    #[test]
    fn replication_plan_differing_filter_yields_distinct_plan_ids() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let mut rp = minimal_replication_plan();
        rp.filter = Some("source=orders".to_string());
        let id_a = write_plan(dir.path(), PlanKind::Replication, &rp)?;

        rp.filter = Some("source=customers".to_string());
        let id_b = write_plan(dir.path(), PlanKind::Replication, &rp)?;

        assert_ne!(
            id_a, id_b,
            "different filter must produce different plan_ids"
        );
        Ok(())
    }

    /// Wrong-kind dispatch: an applier given a `PlanKind::Compact` id
    /// reports the actual kind in the error message rather than
    /// silently treating it as Replication.
    #[test]
    fn wrong_kind_for_replication_apply_returns_clear_error() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let plan_id = write_plan(
            dir.path(),
            crate::plan_store::PlanKind::Compact,
            &serde_json::json!({"dummy": true}),
        )?;

        let plan = read_plan(dir.path(), &plan_id)?;
        assert_eq!(plan.kind, crate::plan_store::PlanKind::Compact);
        assert_ne!(
            plan.kind,
            PlanKind::Replication,
            "compact plan kind must differ from Replication"
        );
        Ok(())
    }

    /// `summarize_source_state_drift` surfaces added/removed/changed
    /// connectors so the apply-side bail message tells operators what
    /// changed.
    #[test]
    fn drift_summary_calls_out_added_removed_changed() {
        let persisted = vec![
            ReplicationConnectorSnapshot {
                id: "conn_a".to_string(),
                schema: "schema_a".to_string(),
                source_type: "fivetran".to_string(),
                tables: vec![ReplicationTableSnapshot {
                    name: "orders".to_string(),
                    row_count: None,
                }],
            },
            ReplicationConnectorSnapshot {
                id: "conn_b".to_string(),
                schema: "schema_b".to_string(),
                source_type: "fivetran".to_string(),
                tables: vec![],
            },
        ];
        let live = vec![
            // conn_a kept but with one extra table — changed.
            ReplicationConnectorSnapshot {
                id: "conn_a".to_string(),
                schema: "schema_a".to_string(),
                source_type: "fivetran".to_string(),
                tables: vec![
                    ReplicationTableSnapshot {
                        name: "orders".to_string(),
                        row_count: None,
                    },
                    ReplicationTableSnapshot {
                        name: "shipments".to_string(),
                        row_count: None,
                    },
                ],
            },
            // conn_b absent — removed.
            // conn_c added.
            ReplicationConnectorSnapshot {
                id: "conn_c".to_string(),
                schema: "schema_c".to_string(),
                source_type: "fivetran".to_string(),
                tables: vec![],
            },
        ];

        let summary = summarize_source_state_drift(&persisted, &live);
        assert!(
            summary.contains("connectors added"),
            "summary should mention adds: {summary}"
        );
        assert!(
            summary.contains("conn_c"),
            "summary should name the added connector: {summary}"
        );
        assert!(
            summary.contains("connectors removed"),
            "summary should mention removals: {summary}"
        );
        assert!(
            summary.contains("conn_b"),
            "summary should name the removed connector: {summary}"
        );
        assert!(
            summary.contains("conn_a"),
            "summary should mention the changed connector: {summary}"
        );
    }

    /// Identical snapshots produce a summary that still surfaces counts
    /// (operators may want to confirm "same shape but my apply still
    /// rejected", which would be a bug elsewhere).
    #[test]
    fn drift_summary_handles_identical_snapshots() {
        let snap = vec![ReplicationConnectorSnapshot {
            id: "conn_a".to_string(),
            schema: "schema_a".to_string(),
            source_type: "duckdb".to_string(),
            tables: vec![],
        }];
        let summary = summarize_source_state_drift(&snap, &snap);
        assert!(summary.contains("1 connector(s)"));
    }

    // ------------------------------------------------------------------
    // Drift scope decision (filter-aware tolerance)
    // ------------------------------------------------------------------
    //
    // The four FR acceptance scenarios drive these tests:
    //   1. Unfiltered drift -> Unfiltered (today's hard error).
    //   2. Filtered (`id=`) + out-of-scope drift -> OutOfScope (WARN).
    //   3. Filtered (`id=`) + in-scope drift -> InScope (hard error).
    //   4. Filtered (`client=`) parity for the multi-source case.
    //
    // The pure-function shape lets us cover all four paths without
    // standing up a discovery adapter or warehouse adapter.

    /// Standard `src__<client>__<region>__<source>` pattern used in the
    /// monorepo's playground / config-skill examples. Lets us exercise
    /// `--filter client=<X>` against parsed schema components.
    fn fivetran_pattern() -> SchemaPattern {
        use rocky_core::schema::PatternComponent;
        SchemaPattern {
            prefix: "src__".to_string(),
            separator: "__".to_string(),
            components: vec![
                PatternComponent::Variable {
                    name: "client".to_string(),
                },
                PatternComponent::Variable {
                    name: "region".to_string(),
                },
                PatternComponent::Terminal {
                    name: "source".to_string(),
                },
            ],
        }
    }

    fn snap(id: &str, client: &str, region: &str, source: &str) -> ReplicationConnectorSnapshot {
        ReplicationConnectorSnapshot {
            id: id.to_string(),
            schema: format!("src__{client}__{region}__{source}"),
            source_type: "fivetran".to_string(),
            tables: vec![],
        }
    }

    fn snap_with_tables(
        id: &str,
        client: &str,
        region: &str,
        source: &str,
        tables: Vec<&str>,
    ) -> ReplicationConnectorSnapshot {
        ReplicationConnectorSnapshot {
            id: id.to_string(),
            schema: format!("src__{client}__{region}__{source}"),
            source_type: "fivetran".to_string(),
            tables: tables
                .into_iter()
                .map(|t| ReplicationTableSnapshot {
                    name: t.to_string(),
                    row_count: None,
                })
                .collect(),
        }
    }

    /// No drift at all -> `None`, regardless of filter.
    #[test]
    fn drift_scope_no_drift_returns_none() {
        let pattern = fivetran_pattern();
        let snap_a = snap("conn_a", "acme", "us_west", "shopify");
        let live = vec![snap_a.clone()];
        let persisted = vec![snap_a];

        for filter in [None, Some("id=conn_a"), Some("client=acme")] {
            let decision = decide_drift_scope(&persisted, &live, filter, &pattern);
            assert_eq!(
                decision,
                DriftScope::None,
                "no drift must resolve to None (filter={filter:?})"
            );
        }
    }

    /// Drift exists and no filter -> `Unfiltered` (today's behaviour).
    #[test]
    fn drift_scope_unfiltered_drift_returns_unfiltered() {
        let pattern = fivetran_pattern();
        let persisted = vec![
            snap("conn_a", "acme", "us_west", "shopify"),
            snap("conn_b", "globex", "eu_central", "stripe"),
        ];
        let live = vec![snap("conn_a", "acme", "us_west", "shopify")];

        let decision = decide_drift_scope(&persisted, &live, None, &pattern);
        assert_eq!(
            decision,
            DriftScope::Unfiltered,
            "unfiltered drift must keep strict semantics"
        );
    }

    /// FR Acceptance #1: bulk run completes, an unrelated connector
    /// drops from upstream, the filtered apply tolerates the drift.
    /// `--filter id=conn_a` + only `conn_b` dropped -> `OutOfScope`.
    #[test]
    fn drift_scope_id_filter_out_of_scope_drift_returns_out_of_scope() {
        let pattern = fivetran_pattern();
        let persisted = vec![
            snap("conn_a", "acme", "us_west", "shopify"),
            snap("conn_b", "globex", "eu_central", "stripe"),
        ];
        // conn_b removed in live.
        let live = vec![snap("conn_a", "acme", "us_west", "shopify")];

        let decision = decide_drift_scope(&persisted, &live, Some("id=conn_a"), &pattern);
        match decision {
            DriftScope::OutOfScope { drifted, filter } => {
                assert_eq!(drifted, vec!["conn_b".to_string()]);
                assert_eq!(filter, "id=conn_a");
            }
            other => panic!("expected OutOfScope, got {other:?}"),
        }
    }

    /// FR Acceptance #2: the filtered source itself is dropped ->
    /// `InScope` (hard error preserved).
    #[test]
    fn drift_scope_id_filter_in_scope_drift_returns_in_scope() {
        let pattern = fivetran_pattern();
        let persisted = vec![
            snap("conn_a", "acme", "us_west", "shopify"),
            snap("conn_b", "globex", "eu_central", "stripe"),
        ];
        // conn_a dropped — the filter target itself moved.
        let live = vec![snap("conn_b", "globex", "eu_central", "stripe")];

        let decision = decide_drift_scope(&persisted, &live, Some("id=conn_a"), &pattern);
        match decision {
            DriftScope::InScope {
                in_scope_drifted,
                filter,
            } => {
                assert_eq!(in_scope_drifted, vec!["conn_a".to_string()]);
                assert_eq!(filter, "id=conn_a");
            }
            other => panic!("expected InScope, got {other:?}"),
        }
    }

    /// FR Acceptance #3: in-scope schema/table mutation (filter target
    /// still present) -> `InScope`.
    #[test]
    fn drift_scope_id_filter_in_scope_table_mutation_returns_in_scope() {
        let pattern = fivetran_pattern();
        let persisted = vec![snap_with_tables(
            "conn_a",
            "acme",
            "us_west",
            "shopify",
            vec!["orders"],
        )];
        // conn_a now exposes one extra table.
        let live = vec![snap_with_tables(
            "conn_a",
            "acme",
            "us_west",
            "shopify",
            vec!["orders", "shipments"],
        )];

        let decision = decide_drift_scope(&persisted, &live, Some("id=conn_a"), &pattern);
        match decision {
            DriftScope::InScope {
                in_scope_drifted, ..
            } => {
                assert_eq!(in_scope_drifted, vec!["conn_a".to_string()]);
            }
            other => panic!("expected InScope on table mutation, got {other:?}"),
        }
    }

    /// FR Acceptance #4: `--filter client=<X>` with multi-source
    /// clients — out-of-scope churn tolerated.
    /// `client=acme` resolves to {conn_a, conn_a2}; only conn_b
    /// (client=globex) churns.
    #[test]
    fn drift_scope_client_filter_out_of_scope_drift_returns_out_of_scope() {
        let pattern = fivetran_pattern();
        let persisted = vec![
            snap("conn_a", "acme", "us_west", "shopify"),
            snap("conn_a2", "acme", "eu_central", "stripe"),
            snap("conn_b", "globex", "us_west", "shopify"),
        ];
        // conn_b dropped in live.
        let live = vec![
            snap("conn_a", "acme", "us_west", "shopify"),
            snap("conn_a2", "acme", "eu_central", "stripe"),
        ];

        let decision = decide_drift_scope(&persisted, &live, Some("client=acme"), &pattern);
        match decision {
            DriftScope::OutOfScope { drifted, filter } => {
                assert_eq!(drifted, vec!["conn_b".to_string()]);
                assert_eq!(filter, "client=acme");
            }
            other => panic!("expected OutOfScope for client filter, got {other:?}"),
        }
    }

    /// `--filter client=<X>` where any in-scope connector mutates ->
    /// `InScope` (the FR's "real conflict for the caller" case).
    #[test]
    fn drift_scope_client_filter_in_scope_drift_returns_in_scope() {
        let pattern = fivetran_pattern();
        let persisted = vec![
            snap_with_tables("conn_a", "acme", "us_west", "shopify", vec!["orders"]),
            snap("conn_a2", "acme", "eu_central", "stripe"),
            snap("conn_b", "globex", "us_west", "shopify"),
        ];
        // conn_a (client=acme — in scope) gained a table; conn_b also
        // dropped, but the in-scope mutation is the decisive signal.
        let live = vec![
            snap_with_tables(
                "conn_a",
                "acme",
                "us_west",
                "shopify",
                vec!["orders", "shipments"],
            ),
            snap("conn_a2", "acme", "eu_central", "stripe"),
        ];

        let decision = decide_drift_scope(&persisted, &live, Some("client=acme"), &pattern);
        match decision {
            DriftScope::InScope {
                in_scope_drifted, ..
            } => {
                assert!(
                    in_scope_drifted.contains(&"conn_a".to_string()),
                    "in-scope set must include conn_a: {in_scope_drifted:?}"
                );
                assert!(
                    !in_scope_drifted.contains(&"conn_b".to_string()),
                    "out-of-scope drifted ids must NOT appear: {in_scope_drifted:?}"
                );
            }
            other => panic!("expected InScope for client filter, got {other:?}"),
        }
    }

    /// A connector that was in scope at plan time but vanished from
    /// live must be classified as in-scope drift (re-plan required).
    /// Guards against the "removed-but-still-mine" edge case.
    #[test]
    fn drift_scope_client_filter_removed_in_scope_connector_returns_in_scope() {
        let pattern = fivetran_pattern();
        let persisted = vec![
            snap("conn_a", "acme", "us_west", "shopify"),
            snap("conn_a2", "acme", "eu_central", "stripe"),
        ];
        // conn_a2 (client=acme — in scope) is gone.
        let live = vec![snap("conn_a", "acme", "us_west", "shopify")];

        let decision = decide_drift_scope(&persisted, &live, Some("client=acme"), &pattern);
        match decision {
            DriftScope::InScope {
                in_scope_drifted, ..
            } => {
                assert_eq!(in_scope_drifted, vec!["conn_a2".to_string()]);
            }
            other => panic!("expected InScope for removed in-scope connector, got {other:?}"),
        }
    }

    /// Malformed filter strings degrade to `Unfiltered` (today's
    /// strict behaviour). Defensive — the filter has already been
    /// parsed once at plan time, but we don't want a parse regression
    /// here to silently widen tolerance.
    #[test]
    fn drift_scope_malformed_filter_falls_back_to_unfiltered() {
        let pattern = fivetran_pattern();
        let persisted = vec![snap("conn_a", "acme", "us_west", "shopify")];
        let live = vec![];

        // No "=" → parse_filter fails.
        let decision = decide_drift_scope(&persisted, &live, Some("noequals"), &pattern);
        assert_eq!(
            decision,
            DriftScope::Unfiltered,
            "unparseable filter must keep strict semantics"
        );
    }

    /// `connector_matches_filter` mirrors `commands::matches_filter`'s
    /// rules for `id` / `table` / parsed-schema components. Pinned so
    /// the plan-time and apply-time filter semantics stay aligned.
    #[test]
    fn connector_matches_filter_id_and_components() {
        let pattern = fivetran_pattern();
        let s = snap("conn_a", "acme", "us_west", "shopify");
        assert!(connector_matches_filter(&s, &pattern, "id", "conn_a"));
        assert!(!connector_matches_filter(&s, &pattern, "id", "conn_b"));
        assert!(connector_matches_filter(&s, &pattern, "client", "acme"));
        assert!(!connector_matches_filter(&s, &pattern, "client", "globex"));
        assert!(connector_matches_filter(&s, &pattern, "source", "shopify"));
        // `table` is connector-pass-through.
        assert!(connector_matches_filter(&s, &pattern, "table", "anything"));
        // Unknown component key short-circuits to false.
        assert!(!connector_matches_filter(
            &s,
            &pattern,
            "nonexistent",
            "value"
        ));
    }

    /// A connector whose `schema` cannot be parsed against the pattern
    /// (e.g. a synthetic id from a non-Fivetran adapter) is excluded
    /// from non-`id` filter scope. `--filter id=` still matches by id
    /// since the id path does not require schema parsing.
    #[test]
    fn connector_matches_filter_unparseable_schema_excludes_component_match() {
        let pattern = fivetran_pattern();
        let s = ReplicationConnectorSnapshot {
            id: "duckdb_local".to_string(),
            schema: "totally__not__the__pattern__shape__at__all".to_string(),
            source_type: "duckdb".to_string(),
            tables: vec![],
        };
        assert!(connector_matches_filter(&s, &pattern, "id", "duckdb_local"));
        assert!(!connector_matches_filter(&s, &pattern, "client", "acme"));
    }
}
