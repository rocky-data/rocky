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
use rocky_core::config::{PolicyCapability, PolicyEffect, PolicyPrincipal, StateBackend};
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
    runtime_principal: PolicyPrincipal,
    output_json: bool,
) -> Result<()> {
    let cwd = std::env::current_dir().context("failed to get current working directory")?;
    run_apply_in(
        &cwd,
        config_path,
        plan_id,
        state_path,
        runtime_principal,
        output_json,
    )
    .await
}

/// Inner implementation — takes an explicit `root` for the plans directory so
/// tests can pass a temp dir without touching the process-global cwd.
///
/// `runtime_principal` is the apply-time identity (`ROCKY_PRINCIPAL` resolved by
/// the CLI) — the enforcement source. Each per-kind seam combines it
/// most-restrictively with the plan's kind-forced principal (see
/// [`PersistedPlan::enforcement_principal`]); the plan's stored `principal`
/// field is never trusted for a gate decision.
pub(crate) async fn run_apply_in(
    root: &Path,
    config_path: &Path,
    plan_id: &str,
    state_path: &Path,
    runtime_principal: PolicyPrincipal,
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
            run_apply_run_plan(
                root,
                config_path,
                plan_id,
                state_path,
                runtime_principal,
                output_json,
            )
            .await
        }
        PlanKind::Replication => {
            run_apply_replication_plan(
                root,
                config_path,
                plan_id,
                state_path,
                runtime_principal,
                output_json,
            )
            .await
        }
        PlanKind::Promote => {
            run_apply_promote_plan(
                root,
                config_path,
                plan_id,
                state_path,
                runtime_principal,
                output_json,
            )
            .await
        }
        PlanKind::AiAuthored => {
            run_apply_ai_authored_plan(
                root,
                config_path,
                plan_id,
                state_path,
                runtime_principal,
                output_json,
            )
            .await
        }
        PlanKind::Backfill => {
            run_apply_backfill_plan(
                root,
                config_path,
                plan_id,
                state_path,
                runtime_principal,
                output_json,
            )
            .await
        }
        PlanKind::Gc => {
            super::gc::run_gc_apply_in(
                root,
                config_path,
                plan_id,
                state_path,
                runtime_principal,
                output_json,
            )
            .await
        }
        PlanKind::Restore => {
            super::restore::run_restore_apply_in(
                root,
                config_path,
                plan_id,
                state_path,
                runtime_principal,
                output_json,
            )
            .await
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
    runtime_principal: PolicyPrincipal,
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

    // policy seam 2: an agent running `rocky apply` (`ROCKY_PRINCIPAL=agent`) is
    // gated as agent; a human applier resolves to human (humans are ungated in
    // v0). Enforcement uses the apply-time runtime principal, not the plan's
    // stored (tamperable) field. Absent `[policy]` this is a no-op.
    let models_dir = Path::new(run_plan.models_dir.as_deref().unwrap_or("models"));
    // THE single fingerprinted config snapshot for this apply (#1120): the
    // replication-only check, the pre-gate sync decision, the policy gate,
    // `execute_run_plan`'s preflight/seam reads, AND `run()`'s execution all
    // read THIS instance, so a `rocky.toml` swap timed anywhere between the
    // gate and execution cannot redirect what runs.
    //
    // Deliberate behavior delta: this load is HARD (formerly a `.ok()`
    // soft-load) — an unloadable config now fails the apply at the gate
    // instead of failing later inside `run()`'s own load. A config that will
    // not load could never execute anyway; failing here keeps the gate and
    // the executor reading one snapshot.
    let loaded = std::sync::Arc::new(
        rocky_core::config::load_rocky_config_fingerprinted(config_path).with_context(|| {
            format!(
                "refusing to apply plan '{plan_id}': failed to load config from {}",
                config_path.display()
            )
        })?,
    );
    // Gate on the models this apply will ACTUALLY execute (fresh compile +
    // `--model` selection), not the plan's informational `models` list.
    // Finding #1: a replication-only plan runs NO models, so it gates none.
    let executable = if is_replication_only(&loaded.config, &run_plan) {
        Vec::new()
    } else {
        run_executable_models(models_dir, &run_plan)
    };
    let touched = touched_models_for_run(&plan, &executable);
    let principal = plan.enforcement_principal(runtime_principal);
    // Finding 4-apply: pull the authoritative remote freeze/budget ledger before
    // the gate reads it, so a cross-pod freeze is enforced (fail-closed). Skipped
    // when the gate won't read the ledger (no policy / empty touched — finding 8).
    sync_remote_ledger_before_gate(&loaded.config, state_path, &touched).await?;
    let gate = evaluate_apply_policy_with_policy(
        loaded.config.policy.as_ref(),
        plan_id,
        principal,
        &touched,
        models_dir,
        state_path,
    );
    apply_policy_gate(root, plan_id, gate)?;

    // Resolve the post-apply verification checks *before* the run plan is moved
    // into execution (the run plan owns the models_dir the resolver reads).
    let verify_checks = required_verify_after(
        loaded.config.policy.as_ref(),
        principal,
        &touched,
        models_dir,
    );

    // Finding 3 (pre-run half): the plain rule decision the gate just recorded
    // must survive `run`'s start-download, which REPLACES the local ledger from
    // remote. Push it to remote NOW so `run`'s download pulls it back — otherwise
    // the budget-burn PAIR (rule decision + verify-after custody) never both reach
    // remote and a failed apply doesn't burn the budget. Only when a verify-after
    // requirement exists (the budget-relevant case).
    if !verify_checks.is_empty() {
        upload_remote_ledger_fail_closed(
            Some(&loaded.config),
            state_path,
            "governed rule decision",
        )
        .await?;
    }

    // One unique id for this apply's run, threaded into execution and into the
    // post-apply gate so the gate reads exactly this run, not "latest".
    let apply_run_id = new_apply_run_id();
    // Governance context — only for an AGENT apply (humans are ungated in v0, so
    // their apply behaviour stays byte-identical). Carries the plan-authorized
    // models fingerprint for the in-run TOCTOU reject and the identity for the
    // post-discovery replication gate.
    let governed = governed_run_context(
        &plan,
        principal,
        plan_id,
        root,
        config_path,
        !run_plan.models.is_empty(),
    );
    execute_run_plan(
        config_path,
        std::sync::Arc::clone(&loaded),
        plan_id,
        run_plan,
        state_path,
        output_json,
        &apply_run_id,
        governed.as_ref(),
    )
    .await?;
    let verify_result = run_verify_after(
        plan_id,
        principal,
        &verify_checks,
        &apply_run_id,
        state_path,
    );
    // Finding 3 (post-verify half): the verify-after custody row `run_verify_after`
    // just wrote lands AFTER `run`'s end-upload, so push it to remote (fail-closed)
    // even when verification FAILED — the failure-custody row is the budget-burning
    // half of the pair. Upload before propagating the verify result.
    if !verify_checks.is_empty() {
        upload_remote_ledger_fail_closed(Some(&loaded.config), state_path, "verify-after custody")
            .await?;
    }
    verify_result
}

/// Build the [`GovernedRunContext`] for a two-step apply — `Some` only for an
/// AGENT principal (a human apply is ungated in v0 and stays byte-identical).
fn governed_run_context<'a>(
    plan: &PersistedPlan,
    principal: PolicyPrincipal,
    plan_id: &'a str,
    root: &'a Path,
    config_path: &'a Path,
    // `!run_plan.models.is_empty()` — the plan reviewed a non-empty model set
    // (finding #2, threaded to the executor fail-closed legs).
    expects_models: bool,
) -> Option<GovernedRunContext<'a>> {
    if principal != PolicyPrincipal::Agent {
        return None;
    }
    let embedded = plan.embedded_capabilities();
    Some(GovernedRunContext {
        principal,
        plan_id,
        root,
        config_path,
        expects_models,
        expected_ir_fingerprint: embedded.models_fingerprint,
        expected_config_identity: embedded.config_identity,
        // A `fingerprint_version >= 1` plan is a NEW plan this binary wrote and
        // MUST carry the exec fingerprint + routing identity; version 0 is
        // genuinely legacy (skip). This is DELIBERATELY `>= 1`, NOT
        // `>= CURRENT_FINGERPRINT_VERSION`: gating on CURRENT (now 2) would make
        // v1 plans stop requiring the fingerprint/routing identity — a regression
        // (finding #2). The v2-only source-snapshot requirement is a SEPARATE
        // preflight (`preflight_snapshot`), not folded into this flag.
        require_fingerprint: embedded.fingerprint_version >= 1,
        reviewed_source_schemas: embedded.reviewed_source_schemas,
    })
}

/// The resolved pipeline is a **Replication** pipeline. Any resolution failure ⇒
/// `false` — the fail-safe for both consumers: the mask omits (harmless, apply
/// never checks the gate on a non-reaching path), and the execution-shape
/// carve-outs below default to STRICT (a non-replication plan is never
/// replication-only, so it keeps full policy gating + the snapshot requirement).
/// Uses the SAME `registry::resolve_pipeline` `commands::run::run` uses at apply.
pub(crate) fn pipeline_is_replication(
    cfg: &rocky_core::config::RockyConfig,
    pipeline_name: Option<&str>,
) -> bool {
    crate::registry::resolve_pipeline(cfg, pipeline_name)
        .map(|(_, p)| matches!(p, rocky_core::config::PipelineConfig::Replication(_)))
        .unwrap_or(false)
}

/// A governed `Run` plan that, at apply, executes NO compiled models — a
/// Replication pipeline with no `--all`, no `--models`, and no `--model` (its
/// model leg at `run.rs`'s `run_all || models_dir.is_some()` is never entered,
/// and it is not the `--model` path). Finding #1 (regression): such a plan must
/// NOT preflight the snapshot and must NOT bind policy over models it never runs.
///
/// A **safe carve-out** (not a positive "executes models"): a resolution failure
/// or any non-Replication pipeline (Transformation runs models via `run_local`
/// unconditionally) is NOT replication-only ⇒ stays strict (snapshot required +
/// full policy gating). So a wrong answer can only ever OVER-gate (fail-closed),
/// never under-gate.
///
/// This carve-out **cannot become a policy bypass**, even against a config swapped
/// to replication-only between plan and apply. Skipping the *model* policy gate is
/// harmless because the models never execute on this path; and the replication
/// mutation the plan *does* run is independently fail-closed at `run.rs`'s governed
/// replication gate, which fires BEFORE any warehouse statement:
/// [`verify_routing_identity`] refuses when the fresh config's routing identity ≠
/// the plan's authorized `config_identity` (so a transformation→replication swap is
/// caught), and [`gate_replication_targets`] re-evaluates the policy plane over the
/// concrete discovered targets. Reading the pipeline type from a fresh
/// `load_rocky_config` at apply is therefore safe in BOTH directions: a won't-load
/// config → strict, and a swapped-to-replication config → caught by routing
/// identity. The only inert case is a genuinely-legacy v0 plan (no identity,
/// `!require_fingerprint`) — the pre-existing legacy exemption this carve-out does
/// not widen.
fn is_replication_only(cfg: &rocky_core::config::RockyConfig, run_plan: &RunPlan) -> bool {
    pipeline_is_replication(cfg, run_plan.pipeline.as_deref())
        && !run_plan.run_all
        && run_plan.models_dir.is_none()
        && run_plan.model.is_none()
}

/// Fail-closed preflight (finding #2) for a MODEL-EXECUTING governed apply: it
/// must carry the v2 reviewed source-schema snapshot (`Some`, even empty), run
/// **before any warehouse mutation** (before replication discovery/DDL). A v1
/// model-executing plan's fingerprint hashes only config+SQL — not
/// `typed_columns` — so typing from the live cache at apply is a TOCTOU; force a
/// re-plan at v2 that captures the snapshot. A v2 plan whose snapshot could not
/// be captured (`None`) is a production failure → refuse. Genuinely-legacy v0
/// plans (`require_fingerprint == false`) and human applies (no context) are
/// exempt. `executes_models == false` (a replication-only plan) is exempt — it
/// runs no models, so there is no typed-columns TOCTOU to close (finding #1).
fn preflight_snapshot(
    governed_ctx: Option<&GovernedRunContext<'_>>,
    plan_id: &str,
    executes_models: bool,
) -> Result<()> {
    if executes_models
        && let Some(ctx) = governed_ctx
        && ctx.require_fingerprint
        && ctx.reviewed_source_schemas.is_none()
    {
        bail!(
            "refusing to apply governed plan '{plan_id}': it executes models but carries no \
             reviewed source-schema snapshot (a v1 plan, or a v2 plan whose snapshot could not be \
             captured), so `typed_columns` would be typed from the live cache at apply — a TOCTOU \
             the fingerprint does not cover. Re-plan with `rocky plan` (which captures the snapshot \
             at fingerprint v2) before applying."
        );
    }
    Ok(())
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
// Mirrors `commands::run::run`'s own allow: the run-plan replay surface is
// wide by design, and the threaded snapshot is one more load-bearing arg.
#[allow(clippy::too_many_arguments)]
async fn execute_run_plan(
    config_path: &Path,
    // The caller's ONE fingerprinted config snapshot (#1120): the same
    // instance the apply gate read. The preflight/seam reads below and the
    // `run()` execution all use it — this function performs NO config load
    // of its own (its former `.ok()` re-load was a swap window between the
    // gate and execution).
    loaded: std::sync::Arc<rocky_core::config::LoadedConfig>,
    plan_id: &str,
    run_plan: RunPlan,
    state_path: &Path,
    output_json: bool,
    // The unique run_id this apply forces `run` to record under, so the
    // post-apply `verify_after` gate can resolve *this apply's own* run by id
    // (see [`run_verify_after`]). The `--dag` early-return below does not thread
    // it — a `dag` apply that also carries `verify_after` fails closed there
    // because no run is recorded under this id.
    apply_run_id: &str,
    // Governance context (agent apply): the in-run TOCTOU models-drift reject +
    // post-discovery replication gate. `None` for a human apply.
    governed_ctx: Option<&GovernedRunContext<'_>>,
) -> Result<()> {
    // ‼️ Finding #2/#1: preflight the reviewed source-schema snapshot BEFORE any
    // warehouse mutation — this path executes models (and, for a replication
    // pipeline, does replication discovery/DDL first), so a v1/missing-snapshot
    // plan must be refused here rather than after the first statement runs. A
    // replication-only plan (no `--all`/`--models`/`--model`) executes NO models
    // → exempt. Reads the caller's threaded snapshot — the config is always
    // loaded here (the caller hard-loaded it), so the former unloadable-config
    // "stay strict" fallback is unreachable by construction.
    let executes_models = !is_replication_only(&loaded.config, &run_plan);
    preflight_snapshot(governed_ctx, plan_id, executes_models)?;

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
        // Fail-closed (D): the DAG runner dispatches sub-runs with NO governance
        // context, so a governed (agent) DAG apply would execute every pipeline
        // — including replication — UNGATED. Refuse loudly rather than run
        // ungated until DAG sub-runs thread the governance context.
        if governed_ctx.is_some() {
            bail!(
                "refusing to apply plan '{plan_id}' as an agent: a `--dag` apply is not yet \
                 policy-gated (its sub-runs execute ungated). Re-plan without `--dag`, or have a \
                 human apply it."
            );
        }
        // A stored run plan predates the build-escape-hatch flags (they were
        // never captured into the plan), so the DAG replay uses defaults —
        // `--force-rebuild` / `--no-reuse` are not part of a persisted plan's
        // contract.
        return crate::commands::run_with_dag(
            config_path,
            state_path,
            output_json,
            &crate::commands::run::SkipRunOptions::default(),
        )
        .await
        .with_context(|| format!("rocky apply run plan '{plan_id}' failed (dag path)"));
    }

    // ‼️ Finding #2 (missing-dir), REPLICATION seam: a governed non-dag apply whose
    // plan REVIEWED a non-empty silver-model set, but whose models directory no
    // longer compiles to any model (deleted / renamed / broken since the plan),
    // must FAIL CLOSED here — otherwise the replication path's model leg silently
    // skips (`if mdir.exists()` at run.rs), the execution fingerprint is never
    // recomputed, and apply reports SUCCESS without executing the planned models.
    // Recompile the exact dir at the seam, BEFORE the replication DDL runs (the
    // first warehouse mutation; the `partition_opts`/`shadow_config` setup above is
    // read-only). Placed AFTER the `--dag` guard so a governed dag apply keeps its
    // categorical refusal (that path never reaches here).
    //
    // SCOPED to a Replication pipeline: replication resolves its silver-model leg
    // cwd-relative (`run.rs`'s `models_dir.unwrap_or("models")`), which is exactly
    // what we recompile here, so seam and executor agree on the path. The
    // TRANSFORMATION path is NOT checked at this seam — `run_local` resolves
    // `config_dir.join(models_base)` from the pipeline config (ignoring
    // `run_plan.models_dir`), so a seam keyed on `run_plan.models_dir` would check a
    // DIFFERENT directory and could false-refuse a valid apply; transformation is
    // instead fail-closed at its OWN executor (`run_local`, which is its first
    // mutation anyway). Scoped to a non-empty reviewed set so a valid pure-
    // replication run with no silver `models/` is not failed; and the executor legs
    // (guarded by `expects_models`) close the residual seam→execute race.
    if governed_ctx.is_some()
        && executes_models
        && !run_plan.models.is_empty()
        && pipeline_is_replication(&loaded.config, run_plan.pipeline.as_deref())
    {
        let mdir = models_dir_path
            .clone()
            .unwrap_or_else(|| std::path::PathBuf::from("models"));
        let compiles_to_models =
            rocky_compiler::compile::compile(&rocky_compiler::compile::CompilerConfig {
                models_dir: mdir.clone(),
                ..Default::default()
            })
            .map(|r| !r.project.models.is_empty())
            .unwrap_or(false);
        if !compiles_to_models {
            bail!(
                "refusing to apply governed plan '{plan_id}': its reviewed models directory '{}' no \
                 longer compiles to any model (deleted, renamed, or broken since the plan), so the \
                 {} planned model(s) cannot be executed or re-fingerprinted — apply would otherwise \
                 skip them and report success. Re-plan with `rocky plan` before applying.",
                mdir.display(),
                run_plan.models.len()
            );
        }
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
        // Execute-from-owned: `run` executes the SAME snapshot the apply gate
        // verified — no internal re-load a timed swap could redirect (#1120).
        loaded,
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
        // Force `run` to record under this apply's unique id so the
        // post-apply `verify_after` gate resolves this run (not "latest").
        Some(apply_run_id),
        // Governance context: the in-run TOCTOU reject + replication gate.
        governed_ctx,
        // `--assume-fresh-state` is a `rocky run` runtime flag, never part of
        // a persisted plan — the two-step apply path always runs without it.
        false,
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
// agent-policy plane — apply/promote enforcement (seams 2 & 3)
// ---------------------------------------------------------------------------

/// Mint a unique run_id for a two-step `rocky apply` so the run it drives
/// records under an id no concurrent run can share. The post-apply
/// `verify_after` gate resolves *this* id (see [`run_verify_after`]), which is
/// what makes the gate immune to a sibling run finishing in between and being
/// mistaken for this apply's run.
fn new_apply_run_id() -> String {
    format!("run-apply-{}", uuid::Uuid::new_v4())
}

/// A resolved agent-policy decision, aggregated most-restrictive across every
/// model a plan touches.
///
/// Produced by [`evaluate_apply_policy`] at each mutating enforcement point —
/// `rocky apply`, promote, and the MCP `propose` gate — so all three share one
/// per-model evaluation and one aggregation rule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolicyGate {
    /// No `[policy]` block in the config — the evaluator was never
    /// constructed. The caller falls back to its pre-policy-plane behaviour
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
        // Transitive blast radius for the `max_downstreams` ceiling. `None`
        // when the model is absent from the compiled graph (fails closed).
        let reachable_downstreams = super::audit::blast_radius_of(&result, &name)
            .map(|(_direct, transitive)| transitive.len() as u64);
        out.insert(
            name.clone(),
            ModelAttributes {
                name,
                tags: model.config.tags.clone(),
                classifications,
                layer,
                contracted,
                downstreams,
                reachable_downstreams,
            },
        );
    }
    out
}

/// Resolve the `[state]` backend for a governed apply seam, returning the config
/// ONLY when the pre-gate remote sync is actually warranted — i.e. all of:
///
/// - `touched` is **non-empty** (an empty set short-circuits
///   [`evaluate_apply_policy`] to `Allow` with NO ledger read), AND
/// - a `[policy]` block is configured (`cfg.policy.is_some()`; without one the
///   gate returns `NotConfigured` and reads NO freeze ledger), AND
/// - the backend is REMOTE (Local needs no transfer).
///
/// When the gate will not consult the `policy_decisions` ledger, pulling remote
/// state buys nothing and its fail-closed abort would be a pure availability
/// regression for a valid human Run/Promote plan on a backend blip (finding 8) —
/// so this returns `None` and the caller skips the download entirely.
///
/// Takes an ALREADY-LOADED config snapshot (finding A: the guard and the gate
/// must see the SAME config, so the caller loads once and threads it into both).
fn remote_state_backend_for_gate(
    cfg: &rocky_core::config::RockyConfig,
    touched: &BTreeMap<String, PolicyCapability>,
) -> Option<rocky_core::config::StateConfig> {
    // Empty touched set ⇒ the gate is a no-op Allow, no ledger read.
    if touched.is_empty() {
        return None;
    }
    // No `[policy]` block ⇒ the gate returns NotConfigured, no ledger read.
    cfg.policy.as_ref()?;
    let state_cfg = cfg.state.clone();
    (!matches!(state_cfg.backend, StateBackend::Local)).then_some(state_cfg)
}

/// Download the authoritative remote `[state]` ledger before a governed policy
/// gate reads it (async caller sites).
///
/// The freeze/budget decisions [`evaluate_apply_policy`] consults live in the
/// `policy_decisions` ledger that `rocky run` downloads at start and uploads at
/// end. A governed apply gates BEFORE any such run-download, so without this a
/// freeze recorded by another pod would be invisible and the gate would clear
/// against a stale local snapshot (finding 4-apply). No-op unless the gate will
/// actually read the ledger (see [`remote_state_backend_for_gate`] — finding 8).
/// Fail-closed: when the sync IS warranted, a remote download failure aborts.
///
/// Takes the SAME `cfg` snapshot the caller passes to
/// [`evaluate_apply_policy_with_policy`], so the sync decision and the gate can
/// never disagree about `[policy]` presence (finding A). `pub(crate)` so the
/// `restore` seam reuses the exact same guarded, fail-closed pre-gate sync
/// (finding 1).
pub(crate) async fn sync_remote_ledger_before_gate(
    cfg: &rocky_core::config::RockyConfig,
    state_path: &Path,
    touched: &BTreeMap<String, PolicyCapability>,
) -> Result<()> {
    let Some(state_cfg) = remote_state_backend_for_gate(cfg, touched) else {
        return Ok(());
    };
    // WP-01 PR-B (2b): the session half-seam owns the download shape; a
    // successful download of either usable variant means the local ledger now
    // mirrors remote truth; failure still `?`-bails fail-closed (unchanged).
    let _authority =
        rocky_core::state_sync::RemoteStateSession::download_only(&state_cfg, state_path)
            .await
            .with_context(|| {
                "failed to download remote state before the agent-policy gate; a remote-backend \
             governed apply requires the state backend reachable so a cross-pod freeze/budget \
             decision is enforced"
            })?;
    Ok(())
}

/// Blocking sibling of [`sync_remote_ledger_before_gate`] for the SYNC promote
/// gate ([`gate_promote_plan`]), which is reached from two async entry points
/// (`rocky apply <promote>` and `rocky branch promote --plan`). Driving the
/// download on a dedicated runtime inside the shared gate closes BOTH entry
/// points without threading an async download through each caller. Same
/// finding-8 guard: skips the download unless the gate will read the ledger.
/// Takes the same `cfg` snapshot used for the gate (finding A).
fn sync_remote_ledger_before_gate_blocking(
    cfg: &rocky_core::config::RockyConfig,
    state_path: &Path,
    touched: &BTreeMap<String, PolicyCapability>,
) -> Result<()> {
    let Some(state_cfg) = remote_state_backend_for_gate(cfg, touched) else {
        return Ok(());
    };
    // WP-01 PR-B (2b): half-seam download — see `sync_remote_ledger_before_gate`.
    let _authority = crate::commands::policy::block_on_state_sync(
        rocky_core::state_sync::RemoteStateSession::download_only(&state_cfg, state_path),
    )
    .with_context(|| {
        "failed to download remote state before the promote policy gate; a remote-backend \
         governed promote requires the state backend reachable so a cross-pod freeze/budget \
         decision is enforced"
    })?;
    Ok(())
}

/// Download the remote `[state]` ledger UNCONDITIONALLY for a remote backend
/// (fail-closed), regardless of `[policy]` presence.
///
/// Used by seams that read a REPLICATED ledger the policy guard does not cover —
/// `restore` reads `TOMBSTONES`, `backfill` writes on top of the artifact/run
/// ledger (finding 2). Gating the download behind the policy guard would make a
/// no-`[policy]` restore read STALE local tombstones and falsely refuse. No-op
/// for the Local backend or an unloadable config.
pub(crate) async fn download_remote_ledger_unconditional(
    cfg: Option<&rocky_core::config::RockyConfig>,
    state_path: &Path,
    context_label: &str,
) -> Result<()> {
    let Some(cfg) = cfg else {
        return Ok(());
    };
    if matches!(cfg.state.backend, StateBackend::Local) {
        return Ok(());
    }
    // WP-01 PR-B (2b): half-seam download — see `sync_remote_ledger_before_gate`.
    let _authority =
        rocky_core::state_sync::RemoteStateSession::download_only(&cfg.state, state_path)
            .await
            .with_context(|| {
                format!(
                    "failed to download remote state before {context_label}; a remote-backend \
                 {context_label} requires the state backend reachable to read the authoritative \
                 ledger"
                )
            })?;
    Ok(())
}

/// Upload the local `[state]` ledger to a REMOTE backend, FAIL-CLOSED
/// (`on_upload_failure = Fail`), so a governed ledger mutation is durable.
///
/// Used for the restore/backfill upload-after (finding 2) and to make the
/// budget-burn decision pair (rule decision + verify-after custody) reach remote
/// (finding 3). No-op for the Local backend or an unloadable config.
pub(crate) async fn upload_remote_ledger_fail_closed(
    cfg: Option<&rocky_core::config::RockyConfig>,
    state_path: &Path,
    context_label: &str,
) -> Result<()> {
    let Some(cfg) = cfg else {
        return Ok(());
    };
    if matches!(cfg.state.backend, StateBackend::Local) {
        return Ok(());
    }
    // WP-01 PR-B (2b): the half-seam owns the forced-`Fail` durability policy.
    rocky_core::state_sync::RemoteStateSession::upload_only_fail_closed(
        &cfg.state,
        state_path,
        context_label,
    )
    .await
    .with_context(|| {
        format!("failed to upload remote state after {context_label} (fail-closed)")
    })?;
    Ok(())
}

/// Evaluate the agent-policy plane over a plan's touched `(model, capability)`
/// set and aggregate the most-restrictive effect. Records one
/// [`PolicyDecisionRecord`] per evaluation to the ledger (best-effort — an
/// audit-write failure never fails the caller; the *gate* is the safety
/// boundary, the ledger is the trail).
///
/// Shared by the mutating enforcement points — `rocky apply`, promote, and the
/// MCP `propose` gate — so each evaluates the same per-model rules with the same
/// aggregation and records to the same ledger. `plan_id` is the id the decision
/// is recorded against (the propose gate passes a deterministic id it may not
/// persist).
///
/// `touched` maps each governed model to the capability that was reviewed at
/// propose time (the embedded classification, or `schema_change.breaking` when
/// the classification was unavailable / fail-closed). An empty map means the
/// plan executes **no models** — a genuine no-op → `Allow`. A no-change plan
/// that still executes models is NOT empty: `EmbeddedCapabilities::touched`
/// synthesizes a bare-`apply` entry per planned model, so its execution stays
/// governed (do not pass an empty map for an executing plan or the gate is
/// bypassed).
pub fn evaluate_apply_policy(
    config_path: &Path,
    plan_id: &str,
    principal: PolicyPrincipal,
    touched: &BTreeMap<String, PolicyCapability>,
    models_dir: &Path,
    state_path: &Path,
) -> PolicyGate {
    // Load the config here for callers that don't already hold a snapshot (gc,
    // the MCP propose gate, tests). Governed apply/restore/promote paths call
    // [`evaluate_apply_policy_with_policy`] with the SAME snapshot they used for
    // the pre-gate sync decision, so the sync-guard and this gate can never
    // disagree about whether `[policy]` is configured (finding A — config-snapshot
    // TOCTOU).
    let policy = rocky_core::config::load_rocky_config(config_path)
        .ok()
        .and_then(|cfg| cfg.policy);
    evaluate_apply_policy_with_policy(
        policy.as_ref(),
        plan_id,
        principal,
        touched,
        models_dir,
        state_path,
    )
}

/// [`evaluate_apply_policy`] over an ALREADY-RESOLVED `[policy]` block, rather
/// than reloading the config from disk.
///
/// The governed paths (run-apply, ai-authored, backfill, promote, restore) load
/// ONE immutable config snapshot and thread its `policy` into both the pre-gate
/// remote-state sync decision AND this evaluation, so a config that gains a
/// `[policy]` block between the two can't make the guard skip the sync while this
/// gate then reads stale local decisions (finding A).
pub fn evaluate_apply_policy_with_policy(
    policy: Option<&rocky_core::config::PolicyConfig>,
    plan_id: &str,
    principal: PolicyPrincipal,
    touched: &BTreeMap<String, PolicyCapability>,
    models_dir: &Path,
    state_path: &Path,
) -> PolicyGate {
    let (policy, attrs_map) = match resolve_policy_and_attrs(policy, touched, models_dir) {
        Ok(pair) => pair,
        Err(gate) => return gate,
    };

    // Snapshot the decision ledger *before* this apply writes any rows, so the
    // dynamic breakers (autonomy-budget burn, active freezes) reflect only
    // prior history. The snapshot is taken through `open_read_only` FIRST:
    // readers skip the advisory write lock (see rocky-core/tests/state_lock.rs),
    // so a concurrent run holding the writer lock cannot blind the
    // freeze/budget projection. The reader handle is dropped before the write
    // handle below is opened — redb forbids two live handles on one file
    // within a process.
    let prior_snapshot: Option<Vec<PolicyDecisionRecord>> = StateStore::open_read_only(state_path)
        .ok()
        .and_then(|reader| reader.list_policy_decisions().ok());

    // Ledger write handle, opened with a bounded retry on transient advisory-lock
    // contention. Finding 6 (scoped, red-team round 6): budget / `verify_after`
    // durability is enforced PER WINNING RULE in the record sink below — a dropped
    // decision row for a rule that actually governs a touched target fails closed,
    // while an ordinary audit-row hiccup never blocks the apply. So an UNRELATED
    // budget rule for a different target no longer forces a false-deny.
    let ledger = open_ledger_with_retry(state_path).ok();

    // Rare fallback: the read-only open lost a transient redb open race but a
    // write handle succeeded — snapshot through it rather than reading nothing.
    let prior_snapshot =
        prior_snapshot.or_else(|| ledger.as_ref().and_then(|s| s.list_policy_decisions().ok()));

    let snapshot_unreadable = prior_snapshot.is_none();
    let prior_decisions: Vec<PolicyDecisionRecord> = prior_snapshot.unwrap_or_default();

    // Tracks a failed budget-relevant decision-row write so we can fail closed
    // after the evaluation loop (the record sink returns `()`).
    let budget_write_failed = std::cell::Cell::new(false);

    let gate = evaluate_apply_policy_core(
        &policy,
        plan_id,
        principal,
        touched,
        &attrs_map,
        &prior_decisions,
        snapshot_unreadable,
        |record| {
            // Durability is critical only when THIS record's WINNING rule uses an
            // autonomy budget or `verify_after` — its decision row pairs with a
            // later custody row to burn the budget. A rule that did not win for any
            // touched target produces no record here, so an unrelated budget rule
            // never forces fail-closed handling (fixes the round-6 false-deny).
            let rec_budget_relevant = record
                .rule_id
                .and_then(|idx| policy.rules.get(idx))
                .map(|r| r.autonomy_budget.is_some() || !r.verify_after.is_empty())
                .unwrap_or(false);
            match &ledger {
                Some(store) => {
                    if let Err(e) = store.record_policy_decision(record) {
                        if rec_budget_relevant {
                            budget_write_failed.set(true);
                            warn!(
                                target: "rocky::policy",
                                error = %e,
                                "fail-closed: could not persist a budget-relevant decision row"
                            );
                        } else {
                            warn!(
                                target: "rocky::policy",
                                error = %e,
                                "failed to record policy decision to the ledger (continuing)"
                            );
                        }
                    }
                }
                // No write handle: fail closed only if this winning rule is
                // itself budget / verify_after-relevant.
                None if rec_budget_relevant => budget_write_failed.set(true),
                None => {}
            }
        },
    );

    if budget_write_failed.get() {
        return fail_closed_budget_gate(
            touched,
            "a budget-relevant decision row could not be persisted",
        );
    }
    gate
}

/// Open the decision ledger for writing with a bounded retry on transient
/// advisory-lock contention (a concurrent run briefly holding the writer lock).
/// Mirrors the download-publish lock retry. Fail-closed: propagates the last
/// error after the retries are exhausted.
fn open_ledger_with_retry(state_path: &Path) -> Result<StateStore, rocky_core::state::StateError> {
    use rocky_core::state::StateError;
    const MAX_ATTEMPTS: u32 = 5;
    let mut last: Option<StateError> = None;
    for attempt in 1..=MAX_ATTEMPTS {
        match StateStore::open(state_path) {
            Ok(store) => return Ok(store),
            Err(e) => {
                // Only retry TRANSIENT advisory-lock contention (a concurrent
                // writer briefly holding the lock). A permanent error (schema
                // mismatch, version parse, lock I/O, a corrupt file, …) is returned
                // immediately — retrying it merely delays the correct failure
                // (red-team round 6).
                let transient = matches!(
                    e,
                    StateError::LockHeldByOther { .. } | StateError::Busy { .. }
                );
                last = Some(e);
                if !transient || attempt == MAX_ATTEMPTS {
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(20 * u64::from(attempt)));
            }
        }
    }
    Err(last.expect("retry loop runs at least once"))
}

/// A fail-closed `Deny` for a budget-relevant decision whose ledger row could
/// not be durably persisted — the autonomy-budget / verify_after pair must be
/// durable, so the mutation is refused rather than proceeding with an incomplete
/// budget trail (finding 6).
fn fail_closed_budget_gate(touched: &BTreeMap<String, PolicyCapability>, why: &str) -> PolicyGate {
    PolicyGate::Deny {
        model: touched
            .keys()
            .next()
            .cloned()
            .unwrap_or_else(|| "*".to_string()),
        rule_id: None,
        reason: format!(
            "fail-closed: {why} — the autonomy-budget / verify_after decision pair must be \
             durable so a failed apply burns the budget, so this mutation is refused. Retry when \
             the state store is not contended."
        ),
    }
}

/// Ledger-through-a-held-handle variant of [`evaluate_apply_policy`].
///
/// The in-`run` replication gate (D) fires while `commands::run::run` already
/// holds an open write handle on the state store. Re-opening the same store
/// in-process would collide (`DatabaseAlreadyOpen` on the reader, advisory
/// `LockHeldByOther` on the writer) and spuriously mark the snapshot
/// unreadable — which the fail-closed D3 floor would then turn into a bogus
/// `deny` for *every* agent replication, even under `allow`. So the snapshot
/// and the decision-row writes both go through the caller's already-open
/// `ledger`, never a fresh open.
pub(crate) fn evaluate_apply_policy_with_store(
    policy: Option<&rocky_core::config::PolicyConfig>,
    plan_id: &str,
    principal: PolicyPrincipal,
    touched: &BTreeMap<String, PolicyCapability>,
    models_dir: &Path,
    ledger: &StateStore,
) -> PolicyGate {
    // Finding 1: takes the SAME `[policy]` snapshot `run` already holds (its L1212
    // `rocky_cfg`), not a reload — the in-run replication gate must evaluate the
    // config `run` executed against, not one a mid-run `rocky.toml` swap points at.
    let (policy, attrs_map) = match resolve_policy_and_attrs(policy, touched, models_dir) {
        Ok(pair) => pair,
        Err(gate) => return gate,
    };

    // Snapshot + record through the held handle. A genuine list error (real
    // corruption, not an in-process collision) still fails closed for agents.
    let prior = ledger.list_policy_decisions();
    let snapshot_unreadable = prior.is_err();
    let prior_decisions = prior.unwrap_or_default();

    evaluate_apply_policy_core(
        &policy,
        plan_id,
        principal,
        touched,
        &attrs_map,
        &prior_decisions,
        snapshot_unreadable,
        |record| {
            if let Err(e) = ledger.record_policy_decision(record) {
                warn!(
                    target: "rocky::policy",
                    error = %e,
                    "failed to record policy decision to the ledger (continuing)"
                );
            }
        },
    )
}

/// Given an ALREADY-RESOLVED `[policy]` block, compile the per-model attributes,
/// or return an early [`PolicyGate`] — `NotConfigured` when there is no policy,
/// `Allow` when `touched` is empty (a genuine no-op executes nothing).
fn resolve_policy_and_attrs(
    policy: Option<&rocky_core::config::PolicyConfig>,
    touched: &BTreeMap<String, PolicyCapability>,
    models_dir: &Path,
) -> std::result::Result<
    (
        rocky_core::config::PolicyConfig,
        BTreeMap<String, ModelAttributes>,
    ),
    PolicyGate,
> {
    let Some(policy) = policy else {
        return Err(PolicyGate::NotConfigured);
    };
    // An empty touched set means the plan executes no models (a genuine no-op).
    // A no-change-but-executing plan is never empty here — see the touched-set
    // synthesis in `EmbeddedCapabilities::touched`.
    if touched.is_empty() {
        return Err(PolicyGate::Allow);
    }
    Ok((policy.clone(), model_attributes(models_dir)))
}

/// The per-model evaluation loop shared by [`evaluate_apply_policy`] and
/// [`evaluate_apply_policy_with_store`]. The snapshot and the record sink are
/// supplied by the caller so the two variants differ only in how they reach the
/// ledger (fresh open vs. a held handle).
#[allow(clippy::too_many_arguments)]
fn evaluate_apply_policy_core(
    policy: &rocky_core::config::PolicyConfig,
    plan_id: &str,
    principal: PolicyPrincipal,
    touched: &BTreeMap<String, PolicyCapability>,
    attrs_map: &BTreeMap<String, ModelAttributes>,
    prior_decisions: &[PolicyDecisionRecord],
    // Fail-closed floor: when the snapshot is genuinely unreadable (a corrupt /
    // forward-incompatible store, or a real cross-process run holding a live
    // redb handle so even the read-only open exhausts its retries), an AGENT
    // mutation cannot be trusted — an active freeze / exhausted budget would be
    // invisible. It is HARD-REFUSED (deny), not `require_review` (which a
    // pre-existing review marker could satisfy and so bypass the freeze).
    snapshot_unreadable: bool,
    mut record: impl FnMut(&PolicyDecisionRecord),
) -> PolicyGate {
    let now = chrono::Utc::now();
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

        // Static base decision, then the dynamic ledger-aware tightening
        // (freeze → deny; exhausted budget → require_review). The post-step
        // never widens the base and never changes the winning rule.
        let decision = policy::evaluate(policy, principal, *capability, attrs);
        let (mut effect, degradation) = policy::autonomy_degradation(
            decision.effect,
            decision.matched_rule,
            policy,
            principal,
            attrs,
            prior_decisions,
            now,
        );
        let mut reason = decision.reason;
        if let Some(suffix) = degradation.reason_suffix() {
            reason.push_str("; ");
            reason.push_str(&suffix);
        }
        if snapshot_unreadable
            && principal == PolicyPrincipal::Agent
            && effect != PolicyEffect::Deny
        {
            effect = PolicyEffect::Deny;
            reason.push_str(
                "; policy ledger unreadable — freeze/budget state unverifiable, agent mutation \
                 refused (fail-closed deny; a review marker cannot satisfy it)",
            );
        }

        record(&PolicyDecisionRecord {
            timestamp: now,
            plan_id: plan_id.to_string(),
            principal,
            capability: *capability,
            model: model.clone(),
            effect,
            rule_id: decision.matched_rule,
            reason: reason.clone(),
            // A plain evaluation row carries no verify_after; the post-apply
            // verification writes its own custody row.
            verify_after: Vec::new(),
            // Ordinary apply/promote evaluation — no auto-apply custody.
            auto_apply: None,
        });

        let gate = match effect {
            PolicyEffect::Allow => PolicyGate::Allow,
            PolicyEffect::RequireReview => PolicyGate::RequireReview {
                model: model.clone(),
                rule_id: decision.matched_rule,
                reason,
            },
            PolicyEffect::Deny => PolicyGate::Deny {
                model: model.clone(),
                rule_id: decision.matched_rule,
                reason,
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
/// `executable_models` is the set the apply will actually execute — for
/// `Run` / `AiAuthored` it is re-derived from a fresh compile narrowed by the
/// plan's `--model` selection (see [`run_executable_models`]); for a
/// `Backfill` it is the composed rebuild closure (`run_plan.models`, which is
/// authoritative there). It is **never** the plan's informational `models`
/// list, which serde-defaults to empty and can over-list — gating on that is a
/// fail-open in both directions (see [`EmbeddedCapabilities::touched`]).
fn touched_models_for_run(
    plan: &PersistedPlan,
    executable_models: &[String],
) -> BTreeMap<String, PolicyCapability> {
    plan.embedded_capabilities().touched(executable_models)
}

/// Re-derive the set of models a `Run` / `AiAuthored` apply will actually
/// execute, the same way the run does: compile `models_dir` and keep only the
/// models the plan's `--model` selection targets (all of them when unset).
///
/// The plan's persisted `models` list is *informational* (serde-default,
/// re-derived at apply via recompile), so the gate must recompute the real
/// execution selection rather than trust it. A compile failure here yields an
/// empty set — the apply's own recompile will fail identically and execute
/// nothing, so there is nothing to gate.
fn run_executable_models(models_dir: &Path, run_plan: &RunPlan) -> Vec<String> {
    use rocky_compiler::compile::{self, CompilerConfig};
    let config = CompilerConfig {
        models_dir: models_dir.to_path_buf(),
        ..Default::default()
    };
    let Ok(result) = compile::compile(&config) else {
        return Vec::new();
    };
    result
        .project
        .models
        .iter()
        .map(|m| m.config.name.clone())
        .filter(|name| {
            run_plan
                .model
                .as_deref()
                .is_none_or(|target| target == name.as_str())
        })
        .collect()
}

/// The `(model, capability)` set the policy plane evaluates for a `Promote`
/// plan, gated under the bare `promote` verb.
///
/// The `promote` verb — not a `schema_change.*` refinement — is used so a
/// `deny agent promote {…}` rule governs promotions while an `apply`-scoped
/// rule does not accidentally fire on a promote (the refinements are shared
/// between the two verbs).
///
/// The gated set is the **full executable target set** — every target the
/// promote will `CREATE OR REPLACE` (`promote.targets`) unioned with every
/// target the breaking-change findings named. Gating only finding-scoped
/// targets was a fail-open: a `deny agent promote { models = ["X"] }` rule
/// scoped to a target that executes but produced no finding (an unchanged or
/// additive-only target) would silently not fire while its SQL ran. Each FQN
/// is mapped to its logical model name (via a compile of `models_dir`) so a
/// name-scoped rule matches; an FQN that cannot be mapped (project doesn't
/// compile, or the target vanished from the project) stays under its FQN
/// (fail-closed) rather than being dropped.
///
/// Only a plan with no targets **and** no findings — a promote that executes
/// nothing — yields an empty set.
fn touched_models_for_promote(
    promote: &PromotePlan,
    models_dir: &Path,
) -> BTreeMap<String, PolicyCapability> {
    // The full executable target set: every SQL target plus every target a
    // finding named (a finding target may, in principle, not appear in
    // `targets` — union both so nothing escapes).
    let mut target_fqns: BTreeSet<String> =
        promote.targets.iter().map(|t| t.target.clone()).collect();
    if let Some(findings) = promote.breaking_changes.as_ref() {
        for f in findings {
            target_fqns.insert(f.change.model().to_string());
        }
    }
    if target_fqns.is_empty() {
        return BTreeMap::new();
    }
    let target_to_name = compile_target_to_name(models_dir);
    target_fqns
        .into_iter()
        .map(|fqn| {
            // Map FQN → logical name so a name-scoped rule matches; fail-closed
            // to the FQN when unmappable rather than dropping the target.
            let name = target_to_name.get(&fqn).cloned().unwrap_or(fqn);
            (name, PolicyCapability::Promote)
        })
        .collect()
}

/// Canonical, process-stable fingerprint of the **compiled-IR projection** that
/// authorization is computed over — the sound close for the gate/execute
/// TOCTOU.
///
/// For each model in the compiled set, hashes a canonical projection =
/// `serde_json::to_value(&ModelConfig)` (which normalises any nested map to
/// sorted-key order — serde_json's `Map` is a `BTreeMap` without the
/// `preserve_order` feature, so this is process-stable across two runs) plus the
/// compiled SQL, assembled **name-sorted** in a `BTreeMap`. Combined with the
/// env-resolved, secret-free config identity ([`config_policy_identity`]), which
/// catches an adapter/target swap with identical model bytes.
///
/// It hashes the **IR**, never file/dir bytes — so it EXCLUDES the state file,
/// locks, namespaced state, caches, and every runtime artifact. (The round-3
/// file-hash attempt hashed `models_dir`, which contains `.rocky-state.redb` —
/// self-invalidating, refuse-everything. This is the fix.)
///
/// `None` only when a model config fails to serialize (never expected in
/// practice); the governed check treats `None` as a refusal (fail-closed).
///
/// `extras` folds in the **seeding-independent** on-disk content the compiled IR
/// alone does not carry — surrogate-key sidecars (#1) and contract
/// presence/contents (#3) — so a `[[surrogate_key]]` or `.contract.toml` change
/// between plan and apply moves the fingerprint even though `config` + `sql` are
/// byte-identical. See [`ExecutionExtras`].
pub(crate) fn execution_ir_fingerprint(
    models: &[rocky_core::models::Model],
    config_identity: &str,
    governance_identity: &str,
    exec_control_identity: &str,
    extras: &ExecutionExtras,
) -> Option<String> {
    let mut projection: BTreeMap<String, serde_json::Value> = BTreeMap::new();
    for m in models {
        // `to_value` normalises any nested HashMap to sorted-key order.
        let config = serde_json::to_value(&m.config).ok()?;
        projection.insert(
            m.config.name.clone(),
            serde_json::json!({ "config": config, "sql": m.sql }),
        );
    }
    // Re-through `to_value` so the whole tree is canonical (sorted keys).
    let root = serde_json::to_value(&projection).ok()?;
    let bytes = serde_json::to_vec(&root).ok()?;
    // Canonical (sorted-key) serialization of the extras — `ExecutionExtras` is
    // built from `BTreeMap`s, so this is process-stable. Fail-closed: a
    // serialization failure returns `None` (→ refusal), never a partial hash.
    let extras_bytes = serde_json::to_vec(extras).ok()?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(&bytes);
    hasher.update(b"\x00cfg\x00");
    hasher.update(config_identity.as_bytes());
    hasher.update(b"\x00gov\x00");
    hasher.update(governance_identity.as_bytes());
    // Execution-control identity (#1095): `[run]`/`[reuse]`/`[resilience]` and
    // the content-addressed `[hook]` set. Hashed in (never compared raw) so a
    // post-plan edit to what executes or which shell command fires refuses at
    // the choke-point.
    hasher.update(b"\x00exec\x00");
    hasher.update(exec_control_identity.as_bytes());
    hasher.update(b"\x00extras\x00");
    hasher.update(&extras_bytes);
    Some(hasher.finalize().to_hex().to_string())
}

/// The seeding-**independent** on-disk execution inputs the compiled `ModelIr`
/// does not carry, folded into the execution fingerprint so a post-plan swap of
/// either is refused at the choke-point. Both inputs are derived purely from
/// files under `models_dir` (never from the source-warehouse schema), so plan
/// and apply compute them identically for an unchanged project — which is why
/// they are safe to fingerprint without risking a false-refuse on a source
/// schema drift (that class is finding #2, deferred).
///
/// - **Surrogate keys (#1).** `[[surrogate_key]]` sidecar blocks are loaded by
///   [`load_surrogate_keys_from_dir`](rocky_core::models::load_surrogate_keys_from_dir),
///   NOT by the compiler, so they are absent from `ModelConfig` and invisible to
///   the config+SQL projection. A model that gains/changes a surrogate key wraps
///   its SELECT at materialization time — a different physical write.
/// - **Contracts (#3).** `contract_path` lives on `Model`, outside
///   `ModelConfig`, so contract presence and contents are unfingerprinted — yet
///   contract PRESENCE is authorization-relevant (the `contracted` policy
///   attribute) and its contents constrain the model's output. Keyed by model
///   name; the value is a content hash. An absent key means "no contract", so
///   adding or removing a contract moves the fingerprint.
/// - **Effective masking plan (finding C).** The masking reconcile applies a
///   strategy to a column only when the column's classification tag (in
///   `ModelConfig`, already hashed) resolves under the active env. Binding the
///   WHOLE `[mask]` map over-binds: an *unused* mask entry would move the
///   fingerprint. Here we bind only the resolution for tags actually declared on
///   the executed models — so an unused mask entry does NOT refuse, a used one
///   does. Computed at the choke-point because it depends on the executed set.
#[derive(Debug, Default, Clone, serde::Serialize)]
pub(crate) struct ExecutionExtras {
    /// Per-model surrogate-key specs, name-sorted for stability.
    surrogate_keys: BTreeMap<String, Vec<rocky_core::models::SurrogateKeySpec>>,
    /// Per-model contract content hash (blake3 hex of the raw `.contract.toml`
    /// bytes). Absent key ⇒ no contract. Hashes CONTENTS, never the path (the
    /// path is machine/tempdir-specific and would cause cross-machine
    /// false-refuse).
    contracts: BTreeMap<String, String>,
    /// The EFFECTIVE masking resolution (finding C): `tag → strategy` restricted
    /// to classification tags actually declared on the executed models. Only the
    /// resolution is bound here — the model `(col → tag)` classification is
    /// already in the config projection.
    effective_masks: BTreeMap<String, rocky_ir::MaskStrategy>,
}

impl ExecutionExtras {
    /// Assemble the extras from the already-loaded surrogate-key map, the
    /// compiled models (for `contract_path`s + classification tags), and the
    /// env-resolved mask map. Called identically at plan time and at the apply
    /// choke-point over the same `models_dir` / resolved mask.
    pub(crate) fn build(
        surrogate_keys: &std::collections::HashMap<
            String,
            Vec<rocky_core::models::SurrogateKeySpec>,
        >,
        models: &[rocky_core::models::Model],
        resolved_mask: &BTreeMap<String, rocky_ir::MaskStrategy>,
    ) -> Self {
        let surrogate_keys: BTreeMap<String, Vec<rocky_core::models::SurrogateKeySpec>> =
            surrogate_keys
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
        let mut contracts = BTreeMap::new();
        let mut effective_masks = BTreeMap::new();
        for m in models {
            if let Some(path) = &m.contract_path {
                // Hash the CONTENTS. A read failure at apply that succeeded at
                // plan yields a distinct stable sentinel → the fingerprint moves
                // → refuse (fail-closed); it only silently matches when the
                // contract is unreadable at both sites (a degenerate case the
                // compile would already have rejected).
                let hash = match std::fs::read(path) {
                    Ok(bytes) => blake3::hash(&bytes).to_hex().to_string(),
                    Err(_) => "<unreadable-contract>".to_string(),
                };
                contracts.insert(m.config.name.clone(), hash);
            }
            // Only the resolution for tags this model actually classifies with.
            for tag in m.config.classification.values() {
                if let Some(strategy) = resolved_mask.get(tag) {
                    effective_masks.insert(tag.clone(), *strategy);
                }
            }
        }
        Self {
            surrogate_keys,
            contracts,
            effective_masks,
        }
    }
}

/// The env-resolved, **routing** config identity that authorization depends on —
/// the physical destination and adapter/target shape, with connection
/// **credentials excluded** (finding #4).
///
/// Each adapter and pipeline is serialized in full via `serde_json::to_value`
/// (sorted keys — serde_json has no `preserve_order`). Every credential field is
/// a [`RedactedString`](rocky_core::redacted::RedactedString), whose `Serialize`
/// writes `"***"`, so tokens / passwords / api-keys / secrets are excluded **by
/// construction** while every routing field — `host`, `account`, `database`,
/// `project_id`, `location`, `warehouse`, `path`, `http_path`, the target
/// `catalog_template` / `schema_template`, the target adapter, and per-pipeline
/// governance — is captured. So swapping `path = a.duckdb → b.duckdb` (a
/// different physical DB), swapping the adapter type, or changing a target
/// template CHANGES the identity (→ refuse), while rotating a credential does
/// NOT (→ no spurious refuse). New secret fields are auto-redacted; new routing
/// fields are auto-included.
///
/// This identity is **routing-only** and env-invariant, so it is safe to compare
/// in the pre-mutation [`GovernedRunContext::verify_routing_identity`] gate
/// (which runs with no `env`). Workspace *governance* policy (mask / roles /
/// cache-selection) is env-resolved and lives in a separate
/// [`governance_policy_identity`], hashed into the execution fingerprint only —
/// never here — so an env-var-templated advisory value cannot cause a
/// cross-process routing false-refuse (finding #5).
///
/// # KNOWN LIMITATION — config-swap TOCTOU (findings 1–4, tracked follow-up)
///
/// `config_policy_identity` binds only **adapters + pipelines** (routing/
/// destination). It does **not** bind `[policy]` or `[state]`. Meanwhile the
/// governed `run` / `promote` / `propose` EXECUTION paths **re-read `rocky.toml`
/// from disk** (see the seam note on [`GovernedRunContext::verify_routing_identity`]
/// and `commands::run::run`'s L1212 snapshot): the apply-time freeze/policy gate
/// reads the config once, and execution reads it again. An attacker with
/// filesystem write access who swaps the on-disk `[policy]` / `[state]` block
/// **between** the apply gate and execution can therefore bypass a freeze or a
/// deny rule (or redirect the `[state]` backend) for those paths — the swap moves
/// fields that are NOT in this routing identity, so the fingerprint gate does not
/// catch it. The single-snapshot threading in this crate closes the swap **within
/// each governed decision function** (the gate + guard + `verify_after` +
/// models-dir all read one snapshot), but does not yet extend that snapshot
/// through the `run()` execution engine. The sound fix — execute every governed
/// mutation from the apply-verified config snapshot (thread the `&RockyConfig`
/// through `execute_run_plan` → `run()`), or bind `[policy]`/`[state]` into the
/// pre-mutation execution identity — is a tracked design follow-up, out of scope
/// here. Mitigating factor: the threat requires local filesystem write access to
/// `rocky.toml` during the apply window.
pub(crate) fn config_policy_identity(cfg: &rocky_core::config::RockyConfig) -> String {
    let adapters: BTreeMap<&str, serde_json::Value> = cfg
        .adapters
        .iter()
        .map(|(name, a)| {
            (
                name.as_str(),
                serde_json::to_value(a).unwrap_or(serde_json::Value::Null),
            )
        })
        .collect();
    let pipelines: BTreeMap<&str, serde_json::Value> = cfg
        .pipelines
        .iter()
        .map(|(name, p)| {
            (
                name.as_str(),
                serde_json::to_value(p).unwrap_or(serde_json::Value::Null),
            )
        })
        .collect();
    // Canonicalize (sorted keys) via `to_value`, then a stable string.
    serde_json::to_value(serde_json::json!({ "adapters": adapters, "pipelines": pipelines }))
        .map(|v| v.to_string())
        .unwrap_or_default()
}

/// The **model-independent governance** identity — the env-invariant,
/// mutation-driving workspace governance the reviewed plan authorized. Hashed
/// into the execution fingerprint **only** (never the routing gate), so a
/// post-plan change to what the governance reconcile would apply is refused,
/// while it cannot cause a routing false-refuse.
///
/// Binds exactly the fields that **drive a warehouse mutation** and are
/// model-independent:
/// - the role graph's **effective GRANT set** — each role's *sorted flattened
///   permissions* only (finding #2 + D). `inherits_from` is deliberately
///   dropped: the adapter derives GRANTs solely from `flattened_permissions`
///   (inherits is logging-only), so reordering semantically-equivalent
///   `inherits` must NOT refuse. `None`/empty when the graph is malformed
///   (deterministic on both sides).
/// - the `[cache.schemas]` **selection toggle** (`enabled`) — an agent-controlled
///   on-disk switch that changes whether apply resolves concrete `typed_columns`
///   vs `Unknown` (finding #4a). Only the boolean toggle is bound, NOT the whole
///   struct: `ttl` can be env-var-templated, which would re-introduce the #5
///   cross-process false-refuse; `enabled` is a closed boolean.
///
/// The **mask** is NOT bound here — it is model-DEPENDENT (only the resolution
/// for tags declared on the executed models drives a mutation), so it lives in
/// [`ExecutionExtras::effective_masks`], computed at the choke-point (finding C).
/// Advisory `[classifications]` is **excluded by design** (finding #5): it drives
/// no warehouse action (W004-only) and its env-var-templated `allow_unmasked`
/// values would differ across processes → a false-refuse.
pub(crate) fn governance_policy_identity(cfg: &rocky_core::config::RockyConfig) -> String {
    // Project each role to its SORTED flattened permissions only (D): the GRANT
    // set the adapter actually applies. `flattened_permissions` is Ord-sorted by
    // construction; `role_graph()` is name-keyed (BTreeMap) → deterministic. A
    // malformed graph → empty map on both sides (no false-refuse); an effective-
    // permission change (SELECT→MANAGE) moves the identity → refuse.
    let role_perms: BTreeMap<String, Vec<rocky_ir::Permission>> = cfg
        .role_graph()
        .map(|roles| {
            roles
                .into_iter()
                .map(|(name, r)| (name, r.flattened_permissions))
                .collect()
        })
        .unwrap_or_default();
    serde_json::to_value(serde_json::json!({
        "roles": role_perms,
        "cache_schemas_enabled": cfg.cache.schemas.enabled,
    }))
    .map(|v| v.to_string())
    .unwrap_or_default()
}

/// The env-resolved **execution-control** identity (#1095(a)): the on-disk
/// `[run]`, `[reuse]`, and `[resilience]` config that governs WHAT executes and
/// how it retries/reuses — none of which the compiled-IR projection or the
/// routing/governance identities capture. Folded into the execution fingerprint
/// (hashed, never compared raw), so a post-plan edit to a skip/reuse/retry toggle
/// refuses at the execution choke-point.
///
/// `[hook]`/`[hook.webhooks]` are deliberately NOT bound here (#1095(c)). Hooks
/// fire at pipeline-start — BEFORE the fingerprint choke-point — and a
/// replication-only apply never reaches the gate, so binding them could not
/// prevent a post-review hook from executing. A governed apply that configures
/// any hook or webhook is instead REFUSED outright before any hook fires; see
/// [`refuse_governed_side_effects`](crate::commands::run::refuse_governed_side_effects).
pub(crate) fn execution_control_identity(cfg: &rocky_core::config::RockyConfig) -> String {
    serde_json::to_value(serde_json::json!({
        "run": serde_json::to_value(&cfg.run).unwrap_or(serde_json::Value::Null),
        "reuse": serde_json::to_value(&cfg.reuse).unwrap_or(serde_json::Value::Null),
        "resilience": serde_json::to_value(&cfg.resilience).unwrap_or(serde_json::Value::Null),
    }))
    .map(|v| v.to_string())
    .unwrap_or_default()
}

/// The TOCTOU check threaded to the single execution choke-point
/// ([`commands::run::execute_models`]). Carries the plan-authorized IR
/// fingerprint plus the execute-time config identity so the choke-point can
/// recompute [`execution_ir_fingerprint`] over the exact compiled set about to
/// execute and REFUSE (fail-closed) on any mismatch — checked == executed.
#[derive(Clone)]
pub struct ExecFingerprintGate {
    /// The IR fingerprint the plan authorized (from `EmbeddedCapabilities`).
    /// `None` on a legacy plan (skip) OR a production fingerprint failure — the
    /// two are told apart by `require`.
    pub expected: Option<String>,
    /// The routing config identity computed from the execute-time config.
    pub config_identity: String,
    /// The model-independent governance identity computed from the execute-time
    /// config (roles / cache-selection).
    pub governance_identity: String,
    /// The execute-time execution-control identity (#1095): `[run]`/`[reuse]`/
    /// `[resilience]` + the content-addressed `[hook]` set. Carried here (like
    /// `resolved_mask`) because `execute_models` has no `cfg`/`config_dir`.
    pub exec_control_identity: String,
    /// The env-resolved mask map (`resolve_mask_for_env(plan-env)`). The
    /// choke-point restricts it to the executed models' classification tags when
    /// building [`ExecutionExtras`] (finding C — an unused mask entry must not
    /// refuse). Carried here because `execute_models` has no `env`/`cfg`.
    pub resolved_mask: BTreeMap<String, rocky_ir::MaskStrategy>,
    /// The plan-authorized REVIEWED source-schema snapshot (finding #2). `Some`
    /// (authoritative even if empty) ⇒ the choke-point seeds its compile's
    /// `source_schemas` from EXACTLY this, never the live cache, so `typed_columns`
    /// replay the reviewed schema. `None` on a genuinely-legacy plan; a required
    /// (v2 governed) plan with `None` REFUSES. Carried here because
    /// `execute_models` has no plan handle.
    pub reviewed_source_schemas: Option<BTreeMap<String, Vec<rocky_ir::types::TypedColumn>>>,
    /// The plan id, for the refusal message.
    pub plan_id: String,
    /// `true` when the plan is a NEW (`fingerprint_version >= 1`) governed plan
    /// that MUST carry a fingerprint (finding #7): a `None` `expected` here is a
    /// production failure and REFUSES, not a legacy skip.
    pub require: bool,
}

impl ExecFingerprintGate {
    /// Fail-closed: recompute the IR fingerprint over the exact compiled set
    /// about to execute and bail on mismatch. A genuinely-legacy plan (no bound
    /// fingerprint AND `!require`) is allowed through; a NEW plan whose
    /// fingerprint is missing (`require && expected.is_none()`) is REFUSED
    /// (production failure, #7); and a live mismatch is refused.
    pub(crate) fn verify(
        &self,
        models: &[rocky_core::models::Model],
        extras: &ExecutionExtras,
    ) -> Result<()> {
        let Some(expected) = self.expected.as_deref() else {
            if self.require {
                bail!(
                    "refusing to execute plan '{}': it is a governed plan whose execution \
                     fingerprint could not be produced at plan time (the project did not \
                     compile), so its execution cannot be authorized. Re-plan with `rocky plan` \
                     (and fix any compile error) before applying.",
                    self.plan_id
                );
            }
            return Ok(()); // genuinely-legacy plan — no bound fingerprint
        };
        let actual = execution_ir_fingerprint(
            models,
            &self.config_identity,
            &self.governance_identity,
            &self.exec_control_identity,
            extras,
        );
        if actual.as_deref() != Some(expected) {
            bail!(
                "refusing to execute plan '{}': the models/config changed since the plan was \
                 authorized and reviewed (compiled-IR fingerprint mismatch). A model's logic or \
                 config, or the resolved target/adapter, differs from what was gated — so the \
                 recorded authorization no longer covers what would execute. Re-plan with \
                 `rocky plan` and (if AI-authored) re-review before applying.",
                self.plan_id
            );
        }
        Ok(())
    }
}

/// Governance context threaded into `commands::run::run` for a two-step agent
/// apply. When present (`Some`), `run` performs the fail-closed **replication
/// gate** at the point of execution: it evaluates the agent-policy plane over
/// the concrete post-discovery replication target set (which the
/// transformation-only apply gate never sees) and refuses a denied / unreviewed
/// agent replication before any table is materialized.
///
/// `None` for bare `rocky run`, the DAG runner, inline execution, and every
/// human apply (ungated in v0) → the gate does not run and behaviour is
/// byte-identical.
///
/// NB: the gate/execute TOCTOU (a model's content changing between the
/// plan-time authorization and execution) is NOT closed here — the round-3
/// file-hash attempt was unsound (it hashed the state file under `models_dir`,
/// self-invalidating). The sound fix (fingerprint the compiled IR projection at
/// the single execution compile in `execute_models`) is a separate change; see
/// the PR body.
pub struct GovernedRunContext<'a> {
    /// The enforcement principal for this apply (see
    /// [`PersistedPlan::enforcement_principal`]).
    pub principal: PolicyPrincipal,
    /// The plan id the decision rows are recorded against, and the review
    /// marker the replication gate consults.
    pub plan_id: &'a str,
    /// Project root holding `.rocky/plans/<plan_id>.reviewed.json`.
    pub root: &'a Path,
    /// The config the policy block loads from.
    pub config_path: &'a Path,
    /// The plan-authorized compiled-IR fingerprint (from `EmbeddedCapabilities`)
    /// — checked at the execution choke-point to close the gate/execute TOCTOU.
    pub expected_ir_fingerprint: Option<String>,
    /// The plan-authorized routing config identity — verified BEFORE any
    /// replication/governance mutation (finding #4/#5). `None` on a legacy plan.
    pub expected_config_identity: Option<String>,
    /// `true` when the plan is a NEW governed plan (`fingerprint_version >= 1`)
    /// that MUST carry a fingerprint/identity: a missing one is a production
    /// failure and REFUSES (finding #7), not a legacy skip.
    pub require_fingerprint: bool,
    /// The plan-authorized REVIEWED source-schema snapshot (finding #2). `Some`
    /// (authoritative even if empty) ⇒ apply seeds its compile from EXACTLY this,
    /// never the live cache. `None` on a legacy plan; a required plan with `None`
    /// REFUSES (fail-closed).
    pub reviewed_source_schemas:
        Option<std::collections::BTreeMap<String, Vec<rocky_ir::types::TypedColumn>>>,
    /// `true` when the plan REVIEWED a non-empty model set (`!run_plan.models
    /// .is_empty()`). Finding #2 (missing-dir): threaded to the executor legs so a
    /// governed model-executing apply whose reviewed models directory is deleted
    /// AFTER the apply seam's compile (the seam→execute race) still fails CLOSED at
    /// the executor rather than silently skipping. `false` (an empty reviewed set,
    /// e.g. a pure-replication run with no silver models) preserves the executor's
    /// legitimate silent-skip.
    pub expects_models: bool,
}

impl GovernedRunContext<'_> {
    /// Build the execution-choke-point TOCTOU gate for this apply, pairing the
    /// plan-authorized fingerprint with the execute-time routing + governance
    /// identity. `env` is the plan's persisted `--env` (from the caller, which
    /// holds it — `execute_models` does not), so the governance identity binds
    /// the mask resolved for the env that will actually run.
    pub(crate) fn exec_fingerprint_gate(
        &self,
        cfg: &rocky_core::config::RockyConfig,
        env: Option<&str>,
    ) -> ExecFingerprintGate {
        ExecFingerprintGate {
            expected: self.expected_ir_fingerprint.clone(),
            config_identity: config_policy_identity(cfg),
            governance_identity: governance_policy_identity(cfg),
            exec_control_identity: execution_control_identity(cfg),
            resolved_mask: cfg.resolve_mask_for_env(env),
            reviewed_source_schemas: self.reviewed_source_schemas.clone(),
            plan_id: self.plan_id.to_string(),
            require: self.require_fingerprint,
        }
    }

    /// Fail-closed pre-mutation routing gate (finding #4/#5): verify the
    /// execute-time routing config identity matches what the plan authorized,
    /// BEFORE any replication/governance warehouse statement runs. A `path` /
    /// adapter / target swap between plan and apply (e.g. duckdb→snowflake, or
    /// `a.duckdb`→`b.duckdb`) is refused before a single DDL executes. A
    /// genuinely-legacy plan (`!require` and no stored identity) is allowed
    /// through; a NEW plan with a missing identity is refused (#7).
    ///
    /// KNOWN LIMITATION — config-swap TOCTOU (findings 1–4, tracked follow-up):
    /// this gate binds ROUTING only ([`config_policy_identity`] = adapters +
    /// pipelines), not `[policy]` / `[state]`. This governed EXECUTION seam
    /// re-reads `rocky.toml` (the `cfg` here is `run`'s own re-read snapshot, not
    /// the apply-time gate's), so an on-disk `[policy]`/`[state]` swap performed
    /// between the apply gate and here is NOT caught — see the full note on
    /// [`config_policy_identity`]. Closing it fully (execute from the
    /// apply-verified snapshot, or fingerprint `[policy]`/`[state]`) is a tracked
    /// design follow-up.
    pub(crate) fn verify_routing_identity(
        &self,
        cfg: &rocky_core::config::RockyConfig,
    ) -> Result<()> {
        let actual = config_policy_identity(cfg);
        match self.expected_config_identity.as_deref() {
            Some(expected) if expected == actual => Ok(()),
            Some(_) => bail!(
                "refusing to execute plan '{}': the resolved routing config (adapter / physical \
                 destination / target / governance) changed since the plan was authorized — a \
                 different warehouse or object would be written. Re-plan with `rocky plan` before \
                 applying.",
                self.plan_id
            ),
            None if self.require_fingerprint => bail!(
                "refusing to execute plan '{}': it is a governed plan whose routing identity \
                 could not be produced at plan time, so its physical destination cannot be \
                 authorized. Re-plan with `rocky plan` before applying.",
                self.plan_id
            ),
            None => Ok(()), // genuinely-legacy plan
        }
    }
}

impl GovernedRunContext<'_> {
    /// Fail-closed replication gate: evaluate the agent-policy plane over the
    /// concrete discovered replication target names and enforce the verdict
    /// before any replication SQL runs. A `NotConfigured` plane (no `[policy]`)
    /// is a no-op.
    /// `ledger` is the state-store handle `run` already holds — the gate reads
    /// and records through it rather than re-opening (which would collide
    /// in-process and spuriously mark the snapshot unreadable → a bogus
    /// fail-closed deny for every agent replication).
    /// `cfg` is the SAME immutable snapshot `run` loaded once (its L1212
    /// `rocky_cfg`); the gate evaluates `[policy]` and resolves the models-dir
    /// from it rather than reloading `rocky.toml`, so a mid-run swap can't change
    /// the plane this gate enforces (finding 1).
    pub(crate) fn gate_replication_targets(
        &self,
        target_names: &BTreeSet<String>,
        ledger: &StateStore,
        cfg: &rocky_core::config::RockyConfig,
    ) -> Result<()> {
        if target_names.is_empty() {
            return Ok(());
        }
        // Replication targets are not compiled models; gate each under the bare
        // `apply` verb (a mutation), matched by table name.
        let touched: BTreeMap<String, PolicyCapability> = target_names
            .iter()
            .map(|n| (n.clone(), PolicyCapability::Apply))
            .collect();
        // No compiled model dir for replication targets — evaluate against
        // bare-named attributes (a nonexistent dir yields an empty attr map, so
        // every target matches by name / the default posture).
        let models_dir = resolve_config_models_dir(self.config_path, Some(cfg));
        let gate = evaluate_apply_policy_with_store(
            cfg.policy.as_ref(),
            self.plan_id,
            self.principal,
            &touched,
            &models_dir,
            ledger,
        );
        apply_policy_gate(self.root, self.plan_id, gate)?;

        // Finding 7: replication pipelines have NO post-run check substrate — a
        // replication apply supplies no `verify_after` run id and nothing runs or
        // records the named checks. A rule's required `verify_after` checks are
        // contractually fail-closed (an absent check halts), so silently ALLOWING
        // an unverified replication mutation under a rule that demands checks is a
        // policy bypass. Refuse instead: if the matched allow-rule(s) for these
        // targets require any `verify_after`, fail closed with a clear message.
        let required =
            required_verify_after(cfg.policy.as_ref(), self.principal, &touched, &models_dir);
        if !required.is_empty() {
            bail!(
                "refusing governed replication apply for plan '{}': the matched policy rule \
                 requires verify_after checks [{}], but `verify_after` is NOT supported for \
                 replication pipelines — there is no post-run check substrate to run or record \
                 them, so the mutation could not be verified. Remove `verify_after` from the \
                 matching rule, or move these targets to a transformation pipeline.",
                self.plan_id,
                required.join(", ")
            );
        }
        Ok(())
    }
}

/// Evaluate the agent-policy plane over a `Promote` plan and enforce the
/// resulting gate, before any promote SQL executes.
///
/// Shared by BOTH promote apply entry points — `rocky apply <promote-plan>`
/// and `rocky branch promote --plan <id>` — so neither can execute a denied
/// agent promote. Absent `[policy]` this is a no-op (`NotConfigured` → the
/// pre-policy-plane behaviour). A human-authored promote resolves to `allow`;
/// an agent-authored one is gated per target under the `promote` verb. The
/// models dir is resolved from config so a name-scoped rule matches the
/// target's logical name.
pub(crate) fn gate_promote_plan(
    root: &Path,
    config_path: &Path,
    plan_id: &str,
    principal: PolicyPrincipal,
    promote_plan: &PromotePlan,
    state_path: &Path,
) -> Result<std::sync::Arc<rocky_core::config::LoadedConfig>> {
    // THE single fingerprinted config snapshot for the promote (#1120): the
    // pre-gate sync decision, the policy gate, AND — via the returned `Arc` —
    // the promote executor's adapter resolution all read THIS instance, so a
    // `rocky.toml` swap timed between the gate and `run_promote_apply` cannot
    // redirect the promote SQL at a different warehouse.
    //
    // Deliberate behavior delta: HARD load (formerly `.ok()`) — an unloadable
    // config now fails at the gate. The executor (`run_promote_apply`)
    // already hard-required a loadable config to resolve its adapter, so the
    // full promote path could never succeed without one; failing here only
    // moves the failure before the gate.
    let loaded = std::sync::Arc::new(
        rocky_core::config::load_rocky_config_fingerprinted(config_path).with_context(|| {
            format!(
                "refusing to gate promote plan '{plan_id}': failed to load config from {}",
                config_path.display()
            )
        })?,
    );
    let promote_models_dir = resolve_config_models_dir(config_path, Some(&loaded.config));
    let touched = touched_models_for_promote(promote_plan, &promote_models_dir);
    // Finding 4-apply: pull the authoritative remote freeze/budget ledger before
    // the gate reads it, so a cross-pod freeze is enforced. Placed inside the
    // shared gate so BOTH promote entry points (`rocky apply <promote>` and
    // `rocky branch promote --plan`) are covered. Fail-closed, but skipped when
    // the gate won't read the ledger (no policy / empty touched — finding 8).
    sync_remote_ledger_before_gate_blocking(&loaded.config, state_path, &touched)?;
    let gate = evaluate_apply_policy_with_policy(
        loaded.config.policy.as_ref(),
        plan_id,
        principal,
        &touched,
        &promote_models_dir,
        state_path,
    );
    apply_policy_gate(root, plan_id, gate)?;
    Ok(loaded)
}

/// Resolve the models directory for the promote gate from an ALREADY-LOADED
/// config snapshot: the first transformation pipeline's `models` glob base
/// (everything before the first wildcard), relative to `config_path`'s parent.
/// Falls back to `<project>/models` when the snapshot is `None` or declares no
/// transformation pipeline. Mirrors the backfill/gc resolution.
///
/// Finding 1: takes the snapshot rather than reloading `rocky.toml`, so a
/// governed path resolves the models dir from the SAME config it gates on.
fn resolve_config_models_dir(
    config_path: &Path,
    cfg: Option<&rocky_core::config::RockyConfig>,
) -> std::path::PathBuf {
    let project_root = config_path.parent().unwrap_or(Path::new(""));
    let glob = cfg.and_then(|cfg| {
        cfg.pipelines.values().find_map(|p| match p {
            rocky_core::config::PipelineConfig::Transformation(t) => Some(t.models.clone()),
            _ => None,
        })
    });
    let base = glob
        .as_deref()
        .and_then(|g| g.split(&['*', '?', '['][..]).next())
        .filter(|b| !b.is_empty())
        .unwrap_or("models");
    project_root.join(base.trim_end_matches('/'))
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
/// The `NotConfigured` arm is handled by the caller (it needs the pre-policy-plane
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

/// The union of `verify_after` check names the winning rules require for a
/// plan's *proceeding* models.
///
/// Re-evaluates the policy per touched model and, for every model that did
/// **not** resolve to `deny` (a `deny` never reaches apply), collects the
/// winning rule's `verify_after` list. Returns a sorted, de-duplicated set;
/// empty when no `[policy]` block is configured or no matched rule carries a
/// `verify_after` (the common case — no post-apply gate).
fn required_verify_after(
    policy: Option<&rocky_core::config::PolicyConfig>,
    principal: PolicyPrincipal,
    touched: &BTreeMap<String, PolicyCapability>,
    models_dir: &Path,
) -> Vec<String> {
    // Finding 1: takes the SAME `[policy]` snapshot the gate used, rather than
    // reloading `rocky.toml` — a swap between the gate and this resolution could
    // otherwise change the `verify_after` set the governed apply enforces.
    let Some(policy) = policy else {
        return Vec::new();
    };
    let attrs_map = model_attributes(models_dir);
    let mut names: BTreeSet<String> = BTreeSet::new();
    for (model, capability) in touched {
        let owned;
        let attrs = match attrs_map.get(model) {
            Some(a) => a,
            None => {
                owned = ModelAttributes {
                    name: model.clone(),
                    ..Default::default()
                };
                &owned
            }
        };
        let decision = policy::evaluate(policy, principal, *capability, attrs);
        if decision.effect == PolicyEffect::Deny {
            continue;
        }
        if let Some(idx) = decision.matched_rule
            && let Some(rule) = policy.rules.get(idx)
        {
            names.extend(rule.verify_after.iter().cloned());
        }
    }
    names.into_iter().collect()
}

/// Run the `verify_after` post-apply gate: confirm every named check ran and
/// **passed** in *this apply's own run*, and record a verification custody
/// entry either way.
///
/// `run_id` is the unique id this apply forced its run to record under (see
/// [`new_apply_run_id`] / the `run_id_override` on `commands::run::run`). The
/// gate resolves the run by that id via [`StateStore::get_run`] rather than
/// reading "the latest run" — a concurrent run finishing in between must never
/// be mistaken for this apply's run and satisfy its gate.
///
/// Fail-closed on three fronts:
/// - **Run not found** for `run_id` (e.g. the run was skipped by idempotency,
///   or a `--dag` apply never recorded under this id) ⇒ every required check is
///   unverifiable ⇒ halt.
/// - A named check that **failed** ⇒ halt.
/// - A named check **absent** from the run's captured outcomes ⇒ halt.
///
/// Duplicate check names are aggregated with **AND**: per-table checks share a
/// fixed name (e.g. `row_count` is emitted once per table), so a required check
/// passes only if it ran at least once *and every occurrence passed*. A naive
/// last-writer-wins map would let a later passing table mask an earlier failing
/// one.
///
/// On success the custody entry records `effect = allow`; on failure it records
/// `effect = deny`, an alert is raised, and an error is returned.
///
/// Auto-rollback runs only where a rollback substrate exists. None does today
/// (the content-addressed / Iceberg pointer-swap path is object-store-only and
/// has no local read-back), so a failed verification is **halt-only**: the
/// mutation has already landed and stays in place until a human reverts it.
/// That state is stated plainly in the error rather than papered over with a
/// rollback that would not actually run.
fn run_verify_after(
    plan_id: &str,
    principal: PolicyPrincipal,
    required: &[String],
    run_id: &str,
    state_path: &Path,
) -> Result<()> {
    if required.is_empty() {
        return Ok(());
    }

    // Finding 6: `run_verify_after` only runs when checks are required, so its
    // custody row is ALWAYS budget/verify-relevant. Open the ledger fail-closed
    // with a bounded retry on advisory-lock contention (rather than a single
    // best-effort open) so the custody half of the budget-burn pair is durable.
    let store = open_ledger_with_retry(state_path)
        .with_context(|| format!("failed to open state store at {}", state_path.display()))?;

    // Resolve *this apply's own* run by id (not "latest"). `None` means no run
    // was recorded under this id — fail closed below (every check unverifiable).
    let run = store
        .get_run(run_id)
        .with_context(|| format!("failed to read run '{run_id}' from state store"))?;

    // AND-aggregate every occurrence of each check name: any `false` occurrence
    // fails the name. Presence in the map ⇒ the check ran at least once.
    let mut outcomes: BTreeMap<&str, bool> = BTreeMap::new();
    if let Some(record) = run.as_ref() {
        for c in &record.check_outcomes {
            outcomes
                .entry(c.name.as_str())
                .and_modify(|passed| *passed &= c.passed)
                .or_insert(c.passed);
        }
    }

    let mut failures: Vec<String> = Vec::new();
    if run.is_none() {
        // The run this apply produced could not be found — treat every required
        // check as unverifiable and halt (fail closed).
        for name in required {
            failures.push(format!(
                "{name} (apply run '{run_id}' not found — unverifiable)"
            ));
        }
    } else {
        for name in required {
            match outcomes.get(name.as_str()) {
                Some(true) => {}
                Some(false) => failures.push(format!("{name} (failed)")),
                // Fail closed: a named check that did not run cannot be confirmed.
                None => failures.push(format!("{name} (absent — did not run)")),
            }
        }
    }
    let passed = failures.is_empty();

    let reason = if passed {
        format!("verify_after passed: [{}]", required.join(", "))
    } else {
        format!(
            "verify_after FAILED: {}. No rollback substrate available — the mutation stands; halt-only.",
            failures.join("; ")
        )
    };
    // Best-effort custody entry — the gate below is the safety boundary; the
    // ledger is the trail.
    let record = PolicyDecisionRecord {
        timestamp: chrono::Utc::now(),
        plan_id: plan_id.to_string(),
        principal,
        capability: PolicyCapability::Apply,
        model: "*".to_string(),
        effect: if passed {
            PolicyEffect::Allow
        } else {
            PolicyEffect::Deny
        },
        rule_id: None,
        reason: reason.clone(),
        verify_after: required.to_vec(),
        auto_apply: None,
    };
    // Finding 6: FAIL-CLOSED — the verify custody row is the budget-burning half
    // of the pair, so a write failure must abort (not warn-and-continue) rather
    // than silently leave the failed apply un-burnable.
    store.record_policy_decision(&record).with_context(|| {
        format!(
            "fail-closed: could not persist the verify_after custody row for plan '{plan_id}' — \
             the autonomy-budget pair would be incomplete, so a later agent action could \
             auto-allow. Retry when the state store is not contended."
        )
    })?;

    if passed {
        eprintln!(
            "verify_after: {} post-apply check(s) passed [{}].",
            required.len(),
            required.join(", ")
        );
        Ok(())
    } else {
        // Alert: a post-apply verification failure is an operational event, not
        // a routine warning.
        warn!(
            target: "rocky::policy",
            plan_id,
            failures = %failures.join("; "),
            "verify_after post-apply gate FAILED"
        );
        bail!(
            "verify_after gate FAILED for plan '{plan_id}': {}. \
             No rollback substrate is available, so the mutation HAS ALREADY LANDED and remains in \
             place — it must be reverted manually. The failure is recorded in the policy-decision ledger.",
            failures.join("; ")
        )
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
    runtime_principal: PolicyPrincipal,
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

    // policy seam 2: rule-driven refusal. When a `[policy]` block is configured,
    // the per-model policy evaluation (over the authoring principal and the
    // embedded, reviewed capability classification) supersedes the fixed
    // AiAuthored gate. Absent a `[policy]` block the evaluator is never
    // constructed and the pre-policy-plane marker gate remains — byte-identical to today.
    let models_dir = Path::new(run_plan.models_dir.as_deref().unwrap_or("models"));
    // THE single fingerprinted config snapshot for this apply (#1120) — the
    // replication-only check, the pre-gate sync decision, the policy gate,
    // `execute_run_plan`'s reads, and `run()`'s execution all read THIS
    // instance. Deliberate behavior delta: HARD load (formerly `.ok()`) — an
    // unloadable config fails at the gate instead of inside `run()`. See the
    // twin note in `run_apply_run_plan`.
    let loaded = std::sync::Arc::new(
        rocky_core::config::load_rocky_config_fingerprinted(config_path).with_context(|| {
            format!(
                "refusing to apply plan '{plan_id}': failed to load config from {}",
                config_path.display()
            )
        })?,
    );
    // Gate on the models this apply will ACTUALLY execute (fresh compile +
    // `--model` selection), not the plan's informational `models` list.
    // Finding #1: a replication-only plan runs NO models, so it gates none.
    let executable = if is_replication_only(&loaded.config, &run_plan) {
        Vec::new()
    } else {
        run_executable_models(models_dir, &run_plan)
    };
    let touched = touched_models_for_run(&plan, &executable);
    let principal = plan.enforcement_principal(runtime_principal);
    // Finding 4-apply: pull the authoritative remote freeze/budget ledger before
    // the gate reads it, so a cross-pod freeze is enforced (fail-closed). Skipped
    // when the gate won't read the ledger (no policy / empty touched — finding 8).
    sync_remote_ledger_before_gate(&loaded.config, state_path, &touched).await?;
    let gate = evaluate_apply_policy_with_policy(
        loaded.config.policy.as_ref(),
        plan_id,
        principal,
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

    // Resolve the post-apply verification checks before the run plan is moved.
    let verify_checks = required_verify_after(
        loaded.config.policy.as_ref(),
        principal,
        &touched,
        models_dir,
    );

    // Finding 3 (pre-run half): make the plain rule decision durable on remote
    // before `run`'s start-download replaces the local ledger — see the twin in
    // `run_apply_run_plan`.
    if !verify_checks.is_empty() {
        upload_remote_ledger_fail_closed(
            Some(&loaded.config),
            state_path,
            "governed rule decision",
        )
        .await?;
    }

    // One unique id for this apply's run, threaded into execution and into the
    // post-apply gate so the gate reads exactly this run, not "latest".
    let apply_run_id = new_apply_run_id();
    let governed = governed_run_context(
        &plan,
        principal,
        plan_id,
        root,
        config_path,
        !run_plan.models.is_empty(),
    );
    execute_run_plan(
        config_path,
        std::sync::Arc::clone(&loaded),
        plan_id,
        run_plan,
        state_path,
        output_json,
        &apply_run_id,
        governed.as_ref(),
    )
    .await?;
    let verify_result = run_verify_after(
        plan_id,
        principal,
        &verify_checks,
        &apply_run_id,
        state_path,
    );
    // Finding 3 (post-verify half): push the verify-after custody row to remote
    // (fail-closed) even on failure — see the twin in `run_apply_run_plan`.
    if !verify_checks.is_empty() {
        upload_remote_ledger_fail_closed(Some(&loaded.config), state_path, "verify-after custody")
            .await?;
    }
    verify_result
}

/// Apply a `PlanKind::Backfill` plan — a scoped, review-gated recovery run.
///
/// A backfill plan is composed by the engine (`rocky backfill`) in response to
/// a failure or gap, so its gate is stricter than a normal plan and is a hard
/// rule rather than policy-tunable:
///
/// - It is **always** review-gated. A `rocky review <plan-id> --approve` marker
///   must exist regardless of any configured policy — a permissive `[policy]`
///   never relaxes this. Backfills are where blast radius hides.
/// - Policy may make the gate *stricter*: an agent-scoped `deny backfill {…}`
///   rule hard-refuses even a reviewed plan. A policy `require_review` is
///   already satisfied by the marker the always-on gate demands.
///
/// Once cleared, execution reuses the standard run path
/// ([`crate::commands::run::execute_backfill_set`]) so classified retry (R1)
/// and failure containment (R2) apply to the rebuild. The closure is never
/// re-authored — a backfill only re-runs existing recipes over its window.
async fn run_apply_backfill_plan(
    root: &Path,
    config_path: &Path,
    plan_id: &str,
    state_path: &Path,
    runtime_principal: PolicyPrincipal,
    output_json: bool,
) -> Result<()> {
    let plan = read_plan(root, plan_id)
        .with_context(|| format!("failed to read backfill plan '{plan_id}'"))?;

    if plan.kind != PlanKind::Backfill {
        bail!(
            "plan '{plan_id}' is a {} plan, not a backfill plan. \
             Use `rocky apply {plan_id}` and let the dispatcher route it.",
            plan.kind,
        );
    }

    let run_plan: RunPlan = serde_json::from_value(plan.payload.clone())
        .context("failed to deserialize backfill plan payload")?;

    // HARD RULE: a backfill is always review-gated, regardless of policy.
    if !ai_plan_is_reviewed(root, plan_id) {
        bail!(
            "backfill plan '{plan_id}' has not been reviewed and approved. \
             A backfill re-runs recipes over a scoped window and can hide blast \
             radius, so it always requires a human sign-off — a permissive policy \
             does not waive it. Review the scope and approve it with \
             `rocky review {plan_id} --approve`, then re-run `rocky apply {plan_id}`."
        );
    }

    // Policy can only tighten the gate: a `deny` hard-refuses even a reviewed
    // backfill. The marker above already satisfies any policy `require_review`.
    //
    // Finding 1: ONE fingerprinted config snapshot for the entire backfill
    // apply — the sync decision, the policy gate, the execute-from-owned
    // routing verification, and `execute_backfill_set` all read THIS instance,
    // so a `rocky.toml` swap can't point any of them at a different config
    // (and the recorded `config_hash` is this snapshot's load-time
    // fingerprint, not a later path re-read — #1120/F10). Fail-closed
    // pre-check folded in: a config-load ERROR bails here (the execution path
    // doesn't itself need the config, so an unenforced `[policy]` must fail
    // loud); a genuinely absent config keeps the NotConfigured posture.
    let cfg = match rocky_core::config::load_rocky_config_fingerprinted(config_path) {
        Ok(c) => Some(c),
        Err(rocky_core::config::ConfigError::FileNotFound { .. }) => None,
        Err(e) => {
            return Err(anyhow::Error::new(e).context(format!(
                "refusing to apply backfill plan '{plan_id}': {} failed to load, so any \
                 configured [policy] rules cannot be enforced (fail-closed). Fix the config and \
                 re-run `rocky apply {plan_id}`.",
                config_path.display()
            )));
        }
    };
    let models_dir = Path::new(run_plan.models_dir.as_deref().unwrap_or("models"));
    // A backfill's `models` list IS the authoritative rebuild closure the
    // engine composed and will execute (see `execute_backfill_set` below), not
    // an informational hint — gate on it directly.
    let touched = touched_models_for_run(&plan, &run_plan.models);
    // WP-01 PR-B (2b, R3-4): ONE session for the whole backfill apply,
    // acquired BEFORE the policy gate and threaded into
    // `execute_backfill_set` (which owns the always-finalize terminal half).
    // This replaces the unconditional `download_remote_ledger_unconditional`
    // that stood here: a backfill writes on top of the artifact/run/provenance
    // ledger, so the download is unconditional for a remote backend (not
    // gated on policy) and fail-closed via `require_synced` — and it must
    // happen exactly ONCE, here. A second in-executor download after the gate
    // would wholesale-replace the replicated `POLICY_DECISIONS` table
    // (downloads replace replicated tables) and erase the decision row the
    // policy gate below writes between the two downloads — the decision must
    // survive to the terminal upload. `Durable`: a backfill is always
    // review-gated, so a lost terminal upload must fail the apply.
    let backfill_state_cfg = cfg
        .as_ref()
        .map(|l| l.config.state.clone())
        .unwrap_or_default();
    let mut session = rocky_core::state_sync::RemoteStateSession::new(
        &backfill_state_cfg,
        state_path,
        rocky_core::state_sync::FinalizeDurability::Durable,
    );
    if let Err(e) = session.acquire().await {
        // Unreachable on a fresh session (`Err` = double-acquire misuse);
        // consume defensively so no exit path can leak the session.
        session.abandon("backfill acquire misuse").await;
        return Err(e.into());
    }
    if let Err(e) = session.require_synced() {
        session
            .abandon("backfill download failure (fail-closed)")
            .await;
        return Err(anyhow::Error::new(e).context(
            "failed to download remote state before backfill apply; a remote-backend \
             backfill apply requires the state backend reachable to read the authoritative \
             ledger",
        ));
    }

    // Gate + preparation, captured so EVERY pre-execution refusal (policy
    // deny, malformed plan window, snapshot preflight, routing mismatch,
    // unloadable config) consumes the session before returning — a refused
    // apply mutated nothing this session must persist, so it abandons
    // (no upload), matching the old shape where the upload-after only ran
    // once execution had been reached.
    #[allow(clippy::type_complexity)]
    let prep: Result<(
        BTreeSet<String>,
        crate::commands::run::PartitionRunOptions,
        Option<ExecFingerprintGate>,
        rocky_core::config::LoadedConfig,
    )> = (|| {
        let gate = evaluate_apply_policy_with_policy(
            cfg.as_ref().and_then(|l| l.config.policy.as_ref()),
            plan_id,
            plan.enforcement_principal(runtime_principal),
            &touched,
            models_dir,
            state_path,
        );
        if let PolicyGate::Deny {
            model,
            rule_id,
            reason,
        } = gate
        {
            let rule = rule_id.map(|r| format!(" (rule {r})")).unwrap_or_default();
            bail!(
                "policy DENIES backfill plan '{plan_id}': model '{model}'{rule} — {reason}. \
                 A deny cannot be satisfied by review; re-scope the backfill or have a \
                 human apply it."
            );
        }

        let set: BTreeSet<String> = run_plan.models.iter().cloned().collect();
        if set.is_empty() {
            bail!("backfill plan '{plan_id}' names no models to rebuild");
        }

        // A half-open window must never execute: `to_selection` only yields a
        // Range when BOTH bounds are present, so a lone bound would silently fall
        // through to `PartitionSelection::Latest` — rebuilding one partition of a
        // scope the reviewer approved as "from January". The CLI now rejects lone
        // bounds at parse time; this guards plans persisted before that (or
        // hand-authored plan files).
        if run_plan.partition_from.is_some() != run_plan.partition_to.is_some() {
            bail!(
                "backfill plan '{plan_id}' carries a half-open partition window \
                 (from: {:?}, to: {:?}) — executing it would rebuild only the \
                 latest partition, not the approved range. Re-compose the backfill \
                 with both --from and --to (or neither).",
                run_plan.partition_from,
                run_plan.partition_to,
            );
        }

        // Only the partition window carries over — a backfill never resumes,
        // shadows, or runs `--latest`/`--missing`.
        let partition_opts = crate::commands::run::PartitionRunOptions {
            partition: None,
            from: run_plan.partition_from.clone(),
            to: run_plan.partition_to.clone(),
            latest: false,
            missing: false,
            lookback: None,
            parallel: run_plan.parallel,
        };

        // Governed-apply TOCTOU gate (E) for the backfill path — an agent backfill
        // apply refuses if the compiled IR / config changed since the plan.
        let governed = governed_run_context(
            &plan,
            plan.enforcement_principal(runtime_principal),
            plan_id,
            root,
            config_path,
            // A backfill executes its `model_set`; carry the reviewed-set signal (it
            // routes through `execute_backfill_set`, not the seam-guarded legs, but
            // keep the derivation consistent).
            !run_plan.models.is_empty(),
        );
        // ‼️ Finding #2: a backfill ALWAYS executes models (its `model_set`) —
        // preflight the reviewed source-schema snapshot before any warehouse mutation.
        preflight_snapshot(governed.as_ref(), plan_id, true)?;
        // Finding 1: the routing verification below runs against the SAME `cfg`
        // snapshot the gate/sync used — not a fresh `backfill_cfg` reload.
        let exec_fp_gate = match (governed.as_ref(), cfg.as_ref()) {
            (Some(ctx), Some(l)) => {
                // Fail-closed (#4/#5): verify the routing identity before executing.
                ctx.verify_routing_identity(&l.config)?;
                // The governance identity resolves the plan's persisted `--env` (a
                // backfill persists `None`), matching the plan-side fingerprint.
                Some(ctx.exec_fingerprint_gate(&l.config, run_plan.env.as_deref()))
            }
            // Fail-closed (#7): a governed backfill whose config would not load
            // cannot be routing-verified — refuse rather than execute ungated.
            (Some(_), None) => bail!(
                "refusing to apply governed backfill plan '{plan_id}': the config at {} could not be \
                 loaded, so the execution's routing/identity cannot be verified against the plan. \
                 Fix the config and re-run `rocky apply {plan_id}`.",
                config_path.display()
            ),
            _ => None,
        };

        // Execute-from-owned (finding B): hand the SAME verified snapshot
        // `verify_routing_identity` checked to `execute_backfill_set`, rather than a
        // reload a timed `rocky.toml` swap could redirect. A config that will not load
        // cannot be executed against; bail.
        let loaded = cfg.with_context(|| {
            format!(
                "failed to load config from {} (required for backfill)",
                config_path.display()
            )
        })?;
        Ok((set, partition_opts, exec_fp_gate, loaded))
    })();
    let (set, partition_opts, exec_fp_gate, loaded) = match prep {
        Ok(v) => v,
        Err(e) => {
            session.abandon("backfill refused before execution").await;
            return Err(e);
        }
    };

    // Finding 5: a backfill MUTATES the warehouse + local ledger (run/artifact/
    // provenance rows) as it executes, so a mid-run failure can still have
    // committed state. The session moves into `execute_backfill_set`, whose
    // capture-result → ALWAYS-finalize → propagate shape preserves the old
    // unconditional fail-closed upload-after (the upload is not gated on
    // success).
    crate::commands::run::execute_backfill_set(
        &loaded,
        session,
        state_path,
        models_dir,
        &set,
        &partition_opts,
        exec_fp_gate.as_ref(),
        output_json,
    )
    .await
    .with_context(|| format!("rocky apply backfill plan '{plan_id}' failed"))
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
    runtime_principal: PolicyPrincipal,
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

    // ONE fingerprinted config snapshot (#1120): the drift-detection
    // discovery below AND the delegated `run()` execution read THIS instance,
    // so a `rocky.toml` swap timed between the drift check and execution
    // cannot redirect the replication at a different config.
    let loaded = std::sync::Arc::new(
        rocky_core::config::load_rocky_config_fingerprinted(config_path)
            .with_context(|| format!("failed to load config from {}", config_path.display()))?,
    );
    let rocky_cfg = &loaded.config;
    let (_pipeline_name, pipeline) = crate::registry::resolve_replication_pipeline(
        rocky_cfg,
        replication_plan.pipeline.as_deref(),
    )?;
    let pattern = pipeline.schema_pattern()?;

    let adapter_registry = crate::registry::AdapterRegistry::from_config(rocky_cfg)?;
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

    // Governance context (agent apply): gates the discovered replication target
    // set inside `run` before any table is materialized (D). A pure replication
    // plan carries no models, so the fingerprint is absent and the TOCTOU reject
    // is a no-op.
    let governed = governed_run_context(
        &plan,
        plan.enforcement_principal(runtime_principal),
        plan_id,
        root,
        config_path,
        // A pure replication apply delegates to `run` with default model args
        // (no --all/--models/--model) → it executes NO compiled models → an empty
        // reviewed set, so the executor legs keep their legitimate silent-skip.
        false,
    );

    crate::commands::run::run(
        config_path,
        // Execute-from-owned: the SAME snapshot the drift check read (#1120).
        loaded,
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
        // Replication apply has no `verify_after` gate; mint the usual id.
        None,
        governed.as_ref(),
        // `--assume-fresh-state` is a `rocky run` runtime flag, never part of
        // a persisted plan — the replication apply path always runs without it.
        false,
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
    runtime_principal: PolicyPrincipal,
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

    // policy seam 3: mirror the apply-time policy enforcement on the promote
    // path. The gate returns the ONE config snapshot it verified; the promote
    // executor below resolves its adapter from that same instance (#1120).
    let loaded = gate_promote_plan(
        root,
        config_path,
        plan_id,
        plan.enforcement_principal(runtime_principal),
        &promote_plan,
        state_path,
    )?;

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
        crate::commands::branch::run_promote_apply(&loaded, &promote_plan.targets).await?;

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
    assume_fresh_state: bool,
) -> Result<()> {
    // THE single fingerprinted config load for a bare `rocky run` (#1120):
    // this entry point loaded nothing before this change (run() re-read the
    // path internally), so this ADDS the one load rather than collapsing one.
    // The upstream signature (main.rs `Command::Run` dispatch) is unchanged.
    let loaded = std::sync::Arc::new(
        rocky_core::config::load_rocky_config_fingerprinted(config_path)
            .with_context(|| format!("failed to load config from {}", config_path.display()))?,
    );
    // Thin passthrough — routes to the existing run implementation.
    crate::commands::run::run(
        config_path,
        loaded,
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
        // The inline `rocky run` path mints its own timestamp run_id; the
        // two-step apply's verify_after gate is the only override consumer.
        None,
        // Inline `rocky run` is never a governed two-step apply — no gate.
        None,
        // `--assume-fresh-state` threads through from the CLI (main.rs
        // validated it against the configured `[state]` backend).
        assume_fresh_state,
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

        // A loadable (policy-free) config: since PR-B the gate HARD-loads the
        // single config snapshot up front (#1120 behavior delta), so the
        // fixture must carry one for the flow to reach the marker check.
        let config_path = dir.path().join("rocky.toml");
        std::fs::write(
            &config_path,
            "[adapter.db]\ntype = \"duckdb\"\npath = \"wh.duckdb\"\n",
        )?;

        // No marker → the gate bails before any warehouse work.
        let err = super::run_apply_ai_authored_plan(
            dir.path(),
            &config_path,
            &plan_id,
            std::path::Path::new("models/.rocky-state.redb"),
            PolicyPrincipal::Human,
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
    // agent-policy enforcement (seams 2 & 3)
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
    /// `principal` field (as if written before the policy plane). The `plan_id`
    /// is unchanged because `principal` rides outside the digest, so the
    /// integrity check still passes on read.
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
    /// Write a minimal compilable transformation model under `models_dir` so
    /// the apply-time re-derivation of the executable set (a fresh compile)
    /// finds at least one model to gate.
    fn write_min_model(models_dir: &Path, name: &str) {
        std::fs::create_dir_all(models_dir).unwrap();
        std::fs::write(models_dir.join(format!("{name}.sql")), "SELECT 1 AS id").unwrap();
        std::fs::write(
            models_dir.join(format!("{name}.toml")),
            format!(
                "name = \"{name}\"\n\n[strategy]\ntype = \"full_refresh\"\n\n\
                 [target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"{name}\"\n"
            ),
        )
        .unwrap();
    }

    #[tokio::test]
    async fn legacy_ai_authored_with_empty_policy_still_requires_review() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        std::fs::write(dir.path().join("rocky.toml"), EMPTY_POLICY_TOML)?;
        // The gate re-derives the executable set from a real compile, so the
        // plan must point at a real models dir with a compilable model.
        let models_dir = dir.path().join("models");
        write_min_model(&models_dir, "orders");
        let mut rp = minimal_run_plan();
        rp.models_dir = Some(models_dir.to_string_lossy().into_owned());
        rp.models = vec!["orders".to_string()];
        let plan_id = write_plan(dir.path(), PlanKind::AiAuthored, &rp)?;
        strip_principal_from_plan(dir.path(), &plan_id)?;

        let state = dir.path().join("state.redb");
        let err = super::run_apply_ai_authored_plan(
            dir.path(),
            &dir.path().join("rocky.toml"),
            &plan_id,
            &state,
            PolicyPrincipal::Human,
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
    /// without a marker is refused with the pre-policy-plane message (the hardcoded gate
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
            PolicyPrincipal::Human,
            true,
        )
        .await
        .expect_err("no [policy] block still gates ai_authored on the marker");
        let msg = err.to_string();
        assert!(
            msg.contains("not been reviewed") && msg.contains("rocky review"),
            "must be the pre-policy-plane marker message, got: {msg}"
        );
        Ok(())
    }

    /// Record an active agent freeze on `scope` into the state store at
    /// `state_path`, exactly as `rocky policy freeze --principal agent` does.
    fn seed_agent_freeze(state_path: &Path, scope: &str) -> anyhow::Result<()> {
        let store = StateStore::open(state_path)?;
        let now = chrono::Utc::now();
        store.record_policy_decision(&PolicyDecisionRecord {
            timestamp: now,
            plan_id: format!(
                "{}agent:{}",
                rocky_core::policy::FREEZE_PLAN_PREFIX,
                now.to_rfc3339()
            ),
            principal: PolicyPrincipal::Agent,
            capability: PolicyCapability::Apply,
            model: scope.to_string(),
            effect: PolicyEffect::Deny,
            rule_id: None,
            reason: "policy freeze: agent actions frozen to deny".to_string(),
            verify_after: Vec::new(),
            auto_apply: None,
        })?;
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

    /// Finding 8: the pre-gate remote sync must run ONLY when the gate will
    /// actually read the freeze/budget ledger — otherwise a backend blip would
    /// falsely abort a valid human Run/Promote plan. `remote_state_backend_for_gate`
    /// (now over a single loaded config snapshot — finding A) returns `Some`
    /// (sync warranted) only for {non-empty touched} ∧ {policy configured} ∧
    /// {remote backend}; `None` (skip the download) otherwise.
    #[test]
    fn remote_state_backend_for_gate_only_syncs_when_ledger_is_read() -> anyhow::Result<()> {
        use std::collections::BTreeMap;
        let dir = tempfile::tempdir()?;

        let mut touched = BTreeMap::new();
        touched.insert("orders".to_string(), PolicyCapability::Apply);
        let empty: BTreeMap<String, PolicyCapability> = BTreeMap::new();

        let state_s3 = "\n[state]\nbackend = \"s3\"\ns3_bucket = \"b\"\n";
        let load = |name: &str, body: String| -> anyhow::Result<rocky_core::config::RockyConfig> {
            let path = dir.path().join(name);
            std::fs::write(&path, body)?;
            Ok(rocky_core::config::load_rocky_config(&path)?)
        };

        // (1) policy + remote + non-empty touched ⇒ Some (sync warranted).
        let remote_policy = load(
            "remote_policy.toml",
            format!("{EMPTY_POLICY_TOML}{state_s3}"),
        )?;
        assert!(
            super::remote_state_backend_for_gate(&remote_policy, &touched).is_some(),
            "policy + remote backend + non-empty touched ⇒ pre-gate sync warranted"
        );

        // (1b) same config, EMPTY touched ⇒ None (gate short-circuits to Allow).
        assert!(
            super::remote_state_backend_for_gate(&remote_policy, &empty).is_none(),
            "empty touched ⇒ the gate reads no ledger ⇒ no pre-gate sync (finding 8)"
        );

        // (2) remote backend but NO [policy] ⇒ None (gate returns NotConfigured).
        let remote_nopolicy = load(
            "remote_nopolicy.toml",
            format!("{NO_POLICY_TOML}{state_s3}"),
        )?;
        assert!(
            super::remote_state_backend_for_gate(&remote_nopolicy, &touched).is_none(),
            "no [policy] block ⇒ gate returns NotConfigured, reads no ledger ⇒ no sync (finding 8)"
        );

        // (3) policy but LOCAL backend (no [state]) ⇒ None (nothing to pull).
        let local_policy = load("local_policy.toml", EMPTY_POLICY_TOML.to_string())?;
        assert!(
            super::remote_state_backend_for_gate(&local_policy, &touched).is_none(),
            "local backend ⇒ nothing to download"
        );

        // (An unloadable config never reaches the guard: the governed paths load
        // the snapshot with `.ok()` and skip the guard when it is `None`.)
        Ok(())
    }

    /// Findings 2 & 3: the ledger-seam helpers used by restore/backfill
    /// (upload-after / unconditional download) and the verify-after budget-pair
    /// uploads are REMOTE-ONLY (a Local backend / absent config is a no-op) and
    /// FAIL-CLOSED (a remote transfer failure propagates as `Err`).
    #[tokio::test]
    async fn ledger_seam_helpers_are_remote_only_and_fail_closed() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let state = dir.path().join("state.redb");
        let load = |name: &str, body: String| -> anyhow::Result<rocky_core::config::RockyConfig> {
            let path = dir.path().join(name);
            std::fs::write(&path, body)?;
            Ok(rocky_core::config::load_rocky_config(&path)?)
        };

        // Local backend (no [state]) + absent config ⇒ both helpers no-op Ok.
        let local_cfg = load("local.toml", NO_POLICY_TOML.to_string())?;
        assert!(matches!(local_cfg.state.backend, StateBackend::Local));
        for cfg in [Some(&local_cfg), None] {
            super::download_remote_ledger_unconditional(cfg, &state, "test")
                .await
                .expect("Local/absent ⇒ download is a no-op");
            super::upload_remote_ledger_fail_closed(cfg, &state, "test")
                .await
                .expect("Local/absent ⇒ upload is a no-op");
        }

        // Remote backend (S3, no bucket) ⇒ both helpers ATTEMPT the transfer
        // unconditionally and FAIL CLOSED (the misconfigured S3 dispatch errors).
        let remote_cfg = load(
            "remote.toml",
            format!("{NO_POLICY_TOML}\n[state]\nbackend = \"s3\"\n"),
        )?;
        assert!(matches!(remote_cfg.state.backend, StateBackend::S3));
        assert!(
            super::download_remote_ledger_unconditional(Some(&remote_cfg), &state, "test")
                .await
                .is_err(),
            "the unconditional download must attempt (and fail closed) on a remote backend"
        );
        // The upload helper needs a local file to exist (else upload_state
        // early-returns Ok before the backend is touched).
        {
            let s = StateStore::open(&state)?;
            drop(s);
        }
        assert!(
            super::upload_remote_ledger_fail_closed(Some(&remote_cfg), &state, "test")
                .await
                .is_err(),
            "the fail-closed upload must propagate a remote failure (on_upload_failure = Fail)"
        );
        Ok(())
    }

    /// Finding A: `evaluate_apply_policy_with_policy` evaluates the PASSED policy
    /// snapshot and never reloads the config from disk — so a governed path can
    /// use the SAME snapshot for the pre-gate sync decision and the gate, closing
    /// the config-snapshot TOCTOU. Proven by evaluating with a policy that has a
    /// deny rule (⇒ Deny) vs `None` (⇒ NotConfigured), with no config file at the
    /// gate's reach.
    #[test]
    fn evaluate_apply_policy_with_policy_uses_passed_snapshot_not_reload() -> anyhow::Result<()> {
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
        let policy = rocky_core::config::load_rocky_config(&config)?.policy;
        assert!(
            policy.is_some(),
            "the fixture config must carry a [policy] block"
        );

        let mut touched = BTreeMap::new();
        touched.insert("m".to_string(), PolicyCapability::Apply);
        // Point the gate at a DIFFERENT directory that has no config file, so a
        // reload would find nothing — only the passed policy can produce a Deny.
        let elsewhere = tempfile::tempdir()?;
        let models_dir = elsewhere.path().join("models");
        let state = elsewhere.path().join("state.redb");

        let gate = super::evaluate_apply_policy_with_policy(
            policy.as_ref(),
            "plan_a",
            PolicyPrincipal::Agent,
            &touched,
            &models_dir,
            &state,
        );
        assert!(
            matches!(gate, PolicyGate::Deny { .. }),
            "the PASSED policy (deny rule) must produce Deny even with no config file at the \
             gate's location; got {gate:?}"
        );

        let gate_none = super::evaluate_apply_policy_with_policy(
            None,
            "plan_a",
            PolicyPrincipal::Agent,
            &touched,
            &models_dir,
            &state,
        );
        assert!(
            matches!(gate_none, PolicyGate::NotConfigured),
            "no policy passed ⇒ NotConfigured (no disk reload); got {gate_none:?}"
        );
        Ok(())
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

    /// An empty `touched` map means the plan executes NO models — a genuine
    /// no-op — so it allows. (Under FIX 3, a no-change plan that still executes
    /// models no longer produces an empty `touched`: `EmbeddedCapabilities::
    /// touched` synthesizes an `apply`-capability entry per planned model. So
    /// reaching `evaluate_apply_policy` with an empty map now specifically
    /// means "nothing to execute", which is the only remaining Allow case.)
    #[test]
    fn evaluate_apply_policy_empty_touched_is_a_noop_allow() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        // A deny-all agent rule is present; an EMPTY touched set must still
        // allow because there is nothing to execute.
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
            "an empty touched set executes nothing ⇒ nothing to gate"
        );
        Ok(())
    }

    /// 🔴 FIX 3 regression: a no-change plan that still executes models is
    /// gated by a `deny agent apply` rule. Pre-fix `touched()` returned an
    /// empty map for a no-change plan, and `evaluate_apply_policy`
    /// short-circuited to `Allow` — so the plan executed every model past the
    /// deny. Post-fix `touched()` synthesizes an `apply` entry per planned
    /// model, so the deny fires.
    #[test]
    fn no_change_plan_gated_by_deny_agent_apply() -> anyhow::Result<()> {
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
        // A no-change plan: diff available, zero changed models, but planned
        // models exist → touched under the bare `apply` verb.
        let caps = crate::plan_store::EmbeddedCapabilities {
            diff_available: true,
            changed: BTreeMap::new(),
            models_fingerprint: None,
            config_identity: None,
            fingerprint_version: 0,
            reviewed_source_schemas: None,
        };
        let touched = caps.touched(&["m".to_string()]);
        assert!(!touched.is_empty(), "FIX 3 must synthesize a touched set");
        let gate = super::evaluate_apply_policy(
            &config,
            "plan_x",
            PolicyPrincipal::Agent,
            &touched,
            &dir.path().join("models"),
            &dir.path().join("state.redb"),
        );
        assert!(
            matches!(gate, PolicyGate::Deny { .. }),
            "a no-change agent plan that executes models must hit the deny, got {gate:?}"
        );
        Ok(())
    }

    /// 🔴 FIX 3 + FIX 8 regression: an active agent freeze blocks a no-change
    /// plan's apply. Requires both the synthesized touched set (FIX 3) and the
    /// ledger snapshot actually reading the freeze (FIX 8).
    #[test]
    fn no_change_plan_blocked_by_active_agent_freeze() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        // Empty [policy] block: no rules, so absent a freeze an agent apply
        // falls to default_agent_effect (require_review). The freeze forces deny.
        let config = write_config(dir.path(), "")?;
        let state = dir.path().join("state.redb");
        seed_agent_freeze(&state, "any")?;

        let caps = crate::plan_store::EmbeddedCapabilities {
            diff_available: true,
            changed: BTreeMap::new(),
            models_fingerprint: None,
            config_identity: None,
            fingerprint_version: 0,
            reviewed_source_schemas: None,
        };
        let touched = caps.touched(&["m".to_string()]);
        let gate = super::evaluate_apply_policy(
            &config,
            "plan_x",
            PolicyPrincipal::Agent,
            &touched,
            &dir.path().join("models"),
            &state,
        );
        assert!(
            matches!(gate, PolicyGate::Deny { .. }),
            "an active agent freeze must force a no-change plan's apply to deny, got {gate:?}"
        );
        Ok(())
    }

    /// A human no-change plan with no policy rules still applies (humans are
    /// ungated in v0) — the FIX 3 synthesis must not gate a human.
    #[test]
    fn human_no_change_plan_without_rules_applies() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let config = write_config(dir.path(), "")?;
        let caps = crate::plan_store::EmbeddedCapabilities {
            diff_available: true,
            changed: BTreeMap::new(),
            models_fingerprint: None,
            config_identity: None,
            fingerprint_version: 0,
            reviewed_source_schemas: None,
        };
        let touched = caps.touched(&["m".to_string()]);
        let gate = super::evaluate_apply_policy(
            &config,
            "plan_x",
            PolicyPrincipal::Human,
            &touched,
            &dir.path().join("models"),
            &dir.path().join("state.redb"),
        );
        assert_eq!(gate, PolicyGate::Allow, "a human is never gated in v0");
        Ok(())
    }

    /// 🔴 D1 regression: the executable set is re-derived from a fresh compile,
    /// NOT the plan's informational `models` list. A run plan whose `models`
    /// list is EMPTY but whose models dir compiles real models must still gate
    /// the real models. Pre-fix `touched()` read the empty list → gated nothing
    /// → an agent apply executed every real model UNGATED.
    #[test]
    fn run_executable_models_ignores_the_informational_list() {
        let dir = tempfile::tempdir().unwrap();
        let models_dir = dir.path().join("models");
        write_min_model(&models_dir, "orders");
        write_min_model(&models_dir, "customers");
        let mut rp = minimal_run_plan();
        rp.models_dir = Some(models_dir.to_string_lossy().into_owned());
        rp.models = Vec::new(); // the informational list is EMPTY
        rp.model = None;
        let exec = super::run_executable_models(&models_dir, &rp);
        assert!(
            exec.contains(&"orders".to_string()) && exec.contains(&"customers".to_string()),
            "the executable set must come from the compile, not the empty list: {exec:?}"
        );
    }

    /// 🔴 D1 regression (over-listing): `--model orders` must narrow the
    /// executable set to just `orders`, even if the plan's informational
    /// `models` list names every compiled model — so a rule scoped to an
    /// unexecuted model does not wrongly fire.
    #[test]
    fn run_executable_models_honors_the_model_filter() {
        let dir = tempfile::tempdir().unwrap();
        let models_dir = dir.path().join("models");
        write_min_model(&models_dir, "orders");
        write_min_model(&models_dir, "customers");
        let mut rp = minimal_run_plan();
        rp.models_dir = Some(models_dir.to_string_lossy().into_owned());
        rp.models = vec!["orders".to_string(), "customers".to_string()]; // over-lists
        rp.model = Some("orders".to_string());
        let exec = super::run_executable_models(&models_dir, &rp);
        assert_eq!(
            exec,
            vec!["orders".to_string()],
            "only the selected model executes"
        );
    }

    /// 🔴 D1(a) end-to-end: an agent run plan with an EMPTY informational
    /// `models` list but real compilable models is DENIED by `deny agent apply
    /// { any }`. Pre-fix the empty list → empty touched → `Allow` → the run
    /// executed every model past the deny.
    #[test]
    fn empty_informational_list_with_real_models_is_gated() -> anyhow::Result<()> {
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
        let models_dir = dir.path().join("models");
        write_min_model(&models_dir, "orders");
        let mut rp = minimal_run_plan();
        rp.models_dir = Some(models_dir.to_string_lossy().into_owned());
        rp.models = Vec::new();
        rp.model = None;
        let plan_id = write_plan(dir.path(), PlanKind::Run, &rp)?;
        let plan = read_plan(dir.path(), &plan_id)?;

        let exec = super::run_executable_models(&models_dir, &rp);
        let touched = super::touched_models_for_run(&plan, &exec);
        assert!(!touched.is_empty(), "D1: real models must be gated");
        let gate = super::evaluate_apply_policy(
            &config,
            &plan_id,
            PolicyPrincipal::Agent,
            &touched,
            &models_dir,
            &dir.path().join("state.redb"),
        );
        assert!(
            matches!(gate, PolicyGate::Deny { .. }),
            "an agent run over real models must hit the deny, got {gate:?}"
        );
        Ok(())
    }

    /// 🔴 D1(b) end-to-end: `--model orders` must NOT fire a `deny agent apply
    /// { models = ["customers"] }` rule, because `customers` does not execute.
    /// Pre-fix the over-listing informational `models` gated `customers` too →
    /// the run was wrongly denied.
    #[test]
    fn model_filter_does_not_fire_an_unexecuted_models_rule() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let config = write_config(
            dir.path(),
            r#"
[[policy.rules]]
principal = "agent"
capability = "apply"
scope = { models = ["customers"] }
effect = "deny"
"#,
        )?;
        let models_dir = dir.path().join("models");
        write_min_model(&models_dir, "orders");
        write_min_model(&models_dir, "customers");
        let mut rp = minimal_run_plan();
        rp.models_dir = Some(models_dir.to_string_lossy().into_owned());
        rp.models = vec!["orders".to_string(), "customers".to_string()]; // over-lists
        rp.model = Some("orders".to_string());
        let plan_id = write_plan(dir.path(), PlanKind::Run, &rp)?;
        let plan = read_plan(dir.path(), &plan_id)?;

        let exec = super::run_executable_models(&models_dir, &rp);
        let touched = super::touched_models_for_run(&plan, &exec);
        assert!(
            !touched.contains_key("customers"),
            "customers does not execute and must not be gated: {touched:?}"
        );
        let gate = super::evaluate_apply_policy(
            &config,
            &plan_id,
            PolicyPrincipal::Agent,
            &touched,
            &models_dir,
            &dir.path().join("state.redb"),
        );
        assert!(
            !matches!(gate, PolicyGate::Deny { .. }),
            "the customers deny must not fire for a run scoped to orders, got {gate:?}"
        );
        Ok(())
    }

    /// 🔴 D4-analog (run side): a partial-change agent run must still gate an
    /// UNCHANGED sibling that re-materializes. With `deny agent apply {
    /// models = ["prod_critical"] }` + `allow agent schema_change.additive
    /// { any }`, a run where `stg_x` changed additively (allowed) but
    /// `prod_critical` is unchanged yet executes must be DENIED — the unchanged
    /// sibling is gated under `apply` and hits the deny. A narrow "gate only
    /// changed models" would have let `prod_critical` rebuild ungated.
    #[test]
    fn partial_change_run_gates_unchanged_sibling_under_model_scoped_deny() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let config = write_config(
            dir.path(),
            r#"
[[policy.rules]]
principal = "agent"
capability = "apply"
scope = { models = ["prod_critical"] }
effect = "deny"

[[policy.rules]]
principal = "agent"
capability = "schema_change.additive"
scope = { any = true }
effect = "allow"
"#,
        )?;
        let caps = crate::plan_store::EmbeddedCapabilities {
            diff_available: true,
            changed: {
                let mut c = BTreeMap::new();
                c.insert("stg_x".to_string(), PolicyCapability::SchemaChangeAdditive);
                c
            },
            models_fingerprint: None,
            config_identity: None,
            fingerprint_version: 0,
            reviewed_source_schemas: None,
        };
        // Both models execute; only stg_x changed.
        let touched = caps.touched(&["stg_x".to_string(), "prod_critical".to_string()]);
        assert_eq!(touched.get("prod_critical"), Some(&PolicyCapability::Apply));
        let gate = super::evaluate_apply_policy(
            &config,
            "plan_x",
            PolicyPrincipal::Agent,
            &touched,
            &dir.path().join("models"),
            &dir.path().join("state.redb"),
        );
        assert!(
            matches!(gate, PolicyGate::Deny { .. }),
            "the unchanged executing sibling must hit the model-scoped deny, got {gate:?}"
        );
        Ok(())
    }

    /// 🔴 FIX 8 regression (read-only snapshot ignores the write lock): a
    /// freeze recorded in the ledger must still gate an apply while another
    /// process holds the advisory WRITE lock. Pre-fix the snapshot was taken
    /// with `StateStore::open` (a write open) which fails under the held lock,
    /// leaving an empty snapshot → the freeze was invisible → the apply
    /// proceeded (require_review, not deny). Post-fix the snapshot is taken via
    /// `open_read_only`, which ignores the advisory lock.
    #[test]
    fn snapshot_sees_freeze_while_write_lock_held_elsewhere() -> anyhow::Result<()> {
        use fs4::FileExt;

        let dir = tempfile::tempdir()?;
        let config = write_config(dir.path(), "")?; // empty [policy]: agent → require_review
        let state = dir.path().join("state.redb");
        seed_agent_freeze(&state, "any")?;

        // Simulate a concurrent writer: hold the advisory lock on the .lock
        // file directly (no redb handle), exactly as a second `rocky run` on
        // another process would (see rocky-core/tests/state_lock.rs).
        let lock_path = state.with_extension("redb.lock");
        let external_lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)?;
        FileExt::try_lock(&external_lock).expect("external write lock should be acquired");

        let touched = {
            let mut m = BTreeMap::new();
            m.insert("m".to_string(), PolicyCapability::Apply);
            m
        };
        let gate = super::evaluate_apply_policy(
            &config,
            "plan_x",
            PolicyPrincipal::Agent,
            &touched,
            &dir.path().join("models"),
            &state,
        );
        FileExt::unlock(&external_lock).ok();
        assert!(
            matches!(gate, PolicyGate::Deny { .. }),
            "the freeze must be visible through the read-only snapshot despite the held write \
             lock, got {gate:?}"
        );
        Ok(())
    }

    /// 🔴 FIX 8 + D3 regression (fail-closed HARD-REFUSE on a genuinely
    /// unreadable store): when BOTH opens fail (a corrupt / forward-incompatible
    /// store) an AGENT's otherwise-`allow` decision is DENIED — not merely
    /// `require_review`, which a pre-existing review marker could satisfy and so
    /// bypass a possibly-active freeze. Pre-D3 this degraded to `require_review`.
    #[test]
    fn unreadable_store_hard_refuses_agent_mutation() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        // A rule that would otherwise grant the agent an outright allow.
        let config = write_config(
            dir.path(),
            r#"
[[policy.rules]]
principal = "agent"
capability = "apply"
scope = { any = true }
effect = "allow"
"#,
        )?;
        // Make the state path a DIRECTORY so both `open` and `open_read_only`
        // fail deterministically (redb cannot create a file where a directory
        // exists) — a genuinely unreadable store.
        let state = dir.path().join("state.redb");
        std::fs::create_dir(&state)?;

        let touched = {
            let mut m = BTreeMap::new();
            m.insert("m".to_string(), PolicyCapability::Apply);
            m
        };
        let gate = super::evaluate_apply_policy(
            &config,
            "plan_x",
            PolicyPrincipal::Agent,
            &touched,
            &dir.path().join("models"),
            &state,
        );
        assert!(
            matches!(gate, PolicyGate::Deny { .. }),
            "an unreadable ledger must HARD-REFUSE an agent mutation (deny, not require_review), \
             got {gate:?}"
        );
        Ok(())
    }

    /// 🔴 D3 regression (live redb handle, not just the advisory lock): a REAL
    /// concurrent run holds an actual `StateStore` write handle (redb's own
    /// exclusive flock), so even `open_read_only` exhausts its retries and
    /// returns `Busy` → the snapshot is unavailable. An agent apply must be
    /// hard-refused (deny). The advisory-lock-only test above never exercised a
    /// live redb handle; this one does.
    #[test]
    fn live_redb_handle_hard_refuses_agent_apply() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let config = write_config(
            dir.path(),
            r#"
[[policy.rules]]
principal = "agent"
capability = "apply"
scope = { any = true }
effect = "allow"
"#,
        )?;
        let state = dir.path().join("state.redb");
        // Hold a LIVE redb write handle for the whole evaluation — redb takes an
        // in-process exclusive handle, so both `open` and the retrying
        // `open_read_only` fail (DatabaseAlreadyOpen → Busy). The snapshot is
        // genuinely unavailable even though the file is a perfectly valid store.
        let _live = StateStore::open(&state)?;

        let touched = {
            let mut m = BTreeMap::new();
            m.insert("m".to_string(), PolicyCapability::Apply);
            m
        };
        let gate = super::evaluate_apply_policy(
            &config,
            "plan_x",
            PolicyPrincipal::Agent,
            &touched,
            &dir.path().join("models"),
            &state,
        );
        assert!(
            matches!(gate, PolicyGate::Deny { .. }),
            "a live concurrent redb handle must hard-refuse an agent apply (deny), got {gate:?}"
        );
        Ok(())
    }

    /// The fail-closed floor does NOT apply to a human: an unreadable store
    /// leaves a human's ungated apply untouched (humans are never gated in v0).
    #[test]
    fn unreadable_store_does_not_gate_human() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let config = write_config(dir.path(), "")?;
        let state = dir.path().join("state.redb");
        std::fs::create_dir(&state)?;
        let touched = {
            let mut m = BTreeMap::new();
            m.insert("m".to_string(), PolicyCapability::Apply);
            m
        };
        let gate = super::evaluate_apply_policy(
            &config,
            "plan_x",
            PolicyPrincipal::Human,
            &touched,
            &dir.path().join("models"),
            &state,
        );
        assert_eq!(gate, PolicyGate::Allow, "a human is never gated in v0");
        Ok(())
    }

    #[test]
    fn touched_models_for_run_fail_closed_without_embed() {
        // A plan with no embedded classification (legacy / diff skipped) marks
        // every EXECUTABLE model breaking.
        let dir = tempfile::tempdir().unwrap();
        let plan_id = write_plan(dir.path(), PlanKind::AiAuthored, &minimal_run_plan()).unwrap();
        let plan = read_plan(dir.path(), &plan_id).unwrap();
        // The executable set is passed explicitly now (re-derived from a
        // compile at the real call sites); here we pass the model directly.
        let executable = vec!["schema.orders".to_string()];
        let touched = super::touched_models_for_run(&plan, &executable);
        assert_eq!(
            touched.get("schema.orders"),
            Some(&PolicyCapability::SchemaChangeBreaking)
        );
    }

    /// 🔴 FIX 5 regression: a promote plan with SQL targets but NO captured
    /// breaking-change findings must still gate every target under `promote`.
    /// Pre-fix an absent/empty `breaking_changes` returned an empty touched set
    /// → `Allow` → the promote's `CREATE OR REPLACE` executed ungated.
    #[test]
    fn promote_without_findings_gates_its_targets() {
        let promote = crate::output::PromotePlan {
            branch_name: "fix".to_string(),
            base_ref: "main".to_string(),
            head_ref: "abc".to_string(),
            branch_state_hash: "h".to_string(),
            approvals_used: vec![],
            approvals_rejected: vec![],
            breaking_changes: None,
            allow_breaking: false,
            targets: vec![crate::output::PromoteTargetPlan {
                target: "cat.prod.orders".to_string(),
                source: "cat.branch.orders".to_string(),
                statement: "CREATE OR REPLACE TABLE ...".to_string(),
            }],
            plan_audit: vec![],
            created_at: chrono::Utc::now(),
        };
        // Empty models_dir → nothing compiles; the fail-closed path must still
        // gate the plan's SQL target.
        let touched = super::touched_models_for_promote(&promote, Path::new("/nonexistent"));
        assert_eq!(
            touched.get("cat.prod.orders"),
            Some(&PolicyCapability::Promote),
            "a findings-less promote must gate each SQL target under `promote`, got {touched:?}"
        );
    }

    /// 🔴 FIX 5 regression: when the target→name mapping is unavailable (the
    /// project fails to compile at apply time), a changed target must stay in
    /// the touched set under its target name — not be silently dropped.
    #[test]
    fn promote_with_unmappable_target_keeps_it_fail_closed() {
        use rocky_core::breaking_change::{BreakingChange, BreakingFinding, BreakingSeverity};
        let promote = crate::output::PromotePlan {
            branch_name: "fix".to_string(),
            base_ref: "main".to_string(),
            head_ref: "abc".to_string(),
            branch_state_hash: "h".to_string(),
            approvals_used: vec![],
            approvals_rejected: vec![],
            breaking_changes: Some(vec![BreakingFinding {
                change: BreakingChange::ColumnDropped {
                    model: "cat.prod.orders".to_string(),
                    column: "amount".to_string(),
                    data_type: "INT".to_string(),
                },
                severity: BreakingSeverity::Breaking,
            }]),
            allow_breaking: true,
            targets: vec![],
            plan_audit: vec![],
            created_at: chrono::Utc::now(),
        };
        // No compilable project → target_to_name is empty → the fail-closed
        // path keeps the changed target under its own name.
        let touched = super::touched_models_for_promote(&promote, Path::new("/nonexistent"));
        assert_eq!(
            touched.get("cat.prod.orders"),
            Some(&PolicyCapability::Promote),
            "an unmappable changed target must be gated under its target name, got {touched:?}"
        );
    }

    /// A promote that executes NOTHING (no findings, no targets) stays empty —
    /// nothing to gate.
    #[test]
    fn promote_with_no_targets_and_no_findings_is_empty() {
        let promote = crate::output::PromotePlan {
            branch_name: "fix".to_string(),
            base_ref: "main".to_string(),
            head_ref: "abc".to_string(),
            branch_state_hash: "h".to_string(),
            approvals_used: vec![],
            approvals_rejected: vec![],
            breaking_changes: None,
            allow_breaking: false,
            targets: vec![],
            plan_audit: vec![],
            created_at: chrono::Utc::now(),
        };
        assert!(super::touched_models_for_promote(&promote, Path::new("/nonexistent")).is_empty());
    }

    /// 🔴 D4 regression: with NON-empty findings, the promote gate must still
    /// gate EVERY executable target — not just the finding-scoped ones. A SQL
    /// target that produced no finding (unchanged / additive-only) must not
    /// escape a `deny agent promote { models = [...] }` rule. Pre-fix only
    /// finding targets were gated and the other targets ran silently.
    #[test]
    fn promote_with_findings_still_gates_non_finding_targets() {
        use rocky_core::breaking_change::{BreakingChange, BreakingFinding, BreakingSeverity};
        let dir = tempfile::tempdir().unwrap();
        let models_dir = dir.path().join("models");
        write_min_model(&models_dir, "orders"); // FQN c.s.orders
        write_min_model(&models_dir, "customers"); // FQN c.s.customers

        let promote = crate::output::PromotePlan {
            branch_name: "fix".to_string(),
            base_ref: "main".to_string(),
            head_ref: "abc".to_string(),
            branch_state_hash: "h".to_string(),
            approvals_used: vec![],
            approvals_rejected: vec![],
            // Only `orders` produced a finding …
            breaking_changes: Some(vec![BreakingFinding {
                change: BreakingChange::SqlBodyChanged {
                    model: "c.s.orders".to_string(),
                },
                severity: BreakingSeverity::Breaking,
            }]),
            allow_breaking: true,
            // … but BOTH targets execute.
            targets: vec![
                crate::output::PromoteTargetPlan {
                    target: "c.s.orders".to_string(),
                    source: "c.b.orders".to_string(),
                    statement: "CREATE OR REPLACE ...".to_string(),
                },
                crate::output::PromoteTargetPlan {
                    target: "c.s.customers".to_string(),
                    source: "c.b.customers".to_string(),
                    statement: "CREATE OR REPLACE ...".to_string(),
                },
            ],
            plan_audit: vec![],
            created_at: chrono::Utc::now(),
        };
        let touched = super::touched_models_for_promote(&promote, &models_dir);
        // Both targets — mapped to their logical names — are gated, even though
        // only `orders` produced a finding.
        assert_eq!(touched.get("orders"), Some(&PolicyCapability::Promote));
        assert_eq!(
            touched.get("customers"),
            Some(&PolicyCapability::Promote),
            "the non-finding target must still be gated (D4), got {touched:?}"
        );
    }

    /// 🔴 D5 regression: with a real models dir, a target FQN is mapped to its
    /// LOGICAL model name so a `models = ["orders"]`-scoped rule matches. Pre-
    /// fix the call site hardcoded `models/` (unresolved in the test cwd) so the
    /// target stayed an FQN and a name-scoped rule missed.
    #[test]
    fn promote_maps_target_fqn_to_logical_name() {
        let dir = tempfile::tempdir().unwrap();
        let models_dir = dir.path().join("models");
        write_min_model(&models_dir, "orders"); // logical "orders" → FQN c.s.orders

        let promote = crate::output::PromotePlan {
            branch_name: "fix".to_string(),
            base_ref: "main".to_string(),
            head_ref: "abc".to_string(),
            branch_state_hash: "h".to_string(),
            approvals_used: vec![],
            approvals_rejected: vec![],
            breaking_changes: None,
            allow_breaking: false,
            targets: vec![crate::output::PromoteTargetPlan {
                target: "c.s.orders".to_string(),
                source: "c.b.orders".to_string(),
                statement: "CREATE OR REPLACE ...".to_string(),
            }],
            plan_audit: vec![],
            created_at: chrono::Utc::now(),
        };
        let touched = super::touched_models_for_promote(&promote, &models_dir);
        assert!(
            touched.contains_key("orders"),
            "the FQN must map to the logical name 'orders': {touched:?}"
        );
        assert!(
            !touched.contains_key("c.s.orders"),
            "the FQN must not remain when it is mappable: {touched:?}"
        );
    }

    /// `resolve_config_models_dir` reads the transformation pipeline's `models`
    /// glob base relative to the config parent (the D5 threading), falling back
    /// to `<project>/models`.
    #[test]
    fn resolve_config_models_dir_reads_the_glob() {
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("rocky.toml");
        std::fs::write(
            &config,
            r#"
[adapter]
type = "duckdb"
path = "x.duckdb"

[pipeline.p]
type = "transformation"
models = "custom_models/**"

[pipeline.p.target.governance]
auto_create_schemas = true
"#,
        )
        .unwrap();
        let loaded = rocky_core::config::load_rocky_config(&config).ok();
        assert_eq!(
            super::resolve_config_models_dir(&config, loaded.as_ref()),
            dir.path().join("custom_models")
        );
        // Missing config (None snapshot) → fallback to <project>/models.
        assert_eq!(
            super::resolve_config_models_dir(&dir.path().join("missing.toml"), None),
            dir.path().join("models")
        );
    }

    /// 🔴 #4 (routing identity): `config_policy_identity` captures ROUTING —
    /// the physical destination (`path`), account, adapter type, target — so a
    /// change there refuses; but a CREDENTIAL change (token/password, a
    /// `RedactedString`) does NOT, because it serializes to `"***"`. This is the
    /// corrected equality (round-5 wrongly treated `path` as a secret).
    #[test]
    fn config_identity_captures_routing_but_not_credentials() {
        fn cfg(body: &str) -> rocky_core::config::RockyConfig {
            let dir = tempfile::tempdir().unwrap();
            let p = dir.path().join("rocky.toml");
            std::fs::write(&p, body).unwrap();
            rocky_core::config::load_rocky_config(&p).unwrap()
        }
        let base = |path: &str, tok: &str| {
            format!(
                "[adapter]\ntype = \"databricks\"\nhost = \"h.example.com\"\nhttp_path = \"{path}\"\ntoken = \"{tok}\"\n\n[pipeline.p]\ntype = \"transformation\"\nmodels = \"models/**\"\n\n[pipeline.p.target]\nadapter = \"default\"\n"
            )
        };
        let id_a = super::config_policy_identity(&cfg(&base("/sql/1", "SECRET_A")));
        // A rotated CREDENTIAL (token, a RedactedString) must NOT change the
        // identity — it serializes to "***".
        assert_eq!(
            id_a,
            super::config_policy_identity(&cfg(&base("/sql/1", "SECRET_B"))),
            "a rotated credential must not change the routing identity (no spurious refuse)"
        );
        // The identity must NOT leak the secret.
        assert!(
            !id_a.contains("SECRET_A"),
            "the identity must redact credentials"
        );
        // A ROUTING change (http_path — where statements are sent) MUST change it.
        assert_ne!(
            id_a,
            super::config_policy_identity(&cfg(&base("/sql/2", "SECRET_A"))),
            "a routing change (http_path) must change the identity"
        );

        // A DuckDB `path` swap (a different physical DB file) MUST change it.
        let duck = |path: &str| {
            format!(
                "[adapter]\ntype = \"duckdb\"\npath = \"{path}\"\n\n[pipeline.p]\ntype = \"transformation\"\nmodels = \"models/**\"\n\n[pipeline.p.target]\nadapter = \"default\"\n"
            )
        };
        assert_ne!(
            super::config_policy_identity(&cfg(&duck("a.duckdb"))),
            super::config_policy_identity(&cfg(&duck("b.duckdb"))),
            "a DuckDB path swap writes a different physical DB — must change the identity"
        );
        // An adapter-type swap MUST change it, and it flows into the fingerprint.
        let snow = super::config_policy_identity(&cfg(
            "[adapter]\ntype = \"snowflake\"\naccount = \"x\"\n\n[pipeline.p]\ntype = \"transformation\"\nmodels = \"models/**\"\n\n[pipeline.p.target]\nadapter = \"default\"\n",
        ));
        let models: Vec<rocky_core::models::Model> = Vec::new();
        let extras = super::ExecutionExtras::default();
        assert_ne!(
            super::execution_ir_fingerprint(&models, &id_a, "", "", &extras),
            super::execution_ir_fingerprint(&models, &snow, "", "", &extras),
            "the routing identity must change the execution fingerprint"
        );

        // Governance policy (mask) must NOT leak into the ROUTING identity
        // (finding #5): the routing gate is env-free and must not refuse on a
        // governance edit. A `[mask]` change leaves `config_policy_identity`
        // unchanged — governance lives in `governance_policy_identity` instead.
        let masked = |strategy: &str| {
            format!(
                "[adapter]\ntype = \"duckdb\"\npath = \"a.duckdb\"\n\n[mask]\npii = \"{strategy}\"\n\n[pipeline.p]\ntype = \"transformation\"\nmodels = \"models/**\"\n\n[pipeline.p.target]\nadapter = \"default\"\n"
            )
        };
        assert_eq!(
            super::config_policy_identity(&cfg(&masked("hash"))),
            super::config_policy_identity(&cfg(&masked("redact"))),
            "a [mask] change must NOT touch the routing identity (routing-only)"
        );
    }

    /// 🔴 #5/#6/#2/#4a (governance identity): the env-resolved governance
    /// identity moves on a mutation-driving change (mask strategy, role
    /// permission, `[cache.schemas]` toggle) but NOT on advisory
    /// `[classifications].allow_unmasked` — including an env-var-templated value
    /// that resolves differently across processes (the false-refuse #5 fixes).
    #[test]
    fn governance_identity_binds_mutation_not_advisory() {
        fn cfg(body: &str) -> rocky_core::config::RockyConfig {
            let dir = tempfile::tempdir().unwrap();
            let p = dir.path().join("rocky.toml");
            std::fs::write(&p, body).unwrap();
            rocky_core::config::load_rocky_config(&p).unwrap()
        }
        let gid = |c: &rocky_core::config::RockyConfig| super::governance_policy_identity(c);
        let base = "[adapter]\ntype = \"duckdb\"\npath = \"a.duckdb\"\n\n[pipeline.p]\ntype = \"transformation\"\nmodels = \"models/**\"\n\n[pipeline.p.target]\nadapter = \"default\"\n";

        // The mask is NOT in the governance identity anymore (it is model-
        // dependent → `ExecutionExtras::effective_masks`, finding C). A `[mask]`
        // change must NOT move the model-independent governance identity.
        assert_eq!(
            gid(&cfg(&format!("{base}\n[mask]\npii = \"hash\"\n"))),
            gid(&cfg(&format!("{base}\n[mask]\npii = \"redact\"\n"))),
            "the mask must not be in the model-independent governance identity (C)"
        );

        // #2 role permission change (SELECT → MANAGE) → identity moves.
        let role = |perm: &str| format!("{base}\n[role.analyst]\npermissions = [\"{perm}\"]\n");
        assert_ne!(
            gid(&cfg(&role("SELECT"))),
            gid(&cfg(&role("MANAGE"))),
            "a role permission change must move the governance identity (#2)"
        );

        // 🔴 D (over-binding roles): reordering `inherits` that yields the SAME
        // flattened permissions must NOT move the identity (GRANTs depend only on
        // the flattened set; inherits is logging-only).
        let two_parents = format!(
            "{base}\n[role.a]\npermissions = [\"SELECT\"]\n\n[role.b]\npermissions = [\"MODIFY\"]\n\n[role.c]\ninherits = [{}]\n",
            "\"a\", \"b\""
        );
        let reordered = format!(
            "{base}\n[role.a]\npermissions = [\"SELECT\"]\n\n[role.b]\npermissions = [\"MODIFY\"]\n\n[role.c]\ninherits = [{}]\n",
            "\"b\", \"a\""
        );
        assert_eq!(
            gid(&cfg(&two_parents)),
            gid(&cfg(&reordered)),
            "reordering equivalent inherits must NOT move the governance identity (D)"
        );

        // #4a [cache.schemas] toggle → identity moves (changes whether apply
        // resolves concrete typed_columns vs Unknown).
        assert_ne!(
            gid(&cfg(&format!("{base}\n[cache.schemas]\nenabled = true\n"))),
            gid(&cfg(&format!("{base}\n[cache.schemas]\nenabled = false\n"))),
            "a [cache.schemas] toggle must move the governance identity (#4a)"
        );

        // 🔴 #5 (the false-refuse this fixes): advisory `allow_unmasked` — even
        // with an env-var-templated value resolving DIFFERENTLY across processes
        // — must NOT move the identity (it drives no warehouse action). Simulate
        // the two processes by setting the env var to different values.
        // SAFETY: single-threaded test; restore after.
        unsafe { std::env::set_var("ROCKY_TEST_TAG_5", "internal") };
        let a = gid(&cfg(&format!(
            "{base}\n[classifications]\nallow_unmasked = [\"${{ROCKY_TEST_TAG_5:-internal}}\"]\n"
        )));
        unsafe { std::env::set_var("ROCKY_TEST_TAG_5", "public") };
        let b = gid(&cfg(&format!(
            "{base}\n[classifications]\nallow_unmasked = [\"${{ROCKY_TEST_TAG_5:-internal}}\"]\n"
        )));
        unsafe { std::env::remove_var("ROCKY_TEST_TAG_5") };
        assert_eq!(
            a, b,
            "advisory classifications must NOT move the governance identity — \
             an env-var-templated allow_unmasked would otherwise false-refuse (#5)"
        );
    }

    /// 🔴 #7 (fail-closed on fingerprint-production failure): a NEW governed
    /// plan (`require`) whose fingerprint is missing REFUSES; a genuinely-legacy
    /// plan (`!require`, no fingerprint) is allowed; a live mismatch refuses.
    #[test]
    fn exec_fingerprint_gate_fail_closed_semantics() {
        let m: Vec<rocky_core::models::Model> = Vec::new();
        let extras = super::ExecutionExtras::default();
        let expected = super::execution_ir_fingerprint(&m, "c", "g", "", &extras).unwrap();
        // Genuinely-legacy (no fingerprint, not required) → allowed.
        super::ExecFingerprintGate {
            expected: None,
            config_identity: "c".to_string(),
            governance_identity: "g".to_string(),
            exec_control_identity: String::new(),
            resolved_mask: std::collections::BTreeMap::new(),
            reviewed_source_schemas: None,
            plan_id: "p".to_string(),
            require: false,
        }
        .verify(&m, &extras)
        .expect("a legacy plan without a fingerprint is allowed through");
        // NEW plan whose fingerprint could not be produced (required) → REFUSE.
        let err = super::ExecFingerprintGate {
            expected: None,
            config_identity: "c".to_string(),
            governance_identity: "g".to_string(),
            exec_control_identity: String::new(),
            resolved_mask: std::collections::BTreeMap::new(),
            reviewed_source_schemas: None,
            plan_id: "p".to_string(),
            require: true,
        }
        .verify(&m, &extras)
        .expect_err("a governed plan with no fingerprint must refuse (#7)");
        assert!(err.to_string().contains("could not be produced"), "{err}");
        // Matching → ok; live mismatch → refuse.
        super::ExecFingerprintGate {
            expected: Some(expected.clone()),
            config_identity: "c".to_string(),
            governance_identity: "g".to_string(),
            exec_control_identity: String::new(),
            resolved_mask: std::collections::BTreeMap::new(),
            reviewed_source_schemas: None,
            plan_id: "p".to_string(),
            require: true,
        }
        .verify(&m, &extras)
        .expect("a matching fingerprint applies");
        assert!(
            super::ExecFingerprintGate {
                expected: Some(expected.clone()),
                config_identity: "DIFFERENT".to_string(),
                governance_identity: "g".to_string(),
                exec_control_identity: String::new(),
                resolved_mask: std::collections::BTreeMap::new(),
                reviewed_source_schemas: None,
                plan_id: "p".to_string(),
                require: true,
            }
            .verify(&m, &extras)
            .is_err(),
            "a config-identity change must refuse"
        );
        // A GOVERNANCE-identity change must refuse too (mask / roles / cache).
        assert!(
            super::ExecFingerprintGate {
                expected: Some(expected),
                config_identity: "c".to_string(),
                governance_identity: "DIFFERENT".to_string(),
                exec_control_identity: String::new(),
                resolved_mask: std::collections::BTreeMap::new(),
                reviewed_source_schemas: None,
                plan_id: "p".to_string(),
                require: true,
            }
            .verify(&m, &extras)
            .is_err(),
            "a governance-identity change must refuse"
        );
        // #1095: an EXECUTION-CONTROL identity change (skip/reuse/resilience or a
        // governed hook) must refuse too — bound into the fingerprint alongside
        // routing/governance.
        let expected_ec = super::execution_ir_fingerprint(&m, "c", "g", "", &extras).unwrap();
        assert!(
            super::ExecFingerprintGate {
                expected: Some(expected_ec),
                config_identity: "c".to_string(),
                governance_identity: "g".to_string(),
                exec_control_identity: "DIFFERENT".to_string(),
                resolved_mask: std::collections::BTreeMap::new(),
                reviewed_source_schemas: None,
                plan_id: "p".to_string(),
                require: true,
            }
            .verify(&m, &extras)
            .is_err(),
            "an execution-control identity change must refuse (#1095)"
        );
    }

    /// #1095(a) closure: [`execution_control_identity`] binds `[run]`/`[reuse]`/
    /// `[resilience]`, so a post-plan edit to any of them moves the identity (→ the
    /// governed apply refuses at the choke-point), while an UNCHANGED project is
    /// byte-stable (no false-refuse). Hooks are enforced by REFUSAL (#1095(c)) —
    /// see `run::refuse_governed_side_effects` — not bound here.
    #[test]
    fn execution_control_identity_binds_run_reuse_resilience() {
        let dir = tempfile::tempdir().unwrap();
        let load = |body: &str| -> rocky_core::config::RockyConfig {
            let p = dir.path().join("rocky.toml");
            std::fs::write(&p, body).unwrap();
            rocky_core::config::load_rocky_config(&p).unwrap()
        };
        let base_toml = "[adapter]\ntype = \"duckdb\"\npath = \"x.duckdb\"\n\n\
            [pipeline.p]\ntype = \"transformation\"\nmodels = \"models/**\"\n\n\
            [pipeline.p.target]\nadapter = \"default\"\n\n\
            [run]\nskip_unchanged = false\n";
        let base = super::execution_control_identity(&load(base_toml));

        // Unchanged config → byte-stable (no false-refuse).
        assert_eq!(
            base,
            super::execution_control_identity(&load(base_toml)),
            "an unchanged project must produce a stable execution-control identity"
        );

        // (a) `[run].skip_unchanged` flip → moves (it arms the post-fingerprint
        // skip gate, so a reviewed-to-build plan could otherwise become a no-op).
        let skip = base_toml.replace("skip_unchanged = false", "skip_unchanged = true");
        assert_ne!(
            base,
            super::execution_control_identity(&load(&skip)),
            "a [run].skip_unchanged flip must move the identity"
        );

        // (b) `[reuse]` toggle → moves.
        let reuse = format!("{base_toml}\n[reuse]\nenabled = true\n");
        assert_ne!(
            base,
            super::execution_control_identity(&load(&reuse)),
            "a [reuse] toggle must move the identity"
        );

        // (c) `[resilience]` change → moves.
        let resil = format!("{base_toml}\n[resilience]\ntransient_max_retries = 9\n");
        assert_ne!(
            base,
            super::execution_control_identity(&load(&resil)),
            "a [resilience] change must move the identity"
        );
    }

    /// 🔴 #4/#5 (pre-mutation routing gate): `verify_routing_identity` refuses a
    /// routing change BEFORE any mutation, allows an unchanged/credential-only
    /// config, refuses a required-but-missing identity (#7), and skips for a
    /// genuinely-legacy plan.
    #[test]
    fn verify_routing_identity_semantics() -> anyhow::Result<()> {
        fn cfg(dir: &Path, path: &str) -> rocky_core::config::RockyConfig {
            let p = dir.join("rocky.toml");
            std::fs::write(
                &p,
                format!("[adapter]\ntype = \"duckdb\"\npath = \"{path}\"\n\n[pipeline.p]\ntype = \"transformation\"\nmodels = \"models/**\"\n\n[pipeline.p.target]\nadapter = \"default\"\n"),
            )
            .unwrap();
            rocky_core::config::load_rocky_config(&p).unwrap()
        }
        let dir = tempfile::tempdir()?;
        let cfg_a = cfg(dir.path(), "a.duckdb");
        let cfg_b = cfg(dir.path(), "b.duckdb");
        let config_path = dir.path().join("rocky.toml");
        let mk = |expected: Option<String>, require: bool| super::GovernedRunContext {
            principal: PolicyPrincipal::Agent,
            plan_id: "plan_x",
            root: dir.path(),
            config_path: &config_path,
            expected_ir_fingerprint: None,
            expected_config_identity: expected,
            require_fingerprint: require,
            reviewed_source_schemas: None,
            expects_models: true,
        };
        let authorized = super::config_policy_identity(&cfg_a);
        // Unchanged routing → proceeds.
        mk(Some(authorized.clone()), true)
            .verify_routing_identity(&cfg_a)
            .expect("unchanged routing must not refuse");
        // Routing change (path a→b) → REFUSE before any mutation.
        let err = mk(Some(authorized), true)
            .verify_routing_identity(&cfg_b)
            .expect_err("a routing change must refuse");
        assert!(err.to_string().contains("routing config"), "{err}");
        // Required but missing identity (#7) → refuse.
        assert!(mk(None, true).verify_routing_identity(&cfg_a).is_err());
        // Genuinely-legacy (not required, no identity) → allowed.
        mk(None, false)
            .verify_routing_identity(&cfg_a)
            .expect("a legacy plan skips the routing check");
        Ok(())
    }

    /// 🔴 #2 (snapshot preflight): a MODEL-EXECUTING governed plan must carry the
    /// v2 reviewed source-schema snapshot BEFORE any mutation. A NEW
    /// (`require_fingerprint`, i.e. version >= 1) plan with `None` snapshot — a v1
    /// plan or a v2 capture failure — is REFUSED (re-plan at v2); a `Some`
    /// (authoritative even empty) plan proceeds; a genuinely-legacy (v0) plan and
    /// a human apply are exempt.
    /// 🔴 #1 (execution-shape carve-out): `is_replication_only` — the SAFE
    /// negative that gates the snapshot preflight + policy selection — must be
    /// `true` ONLY for a replication pipeline with no `--all`/`--models`/`--model`.
    /// The **non-negotiable under-gate guard**: a TRANSFORMATION pipeline is NEVER
    /// replication-only (it runs models via `run_local`), so it stays STRICT (full
    /// policy gating + snapshot required) — the carve-out cannot become a policy
    /// bypass. A resolution failure is also strict (`false`).
    #[test]
    fn is_replication_only_is_a_safe_carve_out() {
        fn cfg(pipeline_type: &str) -> rocky_core::config::RockyConfig {
            let dir = tempfile::tempdir().unwrap();
            let p = dir.path().join("rocky.toml");
            // A replication target carries `catalog_template`/`schema_template`; a
            // transformation target does not — so the two pipeline kinds need
            // structurally different TOML.
            let body = if pipeline_type == "replication" {
                "[pipeline.p]\ntype = \"replication\"\nstrategy = \"full_refresh\"\n\n[pipeline.p.source.schema_pattern]\nprefix = \"raw__\"\nseparator = \"__\"\ncomponents = [\"source\"]\n\n[pipeline.p.target]\nadapter = \"default\"\ncatalog_template = \"c\"\nschema_template = \"s__{source}\"\n"
            } else {
                "[pipeline.p]\ntype = \"transformation\"\nmodels = \"models/**\"\n\n[pipeline.p.target]\nadapter = \"default\"\n"
            };
            std::fs::write(
                &p,
                format!("[adapter]\ntype = \"duckdb\"\npath = \"a.duckdb\"\n\n{body}"),
            )
            .unwrap();
            rocky_core::config::load_rocky_config(&p).unwrap()
        }
        let plan = |run_all: bool, models_dir: Option<&str>, model: Option<&str>| {
            let mut rp = minimal_run_plan();
            rp.pipeline = Some("p".to_string());
            rp.run_all = run_all;
            rp.models_dir = models_dir.map(str::to_string);
            rp.model = model.map(str::to_string);
            rp
        };
        // Replication + no flags → replication-only (carve out preflight + policy).
        assert!(super::is_replication_only(
            &cfg("replication"),
            &plan(false, None, None)
        ));
        // TRANSFORMATION + no flags → NOT replication-only → STRICT (the under-gate
        // guard: a transformation plan keeps full policy gating + snapshot).
        assert!(!super::is_replication_only(
            &cfg("transformation"),
            &plan(false, None, None)
        ));
        // Replication + any execution flag → NOT replication-only (it runs models).
        assert!(!super::is_replication_only(
            &cfg("replication"),
            &plan(true, None, None)
        ));
        assert!(!super::is_replication_only(
            &cfg("replication"),
            &plan(false, Some("m"), None)
        ));
        assert!(!super::is_replication_only(
            &cfg("replication"),
            &plan(false, None, Some("a"))
        ));
    }

    #[test]
    fn preflight_snapshot_semantics() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let config_path = dir.path().join("rocky.toml");
        let mk = |require: bool,
                  snapshot: Option<
            std::collections::BTreeMap<String, Vec<rocky_ir::types::TypedColumn>>,
        >| super::GovernedRunContext {
            principal: PolicyPrincipal::Agent,
            plan_id: "p",
            root: dir.path(),
            config_path: &config_path,
            expected_ir_fingerprint: None,
            expected_config_identity: None,
            require_fingerprint: require,
            reviewed_source_schemas: snapshot,
            expects_models: true,
        };
        // MODEL-EXECUTING (executes_models = true):
        // v1 / v2-capture-failure (require, None) → REFUSE.
        let v1 = mk(true, None);
        let err = super::preflight_snapshot(Some(&v1), "p", true)
            .expect_err("a required model-executing plan with no snapshot must refuse (#2)");
        assert!(
            err.to_string()
                .contains("no reviewed source-schema snapshot"),
            "{err}"
        );
        // v2 (require, Some authoritative even empty) → OK.
        let v2 = mk(true, Some(std::collections::BTreeMap::new()));
        super::preflight_snapshot(Some(&v2), "p", true)
            .expect("a v2 plan with a snapshot proceeds");
        // Genuinely-legacy v0 (not required) → exempt.
        super::preflight_snapshot(Some(&mk(false, None)), "p", true)
            .expect("a legacy v0 plan is exempt");
        // Human apply (no governed context) → exempt.
        super::preflight_snapshot(None, "p", true).expect("a human apply is exempt");
        // 🔴 #1 regression: a REPLICATION-ONLY plan (executes_models = false)
        // executes no models → NOT refused, for BOTH a v1/require plan with a
        // missing snapshot AND a v2 plan carrying one. The snapshot requirement
        // only ever applies to a model-executing plan.
        super::preflight_snapshot(Some(&v1), "p", false)
            .expect("a v1 replication-only (no-model) plan must NOT be refused (#1)");
        super::preflight_snapshot(Some(&v2), "p", false)
            .expect("a v2 replication-only (no-model) plan must NOT be refused (#1)");
        Ok(())
    }

    /// 🔴 D regression: the discovered replication target set is gated. An
    /// agent replication under `deny agent apply { any }` must be refused before
    /// any table materializes. Pre-fix the replication set was never
    /// policy-evaluated.
    #[test]
    fn replication_gate_denies_agent_under_deny_rule() -> anyhow::Result<()> {
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
        let state = dir.path().join("state.redb");
        let ledger = StateStore::open(&state)?;
        let ctx = super::GovernedRunContext {
            principal: PolicyPrincipal::Agent,
            plan_id: "plan_x",
            root: dir.path(),
            config_path: &config,
            expected_ir_fingerprint: None,
            expected_config_identity: None,
            require_fingerprint: false,
            reviewed_source_schemas: None,
            expects_models: true,
        };
        let loaded_cfg = rocky_core::config::load_rocky_config(&config)?;
        let targets: BTreeSet<String> = ["raw_orders".to_string()].into_iter().collect();
        let err = ctx
            .gate_replication_targets(&targets, &ledger, &loaded_cfg)
            .expect_err("an agent replication under a deny rule must be refused");
        assert!(err.to_string().contains("DENIES"), "got: {err}");

        // An empty target set is a no-op (nothing executes).
        ctx.gate_replication_targets(&BTreeSet::new(), &ledger, &loaded_cfg)
            .expect("no targets ⇒ nothing to gate");
        Ok(())
    }

    /// 🔴 Reentrancy regression: the replication gate runs while `run` already
    /// holds a live write handle on the state store. It must read/record
    /// through that held handle, NOT re-open — a re-open collides in-process,
    /// marks the snapshot unreadable, and the fail-closed D3 floor would then
    /// spuriously DENY every agent replication even under `allow`. This test
    /// holds a live handle (as `run` does) and asserts the held-handle gate
    /// allows, while contrasting the re-open path which spuriously denies.
    #[test]
    fn replication_gate_reads_through_held_handle_no_spurious_deny() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        // A benign `allow agent apply { any }` — the gate must NOT deny.
        let config = write_config(
            dir.path(),
            r#"
[[policy.rules]]
principal = "agent"
capability = "apply"
scope = { any = true }
effect = "allow"
"#,
        )?;
        let state = dir.path().join("state.redb");
        // Simulate `run` holding a live write handle for the whole invocation.
        let held = StateStore::open(&state)?;
        let ctx = super::GovernedRunContext {
            principal: PolicyPrincipal::Agent,
            plan_id: "plan_x",
            root: dir.path(),
            config_path: &config,
            expected_ir_fingerprint: None,
            expected_config_identity: None,
            require_fingerprint: false,
            reviewed_source_schemas: None,
            expects_models: true,
        };
        let loaded_cfg = rocky_core::config::load_rocky_config(&config)?;
        let targets: BTreeSet<String> = ["raw_orders".to_string()].into_iter().collect();

        // Through the held handle → reads fine → allow (no spurious deny).
        ctx.gate_replication_targets(&targets, &held, &loaded_cfg)
            .expect(
                "the replication gate must read through the held handle and not spuriously deny \
                 under an `allow` rule",
            );

        // Contrast: the re-open path (evaluate_apply_policy via state_path)
        // DOES fail closed while the handle is held — which is exactly why the
        // gate uses the held-handle variant instead.
        let touched: BTreeMap<String, PolicyCapability> = targets
            .iter()
            .map(|n| (n.clone(), PolicyCapability::Apply))
            .collect();
        let reopen_gate = super::evaluate_apply_policy(
            &config,
            "plan_x",
            PolicyPrincipal::Agent,
            &touched,
            &dir.path().join("models"),
            &state,
        );
        assert!(
            matches!(reopen_gate, PolicyGate::Deny { .. }),
            "the re-open path spuriously denies under a held handle (the reentrancy bug the \
             held-handle variant avoids), got {reopen_gate:?}"
        );
        Ok(())
    }

    /// The replication gate is a no-op absent a `[policy]` block (NotConfigured).
    #[test]
    fn replication_gate_noop_without_policy_block() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        std::fs::write(dir.path().join("rocky.toml"), NO_POLICY_TOML)?;
        let state = dir.path().join("state.redb");
        let ledger = StateStore::open(&state)?;
        let ctx = super::GovernedRunContext {
            principal: PolicyPrincipal::Agent,
            plan_id: "plan_x",
            root: dir.path(),
            config_path: &dir.path().join("rocky.toml"),
            expected_ir_fingerprint: None,
            expected_config_identity: None,
            require_fingerprint: false,
            reviewed_source_schemas: None,
            expects_models: true,
        };
        let loaded_cfg =
            rocky_core::config::load_rocky_config(&dir.path().join("rocky.toml")).unwrap();
        let targets: BTreeSet<String> = ["raw_orders".to_string()].into_iter().collect();
        ctx.gate_replication_targets(&targets, &ledger, &loaded_cfg)
            .expect("no [policy] block ⇒ replication gate is a no-op");
        Ok(())
    }

    /// Finding 7: replication pipelines have NO post-run verify substrate, so a
    /// governed replication apply whose matched allow-rule requires `verify_after`
    /// must be REFUSED (not silently allowed as an unverified mutation).
    #[test]
    fn replication_gate_refuses_verify_after() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        // An `allow` rule that ALSO demands a post-apply check.
        let config = write_config(
            dir.path(),
            r#"
[[policy.rules]]
principal = "agent"
capability = "apply"
scope = { any = true }
effect = "allow"
verify_after = ["row_count"]
"#,
        )?;
        let loaded_cfg = rocky_core::config::load_rocky_config(&config)?;
        let state = dir.path().join("state.redb");
        let ledger = StateStore::open(&state)?;
        let ctx = super::GovernedRunContext {
            principal: PolicyPrincipal::Agent,
            plan_id: "plan_x",
            root: dir.path(),
            config_path: &config,
            expected_ir_fingerprint: None,
            expected_config_identity: None,
            require_fingerprint: false,
            reviewed_source_schemas: None,
            expects_models: true,
        };
        let targets: BTreeSet<String> = ["raw_orders".to_string()].into_iter().collect();
        let err = ctx
            .gate_replication_targets(&targets, &ledger, &loaded_cfg)
            .expect_err(
                "a replication apply under a verify_after rule must be refused (finding 7)",
            );
        assert!(
            err.to_string()
                .contains("verify_after` is NOT supported for replication"),
            "the refusal must name the replication verify_after limitation; got: {err}"
        );
        Ok(())
    }

    /// Finding 6: for a budget-relevant policy (a rule with `autonomy_budget` or
    /// `verify_after`), the decision-row write is FAIL-CLOSED — if the ledger
    /// write handle is unavailable (a concurrent writer holds the advisory lock),
    /// the gate DENIES rather than silently dropping the budget-pair row.
    #[test]
    fn budget_relevant_decision_write_is_fail_closed_under_contention() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let config = write_config(
            dir.path(),
            r#"
[[policy.rules]]
principal = "agent"
capability = "apply"
scope = { any = true }
effect = "allow"
autonomy_budget = { failures = 3, window = "7d" }
"#,
        )?;
        let policy = rocky_core::config::load_rocky_config(&config)?.policy;
        let state = dir.path().join("state.redb");
        let mut touched = BTreeMap::new();
        touched.insert("m".to_string(), PolicyCapability::Apply);

        // Hold the writer lock, as a concurrent run would — the decision-row write
        // handle is unavailable, so the budget-pair row cannot be persisted.
        let held = StateStore::open(&state)?;
        let gate = super::evaluate_apply_policy_with_policy(
            policy.as_ref(),
            "plan_x",
            PolicyPrincipal::Agent,
            &touched,
            &dir.path().join("models"),
            &state,
        );
        drop(held);
        assert!(
            matches!(&gate, PolicyGate::Deny { reason, .. }
                if reason.contains("fail-closed") && reason.contains("budget")),
            "a budget-relevant decision whose row can't be persisted must fail closed with a \
             budget-pair reason; got {gate:?}"
        );
        Ok(())
    }

    /// Finding 6 (round-6 false-deny fix) — the true kill-check. Hold ONLY the
    /// writer lock (not a full `StateStore`), so `open_read_only` still succeeds
    /// (readable snapshot) while the ledger WRITE open fails — the real
    /// cross-process contention shape. This isolates the per-winning-rule budget
    /// scoping (a full `StateStore` would block the reader too and hit the separate
    /// snapshot-unreadable fail-closed). Ordinary winner + unrelated budget rule
    /// ⇒ NOT denied; budget winner whose row can't persist ⇒ fail-closed Deny.
    #[test]
    fn budget_fail_closed_scoped_to_winning_rule_under_writer_lock() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let state = dir.path().join("state.redb");
        // Seed the store so `open_read_only` has a readable ledger, then release.
        drop(StateStore::open(&state)?);
        // Hold ONLY the advisory writer lock (a separate lockfile) — readers skip it.
        let _lock = rocky_core::state::try_acquire_writer_lock(&state)?;
        let models = dir.path().join("models");
        let mut touched = BTreeMap::new();
        touched.insert("orders".to_string(), PolicyCapability::Apply);

        // (a) `orders` wins under the NON-budget `any` rule; the `payments` budget
        // rule is unrelated → must NOT fail-closed-deny despite the write-lock.
        let cfg_a = write_config(
            dir.path(),
            r#"
[[policy.rules]]
principal = "agent"
capability = "apply"
scope = { models = ["payments"] }
effect = "allow"
autonomy_budget = { failures = 3, window = "7d" }

[[policy.rules]]
principal = "agent"
capability = "apply"
scope = { any = true }
effect = "allow"
"#,
        )?;
        let pol_a = rocky_core::config::load_rocky_config(&cfg_a)?.policy;
        let gate_a = super::evaluate_apply_policy_with_policy(
            pol_a.as_ref(),
            "p",
            PolicyPrincipal::Agent,
            &touched,
            &models,
            &state,
        );
        assert!(
            !matches!(&gate_a, PolicyGate::Deny { reason, .. } if reason.contains("fail-closed")),
            "a NON-budget winner must not be fail-closed-denied because an unrelated budget rule \
             exists for a different target; got {gate_a:?}"
        );

        // (b) `orders` wins under a BUDGET `any` rule whose decision row can't be
        // persisted (write-lock held) → fail-closed Deny (durability preserved).
        let cfg_b = write_config(
            dir.path(),
            r#"
[[policy.rules]]
principal = "agent"
capability = "apply"
scope = { any = true }
effect = "allow"
autonomy_budget = { failures = 3, window = "7d" }
"#,
        )?;
        let pol_b = rocky_core::config::load_rocky_config(&cfg_b)?.policy;
        let gate_b = super::evaluate_apply_policy_with_policy(
            pol_b.as_ref(),
            "p",
            PolicyPrincipal::Agent,
            &touched,
            &models,
            &state,
        );
        assert!(
            matches!(&gate_b, PolicyGate::Deny { reason, .. } if reason.contains("fail-closed")),
            "a BUDGET winner whose decision row can't be persisted must fail closed; got {gate_b:?}"
        );
        Ok(())
    }

    /// 🔴 D (DAG) regression: a governed (agent) `--dag` apply is REFUSED,
    /// because the DAG runner dispatches sub-runs with no governance context and
    /// would execute (incl. replication) ungated. Pre-fix an agent dag apply ran
    /// every pipeline unpoliced. A human dag apply is unaffected (governed_ctx
    /// is None for a human).
    #[tokio::test]
    async fn governed_dag_apply_is_refused() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        // No `[policy]` block → the apply gate is NotConfigured (passes); the
        // refusal is the DAG-governed guard, not a policy deny.
        std::fs::write(dir.path().join("rocky.toml"), NO_POLICY_TOML)?;
        let mut rp = minimal_run_plan();
        rp.dag = true;
        let plan_id = write_plan(dir.path(), PlanKind::Run, &rp)?;
        let state = dir.path().join("state.redb");

        // Applied by an AGENT (runtime principal) → governed_ctx is Some → dag
        // apply is refused before dispatching.
        let err = super::run_apply_run_plan(
            dir.path(),
            &dir.path().join("rocky.toml"),
            &plan_id,
            &state,
            PolicyPrincipal::Agent,
            true,
        )
        .await
        .expect_err("an agent --dag apply must be refused");
        assert!(
            err.to_string().contains("not yet policy-gated"),
            "must refuse the governed dag apply, got: {err}"
        );
        Ok(())
    }

    // A replication pipeline (duckdb, creds-free). Its silver-model leg resolves
    // `run_plan.models_dir` cwd-relative — the path the apply seam recompiles.
    const REPLICATION_TOML: &str = r#"
[adapter]
type = "duckdb"
path = "x.duckdb"

[pipeline.p]
type = "replication"
strategy = "full_refresh"

[pipeline.p.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.p.target]
adapter = "default"
catalog_template = "c"
schema_template = "s__{source}"
"#;

    /// ‼️ Finding #2 (missing-dir), REPLICATION apply seam: a governed (agent)
    /// replication apply whose plan reviewed a NON-EMPTY silver-model set, but whose
    /// reviewed models directory no longer compiles (deleted / renamed since the
    /// plan), must FAIL CLOSED at the apply seam — BEFORE the replication DDL — not
    /// silently skip and report SUCCESS. A v0 plan keeps the snapshot preflight
    /// exempt so this isolates the seam. Neuter the seam `bail!` and this apply
    /// still fails, now via the run.rs `else if governed…expects_models` executor
    /// leg (the seam→execute race-closer) — the two layers are independent.
    #[tokio::test]
    async fn governed_replication_apply_seam_refuses_deleted_reviewed_dir() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        std::fs::write(dir.path().join("rocky.toml"), REPLICATION_TOML)?;
        // The plan reviewed a real, compilable silver-models dir …
        let models_dir = dir.path().join("silver");
        write_min_model(&models_dir, "orders");
        let mut rp = minimal_run_plan();
        rp.pipeline = Some("p".to_string());
        rp.run_all = true; // replication --all → enters the silver-model leg
        rp.models_dir = Some(models_dir.to_string_lossy().into_owned());
        rp.models = vec!["s.orders".to_string()]; // a non-empty reviewed set
        let plan_id = write_plan(dir.path(), PlanKind::Run, &rp)?;
        let state = dir.path().join("state.redb");

        // … then the dir is DELETED after the plan was authorized.
        std::fs::remove_dir_all(&models_dir)?;

        let err = super::run_apply_run_plan(
            dir.path(),
            &dir.path().join("rocky.toml"),
            &plan_id,
            &state,
            PolicyPrincipal::Agent,
            true,
        )
        .await
        .expect_err("a governed replication apply of a deleted reviewed dir must fail closed");
        assert!(
            err.to_string().contains("no longer compiles to any model"),
            "must refuse at the replication apply seam, got: {err}"
        );
        Ok(())
    }

    /// ‼️ Finding #2 (missing-dir), TRANSFORMATION executor leg: the apply seam does
    /// NOT check the transformation path (it resolves `config_dir.join(models_base)`
    /// from the pipeline config, which the seam's `run_plan.models_dir` does not
    /// match), so `run_local` is the SOLE fail-closed guard here. A governed
    /// transformation apply whose plan reviewed a non-empty model set but whose
    /// config-resolved models directory is gone at execution must FAIL CLOSED at the
    /// executor — before any warehouse mutation — not report SUCCESS with nothing
    /// built. This is the real seam↔executor path-resolution gap the scoping fixes.
    #[tokio::test]
    async fn governed_transformation_apply_executor_refuses_deleted_reviewed_dir()
    -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        // NO_POLICY_TOML is a transformation pipeline with `models = "models/**"`,
        // so the reviewed dir `run_local` resolves is `<config_dir>/models`.
        std::fs::write(dir.path().join("rocky.toml"), NO_POLICY_TOML)?;
        let models_dir = dir.path().join("models");
        write_min_model(&models_dir, "orders");
        let mut rp = minimal_run_plan();
        rp.pipeline = Some("p".to_string());
        rp.models = vec!["s.orders".to_string()]; // a non-empty reviewed set
        // rp.models_dir intentionally None — `run_local` ignores it and uses the
        // config-resolved `<config_dir>/models`.
        let plan_id = write_plan(dir.path(), PlanKind::Run, &rp)?;
        let state = dir.path().join("state.redb");

        std::fs::remove_dir_all(&models_dir)?;

        let err = super::run_apply_run_plan(
            dir.path(),
            &dir.path().join("rocky.toml"),
            &plan_id,
            &state,
            PolicyPrincipal::Agent,
            true,
        )
        .await
        .expect_err("a governed transformation apply of a deleted reviewed dir must fail closed");
        // The executor `bail!` is wrapped by `run`'s apply context, so inspect the
        // full cause chain (`{:#}`), not just the outermost message.
        assert!(
            format!("{err:#}").contains("does not exist at execution"),
            "must fail closed at the transformation executor leg, got: {err:#}"
        );
        Ok(())
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

    // ---------- verify_after post-apply gate ----------

    /// Record a run under an explicit `run_id` + `started_at`, so a test can
    /// control which run is "latest" (by timestamp) independently of which run
    /// it later asks `run_verify_after` to resolve (by id).
    fn record_run_with_checks_id(
        state_path: &Path,
        run_id: &str,
        started_at: chrono::DateTime<chrono::Utc>,
        checks: &[(&str, bool)],
    ) {
        use rocky_core::state::{
            CheckOutcome, RunRecord, RunStatus, RunTrigger, SessionSource, StateStore,
        };
        let store = StateStore::open(state_path).unwrap();
        let record = RunRecord {
            run_id: run_id.to_string(),
            started_at,
            finished_at: started_at,
            status: RunStatus::Success,
            models_executed: vec![],
            trigger: RunTrigger::Manual,
            config_hash: "h".to_string(),
            triggering_identity: None,
            session_source: SessionSource::Cli,
            git_commit: None,
            git_branch: None,
            idempotency_key: None,
            target_catalog: None,
            hostname: "test".to_string(),
            rocky_version: "test".to_string(),
            check_outcomes: checks
                .iter()
                .map(|(n, p)| CheckOutcome {
                    name: n.to_string(),
                    passed: *p,
                })
                .collect(),
            pipeline: None,
            submission_id: None,
        };
        store.record_run(&record).unwrap();
    }

    /// Record a run with a generated id and return that id so the caller can
    /// thread it into `run_verify_after` (which now resolves the run by id).
    fn record_run_with_checks(state_path: &Path, checks: &[(&str, bool)]) -> String {
        let now = chrono::Utc::now();
        let run_id = format!("run-{}", now.timestamp_nanos_opt().unwrap_or(0));
        record_run_with_checks_id(state_path, &run_id, now, checks);
        run_id
    }

    #[test]
    fn verify_after_passes_when_all_named_checks_pass() {
        let dir = tempfile::tempdir().unwrap();
        let state = dir.path().join("state.redb");
        let run_id =
            record_run_with_checks(&state, &[("row_count", true), ("not_null_keys", true)]);
        let r = super::run_verify_after(
            "plan-x",
            PolicyPrincipal::Agent,
            &["row_count".to_string(), "not_null_keys".to_string()],
            &run_id,
            &state,
        );
        assert!(
            r.is_ok(),
            "all named checks passed → verify_after ok: {r:?}"
        );
    }

    #[test]
    fn verify_after_halts_when_named_check_fails() {
        let dir = tempfile::tempdir().unwrap();
        let state = dir.path().join("state.redb");
        let run_id =
            record_run_with_checks(&state, &[("row_count", true), ("not_null_keys", false)]);
        let err = super::run_verify_after(
            "plan-x",
            PolicyPrincipal::Agent,
            &["not_null_keys".to_string()],
            &run_id,
            &state,
        )
        .expect_err("a failing named check halts the apply");
        let msg = err.to_string();
        assert!(
            msg.contains("verify_after gate FAILED") && msg.contains("not_null_keys"),
            "{msg}"
        );
        // The halt-only state (no rollback substrate) must be stated plainly.
        assert!(msg.contains("HAS ALREADY LANDED"), "halt-only state: {msg}");
    }

    #[test]
    fn verify_after_fails_closed_when_named_check_absent() {
        // A named check that did not run cannot be confirmed → halt (fail
        // closed). This is the verify_after analog of the false-additive
        // soundness direction.
        let dir = tempfile::tempdir().unwrap();
        let state = dir.path().join("state.redb");
        let run_id = record_run_with_checks(&state, &[("row_count", true)]);
        let err = super::run_verify_after(
            "plan-x",
            PolicyPrincipal::Agent,
            &["freshness".to_string()],
            &run_id,
            &state,
        )
        .expect_err("an absent named check fails closed");
        assert!(err.to_string().contains("absent"), "{}", err);
    }

    #[test]
    fn verify_after_empty_is_noop_even_without_a_run() {
        let dir = tempfile::tempdir().unwrap();
        let state = dir.path().join("state.redb");
        // No required checks → the gate is a no-op and never touches state.
        assert!(
            super::run_verify_after("plan-x", PolicyPrincipal::Agent, &[], "unused-id", &state)
                .is_ok()
        );
    }

    /// 🔴 Regression (false-verified): the gate must resolve *this apply's own*
    /// run by id, NOT "the latest run". Two runs share the state store: run A
    /// (this apply, older) failed its required check; a concurrent run B (newer,
    /// "latest") passed the same-named check. If the gate read "latest" it would
    /// verify against B and wrongly pass — so it MUST fail when handed A's id.
    #[test]
    fn verify_after_reads_this_applys_run_not_latest() {
        let dir = tempfile::tempdir().unwrap();
        let state = dir.path().join("state.redb");

        let older = chrono::Utc::now() - chrono::Duration::seconds(60); // A @ 10:00
        let newer = chrono::Utc::now(); // B @ 10:01 (the "latest" by started_at)
        // A is this apply's run: its required check FAILED.
        record_run_with_checks_id(&state, "run-A", older, &[("row_count", false)]);
        // B is a concurrent run that finished later: same check PASSED.
        record_run_with_checks_id(&state, "run-B", newer, &[("row_count", true)]);

        let err = super::run_verify_after(
            "plan-x",
            PolicyPrincipal::Agent,
            &["row_count".to_string()],
            "run-A",
            &state,
        )
        .expect_err(
            "the gate must fail against THIS apply's run (A, failed), not the latest run (B, passed)",
        );
        let msg = err.to_string();
        assert!(
            msg.contains("verify_after gate FAILED") && msg.contains("row_count"),
            "must fail on A's failed check, got: {msg}"
        );
    }

    /// 🔴 Regression (false-verified): duplicate check names must AND-aggregate.
    /// Per-table checks share a fixed name (`row_count` per table), so a run's
    /// outcomes can carry the same name failed *and* passed. A last-writer-wins
    /// map would let the passing occurrence overwrite the failing one and the
    /// gate would wrongly pass. Every occurrence must be AND-ed → fail.
    #[test]
    fn verify_after_duplicate_check_name_failed_then_passed_fails() {
        let dir = tempfile::tempdir().unwrap();
        let state = dir.path().join("state.redb");
        // Same name, failed first then passed — order chosen so a naive
        // last-writer-wins map would land on `true` and wrongly pass.
        record_run_with_checks_id(
            &state,
            "run-dup",
            chrono::Utc::now(),
            &[("row_count", false), ("row_count", true)],
        );

        let err = super::run_verify_after(
            "plan-x",
            PolicyPrincipal::Agent,
            &["row_count".to_string()],
            "run-dup",
            &state,
        )
        .expect_err("a failing occurrence of a duplicate check name must fail the gate");
        let msg = err.to_string();
        assert!(
            msg.contains("verify_after gate FAILED") && msg.contains("row_count"),
            "duplicate-name AND-aggregation must surface the failure: {msg}"
        );
    }
}
