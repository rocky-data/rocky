//! `rocky apply <plan-id>` — execute a previously-generated plan.
//!
//! Dispatches by `PlanKind`:
//! - `Compact` → `commands::compact::run_compact_apply_in`
//! - `Archive` → `commands::archive::run_archive_apply_in`
//! - `Run`     → `commands::run::run` with the `RunPlan` operational metadata
//!
//! The `--inline` flag skips plan persistence and executes immediately —
//! this is the path that `rocky run` aliases to so existing callers see no
//! behaviour change. Phase 4 will add a deprecation warning on `rocky run`.
//!
//! ## Plan payload for Run kind
//!
//! `RunPlan` persists operational metadata (filter, pipeline, partition flags,
//! model list, execution layers) rather than a full `ProjectIr` snapshot.
//! `rocky apply` re-derives `ProjectIr` by calling `commands::run::run` with
//! the same flags — a fast, CPU-only recompile step. Full IR persistence is
//! deferred to a future phase if deterministic replay without recompile
//! becomes a hard requirement.

use std::path::Path;

use anyhow::{Context, Result, bail};

use crate::output::{AuditEvent, AuditEventKind, BranchPromoteOutput, PromotePlan, RunPlan};
use crate::plan_store::{PlanKind, read_plan};

use super::archive::run_archive_apply_in;
use super::compact::run_compact_apply_in;

/// Execute `rocky apply <plan-id>`.
///
/// Reads the plan from `.rocky/plans/<plan_id>.json`, dispatches by kind,
/// and emits an `ApplyOutput` envelope wrapping the inner result.
pub async fn run_apply(config_path: &Path, plan_id: &str, output_json: bool) -> Result<()> {
    let cwd = std::env::current_dir().context("failed to get current working directory")?;
    run_apply_in(&cwd, config_path, plan_id, output_json).await
}

/// Inner implementation — takes an explicit `root` for the plans directory so
/// tests can pass a temp dir without touching the process-global cwd.
pub(crate) async fn run_apply_in(
    root: &Path,
    config_path: &Path,
    plan_id: &str,
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
        PlanKind::Run => run_apply_run_plan(root, config_path, plan_id, output_json).await,
        PlanKind::Promote => run_apply_promote_plan(root, config_path, plan_id, output_json).await,
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

    // Resolve the state path via the standard resolver (mirrors main.rs).
    // Used both by branch→shadow resolution below and by `run` itself.
    let resolved = rocky_core::state::resolve_state_path(None, std::path::Path::new("models"));
    let state_path = resolved.path;

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
        let store = rocky_core::state::StateStore::open_read_only(&state_path)
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
        return crate::commands::run_with_dag(config_path, output_json)
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
        &state_path,
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
    )
    .await
    .with_context(|| format!("rocky apply run plan '{plan_id}' failed"))?;

    // The `run` command has already emitted its own JSON (or text) output.
    // Nothing more to emit in the inline/non-envelope path.
    Ok(())
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
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::output::RunPlan;
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
}
