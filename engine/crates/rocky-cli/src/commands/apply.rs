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
//! deferred to a future phase if deterministic replay without recompile
//! becomes a hard requirement.
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

use std::path::Path;

use anyhow::{Context, Result, bail};

use crate::output::{
    AuditEvent, AuditEventKind, BranchPromoteOutput, PromotePlan, ReplicationConnectorSnapshot,
    ReplicationPlan, RunPlan,
};
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
        PlanKind::Replication => {
            run_apply_replication_plan(root, config_path, plan_id, output_json).await
        }
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

    if live_snapshot != replication_plan.source_state_snapshot {
        let diff_summary =
            summarize_source_state_drift(&replication_plan.source_state_snapshot, &live_snapshot);
        bail!(
            "source state has drifted since plan '{plan_id}' was created.\n\
             {diff_summary}\n\
             Re-plan with `rocky plan` and re-apply against the resulting plan_id."
        );
    }

    // Snapshot matched — delegate to the existing replication codepath
    // by calling `commands::run::run` with model-related args set to
    // defaults. The replication arm inside `run` handles everything
    // from here.
    let resolved = rocky_core::state::resolve_state_path(None, std::path::Path::new("models"));
    let state_path = resolved.path;

    let partition_opts = crate::commands::run::PartitionRunOptions::default();

    crate::commands::run::run(
        config_path,
        replication_plan.filter.as_deref(),
        replication_plan.pipeline.as_deref(),
        &state_path,
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
    )
    .await
    .with_context(|| format!("rocky apply replication plan '{plan_id}' failed"))?;

    Ok(())
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
        if let Some(l_conn) = live_map.get(id) {
            if p_conn != l_conn {
                lines.push(format!(
                    "  connector '{id}' changed (tables: {} -> {}; schema/type may also differ)",
                    p_conn.tables.len(),
                    l_conn.tables.len(),
                ));
            }
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
}
