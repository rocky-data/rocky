use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinSet;
use tracing::{Instrument, debug, info, info_span, warn};

use rocky_adapter_sdk::throttle::AdaptiveThrottle;

use rocky_core::checks;
use rocky_core::config::{GovernanceOverride, ReplicationPipelineConfig};
use rocky_core::drift;
use rocky_core::hooks::{HookContext, HookRegistry};
use rocky_core::ir::*;
use rocky_core::sql_gen;
use rocky_core::state::StateStore;
use rocky_core::traits::{
    BatchCheckAdapter, FreshnessResult as BatchFreshnessResult, GovernanceAdapter, MaskStrategy,
    MaskingPolicy, RowCountResult as BatchRowCountResult, TagTarget, WarehouseAdapter,
};

use crate::output::*;
use crate::registry::{self, AdapterRegistry};
use crate::schema_cache_writer::{SchemaCacheWriteTap, persist_batch_describe};

use super::run_audit::AuditContext;
use super::{matches_filter, parse_filter, parsed_to_json_map};

/// Accumulated check results for a table, assembled after batched execution.
struct PendingCheck {
    asset_key: Vec<String>,
    checks: Vec<rocky_core::checks::CheckResult>,
}

/// Result of processing a single table (returned from parallel tasks).
struct TableResult {
    materialization: MaterializationOutput,
    drift_checked: bool,
    drift_detected: Option<DriftActionOutput>,
    column_match_check: Option<rocky_core::checks::CheckResult>,
    source_batch_ref: Option<TableRef>,
    target_batch_ref: Option<TableRef>,
    freshness_batch_ref: Option<TableRef>,
    asset_key: Vec<String>,
    target_full_name: String,
    /// Deferred governance tags — applied in a post-run batch phase instead
    /// of inside the per-table concurrent loop, avoiding sequential SQL waits
    /// that block the concurrency semaphore.
    deferred_tags: Option<DeferredTagging>,
    /// Deferred watermark — written in a single batch after all tables
    /// complete, eliminating Mutex contention during concurrent processing.
    deferred_watermark: Option<DeferredWatermark>,
}

/// Table tagging deferred to post-run phase.
struct DeferredTagging {
    catalog: String,
    schema: String,
    table: String,
    tags: BTreeMap<String, String>,
}

/// Watermark update deferred to post-run phase.
struct DeferredWatermark {
    state_key: String,
    timestamp: chrono::DateTime<Utc>,
}

/// Error from parallel table processing (partial failure mode).
struct TableError {
    asset_key: Vec<String>,
    error: String,
    /// Index into the original tables_to_process vec (for retry).
    task_index: Option<usize>,
}

/// Sentinel error signalling that `rocky run` was cancelled by a shutdown
/// signal (SIGINT / Ctrl-C, or SIGTERM on Unix — typically sent by
/// Kubernetes during pod eviction). The outer binary
/// (`rocky/src/main.rs`) downcasts this off the `anyhow::Error` chain and
/// maps it to exit code 130 so callers (shell, CI, orchestrators) can
/// distinguish "interrupted" from a generic run failure. A second signal
/// bypasses this path entirely and calls `std::process::exit(130)`
/// directly via the watcher armed on first signal.
#[derive(Debug, thiserror::Error)]
#[error("run was interrupted by a shutdown signal")]
pub struct Interrupted;

/// Arm a background watcher that hard-exits with code 130 on a *second*
/// SIGINT. The first signal (SIGINT or SIGTERM) is handled by the outer
/// `tokio::select!` branch — this watcher gives the user an out when
/// graceful cleanup itself hangs (e.g. a stuck warehouse query that
/// ignores cancellation). Only SIGINT is watched here because K8s escalates
/// SIGTERM → SIGKILL itself after `terminationGracePeriodSeconds`; the
/// hard-exit we care about is the interactive "Ctrl-C again" case.
fn arm_hard_exit_on_second_signal() {
    eprintln!(
        "\n[rocky] shutdown signal received — finishing in-flight statements; press Ctrl-C again to terminate"
    );
    tokio::spawn(async {
        let _ = tokio::signal::ctrl_c().await;
        eprintln!("\n[rocky] second SIGINT — terminating");
        std::process::exit(130);
    });
}

/// Input for a single table processing task.
#[derive(Clone)]
struct TableTask {
    source_catalog: String,
    source_schema: String,
    target_catalog: String,
    target_schema: String,
    table_name: String,
    asset_key_prefix: Vec<String>,
    check_column_match: bool,
    check_row_count: bool,
    check_freshness: bool,
    /// Column names to exclude from column_match check (metadata columns added by Rocky).
    column_match_exclude: Vec<String>,
    /// Metadata columns with template placeholders already resolved for this schema.
    metadata_columns: Vec<MetadataColumn>,
    governance_tags: BTreeMap<String, String>,
    /// Pre-fetched column metadata from batch information_schema query.
    /// When present, `process_table` skips individual DESCRIBE TABLE calls.
    prefetched_source_cols: Option<Vec<ColumnInfo>>,
    prefetched_target_cols: Option<Vec<ColumnInfo>>,
}

/// CLI selection state for `time_interval` partition execution.
///
/// Bundles the seven `--partition` / `--from` / `--to` / `--latest` /
/// `--missing` / `--lookback` / `--parallel` flags so the run() signature
/// stays manageable. Constructed from the parsed `clap` enum in `main.rs`
/// and converted to a `rocky_core::plan_partition::PartitionSelection` per
/// model when execution actually runs against a `time_interval` model.
#[derive(Debug, Clone, Default)]
pub struct PartitionRunOptions {
    pub partition: Option<String>,
    pub from: Option<String>,
    pub to: Option<String>,
    pub latest: bool,
    pub missing: bool,
    pub lookback: Option<u32>,
    pub parallel: u32,
}

impl PartitionRunOptions {
    /// Convert the bundled CLI flags to a `PartitionSelection`, or `None` if
    /// no selection flag is set (in which case the runtime defaults to
    /// `PartitionSelection::Latest` for `time_interval` models).
    ///
    /// Mutual exclusion is enforced by `clap` at parse time, so at most one
    /// of `partition`, `from`/`to`, `latest`, `missing` is set.
    pub fn to_selection(&self) -> Option<rocky_core::plan_partition::PartitionSelection> {
        use rocky_core::plan_partition::PartitionSelection;
        if let Some(key) = &self.partition {
            return Some(PartitionSelection::Single(key.clone()));
        }
        if let (Some(from), Some(to)) = (&self.from, &self.to) {
            return Some(PartitionSelection::Range {
                from: from.clone(),
                to: to.clone(),
            });
        }
        if self.latest {
            return Some(PartitionSelection::Latest);
        }
        if self.missing {
            return Some(PartitionSelection::Missing);
        }
        None
    }

    /// Returns true if the user passed any partition-related flag, signaling
    /// that this run is targeting `time_interval` models specifically.
    pub fn any_set(&self) -> bool {
        self.partition.is_some()
            || self.from.is_some()
            || self.to.is_some()
            || self.latest
            || self.missing
            || self.lookback.is_some()
    }
}

/// Borrowed slice of compile-time facts that the per-model execution path
/// needs at materialization time.
///
/// Bundles the typed-model schemas (for `column_count`) and per-model
/// compile timings (for `compile_time_ms`) into one ref so call signatures
/// stay manageable. Add fields here when more compile-time facts need to
/// reach `process_table` / `run_one_partition` — every other plumbing
/// touchpoint inherits the new field for free.
///
/// Borrows from `rocky_compiler::compile::CompileResult`; the lifetime
/// matches the compile result's owning scope inside `execute_models()`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ExecutionContext<'a> {
    /// Per-model typed column schemas, keyed by model name.
    pub typed_models: &'a indexmap::IndexMap<String, Vec<rocky_compiler::types::TypedColumn>>,
    /// Per-model compile cost (currently `typecheck_ms` only).
    pub model_timings:
        &'a std::collections::HashMap<String, rocky_compiler::compile::ModelCompileTimings>,
}

impl<'a> ExecutionContext<'a> {
    /// Look up the column count for a derived model. Returns `None` for
    /// models the typechecker didn't see (e.g., source replication tables
    /// whose schema is only known at runtime via `DESCRIBE TABLE`).
    pub fn column_count_for(&self, model_name: &str) -> Option<u64> {
        self.typed_models
            .get(model_name)
            .map(|cols| cols.len() as u64)
    }

    /// Look up the per-model compile time in milliseconds. Returns `None`
    /// for models the typechecker didn't see.
    pub fn compile_time_ms_for(&self, model_name: &str) -> Option<u64> {
        self.model_timings.get(model_name).map(|t| t.total_ms)
    }
}

/// Persist a [`rocky_core::state::RunRecord`] to the state store at run
/// finalize. Called at every exit path (happy, interrupted, model-only)
/// after [`RunOutput::populate_cost_summary`] so the recorded
/// `ModelExecution` entries carry cost-populated values.
///
/// Failures are logged and swallowed — the user's run has already
/// succeeded (or not) by the time we reach here; a state-store write
/// failure must not flip the exit code. Matches the resilience posture
/// of state_sync aborts elsewhere in this file.
/// Convert the CLI-level [`AuditContext`] into the output-layer
/// [`RunRecordAudit`] bundle. Kept as a free fn (not a `From` impl) so
/// `run_audit` stays free of a dependency on `crate::output` — the
/// audit module is pure environment-probing with no JSON-schema
/// surface.
fn audit_to_record(ctx: &AuditContext) -> RunRecordAudit {
    RunRecordAudit {
        triggering_identity: ctx.triggering_identity.clone(),
        session_source: ctx.session_source,
        git_commit: ctx.git_commit.clone(),
        git_branch: ctx.git_branch.clone(),
        idempotency_key: ctx.idempotency_key.clone(),
        target_catalog: ctx.target_catalog.clone(),
        hostname: ctx.hostname.clone(),
        rocky_version: ctx.rocky_version.clone(),
    }
}

fn persist_run_record(
    state_store: Option<&StateStore>,
    output: &RunOutput,
    run_id: &str,
    started_at: DateTime<Utc>,
    config_hash: &str,
    audit: &RunRecordAudit,
) {
    let Some(store) = state_store else {
        return;
    };
    let finished_at = Utc::now();
    let status = output.derive_run_status();
    let record = output.to_run_record(
        run_id,
        started_at,
        finished_at,
        config_hash.to_string(),
        rocky_core::state::RunTrigger::Manual,
        status,
        audit.clone(),
    );
    if let Err(e) = store.record_run(&record) {
        warn!(
            error = %e,
            run_id = run_id,
            "failed to record run to state store — history/replay/cost/trace will not surface this run"
        );
    }
}

/// Context captured at idempotency claim time so the finalize path (every
/// `persist_run_record` call site) can stamp the correct terminal state.
struct IdempotencyCtx {
    key: String,
    backend: rocky_core::idempotency::IdempotencyBackend,
    config: rocky_core::config::IdempotencyConfig,
    /// Captured at the claim point; surfaced in the echoed `RunOutput`
    /// field for operator cross-reference.
    _claim_backend_label: &'static str,
}

/// Outcome of the initial claim attempt at the top of `rocky run`.
enum IdempotencyOutcome {
    /// Key dedups — the caller already emitted a `skipped_*` `RunOutput`
    /// and exited. `run()` should return immediately.
    Skipped,
    /// Key claimed (or adopted from stale) — run proceeds; ctx is threaded
    /// to the finalize path.
    Proceed(IdempotencyCtx),
}

/// Attempt to claim the idempotency key. On skip, emit the short-circuit
/// `RunOutput` and return [`IdempotencyOutcome::Skipped`]. On proceed /
/// adopt-stale, return the claim context for the caller to thread into
/// [`finalize_idempotency`] at every exit path.
async fn try_claim_idempotency(
    state_cfg: &rocky_core::config::StateConfig,
    state_path: &Path,
    key: &str,
    run_id: &str,
    output_json: bool,
) -> Result<IdempotencyOutcome> {
    use rocky_core::idempotency::{IdempotencyBackend, IdempotencyCheck, IdempotencyError};

    let backend = IdempotencyBackend::from_state_config(state_cfg);
    let backend_label = backend.backend_label();

    // For the local + tiered paths the StateStore is the primary (local) or
    // mirror (tiered) write target. `open_read_only` is insufficient — the
    // claim performs a redb write txn. Fall back to an in-memory store
    // error-out when the state store can't be opened at all so a broken
    // state file doesn't silently bypass dedup.
    let state_store = match backend {
        IdempotencyBackend::Local
        | IdempotencyBackend::Valkey {
            mirror_to_redb: true,
            ..
        } => Some(StateStore::open(state_path).with_context(|| {
            format!(
                "failed to open state store at {} for idempotency claim",
                state_path.display()
            )
        })?),
        _ => None,
    };

    let verdict = backend
        .check_and_claim(state_store.as_ref(), key, run_id, &state_cfg.idempotency)
        .await
        .map_err(|e| match e {
            IdempotencyError::UnsupportedBackend { backend } => anyhow::anyhow!(
                "--idempotency-key is not supported on state backend \"{backend}\" yet. \
                 Switch to `tiered` (Valkey + S3 fallback — recommended for multi-pod) \
                 or `valkey` to enable idempotency, or drop the flag to proceed without dedup."
            ),
            other => anyhow::Error::from(other),
        })?;

    match verdict {
        IdempotencyCheck::Proceed => {
            info!(
                idempotency_key = key,
                run_id = run_id,
                backend = backend_label,
                "idempotency key claimed — proceeding"
            );
            Ok(IdempotencyOutcome::Proceed(IdempotencyCtx {
                key: key.to_string(),
                backend,
                config: state_cfg.idempotency.clone(),
                _claim_backend_label: backend_label,
            }))
        }
        IdempotencyCheck::AdoptStale { prior_run_id } => {
            warn!(
                idempotency_key = key,
                run_id = run_id,
                prior_run_id = %prior_run_id,
                backend = backend_label,
                "adopted stale idempotency claim (previous run appears crashed) — proceeding"
            );
            Ok(IdempotencyOutcome::Proceed(IdempotencyCtx {
                key: key.to_string(),
                backend,
                config: state_cfg.idempotency.clone(),
                _claim_backend_label: backend_label,
            }))
        }
        IdempotencyCheck::SkipCompleted {
            run_id: prior_run_id,
        } => {
            emit_skipped_run_output(
                key,
                &prior_run_id,
                rocky_core::state::RunStatus::SkippedIdempotent,
                output_json,
            )?;
            Ok(IdempotencyOutcome::Skipped)
        }
        IdempotencyCheck::SkipInFlight {
            run_id: prior_run_id,
        } => {
            emit_skipped_run_output(
                key,
                &prior_run_id,
                rocky_core::state::RunStatus::SkippedInFlight,
                output_json,
            )?;
            Ok(IdempotencyOutcome::Skipped)
        }
    }
}

/// Emit a short-circuit `RunOutput` (status = `skipped_*`) and exit. No
/// state-store mutation, no hook fires, no pipeline-start event — the run
/// never actually started.
fn emit_skipped_run_output(
    key: &str,
    prior_run_id: &str,
    status: rocky_core::state::RunStatus,
    output_json: bool,
) -> Result<()> {
    let mut output = RunOutput::new(String::new(), 0, 0);
    output.status = status;
    output.skipped_by_run_id = Some(prior_run_id.to_string());
    output.idempotency_key = Some(key.to_string());
    if output_json {
        print_json(&output)?;
    } else {
        let label = match status {
            rocky_core::state::RunStatus::SkippedIdempotent => "skipped_idempotent",
            rocky_core::state::RunStatus::SkippedInFlight => "skipped_in_flight",
            _ => "skipped",
        };
        info!(
            idempotency_key = key,
            prior_run_id = prior_run_id,
            status = label,
            "idempotency key already claimed — skipping run"
        );
    }
    Ok(())
}

/// Sweep expired idempotency entries + stale-InFlight corpses on the
/// local/mirror state store. Runs regardless of whether THIS invocation
/// supplied `--idempotency-key` — keeps the table tidy for future dedup
/// consumers and bounds redb growth.
///
/// Failures are logged and swallowed; GC is purely opportunistic.
async fn sweep_idempotency_best_effort(
    _ctx: Option<&IdempotencyCtx>,
    state_store: Option<&StateStore>,
    config: &rocky_core::config::IdempotencyConfig,
) {
    let Some(store) = state_store else {
        return;
    };
    match store.idempotency_sweep_expired(Utc::now(), config.in_flight_ttl_hours) {
        Ok(count) if count > 0 => {
            info!(
                swept_count = count,
                retention_days = config.retention_days,
                in_flight_ttl_hours = config.in_flight_ttl_hours,
                "swept expired idempotency entries"
            );
        }
        Ok(_) => {}
        Err(e) => warn!(
            error = %e,
            "idempotency sweep failed; retrying on next run"
        ),
    }
}

/// Finalize the idempotency entry for the current run. Called from the
/// same exit paths as [`persist_run_record`] (every terminal branch of
/// `rocky run`).
///
/// One-shot: takes the ctx out of the `Option` on the first call so that
/// the outer error-path wrapper in [`run`] doesn't re-finalize an already-
/// stamped entry. (Without this, a `budget_result?` after a Succeeded
/// finalize would trigger the wrapper to issue a Failed finalize, which
/// under the default `dedup_on = "success"` policy would delete the
/// Succeeded entry and incorrectly free the key for re-run. FR-004 F1.)
///
/// Swallows errors with a warn — state-store or Valkey flakiness here must
/// not flip the exit code of an otherwise successful run. The worst case
/// is a stale `InFlight` entry that the `in_flight_ttl_hours` sweep will
/// eventually reap.
async fn finalize_idempotency(
    ctx_slot: &mut Option<IdempotencyCtx>,
    state_store: Option<&StateStore>,
    run_id: &str,
    output: &RunOutput,
) {
    let Some(ctx) = ctx_slot.take() else {
        return;
    };
    let outcome = match output.derive_run_status() {
        rocky_core::state::RunStatus::Success => rocky_core::idempotency::FinalOutcome::Succeeded,
        rocky_core::state::RunStatus::PartialFailure | rocky_core::state::RunStatus::Failure => {
            rocky_core::idempotency::FinalOutcome::Failed
        }
        // Skip variants never reach finalize — we short-circuit before
        // starting the pipeline — but be safe.
        rocky_core::state::RunStatus::SkippedIdempotent
        | rocky_core::state::RunStatus::SkippedInFlight => {
            return;
        }
    };
    if let Err(e) = ctx
        .backend
        .finalize(state_store, &ctx.key, run_id, outcome, &ctx.config)
        .await
    {
        warn!(
            error = %e,
            idempotency_key = %ctx.key,
            run_id = run_id,
            "failed to finalize idempotency entry; key may remain InFlight until TTL sweep"
        );
    }
}

/// Finalize the idempotency entry as `Failed` on the error path of
/// [`run`], used by the outer wrapper that catches any `?` / `bail!` that
/// escapes the run body before the normal `finalize_idempotency` site
/// runs.
///
/// Without this, a `rocky run --idempotency-key K` that bails with any
/// error before [`persist_run_record`] (e.g. missing discovery adapter,
/// state-store open failure, adapter auth, bad pipeline config) leaves
/// the `InFlight` claim in place. The next retry with the same key then
/// returns `skipped_in_flight` for up to `in_flight_ttl_hours` (default
/// 24h) — the F1 follow-up to FR-004.
///
/// This helper opens its own `StateStore` because the body may have
/// failed before one was successfully opened. Best-effort throughout —
/// any error here is swallowed with a `warn!` so the original run error
/// is the one that surfaces to the caller.
async fn finalize_idempotency_on_error(
    ctx_slot: &mut Option<IdempotencyCtx>,
    state_path: &Path,
    run_id: &str,
) {
    use rocky_core::idempotency::{FinalOutcome, IdempotencyBackend};
    let Some(ctx) = ctx_slot.take() else {
        return;
    };
    // For Local + mirrored Tiered, finalize needs an open state store.
    // Pure Valkey / object-store backends don't — finalize dispatches
    // on the backend enum itself.
    let state_store = match ctx.backend {
        IdempotencyBackend::Local
        | IdempotencyBackend::Valkey {
            mirror_to_redb: true,
            ..
        } => match StateStore::open(state_path) {
            Ok(s) => Some(s),
            Err(e) => {
                warn!(
                    error = %e,
                    idempotency_key = %ctx.key,
                    run_id = run_id,
                    "failed to open state store to release idempotency claim on error; \
                     key may remain InFlight until TTL sweep"
                );
                return;
            }
        },
        _ => None,
    };
    if let Err(e) = ctx
        .backend
        .finalize(
            state_store.as_ref(),
            &ctx.key,
            run_id,
            FinalOutcome::Failed,
            &ctx.config,
        )
        .await
    {
        warn!(
            error = %e,
            idempotency_key = %ctx.key,
            run_id = run_id,
            "failed to release idempotency claim on error path; \
             key may remain InFlight until TTL sweep"
        );
    } else {
        info!(
            idempotency_key = %ctx.key,
            run_id = run_id,
            "released idempotency claim on error path; retry with the same key can proceed immediately"
        );
    }
}

/// Finalize the idempotency entry as `Succeeded` on the happy-path exit of
/// a non-replication dispatch arm (Transformation / Quality / Snapshot /
/// Load), which returns early from [`run`] without touching the replication
/// path's `finalize_idempotency` site.
///
/// Without this, a successful run of one of those four arms leaves its
/// `InFlight` stamp in place — the outer error-path wrapper only fires when
/// the body returns `Err`. The next retry of the same key then returns
/// `skipped_in_flight` for up to `in_flight_ttl_hours` (default 24h), the
/// exact latent-landmine behaviour the F1 wrapper closed for the error
/// path. FR-004 F2.
///
/// Opens its own `StateStore` because the four dispatch arms delegate to
/// helpers in `run_local.rs` / `load.rs` that don't thread the claim's
/// state store back out. The claim and this finalize both target the
/// canonical `state_path` owned by `run()`, so stamping here matches the
/// claim's location exactly.
///
/// Best-effort throughout — any error here is swallowed with `warn!` so a
/// successful pipeline run never exits non-zero due to bookkeeping flake.
/// One-shot via `.take()` so the outer error-path wrapper is a no-op after
/// this call, mirroring [`finalize_idempotency`]'s contract.
async fn finalize_idempotency_on_success(
    ctx_slot: &mut Option<IdempotencyCtx>,
    state_path: &Path,
    run_id: &str,
) {
    use rocky_core::idempotency::{FinalOutcome, IdempotencyBackend};
    let Some(ctx) = ctx_slot.take() else {
        return;
    };
    // Local + mirrored Tiered need an open state store; pure Valkey /
    // object-store backends finalize through the backend itself.
    let state_store = match ctx.backend {
        IdempotencyBackend::Local
        | IdempotencyBackend::Valkey {
            mirror_to_redb: true,
            ..
        } => match StateStore::open(state_path) {
            Ok(s) => Some(s),
            Err(e) => {
                warn!(
                    error = %e,
                    idempotency_key = %ctx.key,
                    run_id = run_id,
                    "failed to open state store to stamp idempotency success; \
                     key may remain InFlight until TTL sweep"
                );
                return;
            }
        },
        _ => None,
    };
    if let Err(e) = ctx
        .backend
        .finalize(
            state_store.as_ref(),
            &ctx.key,
            run_id,
            FinalOutcome::Succeeded,
            &ctx.config,
        )
        .await
    {
        warn!(
            error = %e,
            idempotency_key = %ctx.key,
            run_id = run_id,
            "failed to stamp idempotency success; key may remain InFlight until TTL sweep"
        );
    }
}

/// Execute `rocky run` — full pipeline.
#[allow(clippy::too_many_arguments)]
#[tracing::instrument(skip_all, name = "run")]
pub async fn run(
    config_path: &Path,
    filter: Option<&str>,
    pipeline_name_arg: Option<&str>,
    state_path: &Path,
    governance_override: Option<&GovernanceOverride>,
    output_json: bool,
    models_dir: Option<&Path>,
    run_all: bool,
    resume_run_id: Option<&str>,
    resume_latest: bool,
    shadow_config: Option<&rocky_core::shadow::ShadowConfig>,
    partition_opts: &PartitionRunOptions,
    model_name_filter: Option<&str>,
    // `--cache-ttl <seconds>` override (Arc 7 wave 2 wave-2 PR 4).
    // Applied to the model-execution schema cache read; the replication
    // path doesn't consult the cache, so this flag is a no-op for
    // replication-only runs.
    cache_ttl_override: Option<u64>,
    // Caller-supplied `--idempotency-key`. `None` bypasses the dedup path
    // entirely. When `Some`, the run is skipped if the key already dedups
    // (see `rocky_core::idempotency`) or proceeds and stamps the key at
    // every terminal exit.
    idempotency_key: Option<&str>,
    // Caller-supplied `--env <name>`. Flows into
    // `RockyConfig::resolve_mask_for_env` during the post-DAG
    // governance reconcile so `[mask.<env>]` overrides win over the
    // workspace `[mask]` defaults. `None` = resolve against defaults
    // only (the pre-1.16 behavior). The role-graph reconcile is
    // env-invariant — Rocky's role config has no `[role.<env>]`
    // override shape — so this value does NOT flow into
    // `reconcile_role_graph`.
    env: Option<&str>,
) -> Result<()> {
    let start = Instant::now();
    let started_at = Utc::now();
    // Unified run_id minted up front so the idempotency claim and every
    // downstream persistence site (model-only, replication, interrupted,
    // main exit) share one identifier. That keeps
    // `IdempotencyEntry::run_id` == `RunRecord::run_id` so operators can
    // cross-reference a `skipped_by_run_id` directly against
    // `rocky history`.
    let run_id = format!("run-{}", started_at.format("%Y%m%d-%H%M%S-%3f"));

    // -------------------------------------------------------------------
    // Idempotency check + claim (before any state mutation / run_id mint).
    // Dispatch on the configured state backend; short-circuit with a typed
    // `RunOutput` + exit 0 when the key dedups.
    // -------------------------------------------------------------------
    let rocky_cfg_for_idemp = rocky_core::config::load_rocky_config(config_path).context(
        format!("failed to load config from {}", config_path.display()),
    )?;
    let mut idempotency_ctx = match idempotency_key {
        Some(key) => {
            match try_claim_idempotency(
                &rocky_cfg_for_idemp.state,
                state_path,
                key,
                &run_id,
                output_json,
            )
            .await?
            {
                IdempotencyOutcome::Skipped => return Ok(()),
                IdempotencyOutcome::Proceed(ctx) => Some(ctx),
            }
        }
        None => None,
    };

    // FR-004 F1: wrap the entire run body so that any `?` / `bail!` /
    // early error return *after the claim* still releases the idempotency
    // stamp. Without this wrapper, a run that errored before the existing
    // `finalize_idempotency` sites would leave the `InFlight` claim in
    // redb/Valkey/object-store until the 24h `in_flight_ttl_hours` sweep
    // reaped it — so a retry with the same key returned `skipped_in_flight`
    // for up to 24h instead of proceeding. The happy / interrupted /
    // partial-failure paths each call `finalize_idempotency` which is
    // one-shot (takes the ctx out of the `Option`); if that already fired,
    // the wrapper's error-path finalize is a no-op.
    let run_result: Result<()> = async {

    // OTLP metrics exporter (feature-gated). Auto-initialises when
    // `OTEL_EXPORTER_OTLP_ENDPOINT` is set so operators can opt in by
    // env alone — no CLI flag needed. Drop flushes the final snapshot
    // and shuts the periodic reader down, so this guard covers every
    // exit path (happy, interrupted, error) without threading an
    // explicit cleanup call.
    let _otel_guard = crate::otel_guard::OtelGuard::init_if_enabled();

    // Detect Dagster Pipes mode. When the parent process is a Dagster
    // job that launched us via PipesSubprocessClient, both
    // DAGSTER_PIPES_CONTEXT and DAGSTER_PIPES_MESSAGES are set; we
    // emit structured events on the messages channel as the run
    // progresses. Outside Pipes mode, this is a no-op.
    let pipes = crate::pipes::PipesEmitter::detect();
    if let Some(p) = &pipes {
        p.log("INFO", "rocky run starting");
    }

    // Load config (v2 format)
    let rocky_cfg = rocky_core::config::load_rocky_config(config_path).context(format!(
        "failed to load config from {}",
        config_path.display()
    ))?;
    let config_hash = crate::output::config_fingerprint(config_path);

    // Apply the optional `--cache-ttl` override once at the top of the
    // run. All downstream `execute_models` calls receive this already-
    // overridden `SchemaCacheConfig`, so threading an `Option<u64>`
    // through the execution tree isn't needed.
    let schema_cache_cfg = rocky_cfg
        .cache
        .schemas
        .clone()
        .with_ttl_override(cache_ttl_override);

    // Model-only execution: skip the entire replication path and execute
    // just the named model. Dagster uses this for per-asset materialization
    // when it controls the DAG scheduling.
    if let Some(target_model) = model_name_filter {
        let mdir = models_dir.unwrap_or_else(|| Path::new("models"));
        anyhow::ensure!(
            mdir.exists(),
            "models directory '{}' not found (required for --model)",
            mdir.display()
        );

        let adapter_registry = AdapterRegistry::from_config(&rocky_cfg)?;
        // Use the first transformation pipeline's target adapter, or fall back
        // to the first replication pipeline's target adapter.
        let target_adapter_name = rocky_cfg
            .pipelines
            .values()
            .find_map(|p| match p {
                rocky_core::config::PipelineConfig::Transformation(t) => {
                    Some(t.target.adapter.clone())
                }
                _ => None,
            })
            .or_else(|| {
                rocky_cfg.pipelines.values().find_map(|p| match p {
                    rocky_core::config::PipelineConfig::Replication(r) => {
                        Some(r.target.adapter.clone())
                    }
                    _ => None,
                })
            })
            .unwrap_or_else(|| "default".to_string());

        let warehouse = adapter_registry.warehouse_adapter(&target_adapter_name)?;
        let state_store = rocky_core::state::StateStore::open(state_path).ok();
        // `run_id` minted at the top of `run()` — model-only, replication,
        // and idempotency-claim paths all share one identifier.
        let mut output = RunOutput::new(
            filter.unwrap_or("").to_string(),
            0, // duration filled at end
            1, // concurrency
        );
        if let Some(ctx) = &idempotency_ctx {
            output.idempotency_key = Some(ctx.key.clone());
        }

        execute_models(
            mdir,
            warehouse.as_ref(),
            state_store.as_ref(),
            partition_opts,
            &run_id,
            Some(target_model),
            &mut output,
            None, // model-only run has no pipeline hooks
            None,
            &schema_cache_cfg,
        )
        .await?;

        output.duration_ms = start.elapsed().as_millis() as u64;

        let adapter_type = rocky_cfg
            .adapters
            .get(&target_adapter_name)
            .map(|a| a.adapter_type.clone())
            .unwrap_or_default();
        output.populate_cost_summary(&adapter_type, &rocky_cfg.cost);
        let budget_result = output.check_and_record_budget(&rocky_cfg.budget, Some(&run_id));

        // Persist the model-only RunRecord. `rocky replay <run_id>` + cost
        // queries work against per-asset materializations the same way as
        // full-pipeline runs.
        //
        // Audit context: model-only runs have no single target catalog
        // (we don't know which pipeline's templates are in scope), so
        // `target_catalog = None`. Every other audit field populates
        // normally.
        let audit_ctx = AuditContext::detect(
            idempotency_ctx.as_ref().map(|c| c.key.clone()),
            None,
        );
        let audit = audit_to_record(&audit_ctx);
        persist_run_record(
            state_store.as_ref(),
            &output,
            &run_id,
            started_at,
            &config_hash,
            &audit,
        );
        finalize_idempotency(
            &mut idempotency_ctx,
            state_store.as_ref(),
            &run_id,
            &output,
        )
        .await;

        if output_json {
            print_json(&output)?;
        }
        budget_result?;
        return Ok(());
    }

    let (pipeline_name, pipeline_config) =
        registry::resolve_pipeline(&rocky_cfg, pipeline_name_arg)?;

    info!(
        pipeline = pipeline_name,
        pipeline_type = pipeline_config.pipeline_type_str(),
        "resolved pipeline"
    );

    // Hook lifecycle wiring (follow-up to §P2.6). Builds the registry
    // once from the loaded config and drives the four lifecycle events
    // we can emit without restructuring the 1700-line run body:
    //
    //   pipeline_start  — right after run_id is established (below)
    //   pipeline_complete — just before the happy-path Ok(())
    //   pipeline_error  — at the two explicit error returns (Ctrl-C
    //                      Interrupted, partial-failure anyhow::bail!)
    //   wait_async_webhooks — before returning, so fire-and-forget
    //                      webhooks don't vanish
    //
    // Per-table events (before_materialize, after_materialize,
    // materialize_error) require threading the registry through the
    // parallel dispatch loop and are a separate PR.
    //
    // Errors propagating via `?` skip the lifecycle emit — also a
    // known limitation that a top-level wrapper could close.
    let hook_registry = std::sync::Arc::new(HookRegistry::from_config(&rocky_cfg.hooks));

    // Dispatch by pipeline type. Non-replication types have their own
    // execution paths and don't fall through to the replication logic below.
    //
    // Idempotency finalize: each non-replication arm stamps `Succeeded` on
    // its happy-path exit via `finalize_idempotency_on_success`. We can't
    // thread the claim into `run_local::run_*` / `load::run_load` without
    // restructuring those four public entry points, so we stamp at the
    // dispatch site after the delegated call returns `Ok(())`. The one-
    // shot `.take()` on the ctx means the outer `run_result.is_err()`
    // wrapper below is a no-op on the happy path — same invariant #237
    // established for `finalize_idempotency`. On delegated `Err`, the
    // `?` propagates out of the async block and the wrapper releases the
    // `InFlight` stamp. FR-004 F2.
    match pipeline_config {
        rocky_core::config::PipelineConfig::Transformation(t) => {
            super::run_local::run_transformation(
                config_path,
                t,
                &rocky_cfg,
                output_json,
                partition_opts,
                &schema_cache_cfg,
            )
            .await?;
            finalize_idempotency_on_success(&mut idempotency_ctx, state_path, &run_id).await;
            return Ok(());
        }
        rocky_core::config::PipelineConfig::Quality(q) => {
            super::run_local::run_quality(config_path, q, &rocky_cfg, output_json).await?;
            finalize_idempotency_on_success(&mut idempotency_ctx, state_path, &run_id).await;
            return Ok(());
        }
        rocky_core::config::PipelineConfig::Snapshot(s) => {
            super::run_local::run_snapshot(config_path, s, &rocky_cfg, output_json).await?;
            finalize_idempotency_on_success(&mut idempotency_ctx, state_path, &run_id).await;
            return Ok(());
        }
        rocky_core::config::PipelineConfig::Replication(_) => {}
        rocky_core::config::PipelineConfig::Load(_) => {
            // Delegate to the `rocky load` command, driving with the pipeline's
            // own source_dir/format/target. This lets `rocky run --pipeline X`
            // work uniformly across all pipeline types.
            super::load::run_load(
                config_path,
                None, // cli_source_dir — take from config
                None, // format — take from config
                None, // target_table — take from config
                Some(pipeline_name),
                false, // truncate — honour config only
                output_json,
            )
            .await?;
            finalize_idempotency_on_success(&mut idempotency_ctx, state_path, &run_id).await;
            return Ok(());
        }
    }

    let pipeline = pipeline_config
        .as_replication()
        .context("expected replication pipeline")?;

    // Build the governance audit context once, at the top of the
    // replication path. Every `persist_run_record` call site (happy
    // exit, interrupted exit) pulls from the same bundle so a single
    // `rocky run` invocation produces a consistent stamp across all
    // terminal branches. For replication pipelines the target catalog
    // is templated (`{client}`, etc.) and resolves per-table — we
    // record the literal template string here as the "best available
    // at claim time" answer; per-table resolutions remain on each
    // `ModelExecution`.
    let audit_ctx = AuditContext::detect(
        idempotency_ctx.as_ref().map(|c| c.key.clone()),
        Some(pipeline.target.catalog_template.clone()),
    );
    let audit = audit_to_record(&audit_ctx);

    // All adapters flow through this production-grade execution path.
    // Databricks-specific batch optimizations (batch describe, batch tagging,
    // UNION ALL checks) activate only when a Databricks connector is available;
    // non-Databricks adapters (DuckDB, Snowflake, BigQuery) get the same
    // parallel execution, drift detection, governance, checkpoint/resume,
    // retry, anomaly detection, and state sync — just with per-table fallbacks
    // instead of batched queries.

    let pattern = pipeline.schema_pattern()?;
    let parsed_filter = filter.map(parse_filter).transpose()?;

    // Download state from remote storage (S3/GCS/Valkey) if configured
    if let Err(e) = rocky_core::state_sync::download_state(&rocky_cfg.state, state_path).await {
        warn!(error = %e, "state download failed, continuing with local state");
    }

    // Build adapter registry and resolve adapters
    let adapter_registry = AdapterRegistry::from_config(&rocky_cfg)?;
    let warehouse_adapter = adapter_registry.warehouse_adapter(&pipeline.target.adapter)?;

    // Batch check adapter (optional): present when the warehouse has an
    // optimised UNION-ALL / information_schema path (Databricks today).
    // When absent, run.rs falls back to per-table queries via the
    // generic WarehouseAdapter methods — same observable behaviour.
    let batch_check_adapter: Option<Arc<dyn BatchCheckAdapter>> =
        adapter_registry.batch_check_adapter(&pipeline.target.adapter);

    // Governance adapter: wraps catalog/permission/workspace managers behind
    // the GovernanceAdapter trait. Constructed by the registry so run.rs is
    // agnostic to the underlying warehouse kind. When the target adapter
    // doesn't support governance, the registry returns a NoopGovernanceAdapter.
    let governance_adapter: Box<dyn GovernanceAdapter> =
        adapter_registry.governance_adapter(&pipeline.target.adapter);

    let state_store = StateStore::open(state_path).context(format!(
        "failed to open state store at {}",
        state_path.display()
    ))?;

    // Discover sources
    let discovery_result = async {
        if let Some(ref disc) = pipeline.source.discovery {
            let discovery_adapter = adapter_registry.discovery_adapter(&disc.adapter)?;
            discovery_adapter
                .discover(&pattern.prefix)
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))
        } else {
            anyhow::bail!("no discovery adapter configured for this pipeline")
        }
    }
    .instrument(info_span!("discover_sources"))
    .await?;
    if !discovery_result.failed.is_empty() {
        // FR-014: a failed-source list at run time means the run will
        // execute the healthy subset, but the consumer should know that
        // missing materializations are not the same as deletions.
        warn!(
            count = discovery_result.failed.len(),
            "run: source(s) failed metadata fetch — these will be absent from \
             materializations but were NOT removed upstream"
        );
    }
    let connectors = discovery_result.connectors;

    let concurrency = pipeline.execution.concurrency.max_concurrency();
    let mut output = RunOutput::new(filter.unwrap_or("").to_string(), 0, concurrency);
    output.shadow = shadow_config.is_some();
    if let Some(ctx) = &idempotency_ctx {
        output.idempotency_key = Some(ctx.key.clone());
    }
    let mut catalogs_created: usize = 0;
    let mut schemas_created: usize = 0;
    let mut created_catalogs: std::collections::HashSet<String> = std::collections::HashSet::new();
    // All distinct target catalogs resolved from connectors this run,
    // regardless of whether Rocky created them. Fed to
    // `GovernanceAdapter::reconcile_role_graph` so role-graph GRANTs
    // land on every managed catalog — not just the newly-created ones
    // (`created_catalogs` above is gated on `auto_create_catalogs`,
    // so pre-provisioned catalogs would otherwise be skipped).
    let mut managed_catalogs: std::collections::BTreeSet<String> =
        std::collections::BTreeSet::new();

    let mut source_batch_refs: Vec<TableRef> = Vec::new();
    let mut target_batch_refs: Vec<TableRef> = Vec::new();
    let mut freshness_batch_refs: Vec<TableRef> = Vec::new();
    let mut batch_asset_keys: Vec<(String, Vec<String>)> = Vec::new();
    let mut pending_checks: HashMap<String, PendingCheck> = HashMap::new();
    let mut tables_to_process: Vec<TableTask> = Vec::new();

    let governance = &pipeline.target.governance;
    let target_catalog_template = &pipeline.target.catalog_template;
    let target_schema_template = &pipeline.target.schema_template;
    let target_sep = pipeline
        .target
        .separator
        .as_deref()
        .unwrap_or(&pattern.separator);

    // --- Sequential: catalog/schema setup + table collection ---
    // Governance operations route through the GovernanceAdapter trait
    // (Plan 01: genericize run.rs). Catalog/schema creation uses
    // SqlDialect + WarehouseAdapter for SQL generation + execution.
    // Span kept as a declaration only; `.entered()` would hold an
    // `EnteredSpan` (!Send) across the awaits below and break the DAG
    // executor's per-layer parallelism. Tracing events below remain at the
    // subscriber's root context — acceptable for a setup block.
    let _governance_span = info_span!("governance_setup");
    {
        let dialect = warehouse_adapter.dialect();

        for conn in &connectors {
            let parsed = match pattern.parse(&conn.schema) {
                Ok(p) => p,
                Err(_) => continue,
            };

            if let Some((ref filter_key, ref filter_value)) = parsed_filter {
                if !matches_filter(conn, &parsed, filter_key, filter_value) {
                    continue;
                }
            }

            let components = parsed_to_json_map(&parsed);
            let target_catalog = parsed.resolve_template(target_catalog_template, target_sep);
            // Track every catalog this run touches — feeds the
            // role-graph GRANT emission path downstream. Unconditional:
            // we don't want to miss pre-provisioned catalogs where
            // `auto_create_catalogs = false`.
            managed_catalogs.insert(target_catalog.clone());
            let target_schema = if let Some(shadow_cfg) = shadow_config {
                shadow_cfg
                    .schema_override
                    .clone()
                    .unwrap_or_else(|| parsed.resolve_template(target_schema_template, target_sep))
            } else {
                parsed.resolve_template(target_schema_template, target_sep)
            };

            // Create catalog + apply governance (once per catalog)
            if governance.auto_create_catalogs && !created_catalogs.contains(&target_catalog) {
                // FR-009: fail fast on the silent full-revoke footgun.
                // `governance_override.workspace_ids = []` without the
                // opt-in flag would otherwise tell the reconciler below
                // to revoke every existing binding on the catalog.
                // Runs before any warehouse mutation for this catalog
                // so an accidental payload can't leave partial state
                // behind.
                if let Some(ov) = governance_override {
                    ov.validate_workspace_ids(&target_catalog)
                        .map_err(|e| anyhow::anyhow!("{e}"))?;
                }

                // Create catalog via generic dialect SQL
                if let Some(sql_result) = dialect.create_catalog_sql(&target_catalog) {
                    warehouse_adapter
                        .execute_statement(&sql_result?)
                        .await
                        .map_err(|e| anyhow::anyhow!("{e}"))?;
                }
                catalogs_created += 1;

                // Tags via GovernanceAdapter trait
                let tags = governance.build_tags(&components);
                if let Err(e) = governance_adapter
                    .set_tags(&TagTarget::Catalog(target_catalog.clone()), &tags)
                    .await
                {
                    warn!(catalog = target_catalog, error = %e, "set catalog tags failed");
                }

                // Merge workspace bindings: config defaults + per-run override (override wins)
                let mut binding_map: std::collections::HashMap<
                    u64,
                    rocky_core::config::WorkspaceBindingConfig,
                > = std::collections::HashMap::new();
                if let Some(isolation) = &governance.isolation {
                    for b in &isolation.workspace_ids {
                        binding_map.insert(b.id, b.clone());
                    }
                }
                // `workspace_ids` is `Option<Vec<_>>`: `None` means "no
                // per-run override, preserve the config-default bindings",
                // and `Some(ids)` (including `Some(vec![])` when the
                // `allow_empty_workspace_ids` opt-in is set) is an
                // explicit desired state that replaces any overlap by
                // id. The FR-009 validator above has already rejected
                // the dangerous shape (`Some(empty)` without opt-in).
                if let Some(ov) = governance_override {
                    if let Some(ids) = ov.workspace_ids.as_ref() {
                        for b in ids {
                            binding_map.insert(b.id, b.clone());
                        }
                    }
                }
                let all_bindings: Vec<rocky_core::config::WorkspaceBindingConfig> = {
                    let mut v: Vec<_> = binding_map.into_values().collect();
                    v.sort_by_key(|b| b.id);
                    v
                };

                // Reconcile workspace bindings via GovernanceAdapter (best-effort).
                //
                // Unity Catalog users isolating catalogs per workspace previously
                // reconciled bindings out-of-band via scripts; Rocky now treats the
                // desired binding set as declarative state and reconciles
                // current-vs-desired in one pass alongside grants. Bindings
                // declared in config are added, bindings present on the catalog
                // but absent from config are removed, and access-level changes
                // (`READ_WRITE` → `READ_ONLY`) land as a single remove + add.
                //
                // Behavior change: prior releases treated `workspace_ids` as
                // imperative additive state (never removed drift); reconcile
                // semantics now mean bindings not declared in config are
                // removed on next run. Users who hand-added bindings outside
                // `rocky.toml` should declare them there before upgrading.
                //
                // Adapters that don't support workspace bindings (Snowflake,
                // BigQuery, DuckDB) return `Ok(vec![])` from
                // `list_workspace_bindings`, so the diff is empty and the loop
                // is a no-op — matching the existing "not applicable" semantics.
                let current_bindings: Vec<(u64, String)> = governance_adapter
                    .list_workspace_bindings(&target_catalog)
                    .await
                    .unwrap_or_else(|e| {
                        warn!(
                            catalog = target_catalog,
                            error = %e,
                            "workspace-binding reconcile skipped; \
                             drift will not be detected this run \
                             (list_workspace_bindings failed)"
                        );
                        vec![]
                    });

                // Compute add/remove diff by workspace_id.
                let desired_ids: std::collections::HashSet<u64> =
                    all_bindings.iter().map(|b| b.id).collect();
                let current_ids: std::collections::HashMap<u64, String> =
                    current_bindings.into_iter().collect();

                for binding in &all_bindings {
                    let desired_kind = binding.binding_type.as_api_str();
                    let needs_apply = !matches!(
                        current_ids.get(&binding.id),
                        Some(current_kind) if current_kind == desired_kind
                    );
                    if needs_apply {
                        if let Err(e) = governance_adapter
                            .bind_workspace(&target_catalog, binding.id, desired_kind)
                            .await
                        {
                            warn!(
                                catalog = target_catalog,
                                workspace_id = binding.id,
                                binding_type = desired_kind,
                                error = %e,
                                "workspace binding failed"
                            );
                        }
                    }
                }

                // Remove bindings that exist on the catalog but aren't desired.
                for &cur_id in current_ids.keys() {
                    if !desired_ids.contains(&cur_id) {
                        if let Err(e) = governance_adapter
                            .remove_workspace_binding(&target_catalog, cur_id)
                            .await
                        {
                            warn!(
                                catalog = target_catalog,
                                workspace_id = cur_id,
                                error = %e,
                                "remove workspace binding failed"
                            );
                        }
                    }
                }

                let should_isolate = governance.isolation.as_ref().is_some_and(|i| i.enabled);
                if should_isolate {
                    if let Err(e) = governance_adapter
                        .set_isolation(&target_catalog, true)
                        .await
                    {
                        warn!(catalog = target_catalog, error = %e, "catalog isolation failed");
                    }
                }

                // Merge catalog grants: config defaults + per-run override
                let mut all_grants = governance.grants.clone();
                if let Some(ov) = governance_override {
                    all_grants.extend(ov.grants.clone());
                }

                // Catalog-level grants via GovernanceAdapter (best-effort)
                if !all_grants.is_empty() {
                    let grants: Vec<Grant> = all_grants
                        .iter()
                        .flat_map(|grant_cfg| {
                            grant_cfg.permissions.iter().filter_map(|perm_str| {
                                let permission = match perm_str.as_str() {
                                    "BROWSE" => Permission::Browse,
                                    "USE CATALOG" => Permission::UseCatalog,
                                    "USE SCHEMA" => Permission::UseSchema,
                                    "SELECT" => Permission::Select,
                                    "MODIFY" => Permission::Modify,
                                    "MANAGE" => Permission::Manage,
                                    other => {
                                        warn!(permission = other, "unknown permission, skipping");
                                        return None;
                                    }
                                };
                                Some(Grant {
                                    principal: grant_cfg.principal.clone(),
                                    permission,
                                    target: GrantTarget::Catalog(target_catalog.clone()),
                                })
                            })
                        })
                        .collect();

                    if let Err(e) = governance_adapter.apply_grants(&grants).await {
                        warn!(catalog = target_catalog, error = %e, "catalog grants failed");
                    }
                    output.permissions.grants_added += all_grants
                        .iter()
                        .map(|g| g.permissions.len())
                        .sum::<usize>();
                }

                created_catalogs.insert(target_catalog.clone());
            }

            if governance.auto_create_schemas {
                // Create schema via generic dialect SQL
                if let Some(sql_result) = dialect.create_schema_sql(&target_catalog, &target_schema)
                {
                    warehouse_adapter
                        .execute_statement(&sql_result?)
                        .await
                        .map_err(|e| anyhow::anyhow!("{e}"))?;
                }
                schemas_created += 1;

                // Schema tags via GovernanceAdapter
                let tags = governance.build_tags(&components);
                if let Err(e) = governance_adapter
                    .set_tags(
                        &TagTarget::Schema {
                            catalog: target_catalog.clone(),
                            schema: target_schema.clone(),
                        },
                        &tags,
                    )
                    .await
                {
                    warn!(
                        schema = format!("{}.{}", target_catalog, target_schema),
                        error = %e,
                        "set schema tags failed"
                    );
                }

                // Schema grants via GovernanceAdapter
                let mut all_schema_grants = governance.schema_grants.clone();
                if let Some(ov) = governance_override {
                    all_schema_grants.extend(ov.schema_grants.clone());
                }

                if !all_schema_grants.is_empty() {
                    let grants: Vec<Grant> = all_schema_grants
                        .iter()
                        .flat_map(|grant_cfg| {
                            grant_cfg.permissions.iter().filter_map(|perm_str| {
                                let permission = match perm_str.as_str() {
                                    "USE SCHEMA" => Permission::UseSchema,
                                    "SELECT" => Permission::Select,
                                    "MODIFY" => Permission::Modify,
                                    other => {
                                        warn!(
                                            permission = other,
                                            "unknown schema permission, skipping"
                                        );
                                        return None;
                                    }
                                };
                                Some(Grant {
                                    principal: grant_cfg.principal.clone(),
                                    permission,
                                    target: GrantTarget::Schema {
                                        catalog: target_catalog.clone(),
                                        schema: target_schema.clone(),
                                    },
                                })
                            })
                        })
                        .collect();

                    if let Err(e) = governance_adapter.apply_grants(&grants).await {
                        warn!(
                            schema = format!("{}.{}", target_catalog, target_schema),
                            error = %e,
                            "schema grants failed"
                        );
                    }
                }
            }

            // Collect tables to process in parallel
            let source_catalog = pipeline.source.catalog.as_deref().unwrap_or("").to_string();

            // Pre-fetch source table list to skip tables that no longer exist
            // in the source (e.g. Fivetran stopped syncing them). One
            // information_schema query per schema avoids N wasted DESCRIBE +
            // CTAS round-trips for stale tables.
            // Use the generic WarehouseAdapter::list_tables (Plan 02 trait method)
            let source_tables: std::collections::HashSet<String> = warehouse_adapter
                .list_tables(&source_catalog, &conn.schema)
                .await
                .map(|v| v.into_iter().map(|s| s.to_lowercase()).collect())
                .unwrap_or_else(|e| {
                    warn!(
                        schema = conn.schema.as_str(),
                        error = %e,
                        "failed to list source tables, will process all discovered tables"
                    );
                    conn.tables.iter().map(|t| t.name.to_lowercase()).collect()
                });

            let mut skipped_source_missing = 0usize;
            for table in &conn.tables {
                if !source_tables.contains(&table.name.to_lowercase()) {
                    // Build the same asset key the materialization path
                    // would have used (`[source_type, ...components, table]`)
                    // so downstream orchestrators can match excluded
                    // entries against planned asset keys.
                    let mut asset_key = vec![conn.source_type.clone()];
                    for v in components.values() {
                        match v {
                            serde_json::Value::String(s) => asset_key.push(s.clone()),
                            serde_json::Value::Array(arr) => {
                                for item in arr {
                                    if let Some(s) = item.as_str() {
                                        asset_key.push(s.to_string());
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    asset_key.push(table.name.clone());

                    warn!(
                        table = table.name.as_str(),
                        schema = conn.schema.as_str(),
                        "source table not found in destination catalog, excluding from run"
                    );
                    output.excluded_tables.push(ExcludedTableOutput {
                        asset_key,
                        source_schema: conn.schema.clone(),
                        table_name: table.name.clone(),
                        reason: "missing_from_source".to_string(),
                    });
                    skipped_source_missing += 1;
                    continue;
                }
                // In shadow suffix mode, append suffix to table name
                let target_table_name = if let Some(shadow_cfg) = shadow_config {
                    if shadow_cfg.schema_override.is_none() {
                        format!("{}{}", table.name, shadow_cfg.suffix)
                    } else {
                        table.name.clone()
                    }
                } else {
                    table.name.clone()
                };

                tables_to_process.push(TableTask {
                    source_catalog: source_catalog.clone(),
                    source_schema: conn.schema.clone(),
                    target_catalog: target_catalog.clone(),
                    target_schema: target_schema.clone(),
                    table_name: target_table_name,
                    asset_key_prefix: {
                        let mut prefix = vec![conn.source_type.clone()];
                        for v in components.values() {
                            match v {
                                serde_json::Value::String(s) => prefix.push(s.clone()),
                                serde_json::Value::Array(arr) => {
                                    for item in arr {
                                        if let Some(s) = item.as_str() {
                                            prefix.push(s.to_string());
                                        }
                                    }
                                }
                                _ => {}
                            }
                        }
                        prefix
                    },
                    check_column_match: pipeline.checks.column_match.enabled(),
                    check_row_count: pipeline.checks.row_count.enabled(),
                    check_freshness: pipeline.checks.freshness.is_some(),
                    column_match_exclude: pipeline
                        .metadata_columns
                        .iter()
                        .map(|mc| mc.name.clone())
                        .collect(),
                    metadata_columns: pipeline
                        .metadata_columns
                        .iter()
                        .map(|mc| MetadataColumn {
                            name: mc.name.clone(),
                            data_type: mc.data_type.clone(),
                            value: parsed.resolve_template(&mc.value, &pattern.separator),
                        })
                        .collect(),
                    governance_tags: governance.build_tags(&components),
                    // Populated later by batch pre-fetch phase
                    prefetched_source_cols: None,
                    prefetched_target_cols: None,
                });
            }
            if skipped_source_missing > 0 {
                warn!(
                    schema = conn.schema.as_str(),
                    skipped = skipped_source_missing,
                    "excluded tables missing from source — see `excluded_tables` in JSON output for details"
                );
            }
        }
    }
    // --- Checkpoint / resume: filter already-completed tables ---
    // `run_id` minted at the top of `run()`.

    // §P2.6 emit: pipeline_start. Failures here are logged by the
    // registry itself; we swallow the Result because hook failures
    // shouldn't block the pipeline.
    let _ = hook_registry
        .fire(&HookContext::pipeline_start(&run_id, pipeline_name))
        .await;

    // §P2.6 emit: discover_complete. Fired once, right after
    // pipeline_start, with the source-discovery count captured above
    // (discovery runs before run_id is established, so this is the
    // earliest emit point with a stable run_id).
    let _ = hook_registry
        .fire(&HookContext::discover_complete(
            &run_id,
            pipeline_name,
            connectors.len(),
        ))
        .await;

    let completed_keys: std::collections::HashSet<String> = if resume_latest {
        if let Ok(Some(prev)) = state_store.get_latest_run_progress() {
            let keys: std::collections::HashSet<String> = prev
                .tables
                .iter()
                .filter(|t| t.status == rocky_core::state::TableStatus::Success)
                .map(|t| t.table_key.clone())
                .collect();
            if !keys.is_empty() {
                info!(
                    resumed_from = prev.run_id,
                    tables_skipped = keys.len(),
                    "resuming from previous run"
                );
                output.resumed_from = Some(prev.run_id.clone());
            }
            keys
        } else {
            std::collections::HashSet::new()
        }
    } else if let Some(rid) = resume_run_id {
        if let Ok(Some(prev)) = state_store.get_run_progress(rid) {
            let keys: std::collections::HashSet<String> = prev
                .tables
                .iter()
                .filter(|t| t.status == rocky_core::state::TableStatus::Success)
                .map(|t| t.table_key.clone())
                .collect();
            if !keys.is_empty() {
                output.resumed_from = Some(rid.to_string());
            }
            keys
        } else {
            warn!(run_id = rid, "no progress found for run, starting fresh");
            std::collections::HashSet::new()
        }
    } else {
        std::collections::HashSet::new()
    };

    let original_count = tables_to_process.len();
    if !completed_keys.is_empty() {
        tables_to_process.retain(|task| {
            let key = format!(
                "{}.{}.{}",
                task.target_catalog, task.target_schema, task.table_name
            );
            !completed_keys.contains(&key)
        });
        let skipped = original_count - tables_to_process.len();
        output.tables_skipped = skipped;
        info!(
            skipped = skipped,
            remaining = tables_to_process.len(),
            "filtered resumed tables"
        );
    }

    // Initialize run progress tracking
    state_store
        .init_run_progress(&run_id, tables_to_process.len())
        .context("failed to initialize run progress")?;

    // --- Process tables concurrently ---
    let fail_fast = pipeline.execution.fail_fast;

    info!(
        tables = tables_to_process.len(),
        concurrency = concurrency,
        "processing tables"
    );

    // --- Batch column pre-fetch: replace N×2 DESCRIBE TABLE calls with
    //     one information_schema query per unique schema pair. ---
    // Only available when the warehouse implements BatchCheckAdapter
    // (Databricks today); other adapters fall back to per-table DESCRIBE
    // via WarehouseAdapter in process_table().
    if let Some(ref bc_adapter) = batch_check_adapter {
        // Collect unique (catalog, schema) pairs for source and target
        let source_schemas: std::collections::HashSet<(String, String)> = tables_to_process
            .iter()
            .map(|t| (t.source_catalog.clone(), t.source_schema.clone()))
            .collect();
        let target_schemas: std::collections::HashSet<(String, String)> = tables_to_process
            .iter()
            .map(|t| (t.target_catalog.clone(), t.target_schema.clone()))
            .collect();

        // Fetch all source + target schemas in parallel
        let mut describe_futures = Vec::new();
        for (cat, sch) in &source_schemas {
            describe_futures.push(("source", cat.clone(), sch.clone()));
        }
        for (cat, sch) in &target_schemas {
            describe_futures.push(("target", cat.clone(), sch.clone()));
        }

        // key: (catalog, schema) → HashMap<table_name, Vec<ColumnInfo>>
        let mut prefetched: std::collections::HashMap<
            (String, String),
            std::collections::HashMap<String, Vec<ColumnInfo>>,
        > = std::collections::HashMap::new();

        let describe_results =
            futures::future::join_all(describe_futures.iter().map(|(side, cat, sch)| {
                let cat = cat.clone();
                let sch = sch.clone();
                let bc = bc_adapter.clone();
                async move {
                    let result = bc.batch_describe_schema(&cat, &sch).await;
                    (*side, cat, sch, result)
                }
            }))
            .await;

        // Arc 7 wave 2 wave-2 PR 2: tap successful describes into the schema
        // cache. One key per table in the returned map — the DESCRIBE cost
        // is already paid, so populating the cache for sibling tables is
        // free signal for future compiles. Write failures are logged at
        // `warn!` level and never fail the run.
        let mut schema_cache_tap = SchemaCacheWriteTap::default();

        for (side, cat, sch, result) in describe_results {
            match result {
                Ok(cols_map) => {
                    debug!(
                        side,
                        catalog = cat.as_str(),
                        schema = sch.as_str(),
                        tables = cols_map.len(),
                        "batch describe complete"
                    );
                    persist_batch_describe(
                        &state_store,
                        &rocky_cfg.cache.schemas,
                        &mut schema_cache_tap,
                        &cat,
                        &sch,
                        &cols_map,
                    );
                    prefetched.insert((cat, sch), cols_map);
                }
                Err(e) => {
                    warn!(
                        side,
                        catalog = cat.as_str(),
                        schema = sch.as_str(),
                        error = %e,
                        "batch describe failed, will fall back to per-table DESCRIBE"
                    );
                }
            }
        }

        // Attach pre-fetched columns to each task
        for task in &mut tables_to_process {
            if let Some(src_map) =
                prefetched.get(&(task.source_catalog.clone(), task.source_schema.clone()))
            {
                task.prefetched_source_cols = src_map.get(&task.table_name.to_lowercase()).cloned();
            }
            if let Some(tgt_map) =
                prefetched.get(&(task.target_catalog.clone(), task.target_schema.clone()))
            {
                task.prefetched_target_cols = tgt_map.get(&task.table_name.to_lowercase()).cloned();
            }
        }
    }

    // Adaptive concurrency throttle — when enabled, dynamically adjusts the
    // number of in-flight tasks based on rate-limit signals from the warehouse.
    // When fixed, concurrency stays constant at the configured level.
    let adaptive = pipeline.execution.concurrency.is_adaptive();
    let throttle = if adaptive {
        // increase_interval = 10 means: after 10 consecutive successes,
        // concurrency increases by 1 (or 2 during slow-start). This prevents
        // oscillation by requiring sustained success before probing higher.
        let t = AdaptiveThrottle::new(concurrency, 1, 10);
        info!(
            max = concurrency,
            min = 1,
            "adaptive concurrency enabled (AIMD throttle)"
        );
        Some(t)
    } else {
        None
    };

    let semaphore = Arc::new(Semaphore::new(concurrency));
    let shared_batch_check = batch_check_adapter.clone();
    let shared_warehouse = warehouse_adapter.clone();
    let shared_state = Arc::new(Mutex::new(state_store));
    let shared_run_id = run_id.clone();
    let shared_pipeline = Arc::new(pipeline.clone());
    let mut join_set: JoinSet<(usize, Result<TableResult, anyhow::Error>)> = JoinSet::new();

    // Background state sync — flush watermarks to remote storage.
    //
    // Cadence scales with estimated run duration so small runs don't spam
    // the object store. Duration is a coarse estimate of ~3s per table
    // divided by concurrency; the final end-of-run upload (further below)
    // always flushes state, so the periodic loop only exists to bound
    // exposure for long runs. Runs shorter than ~1 minute skip it entirely.
    let estimated_run_secs =
        (tables_to_process.len() as u64).saturating_mul(3) / (concurrency.max(1) as u64);
    let state_sync_handle = if estimated_run_secs < 60 {
        None
    } else {
        // Target one upload per quarter of the estimated run, capped at 30s.
        let cadence_secs = std::cmp::min(30, estimated_run_secs / 4).max(10);
        let cadence = Duration::from_secs(cadence_secs);
        let state_cfg = rocky_cfg.state.clone();
        let state_p = state_path.to_path_buf();
        info!(
            estimated_run_secs,
            cadence_secs, "adaptive state-sync cadence configured"
        );
        Some(tokio::spawn(async move {
            loop {
                tokio::time::sleep(cadence).await;
                if let Err(e) = rocky_core::state_sync::upload_state(&state_cfg, &state_p).await {
                    warn!(error = %e, "periodic state sync failed");
                }
            }
        }))
    };

    // Track the semaphore's effective capacity so we can adjust it when the
    // throttle changes. Starts at `concurrency` and is adjusted after each
    // completed task (only when adaptive concurrency is enabled).
    let mut semaphore_capacity = concurrency;

    // Mutable accumulators declared before the spawn loop so the inline
    // result-drain path (adaptive concurrency) and the post-spawn
    // collection loop can both append to them.
    let mut table_errors: Vec<TableError> = Vec::new();
    let mut deferred_tags: Vec<DeferredTagging> = Vec::new();
    let mut deferred_watermarks: Vec<DeferredWatermark> = Vec::new();
    let error_rate_abort_pct = pipeline.execution.error_rate_abort_pct;
    let mut total_completed: usize = 0;

    // Shutdown-signal handling: a single pinned future per signal source
    // drives both the spawn loop's permit-wait and the drain loop below.
    // On first SIGINT (Ctrl-C) or SIGTERM (K8s pod eviction) we arm a
    // hard-exit watcher for a second SIGINT, set `interrupted`, and stop
    // issuing new work. In-flight tasks drain naturally so state updates
    // remain consistent; anything not yet `Success` or `Failed` in the
    // state store after drain is marked `Interrupted`.
    let ctrl_c_signal = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c_signal);
    // SIGTERM only on Unix — Windows doesn't have it, so the future is
    // wrapped in an `Option` and a `#[cfg(unix)]`-guarded branch.
    #[cfg(unix)]
    let mut sigterm_stream =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).ok();
    let mut interrupted = false;

    for (idx, task) in tables_to_process.iter().enumerate() {
        if interrupted {
            break;
        }

        // When adaptive concurrency is active, drain completed results before
        // spawning so we can adjust the semaphore to match the throttle's
        // recommendation before acquiring the next permit. This interleaves
        // spawning and collection, enabling the AIMD feedback loop.
        if throttle.is_some() {
            while let Some(completed) = join_set.try_join_next() {
                process_completed_result(
                    completed,
                    &tables_to_process,
                    &throttle,
                    &semaphore,
                    &mut semaphore_capacity,
                    &mut output,
                    &mut pending_checks,
                    &mut source_batch_refs,
                    &mut target_batch_refs,
                    &mut freshness_batch_refs,
                    &mut batch_asset_keys,
                    &mut table_errors,
                    &mut deferred_tags,
                    &mut deferred_watermarks,
                    &shared_state,
                    &shared_run_id,
                    &mut total_completed,
                )
                .await;
            }
        }

        let permit = tokio::select! {
            permit = semaphore.clone().acquire_owned() => permit?,
            _ = &mut ctrl_c_signal, if !interrupted => {
                arm_hard_exit_on_second_signal();
                interrupted = true;
                break;
            }
            _ = async {
                #[cfg(unix)]
                if let Some(s) = sigterm_stream.as_mut() {
                    let _ = s.recv().await;
                } else {
                    std::future::pending::<()>().await;
                }
                #[cfg(not(unix))]
                std::future::pending::<()>().await;
            }, if !interrupted => {
                arm_hard_exit_on_second_signal();
                interrupted = true;
                break;
            }
        };
        let warehouse = shared_warehouse.clone();
        let state = shared_state.clone();
        let pipeline_ref = shared_pipeline.clone();
        let task = task.clone();

        // §P2.6 per-table emit: before_materialize fires on the main
        // task just before we spawn — the registry is shared via Arc
        // so we don't need to move it into the spawned future.
        let table_ref = format!(
            "{}.{}.{}",
            task.target_catalog, task.target_schema, task.table_name
        );
        let _ = hook_registry
            .fire(&HookContext::before_materialize(
                &run_id,
                pipeline_name,
                &table_ref,
            ))
            .await;

        join_set.spawn(async move {
            let _permit = permit;
            let result = process_table(warehouse.as_ref(), &state, &pipeline_ref, &task).await;
            (idx, result)
        });
    }

    // Collect remaining results with adaptive error rate monitoring.
    // Driven as a `loop { tokio::select! }` so a first SIGINT fires the
    // hard-exit watcher and flips `interrupted = true`; tasks already in
    // flight keep running and their results are still collected below.
    loop {
        let result = tokio::select! {
            res = join_set.join_next() => match res {
                Some(r) => r,
                None => break,
            },
            _ = &mut ctrl_c_signal, if !interrupted => {
                arm_hard_exit_on_second_signal();
                interrupted = true;
                continue;
            }
            _ = async {
                #[cfg(unix)]
                if let Some(s) = sigterm_stream.as_mut() {
                    let _ = s.recv().await;
                } else {
                    std::future::pending::<()>().await;
                }
                #[cfg(not(unix))]
                std::future::pending::<()>().await;
            }, if !interrupted => {
                arm_hard_exit_on_second_signal();
                interrupted = true;
                continue;
            }
        };
        total_completed += 1;

        match result {
            Ok((_, Ok(tr))) => {
                // Signal success to the adaptive throttle
                if let Some(t) = &throttle {
                    t.on_success();
                    adjust_semaphore(t, &semaphore, &mut semaphore_capacity);
                }
                // §P2.6 per-table emit: after_materialize on success.
                let _ = hook_registry
                    .fire(&HookContext::after_materialize(
                        &run_id,
                        pipeline_name,
                        &tr.target_full_name,
                        tr.materialization.duration_ms,
                        tr.materialization.rows_copied,
                    ))
                    .await;
                output.tables_copied += 1;
                rocky_observe::metrics::METRICS.inc_tables_processed();
                rocky_observe::metrics::METRICS
                    .record_table_duration_ms(tr.materialization.duration_ms);
                output.materializations.push(tr.materialization);

                if tr.drift_checked {
                    output.drift.tables_checked += 1;
                }
                if let Some(drift_action) = tr.drift_detected {
                    // §P2.6 per-table emit: drift_detected. The
                    // column-level list isn't plumbed through
                    // `TableResult` today (see `DriftActionOutput`: just
                    // table + action + reason), so we emit with an
                    // empty columns slice — subscribers get the
                    // table/action surface; column names are a
                    // follow-up that threads the full DriftResult.
                    let _ = hook_registry
                        .fire(&HookContext::drift_detected(
                            &run_id,
                            pipeline_name,
                            &tr.target_full_name,
                            &[],
                        ))
                        .await;
                    output.drift.tables_drifted += 1;
                    output.drift.actions_taken.push(drift_action);
                }

                if let Some(check) = tr.column_match_check {
                    let entry = pending_checks
                        .entry(tr.target_full_name.clone())
                        .or_insert_with(|| PendingCheck {
                            asset_key: tr.asset_key.clone(),
                            checks: Vec::new(),
                        });
                    entry.checks.push(check);
                }

                if let Some(src_ref) = tr.source_batch_ref {
                    source_batch_refs.push(src_ref);
                }
                if let Some(tgt_ref) = tr.target_batch_ref {
                    target_batch_refs.push(tgt_ref);
                }
                batch_asset_keys.push((tr.target_full_name.clone(), tr.asset_key.clone()));

                if let Some(fresh_ref) = tr.freshness_batch_ref {
                    freshness_batch_refs.push(fresh_ref);
                }

                if let Some(tags) = tr.deferred_tags {
                    deferred_tags.push(tags);
                }
                if let Some(wm) = tr.deferred_watermark {
                    deferred_watermarks.push(wm);
                }

                // Checkpoint: record successful table progress
                {
                    let state = shared_state.lock().await;
                    if let Err(e) = state.record_table_progress(
                        &shared_run_id,
                        &rocky_core::state::TableProgress {
                            index: total_completed - 1,
                            table_key: tr.target_full_name,
                            asset_key: tr.asset_key,
                            status: rocky_core::state::TableStatus::Success,
                            error: None,
                            duration_ms: output
                                .materializations
                                .last()
                                .map_or(0, |m| m.duration_ms),
                            completed_at: Utc::now(),
                        },
                    ) {
                        tracing::warn!(error = %e, "failed to record table progress for run ");
                    }
                }
            }
            Ok((idx, Err(e))) => {
                let msg = format!("{e:#}");
                if msg.contains("TABLE_OR_VIEW_NOT_FOUND") {
                    warn!(
                        table_index = idx,
                        error = msg.as_str(),
                        "source table not found, skipping"
                    );
                    continue;
                }

                // Signal the adaptive throttle: rate limits reduce concurrency,
                // other errors are treated as successes (they're permanent
                // failures, not a signal to slow down).
                if let Some(t) = &throttle {
                    if is_rate_limit_error(&msg) {
                        t.on_rate_limit();
                        adjust_semaphore(t, &semaphore, &mut semaphore_capacity);
                    }
                }

                warn!(error = msg, "table processing failed");
                rocky_observe::metrics::METRICS.inc_tables_failed();

                // Checkpoint: record failed table progress
                {
                    let task = tables_to_process.get(idx);
                    let table_key = task
                        .map(|t| {
                            format!("{}.{}.{}", t.target_catalog, t.target_schema, t.table_name)
                        })
                        .unwrap_or_default();

                    // §P2.6 per-table emit: materialize_error.
                    let _ = hook_registry
                        .fire(&HookContext::materialize_error(
                            &run_id,
                            pipeline_name,
                            &table_key,
                            &msg,
                        ))
                        .await;
                    let state = shared_state.lock().await;
                    if let Err(e) = state.record_table_progress(
                        &shared_run_id,
                        &rocky_core::state::TableProgress {
                            index: idx,
                            table_key,
                            asset_key: vec![],
                            status: rocky_core::state::TableStatus::Failed,
                            error: Some(msg.clone()),
                            duration_ms: 0,
                            completed_at: Utc::now(),
                        },
                    ) {
                        tracing::warn!(error = %e, "failed to record table progress for run ");
                    }
                }

                table_errors.push(TableError {
                    asset_key: vec![],
                    error: msg,
                    task_index: Some(idx),
                });
                if fail_fast {
                    join_set.abort_all();
                    break;
                }
            }
            Err(e) => {
                let msg = format!("task failed: {e}");
                warn!(error = msg, "table task panicked");

                // Checkpoint: record panicked task progress
                {
                    let state = shared_state.lock().await;
                    if let Err(e) = state.record_table_progress(
                        &shared_run_id,
                        &rocky_core::state::TableProgress {
                            index: 0,
                            table_key: String::new(),
                            asset_key: vec![],
                            status: rocky_core::state::TableStatus::Failed,
                            error: Some(msg.clone()),
                            duration_ms: 0,
                            completed_at: Utc::now(),
                        },
                    ) {
                        tracing::warn!(error = %e, "failed to record table progress for run ");
                    }
                }

                table_errors.push(TableError {
                    asset_key: vec![],
                    error: msg,
                    task_index: None,
                });
                if fail_fast {
                    join_set.abort_all();
                    break;
                }
            }
        }

        // Adaptive error rate abort
        if error_rate_abort_pct > 0 && total_completed >= 4 {
            let current_rate = (table_errors.len() as f64 / total_completed as f64 * 100.0) as u32;
            if current_rate >= error_rate_abort_pct {
                warn!(
                    error_rate = current_rate,
                    threshold = error_rate_abort_pct,
                    "error rate exceeded threshold, aborting remaining tables"
                );
                join_set.abort_all();
                break;
            }
        }
    }

    // SIGINT cleanup: flush watermarks for tables that completed, mark
    // everything not-yet-`Success`/`Failed` as `Interrupted`, emit partial
    // JSON, and return the sentinel error. Auto-retry + downstream phases
    // are all skipped — they implicitly need a "successful" run baseline.
    if interrupted {
        output.interrupted = true;

        {
            // §P1.6: commit every deferred watermark in a single redb
            // transaction instead of one `begin_write → commit` cycle per
            // entry. Same per-key data, 1 fsync instead of N.
            let state = shared_state.lock().await;
            let materialized: Vec<WatermarkState> = deferred_watermarks
                .iter()
                .map(|wm| WatermarkState {
                    last_value: wm.timestamp,
                    updated_at: wm.timestamp,
                })
                .collect();
            let entries: Vec<(&str, &WatermarkState)> = deferred_watermarks
                .iter()
                .zip(materialized.iter())
                .map(|(wm, state_val)| (wm.state_key.as_str(), state_val))
                .collect();
            if let Err(e) = state.batch_set_watermarks(&entries) {
                tracing::warn!(
                    error = %e,
                    count = entries.len(),
                    "failed to persist watermarks for interrupted run",
                );
            }
        }

        // Diff: plan \ {Success|Failed} → mark as Interrupted. Uses the
        // state store as source of truth for what already committed.
        let settled: std::collections::HashSet<String> = {
            let state = shared_state.lock().await;
            state
                .get_run_progress(&shared_run_id)
                .ok()
                .flatten()
                .map(|p| {
                    p.tables
                        .iter()
                        .filter(|t| {
                            matches!(
                                t.status,
                                rocky_core::state::TableStatus::Success
                                    | rocky_core::state::TableStatus::Failed
                            )
                        })
                        .map(|t| t.table_key.clone())
                        .collect()
                })
                .unwrap_or_default()
        };

        {
            let state = shared_state.lock().await;
            for (idx, task) in tables_to_process.iter().enumerate() {
                let key = format!(
                    "{}.{}.{}",
                    task.target_catalog, task.target_schema, task.table_name
                );
                if !settled.contains(&key) {
                    let mut asset_key = task.asset_key_prefix.clone();
                    asset_key.push(task.table_name.clone());
                    if let Err(e) = state.record_table_progress(
                        &shared_run_id,
                        &rocky_core::state::TableProgress {
                            index: idx,
                            table_key: key,
                            asset_key,
                            status: rocky_core::state::TableStatus::Interrupted,
                            error: None,
                            duration_ms: 0,
                            completed_at: Utc::now(),
                        },
                    ) {
                        tracing::warn!(error = %e, "failed to record table progress for run ");
                    }
                }
            }
        }

        if let Some(h) = &state_sync_handle {
            h.abort();
        }

        output.duration_ms = start.elapsed().as_millis() as u64;

        // Populate cost summary on the interrupted output so orchestrators
        // can see what the partial run cost. Skip the budget check — we
        // don't want to fire `budget_breach` on partial work.
        let adapter_type = rocky_cfg
            .adapters
            .get(&pipeline.target.adapter)
            .map(|a| a.adapter_type.clone())
            .unwrap_or_default();
        output.populate_cost_summary(&adapter_type, &rocky_cfg.cost);

        // Persist interrupted RunRecord so `rocky history` shows it as
        // PartialFailure / Failure and `rocky replay` can inspect what
        // got done before the SIGINT. `output.interrupted` was set
        // above so `derive_run_status` picks the right variant.
        {
            let state = shared_state.lock().await;
            persist_run_record(
                Some(&state),
                &output,
                &shared_run_id,
                started_at,
                &config_hash,
                &audit,
            );
            finalize_idempotency(
                &mut idempotency_ctx,
                Some(&state),
                &shared_run_id,
                &output,
            )
            .await;
        }

        if output_json {
            print_json(&output)?;
        } else {
            eprintln!(
                "[rocky] interrupted at {}/{} tables (run_id: {})",
                total_completed,
                tables_to_process.len(),
                shared_run_id
            );
        }

        // §P2.6 emit: pipeline_error + drain async webhooks on Ctrl-C
        // interrupt. Spawned `async` webhooks are flushed before we exit
        // so the operator sees their summary rather than losing them
        // silently at runtime shutdown.
        let _ = hook_registry
            .fire(&HookContext::pipeline_error(
                &run_id,
                pipeline_name,
                "interrupted by signal",
            ))
            .await;
        let _ = hook_registry.wait_async_webhooks().await;

        return Err(anyhow::Error::new(Interrupted));
    }

    // Auto-retry failed tables
    let table_retries = pipeline.execution.table_retries;
    if table_retries > 0 && !table_errors.is_empty() {
        let retryable: Vec<usize> = table_errors.iter().filter_map(|e| e.task_index).collect();

        if !retryable.is_empty() {
            info!(
                tables = retryable.len(),
                "retrying failed tables (attempt 2)"
            );
            let mut still_failed = Vec::new();
            for idx in retryable {
                let task = &tables_to_process[idx];
                match process_table(
                    shared_warehouse.as_ref(),
                    &shared_state,
                    &shared_pipeline,
                    task,
                )
                .await
                {
                    Ok(tr) => {
                        output.tables_copied += 1;
                        rocky_observe::metrics::METRICS.inc_tables_processed();
                        output.materializations.push(tr.materialization);
                        if tr.drift_checked {
                            output.drift.tables_checked += 1;
                        }
                        if let Some(src_ref) = tr.source_batch_ref {
                            source_batch_refs.push(src_ref);
                        }
                        if let Some(tgt_ref) = tr.target_batch_ref {
                            target_batch_refs.push(tgt_ref);
                        }
                        batch_asset_keys.push((tr.target_full_name.clone(), tr.asset_key));
                        if let Some(tags) = tr.deferred_tags {
                            deferred_tags.push(tags);
                        }
                        if let Some(wm) = tr.deferred_watermark {
                            deferred_watermarks.push(wm);
                        }
                        info!(table = task.table_name.as_str(), "retry succeeded");
                    }
                    Err(e) => {
                        let msg = format!("{e:#}");
                        warn!(
                            table = task.table_name.as_str(),
                            error = msg,
                            "retry also failed"
                        );
                        still_failed.push(TableError {
                            asset_key: vec![],
                            error: msg,
                            task_index: Some(idx),
                        });
                    }
                }
            }
            table_errors.retain(|e| e.task_index.is_none());
            table_errors.extend(still_failed);
        }
    }

    // Stop background state sync and reclaim resources for post-run phase
    if let Some(h) = state_sync_handle.as_ref() {
        h.abort();
    }

    // --- Batch watermark updates (no lock contention — sequential post-run) ---
    //
    // Under `fail_fast = true` semantics, any table failure should prevent
    // the surviving tables from advancing their watermarks — otherwise the
    // next run resumes from a point that implies siblings succeeded. Under
    // `fail_fast = false` (the default, partial-success semantics), commit
    // whatever completed cleanly so forward progress isn't lost.
    let skip_watermarks_due_to_failure = fail_fast && !table_errors.is_empty();
    if skip_watermarks_due_to_failure {
        tracing::warn!(
            deferred = deferred_watermarks.len(),
            failed_tables = table_errors.len(),
            "fail_fast + partial failure: skipping {} pending watermark commits so the next run starts from the same baseline",
            deferred_watermarks.len(),
        );
    } else {
        // §P1.6: single redb transaction for every deferred watermark.
        let state = shared_state.lock().await;
        let materialized: Vec<WatermarkState> = deferred_watermarks
            .iter()
            .map(|wm| WatermarkState {
                last_value: wm.timestamp,
                updated_at: wm.timestamp,
            })
            .collect();
        let entries: Vec<(&str, &WatermarkState)> = deferred_watermarks
            .iter()
            .zip(materialized.iter())
            .map(|(wm, state_val)| (wm.state_key.as_str(), state_val))
            .collect();
        if let Err(e) = state.batch_set_watermarks(&entries) {
            tracing::warn!(
                error = %e,
                count = entries.len(),
                "failed to persist watermarks for run",
            );
        }
    }

    // --- Batch table tagging (concurrent, outside the main processing loop) ---
    // Tagging is dispatched through the GovernanceAdapter trait, so it works
    // for any warehouse whose adapter registers a governance implementation.
    // The NoopGovernanceAdapter silently succeeds, which matches the previous
    // behaviour for non-Databricks targets (deferred_tags was never populated
    // into SQL calls before either).
    if !deferred_tags.is_empty() {
        let tag_start = Instant::now();

        let tag_results: Vec<_> = futures::future::join_all(deferred_tags.iter().map(|dt| {
            let gov = &governance_adapter;
            let target = TagTarget::Table {
                catalog: dt.catalog.clone(),
                schema: dt.schema.clone(),
                table: dt.table.clone(),
            };
            async move {
                gov.set_tags(&target, &dt.tags)
                    .await
                    .map_err(|e| (format!("{}.{}.{}", dt.catalog, dt.schema, dt.table), e))
            }
        }))
        .await;

        let mut tag_failures = 0usize;
        for result in &tag_results {
            if let Err((table, e)) = result {
                warn!(table, error = %e, "table tagging failed");
                tag_failures += 1;
            }
        }
        debug!(
            tables = deferred_tags.len(),
            failures = tag_failures,
            elapsed_ms = tag_start.elapsed().as_millis() as u64,
            "batch tagging complete"
        );
    }

    let state_store = Arc::try_unwrap(shared_state)
        .ok()
        .map(tokio::sync::Mutex::into_inner);

    // --- Batched checks ---
    // See the `_governance_span` note above — same reason.
    let _checks_span = info_span!("batched_checks");
    let checks_start = Instant::now();

    let row_count_enabled = pipeline.checks.row_count.enabled() && !source_batch_refs.is_empty();
    let freshness_enabled = pipeline.checks.freshness.is_some() && !freshness_batch_refs.is_empty();

    // §P2.6 emit: before_checks. Table count is the number of tables
    // participating in row-count / freshness batched checks (distinct
    // from the per-table count from materialization — a table might
    // skip one or both check types).
    let _ = hook_registry
        .fire(&HookContext::before_checks(
            &run_id,
            pipeline_name,
            source_batch_refs.len().max(freshness_batch_refs.len()),
        ))
        .await;

    if row_count_enabled || freshness_enabled {
        info!(
            row_count_tables = source_batch_refs.len(),
            freshness_tables = freshness_batch_refs.len(),
            "running batched checks concurrently"
        );
    }

    // Batch checks dispatch through the BatchCheckAdapter trait when the
    // warehouse provides one (UNION ALL batching). Otherwise, fall back to
    // per-table queries via the generic WarehouseAdapter — same observable
    // check results, just more round-trips.
    let (source_counts, target_counts, freshness_results): (
        Vec<BatchRowCountResult>,
        Vec<BatchRowCountResult>,
        Vec<BatchFreshnessResult>,
    ) = if let Some(ref bc) = shared_batch_check {
        let (src, tgt, fresh) = tokio::try_join!(
            async {
                if row_count_enabled {
                    bc.batch_row_counts(&source_batch_refs).await
                } else {
                    Ok(vec![])
                }
            },
            async {
                if row_count_enabled {
                    bc.batch_row_counts(&target_batch_refs).await
                } else {
                    Ok(vec![])
                }
            },
            async {
                if freshness_enabled {
                    bc.batch_freshness(&freshness_batch_refs, &pipeline.timestamp_column)
                        .await
                } else {
                    Ok(vec![])
                }
            },
        )
        .map_err(|e| anyhow::anyhow!("{e}"))?;
        (src, tgt, fresh)
    } else {
        // Per-table fallback via WarehouseAdapter for adapters with no
        // BatchCheckAdapter implementation.
        let dialect = shared_warehouse.dialect();
        let mut src_counts = Vec::new();
        let mut tgt_counts = Vec::new();
        let mut fresh_results: Vec<BatchFreshnessResult> = Vec::new();

        if row_count_enabled {
            for br in &source_batch_refs {
                let table_ref = dialect
                    .format_table_ref(&br.catalog, &br.schema, &br.table)
                    .map_err(|e| anyhow::anyhow!("{e}"))?;
                let sql = format!("SELECT COUNT(*) FROM {table_ref}");
                match shared_warehouse.execute_query(&sql).await {
                    Ok(result) => {
                        let count = result
                            .rows
                            .first()
                            .and_then(|r| r.first())
                            .and_then(|v| {
                                v.as_u64()
                                    .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                            })
                            .unwrap_or(0);
                        src_counts.push(BatchRowCountResult {
                            table: br.clone(),
                            count,
                        });
                    }
                    Err(e) => {
                        warn!(table = br.table.as_str(), error = %e, "per-table row count failed");
                    }
                }
            }
            for br in &target_batch_refs {
                let table_ref = dialect
                    .format_table_ref(&br.catalog, &br.schema, &br.table)
                    .map_err(|e| anyhow::anyhow!("{e}"))?;
                let sql = format!("SELECT COUNT(*) FROM {table_ref}");
                match shared_warehouse.execute_query(&sql).await {
                    Ok(result) => {
                        let count = result
                            .rows
                            .first()
                            .and_then(|r| r.first())
                            .and_then(|v| {
                                v.as_u64()
                                    .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                            })
                            .unwrap_or(0);
                        tgt_counts.push(BatchRowCountResult {
                            table: br.clone(),
                            count,
                        });
                    }
                    Err(e) => {
                        warn!(table = br.table.as_str(), error = %e, "per-table row count failed");
                    }
                }
            }
        }

        if freshness_enabled {
            for br in &freshness_batch_refs {
                let table_ref = dialect
                    .format_table_ref(&br.catalog, &br.schema, &br.table)
                    .map_err(|e| anyhow::anyhow!("{e}"))?;
                let sql = format!("SELECT MAX({}) FROM {table_ref}", pipeline.timestamp_column);
                match shared_warehouse.execute_query(&sql).await {
                    Ok(result) => {
                        let max_ts = result
                            .rows
                            .first()
                            .and_then(|r| r.first())
                            .and_then(|v| v.as_str())
                            .and_then(|s| {
                                s.parse::<chrono::DateTime<Utc>>().ok().or_else(|| {
                                    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
                                        .or_else(|_| {
                                            chrono::NaiveDateTime::parse_from_str(
                                                s,
                                                "%Y-%m-%d %H:%M:%S",
                                            )
                                        })
                                        .ok()
                                        .map(|naive| naive.and_utc())
                                })
                            });
                        fresh_results.push(BatchFreshnessResult {
                            table: br.clone(),
                            max_timestamp: max_ts,
                        });
                    }
                    Err(e) => {
                        warn!(table = br.table.as_str(), error = %e, "per-table freshness check failed");
                    }
                }
            }
        }

        (src_counts, tgt_counts, fresh_results)
    };

    // Process row count results
    if row_count_enabled {
        let source_map: HashMap<String, u64> = source_counts
            .iter()
            .map(|r| (r.table.full_name(), r.count))
            .collect();
        let target_map: HashMap<String, u64> = target_counts
            .iter()
            .map(|r| (r.table.full_name(), r.count))
            .collect();

        for (target_key, asset_key) in &batch_asset_keys {
            let src_ref = &source_batch_refs
                .iter()
                .zip(target_batch_refs.iter())
                .find(|(_, t)| t.full_name() == *target_key)
                .map(|(s, _)| s.full_name());

            if let Some(src_key) = src_ref {
                let src_count = source_map.get(src_key).copied().unwrap_or(0);
                let tgt_count = target_map.get(target_key).copied().unwrap_or(0);
                let check = checks::check_row_count(src_count, tgt_count);
                let entry =
                    pending_checks
                        .entry(target_key.clone())
                        .or_insert_with(|| PendingCheck {
                            asset_key: asset_key.clone(),
                            checks: Vec::new(),
                        });
                entry.checks.push(check);
            }
        }
    }

    // Anomaly detection
    if row_count_enabled {
        if let Some(ref store) = state_store {
            for (target_key, _) in &batch_asset_keys {
                let tgt_count = target_counts
                    .iter()
                    .find(|r| r.table.full_name() == *target_key)
                    .map(|r| r.count)
                    .unwrap_or(0);

                let _ = store.record_row_count(target_key, tgt_count, 10);

                if let Ok(history) = store.get_check_history(target_key) {
                    let anomaly = rocky_core::state::detect_anomaly(
                        target_key,
                        tgt_count,
                        &history,
                        pipeline.checks.anomaly_threshold_pct,
                    );
                    if anomaly.is_anomaly {
                        warn!(
                            table = target_key.as_str(),
                            reason = anomaly.reason.as_str(),
                            "row count anomaly detected"
                        );
                        rocky_observe::metrics::METRICS.inc_anomalies_detected();
                        // §P2.6 emit: anomaly_detected. Fires before
                        // pushing to output.anomalies so we can pass
                        // references without cloning.
                        let _ = hook_registry
                            .fire(&HookContext::anomaly_detected(
                                &run_id,
                                pipeline_name,
                                &anomaly.table,
                                &anomaly.reason,
                            ))
                            .await;
                        output.anomalies.push(AnomalyOutput {
                            table: anomaly.table,
                            current_count: anomaly.current_count,
                            baseline_avg: anomaly.baseline_avg,
                            deviation_pct: anomaly.deviation_pct,
                            reason: anomaly.reason,
                        });
                    }
                }
            }
        }
    }

    // Process freshness results
    if let Some(ref freshness_cfg) = pipeline.checks.freshness {
        let now = Utc::now();
        for fr in &freshness_results {
            let key = fr.table.full_name();
            if let Some(ts) = fr.max_timestamp {
                let lag = (now - ts).num_seconds().unsigned_abs();
                let mut check = checks::check_freshness(lag, freshness_cfg.threshold_seconds);
                check.severity = freshness_cfg.severity;

                if let Some((_, asset_key)) = batch_asset_keys.iter().find(|(k, _)| *k == key) {
                    let entry = pending_checks.entry(key).or_insert_with(|| PendingCheck {
                        asset_key: asset_key.clone(),
                        checks: Vec::new(),
                    });
                    entry.checks.push(check);
                }
            }
        }
    }

    // Assemble check results
    for (_table_key, pending) in pending_checks {
        if !pending.checks.is_empty() {
            // §P2.6 emit: check_result. Fires per-check inside the
            // assembled bag so subscribers see every pass/fail
            // (after_checks is the summary roll-up).
            for check in &pending.checks {
                let _ = hook_registry
                    .fire(&HookContext::check_result(
                        &run_id,
                        pipeline_name,
                        &check.name,
                        check.passed,
                    ))
                    .await;
            }
            output.check_results.push(TableCheckOutput {
                asset_key: pending.asset_key,
                checks: pending.checks,
            });
        }
    }

    if checks_start.elapsed().as_millis() > 0 {
        info!(
            duration_ms = checks_start.elapsed().as_millis() as u64,
            tables = output.tables_copied,
            "batched checks complete"
        );
    }

    // §P2.6 emit: after_checks — count total / passed / failed across
    // every table's check bag. One event per run (batched), not per
    // table, matching the "summary" semantics of the phase.
    {
        let mut total = 0usize;
        let mut passed = 0usize;
        let mut failed = 0usize;
        for table_check in &output.check_results {
            for check in &table_check.checks {
                total += 1;
                if check.passed {
                    passed += 1;
                } else {
                    failed += 1;
                }
            }
        }
        let _ = hook_registry
            .fire(&HookContext::after_checks(
                &run_id,
                pipeline_name,
                total,
                passed,
                failed,
            ))
            .await;
    }
    // --- Compiled model execution (--all or --models) ---
    if run_all || models_dir.is_some() {
        let mdir = models_dir.unwrap_or_else(|| std::path::Path::new("models"));
        if mdir.exists() {
            // §P2.6 follow-up — execute_models + warehouse adapter
            // construction were the remaining `?`-propagation sites
            // where a pipeline error could surface without firing
            // `pipeline_error`. Other early-return sites — Ctrl-C at
            // line 1520 and partial-failure bail! below — already
            // fire. Wrap both calls so subscribers see
            // `pipeline_error` before the error reaches the caller.
            let exec_result: Result<()> = async {
                let warehouse = adapter_registry.warehouse_adapter(&pipeline.target.adapter)?;
                execute_models(
                    mdir,
                    warehouse.as_ref(),
                    state_store.as_ref(),
                    partition_opts,
                    &run_id,
                    None, // no model filter in replication path
                    &mut output,
                    Some(&hook_registry),
                    Some(pipeline_name),
                    &schema_cache_cfg,
                )
                .await?;
                Ok(())
            }
            .await;
            if let Err(e) = exec_result {
                let _ = hook_registry
                    .fire(&HookContext::pipeline_error(
                        &run_id,
                        pipeline_name,
                        &format!("{e:#}"),
                    ))
                    .await;
                let _ = hook_registry.wait_async_webhooks().await;
                return Err(e);
            }

            // Governance: column classification + masking reconcile (§1.1 + §1.2).
            // Walks the model set after a successful DAG run and applies
            // `[classification]` sidecar tags + the resolved `[mask]` strategy
            // through the same `GovernanceAdapter` that handled catalog and
            // schema grants above. Best-effort: failures warn but never abort,
            // mirroring the `apply_grants` semantics earlier in this path.
            //
            // `env` threads the caller's `--env <name>` into
            // `resolve_mask_for_env` so `[mask.<env>]` overrides win over
            // the workspace `[mask]` defaults. `None` preserves the
            // pre-1.16 behavior of resolving defaults only.
            let governance_compile = rocky_compiler::compile::compile(
                &rocky_compiler::compile::CompilerConfig {
                    models_dir: mdir.to_path_buf(),
                    contracts_dir: None,
                    source_schemas: std::collections::HashMap::new(),
                    source_column_info: std::collections::HashMap::new(),
                    // W004 completeness check: surface classification tags
                    // that aren't mapped to a masking strategy as warnings
                    // on the governance compile path too — cheap, and
                    // keeps parity with `rocky compile`'s diagnostic set.
                    mask: rocky_cfg.mask.clone(),
                    allow_unmasked: rocky_cfg.classifications.allow_unmasked.clone(),
                },
            );
            if let Ok(gov_compile) = governance_compile {
                let tag_to_strategy = rocky_cfg.resolve_mask_for_env(env);
                for model in &gov_compile.project.models {
                    let plan = model.to_plan();
                    let table_ref = TableRef {
                        catalog: plan.target.catalog.clone(),
                        schema: plan.target.schema.clone(),
                        table: plan.target.table.clone(),
                    };

                    // --- Classification tags + masking (Wave A) ---
                    if !model.config.classification.is_empty() {
                        let mut column_tags: BTreeMap<String, BTreeMap<String, String>> =
                            BTreeMap::new();
                        for (col, tag) in &model.config.classification {
                            let mut m = BTreeMap::new();
                            m.insert("classification".to_string(), tag.clone());
                            column_tags.insert(col.clone(), m);
                        }
                        if let Err(e) = governance_adapter
                            .apply_column_tags(&table_ref, &column_tags)
                            .await
                        {
                            warn!(
                                model = %model.config.name,
                                error = %e,
                                "apply column classification tags failed"
                            );
                        }

                        let mut column_strategies: BTreeMap<String, MaskStrategy> =
                            BTreeMap::new();
                        for (col, tag) in &model.config.classification {
                            if let Some(strategy) = tag_to_strategy.get(tag) {
                                column_strategies.insert(col.clone(), *strategy);
                            }
                        }
                        if !column_strategies.is_empty() {
                            let policy = MaskingPolicy { column_strategies };
                            if let Err(e) = governance_adapter
                                .apply_masking_policy(&table_ref, &policy, "default")
                                .await
                            {
                                warn!(
                                    model = %model.config.name,
                                    error = %e,
                                    "apply column masking policy failed"
                                );
                            }
                        }
                    }

                    // --- Data retention policy (Wave C-2) ---
                    //
                    // Sidecar `retention = "<N>[dy]"` resolved to a typed
                    // RetentionPolicy at parse time. Forward to the
                    // governance adapter; best-effort — failure warns
                    // but does not abort the run, matching the
                    // classification/masking contract above.
                    if let Some(retention) = model.config.retention {
                        if let Err(e) = governance_adapter
                            .apply_retention_policy(&table_ref, &retention)
                            .await
                        {
                            warn!(
                                model = %model.config.name,
                                duration_days = retention.duration_days,
                                error = %e,
                                "apply retention policy failed"
                            );
                        }
                    }
                }
            }

            // Governance: role-graph reconcile (§1.1 + §1.2 Wave C-1).
            // Flatten the `[role.*]` config and dispatch through the same
            // `GovernanceAdapter` that handled classification + masking
            // above. Best-effort: failures warn but never abort, mirroring
            // the Wave A semantics.
            //
            // Cycle detection + unknown-parent validation already ran when
            // the config was loaded (see `RockyConfig::role_graph`), so a
            // failure here is an adapter-side one (e.g., SCIM API down
            // when group creation lands in a follow-up).
            //
            // Role graph is intentionally env-invariant: `rocky.toml` has
            // no `[role.<env>]` override shape (contrast `[mask.<env>]`).
            // Roles represent deployment-wide permission groups; masks
            // vary per env because dev/prod sensitivity differs. The
            // caller's `--env` therefore does NOT flow into
            // `reconcile_role_graph`.
            match rocky_cfg.role_graph() {
                Ok(resolved) if !resolved.is_empty() => {
                    // Borrow the `BTreeSet<String>` as `&[&str]` for the
                    // trait call — keeps `managed_catalogs` alive and
                    // lets the adapter iterate without cloning.
                    let catalog_refs: Vec<&str> =
                        managed_catalogs.iter().map(String::as_str).collect();
                    if let Err(e) = governance_adapter
                        .reconcile_role_graph(&resolved, &catalog_refs)
                        .await
                    {
                        warn!(
                            roles = resolved.len(),
                            catalogs = catalog_refs.len(),
                            error = %e,
                            "reconcile role graph failed"
                        );
                    }
                }
                Ok(_) => {
                    // No `[role.*]` block — nothing to reconcile.
                }
                Err(e) => {
                    warn!(error = %e, "role graph flatten failed; skipping role reconcile");
                }
            }
        }
    }

    output.duration_ms = start.elapsed().as_millis() as u64;
    output.permissions.catalogs_created = catalogs_created;
    output.permissions.schemas_created = schemas_created;
    output.metrics = Some(rocky_observe::metrics::METRICS.snapshot());

    // Sweep expired idempotency stamps + stale InFlight corpses before
    // the remote upload so the uploaded snapshot doesn't carry dead
    // state. Swallow errors — the sweep is purely opportunistic.
    sweep_idempotency_best_effort(
        idempotency_ctx.as_ref(),
        state_store.as_ref(),
        &rocky_cfg.state.idempotency,
    )
    .await;

    // Upload state to remote storage
    if let Err(e) = rocky_core::state_sync::upload_state(&rocky_cfg.state, state_path).await {
        warn!(error = %e, "state upload failed");
    }

    // §P2.6 emit: state_synced — final upload is done (either
    // successfully or the warn above tells the operator it failed;
    // the hook still fires so downstream observers can tick the
    // "sync attempted" lifecycle marker).
    let _ = hook_registry
        .fire(&HookContext::state_synced(&run_id, pipeline_name))
        .await;

    output.execution.tables_processed = output.tables_copied + table_errors.len();
    output.execution.tables_failed = table_errors.len();
    output.execution.adaptive_concurrency = adaptive;
    if let Some(t) = &throttle {
        output.execution.final_concurrency = Some(t.current());
        output.execution.rate_limits_detected = Some(t.rate_limits_total());
    }
    output.tables_failed = table_errors.len();
    output.errors = table_errors
        .iter()
        .map(|e| TableErrorOutput {
            asset_key: e.asset_key.clone(),
            error: e.error.clone(),
        })
        .collect();

    // Populate per-model / per-run cost attribution and run the
    // configured `[budget]` check. Populate always; propagate the
    // budget error (if any) after the output has been printed so
    // orchestrators still see the final JSON.
    let adapter_type = rocky_cfg
        .adapters
        .get(&pipeline.target.adapter)
        .map(|a| a.adapter_type.clone())
        .unwrap_or_default();
    output.populate_cost_summary(&adapter_type, &rocky_cfg.cost);
    let budget_result = output.check_and_record_budget(&rocky_cfg.budget, Some(&run_id));

    // Persist the RunRecord so `rocky history`, `rocky replay`,
    // `rocky trace`, and `rocky cost` have real data to read.
    // Record-store failures never fail the command — the user's run
    // succeeded, bookkeeping can't be allowed to flip the exit code.
    persist_run_record(
        state_store.as_ref(),
        &output,
        &run_id,
        started_at,
        &config_hash,
        &audit,
    );
    finalize_idempotency(
        &mut idempotency_ctx,
        state_store.as_ref(),
        &run_id,
        &output,
    )
    .await;

    // Fire the `on_budget_breach` hook for each recorded breach so
    // shell hooks / webhooks configured via `[hook.on_budget_breach]`
    // see the breach alongside the bus event.
    for breach in &output.budget_breaches {
        let _ = hook_registry
            .fire(&HookContext::budget_breach(
                &run_id,
                pipeline_name,
                &breach.limit_type,
                breach.limit,
                breach.actual,
            ))
            .await;
    }

    // Dagster Pipes message emission. We emit at the END of the run
    // (not per-event during) so the streaming is batch-at-the-end
    // rather than truly live — that's a simplification of the full
    // Pipes story (per-table events would require threading the
    // emitter through the async parallel execution path, which is a
    // larger refactor). Even batch-at-end gives Dagster's run viewer
    // structured MaterializationEvent / AssetCheckEvaluation entries
    // (instead of just stderr forwarding), which is the core T2
    // contract.
    if let Some(p) = &pipes {
        emit_pipes_events(p, &output);
        p.log(
            "INFO",
            &format!(
                "rocky run complete: {} copied, {} failed in {}ms",
                output.tables_copied, output.tables_failed, output.duration_ms,
            ),
        );
        p.closed();
    }

    if output_json {
        print_json(&output)?;
    } else {
        if let Some(ref resumed_from) = output.resumed_from {
            println!(
                "Resumed from {resumed_from} (skipped {} tables)",
                output.tables_skipped
            );
        }
        println!(
            "Copied {} tables in {:.1}s (run_id: {run_id})",
            output.tables_copied,
            output.duration_ms as f64 / 1000.0
        );
        if output.drift.tables_drifted > 0 {
            println!(
                "Drift: {}/{} tables drifted",
                output.drift.tables_drifted, output.drift.tables_checked
            );
        }
    }

    if !table_errors.is_empty() {
        let count = table_errors.len();
        for e in &table_errors {
            warn!(error = e.error.as_str(), "table error");
        }
        // §P2.6 emit: pipeline_error + drain on partial failure.
        let msg = format!("{count} table(s) failed during parallel execution");
        let _ = hook_registry
            .fire(&HookContext::pipeline_error(&run_id, pipeline_name, &msg))
            .await;
        let _ = hook_registry.wait_async_webhooks().await;
        anyhow::bail!(
            "{count} table(s) failed during parallel execution (run_id: {run_id}, use --resume {run_id} to retry)"
        );
    }

    // §P2.6 emit: pipeline_complete on happy-path exit. Drain async
    // webhooks before returning.
    let _ = hook_registry
        .fire(&HookContext::pipeline_complete(
            &run_id,
            pipeline_name,
            output.duration_ms,
            output.tables_copied,
        ))
        .await;
    let _ = hook_registry.wait_async_webhooks().await;

    // Propagate budget breach error (if any) after the hooks have
    // drained, so subscribers see the breach event before the CLI
    // exits non-zero.
    budget_result?;

        Ok(())
    }
    .await;

    // FR-004 F1 error-path finalize. If the body returned `Err`, the
    // claim is almost certainly still `InFlight` (the happy/interrupted/
    // partial-failure sites all call `finalize_idempotency` which takes
    // the ctx out). Release it so a retry with the same key proceeds
    // immediately instead of waiting up to 24h for the TTL sweep. The
    // one-shot semantics of `finalize_idempotency` guarantee we never
    // over-write a Succeeded stamp with a Failed one here (which would
    // delete the entry under `dedup_on = "success"` and silently strip
    // the idempotency record of a genuinely-successful run).
    if run_result.is_err() {
        finalize_idempotency_on_error(&mut idempotency_ctx, state_path, &run_id).await;
    }

    run_result
}

/// Emit Dagster Pipes messages for every materialization, check, and
/// drift action in a completed `RunOutput`.
///
/// This is the "batch at end of run" approach to Pipes streaming. The
/// fully-streaming alternative (per-table events as they complete)
/// would require threading the [`crate::pipes::PipesEmitter`] through
/// the async parallel execution path in `process_table` and
/// `run_one_partition`. That's a larger refactor; the batch approach
/// gives Dagster's run viewer structured `MaterializationEvent` and
/// `AssetCheckEvaluation` entries today and can be upgraded to
/// per-event streaming in a follow-up without changing the wire
/// protocol or the dagster-side consumer.
///
/// The `asset_key` for each event is slash-joined per the Dagster
/// Pipes wire convention (e.g. `"fivetran/acme/orders"`), matching
/// the `[Vec<String>]` paths the engine emits in `MaterializationOutput`.
///
/// `pub(super)` so the non-replication pipeline paths in `run_local.rs`
/// can call it with the same wire format.
pub(super) fn emit_pipes_events(pipes: &crate::pipes::PipesEmitter, output: &RunOutput) {
    use serde_json::json;

    // Materializations.
    for mat in &output.materializations {
        let asset_key = mat.asset_key.join("/");
        let mut metadata = serde_json::Map::new();
        metadata.insert("strategy".into(), json!(mat.metadata.strategy));
        metadata.insert("duration_ms".into(), json!(mat.duration_ms));
        if let Some(rows) = mat.rows_copied {
            metadata.insert("rows_copied".into(), json!(rows));
        }
        if let Some(ref tn) = mat.metadata.target_table_full_name {
            metadata.insert("target_table_full_name".into(), json!(tn));
        }
        if let Some(ref hash) = mat.metadata.sql_hash {
            metadata.insert("sql_hash".into(), json!(hash));
        }
        if let Some(ref watermark) = mat.metadata.watermark {
            metadata.insert("watermark".into(), json!(watermark.to_rfc3339()));
        }
        if let Some(ref part) = mat.partition {
            metadata.insert("partition_key".into(), json!(part.key));
        }
        pipes.report_asset_materialization(&asset_key, &json!(metadata));
    }

    // Asset checks. Each TableCheckOutput has multiple per-check rows;
    // we emit one Pipes message per check entry. CheckResult is an
    // untagged enum (rocky_core::checks::CheckResult); for now we
    // serialize whatever the engine produced and let Dagster type-infer.
    for table_check in &output.check_results {
        let asset_key = table_check.asset_key.join("/");
        for check in &table_check.checks {
            let check_value = serde_json::to_value(check).unwrap_or(json!({}));
            // Extract `name` and `passed` from the serialized check
            // BEFORE moving check_value into the report call. CheckResult
            // variants use lowercase tagged keys; the common shape exposes
            // a "name" field at the top level.
            let check_name = check_value
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string();
            let passed = check_value
                .get("passed")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(true);
            pipes.report_asset_check(
                &asset_key,
                &check_name,
                passed,
                crate::pipes::PipesCheckSeverity::Error,
                &check_value,
            );
        }
    }

    // Drift actions: emit as asset checks (severity=WARN, passed=true)
    // so they surface as structured events in the Dagster run viewer.
    // The check name is "drift" (grouped per table); action details go
    // into metadata so the UI doesn't fragment by action type.
    // Also emit a log line for human-readable visibility.
    for action in &output.drift.actions_taken {
        pipes.report_asset_check(
            &action.table,
            "drift",
            true,
            crate::pipes::PipesCheckSeverity::Warn,
            &json!({
                "table": action.table,
                "action": action.action,
                "reason": action.reason,
            }),
        );
        pipes.log(
            "WARN",
            &format!(
                "rocky drift on {}: {} ({})",
                action.table, action.action, action.reason,
            ),
        );
    }
}

/// Compile and execute every model in `models_dir` against the given
/// warehouse adapter. Shared between `run()` (Databricks path) and
/// `run_local()` (DuckDB / non-Databricks path) so the dispatch in `run()`
/// doesn't have to duplicate the time_interval branching logic.
///
/// Dispatches per-model on the materialization strategy: `time_interval`
/// goes through `execute_time_interval_model()` (per-partition path);
/// everything else (full_refresh / incremental / merge / materialized_view)
/// uses the single-statement path via `generate_transformation_sql`.
#[tracing::instrument(skip_all, name = "execute_models")]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn execute_models(
    models_dir: &Path,
    warehouse: &dyn rocky_core::traits::WarehouseAdapter,
    state_store: Option<&StateStore>,
    partition_opts: &PartitionRunOptions,
    run_id: &str,
    model_name_filter: Option<&str>,
    output: &mut RunOutput,
    // §P2.6 hook plumbing — optional so `rocky run --model <X>`
    // (which skips the full pipeline context) can pass `None` without
    // the caller having to synthesise a fake pipeline name.
    hook_registry: Option<&HookRegistry>,
    pipeline_name: Option<&str>,
    // Arc 7 wave 2 wave-2: honour `[cache.schemas]` for source-schema
    // loading during compile. The write tap that keeps the cache fresh
    // lands in PR 2; today the loader reads whatever's there.
    schema_cache_config: &rocky_core::config::SchemaCacheConfig,
) -> Result<()> {
    info!(models_dir = %models_dir.display(), "compiling and executing transformation models");

    // Wave-2 of Arc 7 wave 2: load the persisted schema cache directly
    // from the live `StateStore` — no round-trip through
    // `crate::source_schemas::load_cached_source_schemas`, because that
    // helper opens a read-only handle and we already hold a write-capable
    // one here. Degrades silently when the cache is cold or disabled.
    let source_schemas = if schema_cache_config.enabled {
        match state_store {
            Some(store) => rocky_compiler::schema_cache::load_source_schemas_from_cache(
                store,
                chrono::Utc::now(),
                schema_cache_config.ttl(),
            )
            .unwrap_or_default(),
            None => std::collections::HashMap::new(),
        }
    } else {
        std::collections::HashMap::new()
    };

    let compile_config = rocky_compiler::compile::CompilerConfig {
        models_dir: models_dir.to_path_buf(),
        contracts_dir: None,
        source_schemas,
        source_column_info: std::collections::HashMap::new(),
        // W004 wiring happens on the governance compile path later in
        // this function (it already holds the loaded `RockyConfig`).
        // This pre-execution compile stays scoped to typecheck +
        // contract diagnostics to avoid broadening its signature.
        ..Default::default()
    };

    let compile_result = match rocky_compiler::compile::compile(&compile_config) {
        Ok(r) => r,
        Err(e) => {
            warn!(error = %e, "model compilation failed — skipping model execution");
            return Ok(());
        }
    };

    if compile_result.has_errors {
        for d in &compile_result.diagnostics {
            if d.is_error() {
                warn!(
                    model = d.model.as_str(),
                    code = &*d.code,
                    message = &*d.message,
                    "compile error"
                );
            }
        }
        warn!("model compilation had errors — skipping model execution");
        return Ok(());
    }

    // §P2.6 emit: compile_complete — fires once after a successful
    // compile pass, with the model count from the compiled project.
    // Gated on both hook_registry + pipeline_name being supplied so
    // `rocky run --model <X>` stays silent.
    if let (Some(reg), Some(pipe)) = (hook_registry, pipeline_name) {
        let _ = reg
            .fire(&HookContext::compile_complete(
                run_id,
                pipe,
                compile_result.project.models.len(),
            ))
            .await;
    }

    let dialect = warehouse.dialect();
    let mut models_executed = 0usize;

    // Bundle borrowed compile-time facts (typed schemas + per-model timings)
    // into a single ExecutionContext that's threaded through every per-model
    // execution path. Future compile-time fields (semantic info, optimize
    // results, contract diagnostics) ride on this struct without further
    // signature churn.
    let exec_ctx = ExecutionContext {
        typed_models: &compile_result.type_check.typed_models,
        model_timings: &compile_result.model_timings,
    };

    for layer in &compile_result.project.layers {
        for model_name in layer {
            // When a model name filter is active, skip models that don't match.
            if let Some(target) = model_name_filter {
                if model_name != target {
                    continue;
                }
            }

            let Some(model) = compile_result.project.model(model_name) else {
                continue;
            };

            // §P2.6 per-model emit: before_model_run. Duration is
            // reserved for after_model_run — here we just announce
            // the model is about to execute.
            if let (Some(reg), Some(pipe)) = (hook_registry, pipeline_name) {
                let _ = reg
                    .fire(&HookContext::before_model_run(run_id, pipe, model_name))
                    .await;
            }
            let model_start = Instant::now();

            // Dispatch on the materialization strategy. time_interval models
            // take the per-partition path so the runtime can iterate
            // partitions, populate the window per partition, and record
            // state-store rows. Other strategies use the single-statement
            // path via generate_transformation_sql.
            if matches!(
                model.config.strategy,
                rocky_core::models::StrategyConfig::TimeInterval { .. }
            ) {
                let time_interval_res = execute_time_interval_model(
                    model,
                    warehouse,
                    dialect,
                    state_store,
                    partition_opts,
                    run_id,
                    output,
                    &exec_ctx,
                )
                .await
                .with_context(|| format!("model '{model_name}' failed"));
                if let Err(ref e) = time_interval_res {
                    if let (Some(reg), Some(pipe)) = (hook_registry, pipeline_name) {
                        let _ = reg
                            .fire(&HookContext::model_error(
                                run_id,
                                pipe,
                                model_name,
                                &format!("{e:#}"),
                            ))
                            .await;
                    }
                }
                time_interval_res?;
                if let (Some(reg), Some(pipe)) = (hook_registry, pipeline_name) {
                    let _ = reg
                        .fire(&HookContext::after_model_run(
                            run_id,
                            pipe,
                            model_name,
                            model_start.elapsed().as_millis() as u64,
                        ))
                        .await;
                }
                models_executed += 1;
                continue;
            }

            let plan = model.to_plan();
            let target_ref = dialect
                .format_table_ref(
                    &plan.target.catalog,
                    &plan.target.schema,
                    &plan.target.table,
                )
                .map_err(|e| anyhow::anyhow!("{e}"))?;

            let exec_stmts = rocky_core::sql_gen::generate_transformation_sql(&plan, dialect)?;

            info!(
                model = model_name.as_str(),
                target = target_ref.as_str(),
                statements = exec_stmts.len(),
                "executing model"
            );
            // Capture per-statement stats so derived models report the
            // same `bytes_scanned` / `bytes_written` shape that replication
            // tables already have (Arc 2 wave 3 residual: plain
            // transformation models were calling `execute_statement` and
            // dropping the `ExecutionStats` reported by stats-aware
            // adapters like BigQuery).
            let model_started_at = Utc::now();
            let mut exec_error: Option<anyhow::Error> = None;
            let mut bytes_scanned_acc: Option<u64> = None;
            let mut bytes_written_acc: Option<u64> = None;
            for exec_sql in &exec_stmts {
                match warehouse.execute_statement_with_stats(exec_sql).await {
                    Ok(stats) => {
                        bytes_scanned_acc =
                            accumulate_bytes(bytes_scanned_acc, stats.bytes_scanned);
                        bytes_written_acc =
                            accumulate_bytes(bytes_written_acc, stats.bytes_written);
                    }
                    Err(e) => {
                        exec_error = Some(anyhow::anyhow!("model '{model_name}' failed: {e}"));
                        break;
                    }
                }
            }
            if let Some(e) = exec_error {
                if let (Some(reg), Some(pipe)) = (hook_registry, pipeline_name) {
                    let _ = reg
                        .fire(&HookContext::model_error(
                            run_id,
                            pipe,
                            model_name,
                            &format!("{e:#}"),
                        ))
                        .await;
                }
                return Err(e);
            }
            // Push a MaterializationOutput so `rocky cost` and downstream
            // consumers see derived models in the run output instead of
            // having to infer them from check / drift records. Mirrors
            // the time_interval and replication paths.
            let model_duration_ms = model_start.elapsed().as_millis() as u64;
            let target_table_full_name = format!(
                "{}.{}.{}",
                plan.target.catalog, plan.target.schema, plan.target.table
            );
            let asset_key = vec![
                plan.target.catalog.clone(),
                plan.target.schema.clone(),
                plan.target.table.clone(),
            ];
            output.materializations.push(MaterializationOutput {
                asset_key,
                rows_copied: None,
                duration_ms: model_duration_ms,
                started_at: model_started_at,
                metadata: MaterializationMetadata {
                    strategy: transformation_strategy_name(&plan.strategy).to_string(),
                    watermark: None,
                    target_table_full_name: Some(target_table_full_name),
                    sql_hash: Some(crate::output::sql_fingerprint(&exec_stmts)),
                    column_count: exec_ctx.column_count_for(model_name),
                    compile_time_ms: exec_ctx.compile_time_ms_for(model_name),
                },
                partition: None,
                cost_usd: None,
                bytes_scanned: bytes_scanned_acc,
                bytes_written: bytes_written_acc,
            });
            if let (Some(reg), Some(pipe)) = (hook_registry, pipeline_name) {
                let _ = reg
                    .fire(&HookContext::after_model_run(
                        run_id,
                        pipe,
                        model_name,
                        model_duration_ms,
                    ))
                    .await;
            }
            models_executed += 1;
        }
    }

    info!(models = models_executed, "model execution complete");
    Ok(())
}

/// Map a `MaterializationStrategy` (post-`to_plan()`) onto the string used
/// in `MaterializationMetadata.strategy`. Mirrors the names used elsewhere
/// in `run.rs` (replication path lines ~4024-4076).
fn transformation_strategy_name(strategy: &MaterializationStrategy) -> &'static str {
    match strategy {
        MaterializationStrategy::FullRefresh => "full_refresh",
        MaterializationStrategy::Incremental { .. } => "incremental",
        MaterializationStrategy::Merge { .. } => "merge",
        MaterializationStrategy::MaterializedView => "materialized_view",
        MaterializationStrategy::DynamicTable { .. } => "dynamic_table",
        MaterializationStrategy::TimeInterval { .. } => "time_interval",
        MaterializationStrategy::Ephemeral => "ephemeral",
        MaterializationStrategy::DeleteInsert { .. } => "delete_insert",
        MaterializationStrategy::Microbatch { .. } => "microbatch",
    }
}

/// Execute a `time_interval` model: plan partitions, execute each, record
/// state-store rows, and append per-partition entries to the run output.
///
/// This is the per-partition path the runtime takes for any model with
/// `StrategyConfig::TimeInterval`. The flow:
///
/// 1. Resolve the user's CLI selection (`--partition` / `--from` /
///    `--to` / `--latest` / `--missing`) into a `PartitionSelection`,
///    defaulting to `Latest` if no flag was passed.
/// 2. Call `plan_partitions()` to expand the selection (with lookback
///    + batching) into a `Vec<PartitionPlan>`.
/// 3. For each plan: populate `MaterializationStrategy::TimeInterval
///    { window: Some(...) }`, generate SQL via
///    `generate_transformation_sql()` (which calls
///    `dialect.insert_overwrite_partition()` to get the warehouse-
///    specific `Vec<String>`), execute each statement in order, and
///    record the partition state in the `PARTITIONS` table.
/// 4. Build a `PartitionInfo` per materialization and a single
///    `PartitionSummary` per model and push them to the run output.
///
/// Errors propagate to the caller; the state-store row records the
/// `Failed` status before bubbling so a subsequent `--missing` run can
/// pick up where this one left off.
#[allow(clippy::too_many_arguments)]
#[tracing::instrument(skip_all, fields(model = %model.config.name))]
async fn execute_time_interval_model(
    model: &rocky_core::models::Model,
    warehouse: &dyn rocky_core::traits::WarehouseAdapter,
    dialect: &dyn rocky_core::traits::SqlDialect,
    state_store: Option<&StateStore>,
    partition_opts: &PartitionRunOptions,
    run_id: &str,
    output: &mut RunOutput,
    exec_ctx: &ExecutionContext<'_>,
) -> Result<()> {
    use rocky_core::plan_partition::{PartitionSelection, plan_partitions};

    let model_name = model.config.name.as_str();

    // Sanity check the strategy. We accept the model as-is — granularity
    // and other config flow through `model.to_plan()` below.
    if !matches!(
        model.config.strategy,
        rocky_core::models::StrategyConfig::TimeInterval { .. }
    ) {
        anyhow::bail!(
            "execute_time_interval_model called for non-time_interval model '{model_name}'"
        );
    }

    // Default selection: --latest. Matches the plan: "Default behavior when
    // running a time_interval model with no flags: --latest, with lookback
    // from the TOML applied."
    let selection = partition_opts
        .to_selection()
        .unwrap_or(PartitionSelection::Latest);

    // plan_partitions needs a state-store reference for --missing discovery.
    // For non-Missing selections it doesn't read state, but we still need
    // a valid reference. If state_store is None (caller skipped state init)
    // we open a fresh in-memory-ish store at a temp path. In practice
    // state_store is always Some when reaching this code.
    let temp_state_dir;
    let local_state;
    let state_ref: &StateStore = if let Some(s) = state_store {
        s
    } else {
        temp_state_dir = tempfile::TempDir::new()
            .context("failed to allocate temp state store for partition planning")?;
        local_state = StateStore::open(&temp_state_dir.path().join("partitions.redb"))
            .context("failed to open temp state store")?;
        &local_state
    };

    let plans = plan_partitions(model, &selection, partition_opts.lookback, state_ref)
        .with_context(|| format!("failed to plan partitions for model '{model_name}'"))?;

    info!(
        model = model_name,
        partitions = plans.len(),
        "planning partitions for time_interval model"
    );

    if plans.is_empty() {
        // No partitions to run (e.g., --missing with everything already
        // computed). Still emit an empty PartitionSummary so dagster can
        // tell the model was considered.
        output.partition_summaries.push(PartitionSummary {
            model: model_name.into(),
            partitions_planned: 0,
            partitions_succeeded: 0,
            partitions_failed: 0,
            partitions_skipped: 0,
        });
        return Ok(());
    }

    let target_table = format!(
        "{}.{}.{}",
        model.config.target.catalog, model.config.target.schema, model.config.target.table
    );
    let asset_key = vec![
        model.config.target.catalog.clone(),
        model.config.target.schema.clone(),
        model.config.target.table.clone(),
    ];

    // Bootstrap-on-first-run: the time_interval DELETE+INSERT cycle requires
    // the target table to exist. If it doesn't (first run, or after manual
    // cleanup), render the model SQL with an empty `[start, start)` window
    // and wrap in CREATE OR REPLACE TABLE AS to materialize an empty table
    // with the right output schema. Subsequent partition runs use the
    // normal DELETE+INSERT path.
    let target_ref_struct = rocky_core::ir::TableRef {
        catalog: model.config.target.catalog.clone(),
        schema: model.config.target.schema.clone(),
        table: model.config.target.table.clone(),
    };
    let target_exists = warehouse.describe_table(&target_ref_struct).await.is_ok();
    if !target_exists {
        let bootstrap_plan = model.to_plan();
        let bootstrap_sql = sql_gen::generate_time_interval_bootstrap_sql(&bootstrap_plan, dialect)
            .with_context(|| format!("failed to render bootstrap SQL for model '{model_name}'"))?;
        info!(
            model = model_name,
            target = target_table.as_str(),
            "target table does not exist — bootstrapping empty table from model schema"
        );
        warehouse
            .execute_statement(&bootstrap_sql)
            .await
            .map_err(|e| {
                anyhow::anyhow!(
                    "bootstrap of '{target_table}' failed for model '{model_name}': {e}"
                )
            })?;
    }

    let mut summary = PartitionSummary {
        model: model_name.into(),
        partitions_planned: plans.len(),
        partitions_succeeded: 0,
        partitions_failed: 0,
        partitions_skipped: 0,
    };

    // --parallel N drives concurrency. The flag is parsed in main.rs and
    // arrives here in `partition_opts.parallel`. The bootstrap above is
    // already complete (sequential), so it's safe to fan out per-partition
    // execution: each partition is independent SQL, the state-store writes
    // serialize through redb's single writer lock, and warehouse-query
    // parallelism is what the user asked for.
    //
    // We use `futures::stream::buffer_unordered` rather than
    // tokio::task::JoinSet because the per-partition futures don't need
    // to be Send / 'static — they're polled in the same task as
    // `execute_time_interval_model`, just up to N at a time. This avoids
    // having to wrap warehouse and state_ref in Arc.
    let parallel_limit = (partition_opts.parallel as usize).max(1);
    info!(
        model = model_name,
        partitions = plans.len(),
        parallel = parallel_limit,
        "executing partitions"
    );

    use futures::stream::StreamExt;
    let mut results: Vec<PartitionExecutionResult> = futures::stream::iter(plans.into_iter())
        .map(|partition_plan| {
            run_one_partition(
                partition_plan,
                model,
                warehouse,
                dialect,
                state_ref,
                run_id,
                model_name,
                &asset_key,
                exec_ctx,
            )
        })
        .buffer_unordered(parallel_limit)
        .collect()
        .await;

    // Re-sort chronologically so the JSON output is deterministic regardless
    // of which partition's future completed first under the parallel scheduler.
    results.sort_by(|a, b| a.partition_key.cmp(&b.partition_key));

    // Walk results: push successes to output, surface the first failure.
    // We collect ALL results before checking errors so other in-flight
    // partitions complete cleanly (rather than being cancelled mid-flight).
    let mut first_error: Option<(anyhow::Error, String)> = None;
    for r in results {
        match r.outcome {
            Ok(materialization) => {
                output.materializations.push(materialization);
                summary.partitions_succeeded += 1;
            }
            Err(err) => {
                summary.partitions_failed += 1;
                if first_error.is_none() {
                    first_error = Some((err, r.partition_key));
                }
            }
        }
    }

    info!(
        model = model_name,
        target = target_table.as_str(),
        succeeded = summary.partitions_succeeded,
        failed = summary.partitions_failed,
        "time_interval model execution complete"
    );

    output.partition_summaries.push(summary);

    if let Some((err, key)) = first_error {
        return Err(err.context(format!("partition '{key}' of model '{model_name}' failed")));
    }
    Ok(())
}

/// Result of running one partition. Returned from `run_one_partition` so the
/// outer `execute_time_interval_model` can fan out via `buffer_unordered` and
/// then collect results back into the run output sequentially.
struct PartitionExecutionResult {
    /// Canonical partition key, used for sorting and error messages.
    partition_key: String,
    /// Either the materialization to push into `RunOutput.materializations`,
    /// or an error to surface after all partitions complete.
    outcome: Result<MaterializationOutput, anyhow::Error>,
}

/// Execute exactly one partition: mark InProgress in the state store, generate
/// SQL via `dialect.insert_overwrite_partition`, run the statements, and mark
/// Computed (or Failed on error). Pure with respect to outer state — touches
/// only the state store and the warehouse, never `RunOutput` directly.
///
/// Pulled out of `execute_time_interval_model` so it can be driven by
/// `futures::stream::buffer_unordered` for `--parallel N` execution. The
/// per-partition futures are polled concurrently in the same task as the
/// caller; redb's single-writer lock serializes the state-store writes.
#[allow(clippy::too_many_arguments)]
#[tracing::instrument(skip_all, fields(model = %model_name, partition = %partition_plan.partition_key))]
async fn run_one_partition(
    partition_plan: rocky_core::plan_partition::PartitionPlan,
    model: &rocky_core::models::Model,
    warehouse: &dyn rocky_core::traits::WarehouseAdapter,
    dialect: &dyn rocky_core::traits::SqlDialect,
    state_ref: &StateStore,
    run_id: &str,
    model_name: &str,
    asset_key: &[String],
    exec_ctx: &ExecutionContext<'_>,
) -> PartitionExecutionResult {
    use rocky_core::incremental::{PartitionRecord, PartitionStatus};
    let key = partition_plan.partition_key.clone();
    let partition_start = std::time::Instant::now();
    let partition_started_at = chrono::Utc::now();

    // Mark InProgress before executing so a crashed runner leaves a
    // diagnostic breadcrumb in the state store.
    let mut record = PartitionRecord {
        model_name: model_name.into(),
        partition_key: key.clone(),
        status: PartitionStatus::InProgress,
        computed_at: chrono::Utc::now(),
        row_count: 0,
        duration_ms: 0,
        run_id: run_id.into(),
        checksum: None,
    };
    if let Err(e) = state_ref.record_partition(&record) {
        return PartitionExecutionResult {
            partition_key: key.clone(),
            outcome: Err(anyhow::anyhow!(
                "failed to record InProgress for {model_name}|{key}: {e}"
            )),
        };
    }

    // Build the per-partition plan: clone the base TransformationPlan and
    // inject the PartitionWindow into the strategy.
    let mut tplan = model.to_plan();
    if let MaterializationStrategy::TimeInterval { window, .. } = &mut tplan.strategy {
        *window = Some(partition_plan.window.clone());
    }

    // Generate SQL — this is where dialect.insert_overwrite_partition() gets
    // called and the @start_date / @end_date placeholders are substituted
    // (Phase 2D).
    let stmts = match sql_gen::generate_transformation_sql(&tplan, dialect) {
        Ok(s) => s,
        Err(e) => {
            return PartitionExecutionResult {
                partition_key: key.clone(),
                outcome: Err(anyhow::anyhow!(
                    "SQL gen failed for {model_name}|{key}: {e}"
                )),
            };
        }
    };

    info!(
        model = model_name,
        partition = key.as_str(),
        statements = stmts.len(),
        "executing partition"
    );

    // Execute each statement in order. On any failure, attempt ROLLBACK so
    // transactional dialects (Snowflake / DuckDB) don't leave partial state
    // visible.
    //
    // Accumulate warehouse-reported bytes across all statements — the BQ
    // `insert_overwrite_partition` contract emits 4 statements
    // (BEGIN / DELETE / INSERT / COMMIT), each of which potentially reports
    // its own `totalBytesBilled`. Summing gives the partition-total cost
    // input. Non-BQ adapters return `None` from the default trait method,
    // so `bytes_acc` stays `None` and `compute_observed_cost_usd` falls
    // through to the duration-based branch.
    let mut exec_err: Option<anyhow::Error> = None;
    let mut bytes_scanned_acc: Option<u64> = None;
    let mut bytes_written_acc: Option<u64> = None;
    for stmt in &stmts {
        match warehouse.execute_statement_with_stats(stmt).await {
            Ok(stats) => {
                bytes_scanned_acc = accumulate_bytes(bytes_scanned_acc, stats.bytes_scanned);
                bytes_written_acc = accumulate_bytes(bytes_written_acc, stats.bytes_written);
            }
            Err(e) => {
                exec_err = Some(anyhow::anyhow!("{e}"));
                break;
            }
        }
    }

    if let Some(err) = exec_err {
        // Best-effort rollback. Ignored for dialects that don't use
        // transactions (Databricks REPLACE WHERE) — ROLLBACK errors there
        // are expected.
        let _ = warehouse.execute_statement("ROLLBACK").await;

        record.status = PartitionStatus::Failed;
        record.duration_ms = partition_start.elapsed().as_millis() as u64;
        let _ = state_ref.record_partition(&record);

        return PartitionExecutionResult {
            partition_key: key,
            outcome: Err(err),
        };
    }

    // Success: mark Computed.
    record.status = PartitionStatus::Computed;
    record.duration_ms = partition_start.elapsed().as_millis() as u64;
    if let Err(e) = state_ref.record_partition(&record) {
        return PartitionExecutionResult {
            partition_key: key.clone(),
            outcome: Err(anyhow::anyhow!(
                "failed to record Computed for {model_name}|{key}: {e}"
            )),
        };
    }

    let target_table_full_name = format!(
        "{}.{}.{}",
        model.config.target.catalog, model.config.target.schema, model.config.target.table,
    );
    let sql_hash = Some(crate::output::sql_fingerprint(&stmts));
    let column_count = exec_ctx.column_count_for(model_name);
    let compile_time_ms = exec_ctx.compile_time_ms_for(model_name);
    PartitionExecutionResult {
        partition_key: key.clone(),
        outcome: Ok(MaterializationOutput {
            asset_key: asset_key.to_vec(),
            rows_copied: None,
            duration_ms: record.duration_ms,
            started_at: partition_started_at,
            metadata: MaterializationMetadata {
                strategy: "time_interval".to_string(),
                watermark: None,
                target_table_full_name: Some(target_table_full_name),
                sql_hash,
                column_count,
                compile_time_ms,
            },
            partition: Some(PartitionInfo {
                key,
                start: partition_plan.window.start,
                end: partition_plan.window.end,
                batched_with: partition_plan.batch_with.clone(),
            }),
            cost_usd: None,
            bytes_scanned: bytes_scanned_acc,
            bytes_written: bytes_written_acc,
        }),
    }
}

/// Combine an existing byte accumulator with a newly reported value.
///
/// `None + None` stays `None` (adapter reported nothing). `Some(n) +
/// None` keeps the partial figure because at least one statement in
/// the transaction did report bytes. `Some(a) + Some(b)` uses
/// `saturating_add` so a 64-bit overflow can't panic (would require a
/// single partition to scan ~18 exabytes — defensive, not expected).
fn accumulate_bytes(acc: Option<u64>, next: Option<u64>) -> Option<u64> {
    match (acc, next) {
        (None, None) => None,
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (Some(a), Some(b)) => Some(a.saturating_add(b)),
    }
}

/// Processes a single table: drift detection + replication.
///
/// Uses `WarehouseAdapter` for all SQL execution and schema introspection,
/// making this function adapter-agnostic (Databricks, Snowflake, BigQuery,
/// DuckDB). Tagging and watermark updates are returned as deferred
/// operations and applied in a post-run batch phase for better concurrency.
#[tracing::instrument(skip_all, fields(table = %task.table_name))]
async fn process_table(
    warehouse: &dyn WarehouseAdapter,
    _state: &Mutex<StateStore>,
    pipeline: &ReplicationPipelineConfig,
    task: &TableTask,
) -> Result<TableResult> {
    let table_start = Instant::now();
    let table_started_at = Utc::now();
    let dialect = warehouse.dialect();

    let source_table = TableRef {
        catalog: task.source_catalog.clone(),
        schema: task.source_schema.clone(),
        table: task.table_name.clone(),
    };
    let target_table = TableRef {
        catalog: task.target_catalog.clone(),
        schema: task.target_schema.clone(),
        table: task.table_name.clone(),
    };

    let mut asset_key = task.asset_key_prefix.clone();
    asset_key.push(task.table_name.clone());

    // Drift detection — use pre-fetched columns when available (batch
    // information_schema query), falling back to individual DESCRIBE TABLE
    // via WarehouseAdapter.
    let (source_cols, target_cols) =
        if task.prefetched_source_cols.is_some() || task.prefetched_target_cols.is_some() {
            (
                task.prefetched_source_cols.clone().unwrap_or_default(),
                task.prefetched_target_cols.clone().unwrap_or_default(),
            )
        } else {
            let (src, tgt) = tokio::join!(
                warehouse.describe_table(&source_table),
                warehouse.describe_table(&target_table),
            );
            (src.unwrap_or_default(), tgt.unwrap_or_default())
        };
    let target_exists = !target_cols.is_empty();

    let mut use_full_refresh = !target_exists;
    let mut drift_action: Option<DriftActionOutput> = None;

    if target_exists {
        let drift_result = drift::detect_drift(&target_table, &source_cols, &target_cols);
        if drift_result.action == DriftAction::DropAndRecreate {
            info!(
                table = target_table.full_name(),
                drifted = drift_result.drifted_columns.len(),
                "drift detected, dropping target"
            );
            let drop_sql = drift::generate_drop_table_sql(&target_table, dialect)?;
            warehouse
                .execute_statement(&drop_sql)
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            use_full_refresh = true;

            let reason = drift_result
                .drifted_columns
                .iter()
                .map(|c| {
                    format!(
                        "column '{}' changed {} → {}",
                        c.name, c.source_type, c.target_type
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            drift_action = Some(DriftActionOutput {
                table: target_table.full_name(),
                action: "drop_and_recreate".into(),
                reason,
            });
        }
    }

    // Build replication plan
    let replication = ReplicationPlan {
        source: SourceRef {
            catalog: task.source_catalog.clone(),
            schema: task.source_schema.clone(),
            table: task.table_name.clone(),
        },
        target: TargetRef {
            catalog: task.target_catalog.clone(),
            schema: task.target_schema.clone(),
            table: task.table_name.clone(),
        },
        strategy: if pipeline.strategy == "incremental" {
            MaterializationStrategy::Incremental {
                timestamp_column: pipeline.timestamp_column.clone(),
                watermark: None,
            }
        } else {
            MaterializationStrategy::FullRefresh
        },
        columns: ColumnSelection::All,
        metadata_columns: task.metadata_columns.clone(),
        governance: GovernanceConfig {
            permissions_file: None,
            auto_create_catalogs: pipeline.target.governance.auto_create_catalogs,
            auto_create_schemas: pipeline.target.governance.auto_create_schemas,
        },
    };

    let strategy_name;
    let sql = if use_full_refresh {
        strategy_name = "full_refresh";
        sql_gen::generate_create_table_as_sql(&replication, dialect)?
    } else {
        match &replication.strategy {
            MaterializationStrategy::FullRefresh => {
                strategy_name = "full_refresh";
                sql_gen::generate_create_table_as_sql(&replication, dialect)?
            }
            MaterializationStrategy::Incremental { .. } => {
                strategy_name = "incremental";
                sql_gen::generate_insert_sql(&replication, dialect)?
            }
            MaterializationStrategy::Merge { .. } => {
                strategy_name = "merge";
                sql_gen::generate_merge_sql(&replication, dialect)?
            }
            MaterializationStrategy::MaterializedView => {
                strategy_name = "materialized_view";
                sql_gen::generate_create_table_as_sql(&replication, dialect)?
            }
            MaterializationStrategy::DynamicTable { .. } => {
                strategy_name = "dynamic_table";
                sql_gen::generate_create_table_as_sql(&replication, dialect)?
            }
            MaterializationStrategy::TimeInterval { .. } => {
                // time_interval is a transformation strategy (silver-layer
                // models), not a replication strategy. The replication
                // pipeline cannot construct this variant from rocky.toml.
                anyhow::bail!(
                    "time_interval strategy is not supported on replication tables — \
                     it only applies to transformation models"
                );
            }
            MaterializationStrategy::Ephemeral => {
                // Ephemeral models are never materialized — skip.
                anyhow::bail!(
                    "ephemeral strategy is not supported on replication tables — \
                     it only applies to transformation models"
                );
            }
            MaterializationStrategy::DeleteInsert { .. } => {
                strategy_name = "delete_insert";
                // For replication, delete+insert falls back to full refresh.
                sql_gen::generate_create_table_as_sql(&replication, dialect)?
            }
            MaterializationStrategy::Microbatch { .. } => {
                strategy_name = "microbatch";
                // Microbatch on replication tables falls back to incremental insert.
                sql_gen::generate_insert_sql(&replication, dialect)?
            }
        }
    };

    debug!(
        table = target_table.full_name(),
        strategy = strategy_name,
        sql = sql.as_str(),
        "generated SQL"
    );
    info!(
        table = target_table.full_name(),
        strategy = strategy_name,
        "copying data"
    );
    let exec_stats = warehouse
        .execute_statement_with_stats(&sql)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    // Tagging and watermark updates are deferred to post-run batch phase
    // to avoid blocking the concurrency semaphore with sequential SQL
    // (tagging) and Mutex contention (watermark).
    let now = Utc::now();

    let deferred_tags = if !task.governance_tags.is_empty() {
        Some(DeferredTagging {
            catalog: target_table.catalog.clone(),
            schema: target_table.schema.clone(),
            table: target_table.table.clone(),
            tags: task.governance_tags.clone(),
        })
    } else {
        None
    };

    let deferred_watermark = Some(DeferredWatermark {
        state_key: target_table.state_key(),
        timestamp: now,
    });

    let table_duration = table_start.elapsed().as_millis() as u64;

    let column_match_check = if task.check_column_match {
        Some(checks::check_column_match(
            &source_cols,
            &target_cols,
            &task.column_match_exclude,
        ))
    } else {
        None
    };

    let source_batch_ref = if task.check_row_count {
        Some(TableRef {
            catalog: source_table.catalog.clone(),
            schema: source_table.schema.clone(),
            table: source_table.table.clone(),
        })
    } else {
        None
    };

    let target_batch_ref = if task.check_row_count {
        Some(TableRef {
            catalog: target_table.catalog.clone(),
            schema: target_table.schema.clone(),
            table: target_table.table.clone(),
        })
    } else {
        None
    };

    let freshness_batch_ref = if task.check_freshness {
        Some(TableRef {
            catalog: target_table.catalog.clone(),
            schema: target_table.schema.clone(),
            table: target_table.table.clone(),
        })
    } else {
        None
    };

    let target_table_full_name = target_table.full_name();
    Ok(TableResult {
        materialization: MaterializationOutput {
            asset_key: asset_key.clone(),
            rows_copied: None,
            duration_ms: table_duration,
            started_at: table_started_at,
            metadata: MaterializationMetadata {
                strategy: strategy_name.to_string(),
                watermark: Some(now),
                target_table_full_name: Some(target_table_full_name.clone()),
                sql_hash: None,
                column_count: None,
                compile_time_ms: None,
            },
            // Replication tables are never time_interval; no partition info.
            partition: None,
            cost_usd: None,
            bytes_scanned: exec_stats.bytes_scanned,
            bytes_written: exec_stats.bytes_written,
        },
        drift_checked: true,
        drift_detected: drift_action,
        column_match_check,
        source_batch_ref,
        target_batch_ref,
        freshness_batch_ref,
        asset_key,
        target_full_name: target_table_full_name,
        deferred_tags,
        deferred_watermark,
    })
}

/// Adjusts the semaphore capacity to match the throttle's current recommendation.
///
/// When the throttle reduces concurrency (after a rate limit), we leave permits
/// unreleased — the semaphore naturally blocks new tasks until enough in-flight
/// tasks complete. When the throttle increases concurrency (after sustained
/// success), we add permits so more tasks can be spawned immediately.
fn adjust_semaphore(
    throttle: &AdaptiveThrottle,
    semaphore: &Semaphore,
    semaphore_capacity: &mut usize,
) {
    let target = throttle.current();
    if target > *semaphore_capacity {
        let added = target - *semaphore_capacity;
        semaphore.add_permits(added);
        debug!(
            from = *semaphore_capacity,
            to = target,
            "adaptive throttle: increased semaphore permits"
        );
        *semaphore_capacity = target;
    } else if target < *semaphore_capacity {
        // Reducing permits: we can't remove permits from a Tokio semaphore,
        // but we can acquire and forget them to permanently consume them.
        // However, this would block if no permits are available. Instead,
        // we use try_acquire_many to consume only what's immediately free.
        let reduce_by = *semaphore_capacity - target;
        if let Ok(permit) = semaphore.try_acquire_many(reduce_by as u32) {
            permit.forget();
            debug!(
                from = *semaphore_capacity,
                to = target,
                "adaptive throttle: reduced semaphore permits"
            );
        } else {
            // Can't acquire all at once (tasks hold them). Acquire what we
            // can — the rest will drain naturally as tasks complete.
            for _ in 0..reduce_by {
                if let Ok(p) = semaphore.try_acquire() {
                    p.forget();
                }
            }
            debug!(
                from = *semaphore_capacity,
                to = target,
                "adaptive throttle: partially reduced semaphore permits"
            );
        }
        *semaphore_capacity = target;
    }
}

/// Processes a single completed task result during the spawn loop's inline
/// drain pass (adaptive concurrency only). This avoids duplicating the
/// result-handling logic from the main collection loop for results that
/// arrive while we're still spawning tasks.
#[allow(clippy::too_many_arguments)]
async fn process_completed_result(
    result: Result<(usize, Result<TableResult, anyhow::Error>), tokio::task::JoinError>,
    tables_to_process: &[TableTask],
    throttle: &Option<AdaptiveThrottle>,
    semaphore: &Semaphore,
    semaphore_capacity: &mut usize,
    output: &mut RunOutput,
    pending_checks: &mut HashMap<String, PendingCheck>,
    source_batch_refs: &mut Vec<TableRef>,
    target_batch_refs: &mut Vec<TableRef>,
    freshness_batch_refs: &mut Vec<TableRef>,
    batch_asset_keys: &mut Vec<(String, Vec<String>)>,
    table_errors: &mut Vec<TableError>,
    deferred_tags: &mut Vec<DeferredTagging>,
    deferred_watermarks: &mut Vec<DeferredWatermark>,
    shared_state: &Mutex<StateStore>,
    shared_run_id: &str,
    total_completed: &mut usize,
) {
    *total_completed += 1;

    match result {
        Ok((_, Ok(tr))) => {
            if let Some(t) = &throttle {
                t.on_success();
                adjust_semaphore(t, semaphore, semaphore_capacity);
            }
            output.tables_copied += 1;
            rocky_observe::metrics::METRICS.inc_tables_processed();
            rocky_observe::metrics::METRICS
                .record_table_duration_ms(tr.materialization.duration_ms);
            output.materializations.push(tr.materialization);

            if tr.drift_checked {
                output.drift.tables_checked += 1;
            }
            if let Some(drift_action) = tr.drift_detected {
                output.drift.tables_drifted += 1;
                output.drift.actions_taken.push(drift_action);
            }

            if let Some(check) = tr.column_match_check {
                let entry = pending_checks
                    .entry(tr.target_full_name.clone())
                    .or_insert_with(|| PendingCheck {
                        asset_key: tr.asset_key.clone(),
                        checks: Vec::new(),
                    });
                entry.checks.push(check);
            }

            if let Some(src_ref) = tr.source_batch_ref {
                source_batch_refs.push(src_ref);
            }
            if let Some(tgt_ref) = tr.target_batch_ref {
                target_batch_refs.push(tgt_ref);
            }
            batch_asset_keys.push((tr.target_full_name.clone(), tr.asset_key.clone()));

            if let Some(fresh_ref) = tr.freshness_batch_ref {
                freshness_batch_refs.push(fresh_ref);
            }

            if let Some(tags) = tr.deferred_tags {
                deferred_tags.push(tags);
            }
            if let Some(wm) = tr.deferred_watermark {
                deferred_watermarks.push(wm);
            }

            // Checkpoint: record successful table progress
            {
                let state = shared_state.lock().await;
                if let Err(e) = state.record_table_progress(
                    shared_run_id,
                    &rocky_core::state::TableProgress {
                        index: *total_completed - 1,
                        table_key: tr.target_full_name,
                        asset_key: tr.asset_key,
                        status: rocky_core::state::TableStatus::Success,
                        error: None,
                        duration_ms: output.materializations.last().map_or(0, |m| m.duration_ms),
                        completed_at: Utc::now(),
                    },
                ) {
                    tracing::warn!(error = %e, "failed to record table progress for run ");
                }
            }
        }
        Ok((idx, Err(e))) => {
            let msg = format!("{e:#}");
            if msg.contains("TABLE_OR_VIEW_NOT_FOUND") {
                warn!(
                    table_index = idx,
                    error = msg.as_str(),
                    "source table not found, skipping"
                );
                return;
            }

            // Signal the adaptive throttle
            if let Some(t) = &throttle {
                if is_rate_limit_error(&msg) {
                    t.on_rate_limit();
                    adjust_semaphore(t, semaphore, semaphore_capacity);
                }
            }

            warn!(error = msg, "table processing failed");
            rocky_observe::metrics::METRICS.inc_tables_failed();

            // Checkpoint: record failed table progress
            {
                let task = tables_to_process.get(idx);
                let table_key = task
                    .map(|t| format!("{}.{}.{}", t.target_catalog, t.target_schema, t.table_name))
                    .unwrap_or_default();
                let state = shared_state.lock().await;
                if let Err(e) = state.record_table_progress(
                    shared_run_id,
                    &rocky_core::state::TableProgress {
                        index: idx,
                        table_key,
                        asset_key: vec![],
                        status: rocky_core::state::TableStatus::Failed,
                        error: Some(msg.clone()),
                        duration_ms: 0,
                        completed_at: Utc::now(),
                    },
                ) {
                    tracing::warn!(error = %e, "failed to record table progress for run ");
                }
            }

            table_errors.push(TableError {
                asset_key: vec![],
                error: msg,
                task_index: Some(idx),
            });
        }
        Err(e) => {
            let msg = format!("task failed: {e}");
            warn!(error = msg, "table task panicked");

            // Checkpoint: record panicked task progress
            {
                let state = shared_state.lock().await;
                if let Err(e) = state.record_table_progress(
                    shared_run_id,
                    &rocky_core::state::TableProgress {
                        index: 0,
                        table_key: String::new(),
                        asset_key: vec![],
                        status: rocky_core::state::TableStatus::Failed,
                        error: Some(msg.clone()),
                        duration_ms: 0,
                        completed_at: Utc::now(),
                    },
                ) {
                    tracing::warn!(error = %e, "failed to record table progress for run ");
                }
            }

            table_errors.push(TableError {
                asset_key: vec![],
                error: msg,
                task_index: None,
            });
        }
    }
}

/// Returns `true` if the error message indicates a warehouse rate limit.
///
/// Used by the adaptive concurrency controller to distinguish rate-limit
/// errors (which should reduce concurrency) from other transient or
/// permanent errors. Matches Databricks-specific signals: HTTP 429 and
/// the Unity Catalog `UC_REQUEST_LIMIT_EXCEEDED` error code.
fn is_rate_limit_error(error_msg: &str) -> bool {
    let upper = error_msg.to_uppercase();
    upper.contains("429")
        || upper.contains("UC_REQUEST_LIMIT_EXCEEDED")
        || upper.contains("RATE LIMIT")
        || upper.contains("TOO MANY REQUESTS")
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocky_core::plan_partition::PartitionSelection;

    #[test]
    fn test_partition_options_default_no_selection() {
        // No flags → to_selection returns None (caller defaults to Latest).
        let opts = PartitionRunOptions::default();
        assert!(opts.to_selection().is_none());
        assert!(!opts.any_set());
    }

    #[test]
    fn test_partition_options_single() {
        let opts = PartitionRunOptions {
            partition: Some("2026-04-07".into()),
            ..Default::default()
        };
        match opts.to_selection() {
            Some(PartitionSelection::Single(key)) => assert_eq!(key, "2026-04-07"),
            other => panic!("expected Single, got {other:?}"),
        }
        assert!(opts.any_set());
    }

    #[test]
    fn test_partition_options_range() {
        let opts = PartitionRunOptions {
            from: Some("2026-04-01".into()),
            to: Some("2026-04-07".into()),
            ..Default::default()
        };
        match opts.to_selection() {
            Some(PartitionSelection::Range { from, to }) => {
                assert_eq!(from, "2026-04-01");
                assert_eq!(to, "2026-04-07");
            }
            other => panic!("expected Range, got {other:?}"),
        }
    }

    #[test]
    fn test_partition_options_latest() {
        let opts = PartitionRunOptions {
            latest: true,
            ..Default::default()
        };
        assert!(matches!(
            opts.to_selection(),
            Some(PartitionSelection::Latest)
        ));
    }

    #[test]
    fn test_partition_options_missing() {
        let opts = PartitionRunOptions {
            missing: true,
            ..Default::default()
        };
        assert!(matches!(
            opts.to_selection(),
            Some(PartitionSelection::Missing)
        ));
    }

    #[test]
    fn test_partition_options_lookback_alone_does_not_imply_selection() {
        // --lookback without a primary selection flag is a no-op selection-wise;
        // it modifies whichever default the runtime picks (Latest).
        let opts = PartitionRunOptions {
            lookback: Some(3),
            ..Default::default()
        };
        assert!(opts.to_selection().is_none());
        assert!(opts.any_set()); // any_set returns true so the runtime knows
    }

    #[test]
    fn test_partition_options_priority_partition_over_range() {
        // Defensive: if somehow both `partition` and `from`/`to` slip past
        // clap's `conflicts_with`, partition wins.
        let opts = PartitionRunOptions {
            partition: Some("2026-04-07".into()),
            from: Some("2026-04-01".into()),
            to: Some("2026-04-08".into()),
            ..Default::default()
        };
        assert!(matches!(
            opts.to_selection(),
            Some(PartitionSelection::Single(_))
        ));
    }

    #[test]
    fn test_execution_context_lookups_known_model() {
        // ExecutionContext::column_count_for and ::compile_time_ms_for both
        // hit when the model is in the typecheck and timings maps. This is
        // the happy path the run.rs paths take for derived models.
        use indexmap::IndexMap;
        use rocky_compiler::compile::ModelCompileTimings;
        use rocky_compiler::types::{RockyType, TypedColumn};
        use std::collections::HashMap;

        let mut typed_models: IndexMap<String, Vec<TypedColumn>> = IndexMap::new();
        typed_models.insert(
            "fct_orders".into(),
            vec![
                TypedColumn {
                    name: "order_id".into(),
                    data_type: RockyType::Int64,
                    nullable: false,
                },
                TypedColumn {
                    name: "amount".into(),
                    data_type: RockyType::Float64,
                    nullable: true,
                },
                TypedColumn {
                    name: "order_date".into(),
                    data_type: RockyType::Date,
                    nullable: false,
                },
            ],
        );

        let mut model_timings: HashMap<String, ModelCompileTimings> = HashMap::new();
        model_timings.insert(
            "fct_orders".into(),
            ModelCompileTimings {
                typecheck_ms: 17,
                total_ms: 17,
            },
        );

        let ctx = ExecutionContext {
            typed_models: &typed_models,
            model_timings: &model_timings,
        };

        assert_eq!(ctx.column_count_for("fct_orders"), Some(3));
        assert_eq!(ctx.compile_time_ms_for("fct_orders"), Some(17));
    }

    #[test]
    fn test_execution_context_lookups_unknown_model_returns_none() {
        // Source replication tables and any model the typechecker didn't see
        // must miss cleanly with None — the run path leaves the metadata
        // fields unpopulated rather than fabricating zeros.
        use indexmap::IndexMap;
        use rocky_compiler::compile::ModelCompileTimings;
        use std::collections::HashMap;

        let typed_models = IndexMap::new();
        let model_timings: HashMap<String, ModelCompileTimings> = HashMap::new();
        let ctx = ExecutionContext {
            typed_models: &typed_models,
            model_timings: &model_timings,
        };

        assert_eq!(ctx.column_count_for("raw__shopify__orders"), None);
        assert_eq!(ctx.compile_time_ms_for("raw__shopify__orders"), None);
    }

    // -----------------------------------------------------------------------
    // Adaptive concurrency tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_is_rate_limit_error_http_429() {
        assert!(is_rate_limit_error("API error 429: rate limited"));
        assert!(is_rate_limit_error("HTTP 429 Too Many Requests"));
    }

    #[test]
    fn test_is_rate_limit_error_uc_request_limit() {
        assert!(is_rate_limit_error(
            "statement stmt-1 failed: UC_REQUEST_LIMIT_EXCEEDED: rate limit"
        ));
    }

    #[test]
    fn test_is_rate_limit_error_generic_rate_limit() {
        assert!(is_rate_limit_error("rate limit exceeded"));
        assert!(is_rate_limit_error("Too Many Requests"));
    }

    #[test]
    fn test_is_rate_limit_error_false_for_other_errors() {
        assert!(!is_rate_limit_error("TABLE_OR_VIEW_NOT_FOUND"));
        assert!(!is_rate_limit_error("PARSE_ERROR: syntax error"));
        assert!(!is_rate_limit_error("connection refused"));
        assert!(!is_rate_limit_error(
            "statement stmt-1 failed: unauthorized"
        ));
    }

    #[test]
    fn test_adjust_semaphore_increase() {
        let throttle = AdaptiveThrottle::new(8, 1, 1);
        let semaphore = Semaphore::new(4);
        let mut capacity = 4;

        // Reduce throttle first, then increase
        throttle.on_rate_limit(); // 8 -> 4
        // At c=4 and half_max=4, c < half_max is false, so congestion
        // avoidance applies: increase by 1 per success.
        throttle.on_success(); // 4 -> 5

        adjust_semaphore(&throttle, &semaphore, &mut capacity);
        assert_eq!(capacity, 5);
        // Semaphore should have 1 extra permit (4 original + 1 added)
        assert_eq!(semaphore.available_permits(), 5);
    }

    #[test]
    fn test_adjust_semaphore_decrease() {
        let throttle = AdaptiveThrottle::new(8, 1, 10);
        let semaphore = Semaphore::new(8);
        let mut capacity = 8;

        throttle.on_rate_limit(); // 8 -> 4

        adjust_semaphore(&throttle, &semaphore, &mut capacity);
        assert_eq!(capacity, 4);
        // Semaphore should have fewer available permits
        assert_eq!(semaphore.available_permits(), 4);
    }

    #[test]
    fn test_adjust_semaphore_no_change() {
        let throttle = AdaptiveThrottle::new(8, 1, 10);
        let semaphore = Semaphore::new(8);
        let mut capacity = 8;

        // No rate limit, no change
        adjust_semaphore(&throttle, &semaphore, &mut capacity);
        assert_eq!(capacity, 8);
        assert_eq!(semaphore.available_permits(), 8);
    }

    #[test]
    fn test_throttle_wired_into_result_loop() {
        // Verify that the AdaptiveThrottle clone/share semantics work
        // correctly when used from the run loop pattern.
        let throttle = AdaptiveThrottle::new(16, 1, 5);

        // Simulate rate limits reducing concurrency
        throttle.on_rate_limit(); // 16 -> 8
        assert_eq!(throttle.current(), 8);
        throttle.on_rate_limit(); // 8 -> 4
        assert_eq!(throttle.current(), 4);

        // Simulate sustained success increasing concurrency
        for _ in 0..5 {
            throttle.on_success();
        }
        // Below half of max (4 < 8), slow-start phase: increase by 2
        assert_eq!(throttle.current(), 6);

        // Verify metrics
        assert_eq!(throttle.rate_limits_total(), 2);
    }

    #[test]
    fn test_emit_pipes_drift_produces_structured_check_and_log() {
        use crate::output::{
            DriftActionOutput, DriftSummary, ExecutionSummary, PermissionSummary, RunOutput,
        };
        use crate::pipes::PipesEmitter;
        use std::io::Read;
        use std::sync::Mutex;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pipes_drift.txt");
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .unwrap();
        let emitter = PipesEmitter {
            channel: Mutex::new(Box::new(file)),
        };

        let output = RunOutput {
            version: "1.0.2".into(),
            command: "run".into(),
            status: rocky_core::state::RunStatus::Success,
            skipped_by_run_id: None,
            idempotency_key: None,
            pipeline_type: None,
            filter: "tenant=acme".into(),
            duration_ms: 100,
            tables_copied: 0,
            tables_failed: 0,
            tables_skipped: 0,
            excluded_tables: vec![],
            resumed_from: None,
            shadow: false,
            materializations: vec![],
            check_results: vec![],
            quarantine: vec![],
            anomalies: vec![],
            errors: vec![],
            execution: ExecutionSummary {
                concurrency: 1,
                tables_processed: 0,
                tables_failed: 0,
                adaptive_concurrency: false,
                final_concurrency: None,
                rate_limits_detected: None,
            },
            metrics: None,
            permissions: PermissionSummary {
                grants_added: 0,
                grants_revoked: 0,
                catalogs_created: 0,
                schemas_created: 0,
            },
            drift: DriftSummary {
                tables_checked: 1,
                tables_drifted: 1,
                actions_taken: vec![DriftActionOutput {
                    table: "acme.raw_orders".into(),
                    action: "add_column".into(),
                    reason: "column 'email' found in source but not target".into(),
                }],
            },
            partition_summaries: vec![],
            interrupted: false,
            cost_summary: None,
            budget_breaches: vec![],
        };

        emit_pipes_events(&emitter, &output);

        let mut content = String::new();
        std::fs::File::open(&path)
            .unwrap()
            .read_to_string(&mut content)
            .unwrap();
        let lines: Vec<serde_json::Value> = content
            .lines()
            .filter(|l| !l.is_empty())
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();

        // Should produce 2 messages: one report_asset_check + one log
        assert_eq!(lines.len(), 2);

        // First: structured check
        let check = &lines[0];
        assert_eq!(check["method"], "report_asset_check");
        assert_eq!(check["params"]["check_name"], "drift");
        assert_eq!(check["params"]["passed"], true);
        assert_eq!(check["params"]["severity"], "WARN");
        assert_eq!(check["params"]["metadata"]["table"], "acme.raw_orders");
        assert_eq!(check["params"]["metadata"]["action"], "add_column");

        // Second: human-readable log
        let log = &lines[1];
        assert_eq!(log["method"], "log");
        assert_eq!(log["params"]["level"], "WARN");
        let msg = log["params"]["message"].as_str().unwrap();
        assert!(msg.contains("acme.raw_orders"));
        assert!(msg.contains("add_column"));
    }

    // -----------------------------------------------------------------------
    // FR-004 F1 — error-path idempotency finalize.
    //
    // These tests cover the guarantee that a `rocky run --idempotency-key K`
    // which fails with any error *before* the existing happy-path
    // `finalize_idempotency` site still releases its `InFlight` claim, so a
    // retry with the same key proceeds immediately instead of waiting up to
    // `in_flight_ttl_hours` (default 24h) for the sweep.
    //
    // We test `finalize_idempotency_on_error` directly because driving
    // `run()` end-to-end through a contrived error path is substantially
    // more setup than the guarantee requires — the helper is the single
    // point of truth for the error-path release semantics.
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn finalize_on_error_releases_inflight_claim_allowing_immediate_retry() {
        use rocky_core::config::{DedupPolicy, IdempotencyConfig};
        use rocky_core::idempotency::{IdempotencyBackend, IdempotencyCheck, IdempotencyEntry};
        use rocky_core::state::StateStore;

        let tmp = tempfile::TempDir::new().expect("temp dir");
        let state_path = tmp.path().join("state.redb");

        // --- Simulate run 1: claim the key, then crash before finalize. ---
        let store = StateStore::open(&state_path).expect("open state store");
        let key = "test-key-error-path";
        let run_id_1 = "run-crashed-before-finalize";
        let now = chrono::Utc::now();
        let entry = IdempotencyEntry {
            key: key.to_string(),
            run_id: run_id_1.to_string(),
            state: rocky_core::idempotency::IdempotencyState::InFlight,
            stamped_at: now,
            expires_at: now + chrono::Duration::hours(24),
            dedup_on: DedupPolicy::Success,
        };
        let verdict = store
            .idempotency_check_and_claim(&entry, now)
            .expect("claim");
        assert!(
            matches!(verdict, IdempotencyCheck::Proceed),
            "first claim must Proceed, got {verdict:?}"
        );

        // Pre-condition: an InFlight entry exists for run_id_1.
        let got = store.idempotency_get(key).expect("get").expect("present");
        assert_eq!(got.run_id, run_id_1);
        assert_eq!(
            got.state,
            rocky_core::idempotency::IdempotencyState::InFlight
        );

        // Drop the store handle before running the error-path helper —
        // matches the real-world flow where the claim's store goes out
        // of scope and the helper opens its own read/write handle.
        drop(store);

        // --- Fire the error-path finalize helper. ---
        let mut ctx_slot = Some(IdempotencyCtx {
            key: key.to_string(),
            backend: IdempotencyBackend::Local,
            config: IdempotencyConfig {
                retention_days: 30,
                dedup_on: DedupPolicy::Success,
                in_flight_ttl_hours: 24,
            },
            _claim_backend_label: "local",
        });
        finalize_idempotency_on_error(&mut ctx_slot, &state_path, run_id_1).await;
        assert!(
            ctx_slot.is_none(),
            "finalize_idempotency_on_error must take the ctx (one-shot)"
        );

        // --- Post-condition: the entry is gone (dedup_on = success + Failed → delete). ---
        let store = StateStore::open(&state_path).expect("reopen");
        assert!(
            store.idempotency_get(key).expect("get").is_none(),
            "InFlight entry must be released on error so retries can claim immediately"
        );

        // --- Retry semantics: second claim with the same key must Proceed. ---
        let run_id_2 = "run-retry-after-error";
        let retry_entry = IdempotencyEntry {
            key: key.to_string(),
            run_id: run_id_2.to_string(),
            state: rocky_core::idempotency::IdempotencyState::InFlight,
            stamped_at: now,
            expires_at: now + chrono::Duration::hours(24),
            dedup_on: DedupPolicy::Success,
        };
        let retry_verdict = store
            .idempotency_check_and_claim(&retry_entry, now)
            .expect("retry claim");
        assert!(
            matches!(retry_verdict, IdempotencyCheck::Proceed),
            "retry with same key must Proceed immediately after error-path release, got {retry_verdict:?}"
        );
    }

    #[tokio::test]
    async fn finalize_on_error_is_one_shot_and_idempotent_when_ctx_already_taken() {
        use rocky_core::config::{DedupPolicy, IdempotencyConfig};
        use rocky_core::idempotency::IdempotencyBackend;
        use rocky_core::state::StateStore;

        let tmp = tempfile::TempDir::new().expect("temp dir");
        let state_path = tmp.path().join("state.redb");
        // Create the file by opening and dropping a store so the helper
        // doesn't have to deal with a missing state file.
        drop(StateStore::open(&state_path).expect("init"));

        let mut ctx_slot: Option<IdempotencyCtx> = None;
        // Second call with `None` must be a no-op — guards against the
        // wrapper firing after the happy-path finalize already ran.
        finalize_idempotency_on_error(&mut ctx_slot, &state_path, "run-x").await;
        assert!(ctx_slot.is_none());

        // Seed once and verify the first call drains it.
        ctx_slot = Some(IdempotencyCtx {
            key: "k".into(),
            backend: IdempotencyBackend::Local,
            config: IdempotencyConfig {
                retention_days: 30,
                dedup_on: DedupPolicy::Success,
                in_flight_ttl_hours: 24,
            },
            _claim_backend_label: "local",
        });
        finalize_idempotency_on_error(&mut ctx_slot, &state_path, "run-x").await;
        assert!(ctx_slot.is_none(), "helper must drain the Option");
    }

    #[tokio::test]
    async fn finalize_idempotency_is_one_shot_take() {
        // Guards the contract that `finalize_idempotency` takes the ctx
        // out of its `Option<_>` so the outer error-path wrapper in
        // `run()` cannot over-write a Succeeded stamp with a Failed one
        // (which under `dedup_on = "success"` would delete the entry).
        use rocky_core::config::{DedupPolicy, IdempotencyConfig};
        use rocky_core::idempotency::IdempotencyBackend;
        use rocky_core::state::StateStore;

        let tmp = tempfile::TempDir::new().expect("temp dir");
        let state_path = tmp.path().join("state.redb");
        let store = StateStore::open(&state_path).expect("open");

        let mut ctx_slot = Some(IdempotencyCtx {
            key: "one-shot".into(),
            backend: IdempotencyBackend::Local,
            config: IdempotencyConfig {
                retention_days: 30,
                dedup_on: DedupPolicy::Success,
                in_flight_ttl_hours: 24,
            },
            _claim_backend_label: "local",
        });

        // Synthetic successful output.
        let output = RunOutput::new(String::new(), 0, 1);
        finalize_idempotency(&mut ctx_slot, Some(&store), "run-ok", &output).await;
        assert!(
            ctx_slot.is_none(),
            "first finalize must drain the Option; the outer wrapper relies on this \
             to skip the error-path finalize after a happy-path finalize ran"
        );

        // Second call is a no-op — no panic, ctx remains None.
        finalize_idempotency(&mut ctx_slot, Some(&store), "run-ok", &output).await;
        assert!(ctx_slot.is_none());
    }

    // -----------------------------------------------------------------------
    // FR-004 F2 — success-path idempotency finalize for non-replication
    // dispatch arms.
    //
    // The four non-replication arms (Transformation, Quality, Snapshot,
    // Load) delegate to helpers in `run_local.rs` / `load.rs` that don't
    // receive or produce an `IdempotencyCtx`. Before F2, those arms
    // returned `Ok(())` directly from their dispatch branch — the happy
    // path never called `finalize_idempotency`, and the error-path
    // wrapper's `is_err()` check skipped it. The `InFlight` stamp then
    // lingered until the 24h `in_flight_ttl_hours` sweep reaped it, and
    // a retry with the same key inside that window returned
    // `skipped_in_flight` instead of proceeding — the exact latent
    // landmine #237 closed for the error path.
    //
    // This test drives `run()` end-to-end against a transformation
    // pipeline and confirms the redb entry is `Succeeded`, not
    // `InFlight`, after a successful run. Transformation is the cleanest
    // of the four arms to exercise — a transformation pipeline with a
    // missing models directory succeeds trivially without needing live
    // adapter credentials, DDL, or warehouse mocks. Without the F2 fix,
    // the `Succeeded` assertion fails because the entry is still
    // `InFlight`.
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn transformation_success_stamps_idempotency_succeeded_not_inflight() {
        use rocky_core::idempotency::IdempotencyState;
        use rocky_core::state::StateStore;

        let tmp = tempfile::TempDir::new().expect("temp dir");
        let config_dir = tmp.path();
        let config_path = config_dir.join("rocky.toml");
        // Transformation pipeline with a non-existent models dir.
        // `run_transformation` warns and returns `Ok(())` — perfect for
        // exercising the happy-path dispatch without needing warehouse
        // state beyond an in-memory DuckDB handle.
        std::fs::write(
            &config_path,
            r#"
[adapter]
type = "duckdb"
path = ":memory:"

[state]
backend = "local"

[pipeline.t]
type = "transformation"
models = "models_nonexistent/**"

[pipeline.t.target]
adapter = "default"
"#,
        )
        .expect("write rocky.toml");

        let state_path = config_dir.join("state.redb");

        let key = "fr-004-f2-transformation-success";
        let opts = PartitionRunOptions::default();

        // Drive `run()` end-to-end. With `--idempotency-key`, the claim
        // stamps InFlight before dispatch, then the Transformation arm
        // returns Ok(()) and (post-F2) fires `finalize_idempotency_on_success`.
        super::run(
            &config_path,
            None,
            None,
            &state_path,
            None,
            false,
            None,
            false,
            None,
            false,
            None,
            &opts,
            None,
            None,
            Some(key),
            None,
        )
        .await
        .expect("transformation run should succeed");

        // Post-condition: the idempotency entry exists and is `Succeeded`.
        // Pre-F2 this fails because the entry is still `InFlight` — the
        // success-path never called finalize, and the error-path wrapper
        // is a no-op when the body returned `Ok`.
        let store = StateStore::open(&state_path).expect("reopen state store");
        let entry = store
            .idempotency_get(key)
            .expect("get idempotency entry")
            .expect("entry present — claim should have stamped it");
        assert_eq!(
            entry.state,
            IdempotencyState::Succeeded,
            "success-path finalize on the Transformation dispatch arm must stamp \
             Succeeded, not leave the InFlight claim for the TTL sweep to reap \
             (FR-004 F2 regression guard)"
        );
    }
}
