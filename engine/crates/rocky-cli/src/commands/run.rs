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

use rocky_catalog_core::{GovernanceCatalogClient, Grant as CatalogGrant, Securable};
use rocky_core::checks;
use rocky_core::config::{
    GovernanceOverride, ReplicationPipelineConfig, ResolvedTableOverride, resolve_table_override,
};
use rocky_core::drift;
use rocky_core::hooks::{HookContext, HookRegistry};
use rocky_core::sql_gen;
use rocky_core::state::StateStore;
use rocky_core::traits::{
    BatchCheckAdapter, FreshnessResult as BatchFreshnessResult, GovernanceAdapter, MaskingPolicy,
    RowCountResult as BatchRowCountResult, TagTarget, WarehouseAdapter,
};
use rocky_ir::*;

use crate::output::*;
use crate::registry::{self, AdapterRegistry};
use crate::schema_cache_writer::{SchemaCacheWriteTap, persist_batch_describe};

use super::run_audit::AuditContext;
use super::{filter_table_matches, matches_filter, parse_filter, parsed_to_json_map};

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
    /// Coarse classification of the failure for the public JSON output.
    /// Set by walking the typed connector-error chain on `e` before the
    /// error is stringified; defaults to `Unknown` when no connector
    /// variant is in the chain (drift / governance / panic paths).
    failure_kind: crate::output::FailureKind,
    /// Engine-supplied retry-after hint in whole seconds, populated
    /// alongside `failure_kind` from the typed connector chain when a
    /// warehouse circuit breaker tripped on a half-open-recovery
    /// configuration. `None` when no cooldown is available (legacy
    /// failures, non-breaker errors, breakers without a configured
    /// `circuit_breaker_recovery_timeout_secs`). Mirrors the
    /// source-side hint on `FailedSource.cooldown_seconds`.
    cooldown_seconds: Option<u64>,
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

/// Sentinel error signalling that `rocky run` completed with at least
/// one successful materialization *and* at least one table failure
/// (`RunStatus::PartialFailure`). The outer binary
/// (`rocky/src/main.rs`) downcasts this off the `anyhow::Error` chain
/// and maps it to exit code 2 so dagster's `allow_partial=True` can
/// distinguish partial success from a total failure (exit 1) or an
/// interrupt (exit 130). The full JSON `RunOutput` has already been
/// emitted on stdout by the time this is returned.
#[derive(Debug, thiserror::Error)]
#[error(
    "{count} table(s) failed during parallel execution (run_id: {run_id}, use --resume {run_id} to retry)"
)]
pub struct PartialFailure {
    pub count: usize,
    pub run_id: String,
}

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
    /// Per-table override resolved against
    /// `ReplicationPipelineConfig::table_overrides` at connector-loop
    /// time. Empty when no rule matched this `(connector, table)` pair.
    /// `process_table` reads this when building the materialization
    /// strategy so per-table overrides apply ahead of pipeline-level
    /// defaults.
    effective_override: ResolvedTableOverride,
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

/// Convert a slice of `rocky_ir::Grant` into the catalog-agnostic
/// `(Securable, Vec<CatalogGrant>)` shape expected by
/// [`GovernanceCatalogClient::apply_grants`].
///
/// The `GovernanceCatalogClient` trait surface speaks in
/// [`rocky_catalog_core::Grant`] (principal + privilege string) keyed on
/// a single [`Securable`]; the call sites in `run.rs` build
/// [`rocky_ir::Grant`] (principal + permission enum + target). All grants
/// in `grants` are required to share the same target — the call site
/// builds them that way (one securable per batch), so the conversion is
/// total when the target is `Catalog` or `Schema`.
///
/// Returns `None` when `grants` is empty or carries no target the catalog
/// trait can speak; in that case the caller falls through to the existing
/// [`GovernanceAdapter::apply_grants`] path.
fn ir_grants_to_securable_batch(grants: &[Grant]) -> Option<(Securable, Vec<CatalogGrant>)> {
    let first = grants.first()?;
    let securable = match &first.target {
        GrantTarget::Catalog(name) => Securable::Catalog { name: name.clone() },
        GrantTarget::Schema { catalog, schema } => Securable::Schema {
            catalog: catalog.clone(),
            name: schema.clone(),
        },
    };
    // Every grant in the batch must share the same target — the call
    // site builds them that way. Bail out if any disagrees so the caller
    // routes through the legacy path rather than splitting the batch.
    if grants.iter().any(|g| g.target != first.target) {
        return None;
    }
    let catalog_grants = grants
        .iter()
        .map(|g| CatalogGrant {
            principal: g.principal.clone(),
            // Unity's REST permissions endpoint uses the underscore-delimited
            // `Privilege` enum (`USE_CATALOG`, `USE_SCHEMA`), unlike the SQL
            // `GRANT` grammar (`USE CATALOG`) that `Permission`'s `Display`
            // renders. Map spaces to underscores at this IR -> REST boundary
            // so multi-word privileges reach the API in the shape it accepts.
            privilege: g.permission.to_string().replace(' ', "_"),
        })
        .collect();
    Some((securable, catalog_grants))
}

/// Apply a batch of `Vec<Grant>` against `securable`, preferring the
/// REST-routed [`GovernanceCatalogClient`] path when present, falling
/// back to the singular-target [`GovernanceAdapter::apply_grants`] path
/// otherwise.
///
/// The REST path collapses the batch into one HTTP PATCH per securable
/// (multi-change wire shape); the legacy path remains the existing per-
/// grant SQL `GRANT` sequence. Both are best-effort: errors are warned
/// rather than aborting the run, matching the call site's existing
/// failure semantics.
async fn apply_grants_with_catalog_client_fallback(
    governance_catalog_client: Option<&std::sync::Arc<dyn GovernanceCatalogClient>>,
    governance_adapter: &dyn GovernanceAdapter,
    grants: &[Grant],
    scope_label: &str,
) {
    if grants.is_empty() {
        return;
    }
    if let Some(client) = governance_catalog_client
        && let Some((securable, catalog_grants)) = ir_grants_to_securable_batch(grants)
    {
        match client.apply_grants(&securable, &catalog_grants).await {
            Ok(()) => return,
            Err(e) => {
                warn!(
                    scope = scope_label,
                    error = %e,
                    "REST-routed apply_grants failed; falling back to GovernanceAdapter"
                );
                // Fall through to the legacy path below.
            }
        }
    }
    if let Err(e) = governance_adapter.apply_grants(grants).await {
        warn!(scope = scope_label, error = %e, "grants failed");
    }
}

/// Execute `rocky run` — full pipeline.
#[allow(clippy::too_many_arguments)]
#[tracing::instrument(skip_all, name = "run", fields(run_id))]
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
    // `--cache-ttl <seconds>` override applied to the model-execution
    // schema cache read; the replication path doesn't consult the
    // cache, so this flag is a no-op for replication-only runs.
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
    // Record `run_id` on the `run` span declared by `#[tracing::instrument]`
    // above so every event nested under this scope (and emitted into the
    // JSONL trace file) carries the identifier in its `spans[]` chain.
    // The macro reserves the field at span entry with an empty value;
    // this call fills it in once minted.
    tracing::Span::current().record("run_id", run_id.as_str());

    // -------------------------------------------------------------------
    // Idempotency check + claim (before any state mutation / run_id mint).
    // Dispatch on the configured state backend; short-circuit with a typed
    // `RunOutput` + exit 0 when the key dedups.
    // -------------------------------------------------------------------
    let rocky_cfg_for_idemp = rocky_core::config::load_rocky_config(config_path).context(
        format!("failed to load config from {}", config_path.display()),
    )?;

    // OTLP **metrics** exporter (feature-gated). Auto-initialises when
    // `OTEL_EXPORTER_OTLP_ENDPOINT` is set so operators can opt in by
    // env alone — no CLI flag needed. Drop flushes the final snapshot
    // and shuts the periodic reader down, so this guard covers every
    // exit path (happy, interrupted, error) without threading an
    // explicit cleanup call.
    //
    // The OTel **tracer** provider is owned by `init_tracing` at the
    // top of `main.rs` — see the `TracingGuard` returned there. This
    // split lets every existing `info_span!` (in
    // `dag_executor::materialize.table`, `commands/run.rs::*`,
    // `state_sync.rs::state.*`, etc.) reach the OTLP collector even
    // before the run dispatch enters this function.
    let _otel_guard = crate::otel_guard::OtelGuard::init_if_enabled();

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
        // Propagate state-store open errors with context, matching the
        // full-pipeline run path below. The previous `.ok()` silently
        // disabled state persistence (lost watermarks, missing run history)
        // whenever the state file was corrupted or unreadable.
        let state_store = Some(
            rocky_core::state::StateStore::open(state_path).context(format!(
                "failed to open state store at {}",
                state_path.display()
            ))?,
        );
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
            false, // model-only entry point has no pipeline governance context
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

    // Opt-in REST-routed governance client: adapters that expose
    // multi-securable RBAC over REST implement [`GovernanceCatalogClient`]
    // so grant batches collapse into one PATCH per `Securable`. When `None`,
    // we fall through to the existing [`GovernanceAdapter::apply_grants`] SQL
    // path with no behavioural delta.
    let governance_catalog_client: Option<std::sync::Arc<dyn GovernanceCatalogClient>> =
        adapter_registry.governance_catalog_client(&pipeline.target.adapter);

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
                .map_err(anyhow::Error::from)
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
    // PR-B3: count how many `(connector, table)` pairs matched each
    // `[[table_overrides]]` rule. Rules with zero matches surface as
    // soft warnings on `RunOutput.override_warnings` (T2) so
    // orchestrators can flag stale rules without breaking the pipeline
    // for connectors that come and go.
    let mut override_rule_match_counts: HashMap<usize, usize> = HashMap::new();

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

            if let Some((ref filter_key, ref filter_value)) = parsed_filter
                && !matches_filter(conn, &parsed, filter_key, filter_value) {
                    continue;
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
                        .map_err(anyhow::Error::from)?;
                }

                // Create catalog via generic dialect SQL
                if let Some(sql_result) = dialect.create_catalog_sql(&target_catalog) {
                    warehouse_adapter
                        .execute_statement(&sql_result?)
                        .await
                        .map_err(anyhow::Error::from)?;
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
                if let Some(ov) = governance_override
                    && let Some(ids) = ov.workspace_ids.as_ref() {
                        for b in ids {
                            binding_map.insert(b.id, b.clone());
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
                    if needs_apply
                        && let Err(e) = governance_adapter
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

                // Remove bindings that exist on the catalog but aren't desired.
                for &cur_id in current_ids.keys() {
                    if !desired_ids.contains(&cur_id)
                        && let Err(e) = governance_adapter
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

                let should_isolate = governance.isolation.as_ref().is_some_and(|i| i.enabled);
                if should_isolate
                    && let Err(e) = governance_adapter
                        .set_isolation(&target_catalog, true)
                        .await
                    {
                        warn!(catalog = target_catalog, error = %e, "catalog isolation failed");
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

                    apply_grants_with_catalog_client_fallback(
                        governance_catalog_client.as_ref(),
                        governance_adapter.as_ref(),
                        &grants,
                        &format!("catalog={target_catalog}"),
                    )
                    .await;
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
                        .map_err(anyhow::Error::from)?;
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

                    apply_grants_with_catalog_client_fallback(
                        governance_catalog_client.as_ref(),
                        governance_adapter.as_ref(),
                        &grants,
                        &format!("schema={target_catalog}.{target_schema}"),
                    )
                    .await;
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
                // PR-B3: CLI `--filter table=<literal>` consumed here
                // — every other connector-level filter dimension has
                // already short-circuited above via `matches_filter`.
                if !filter_table_matches(parsed_filter.as_ref(), &table.name) {
                    continue;
                }
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

                // PR-B3: resolve per-table override against the
                // pipeline's `table_overrides` rules. Record per-rule
                // match counts for the discovery-time zero-match
                // warning (T2).
                let effective_override = resolve_table_override(
                    &pipeline.table_overrides,
                    &conn.id,
                    &conn.schema,
                    &table.name,
                );
                for (idx, rule) in pipeline.table_overrides.iter().enumerate() {
                    if rule.match_.matches(&conn.id, &conn.schema, &table.name) {
                        override_rule_match_counts
                            .entry(idx)
                            .and_modify(|c| *c += 1usize)
                            .or_insert(1);
                    }
                }

                // `enabled = false` → skip the table with an
                // explanatory excluded_tables entry. Mirrors the
                // missing-from-source path above so orchestrators see
                // a uniform exclusion surface.
                if effective_override.enabled == Some(false) {
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
                    info!(
                        table = table.name.as_str(),
                        schema = conn.schema.as_str(),
                        "table excluded by [[table_overrides]] (enabled = false)"
                    );
                    output.excluded_tables.push(ExcludedTableOutput {
                        asset_key,
                        source_schema: conn.schema.clone(),
                        table_name: table.name.clone(),
                        reason: "table_override_disabled".to_string(),
                    });
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
                    effective_override,
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

    // PR-B3 (T2): emit a structured warning for every
    // `[[table_overrides]]` rule that matched no `(connector, table)`
    // pair this run. Soft warning only — connectors come and go in
    // real production environments, and a stale rule is recoverable.
    // The output struct lets Dagster branch on `kind` without text
    // parsing.
    for (idx, rule) in pipeline.table_overrides.iter().enumerate() {
        if override_rule_match_counts.get(&idx).copied().unwrap_or(0) > 0 {
            continue;
        }
        let kind = if rule.match_.connector.is_some() && rule.match_.table.is_some() {
            "zero_match".to_string()
        } else if rule.match_.connector.is_some() {
            "connector_match_empty".to_string()
        } else {
            "zero_match".to_string()
        };
        let message = format!(
            "[[table_overrides]] rule index {idx} (connector = {:?}, table = {:?}) \
             matched no (connector, table) pair in this run",
            rule.match_.connector, rule.match_.table
        );
        warn!(
            rule_index = idx,
            kind = kind.as_str(),
            message = message.as_str(),
            "table_override rule matched zero tables"
        );
        output.override_warnings.push(OverrideWarningOutput {
            rule_index: idx,
            kind,
            message,
            connector: rule.match_.connector.clone(),
            table: rule.match_.table.clone(),
        });
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

        // Tap successful describes into the schema cache. One key per
        // table in the returned map — the DESCRIBE cost is already paid,
        // so populating the cache for sibling tables is free signal for
        // future compiles. Write failures are logged at `warn!` level
        // and never fail the run.
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
                // Classify before stringification so the typed connector
                // variant is preserved on `TableError.failure_kind` and
                // the optional engine-supplied cooldown hint
                // (warehouse-side breaker trip) is captured.
                let (failure_kind, cooldown_seconds) = classify_anyhow_error_with_cooldown(&e);
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
                if let Some(t) = &throttle
                    && is_rate_limit_error(&msg) {
                        t.on_rate_limit();
                        adjust_semaphore(t, &semaphore, &mut semaphore_capacity);
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
                    failure_kind,
                    cooldown_seconds,
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
                    failure_kind: FailureKind::Unknown,
                    cooldown_seconds: None,
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
                        let (failure_kind, cooldown_seconds) =
                            classify_anyhow_error_with_cooldown(&e);
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
                            failure_kind,
                            cooldown_seconds,
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
        .map_err(anyhow::Error::from)?;
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
                    .map_err(anyhow::Error::from)?;
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
                    .map_err(anyhow::Error::from)?;
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
                    .map_err(anyhow::Error::from)?;
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
    if row_count_enabled
        && let Some(ref store) = state_store {
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
                    pipeline.target.governance.auto_create_schemas,
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
                    project_freshness_default: rocky_cfg.freshness.has_default(),
                },
            );
            if let Ok(gov_compile) = governance_compile {
                let tag_to_strategy = rocky_cfg.resolve_mask_for_env(env);
                for model in &gov_compile.project.models {
                    let table_ref = TableRef {
                        catalog: model.config.target.catalog.clone(),
                        schema: model.config.target.schema.clone(),
                        table: model.config.target.table.clone(),
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
                    if let Some(retention) = model.config.retention
                        && let Err(e) = governance_adapter
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
            failure_kind: e.failure_kind,
            cooldown_seconds: e.cooldown_seconds,
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

        // Branch the exit-code contract: a *partial* failure (at least
        // one table copied + at least one failed) returns the
        // `PartialFailure` sentinel so `rocky/src/main.rs` can map it
        // to exit code 2. A total failure (no tables copied) keeps the
        // existing exit-code-1 path. The valid JSON `RunOutput` has
        // already been written to stdout above; dagster
        // (`allow_partial=True`) reads stdout regardless of exit code
        // for code 2.
        if matches!(
            output.derive_run_status(),
            rocky_core::state::RunStatus::PartialFailure
        ) {
            return Err(PartialFailure {
                count,
                run_id: run_id.clone(),
            }
            .into());
        }
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

    // End-of-run retention auto-sweep. Wires `StateStore::sweep_retention`
    // into the run loop so retention "just works" without an external cron
    // — the manual `rocky state retention sweep` subcommand stays as the
    // operator escape hatch. Runs unconditionally on both Ok and Err
    // paths: a failed run shouldn't postpone state cleanup, and the
    // interval gate inside `auto_sweep_retention_at_end_of_run` still
    // suppresses thrash. All errors swallowed as `tracing::warn` — the
    // run's exit code reflects the run, never the sweep. Placed before
    // the idempotency error-path finalize so an over-budget sweep never
    // delays the InFlight stamp release.
    auto_sweep_retention_at_end_of_run(state_path, &rocky_cfg_for_idemp.state.retention).await;

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

/// End-of-run retention sweep, gated by `last_retention_sweep_at`.
///
/// Reads the configured `[state.retention]` policy and the timestamp of
/// the most recent prior sweep from the state store's metadata table. If
/// the prior sweep is younger than `sweep_interval_seconds`, returns
/// without doing any work. Otherwise invokes [`StateStore::sweep_retention`],
/// measures wall-clock against `sweep_budget_ms`, and emits a
/// `tracing::warn` line on overrun. The sweep timestamp is stamped
/// regardless of whether the budget was met — otherwise an over-budget
/// sweep would fire every run.
///
/// Every error path is swallowed as `tracing::warn`. The run's exit code
/// reflects the run, never the sweep — auto-sweep is opportunistic by
/// contract.
///
/// Skipped when `applies_to = []` (sweep disabled by config) or when the
/// state store cannot be opened (lock contention, missing file mid-run).
/// In the disabled case we never stamp `last_retention_sweep_at` so
/// flipping `applies_to` back on triggers a sweep next run regardless of
/// recency.
async fn auto_sweep_retention_at_end_of_run(
    state_path: &Path,
    policy: &rocky_core::retention::StateRetentionConfig,
) {
    // JSONL trace sweep (Arc 4 span retention) runs first and is
    // independent of the redb-backed retention policy:
    //   * not gated by `applies_to.is_empty()` (state-retention disabled
    //     doesn't imply trace-retention disabled),
    //   * not gated by `state_path.exists()` (fresh project still emits
    //     trace files via the JSONL fmt layer),
    //   * not gated by the `sweep_interval_seconds` window (trace files
    //     are cheap to enumerate and `ROCKY_TRACE_RETAIN_RUNS` is a
    //     fixed file-count budget, not a time budget).
    //
    // Honors `ROCKY_TRACE_DISABLE=1` by skipping the sweep — when the
    // JSONL layer is off there's nothing to retain.
    let traces_deleted = if std::env::var_os(rocky_observe::traces::TRACE_DISABLE_ENV).is_some() {
        0
    } else {
        let keep = rocky_observe::traces::retain_runs_from_env();
        let dir = std::path::Path::new(rocky_observe::traces::TRACE_DIR);
        rocky_observe::traces::sweep_traces(dir, keep)
    };
    if traces_deleted > 0 {
        tracing::debug!(traces_deleted, "auto-sweep: JSONL trace files removed");
    }

    if policy.applies_to.is_empty() {
        // State-retention sweep disabled by config. Don't stamp the
        // timestamp — flipping `applies_to` back on should trigger a
        // sweep next run. Trace sweep above has already fired.
        return;
    }

    if !state_path.exists() {
        // Fresh project on an ephemeral runner; nothing to sweep on the
        // redb side. Trace sweep above has already fired.
        return;
    }

    // Safe to open: the inner async block's `state_store` (the writer
    // held during the run body) has dropped by the time the outer
    // `.await` resolves, so re-opening here doesn't deadlock on its
    // exclusive flock.
    let store = match rocky_core::state::StateStore::open(state_path) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(error = %e, path = %state_path.display(),
                "auto-sweep: failed to open state store; skipping");
            return;
        }
    };

    let now = chrono::Utc::now();
    match store.get_last_retention_sweep_at() {
        Ok(Some(last)) => {
            let elapsed = now.signed_duration_since(last);
            let interval = chrono::Duration::seconds(policy.sweep_interval_seconds as i64);
            if elapsed < interval {
                tracing::debug!(
                    last_sweep_at = %last.to_rfc3339(),
                    elapsed_secs = elapsed.num_seconds(),
                    interval_secs = policy.sweep_interval_seconds,
                    "auto-sweep: within configured interval; skipping"
                );
                return;
            }
        }
        Ok(None) => {
            // First run with auto-sweep enabled — proceed.
        }
        Err(e) => {
            tracing::warn!(error = %e,
                "auto-sweep: cannot read last_retention_sweep_at; skipping");
            return;
        }
    }

    let started = std::time::Instant::now();
    let mut report = match store.sweep_retention(policy) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, "auto-sweep: sweep_retention failed");
            // Do not stamp the timestamp — a sweep that never landed
            // shouldn't reset the interval gate.
            return;
        }
    };
    // Carry the trace-file delete count from the JSONL sweep above into
    // the SweepReport for parity with `runs_deleted`/`lineage_deleted`/
    // `audit_deleted`. The redb sweep itself never touches JSONL files,
    // so we splice the count in at the CLI layer where both sweeps are
    // observed.
    report.traces_deleted = traces_deleted;

    let elapsed_ms = started.elapsed().as_millis() as u64;
    if elapsed_ms > policy.sweep_budget_ms {
        tracing::warn!(
            elapsed_ms,
            budget_ms = policy.sweep_budget_ms,
            overrun_ms = elapsed_ms.saturating_sub(policy.sweep_budget_ms),
            runs_deleted = report.runs_deleted,
            lineage_deleted = report.lineage_deleted,
            audit_deleted = report.audit_deleted,
            traces_deleted = report.traces_deleted,
            "auto-sweep: budget exceeded; consider running `rocky state retention sweep` \
             out-of-band or shortening max_age_days"
        );
    } else {
        tracing::debug!(
            elapsed_ms,
            budget_ms = policy.sweep_budget_ms,
            runs_deleted = report.runs_deleted,
            lineage_deleted = report.lineage_deleted,
            audit_deleted = report.audit_deleted,
            traces_deleted = report.traces_deleted,
            "auto-sweep: complete"
        );
    }

    // Stamp regardless of overrun — otherwise a consistently-slow sweep
    // would fire every run and degrade the user experience further.
    if let Err(e) = store.set_last_retention_sweep_at(now) {
        tracing::warn!(error = %e,
            "auto-sweep: failed to stamp last_retention_sweep_at");
    }
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
    // Honour `[cache.schemas]` for source-schema loading during compile.
    schema_cache_config: &rocky_core::config::SchemaCacheConfig,
    // When true, issue `CREATE SCHEMA IF NOT EXISTS` for each unique
    // model target schema before executing models. Mirrors the per-source
    // pre-create on the replication path. Caller plumbs
    // `pipeline.target.governance.auto_create_schemas` here; defaults to
    // `false` for the model-only entry point that has no pipeline context.
    auto_create_schemas: bool,
) -> Result<()> {
    info!(models_dir = %models_dir.display(), "compiling and executing transformation models");

    // Load the persisted schema cache directly from the live
    // `StateStore` — no round-trip through
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

    // Pre-create target schemas when the caller requested auto-create.
    // Mirrors the replication path's per-source loop (see the
    // `governance.auto_create_schemas` block earlier in this file). Without
    // this, transformation models targeting a non-existent schema fail at
    // execute time with "Schema with name X does not exist".
    if auto_create_schemas {
        let mut targets: std::collections::BTreeSet<(String, String)> =
            std::collections::BTreeSet::new();
        for model in &compile_result.project.models {
            targets.insert((
                model.config.target.catalog.clone(),
                model.config.target.schema.clone(),
            ));
        }
        for (catalog, schema) in &targets {
            if let Some(sql_result) = dialect.create_schema_sql(catalog, schema) {
                let sql = sql_result.map_err(anyhow::Error::from)?;
                warehouse
                    .execute_statement(&sql)
                    .await
                    .map_err(anyhow::Error::from)?;
            }
        }
    }

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

    // Intra-layer concurrency is strictly opt-in via `--parallel N`.
    // Kahn's layering guarantees no dependencies within a layer, so every
    // model in a layer can run concurrently; the per-layer barrier (await
    // the whole layer before the next) preserves cross-layer dependency
    // ordering. DuckDB and any adapter that reports
    // `supports_concurrent_execution() == false` is forced serial because
    // its single connection just serializes the work anyway.
    let requested_parallel = (partition_opts.parallel as usize).max(1);
    let adapter_supports_concurrency = warehouse.supports_concurrent_execution();

    for layer in &compile_result.project.layers {
        // Collect this layer's matched models (honouring the name filter),
        // tagged with their stable within-layer index so concurrent results
        // can be re-ordered to match serial output exactly.
        let matched: Vec<(usize, &String, &rocky_core::models::Model)> = layer
            .iter()
            .filter(|name| model_name_filter.is_none_or(|target| target == name.as_str()))
            .filter_map(|name| compile_result.project.model(name).map(|m| (name, m)))
            .enumerate()
            .map(|(idx, (name, model))| (idx, name, model))
            .collect();

        // Eligible for concurrent execution only when the user opted in,
        // the adapter supports it, and every matched model is a "plain"
        // single-statement strategy. The two special paths
        // (`content_addressed`, `time_interval`) stay serial — they push to
        // `output` inline and `time_interval` already self-parallelizes
        // per-partition. A layer mixing strategies runs entirely serially.
        let can_parallelize = requested_parallel > 1
            && adapter_supports_concurrency
            && !matched.is_empty()
            && matched.iter().all(|(_, _, model)| is_plain_strategy(model));

        if can_parallelize {
            // Fire before_model_run for every model up front, in index
            // order, so subscribers see the same announce ordering the
            // serial loop emits.
            if let (Some(reg), Some(pipe)) = (hook_registry, pipeline_name) {
                for (_, name, _) in &matched {
                    let _ = reg
                        .fire(&HookContext::before_model_run(run_id, pipe, name))
                        .await;
                }
            }

            // Fan out the plain models, bounded by `--parallel`. We use
            // `futures::stream::buffer_unordered` (not `JoinSet`/`spawn`) so
            // the per-model futures don't need to be `Send + 'static` — they
            // borrow `warehouse` / `dialect` / `exec_ctx` and are polled in
            // this same task, exactly like the time_interval per-partition
            // path. Each result carries its within-layer index for ordering.
            use futures::stream::StreamExt;
            let concurrency = requested_parallel.min(matched.len());
            // Drive the stream over owned `usize` indices and index back
            // into `matched` inside the closure. Carrying borrows (`&Model`)
            // as stream *items* (rather than closure captures) trips
            // `buffer_unordered`'s higher-ranked `Send` bound and breaks the
            // boxed `run()` future — mirror the time_interval path, whose
            // stream items are owned.
            let matched_ref = &matched;
            let mut layer_results: Vec<(usize, &String, Result<MaterializationOutput>)> =
                futures::stream::iter(0..matched.len())
                    .map(|idx| async move {
                        let (_, name, model) = matched_ref[idx];
                        let model_start = Instant::now();
                        let r = execute_one_plain_model(
                            model,
                            warehouse,
                            dialect,
                            name,
                            model_start,
                            exec_ctx,
                        )
                        .await;
                        (idx, name, r)
                    })
                    .buffer_unordered(concurrency)
                    .collect()
                    .await;

            // Re-order by within-layer index so the JSON output and
            // per-model result vectors are deterministic regardless of which
            // future completed first under the concurrent scheduler.
            layer_results.sort_by_key(|(idx, _, _)| *idx);

            // Apply results in deterministic order: push successes to
            // `output`, fire after_model_run, and surface the first failure.
            // We collect ALL results before bailing so in-flight models in
            // this layer complete cleanly rather than being cancelled — the
            // same tradeoff the time_interval per-partition path makes. The
            // per-layer barrier means a failure here stops the run before
            // any later (dependent) layer starts, preserving the serial
            // path's fail-fast-no-downstream behavior.
            let mut first_error: Option<anyhow::Error> = None;
            for (_, name, result) in layer_results {
                match result {
                    Ok(materialization) => {
                        let model_duration_ms = materialization.duration_ms;
                        output.materializations.push(materialization);
                        if let (Some(reg), Some(pipe)) = (hook_registry, pipeline_name) {
                            let _ = reg
                                .fire(&HookContext::after_model_run(
                                    run_id,
                                    pipe,
                                    name,
                                    model_duration_ms,
                                ))
                                .await;
                        }
                        models_executed += 1;
                    }
                    Err(e) => {
                        if first_error.is_none() {
                            if let (Some(reg), Some(pipe)) = (hook_registry, pipeline_name) {
                                let _ = reg
                                    .fire(&HookContext::model_error(
                                        run_id,
                                        pipe,
                                        name,
                                        &format!("{e:#}"),
                                    ))
                                    .await;
                            }
                            first_error = Some(e);
                        }
                    }
                }
            }

            if let Some(e) = first_error {
                return Err(e);
            }
            continue;
        }

        for &(_, model_name, model) in &matched {
            let model_name: &str = model_name.as_str();

            // §P2.6 per-model emit: before_model_run. Duration is
            // reserved for after_model_run — here we just announce
            // the model is about to execute.
            if let (Some(reg), Some(pipe)) = (hook_registry, pipeline_name) {
                let _ = reg
                    .fire(&HookContext::before_model_run(run_id, pipe, model_name))
                    .await;
            }
            let model_start = Instant::now();

            // Dispatch on the materialization strategy. Three special
            // paths:
            //
            // - `time_interval` takes the per-partition path so the
            //   runtime can iterate partitions, populate the window per
            //   partition, and record state-store rows.
            // - `content_addressed` writes the model's SELECT result
            //   through `rocky_iceberg::uniform_writer` (content-hash
            //   Parquet + Delta log commit) instead of generating SQL.
            // - All other strategies use the single-statement path via
            //   `generate_transformation_sql`.
            if matches!(
                model.config.strategy,
                rocky_core::models::StrategyConfig::ContentAddressed { .. }
            ) {
                let model_ir = model.to_model_ir();
                let model_started_at = Utc::now();
                let target_table_full_name = format!(
                    "{}.{}.{}",
                    model_ir.target.catalog, model_ir.target.schema, model_ir.target.table,
                );
                let asset_key = vec![
                    model_ir.target.catalog.clone(),
                    model_ir.target.schema.clone(),
                    model_ir.target.table.clone(),
                ];
                let summary = super::run_content_addressed::execute_content_addressed_model(
                    &model_ir, warehouse,
                )
                .await
                .with_context(|| format!("model '{model_name}' failed"));
                match summary {
                    Ok(summary) => {
                        let duration_ms = model_start.elapsed().as_millis() as u64;
                        output.materializations.push(MaterializationOutput {
                            asset_key,
                            rows_copied: Some(summary.num_rows as u64),
                            duration_ms,
                            started_at: model_started_at,
                            metadata: MaterializationMetadata {
                                strategy: "content_addressed".to_string(),
                                watermark: None,
                                target_table_full_name: Some(target_table_full_name.clone()),
                                sql_hash: Some(crate::output::sql_fingerprint(
                                    std::slice::from_ref(&model_ir.sql),
                                )),
                                column_count: exec_ctx.column_count_for(model_name),
                                compile_time_ms: exec_ctx.compile_time_ms_for(model_name),
                            },
                            partition: None,
                            cost_usd: None,
                            bytes_scanned: None,
                            bytes_written: Some(summary.size_bytes),
                            job_ids: vec![],
                        });
                        info!(
                            model = model_name,
                            target = target_table_full_name.as_str(),
                            num_rows = summary.num_rows,
                            commit_version = summary.commit_version,
                            blake3 = summary.blake3_hash.as_str(),
                            file_path = summary.file_path.as_str(),
                            "content_addressed model materialized"
                        );
                        // Persist the content-hash artifact to the
                        // `OUTPUT_ARTIFACTS` redb table so Phase 6 VACUUM
                        // refcount can query "which runs touched this
                        // hash?" without re-reading the Delta log. Best-
                        // effort: a failed insert logs + continues
                        // rather than failing the run (the run is
                        // already durable in the Delta log; missing
                        // refcount data is recoverable, an aborted run
                        // is not). TODO(Phase 6): the partitioned write
                        // loop in `execute_content_addressed_model` only
                        // returns the *last* group's hash here — every
                        // group's artifact needs to be recorded for the
                        // refcount sweep to be correct on partitioned
                        // tables. Tracked on the spike memo.
                        if let Some(store) = state_store {
                            let artifact = rocky_core::state::ArtifactRecord {
                                blake3_hash: summary.blake3_hash.clone(),
                                run_id: run_id.to_string(),
                                model_name: model_name.to_string(),
                                file_path: summary.file_path.clone(),
                                commit_version: summary.commit_version,
                                size_bytes: summary.size_bytes,
                                written_at: Utc::now(),
                            };
                            if let Err(e) = store.record_artifact(&artifact) {
                                warn!(
                                    error = %e,
                                    model = model_name,
                                    blake3 = summary.blake3_hash.as_str(),
                                    "failed to persist content-addressed artifact record \
                                     (run still successful; Phase 6 refcount may be incomplete)"
                                );
                            }
                        }
                        if let (Some(reg), Some(pipe)) = (hook_registry, pipeline_name) {
                            let _ = reg
                                .fire(&HookContext::after_model_run(
                                    run_id,
                                    pipe,
                                    model_name,
                                    duration_ms,
                                ))
                                .await;
                        }
                        models_executed += 1;
                        continue;
                    }
                    Err(e) => {
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
                }
            }
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
                if let Err(ref e) = time_interval_res
                    && let (Some(reg), Some(pipe)) = (hook_registry, pipeline_name)
                {
                    let _ = reg
                        .fire(&HookContext::model_error(
                            run_id,
                            pipe,
                            model_name,
                            &format!("{e:#}"),
                        ))
                        .await;
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

            // Plain single-statement strategy: delegate to the shared
            // helper so the serial and intra-layer concurrent paths produce
            // identical output by construction.
            match execute_one_plain_model(
                model,
                warehouse,
                dialect,
                model_name,
                model_start,
                exec_ctx,
            )
            .await
            {
                Ok(materialization) => {
                    let model_duration_ms = materialization.duration_ms;
                    // Push a MaterializationOutput so `rocky cost` and
                    // downstream consumers see derived models in the run
                    // output instead of having to infer them from check /
                    // drift records. Mirrors the time_interval and
                    // replication paths.
                    output.materializations.push(materialization);
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
                Err(e) => {
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
            }
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
        MaterializationStrategy::View => "view",
        MaterializationStrategy::MaterializedView => "materialized_view",
        MaterializationStrategy::DynamicTable { .. } => "dynamic_table",
        MaterializationStrategy::TimeInterval { .. } => "time_interval",
        MaterializationStrategy::Ephemeral => "ephemeral",
        MaterializationStrategy::DeleteInsert { .. } => "delete_insert",
        MaterializationStrategy::Microbatch { .. } => "microbatch",
        MaterializationStrategy::ContentAddressed { .. } => "content_addressed",
    }
}

/// True when `model` uses a "plain" single-statement transformation
/// strategy — i.e. anything except the two special paths
/// (`content_addressed`, `time_interval`) that the serial loop handles
/// inline. Only plain models are eligible for intra-layer concurrent
/// execution via [`execute_one_plain_model`].
fn is_plain_strategy(model: &rocky_core::models::Model) -> bool {
    !matches!(
        model.config.strategy,
        rocky_core::models::StrategyConfig::ContentAddressed { .. }
            | rocky_core::models::StrategyConfig::TimeInterval { .. }
    )
}

/// Execute exactly one "plain" single-statement transformation model:
/// bootstrap a MERGE target if missing, generate SQL, run the statements
/// in order, and build the resulting [`MaterializationOutput`].
///
/// Pure with respect to outer state — touches only the warehouse, never
/// `RunOutput` or hooks directly. Both the serial loop and the
/// intra-layer concurrent path in [`execute_models`] call this, so the
/// success output is identical by construction regardless of which path
/// ran the model. Pulled out (mirroring [`run_one_partition`]) so it can
/// be driven by `futures::stream::buffer_unordered` for `--parallel N`.
///
/// The returned error is already `.context("model '<name>' failed")`-
/// wrapped, matching the serial path's error shape.
async fn execute_one_plain_model(
    model: &rocky_core::models::Model,
    warehouse: &dyn rocky_core::traits::WarehouseAdapter,
    dialect: &dyn rocky_core::traits::SqlDialect,
    model_name: &str,
    model_start: Instant,
    // Taken by value (`ExecutionContext` is `Copy`) rather than by
    // reference so the concurrent-path closure can capture it without a
    // higher-ranked `for<'a> &'a ExecutionContext: Send` borrow that
    // breaks the `Send` bound on the boxed `run()` future.
    exec_ctx: ExecutionContext<'_>,
) -> Result<MaterializationOutput> {
    let model_ir = model.to_model_ir();
    let target_ref = dialect
        .format_table_ref(
            &model_ir.target.catalog,
            &model_ir.target.schema,
            &model_ir.target.table,
        )
        .map_err(anyhow::Error::from)?;

    // MERGE requires the target to exist before the statement runs.
    // sql_gen exposes `generate_transformation_initial_ddl` for
    // exactly this purpose but no call site has been wiring it up,
    // so MERGE on a fresh target failed with "table not found".
    // Mirror the time_interval bootstrap pattern: probe via
    // describe_table().is_ok() and run the initial DDL if missing.
    if matches!(
        model_ir.materialization,
        rocky_ir::MaterializationStrategy::Merge { .. }
    ) {
        let target_table_struct = rocky_ir::TableRef {
            catalog: model_ir.target.catalog.clone(),
            schema: model_ir.target.schema.clone(),
            table: model_ir.target.table.clone(),
        };
        if warehouse
            .describe_table(&target_table_struct)
            .await
            .is_err()
        {
            let initial_ddls =
                rocky_core::sql_gen::generate_transformation_initial_ddl(&model_ir, dialect)?;
            info!(
                model = model_name,
                target = target_ref.as_str(),
                statements = initial_ddls.len(),
                "merge target does not exist — bootstrapping empty table from model schema"
            );
            for ddl in &initial_ddls {
                warehouse.execute_statement(ddl).await.map_err(|e| {
                    anyhow::Error::from(e).context(format!(
                        "bootstrap of '{target_ref}' for model '{model_name}' failed"
                    ))
                })?;
            }
        }
    }

    // Thread the Snowflake compute warehouse (when applicable) so
    // `DynamicTable` strategies can emit `WAREHOUSE = …`. Other
    // adapters return `None` and the helper short-circuits.
    let exec_stmts = rocky_core::sql_gen::generate_transformation_sql_with_warehouse(
        &model_ir,
        dialect,
        warehouse.warehouse_name(),
    )?;

    info!(
        model = model_name,
        target = target_ref.as_str(),
        statements = exec_stmts.len(),
        "executing model"
    );
    // Capture per-statement stats so derived models report the
    // same `bytes_scanned` / `bytes_written` shape that replication
    // tables already have.
    let model_started_at = Utc::now();
    let mut bytes_scanned_acc: Option<u64> = None;
    let mut bytes_written_acc: Option<u64> = None;
    let mut job_ids_acc: Vec<String> = Vec::new();
    for exec_sql in &exec_stmts {
        match warehouse.execute_statement_with_stats(exec_sql).await {
            Ok(stats) => {
                bytes_scanned_acc = accumulate_bytes(bytes_scanned_acc, stats.bytes_scanned);
                bytes_written_acc = accumulate_bytes(bytes_written_acc, stats.bytes_written);
                if let Some(jid) = stats.job_id {
                    job_ids_acc.push(jid);
                }
            }
            Err(e) => {
                return Err(anyhow::Error::from(e).context(format!("model '{model_name}' failed")));
            }
        }
    }

    let model_duration_ms = model_start.elapsed().as_millis() as u64;
    let target_table_full_name = format!(
        "{}.{}.{}",
        model_ir.target.catalog, model_ir.target.schema, model_ir.target.table
    );
    let asset_key = vec![
        model_ir.target.catalog.clone(),
        model_ir.target.schema.clone(),
        model_ir.target.table.clone(),
    ];
    Ok(MaterializationOutput {
        asset_key,
        rows_copied: None,
        duration_ms: model_duration_ms,
        started_at: model_started_at,
        metadata: MaterializationMetadata {
            strategy: transformation_strategy_name(&model_ir.materialization).to_string(),
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
        job_ids: job_ids_acc,
    })
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
    let target_ref_struct = rocky_ir::TableRef {
        catalog: model.config.target.catalog.clone(),
        schema: model.config.target.schema.clone(),
        table: model.config.target.table.clone(),
    };
    let target_exists = warehouse.describe_table(&target_ref_struct).await.is_ok();
    if !target_exists {
        let bootstrap_ir = model.to_model_ir();
        let bootstrap_sql = sql_gen::generate_time_interval_bootstrap_sql(&bootstrap_ir, dialect)
            .with_context(|| {
            format!("failed to render bootstrap SQL for model '{model_name}'")
        })?;
        info!(
            model = model_name,
            target = target_table.as_str(),
            "target table does not exist — bootstrapping empty table from model schema"
        );
        warehouse
            .execute_statement(&bootstrap_sql)
            .await
            .map_err(|e| {
                anyhow::Error::from(e).context(format!(
                    "bootstrap of '{target_table}' failed for model '{model_name}'"
                ))
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
            outcome: Err(anyhow::Error::from(e).context(format!(
                "failed to record InProgress for {model_name}|{key}"
            ))),
        };
    }

    // Build the per-partition IR: take the base ModelIr and inject the
    // PartitionWindow into the TimeInterval strategy.
    let mut tplan_ir = model.to_model_ir();
    if let MaterializationStrategy::TimeInterval { window, .. } = &mut tplan_ir.materialization {
        *window = Some(partition_plan.window.clone());
    }

    // Generate SQL — this is where dialect.insert_overwrite_partition() gets
    // called and the @start_date / @end_date placeholders are substituted
    // (Phase 2D).
    let stmts = match sql_gen::generate_transformation_sql(&tplan_ir, dialect) {
        Ok(s) => s,
        Err(e) => {
            return PartitionExecutionResult {
                partition_key: key.clone(),
                outcome: Err(anyhow::Error::from(e)
                    .context(format!("SQL gen failed for {model_name}|{key}"))),
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
    // Accumulate warehouse-reported bytes across all statements. Snowflake
    // and DuckDB emit 4 statements (BEGIN / DELETE / INSERT / COMMIT);
    // BigQuery emits a single semicolon-joined script (its stateless REST
    // API rejects standalone BEGIN/COMMIT calls); Databricks emits one
    // `INSERT INTO ... REPLACE WHERE`. Each statement potentially reports
    // its own `totalBytesBilled` — summing gives the partition-total cost
    // input. Non-BQ adapters return `None` from the default trait method,
    // so `bytes_acc` stays `None` and `compute_observed_cost_usd` falls
    // through to the duration-based branch.
    let mut exec_err: Option<anyhow::Error> = None;
    let mut bytes_scanned_acc: Option<u64> = None;
    let mut bytes_written_acc: Option<u64> = None;
    let mut job_ids_acc: Vec<String> = Vec::new();
    for stmt in &stmts {
        match warehouse.execute_statement_with_stats(stmt).await {
            Ok(stats) => {
                bytes_scanned_acc = accumulate_bytes(bytes_scanned_acc, stats.bytes_scanned);
                bytes_written_acc = accumulate_bytes(bytes_written_acc, stats.bytes_written);
                if let Some(jid) = stats.job_id {
                    job_ids_acc.push(jid);
                }
            }
            Err(e) => {
                exec_err = Some(anyhow::Error::from(e));
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
            outcome: Err(anyhow::Error::from(e)
                .context(format!("failed to record Computed for {model_name}|{key}"))),
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
            job_ids: job_ids_acc,
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

/// Maps a `ReplicationPipelineConfig.strategy` string onto a
/// `MaterializationStrategy` variant for the replication runner.
///
/// `"incremental"` → `Incremental { timestamp_column }`.
/// `"merge"` → `Merge { unique_key, update_columns: ColumnSelection::All }`,
///   reading keys from `resolved_merge_keys()` (`merge_keys` falls back to
///   `merge_keys_fallback`). `merge` requires keys; absence is treated as
///   a defensive `anyhow!` because `validate_replication_strategies` should
///   have rejected it at config load time. The `ColumnSelection::All`
///   placeholder is replaced by the explicit source-schema column list in
///   `process_table` BEFORE the IR reaches SQL-gen — Snowflake's MERGE
///   rejects `UPDATE SET *`, so threading discovered columns is required for
///   adapter parity. This function stays warehouse-free; the resolution
///   happens at the call site where `describe_table(&source_table)` is
///   already awaited for drift detection.
/// Anything else → `FullRefresh` (backwards-compatible fallback; see follow-up
///   note in PR body about tightening unknown strategy values).
///
/// Note: the watermark-filter clause (`WHERE ts > TIMESTAMP '<prior>'`, where
/// `<prior>` is the previous run's recorded watermark) lives in
/// `sql_gen::generate_select_sql` and applies for `Incremental` only; `merge`
/// upserts the entire source via the dialect-specific `MERGE INTO`. After each
/// incremental/microbatch tick the runner records `MAX(ts) FROM target` (via
/// [`query_target_max_timestamp`] / [`resolve_new_watermark`]) — the max among
/// the rows actually loaded — into `DeferredWatermark`. Reading the target
/// rather than re-querying the source closes a TOCTOU where a row appended to
/// source after the INSERT's snapshot would otherwise advance the watermark
/// past a row that was never loaded.
///
/// Convenience wrapper used by unit tests — applies no per-table
/// override. Production code paths go through
/// [`build_replication_strategy_with_override`] so per-table overrides
/// land.
#[cfg(test)]
fn build_replication_strategy(
    pipeline: &ReplicationPipelineConfig,
) -> Result<MaterializationStrategy> {
    build_replication_strategy_with_override(pipeline, &ResolvedTableOverride::default())
}

/// Build a materialization strategy applying any per-table override on
/// top of pipeline defaults. Per-field most-specific-match-wins
/// resolution has already produced `override_` at the connector-loop
/// level; this function consumes the merged result with pipeline
/// fallback.
///
/// T3 fail-fast lives here — an effective `strategy = "merge"` with
/// no reachable merge_keys returns an error so the table fails (the
/// pipeline keeps running and other tables continue).
fn build_replication_strategy_with_override(
    pipeline: &ReplicationPipelineConfig,
    override_: &ResolvedTableOverride,
) -> Result<MaterializationStrategy> {
    let effective_strategy = override_
        .strategy
        .as_deref()
        .unwrap_or(pipeline.strategy.as_str());
    let effective_timestamp = override_
        .timestamp_column
        .as_deref()
        .unwrap_or(pipeline.timestamp_column.as_str());

    match effective_strategy {
        "incremental" => Ok(MaterializationStrategy::Incremental {
            timestamp_column: effective_timestamp.to_string(),
        }),
        "merge" => {
            // Resolve merge keys with per-field inheritance.
            // Effective merge_keys: override > pipeline.merge_keys >
            // override.merge_keys_fallback > pipeline.merge_keys_fallback.
            let effective_keys: Option<Vec<String>> = override_
                .merge_keys
                .clone()
                .or_else(|| pipeline.merge_keys.clone())
                .or_else(|| override_.merge_keys_fallback.clone())
                .or_else(|| pipeline.merge_keys_fallback.clone());
            let keys = effective_keys.filter(|k| !k.is_empty()).ok_or_else(|| {
                anyhow::anyhow!(
                    "table override produces strategy = \"merge\" with empty effective \
                     merge_keys — set merge_keys on the override or the pipeline default \
                     (this guard catches a residual not caught at parse time)"
                )
            })?;
            let unique_key: Vec<Arc<str>> = keys.iter().map(|k| Arc::from(k.as_str())).collect();
            Ok(MaterializationStrategy::Merge {
                unique_key,
                update_columns: ColumnSelection::All,
            })
        }
        "view" => Ok(MaterializationStrategy::View),
        "materialized_view" => Ok(MaterializationStrategy::MaterializedView),
        // `dynamic_table` requires a target_lag specifier — the
        // replication pipeline doesn't currently expose one, so we
        // surface a clear error rather than silently dropping to
        // full-refresh.
        "dynamic_table" => Err(anyhow::anyhow!(
            "dynamic_table strategy is not configurable from a replication pipeline \
             without a target_lag specifier — declare it on a transformation model's \
             sidecar TOML instead"
        )),
        _ => Ok(MaterializationStrategy::FullRefresh),
    }
}

/// Resolves `ColumnSelection::All` on a `Merge` strategy against the
/// discovered source schema so the SQL-gen layer can emit an explicit
/// `UPDATE SET t.col1 = s.col1, ...` clause.
///
/// Returns the input strategy untouched when:
/// - the strategy is not `Merge`, OR
/// - `source_cols` is empty (prefetch miss + describe failure).
///
/// The explicit form mirrors `UPDATE SET *` on Databricks/DuckDB: every
/// source column plus every metadata column projected by SELECT. Merge
/// keys are included — `t.id = s.id` under `ON t.id = s.id` is a no-op
/// and matches `*` semantics on the other adapters; excluding them would
/// be a behavior change.
///
/// When `source_cols` is empty, the strategy is returned with its existing
/// `ColumnSelection::All` so other adapters preserve current behavior and
/// Snowflake's dialect still surfaces its clear "Snowflake MERGE does not
/// support UPDATE SET *" error.
fn resolve_merge_update_columns(
    strategy: &MaterializationStrategy,
    source_cols: &[ColumnInfo],
    metadata_columns: &[MetadataColumn],
) -> MaterializationStrategy {
    let MaterializationStrategy::Merge {
        unique_key,
        update_columns: _,
    } = strategy
    else {
        return strategy.clone();
    };
    if source_cols.is_empty() {
        return strategy.clone();
    }
    let mut cols: Vec<Arc<str>> = source_cols
        .iter()
        .map(|c| Arc::from(c.name.as_str()))
        .collect();
    for m in metadata_columns {
        cols.push(Arc::from(m.name.as_str()));
    }
    MaterializationStrategy::Merge {
        unique_key: unique_key.clone(),
        update_columns: ColumnSelection::Explicit(cols),
    }
}

/// `MAX(<timestamp_column>) FROM <target>` — the post-execute watermark for the
/// replication path. Reads the **target** table so the recorded value reflects
/// exactly what was loaded (closing the source-side TOCTOU described on
/// [`resolve_new_watermark`]). Returns `None` on NULL / empty / failure so the
/// caller can keep the prior watermark.
async fn query_target_max_timestamp(
    warehouse: &dyn WarehouseAdapter,
    dialect: &dyn rocky_core::traits::SqlDialect,
    target: &TableRef,
    timestamp_column: &str,
) -> Option<chrono::DateTime<Utc>> {
    if rocky_sql::validation::validate_identifier(timestamp_column).is_err() {
        return None;
    }
    let target_ref = dialect
        .format_table_ref(&target.catalog, &target.schema, &target.table)
        .ok()?;
    let sql = format!("SELECT MAX({timestamp_column}) FROM {target_ref}");

    let result = match warehouse.execute_query(&sql).await {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(
                table = target.full_name(),
                error = %e,
                "target-side MAX(ts) query failed — keeping prior watermark"
            );
            return None;
        }
    };

    result.rows.first().and_then(|r| r.first()).and_then(|v| {
        v.as_str().and_then(|s| {
            s.parse::<chrono::DateTime<Utc>>().ok().or_else(|| {
                chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
                    .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S"))
                    .ok()
                    .map(|naive| naive.and_utc())
            })
        })
    })
}

/// Resolve the watermark to persist after copying into `target_table`.
///
/// # The TOCTOU this fixes (M2)
///
/// The incremental INSERT filters `WHERE source.{ts} > prior_watermark` against
/// the source's snapshot, then a separate later query recorded the new
/// watermark. If that later query read `MAX(ts) FROM source`, any row appended
/// to source *between* the INSERT's snapshot and the MAX query would advance the
/// watermark past rows the INSERT never loaded — a permanent skip on the next
/// strict-`>` run.
///
/// On the replication path the timestamp column is identity-mapped source →
/// target (a `SELECT *` / `ColumnSelection::All` passthrough copy, with all
/// transformation strategies rejected upstream), so reading `MAX(ts) FROM
/// target` records exactly the max timestamp among the rows that were actually
/// loaded — no loss, and no spurious reprocessing of source rows that weren't.
///
/// # Empty-target handling
///
/// `MAX` over an empty / freshly-truncated target returns NULL. On the
/// incremental path `now` is NEVER a safe watermark — it is later than every
/// real row and would skip any source row written after this run's snapshot.
/// So a NULL / unparseable / failed target MAX falls back to the **prior
/// watermark** (unchanged). When there is no prior watermark either (a true
/// first run that loaded nothing), it falls back to the epoch sentinel
/// (`1970-01-01`), which is "no progress" — the next run re-scans the whole
/// source rather than advancing past unloaded rows.
async fn resolve_new_watermark(
    strategy: &MaterializationStrategy,
    warehouse: &dyn WarehouseAdapter,
    dialect: &dyn rocky_core::traits::SqlDialect,
    target_table: &TableRef,
    timestamp_column: &str,
    prior_watermark: Option<chrono::DateTime<Utc>>,
    now: chrono::DateTime<Utc>,
) -> chrono::DateTime<Utc> {
    let advances_watermark = matches!(
        strategy,
        MaterializationStrategy::Incremental { .. } | MaterializationStrategy::Microbatch { .. }
    );
    if advances_watermark {
        // Record what was actually loaded (MAX over the target). On an empty
        // target / NULL / failure, keep the prior watermark — never `now`,
        // which would skip rows written to source after this run's snapshot.
        // With no prior either, fall back to the epoch sentinel = no progress.
        match query_target_max_timestamp(warehouse, dialect, target_table, timestamp_column).await {
            Some(target_max) => target_max,
            None => prior_watermark.unwrap_or_else(epoch_watermark_sentinel),
        }
    } else {
        now
    }
}

/// The "no progress" watermark sentinel (`1970-01-01T00:00:00Z`). Recorded when
/// an incremental run loaded nothing and has no prior watermark, so the next
/// run re-scans the whole source rather than advancing past unloaded rows.
fn epoch_watermark_sentinel() -> chrono::DateTime<Utc> {
    chrono::DateTime::<Utc>::from_timestamp(0, 0).unwrap_or_else(Utc::now)
}

/// Processes a single table: drift detection + replication.
///
/// Uses `WarehouseAdapter` for all SQL execution and schema introspection,
/// making this function adapter-agnostic (Databricks, Snowflake, BigQuery,
/// DuckDB). Tagging and watermark updates are returned as deferred
/// operations and applied in a post-run batch phase for better concurrency.
///
/// ## Source-side watermark semantics
///
/// For incremental replication, the runner reads the previous run's
/// `MAX({timestamp_column}) FROM source` from the state store **before**
/// SQL gen — that value bounds the WHERE clause as a literal. **After** a
/// successful execute, the runner issues a fresh
/// `SELECT MAX({timestamp_column}) FROM source` query and records the
/// result as the new watermark (replaces the older `Utc::now()` semantics) —
/// including on the bootstrap full-refresh run of an incremental table, so the
/// first incremental tick starts from the real data max. Falls back to
/// `Utc::now()` only when the query returns no parseable timestamp (empty
/// source, non-incremental strategies). See `resolve_new_watermark` for the
/// per-strategy dispatch. The watermark reflects what's been processed *from*
/// source — not what's *in* target.
#[tracing::instrument(skip_all, fields(table = %task.table_name))]
async fn process_table(
    warehouse: &dyn WarehouseAdapter,
    state: &Mutex<StateStore>,
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
        let drift_result = drift::detect_drift(&target_table, &source_cols, &target_cols, dialect);
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
                .map_err(anyhow::Error::from)?;
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
        } else if drift_result.action == DriftAction::AlterColumnTypes {
            // Every drifted column passed the dialect's safe-widening
            // check. Issue `ALTER TABLE ALTER COLUMN ... TYPE ...` (or
            // BigQuery's `SET DATA TYPE` form) for each before the
            // INSERT continues — target keeps its existing rows with
            // the values reinterpreted under the new type.
            info!(
                table = target_table.full_name(),
                drifted = drift_result.drifted_columns.len(),
                "drift detected, widening {} column type(s)",
                drift_result.drifted_columns.len()
            );
            let alter_stmts = drift::generate_alter_column_sql(
                &target_table,
                &drift_result.drifted_columns,
                dialect,
            )?;
            for stmt in &alter_stmts {
                warehouse
                    .execute_statement(stmt)
                    .await
                    .map_err(anyhow::Error::from)?;
            }
            // If the same drift round also surfaced added columns,
            // apply them as part of the same schema-evolution step
            // before the INSERT runs.
            if !drift_result.added_columns.is_empty() {
                let add_stmts = drift::generate_add_column_sql(
                    &target_table,
                    &drift_result.added_columns,
                    dialect,
                )?;
                for stmt in &add_stmts {
                    warehouse
                        .execute_statement(stmt)
                        .await
                        .map_err(anyhow::Error::from)?;
                }
            }
            let reason = drift_result
                .drifted_columns
                .iter()
                .map(|c| {
                    format!(
                        "column '{}' widened {} → {}",
                        c.name, c.target_type, c.source_type
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            drift_action = Some(DriftActionOutput {
                table: target_table.full_name(),
                action: "alter_column_types".into(),
                reason,
            });
        } else if !drift_result.added_columns.is_empty() {
            // Source added columns the target doesn't have. Without an
            // ALTER, the next `INSERT INTO target SELECT * FROM source`
            // produces more columns than the target accepts and BQ /
            // Snowflake / Databricks all reject it. Issue
            // `ALTER TABLE ADD COLUMN <name> <type>` for each new
            // column before the regular INSERT path runs. New columns
            // are nullable by default — historical rows stay NULL,
            // newly inserted rows pick up source values via SELECT *.
            info!(
                table = target_table.full_name(),
                added = drift_result.added_columns.len(),
                "drift detected, adding {} column(s) to target",
                drift_result.added_columns.len()
            );
            let alter_stmts = drift::generate_add_column_sql(
                &target_table,
                &drift_result.added_columns,
                dialect,
            )?;
            for stmt in &alter_stmts {
                warehouse
                    .execute_statement(stmt)
                    .await
                    .map_err(anyhow::Error::from)?;
            }
            let reason = drift_result
                .added_columns
                .iter()
                .map(|c| format!("added column '{}' ({})", c.name, c.data_type))
                .collect::<Vec<_>>()
                .join(", ");
            drift_action = Some(DriftActionOutput {
                table: target_table.full_name(),
                action: "add_columns".into(),
                reason,
            });
        }
    }

    // Build replication IR directly. Applies any per-table override
    // already resolved at the connector-loop level — see
    // `TableTask::effective_override`.
    let strategy = build_replication_strategy_with_override(pipeline, &task.effective_override)?;
    // Effective timestamp column after per-table override. Mirrors the same
    // override > pipeline-default precedence used inside
    // `build_replication_strategy_with_override` so the strategy's WHERE
    // clause and the post-execute `query_target_max_timestamp` call below
    // both operate on the same column — if an override changes the column,
    // the recorded watermark stays aligned with the next run's filter.
    let effective_timestamp_column = task
        .effective_override
        .timestamp_column
        .as_deref()
        .unwrap_or(pipeline.timestamp_column.as_str())
        .to_string();

    // For `strategy = "merge"`, resolve `ColumnSelection::All` against the
    // discovered source schema BEFORE handing the IR to SQL-gen. Snowflake's
    // MERGE rejects `UPDATE SET *` (Databricks / BigQuery / DuckDB accept it),
    // so emitting the literal `*` from the replication runner makes
    // Snowflake-merge unusable. We already awaited `describe_table(&source_table)`
    // above for drift detection; `source_cols` is in scope and free. Reuse it.
    let resolved_strategy =
        resolve_merge_update_columns(&strategy, &source_cols, &task.metadata_columns);

    let model_ir = ModelIr::replication(
        TargetRef {
            catalog: task.target_catalog.clone(),
            schema: task.target_schema.clone(),
            table: task.table_name.clone(),
        },
        resolved_strategy.clone(),
        SourceRef {
            catalog: task.source_catalog.clone(),
            schema: task.source_schema.clone(),
            table: task.table_name.clone(),
        },
        ColumnSelection::All,
        task.metadata_columns.clone(),
        GovernanceConfig {
            permissions_file: None,
            auto_create_catalogs: pipeline.target.governance.auto_create_catalogs,
            auto_create_schemas: pipeline.target.governance.auto_create_schemas,
        },
    );

    // Source-side watermark: read the previous run's `MAX(ts) FROM source`
    // from state. The dialect-level `watermark_where` substitutes this as
    // a literal (no correlated subquery against target). On a first run /
    // post-`delete_watermark`, `None` falls back to the 1970-01-01 sentinel
    // so the whole source is scanned.
    let prior_watermark: Option<chrono::DateTime<Utc>> = {
        let state_guard = state.lock().await;
        match state_guard.get_watermark(&target_table.state_key()) {
            Ok(Some(wm)) => Some(wm.last_value),
            Ok(None) => None,
            Err(e) => {
                tracing::warn!(
                    table = target_table.full_name(),
                    error = %e,
                    "could not read prior watermark from state — falling back to full scan"
                );
                None
            }
        }
    };

    let strategy_name;
    let sql = if use_full_refresh {
        strategy_name = "full_refresh";
        sql_gen::generate_create_table_as_sql(&model_ir, dialect)?
    } else {
        match &strategy {
            MaterializationStrategy::FullRefresh => {
                strategy_name = "full_refresh";
                sql_gen::generate_create_table_as_sql(&model_ir, dialect)?
            }
            MaterializationStrategy::Incremental { .. } => {
                strategy_name = "incremental";
                sql_gen::generate_insert_sql(&model_ir, dialect, prior_watermark.as_ref())?
            }
            MaterializationStrategy::Merge { .. } => {
                strategy_name = "merge";
                sql_gen::generate_merge_sql(&model_ir, dialect)?
            }
            MaterializationStrategy::View => {
                // Views on a replication table create a thin DDL pointing
                // at the source; no row movement. Only useful when the
                // user wants a "passthrough" replication endpoint.
                strategy_name = "view";
                // Reuse the transformation dispatch — the strategy is the
                // same shape on either pipeline kind. We hand-build the
                // SELECT * FROM <source> here so `generate_view_sql`'s
                // Transformation-variant guard is satisfied via a wrapper.
                let select = dialect.select_clause(
                    model_ir
                        .columns
                        .as_ref()
                        .expect("Replication variant guarantees columns"),
                    &model_ir.metadata_columns,
                )?;
                let source = model_ir
                    .source
                    .as_ref()
                    .expect("Replication variant guarantees source");
                let source_ref =
                    dialect.format_table_ref(&source.catalog, &source.schema, &source.table)?;
                let view_target = dialect.format_table_ref(
                    &model_ir.target.catalog,
                    &model_ir.target.schema,
                    &model_ir.target.table,
                )?;
                let body = format!("{select}\nFROM {source_ref}");
                dialect.view_ddl(&view_target, &body)?
            }
            MaterializationStrategy::MaterializedView => {
                // Dispatch to the dialect's MV generator so DuckDB / Trino
                // fail loud rather than silently emitting plain CTAS.
                strategy_name = "materialized_view";
                let select = dialect.select_clause(
                    model_ir
                        .columns
                        .as_ref()
                        .expect("Replication variant guarantees columns"),
                    &model_ir.metadata_columns,
                )?;
                let source = model_ir
                    .source
                    .as_ref()
                    .expect("Replication variant guarantees source");
                let source_ref =
                    dialect.format_table_ref(&source.catalog, &source.schema, &source.table)?;
                let mv_target = dialect.format_table_ref(
                    &model_ir.target.catalog,
                    &model_ir.target.schema,
                    &model_ir.target.table,
                )?;
                let body = format!("{select}\nFROM {source_ref}");
                dialect.materialized_view_ddl(&mv_target, &body)?
            }
            MaterializationStrategy::DynamicTable { target_lag } => {
                // Snowflake-only on the replication path. Threading the
                // warehouse from the adapter mirrors the transformation
                // dispatch so the SQL is identical regardless of pipeline
                // kind. Non-Snowflake adapters surface the unsupported
                // error from the dialect's default impl.
                strategy_name = "dynamic_table";
                let warehouse_name = warehouse.warehouse_name().ok_or_else(|| {
                    anyhow::anyhow!(
                        "dynamic_table strategy requires a compute warehouse \
                         (Snowflake-only) — current adapter does not expose one"
                    )
                })?;
                let select = dialect.select_clause(
                    model_ir
                        .columns
                        .as_ref()
                        .expect("Replication variant guarantees columns"),
                    &model_ir.metadata_columns,
                )?;
                let source = model_ir
                    .source
                    .as_ref()
                    .expect("Replication variant guarantees source");
                let source_ref =
                    dialect.format_table_ref(&source.catalog, &source.schema, &source.table)?;
                let dt_target = dialect.format_table_ref(
                    &model_ir.target.catalog,
                    &model_ir.target.schema,
                    &model_ir.target.table,
                )?;
                let body = format!("{select}\nFROM {source_ref}");
                dialect.dynamic_table_ddl(&dt_target, &body, target_lag, warehouse_name)?
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
                sql_gen::generate_create_table_as_sql(&model_ir, dialect)?
            }
            MaterializationStrategy::Microbatch { .. } => {
                strategy_name = "microbatch";
                // Microbatch on replication tables falls back to incremental insert.
                sql_gen::generate_insert_sql(&model_ir, dialect, prior_watermark.as_ref())?
            }
            MaterializationStrategy::ContentAddressed { .. } => {
                // content_addressed is a transformation strategy backed by
                // rocky-iceberg::uniform_writer; the replication pipeline
                // does not synthesise this variant from rocky.toml.
                anyhow::bail!(
                    "content_addressed strategy is not supported on replication tables — \
                     it only applies to transformation models"
                );
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
        .map_err(anyhow::Error::from)?;

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

    // Record the watermark this run advanced to. Incremental and microbatch
    // strategies record `MAX(target.ts)` — exactly the max timestamp among the
    // rows actually loaded into the target. Reading the TARGET (not re-querying
    // the source) closes a TOCTOU: a row appended to source after the INSERT's
    // snapshot but before the watermark capture would otherwise advance the
    // watermark past a row that was never loaded, permanently skipping it on
    // the next strict-`>` run. On the replication path the ts column is an
    // identity-mapped passthrough, so target-MAX is in the same value space as
    // source-MAX for the loaded rows. See `resolve_new_watermark`.
    let new_watermark = resolve_new_watermark(
        &strategy,
        warehouse,
        dialect,
        &target_table,
        &effective_timestamp_column,
        prior_watermark,
        now,
    )
    .await;

    let deferred_watermark = Some(DeferredWatermark {
        state_key: target_table.state_key(),
        timestamp: new_watermark,
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
                // Surface the source-side watermark (what's been processed
                // *from* source). Falls back to `now` when the source query
                // returned no parseable timestamp.
                watermark: Some(new_watermark),
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
            job_ids: exec_stats.job_id.clone().into_iter().collect(),
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
            // Classify before stringification so the typed connector
            // variant is preserved on `TableError.failure_kind` (plus
            // the optional warehouse-breaker cooldown hint).
            let (failure_kind, cooldown_seconds) = classify_anyhow_error_with_cooldown(&e);
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
            if let Some(t) = &throttle
                && is_rate_limit_error(&msg)
            {
                t.on_rate_limit();
                adjust_semaphore(t, semaphore, semaphore_capacity);
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
                failure_kind,
                cooldown_seconds,
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
                failure_kind: FailureKind::Unknown,
                cooldown_seconds: None,
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
    fn ir_grants_to_securable_batch_renders_rest_privilege_form() {
        // The IR -> GovernanceCatalogClient boundary must hand Unity's REST
        // permissions endpoint the underscore-delimited privilege form
        // (`USE_CATALOG`), NOT the spaced SQL `GRANT` form (`USE CATALOG`).
        // This pins the conversion the wiremock test cannot reach.
        let grants = vec![
            Grant {
                principal: "engineers".into(),
                permission: Permission::UseCatalog,
                target: GrantTarget::Catalog("hcv2_cat".into()),
            },
            Grant {
                principal: "engineers".into(),
                permission: Permission::Select,
                target: GrantTarget::Catalog("hcv2_cat".into()),
            },
        ];
        let (securable, catalog_grants) =
            ir_grants_to_securable_batch(&grants).expect("catalog batch converts");
        assert_eq!(
            securable,
            Securable::Catalog {
                name: "hcv2_cat".into()
            }
        );
        let privileges: Vec<&str> = catalog_grants
            .iter()
            .map(|g| g.privilege.as_str())
            .collect();
        assert!(
            privileges.contains(&"USE_CATALOG"),
            "multi-word privilege must reach REST as USE_CATALOG, got {privileges:?}"
        );
        assert!(privileges.contains(&"SELECT"));
        assert!(
            !privileges.iter().any(|p| p.contains(' ')),
            "no spaced privilege may leak to the REST endpoint: {privileges:?}"
        );
    }

    #[test]
    fn ir_grants_to_securable_batch_maps_schema_target() {
        let grants = vec![Grant {
            principal: "analysts".into(),
            permission: Permission::UseSchema,
            target: GrantTarget::Schema {
                catalog: "hcv2_cat".into(),
                schema: "hcv2_sch".into(),
            },
        }];
        let (securable, catalog_grants) =
            ir_grants_to_securable_batch(&grants).expect("schema batch converts");
        assert_eq!(
            securable,
            Securable::Schema {
                catalog: "hcv2_cat".into(),
                name: "hcv2_sch".into(),
            }
        );
        assert_eq!(catalog_grants[0].privilege, "USE_SCHEMA");
    }

    #[test]
    fn ir_grants_to_securable_batch_empty_is_none() {
        assert!(ir_grants_to_securable_batch(&[]).is_none());
    }

    #[test]
    fn ir_grants_to_securable_batch_mixed_targets_bail_to_legacy_path() {
        // Defensive: a batch mixing securables routes through the legacy
        // SQL path (returns None) rather than silently dropping grants.
        let grants = vec![
            Grant {
                principal: "a".into(),
                permission: Permission::Select,
                target: GrantTarget::Catalog("hcv2_cat".into()),
            },
            Grant {
                principal: "a".into(),
                permission: Permission::Select,
                target: GrantTarget::Schema {
                    catalog: "hcv2_cat".into(),
                    schema: "hcv2_sch".into(),
                },
            },
        ];
        assert!(ir_grants_to_securable_batch(&grants).is_none());
    }

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
            override_warnings: vec![],
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

    // -----------------------------------------------------------------------
    // Transformation auto_create_schemas — pre-creates target schemas before
    // executing models so a model targeting a fresh schema doesn't fail with
    // "Schema with name X does not exist". Mirrors the per-source pre-create
    // on the replication path (the `governance.auto_create_schemas` block
    // earlier in this file).
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn transformation_auto_create_schemas_materializes_fresh_schema() {
        let tmp = tempfile::TempDir::new().expect("temp dir");
        let config_dir = tmp.path();
        let duckdb_path = config_dir.join("warehouse.duckdb");
        let config_path = config_dir.join("rocky.toml");

        // Transformation pipeline with `auto_create_schemas = true` and a
        // model targeting `mart` — a schema that does not exist in the
        // freshly-created DuckDB file. Without the fix, the model's
        // `CREATE OR REPLACE TABLE warehouse.mart.summary AS SELECT ...`
        // fails with "Schema with name mart does not exist".
        std::fs::write(
            &config_path,
            format!(
                r#"
[adapter]
type = "duckdb"
path = "{}"

[pipeline.t]
type = "transformation"
models = "models/**"

[pipeline.t.target]
adapter = "default"

[pipeline.t.target.governance]
auto_create_schemas = true
"#,
                duckdb_path.display()
            ),
        )
        .expect("write rocky.toml");

        let models_dir = config_dir.join("models");
        std::fs::create_dir(&models_dir).expect("mkdir models");
        std::fs::write(
            models_dir.join("summary.sql"),
            "SELECT 1 AS one, 2 AS two\n",
        )
        .expect("write summary.sql");
        std::fs::write(
            models_dir.join("summary.toml"),
            r#"
[strategy]
type = "full_refresh"

[target]
catalog = "warehouse"
schema = "mart"
"#,
        )
        .expect("write summary.toml");

        let state_path = config_dir.join("state.redb");
        let opts = PartitionRunOptions::default();

        super::run(
            &config_path,
            None,
            Some("t"),
            &state_path,
            None,
            true, // output_json (suppresses pretty stdout in tests)
            None,
            false,
            None,
            false,
            None,
            &opts,
            None,
            None,
            None,
            None,
        )
        .await
        .expect(
            "transformation run with auto_create_schemas=true must succeed even when the \
             target schema doesn't exist yet",
        );
    }

    // -----------------------------------------------------------------
    // Auto-sweep at end-of-run
    // -----------------------------------------------------------------

    #[tokio::test]
    async fn auto_sweep_missing_state_path_is_a_noop() {
        // Ephemeral CI runner: no `state.redb` yet. The helper must
        // exit quietly without creating the file.
        use rocky_core::retention::StateRetentionConfig;
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("state.redb");
        assert!(!path.exists());

        auto_sweep_retention_at_end_of_run(&path, &StateRetentionConfig::default()).await;
        assert!(!path.exists(), "auto-sweep must not create state.redb");
    }

    #[tokio::test]
    async fn auto_sweep_disabled_by_empty_applies_to_does_not_stamp() {
        // applies_to = [] is the documented "disable" knob. We must
        // skip the sweep AND not stamp last_retention_sweep_at — so
        // flipping applies_to back on triggers a sweep next run
        // regardless of recency.
        use rocky_core::retention::StateRetentionConfig;
        use rocky_core::state::StateStore;
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("state.redb");
        {
            let _ = StateStore::open(&path).expect("init");
        }

        let mut policy = StateRetentionConfig::default();
        policy.applies_to.clear();

        auto_sweep_retention_at_end_of_run(&path, &policy).await;

        let store = StateStore::open(&path).expect("reopen");
        assert_eq!(
            store.get_last_retention_sweep_at().unwrap(),
            None,
            "disabled sweep must not stamp last_retention_sweep_at"
        );
    }

    #[tokio::test]
    async fn auto_sweep_first_run_stamps_timestamp() {
        // Fresh state store, default policy: the helper should run
        // the sweep (no-op on an empty history table) and stamp the
        // timestamp.
        use rocky_core::retention::StateRetentionConfig;
        use rocky_core::state::StateStore;
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("state.redb");
        {
            let _ = StateStore::open(&path).expect("init");
        }

        let before = chrono::Utc::now();
        auto_sweep_retention_at_end_of_run(&path, &StateRetentionConfig::default()).await;
        let after = chrono::Utc::now();

        let store = StateStore::open(&path).expect("reopen");
        let stamped = store
            .get_last_retention_sweep_at()
            .unwrap()
            .expect("first auto-sweep must stamp the timestamp");
        assert!(
            stamped >= before && stamped <= after,
            "stamped timestamp {stamped} should sit between {before} and {after}"
        );
    }

    #[tokio::test]
    async fn auto_sweep_within_interval_skips_without_updating_stamp() {
        // Pre-stamp a recent sweep timestamp. A second invocation
        // within the configured interval must be a no-op — the
        // stamp must NOT advance, so the same call once the interval
        // does elapse will fire (if we'd advanced the stamp on every
        // gated call we'd never sweep).
        use rocky_core::retention::StateRetentionConfig;
        use rocky_core::state::StateStore;
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("state.redb");
        let pre_stamp = chrono::Utc::now() - chrono::Duration::seconds(60);
        {
            let store = StateStore::open(&path).expect("init");
            store.set_last_retention_sweep_at(pre_stamp).unwrap();
        }

        // Default interval is 1 hour; pre_stamp is 60s ago — gate fires.
        auto_sweep_retention_at_end_of_run(&path, &StateRetentionConfig::default()).await;

        let store = StateStore::open(&path).expect("reopen");
        let stamped = store.get_last_retention_sweep_at().unwrap().unwrap();
        assert_eq!(
            stamped, pre_stamp,
            "within-interval skip must not update last_retention_sweep_at"
        );
    }

    #[tokio::test]
    async fn auto_sweep_after_interval_runs_and_advances_stamp() {
        // Pre-stamp a sweep timestamp older than the configured
        // interval. The helper should run the sweep and advance the
        // stamp.
        use rocky_core::retention::StateRetentionConfig;
        use rocky_core::state::StateStore;
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("state.redb");
        // Use a tight interval so the test is deterministic without
        // having to fast-forward time.
        let policy = StateRetentionConfig {
            sweep_interval_seconds: 1,
            ..StateRetentionConfig::default()
        };
        let pre_stamp = chrono::Utc::now() - chrono::Duration::seconds(10);
        {
            let store = StateStore::open(&path).expect("init");
            store.set_last_retention_sweep_at(pre_stamp).unwrap();
        }

        auto_sweep_retention_at_end_of_run(&path, &policy).await;

        let store = StateStore::open(&path).expect("reopen");
        let stamped = store.get_last_retention_sweep_at().unwrap().unwrap();
        assert!(
            stamped > pre_stamp,
            "post-interval sweep must advance stamp ({stamped} should be > {pre_stamp})"
        );
    }

    #[tokio::test]
    async fn auto_sweep_does_not_propagate_failure() {
        // The contract: auto-sweep is opportunistic; any failure
        // path is swallowed as `tracing::warn` so the run's exit
        // code reflects the run, not the sweep. We exercise that
        // by pointing at a directory (not a file) that opens as a
        // path the StateStore cannot use as a database file. The
        // helper must return cleanly.
        use rocky_core::retention::StateRetentionConfig;
        let dir = tempfile::TempDir::new().unwrap();
        // Create a sub-directory, then point the state path AT the
        // directory: redb cannot open a directory as a database.
        let blocked = dir.path().join("blocked");
        std::fs::create_dir(&blocked).unwrap();

        // Should return without panicking or propagating.
        auto_sweep_retention_at_end_of_run(&blocked, &StateRetentionConfig::default()).await;
    }

    /// Pipeline TOML fragment used by `build_replication_strategy` tests.
    /// Strategy + merge-key fields are concatenated on top per-case.
    fn pipeline_toml_for_strategy_test() -> &'static str {
        r#"
[adapter.default]
type = "duckdb"
path = "/tmp/x.duckdb"

[pipeline.bronze]
type = "replication"
"#
    }

    fn parse_pipeline(extra: &str) -> ReplicationPipelineConfig {
        let mut toml_str = String::from(pipeline_toml_for_strategy_test());
        toml_str.push_str(extra);
        toml_str.push_str(
            r#"

[pipeline.bronze.source]
catalog = "raw_catalog"

[pipeline.bronze.source.schema_pattern]
prefix = "src__"
separator = "__"
components = ["source"]

[pipeline.bronze.target]
catalog_template = "wh"
schema_template = "raw__{source}"
"#,
        );
        let config: rocky_core::config::RockyConfig = toml::from_str(&toml_str).unwrap();
        config.pipelines["bronze"].as_replication().unwrap().clone()
    }

    #[test]
    fn build_replication_strategy_incremental() {
        let pipeline = parse_pipeline(
            r#"strategy = "incremental"
timestamp_column = "_synced_at"
"#,
        );
        let strategy = build_replication_strategy(&pipeline).expect("strategy build");
        match strategy {
            MaterializationStrategy::Incremental { timestamp_column } => {
                assert_eq!(timestamp_column, "_synced_at");
            }
            other => panic!("expected Incremental, got {other:?}"),
        }
    }

    #[test]
    fn build_replication_strategy_full_refresh() {
        let pipeline = parse_pipeline(r#"strategy = "full_refresh""#);
        let strategy = build_replication_strategy(&pipeline).expect("strategy build");
        assert!(matches!(strategy, MaterializationStrategy::FullRefresh));
    }

    #[test]
    fn build_replication_strategy_view() {
        let pipeline = parse_pipeline(r#"strategy = "view""#);
        let strategy = build_replication_strategy(&pipeline).expect("strategy build");
        assert!(matches!(strategy, MaterializationStrategy::View));
    }

    #[test]
    fn build_replication_strategy_materialized_view() {
        // The fix to the legacy "silently fall through to full_refresh"
        // behaviour: `materialized_view` now resolves to the right IR
        // variant instead of dropping to FullRefresh.
        let pipeline = parse_pipeline(r#"strategy = "materialized_view""#);
        let strategy = build_replication_strategy(&pipeline).expect("strategy build");
        assert!(matches!(
            strategy,
            MaterializationStrategy::MaterializedView
        ));
    }

    #[test]
    fn build_replication_strategy_dynamic_table_requires_target_lag() {
        // dynamic_table on a replication pipeline today errors out because
        // the strategy string can't carry a target_lag specifier. Pinning
        // the error here so a future config-shape change must update both
        // sides of the contract.
        let pipeline = parse_pipeline(r#"strategy = "dynamic_table""#);
        let err = build_replication_strategy(&pipeline)
            .expect_err("dynamic_table without target_lag should error");
        assert!(
            err.to_string().contains("dynamic_table"),
            "error should mention dynamic_table: {err}"
        );
    }

    #[test]
    fn build_replication_strategy_merge_with_merge_keys() {
        let pipeline = parse_pipeline(
            r#"strategy = "merge"
merge_keys = ["id", "tenant_id"]
"#,
        );
        let strategy = build_replication_strategy(&pipeline).expect("strategy build");
        match strategy {
            MaterializationStrategy::Merge {
                unique_key,
                update_columns,
            } => {
                let keys: Vec<&str> = unique_key.iter().map(AsRef::as_ref).collect();
                assert_eq!(keys, vec!["id", "tenant_id"]);
                assert!(matches!(update_columns, ColumnSelection::All));
            }
            other => panic!("expected Merge, got {other:?}"),
        }
    }

    #[test]
    fn build_replication_strategy_merge_falls_back_to_merge_keys_fallback() {
        let pipeline = parse_pipeline(
            r#"strategy = "merge"
merge_keys_fallback = ["pk"]
"#,
        );
        let strategy = build_replication_strategy(&pipeline).expect("strategy build");
        match strategy {
            MaterializationStrategy::Merge { unique_key, .. } => {
                let keys: Vec<&str> = unique_key.iter().map(AsRef::as_ref).collect();
                assert_eq!(keys, vec!["pk"]);
            }
            other => panic!("expected Merge, got {other:?}"),
        }
    }

    #[test]
    fn build_replication_strategy_merge_prefers_merge_keys_over_fallback() {
        let pipeline = parse_pipeline(
            r#"strategy = "merge"
merge_keys = ["primary"]
merge_keys_fallback = ["fallback_only"]
"#,
        );
        let strategy = build_replication_strategy(&pipeline).expect("strategy build");
        match strategy {
            MaterializationStrategy::Merge { unique_key, .. } => {
                let keys: Vec<&str> = unique_key.iter().map(AsRef::as_ref).collect();
                assert_eq!(keys, vec!["primary"]);
            }
            other => panic!("expected Merge, got {other:?}"),
        }
    }

    #[test]
    fn resolve_merge_update_columns_expands_all_to_source_schema() {
        // Snowflake's MERGE rejects `UPDATE SET *`; the replication runner
        // resolves `ColumnSelection::All` against the discovered source
        // schema before SQL-gen so the explicit form lands. This test pins
        // that resolution against a representative source-schema + metadata
        // column list, mirroring the production call site in
        // `process_table`.
        let strategy = MaterializationStrategy::Merge {
            unique_key: vec![Arc::from("id")],
            update_columns: ColumnSelection::All,
        };
        let source_cols = vec![
            ColumnInfo {
                name: "id".to_string(),
                data_type: "INTEGER".to_string(),
                nullable: false,
            },
            ColumnInfo {
                name: "name".to_string(),
                data_type: "STRING".to_string(),
                nullable: true,
            },
            ColumnInfo {
                name: "amount".to_string(),
                data_type: "DECIMAL(10,2)".to_string(),
                nullable: true,
            },
        ];
        let metadata = vec![MetadataColumn {
            name: "_loaded_at".to_string(),
            data_type: "TIMESTAMP".to_string(),
            value: "CURRENT_TIMESTAMP()".to_string(),
        }];

        let resolved = resolve_merge_update_columns(&strategy, &source_cols, &metadata);

        match resolved {
            MaterializationStrategy::Merge {
                update_columns: ColumnSelection::Explicit(cols),
                ..
            } => {
                let names: Vec<&str> = cols.iter().map(AsRef::as_ref).collect();
                // Source-column order preserved; metadata appended. Merge
                // keys included — matches `UPDATE SET *` semantics on
                // Databricks/DuckDB.
                assert_eq!(names, vec!["id", "name", "amount", "_loaded_at"]);
            }
            other => panic!("expected Merge with Explicit columns, got {other:?}"),
        }
    }

    #[test]
    fn resolve_merge_update_columns_falls_back_to_all_when_source_cols_empty() {
        // Prefetch miss + describe failure: `source_cols` arrives empty.
        // Resolving against zero columns would emit `UPDATE SET ` (invalid).
        // Fall back to `ColumnSelection::All` so other adapters preserve
        // current behavior and Snowflake's dialect still surfaces its clear
        // "MERGE does not support UPDATE SET *" error.
        let strategy = MaterializationStrategy::Merge {
            unique_key: vec![Arc::from("id")],
            update_columns: ColumnSelection::All,
        };
        let resolved = resolve_merge_update_columns(&strategy, &[], &[]);
        assert!(matches!(
            resolved,
            MaterializationStrategy::Merge {
                update_columns: ColumnSelection::All,
                ..
            }
        ));
    }

    #[test]
    fn resolve_merge_update_columns_is_noop_for_non_merge_strategy() {
        // Non-merge strategies pass through unchanged. The resolver is
        // strictly a Merge-IR rewrite; Incremental/FullRefresh/etc. don't
        // carry an `update_columns` field.
        let strategy = MaterializationStrategy::Incremental {
            timestamp_column: "ts".to_string(),
        };
        let source_cols = vec![ColumnInfo {
            name: "id".to_string(),
            data_type: "INTEGER".to_string(),
            nullable: false,
        }];
        let resolved = resolve_merge_update_columns(&strategy, &source_cols, &[]);
        assert!(matches!(
            resolved,
            MaterializationStrategy::Incremental { .. }
        ));
    }

    #[test]
    fn build_replication_strategy_merge_without_keys_errors() {
        // Defensive guard: `load_rocky_config` rejects this earlier via
        // `validate_replication_strategies`, but callers that build the
        // config struct in-process and bypass that path must still get
        // a clean error.
        let pipeline = parse_pipeline(r#"strategy = "merge""#);
        let err = build_replication_strategy(&pipeline).expect_err("merge without keys must fail");
        let msg = err.to_string();
        assert!(
            msg.contains("merge_keys"),
            "error should name merge_keys: {msg}"
        );
    }

    #[test]
    fn build_replication_strategy_unknown_falls_back_to_full_refresh() {
        // Preserves prior behavior: any strategy string other than
        // "incremental" / "merge" maps to FullRefresh. Tightening this
        // into a typed error is a follow-up.
        let pipeline = parse_pipeline(r#"strategy = "totally_made_up""#);
        let strategy = build_replication_strategy(&pipeline).expect("strategy build");
        assert!(matches!(strategy, MaterializationStrategy::FullRefresh));
    }

    // ----- PR-B3: per-table override application -----

    #[test]
    fn override_changes_strategy_from_merge_to_incremental() {
        // Pipeline default is strategy = "merge", but the override
        // resolved for this table flips to "incremental". The
        // resulting MaterializationStrategy must be Incremental and
        // pick up the override's timestamp column.
        let pipeline = parse_pipeline(
            r#"strategy = "merge"
merge_keys = ["id"]
"#,
        );
        let resolved = ResolvedTableOverride {
            strategy: Some("incremental".to_string()),
            merge_keys: None,
            merge_keys_fallback: None,
            timestamp_column: Some("occurred_at".to_string()),
            enabled: None,
        };
        let strategy = build_replication_strategy_with_override(&pipeline, &resolved)
            .expect("strategy build with override");
        match strategy {
            MaterializationStrategy::Incremental { timestamp_column } => {
                assert_eq!(timestamp_column, "occurred_at");
            }
            other => panic!("expected Incremental, got {other:?}"),
        }
    }

    #[test]
    fn override_replaces_pipeline_merge_keys() {
        let pipeline = parse_pipeline(
            r#"strategy = "merge"
merge_keys = ["id"]
"#,
        );
        let resolved = ResolvedTableOverride {
            strategy: None,
            merge_keys: Some(vec!["user_id".into(), "tenant_id".into()]),
            merge_keys_fallback: None,
            timestamp_column: None,
            enabled: None,
        };
        let strategy =
            build_replication_strategy_with_override(&pipeline, &resolved).expect("strategy build");
        match strategy {
            MaterializationStrategy::Merge { unique_key, .. } => {
                let keys: Vec<&str> = unique_key.iter().map(AsRef::as_ref).collect();
                assert_eq!(keys, vec!["user_id", "tenant_id"]);
            }
            other => panic!("expected Merge, got {other:?}"),
        }
    }

    #[test]
    fn override_inherits_pipeline_merge_keys_when_unset() {
        // Override sets only the strategy; merge_keys inherit from
        // the pipeline default.
        let pipeline = parse_pipeline(
            r#"strategy = "incremental"
merge_keys = ["id"]
"#,
        );
        let resolved = ResolvedTableOverride {
            strategy: Some("merge".to_string()),
            merge_keys: None,
            merge_keys_fallback: None,
            timestamp_column: None,
            enabled: None,
        };
        let strategy =
            build_replication_strategy_with_override(&pipeline, &resolved).expect("strategy build");
        match strategy {
            MaterializationStrategy::Merge { unique_key, .. } => {
                let keys: Vec<&str> = unique_key.iter().map(AsRef::as_ref).collect();
                assert_eq!(keys, vec!["id"]);
            }
            other => panic!("expected Merge, got {other:?}"),
        }
    }

    #[test]
    fn override_merge_with_empty_effective_keys_errors_t3() {
        // T3 fail-fast guard: even though the parse-time validator
        // catches the obvious case, an override that explicitly sets
        // `merge_keys = []` (empty vec) is also caught here at
        // strategy-build time.
        let pipeline = parse_pipeline(r#"strategy = "full_refresh""#);
        let resolved = ResolvedTableOverride {
            strategy: Some("merge".to_string()),
            merge_keys: Some(vec![]),
            merge_keys_fallback: None,
            timestamp_column: None,
            enabled: None,
        };
        let err = build_replication_strategy_with_override(&pipeline, &resolved)
            .expect_err("empty effective keys must fail");
        let msg = format!("{err}");
        assert!(msg.contains("empty effective"), "wrong error: {msg}");
    }

    #[test]
    fn override_empty_is_pipeline_identity() {
        // Sanity: ResolvedTableOverride::default() yields the
        // pre-PR-B3 behavior — equal to plain build_replication_strategy.
        let pipeline = parse_pipeline(
            r#"strategy = "merge"
merge_keys = ["id"]
"#,
        );
        let baseline = build_replication_strategy(&pipeline).expect("baseline");
        let overridden =
            build_replication_strategy_with_override(&pipeline, &ResolvedTableOverride::default())
                .expect("override empty");
        match (baseline, overridden) {
            (
                MaterializationStrategy::Merge {
                    unique_key: a,
                    update_columns: _,
                },
                MaterializationStrategy::Merge {
                    unique_key: b,
                    update_columns: _,
                },
            ) => {
                let ak: Vec<&str> = a.iter().map(AsRef::as_ref).collect();
                let bk: Vec<&str> = b.iter().map(AsRef::as_ref).collect();
                assert_eq!(ak, bk);
            }
            (a, b) => panic!("mismatch: {a:?} vs {b:?}"),
        }
    }

    /// End-to-end test for `query_target_max_timestamp` against an
    /// in-memory DuckDB.
    ///
    /// Seeds a table with three rows at known timestamps, asks the helper to
    /// query `MAX(ts)`, and asserts the returned `DateTime<Utc>` matches the
    /// max seed value. This is the discriminating check the SqlDialect-level
    /// tests don't cover — the runner reads the warehouse result row, parses
    /// the timestamp shape DuckDB returns, and we want to find out at
    /// unit-test time (not at the first live run) if those assumptions shift.
    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn query_target_max_timestamp_returns_max_of_seeded_table() {
        use chrono::TimeZone;
        use rocky_core::traits::{SqlDialect, WarehouseAdapter};
        use rocky_duckdb::adapter::DuckDbWarehouseAdapter;
        use rocky_duckdb::dialect::DuckDbSqlDialect;
        use rocky_ir::TableRef;

        let adapter = DuckDbWarehouseAdapter::in_memory().unwrap();
        adapter
            .execute_statement("CREATE SCHEMA IF NOT EXISTS source")
            .await
            .unwrap();
        adapter
            .execute_statement(
                "CREATE TABLE source.events ( \
                    id INTEGER, \
                    _fivetran_synced TIMESTAMP \
                 )",
            )
            .await
            .unwrap();
        adapter
            .execute_statement(
                "INSERT INTO source.events VALUES \
                    (1, TIMESTAMP '2026-03-01 10:00:00'), \
                    (2, TIMESTAMP '2026-03-02 11:00:00'), \
                    (3, TIMESTAMP '2026-03-03 12:00:00')",
            )
            .await
            .unwrap();

        let dialect = DuckDbSqlDialect;
        let source = TableRef {
            catalog: String::new(),
            schema: "source".into(),
            table: "events".into(),
        };

        let watermark = super::query_target_max_timestamp(
            &adapter as &dyn WarehouseAdapter,
            &dialect as &dyn SqlDialect,
            &source,
            "_fivetran_synced",
        )
        .await;

        let expected = chrono::Utc.with_ymd_and_hms(2026, 3, 3, 12, 0, 0).unwrap();
        assert_eq!(
            watermark,
            Some(expected),
            "expected MAX(_fivetran_synced) = {expected}, got {watermark:?}"
        );
    }

    /// `query_target_max_timestamp` returns `None` for an empty table so
    /// `resolve_new_watermark` keeps the prior watermark rather than advancing.
    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn query_target_max_timestamp_returns_none_for_empty_table() {
        use rocky_core::traits::{SqlDialect, WarehouseAdapter};
        use rocky_duckdb::adapter::DuckDbWarehouseAdapter;
        use rocky_duckdb::dialect::DuckDbSqlDialect;
        use rocky_ir::TableRef;

        let adapter = DuckDbWarehouseAdapter::in_memory().unwrap();
        adapter
            .execute_statement("CREATE SCHEMA IF NOT EXISTS source")
            .await
            .unwrap();
        adapter
            .execute_statement(
                "CREATE TABLE source.empty_events (id INTEGER, _fivetran_synced TIMESTAMP)",
            )
            .await
            .unwrap();

        let dialect = DuckDbSqlDialect;
        let source = TableRef {
            catalog: String::new(),
            schema: "source".into(),
            table: "empty_events".into(),
        };

        let watermark = super::query_target_max_timestamp(
            &adapter as &dyn WarehouseAdapter,
            &dialect as &dyn SqlDialect,
            &source,
            "_fivetran_synced",
        )
        .await;

        assert!(
            watermark.is_none(),
            "empty source should yield None so the caller falls back to Utc::now()"
        );
    }

    /// Regression: a bootstrap incremental run (first run / drift recreate, i.e.
    /// the `use_full_refresh` path) must record the source **data** max as its
    /// watermark, not a wall-clock `now`. With `now`, the next incremental tick
    /// filters `ts > now` and drops every existing row — they're all dated in
    /// the past — silently losing the delta. Guards against re-introducing the
    /// `&& !use_full_refresh` exclusion.
    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn incremental_bootstrap_watermark_is_source_max_not_wallclock() {
        use chrono::TimeZone;
        use rocky_core::traits::{SqlDialect, WarehouseAdapter};
        use rocky_duckdb::adapter::DuckDbWarehouseAdapter;
        use rocky_duckdb::dialect::DuckDbSqlDialect;
        use rocky_ir::{MaterializationStrategy, TableRef};

        let adapter = DuckDbWarehouseAdapter::in_memory().unwrap();
        adapter
            .execute_statement("CREATE SCHEMA IF NOT EXISTS source")
            .await
            .unwrap();
        adapter
            .execute_statement(
                "CREATE TABLE source.events (id INTEGER, _fivetran_synced TIMESTAMP)",
            )
            .await
            .unwrap();
        // Every row is dated well before `now`.
        adapter
            .execute_statement(
                "INSERT INTO source.events VALUES \
                    (1, TIMESTAMP '2026-03-01 10:00:00'), \
                    (2, TIMESTAMP '2026-03-03 12:00:00')",
            )
            .await
            .unwrap();

        let dialect = DuckDbSqlDialect;
        let source = TableRef {
            catalog: String::new(),
            schema: "source".into(),
            table: "events".into(),
        };
        // The bootstrap case the bug missed: incremental strategy, first run.
        let strategy = MaterializationStrategy::Incremental {
            timestamp_column: "_fivetran_synced".to_string(),
        };
        let now = chrono::Utc::now();

        let watermark = super::resolve_new_watermark(
            &strategy,
            &adapter as &dyn WarehouseAdapter,
            &dialect as &dyn SqlDialect,
            &source,
            "_fivetran_synced",
            None,
            now,
        )
        .await;

        let data_max = chrono::Utc.with_ymd_and_hms(2026, 3, 3, 12, 0, 0).unwrap();
        assert_eq!(
            watermark, data_max,
            "incremental bootstrap must record MAX(target.ts), not wall-clock now"
        );
        assert!(
            watermark < now,
            "watermark must be the past data max, not a wall-clock value"
        );

        // Microbatch shares the same source-MAX semantics as Incremental.
        let microbatch = MaterializationStrategy::Microbatch {
            timestamp_column: "_fivetran_synced".to_string(),
            granularity: rocky_ir::TimeGrain::Hour,
        };
        let mb_watermark = super::resolve_new_watermark(
            &microbatch,
            &adapter as &dyn WarehouseAdapter,
            &dialect as &dyn SqlDialect,
            &source,
            "_fivetran_synced",
            None,
            now,
        )
        .await;
        assert_eq!(
            mb_watermark, data_max,
            "microbatch must also record MAX(target.ts), not wall-clock now"
        );
    }

    /// M2 regression: a row appended to SOURCE between the incremental INSERT's
    /// snapshot and the watermark capture must NOT be skipped on the next run.
    ///
    /// Recording `MAX(target.ts)` (what was loaded) instead of a later
    /// `MAX(source.ts)` closes the TOCTOU. This drives the actual SQL the
    /// runner emits (the dialect's `watermark_where` filter + INSERT) against
    /// in-memory DuckDB across two consecutive incremental runs and asserts no
    /// row loss.
    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn incremental_no_row_loss_on_source_append_between_insert_and_watermark() {
        use chrono::TimeZone;
        use rocky_core::traits::{SqlDialect, WarehouseAdapter};
        use rocky_duckdb::adapter::DuckDbWarehouseAdapter;
        use rocky_duckdb::dialect::DuckDbSqlDialect;
        use rocky_ir::{MaterializationStrategy, TableRef};

        let adapter = DuckDbWarehouseAdapter::in_memory().unwrap();
        for ddl in [
            "CREATE SCHEMA src",
            "CREATE SCHEMA tgt",
            "CREATE TABLE src.events (id INTEGER, ts TIMESTAMP)",
            "CREATE TABLE tgt.events (id INTEGER, ts TIMESTAMP)",
            // Initial source rows, all in the past.
            "INSERT INTO src.events VALUES (1, TIMESTAMP '2026-03-01 10:00:00'), \
             (2, TIMESTAMP '2026-03-02 10:00:00')",
        ] {
            adapter.execute_statement(ddl).await.unwrap();
        }

        let dialect = DuckDbSqlDialect;
        let strategy = MaterializationStrategy::Incremental {
            timestamp_column: "ts".to_string(),
        };
        let target = TableRef {
            catalog: String::new(),
            schema: "tgt".into(),
            table: "events".into(),
        };
        let now = chrono::Utc::now();

        // ── Run 1 ────────────────────────────────────────────────────────
        // INSERT INTO target SELECT * FROM source WHERE ts > epoch.
        let where1 = dialect.watermark_where("ts", None).unwrap();
        adapter
            .execute_statement(&format!(
                "INSERT INTO tgt.events SELECT * FROM src.events {where1}"
            ))
            .await
            .unwrap();

        // *** The TOCTOU window ***: a row lands in SOURCE with a timestamp
        // later than the rows the INSERT above selected, but it was NOT part of
        // run 1's snapshot. A source-MAX watermark would jump to 03-03 and the
        // next `ts > 03-03` filter would skip this row forever.
        adapter
            .execute_statement("INSERT INTO src.events VALUES (3, TIMESTAMP '2026-03-03 10:00:00')")
            .await
            .unwrap();

        // Capture the watermark the way the runner does — from the TARGET.
        let wm1 = super::resolve_new_watermark(
            &strategy,
            &adapter as &dyn WarehouseAdapter,
            &dialect as &dyn SqlDialect,
            &target,
            "ts",
            None,
            now,
        )
        .await;
        // It must reflect only what was loaded (max = 03-02), NOT the appended
        // source row (03-03).
        let loaded_max = chrono::Utc.with_ymd_and_hms(2026, 3, 2, 10, 0, 0).unwrap();
        assert_eq!(
            wm1, loaded_max,
            "watermark must be MAX(target.ts), not the post-snapshot source append"
        );

        // ── Run 2 ────────────────────────────────────────────────────────
        // Filter `ts > wm1`. Because wm1 = 03-02 (not 03-03), the appended row
        // (id=3, 03-03) IS picked up — no loss.
        let where2 = dialect.watermark_where("ts", Some(&wm1)).unwrap();
        adapter
            .execute_statement(&format!(
                "INSERT INTO tgt.events SELECT * FROM src.events {where2}"
            ))
            .await
            .unwrap();

        // Assert the target now has all three source rows, exactly once each.
        let count = adapter
            .execute_query("SELECT COUNT(*) FROM tgt.events")
            .await
            .unwrap();
        let n = count.rows[0][0].as_u64().or_else(|| {
            count.rows[0][0]
                .as_str()
                .and_then(|s| s.parse::<u64>().ok())
        });
        assert_eq!(
            n,
            Some(3),
            "all three source rows must be loaded exactly once — no loss, no dup"
        );
        let ids = adapter
            .execute_query("SELECT id FROM tgt.events ORDER BY id")
            .await
            .unwrap();
        let got: Vec<i64> = ids
            .rows
            .iter()
            .filter_map(|r| {
                r[0].as_i64()
                    .or_else(|| r[0].as_str().and_then(|s| s.parse().ok()))
            })
            .collect();
        assert_eq!(
            got,
            vec![1, 2, 3],
            "the appended row (id=3) must not be skipped"
        );
    }

    /// Bad identifier rejected before any warehouse query is issued —
    /// defense in depth against SQL injection via the `timestamp_column`
    /// parameter (the runner reads it from `rocky.toml`).
    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn query_target_max_timestamp_rejects_unsafe_timestamp_column() {
        use rocky_core::traits::{SqlDialect, WarehouseAdapter};
        use rocky_duckdb::adapter::DuckDbWarehouseAdapter;
        use rocky_duckdb::dialect::DuckDbSqlDialect;
        use rocky_ir::TableRef;

        let adapter = DuckDbWarehouseAdapter::in_memory().unwrap();
        let dialect = DuckDbSqlDialect;
        let source = TableRef {
            catalog: String::new(),
            schema: "source".into(),
            table: "events".into(),
        };

        let watermark = super::query_target_max_timestamp(
            &adapter as &dyn WarehouseAdapter,
            &dialect as &dyn SqlDialect,
            &source,
            "'; DROP TABLE",
        )
        .await;

        assert!(
            watermark.is_none(),
            "unsafe timestamp_column must be rejected before the SQL is built"
        );
    }

    // -----------------------------------------------------------------------
    // Intra-layer transformation concurrency (`--parallel N`)
    //
    // These exercise the concurrent execution path added in
    // `execute_models`. DuckDB is forced serial in production, so the
    // concurrency tests opt the adapter into concurrency via the
    // `#[doc(hidden)]` test-only builder — the single Mutex connection still
    // serializes the actual DB work (now off the reactor via
    // spawn_blocking), but the concurrent control-flow (fan-out, per-layer
    // barrier, ordered collect) runs for real against DuckDB storage.
    // -----------------------------------------------------------------------

    /// Write `name.sql` + `name.toml` (full_refresh into `main`) to `dir`.
    #[cfg(feature = "duckdb")]
    fn write_plain_model(dir: &std::path::Path, name: &str, sql: &str) {
        std::fs::write(dir.join(format!("{name}.sql")), format!("{sql}\n"))
            .expect("write model sql");
        std::fs::write(
            dir.join(format!("{name}.toml")),
            "[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"\"\nschema = \"main\"\n",
        )
        .expect("write model toml");
    }

    /// Materialize all models in `models_dir` against a fresh DuckDB file,
    /// returning the resulting `RunOutput` and the `execute_models` result.
    /// `concurrent_adapter` flips the test-only concurrency flag so the
    /// parallel path engages; `parallel` is the `--parallel N` value.
    #[cfg(feature = "duckdb")]
    async fn run_models_against_duckdb(
        models_dir: &std::path::Path,
        db_path: &std::path::Path,
        concurrent_adapter: bool,
        parallel: u32,
    ) -> (RunOutput, Result<()>) {
        use rocky_duckdb::adapter::DuckDbWarehouseAdapter;

        let adapter = DuckDbWarehouseAdapter::open(db_path)
            .expect("open duckdb")
            .with_concurrent_for_test(concurrent_adapter);
        let opts = PartitionRunOptions {
            parallel,
            ..Default::default()
        };
        let mut output = RunOutput::new(String::new(), 0, parallel.max(1) as usize);
        let result = super::execute_models(
            models_dir,
            &adapter as &dyn rocky_core::traits::WarehouseAdapter,
            None,
            &opts,
            "test-run",
            None,
            &mut output,
            None,
            None,
            &rocky_core::config::SchemaCacheConfig::default(),
            false,
        )
        .await;
        (output, result)
    }

    /// (a) A wide single-layer project (four dependency-free models) runs
    /// correctly under `--parallel 4` and produces *identical*
    /// materialization results and ordering to the serial run.
    #[cfg(feature = "duckdb")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn parallel_wide_layer_matches_serial_output() {
        let tmp = tempfile::TempDir::new().expect("temp dir");
        let models_dir = tmp.path().join("models");
        std::fs::create_dir(&models_dir).expect("mkdir models");
        // Four independent models — no refs between them, so they all land
        // in execution layer 0 (one wide layer).
        for (i, name) in ["alpha", "bravo", "charlie", "delta"].iter().enumerate() {
            write_plain_model(&models_dir, name, &format!("SELECT {i} AS v"));
        }

        let serial_db = tmp.path().join("serial.duckdb");
        let parallel_db = tmp.path().join("parallel.duckdb");

        let (serial_out, serial_res) =
            run_models_against_duckdb(&models_dir, &serial_db, false, 1).await;
        serial_res.expect("serial run must succeed");

        let (parallel_out, parallel_res) =
            run_models_against_duckdb(&models_dir, &parallel_db, true, 4).await;
        parallel_res.expect("parallel run must succeed");

        // Same number of materializations.
        assert_eq!(
            serial_out.materializations.len(),
            4,
            "all four models must materialize"
        );
        assert_eq!(
            parallel_out.materializations.len(),
            serial_out.materializations.len(),
            "parallel run must materialize the same model count as serial"
        );

        // Identical ordering: asset_key sequence must match exactly. The
        // parallel path sorts collected results by within-layer index, so
        // this is the deterministic-ordering guarantee.
        let serial_keys: Vec<&Vec<String>> = serial_out
            .materializations
            .iter()
            .map(|m| &m.asset_key)
            .collect();
        let parallel_keys: Vec<&Vec<String>> = parallel_out
            .materializations
            .iter()
            .map(|m| &m.asset_key)
            .collect();
        assert_eq!(
            parallel_keys, serial_keys,
            "parallel materialization ordering must be identical to serial"
        );

        // Pin the absolute order too (not just serial == parallel) so a
        // broken within-layer sort can't pass by being consistently wrong
        // across both runs. Models load name-sorted and sit in one layer,
        // so the deterministic order is alphabetical.
        let expected: Vec<Vec<String>> = ["alpha", "bravo", "charlie", "delta"]
            .iter()
            .map(|n| vec![String::new(), "main".to_string(), n.to_string()])
            .collect();
        let expected_refs: Vec<&Vec<String>> = expected.iter().collect();
        assert_eq!(
            parallel_keys, expected_refs,
            "concurrent results must be re-ordered to the deterministic \
             (name-sorted, within-layer) sequence"
        );

        // Every model's table actually exists in the parallel DB.
        use rocky_core::traits::WarehouseAdapter;
        use rocky_duckdb::adapter::DuckDbWarehouseAdapter;
        let verify = DuckDbWarehouseAdapter::open(&parallel_db).expect("reopen");
        for name in ["alpha", "bravo", "charlie", "delta"] {
            let r = verify
                .execute_query(&format!("SELECT COUNT(*) FROM main.{name}"))
                .await
                .unwrap_or_else(|e| panic!("table main.{name} must exist after parallel run: {e}"));
            assert_eq!(r.rows.len(), 1);
        }
    }

    /// (b) A mid-DAG failure under `--parallel` stops downstream layers and
    /// yields the same fail-fast `Err` the serial path returns: a failing
    /// layer-0 model means the layer-1 dependent never materializes.
    #[cfg(feature = "duckdb")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn parallel_mid_layer_failure_skips_downstream() {
        let tmp = tempfile::TempDir::new().expect("temp dir");
        let models_dir = tmp.path().join("models");
        std::fs::create_dir(&models_dir).expect("mkdir models");

        // Layer 0: `good` succeeds, `bad` fails (selects from a table that
        // does not exist). Layer 1: `downstream` depends on `good` AND `bad`
        // via SQL refs, so it sits in a later layer and must never run once
        // layer 0 reports a failure.
        write_plain_model(&models_dir, "good", "SELECT 1 AS v");
        write_plain_model(&models_dir, "bad", "SELECT * FROM main.does_not_exist_xyz");
        write_plain_model(
            &models_dir,
            "downstream",
            "SELECT g.v FROM good g CROSS JOIN bad b",
        );

        let parallel_db = tmp.path().join("parallel.duckdb");
        let (_out, result) = run_models_against_duckdb(&models_dir, &parallel_db, true, 4).await;

        // Fail-fast: the run returns Err (same as the serial path).
        let err = result.expect_err("a failing model must surface as Err");
        assert!(
            format!("{err:#}").contains("bad"),
            "error should name the failing model: {err:#}"
        );

        // Downstream dependent must NOT have been materialized — the
        // per-layer barrier stopped the run after the failing layer.
        use rocky_core::traits::WarehouseAdapter;
        use rocky_duckdb::adapter::DuckDbWarehouseAdapter;
        let verify = DuckDbWarehouseAdapter::open(&parallel_db).expect("reopen");
        let exists = verify
            .execute_query(
                "SELECT COUNT(*) FROM information_schema.tables \
                 WHERE table_schema = 'main' AND table_name = 'downstream'",
            )
            .await
            .ok()
            .and_then(|r| r.rows.first()?.first()?.as_str().map(|s| s != "0"))
            .unwrap_or(false);
        assert!(
            !exists,
            "downstream model in a later layer must not run after an upstream-layer failure"
        );
    }

    /// (c) A normal DuckDB adapter (concurrency unsupported) stays
    /// effectively serial even when `--parallel 4` is requested, and still
    /// materializes correctly — the opt-in concurrency must never break the
    /// default DuckDB path.
    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn duckdb_stays_serial_with_parallel_flag() {
        let tmp = tempfile::TempDir::new().expect("temp dir");
        let models_dir = tmp.path().join("models");
        std::fs::create_dir(&models_dir).expect("mkdir models");
        for (i, name) in ["one", "two", "three"].iter().enumerate() {
            write_plain_model(&models_dir, name, &format!("SELECT {i} AS v"));
        }

        // Adapter reports `supports_concurrent_execution() == false` (the
        // production default for DuckDB), so even with `--parallel 4` the
        // serial branch runs.
        let db = tmp.path().join("serial.duckdb");
        let (out, result) = run_models_against_duckdb(&models_dir, &db, false, 4).await;
        result.expect("run must succeed with parallel flag on serial DuckDB");

        assert_eq!(
            out.materializations.len(),
            3,
            "all three models must materialize on the forced-serial DuckDB path"
        );
    }
}
