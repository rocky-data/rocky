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

/// Outcome of processing one replication table: it was either materialized
/// (copied, with full [`TableResult`] detail) or pruned as unchanged since the
/// last successful copy (`prune_unchanged` skip-unchanged pruning). The large
/// `Materialized` payload is boxed so the enum stays small.
enum TableOutcome {
    Materialized(Box<TableResult>),
    Pruned(PrunedTable),
}

/// A table skipped by `prune_unchanged` pruning because its source is unchanged
/// since the last successful copy. Recorded on `RunOutput.excluded_tables` with
/// reason `"unchanged_since_last_copy"`; emits no materialization, so the
/// orchestrator's `satisfy_empty_outputs` provides the 0-row continuity signal.
struct PrunedTable {
    asset_key: Vec<String>,
    source_schema: String,
    table_name: String,
}

/// Record a skip-unchanged pruned table on the run output. It lands in
/// `excluded_tables` with reason `"unchanged_since_last_copy"` (the free-form
/// reason field, so no schema change) and emits no materialization — the
/// orchestrator's `satisfy_empty_outputs` stamps the 0-row continuity signal.
fn record_pruned(output: &mut RunOutput, pruned: PrunedTable) {
    output.excluded_tables.push(ExcludedTableOutput {
        asset_key: pruned.asset_key,
        source_schema: pruned.source_schema,
        table_name: pruned.table_name,
        reason: "unchanged_since_last_copy".to_string(),
    });
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
    /// Fully-qualified target table ref, populated unconditionally (unlike
    /// `target_batch_ref`, which is gated on `check_row_count`). The assertion
    /// phase needs it for every materialized table so uniqueness/assertions run
    /// even when row-count and freshness checks are disabled.
    target_ref: TableRef,
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

/// Map a finalized [`RunOutput`]'s derived status onto the CLI exit-code
/// contract for the transformation / model-only execution paths.
///
/// - `Success` → `Ok(())` (exit 0).
/// - `PartialFailure` (some models built, some failed) → the
///   [`PartialFailure`] sentinel `main.rs` maps to exit 2.
/// - `Failure` (nothing built) → an `anyhow` error (exit 1).
///
/// Callers MUST have already emitted the JSON `RunOutput` on stdout before
/// invoking this — the returned error short-circuits the rest of the path,
/// so the structured payload (status, `tables_failed`, `errors`) is what a
/// JSON consumer reads regardless of exit code. This is what makes a
/// compile-failed model a first-class run failure instead of a silently
/// skipped no-op that still reported `status: "Success"`, exit 0.
pub(crate) fn run_status_exit_result(output: &RunOutput, run_id: &str) -> Result<()> {
    match output.derive_run_status() {
        rocky_core::state::RunStatus::PartialFailure => Err(PartialFailure {
            count: output.tables_failed,
            run_id: run_id.to_string(),
        }
        .into()),
        rocky_core::state::RunStatus::Failure => anyhow::bail!(
            "{} model(s) failed (run_id: {run_id}, see the `errors` array in the JSON output)",
            output.tables_failed
        ),
        _ => Ok(()),
    }
}

/// Merge `execute_models`' compile-error bookkeeping into the replication
/// path's parallel-copy tallies, then stamp the terminal `status`.
///
/// The replication path runs the compiled `--models` / `--all` set through
/// [`execute_models`], which records each per-model compile failure (e.g.
/// E020 — a `time_interval` model whose `time_column` is absent from its
/// SELECT output) on `output.errors` with [`crate::output::FailureKind::CompileError`]
/// and bumps `output.tables_failed` once per distinct failing model — exactly
/// as on the transformation / model-only paths. The parallel-copy bookkeeping
/// then used to *overwrite* both fields with only its own `table_errors`,
/// silently dropping those compile failures and flipping a broken project back
/// to `status = Success` / exit 0.
///
/// This merges instead: it keeps the already-recorded compile-error entries,
/// folds their count (the number of distinct compile-failed models, counted
/// directly from the retained entries rather than inferred from
/// `output.tables_failed`) into the total alongside the copy failures, appends
/// the parallel-copy errors, and re-derives `output.status` so a
/// `--output json` consumer reads the right terminal status (not the
/// `RunOutput::new` default of `success`). A clean run still derives
/// `Success`; a copy-only failure behaves exactly as before.
fn merge_replication_compile_and_copy_errors(output: &mut RunOutput, table_errors: &[TableError]) {
    output
        .errors
        .retain(|e| e.failure_kind == crate::output::FailureKind::CompileError);
    // Count distinct compile-failed models from the retained entries rather
    // than inferring from `tables_failed`: the compile path records one
    // `errors` entry per diagnostic but bumps `tables_failed` once per model,
    // so count distinct asset_keys, not raw entries.
    let compile_failed_count = output
        .errors
        .iter()
        .map(|e| e.asset_key.as_slice())
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    output.tables_failed = compile_failed_count + table_errors.len();
    output
        .errors
        .extend(table_errors.iter().map(|e| TableErrorOutput {
            asset_key: e.asset_key.clone(),
            error: e.error.clone(),
            failure_kind: e.failure_kind,
            cooldown_seconds: e.cooldown_seconds,
        }));
    output.status = output.derive_run_status();
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
    /// Tenant this table belongs to, lifted from the discover-time
    /// schema-pattern `{tenant}` component. `None` when the pipeline's
    /// schema pattern declares no `{tenant}` placeholder. Carried onto
    /// the resulting `MaterializationOutput` so the persisted
    /// `ModelExecution` records a tenant for `rocky cost --by tenant`.
    tenant: Option<String>,
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

/// CLI selection state for `rocky run --defer` (dev-against-prod-upstreams).
///
/// When [`enabled`](Self::enabled) is set, transformation models that are
/// **not** part of the build selection (`--model <name>`) have their bare
/// `ref()` upstreams rewritten to resolve against an existing "defer target"
/// — typically the production schema — instead of the working schema. This
/// lets a developer build only their changed models locally while reading
/// every unbuilt upstream from production, the standard dbt `--defer`
/// convenience.
///
/// Default is OFF (`enabled = false`), in which case the run is byte-for-byte
/// identical to today. `--defer` only takes effect alongside `--model`: a
/// full run builds every model, so there are no unbuilt upstreams to defer.
#[derive(Debug, Clone, Default)]
pub struct DeferOptions {
    /// Whether `--defer` was passed.
    pub enabled: bool,
    /// Optional `--defer-to <schema>` override. When `None`, each unbuilt
    /// upstream's reference resolves to its own configured target schema (its
    /// production home). When `Some(schema)`, every deferred reference is
    /// pointed at that schema instead (catalog + table preserved).
    pub defer_to: Option<String>,
}

/// CLI overlay for the run command's build-decision gates — the
/// `--skip-unchanged` model-skip gate and the `--no-reuse` reuse override.
///
/// Every field defaults to `false`, which keeps both gates off and the run
/// byte-identical to before they existed. Neither gate is a result-equivalence
/// guarantee — see [`rocky_core::config::RunConfig`] and `ModelIr::skip_hash`
/// for the skip gate, and the fail-closed reuse decision
/// ([`crate::commands::reuse_decision`]) for reuse.
#[derive(Debug, Clone, Copy, Default)]
pub struct SkipRunOptions {
    /// Whether `--skip-unchanged` was passed. Turns the gate on for this
    /// invocation regardless of the `[run] skip_unchanged` config value
    /// (the two are OR-ed at the gate).
    pub skip_unchanged: bool,
    /// Whether `--force-rebuild` was passed. When `true`, every selected
    /// model builds unconditionally — the gate's escape hatch. Takes
    /// precedence over `skip_unchanged` and over any config.
    pub force_rebuild: bool,
    /// Whether `--no-reuse` was passed. When `true`, the content-addressed
    /// reuse decision is disabled for this invocation even if `[reuse]` is
    /// enabled in config — clause 1 of the fail-closed gate. The escape
    /// hatch that forces every content-addressed model to BUILD.
    pub no_reuse: bool,
    /// Whether `--no-prune` was passed. Forces a full replication pass for
    /// this invocation, disabling `[pipeline] prune_unchanged` skip-unchanged
    /// pruning even when the config opts in — e.g. after a manual target-side
    /// mutation. No effect when `prune_unchanged` is off.
    pub no_prune: bool,
}

/// Fully-resolved configuration for the model-skip gate, assembled in
/// `run()` from the CLI overlay ([`SkipRunOptions`]), the `[run]` config
/// ([`rocky_core::config::RunConfig`]), and the run mode (shadow/branch),
/// then handed to [`execute_models`].
///
/// The gate is a best-effort optimization, **not** a result-equivalence
/// guarantee. [`Self::enabled`] is the single "should the gate even run?"
/// predicate; when it is `false`, `execute_models` issues zero extra
/// state reads or warehouse queries and behaves byte-identically to before
/// the gate existed.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SkipGateConfig {
    /// `--skip-unchanged` flag OR `[run] skip_unchanged = true`.
    pub feature_enabled: bool,
    /// `--force-rebuild` — forces every model to build (gate never skips).
    pub force_rebuild: bool,
    /// `[run] skip_rowcount_fallback` — permit a `COUNT(*)` data-stability
    /// signal when an upstream has no tracked timestamp column.
    pub rowcount_fallback: bool,
    /// `[run] lag_tolerance_seconds` — treat an upstream `MAX(ts)` that
    /// moved by fewer than this many seconds as unchanged.
    pub lag_tolerance_seconds: u64,
    /// True when this is a shadow / branch run. Shadow and branch runs are
    /// never skip-eligible in v1 — they are verification runs that write to
    /// different targets, so skipping defeats their purpose.
    pub shadow_or_branch: bool,
}

impl SkipGateConfig {
    /// Resolve the effective gate config from the CLI overlay, the `[run]`
    /// config block, and whether this is a shadow/branch run.
    pub(crate) fn resolve(
        skip_opts: &SkipRunOptions,
        run_config: &rocky_core::config::RunConfig,
        shadow_or_branch: bool,
    ) -> Self {
        Self {
            feature_enabled: skip_opts.skip_unchanged || run_config.skip_unchanged,
            force_rebuild: skip_opts.force_rebuild,
            rowcount_fallback: run_config.skip_rowcount_fallback,
            lag_tolerance_seconds: run_config.lag_tolerance_seconds,
            shadow_or_branch,
        }
    }

    /// Whether the gate may skip *any* model on this run. `false` ⇒ the gate
    /// is fully inert (default-off, force-rebuild, or a shadow/branch run),
    /// and `execute_models` takes the unchanged build-everything path.
    pub(crate) fn is_active(&self) -> bool {
        self.feature_enabled && !self.force_rebuild && !self.shadow_or_branch
    }

    /// The fully-inert gate — the default-off configuration used by tests
    /// that exercise `execute_models` without the skip gate.
    #[cfg(test)]
    pub(crate) fn off() -> Self {
        Self {
            feature_enabled: false,
            force_rebuild: false,
            rowcount_fallback: false,
            lag_tolerance_seconds: 0,
            shadow_or_branch: false,
        }
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
    /// Per-model surrogate-key specs (`[[surrogate_key]]` sidecar blocks),
    /// resolved into injected metadata columns at materialization time.
    pub surrogate_keys:
        &'a std::collections::HashMap<String, Vec<rocky_core::models::SurrogateKeySpec>>,
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
pub(crate) fn audit_to_record(ctx: &AuditContext) -> RunRecordAudit {
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

pub(crate) fn persist_run_record(
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
        } => Some(
            StateStore::open_with_policy(state_path, state_cfg.on_schema_mismatch).with_context(
                || {
                    format!(
                        "failed to open state store at {} for idempotency claim",
                        state_path.display()
                    )
                },
            )?,
        ),
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
    // `--defer` / `--defer-to`. Default OFF ⇒ byte-identical behavior. When
    // enabled, transformation models outside the `--model` selection have
    // their bare upstream `ref()`s rewritten to resolve against the defer
    // target (its production schema, or `--defer-to`). Only meaningful with
    // `--model`; a full run builds everything and defers nothing.
    defer_opts: &DeferOptions,
    // `--skip-unchanged` / `--force-rebuild` CLI overlay for the opt-in
    // model-skip gate. Default OFF (both `false`) ⇒ byte-identical behavior.
    skip_opts: &SkipRunOptions,
    // `--var name=value` per-run variables. Substituted into `@var(name)`
    // placeholders in model SQL at compile time. Empty ⇒ no `@var()` model
    // resolves and behavior is byte-identical.
    run_vars: &rocky_core::run_vars::RunVars,
) -> Result<()> {
    // With `-o json` stdout is reserved for the JSON payload — route any
    // human-readable summary/progress line (e.g. a `depends_on` upstream
    // pipeline's "Copied …") to stderr so it can't precede the JSON document.
    // Sub-runs under the unified DAG are invoked with `output_json = false`,
    // so this is a no-op for them; the outer `run_with_dag` already reserved.
    if output_json {
        crate::output::reserve_stdout_for_json();
    }

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

    // Resolve the model-skip gate once. Shadow / branch runs are never
    // skip-eligible (they write to different targets), so a shadow config
    // forces the gate inert regardless of the flag / config.
    let skip_gate =
        SkipGateConfig::resolve(skip_opts, &rocky_cfg.run, shadow_config.is_some());

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
            rocky_core::state::StateStore::open_with_policy(
                state_path,
                rocky_cfg.state.on_schema_mismatch,
            )
            .context(format!(
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

        let exec_result = execute_models(
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
            defer_opts,
            skip_gate,
            // Reuse is active iff `[reuse]` is enabled AND `--no-reuse` was
            // not passed (clause 1 of the fail-closed decision). `--no-reuse`
            // suppresses the whole reuse path for this invocation — both the
            // point-to decision and the spine population it would feed.
            rocky_cfg.reuse.enabled && !skip_opts.no_reuse,
            run_vars,
        )
        .await;

        // A runtime model failure (warehouse rejected the SQL, an unresolved
        // upstream, a divide-by-zero, ...) surfaces here as `Err` —
        // `execute_models` stops at the first failing model and returns the
        // error with the model named in the chain. Record it as a first-class
        // run failure (bump `tables_failed`, push the error) so the JSON
        // `RunOutput` below still emits with `status` / `errors[]` / any
        // sibling materializations BEFORE the non-zero exit, mirroring the
        // compile-error path which records into `output` and returns `Ok`.
        // Without this the `?` short-circuited before `print_json`, leaving an
        // orchestrator (Dagster) consuming `--output json` with empty stdout on
        // a runtime failure. The terminal-status exit contract is honoured by
        // `run_status_exit_result` below.
        match exec_result {
            Ok(()) => {
                // Per-model `[governance.tags]` apply (scoped to the built
                // model). The model-only path is one of the entry points the
                // SDK / Dagster drive; without this its tags were silently
                // dropped. Best-effort; no-op on DuckDB's Noop adapter.
                let governance_adapter =
                    adapter_registry.governance_adapter(&target_adapter_name);
                apply_model_governance_tags(
                    mdir,
                    governance_adapter.as_ref(),
                    &rocky_cfg,
                    run_vars,
                    Some(target_model),
                )
                .await;
            }
            Err(e) => {
                output.tables_failed += 1;
                output.errors.push(crate::output::TableErrorOutput {
                    asset_key: vec![target_model.to_string()],
                    error: format!("{e:#}"),
                    failure_kind: crate::output::FailureKind::Unknown,
                    cooldown_seconds: None,
                });
            }
        }

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

        // Stamp the terminal status so the JSON payload reports the run
        // outcome directly (a compile-failed target makes this `Failure`).
        output.status = output.derive_run_status();
        if output_json {
            print_json(&output)?;
        }
        budget_result?;
        // Same run-status exit contract as the transformation path: a
        // model-only run (`rocky run --model <X>`) whose target failed to
        // compile is recorded in `output.errors` / `tables_failed` by
        // `execute_models`; propagate the non-zero exit so it doesn't
        // report exit 0 with a JSON payload that says the model failed.
        return run_status_exit_result(&output, &run_id);
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
                skip_gate,
                // Canonical state path resolved once by `main.rs`
                // (`--state-path` / `--state-namespace` / the global
                // default) — the same path the replication path opens. The
                // transformation path persists its `RunRecord` here so
                // `--skip-unchanged` has a prior-successful baseline and the
                // run shows in `rocky history`.
                state_path,
                // Share `run()`'s `run_id` + `started_at` so the persisted
                // `RunRecord::run_id` matches the idempotency stamp below
                // (invariant: `IdempotencyEntry::run_id == RunRecord::run_id`).
                &run_id,
                started_at,
                &config_hash,
                // Raw `--idempotency-key` so the persisted audit records the
                // claimed key, matching the model-only / replication paths.
                // The claim is finalized under this same value just below.
                idempotency_key,
                // `--var` per-run variables, so `@var()` markers in the
                // transformation pipeline's models resolve to the supplied
                // values (or inline defaults) at compile time.
                run_vars,
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

    let state_store = StateStore::open_with_policy(state_path, rocky_cfg.state.on_schema_mismatch)
        .context(format!(
            "failed to open state store at {}",
            state_path.display()
        ))?;

    // When the on-disk state was forward-incompatible (newer schema than this
    // binary) and `on_schema_mismatch = recreate`, the store was bootstrapped
    // fresh locally. We must NOT write that downgraded state back to a shared
    // remote tier — doing so would clobber the newer state that already-upgraded
    // pods depend on. Suppress both the periodic and the end-of-run upload.
    let suppress_state_upload = state_store.was_recreated_for_forward_incompat();

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
    // (target_ref, asset_key) per materialized table — populated unconditionally
    // so row-level assertions run regardless of row_count/freshness config.
    let mut assertion_targets: Vec<(TableRef, Vec<String>)> = Vec::new();
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
                    // Lift the tenant dimension off the discover-time
                    // schema-pattern components. The map stores it as a
                    // `Value::String`; `None` when the pattern has no
                    // `{tenant}` placeholder. Flows to the materialization
                    // and on to the persisted `ModelExecution`.
                    tenant: components
                        .get("tenant")
                        .and_then(|v| v.as_str())
                        .map(str::to_string),
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
    // Skip-unchanged pruning is active only when the pipeline opts in
    // (`prune_unchanged`) and the run didn't force a full pass (`--no-prune`).
    // Threaded into every `process_table` call in the concurrent loop below.
    let prune_enabled = shared_pipeline.prune_unchanged && !skip_opts.no_prune;
    let mut join_set: JoinSet<(usize, Result<TableOutcome, anyhow::Error>)> = JoinSet::new();

    // Background state sync — flush watermarks to remote storage.
    //
    // Cadence scales with estimated run duration so small runs don't spam
    // the object store. Duration is a coarse estimate of ~3s per table
    // divided by concurrency; the final end-of-run upload (further below)
    // always flushes state, so the periodic loop only exists to bound
    // exposure for long runs. Runs shorter than ~1 minute skip it entirely.
    let estimated_run_secs =
        (tables_to_process.len() as u64).saturating_mul(3) / (concurrency.max(1) as u64);
    let state_sync_handle = if estimated_run_secs < 60 || suppress_state_upload {
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
            let result =
                process_table(warehouse.as_ref(), &state, &pipeline_ref, &task, prune_enabled).await;
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
            Ok((_, Ok(TableOutcome::Pruned(pruned)))) => {
                // A prune is a cheap successful metadata read, not real work —
                // signal the throttle so it can widen concurrency, then record
                // the skip. No materialization is emitted: the orchestrator's
                // satisfy_empty_outputs stamps the 0-row continuity signal.
                if let Some(t) = &throttle {
                    t.on_success();
                    adjust_semaphore(t, &semaphore, &mut semaphore_capacity);
                }
                record_pruned(&mut output, pruned);
            }
            Ok((_, Ok(TableOutcome::Materialized(tr)))) => {
                let tr = *tr;
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
                assertion_targets.push((tr.target_ref.clone(), tr.asset_key.clone()));

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
                let raw = format!("{e:#}");
                if raw.contains("TABLE_OR_VIEW_NOT_FOUND") {
                    warn!(
                        table_index = idx,
                        error = raw.as_str(),
                        "source table not found, skipping"
                    );
                    continue;
                }
                // Frame common warehouse auth failures (403/401) into an
                // actionable message for the user-facing `error` field.
                // The raw status + body stays in the `warn!` below and in
                // the structured `failure_kind`. Falls back to the raw
                // chain for anything we don't recognise.
                let target = tables_to_process
                    .get(idx)
                    .map(|t| format!("{}.{}.{}", t.target_catalog, t.target_schema, t.table_name))
                    .unwrap_or_default();
                let msg = crate::output::frame_warehouse_anyhow_error(&e, &target)
                    .unwrap_or_else(|| raw.clone());

                // Signal the adaptive throttle: rate limits reduce concurrency,
                // other errors are treated as successes (they're permanent
                // failures, not a signal to slow down). Detect on the raw
                // chain — the framed `msg` only covers auth (403/401).
                if let Some(t) = &throttle
                    && is_rate_limit_error(&raw) {
                        t.on_rate_limit();
                        adjust_semaphore(t, &semaphore, &mut semaphore_capacity);
                    }

                // Log the raw status + body for debugging; `msg` (framed)
                // is what surfaces to the user on `TableErrorOutput.error`.
                warn!(error = raw.as_str(), framed = msg.as_str(), "table processing failed");
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
                    // Retry still honours pruning. A failed copy is atomic
                    // (Delta commits don't half-apply), so on failure the target
                    // sits at the last successful copy; if the source marker
                    // still matches that copy the target is already correct and
                    // the table prunes, otherwise the source changed and the
                    // retry copies. Either outcome records the marker on a
                    // successful copy, so a transient failure doesn't force a
                    // re-copy every run thereafter.
                    prune_enabled,
                )
                .await
                {
                    Ok(TableOutcome::Pruned(pruned)) => {
                        record_pruned(&mut output, pruned);
                        info!(table = task.table_name.as_str(), "retry pruned as unchanged");
                    }
                    Ok(TableOutcome::Materialized(tr)) => {
                        let tr = *tr;
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
                        assertion_targets.push((tr.target_ref.clone(), tr.asset_key.clone()));
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

    // Row-level assertions (uniqueness, etc.) on each replication target.
    // The replication runner runs the SAME `[checks].assertions` the quality
    // runner does, so uniqueness fires at replication time. Driven by
    // `assertion_targets` (populated per materialized table, independent of
    // row_count/freshness) so it runs even when those checks are disabled.
    // Results are surfaced advisorily — like every other check in this runner,
    // the run does not bail; the orchestrator decides from the JSON
    // CheckResult + severity. (The data has already landed by check time, so
    // bailing would only change the exit code, not prevent the write.)
    if !pipeline.checks.assertions.is_empty() {
        let dialect = shared_warehouse.dialect();
        for (tref, asset_key) in &assertion_targets {
            let full = match dialect.format_table_ref(&tref.catalog, &tref.schema, &tref.table) {
                Ok(s) => s,
                Err(e) => {
                    warn!(
                        table = %tref.full_name(),
                        error = %e,
                        "assertion table ref formatting failed — skipping"
                    );
                    continue;
                }
            };
            let results = super::run_local::run_table_assertions(
                shared_warehouse.as_ref(),
                dialect,
                &full,
                &tref.table,
                &pipeline.checks.assertions,
            )
            .await;
            if !results.is_empty() {
                let entry =
                    pending_checks
                        .entry(tref.full_name())
                        .or_insert_with(|| PendingCheck {
                            asset_key: asset_key.clone(),
                            checks: Vec::new(),
                        });
                entry.checks.extend(results);
            }
        }
    }

    // Custom SQL checks on each replication target. Ported from the quality
    // runner so `[[checks.custom]]` fires at replication time too. Driven by
    // `assertion_targets` (every materialized table); surfaced advisorily like
    // the other replication checks — the run reports pass/fail in the JSON
    // CheckResult + severity rather than bailing (the data has already landed).
    if !pipeline.checks.custom.is_empty() {
        let dialect = shared_warehouse.dialect();
        for (tref, asset_key) in &assertion_targets {
            let full = match dialect.format_table_ref(&tref.catalog, &tref.schema, &tref.table) {
                Ok(s) => s,
                Err(e) => {
                    warn!(
                        table = %tref.full_name(),
                        error = %e,
                        "custom check table ref formatting failed — skipping"
                    );
                    continue;
                }
            };
            let results = super::run_local::run_custom_checks(
                shared_warehouse.as_ref(),
                &full,
                &pipeline.checks.custom,
            )
            .await;
            if !results.is_empty() {
                pending_checks
                    .entry(tref.full_name())
                    .or_insert_with(|| PendingCheck {
                        asset_key: asset_key.clone(),
                        checks: Vec::new(),
                    })
                    .checks
                    .extend(results);
            }
        }
    }

    // Null-rate checks: sample each configured column and flag when the null
    // fraction exceeds the threshold. `generate_null_rate_sql` returns one row
    // per column `(col, nulls, sampled)`; the rate is computed per row. Like the
    // other replication checks this is advisory — the configured severity rides
    // into the JSON CheckResult and the orchestrator decides.
    if let Some(ref null_rate_cfg) = pipeline.checks.null_rate {
        let dialect = shared_warehouse.dialect();
        for (tref, asset_key) in &assertion_targets {
            let sql = match checks::generate_null_rate_sql(
                tref,
                &null_rate_cfg.columns,
                null_rate_cfg.sample_percent,
                dialect,
            ) {
                Ok(s) => s,
                Err(e) => {
                    warn!(
                        table = %tref.full_name(),
                        error = %e,
                        "null_rate SQL generation failed — skipping"
                    );
                    continue;
                }
            };
            match shared_warehouse.execute_query(&sql).await {
                Ok(result) => {
                    for row in &result.rows {
                        let col = row.first().and_then(|v| v.as_str()).unwrap_or_default();
                        // A parse failure on the counts is a malformed result,
                        // not a real zero — skip the column rather than emit a
                        // false PASS (rate 0.0). An empty (0-row) sample still
                        // parses to a real 0 and correctly passes below.
                        let (Some(nulls), Some(sampled)) =
                            (checks::cell_as_u64(row.get(1)), checks::cell_as_u64(row.get(2)))
                        else {
                            warn!(table = %tref.full_name(), column = col, "null_rate counts unparseable — skipping column");
                            continue;
                        };
                        if col.is_empty() {
                            warn!(table = %tref.full_name(), "null_rate result missing column name — skipping");
                            continue;
                        }
                        let rate = if sampled == 0 {
                            0.0
                        } else {
                            nulls as f64 / sampled as f64
                        };
                        // `check_null_rate` names the result `null_rate:{col}`,
                        // byte-matching discover's projection.
                        let mut check = checks::check_null_rate(col, rate, null_rate_cfg.threshold);
                        check.severity = null_rate_cfg.severity;
                        pending_checks
                            .entry(tref.full_name())
                            .or_insert_with(|| PendingCheck {
                                asset_key: asset_key.clone(),
                                checks: Vec::new(),
                            })
                            .checks
                            .push(check);
                    }
                }
                Err(e) => {
                    warn!(table = %tref.full_name(), error = %e, "null_rate query failed — skipping");
                }
            }
        }
    }

    // Cross-source overlap — flag a business key shared across SIBLING targets:
    // tables with the same source type + table name that landed in more than
    // one target schema (the hierarchy/tenant fan-out that gets unioned
    // downstream, doubling rows). Generic — groups by the discovered source
    // type (asset_key[0]) + table name, with no schema-pattern-component
    // assumptions. Self-limiting: only groups of ≥2 siblings are checked, so a
    // single-target pipeline runs nothing. Surfaced advisorily like the other
    // replication checks; a missing-key/keyless table is skipped with a logged
    // reason rather than failing.
    if let Some(ref overlap_cfg) = pipeline.checks.cross_source_overlap {
        match overlap_cfg.resolved_key_exprs() {
            Ok(key_exprs) => {
                // (source_type, table) -> sibling members (target ref, asset key).
                // The ≥2 grouping and the result name come from shared helpers
                // in rocky-core so `rocky discover` can declare the exact same
                // overlap names this runner emits (byte-match guarantee).
                type SiblingGroups = HashMap<(String, String), Vec<(TableRef, Vec<String>)>>;
                let mut groups: SiblingGroups = HashMap::new();
                let mut pairs: Vec<(String, String)> = Vec::new();
                for (tref, asset_key) in &assertion_targets {
                    let source_type = asset_key.first().cloned().unwrap_or_default();
                    let key = (source_type, tref.table.clone());
                    pairs.push(key.clone());
                    groups
                        .entry(key)
                        .or_default()
                        .push((tref.clone(), asset_key.clone()));
                }
                // Qualifying (source_type, table) keys (≥2 siblings), sorted.
                let qualifying = checks::cross_source_overlap_groups(pairs);

                let dialect = shared_warehouse.dialect();
                for gk in &qualifying {
                    let members = &groups[gk];
                    let (source_type, table) = gk;
                    let siblings: Vec<TableRef> =
                        members.iter().map(|(t, _)| t.clone()).collect();
                    let sql = match rocky_core::checks::generate_cross_source_overlap_sql(
                        &siblings, &key_exprs, dialect,
                    ) {
                        Ok(s) => s,
                        Err(e) => {
                            warn!(source_type, table, error = %e, "cross_source_overlap SQL generation failed — skipping group");
                            continue;
                        }
                    };
                    let name = checks::cross_source_overlap_name(source_type, table);
                    match shared_warehouse.execute_query(&sql).await {
                        Ok(result) => {
                            let overlap_count = result.rows.len() as u64;
                            let sample: Vec<String> = result
                                .rows
                                .iter()
                                .take(overlap_cfg.sample)
                                .map(|row| {
                                    row.iter()
                                        .take(key_exprs.len())
                                        .map(|v| {
                                            v.as_str()
                                                .map(str::to_string)
                                                .unwrap_or_else(|| v.to_string())
                                        })
                                        .collect::<Vec<_>>()
                                        .join(" | ")
                                })
                                .collect();
                            let contributing: Vec<String> =
                                siblings.iter().map(TableRef::full_name).collect();
                            let check = rocky_core::checks::check_cross_source_overlap(
                                name,
                                overlap_count,
                                overlap_cfg.max_overlap_rows,
                                contributing,
                                sample,
                                overlap_cfg.severity,
                            );
                            // Attach to the first sibling's asset (the result's
                            // contributing_tables lists the whole group).
                            let entry = pending_checks
                                .entry(siblings[0].full_name())
                                .or_insert_with(|| PendingCheck {
                                    asset_key: members[0].1.clone(),
                                    checks: Vec::new(),
                                });
                            entry.checks.push(check);
                        }
                        Err(e) => {
                            // A missing key column / keyless table surfaces as a
                            // query error — skip the group with a reason, don't
                            // fail (FR acceptance criterion).
                            warn!(source_type, table, error = %e, "cross_source_overlap query failed (missing key column / keyless table?) — skipping group");
                        }
                    }
                }
            }
            Err(e) => warn!(error = %e, "cross_source_overlap misconfigured — skipping"),
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
                    // No `--model` selection on the full-pipeline path, so
                    // `apply_defer_rewrite` is a no-op even when `--defer` is
                    // set (a full run builds every model).
                    defer_opts,
                    skip_gate,
                    // Reuse is active iff `[reuse]` is enabled AND `--no-reuse`
                    // was not passed (clause 1 of the fail-closed decision).
                    rocky_cfg.reuse.enabled && !skip_opts.no_reuse,
                    run_vars,
                )
                .await?;
                Ok(())
            }
            .await;
            if let Err(e) = exec_result {
                // Record the runtime model failure as a table error and fall
                // through to the terminal emit instead of returning raw `Err`.
                // `execute_models` stops at the first failing model, having
                // already recorded the earlier successes in `output`; returning
                // `Err` here skipped `print_json` + `persist_run_record`, so
                // `--output json` stdout was empty and the process exited 1
                // instead of the PartialFailure sentinel (exit 2). Routing
                // through `table_errors` lets the existing merge + terminal
                // machinery report it like any other partial failure (the
                // copy-failure block below fires `pipeline_error` + drains
                // webhooks once for the accumulated failures), mirroring the
                // model-only path.
                table_errors.push(TableError {
                    asset_key: vec![pipeline_name.to_string()],
                    error: format!("{e:#}"),
                    task_index: None,
                    failure_kind: crate::output::FailureKind::Unknown,
                    cooldown_seconds: None,
                });
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
                    // Resolve `@var()` on the governance compile too, so this
                    // second compile of the same models doesn't re-read raw
                    // `@var()` tokens from disk and spuriously report every
                    // variable as missing (or leave placeholders in the SQL
                    // the masking/tag reconcile inspects).
                    run_vars: run_vars.clone(),
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

                    // --- Per-model governance tags ---
                    //
                    // Apply each model's `[governance.tags]` to its target
                    // securable, reusing the governance compile already in
                    // scope. The `--all`/`--models` path is one of the entry
                    // points the SDK / Dagster drive; without this its tags
                    // were silently dropped (only the full-DAG transformation
                    // path applied them). Strategy-aware dispatch mirrors
                    // [`apply_model_governance_tags`]; best-effort — a failure
                    // warns but never aborts. No-op on DuckDB's Noop adapter.
                    let tags = &model.config.governance.tags;
                    if !tags.is_empty() {
                        let tag_target = governance_tag_target(
                            &model.config.strategy,
                            &model.config.target.catalog,
                            &model.config.target.schema,
                            &model.config.target.table,
                        );
                        if let Err(e) = governance_adapter.set_tags(&tag_target, tags).await {
                            warn!(
                                model = %model.config.name,
                                error = %e,
                                "apply model governance tags failed"
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

    // Upload state to remote storage — unless this store was bootstrapped
    // fresh from a forward-incompatible on-disk schema. In that case the local
    // state is a downgrade; persisting it back would clobber the newer state
    // that already-upgraded pods depend on, so the upload is deliberately
    // skipped (the periodic mid-run uploader above is gated by the same flag).
    if let Err(e) = rocky_core::state_sync::upload_state_unless_recreated(
        suppress_state_upload,
        &rocky_cfg.state,
        state_path,
    )
    .await
    {
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
    // Merge `execute_models`' compile-error bookkeeping with the parallel-copy
    // `table_errors` (see [`merge_replication_compile_and_copy_errors`]) rather
    // than overwriting it, then stamp `output.status`. This keeps a model that
    // fails to type-check (E020) a first-class failure on the replication
    // `--models` path instead of letting the copy bookkeeping clobber it back
    // to status=Success / exit 0.
    merge_replication_compile_and_copy_errors(&mut output, &table_errors);

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
            crate::status_line!(
                "Resumed from {resumed_from} (skipped {} tables)",
                output.tables_skipped
            );
        }
        crate::status_line!(
            "Copied {} tables in {:.1}s (run_id: {run_id})",
            output.tables_copied,
            output.duration_ms as f64 / 1000.0
        );
        if output.drift.tables_drifted > 0 {
            crate::status_line!(
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

    // Compile-only failure on the replication path. The block above only
    // fires when at least one source *copy* failed; a `--models` run where a
    // model fails to compile (E020) but every source copied cleanly leaves
    // `table_errors` empty while `output.tables_failed` is non-zero from the
    // merged compile errors. Without this guard such a run would print its
    // JSON (already carrying the compile errors + a non-`success` status) and
    // then fall through to `Ok(())` / exit 0 — exactly the silent skip this
    // change closes. Re-derive the exit contract from the merged tallies so it
    // fails loudly, matching the transformation / model-only paths.
    if output.tables_failed > 0 {
        let msg = format!(
            "{} model(s) failed to compile (run_id: {run_id}, see the `errors` array in the JSON output)",
            output.tables_failed
        );
        let _ = hook_registry
            .fire(&HookContext::pipeline_error(&run_id, pipeline_name, &msg))
            .await;
        let _ = hook_registry.wait_async_webhooks().await;
        return run_status_exit_result(&output, &run_id);
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
/// Rewrite the build-selected models' SQL so bare references to **unbuilt**
/// upstreams resolve against the defer target (`--defer`).
///
/// The deferred set is every compiled project model whose name is not the
/// `--model` selection. For each selected model, bare `ref()`s to a deferred
/// model are qualified to `<defer_schema>.<table>` (or
/// `<catalog>.<defer_schema>.<table>`), where `defer_schema` is the upstream's
/// own configured target schema (its production home) unless `--defer-to`
/// overrides it. Models in the selection build normally and keep bare refs to
/// each other.
///
/// No-op (returns `Ok` without mutating) when there is no `--model` selection
/// — a full run builds every model, so nothing is deferred.
///
/// The deferred upstream's **catalog** is validated with the rule that matches
/// `dialect`: BigQuery allows hyphenated project IDs (validated via
/// `validate_gcp_project_id`) and the rewritten reference is backtick-quoted to
/// match the dialect's emitted form; every other dialect keeps the strict
/// `validate_identifier` rule and renders bare identifiers. Schema and table
/// always use `validate_identifier` — datasets/schemas and tables have no
/// hyphens on any supported warehouse.
///
/// # Errors
///
/// Returns an error when `--defer-to` (or a deferred upstream's catalog /
/// schema / table) fails the applicable identifier-validation rule, or when a
/// selected model's SQL cannot be parsed and rewritten. Identifier validation
/// guards against interpolating an unvalidated string into the rewritten
/// reference.
fn apply_defer_rewrite(
    compile_result: &mut rocky_compiler::compile::CompileResult,
    model_name_filter: Option<&str>,
    defer_opts: &DeferOptions,
    dialect: &dyn rocky_core::traits::SqlDialect,
) -> Result<()> {
    use std::collections::{HashMap, HashSet};

    // Without a `--model` selection every model is built; there are no
    // unbuilt upstreams to defer. Leave the SQL untouched.
    let Some(selected) = model_name_filter else {
        return Ok(());
    };

    // BigQuery is the one shipping dialect whose qualified parts (the project /
    // catalog) may contain characters bare SQL identifiers reject — hyphens.
    // For it the catalog is validated with the GCP-project rule and the
    // rewritten reference is backtick-quoted to match `format_table_ref`; every
    // other dialect keeps the strict identifier rule and renders bare names.
    let is_bigquery = dialect.name() == "bigquery";
    let quote_style: Option<char> = if is_bigquery { Some('`') } else { None };

    // Validate the `--defer-to` schema override once up front — it is user
    // input that ends up serialized into SQL. Schemas/datasets never carry
    // hyphens, so the strict identifier rule applies on every dialect.
    if let Some(schema) = &defer_opts.defer_to {
        rocky_sql::validation::validate_identifier(schema)
            .with_context(|| format!("invalid --defer-to schema '{schema}'"))?;
    }

    // Build the set of selected (built-locally) model names. Today `--model`
    // selects exactly one model, but treat it as a set so the logic survives
    // a future multi-select.
    let selected_set: HashSet<&str> = std::iter::once(selected).collect();

    // The deferred set = every compiled model not in the selection, mapped to
    // its qualified defer target. `--defer-to` overrides the schema part;
    // catalog + table always come from the upstream's own configured target.
    let mut deferred: HashMap<String, rocky_sql::defer::DeferTarget> = HashMap::new();
    for model in &compile_result.project.models {
        let name = model.config.name.as_str();
        if selected_set.contains(name) {
            continue;
        }
        let target = &model.config.target;
        let schema = defer_opts
            .defer_to
            .clone()
            .unwrap_or_else(|| target.schema.clone());
        // Defense in depth: validate every part before it is serialized into
        // the rewritten reference. The catalog uses the dialect-appropriate
        // rule (BigQuery project IDs allow hyphens); schema + table always use
        // the strict identifier rule regardless of where the schema came from
        // (config target or `--defer-to`).
        if !target.catalog.is_empty() {
            if is_bigquery {
                rocky_sql::validation::validate_gcp_project_id(&target.catalog).with_context(
                    || format!("deferred upstream '{name}' has an invalid BigQuery project ID"),
                )?;
            } else {
                rocky_sql::validation::validate_identifier(&target.catalog).with_context(|| {
                    format!("deferred upstream '{name}' has an invalid catalog identifier")
                })?;
            }
        }
        rocky_sql::validation::validate_identifier(&schema).with_context(|| {
            format!(
                "deferred upstream '{name}' resolves to an invalid schema identifier '{schema}'"
            )
        })?;
        rocky_sql::validation::validate_identifier(&target.table).with_context(|| {
            format!("deferred upstream '{name}' has an invalid table identifier")
        })?;
        deferred.insert(
            model.config.name.clone(),
            rocky_sql::defer::DeferTarget {
                catalog: target.catalog.clone(),
                schema,
                table: target.table.clone(),
                quote_style,
            },
        );
    }

    if deferred.is_empty() {
        return Ok(());
    }

    // Rewrite each selected model's SQL in place. Only the selected models
    // execute, so rewriting just those is sufficient (and avoids touching
    // upstream SQL that won't run).
    for model in &mut compile_result.project.models {
        if !selected_set.contains(model.config.name.as_str()) {
            continue;
        }
        let rewritten =
            rocky_sql::defer::qualify_deferred_refs(&model.sql, &deferred).map_err(|e| {
                anyhow::anyhow!(
                    "`--defer` could not rewrite the upstream references in model '{}': its SQL \
                     did not parse ({e}). `--defer` parses each selected model's SQL to qualify \
                     deferred upstreams; constructs the parser does not yet support (e.g. \
                     `SELECT * EXCEPT (...)`, trailing-comma select lists, `STRUCT(...)` \
                     literals) cannot be rewritten. Build this model without `--defer`, or \
                     adjust its SQL.",
                    model.config.name,
                )
            })?;
        model.sql = rewritten;
    }

    Ok(())
}

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
    // `--defer` selection state. When enabled (and a `model_name_filter` is
    // set), every project model NOT in the selection is treated as a deferred
    // upstream: the selected models' bare `ref()`s to those upstreams are
    // rewritten to qualified `<defer_schema>.<table>` references before SQL
    // generation. Default OFF ⇒ no rewrite, byte-identical behavior.
    defer_opts: &DeferOptions,
    // Fully-resolved model-skip gate config. When `!skip_gate.is_active()`
    // the gate is inert and this function builds every selected model
    // exactly as before — no extra state reads or warehouse queries.
    skip_gate: SkipGateConfig,
    // Opt-in `[reuse]` (= `[reuse].enabled && !--no-reuse`). When `false`
    // (the default) no input-match index entry or provenance record is
    // written and no per-model spine hashing cost is paid — the run is byte-
    // and cost-identical, and no model reuses. When `true`, the spine is
    // populated AND an eligible model whose inputs match a prior strong run
    // points-to that run's parquet instead of executing its SQL.
    reuse_enabled: bool,
    // Per-run variables (`rocky run --var name=value`). Threaded into the
    // compile config so `@var(name)` placeholders in model SQL resolve to the
    // supplied value (or inline default) before typecheck and SQL generation.
    // Empty for the call sites that don't expose `--var` (watch / dag / local
    // / tests).
    run_vars: &rocky_core::run_vars::RunVars,
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
        run_vars: run_vars.clone(),
        ..Default::default()
    };

    let mut compile_result = match rocky_compiler::compile::compile(&compile_config) {
        Ok(r) => r,
        Err(e) => {
            // A whole-project compile failure (couldn't even produce a
            // result — bad models dir, unparseable project) means nothing
            // can execute. Surface it as a first-class run failure instead
            // of returning `Ok(())`: record an error entry + bump
            // `tables_failed` so `derive_run_status()` yields `Failure`,
            // the caller emits the JSON, and the process exits non-zero.
            // Without this a broken project reported `status: "Success"`,
            // exit 0, with zero data materialized.
            warn!(error = %e, "model compilation failed — no models can execute");
            output.tables_failed += 1;
            output.errors.push(crate::output::TableErrorOutput {
                asset_key: vec!["<compile>".to_string()],
                error: format!("model compilation failed: {e:#}"),
                failure_kind: crate::output::FailureKind::CompileError,
                cooldown_seconds: None,
            });
            return Ok(());
        }
    };

    // Per-model compile errors are first-class run failures, not silent
    // skips. Each model that fails to type-check (e.g. E020 — a
    // `time_interval` model whose `time_column` is absent from its SELECT
    // output) is recorded in `output.errors` with the diagnostic, counted
    // in `tables_failed`, and excluded from execution. Models that compiled
    // cleanly still build, so a mixed project is a `PartialFailure` (good
    // models materialize) rather than a green run that dropped data.
    //
    // Before this, `execute_models` warned and `return`ed `Ok(())`, so the
    // run reported `status: "Success"`, exit 0, `materializations: []` —
    // an orchestrator consuming `--output json` saw fully green while zero
    // data materialized.
    let mut compile_failed_models: std::collections::BTreeSet<String> =
        std::collections::BTreeSet::new();
    if compile_result.has_errors {
        for d in &compile_result.diagnostics {
            if d.is_error() {
                warn!(
                    model = d.model.as_str(),
                    code = &*d.code,
                    message = &*d.message,
                    "compile error"
                );
                // Surface every error diagnostic as a run error so JSON
                // consumers see the code + message; count each distinct
                // model once toward `tables_failed`.
                if compile_failed_models.insert(d.model.clone()) {
                    output.tables_failed += 1;
                }
                output.errors.push(crate::output::TableErrorOutput {
                    asset_key: vec![d.model.clone()],
                    error: format!("[{}] {}", &*d.code, &*d.message),
                    failure_kind: crate::output::FailureKind::CompileError,
                    cooldown_seconds: None,
                });
            }
        }
        warn!(
            failed_models = compile_failed_models.len(),
            "model(s) failed to compile — excluded from execution; run will report failure"
        );
    }

    // `--defer`: rewrite the selected models' bare upstream `ref()`s to point
    // at the defer target (production) for every model NOT in the selection.
    // Lineage was already computed during compile, so mutating `.sql` now is
    // safe — it only affects the SQL fed to `to_model_ir()` downstream. A
    // no-op when `--defer` is off, when there's no `--model` selection, or
    // when the selected models reference no deferred upstreams.
    if defer_opts.enabled {
        apply_defer_rewrite(
            &mut compile_result,
            model_name_filter,
            defer_opts,
            warehouse.dialect(),
        )?;
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

    // Auditable-reuse spine accumulator. When `[reuse]` is enabled, each
    // successfully-built content-addressed model contributes an input-match
    // index entry + an offline-verifiable provenance record, flushed in one
    // transaction at end-of-run; that index is what a later run's reuse
    // decision reads to point-to a prior run. Empty (and never touched) when
    // reuse is off, so the default path pays nothing.
    //
    // `reuse_outputs` maps a built model's fully-qualified target identity to
    // its output blake3 keyed by its target identity, so a *downstream*
    // content-addressed model in a later layer can resolve a table it reads to
    // a STRONG content-hash identity without any extra warehouse query. Stage 1
    // indexes a model only when EVERY table it reads resolves to a content
    // hash; a raw-source read (or any unresolved read) leaves the model
    // unindexed rather than mislabeled strong.
    let mut reuse_index_entries: Vec<rocky_core::state::InputIndexEntry> = Vec::new();
    let mut reuse_provenance: Vec<rocky_core::state::ProvenanceRecord> = Vec::new();
    let mut reuse_outputs: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();

    // Producer-side per-column output hashes for every content-addressed model
    // built earlier in THIS run, keyed by target `catalog.schema.table` full
    // name (as `reuse_outputs` is). A later content-addressed model reads this
    // to record what its consumed columns hashed to at build time — the
    // consumer-side per-column baseline (schema v13, record-only). Populated
    // independently of `[reuse]` (the consumer baseline is content-addressed
    // path, not reuse, scoped); empty and untouched when no content-addressed
    // model produces column hashes.
    let mut built_output_hashes: std::collections::HashMap<
        String,
        Vec<rocky_core::state::ColumnHash>,
    > = std::collections::HashMap::new();

    // Static lookup built once when reuse is on: every lowercased identity a
    // model can be UNAMBIGUOUSLY *read by* in SQL → that model's target
    // `catalog.schema.table` full name. Lets the read-set resolver map a table
    // reference from SQL lineage to the producing model's recorded output.
    // Empty when reuse is off so the default path allocates nothing.
    let reuse_target_by_model: std::collections::HashMap<String, String> = if reuse_enabled {
        build_reuse_target_by_model(
            compile_result
                .project
                .models
                .iter()
                .map(|m| (m.config.name.as_str(), &m.config.target)),
        )
    } else {
        Default::default()
    };

    // The same read-by → target-full lookup, always built: the consumer-side
    // per-column baseline is scoped to the content-addressed path, which runs
    // with or without `[reuse]`, so it can't borrow the reuse-gated map above.
    // Cheap (one entry per model name + identity) and only consulted when a
    // content-addressed model records its baseline.
    let baseline_target_by_model = build_reuse_target_by_model(
        compile_result
            .project
            .models
            .iter()
            .map(|m| (m.config.name.as_str(), &m.config.target)),
    );

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
    // Per-model surrogate-key specs, loaded once and threaded via the context.
    // Surface a malformed key spec (bad output name, empty/invalid input
    // columns) as a hard run failure rather than silently emitting broken SQL.
    let surrogate_keys = rocky_core::models::load_surrogate_keys_from_dir(models_dir)
        .context("invalid surrogate_key configuration")?;
    let exec_ctx = ExecutionContext {
        typed_models: &compile_result.type_check.typed_models,
        model_timings: &compile_result.model_timings,
        surrogate_keys: &surrogate_keys,
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

    // Opt-in model-skip gate. Inert unless `skip_gate.is_active()`; when
    // inactive the gate below short-circuits and `execute_models` builds every
    // model exactly as before (no extra state reads / warehouse queries).
    let mut gate = super::skip_gate::SkipGate::new(skip_gate, &compile_result.project);

    for layer in &compile_result.project.layers {
        // Collect this layer's matched models (honouring the name filter),
        // tagged with their stable within-layer index so concurrent results
        // can be re-ordered to match serial output exactly.
        let matched: Vec<(usize, &String, &rocky_core::models::Model)> = layer
            .iter()
            .filter(|name| model_name_filter.is_none_or(|target| target == name.as_str()))
            // Exclude models that failed to compile — they're already
            // recorded as failures in `output.errors` above. A downstream
            // model that `ref()`s an excluded one will fail at execution
            // (missing table) and surface through the normal error path.
            .filter(|name| !compile_failed_models.contains(name.as_str()))
            .filter_map(|name| compile_result.project.model(name).map(|m| (name, m)))
            .enumerate()
            .map(|(idx, (name, model))| (idx, name, model))
            .collect();

        // ---- skip gate (B2 ∧ B3) --------------------------------------
        // Evaluate the gate over this layer's matched set BEFORE dispatch, so
        // the serial and `--parallel` paths see identical verdicts and the B3
        // recursion map is fully populated layer-by-layer. The result is a
        // `to_build` subset (each carrying the `ModelSkipState` to stamp on a
        // successful build) plus the skipped models, whose verdict is recorded
        // and whose `tables_skipped` counter is bumped. When the gate is
        // inactive, every model passes straight through with no skip state.
        let mut to_build: Vec<(
            usize,
            &String,
            &rocky_core::models::Model,
            Option<crate::output::ModelSkipState>,
        )> = Vec::with_capacity(matched.len());
        if gate.config().is_active() {
            for &(idx, name, model) in &matched {
                // Build the fully-typed IR (merge the typechecker's columns,
                // mirroring `project_ir_from_compile`) so `skip_hash()` can
                // canonicalise. An empty `typed_columns` ⇒ `skip_hash()` is
                // `None` ⇒ the gate builds.
                let typed_ir = typed_model_ir(
                    model,
                    exec_ctx.typed_models,
                    exec_ctx.surrogate_keys,
                    dialect,
                );
                match gate
                    .evaluate(model, &typed_ir, warehouse, state_store)
                    .await
                {
                    super::skip_gate::GateDecision::Skip { reason } => {
                        gate.record(name, super::skip_gate::Verdict::Skipped);
                        output.tables_skipped += 1;
                        // Surface the per-model decision (reporting-only —
                        // mirrors the log line below, never changes behavior).
                        output
                            .model_decisions
                            .push(crate::output::ModelDecisionOutput {
                                model: name.to_string(),
                                decision: crate::output::ModelDecision::Skip,
                                reason: reason.message().to_string(),
                            });
                        info!(
                            model = name.as_str(),
                            reason = reason.as_str(),
                            "skip-unchanged: logic and upstream data both appear \
                             unchanged — skipping re-materialization (best-effort)"
                        );
                    }
                    super::skip_gate::GateDecision::Build { skip_state, reason } => {
                        // Record the Build decision here for the *plain*
                        // strategies the gate actually adjudicates. The two
                        // special strategies (`content_addressed`,
                        // `time_interval`) are never skip-eligible and own
                        // their own decision entry at their execution site
                        // (content_addressed surfaces Build/Reused there,
                        // time_interval surfaces Build), so suppress a
                        // duplicate here.
                        if is_plain_strategy(model) {
                            output
                                .model_decisions
                                .push(crate::output::ModelDecisionOutput {
                                    model: name.to_string(),
                                    decision: crate::output::ModelDecision::Build,
                                    reason: reason.message().to_string(),
                                });
                        }
                        to_build.push((idx, name, model, Some(skip_state)));
                    }
                }
            }
        } else {
            for &(idx, name, model) in &matched {
                to_build.push((idx, name, model, None));
            }
        }

        // Re-tag with a dense within-layer index so the parallel path's
        // result re-ordering stays contiguous after any skips. Preserves the
        // matched-order shape (`(idx, name, model)`) the dispatch loops below
        // expect, with the optional skip state kept alongside.
        let matched: Vec<(usize, &String, &rocky_core::models::Model)> = to_build
            .iter()
            .enumerate()
            .map(|(i, (_, name, model, _))| (i, *name, *model))
            .collect();
        let skip_states: Vec<Option<crate::output::ModelSkipState>> =
            to_build.into_iter().map(|(_, _, _, s)| s).collect();

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
            for (idx, name, result) in layer_results {
                match result {
                    Ok(mut materialization) => {
                        let model_duration_ms = materialization.duration_ms;
                        // Stamp the gate's logic key + upstream signatures so
                        // the next run can compare against this build, and
                        // record the model as Built so any downstream in a
                        // later layer treats it as a changed input. `skip_states`
                        // is index-aligned with the (post-gate) `matched` set.
                        if let Some(state) = skip_states.get(idx).and_then(Clone::clone) {
                            materialization.skip_internal = Some(state);
                        }
                        gate.record(name, super::skip_gate::Verdict::Built);
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

        for &(idx, model_name, model) in &matched {
            let model_name: &str = model_name.as_str();
            // Every model reached here is being built (the gate already
            // removed any skips). Record the Built verdict so a downstream in
            // a later layer treats it as a changed input. `content_addressed`
            // / `time_interval` are never skip-eligible but still count as a
            // build for the B3 recursion signal.
            gate.record(model_name, super::skip_gate::Verdict::Built);

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
                // Build the fail-closed reuse-decision context (clauses 1–3
                // input side). Only for an unpartitioned (clause 2) model when
                // `[reuse]` is active (clause 1) and a state store exists —
                // otherwise pass `None` for a zero-cost pure BUILD. The
                // decision-time `input_hash` is recomputed from the SAME
                // all-strong upstream chain + `skip_hash` + target that spine
                // population uses, so a hit is only possible when the inputs
                // genuinely match a prior strong run.
                let reuse_ctx = if reuse_enabled {
                    let model_is_unpartitioned = !matches!(
                        &model_ir.materialization,
                        MaterializationStrategy::ContentAddressed { partition_columns, .. }
                            if !partition_columns.is_empty()
                    );
                    match (model_is_unpartitioned, state_store) {
                        (true, Some(store)) => {
                            let input_hash = compute_decision_input_hash(
                                &model_ir,
                                &reuse_target_by_model,
                                &reuse_outputs,
                            );
                            Some(super::run_content_addressed::ReuseDecisionCtx {
                                input_hash,
                                state_store: store,
                                run_id,
                            })
                        }
                        _ => None,
                    }
                } else {
                    None
                };
                let summary = super::run_content_addressed::execute_content_addressed_model(
                    &model_ir, warehouse, reuse_ctx,
                )
                .await
                .with_context(|| format!("model '{model_name}' failed"));
                match summary {
                    Ok(summary) => {
                        let duration_ms = model_start.elapsed().as_millis() as u64;
                        // Consumer-side per-column baseline (schema v13,
                        // record-only). For each upstream this model provably
                        // consumes, record the columns `consumed(D, U)` paired
                        // with `U`'s producer output-column hashes as observed
                        // at THIS build — what `D` read, not producer-now-vs-
                        // prior (Fork 2). `None` when the consumed set isn't
                        // provably complete (⇒ a safe rebuild in the later
                        // gate). Nothing consults it yet.
                        let consumed_column_baseline = compute_consumer_baseline(
                            &model_ir.sql,
                            &baseline_target_by_model,
                            &built_output_hashes,
                        );
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
                            // Transformation models carry no tenant
                            // dimension (no source-schema `{tenant}`).
                            tenant: None,
                            job_ids: vec![],
                            // content_addressed is never skip-eligible.
                            skip_internal: None,
                            recipe_identity: Some(crate::output::recipe_identity_internal(
                                &model_ir,
                                warehouse.dialect().name(),
                            )),
                            // Producer-side per-output-column content hashes for
                            // this content-addressed build. Empty (⇒ `None`) on a
                            // point-to reuse or a partitioned table — both degrade
                            // to a safe rebuild in the later skip gate. Captured
                            // only; nothing consults it yet.
                            output_column_hashes: (!summary.output_column_hashes.is_empty())
                                .then(|| summary.output_column_hashes.clone()),
                            // The consumer-side per-column baseline computed
                            // above; routed onto `ModelExecution.upstream_freshness`
                            // by `to_run_record`.
                            consumed_column_baseline,
                        });
                        // Make this model's producer column hashes visible to
                        // later content-addressed models that consume it, so a
                        // downstream can record what it read from this upstream.
                        // Recorded only when the build produced hashes: a
                        // genuine unpartitioned build always does; a point-to
                        // reuse carries the referenced run's recorded hashes
                        // when present; a partitioned build produces none (the
                        // per-file fold is deferred). An absent entry degrades a
                        // downstream baseline to a safe rebuild. Keyed by target
                        // full name as `reuse_outputs` is.
                        if !summary.output_column_hashes.is_empty() {
                            built_output_hashes.insert(
                                target_table_full_name.clone(),
                                summary.output_column_hashes.clone(),
                            );
                        }
                        // Per-model decision (reporting-only). A
                        // content_addressed model is never skip-eligible, so it
                        // is suppressed in the skip-gate loop above and owns its
                        // decision here: `Reused` when the zero-copy point-to
                        // landed (no SQL ran), else `Build`. Only emitted when
                        // the skip gate is active or `[reuse]` is enabled so a
                        // default run's output stays byte-identical.
                        if gate.config().is_active() || reuse_enabled {
                            let (decision, reason) = match &summary.reused_from {
                                Some(reuse) => (
                                    crate::output::ModelDecision::Reused,
                                    format!(
                                        "reused prior run {}'s bytes ({} proof)",
                                        reuse.reused_run_id, reuse.proof_class
                                    ),
                                ),
                                None if reuse_enabled => (
                                    crate::output::ModelDecision::Build,
                                    "content-addressed: no reusable prior run; built".to_string(),
                                ),
                                None => (
                                    crate::output::ModelDecision::Build,
                                    "content-addressed: built".to_string(),
                                ),
                            };
                            output
                                .model_decisions
                                .push(crate::output::ModelDecisionOutput {
                                    model: model_name.to_string(),
                                    decision,
                                    reason,
                                });
                        }
                        info!(
                            model = model_name,
                            target = target_table_full_name.as_str(),
                            num_rows = summary.num_rows,
                            commit_version = summary.commit_version,
                            blake3 = summary.blake3_hash.as_str(),
                            file_path = summary.file_path.as_str(),
                            reused = summary.reused_from.is_some(),
                            "content_addressed model materialized"
                        );
                        // Point-to reuse: this run referenced a prior run R's
                        // bytes (zero copy) instead of executing the SQL. The
                        // artifact + spine writes below run unchanged — they
                        // key off `summary.blake3_hash`, which is R's shared
                        // blake3 here, so the ledger gains a SECOND reference
                        // at that hash (`refcount_for_hash` >= 2) and the spine
                        // records this run's input-match entry at R's hash. The
                        // back-link to R is surfaced here for the run log.
                        if let Some(reuse) = &summary.reused_from {
                            info!(
                                model = model_name,
                                target = target_table_full_name.as_str(),
                                reused_run_id = reuse.reused_run_id.as_str(),
                                proof_class = reuse.proof_class.as_str(),
                                blake3 = reuse.blake3_hash.as_str(),
                                "content_addressed model REUSED prior run R's parquet \
                                 (no SQL executed; shared-bytes reference recorded)"
                            );
                        }
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
                        // Auditable-reuse spine. When `[reuse]` is enabled,
                        // accumulate this model's input-match index entry +
                        // provenance record. A model is indexed only when
                        // EVERY table it reads resolves to a STRONG content
                        // hash produced earlier in this run — no extra
                        // warehouse query. A read of a raw source, a model not
                        // written content-addressed, or a model not built this
                        // run means the chain is not byte-verifiable
                        // end-to-end, so the model is left unindexed rather
                        // than mislabeled strong (heuristic/watermark
                        // population is deferred). Best-effort and additive: it
                        // never changes what was materialized.
                        //
                        // Only UNPARTITIONED writes participate: the
                        // partitioned ledger is last-group-only (the Phase 6
                        // TODO above), so a partitioned hash is incomplete and
                        // is recorded neither as this model's identity nor as a
                        // resolvable upstream.
                        if reuse_enabled {
                            let is_unpartitioned = !matches!(
                                &model_ir.materialization,
                                MaterializationStrategy::ContentAddressed { partition_columns, .. }
                                    if !partition_columns.is_empty()
                            );
                            if is_unpartitioned {
                                if let Some(upstreams) = resolve_read_set_content_upstreams(
                                    &model_ir.sql,
                                    &reuse_target_by_model,
                                    &reuse_outputs,
                                ) {
                                    let outputs = vec![rocky_core::reuse::OutputArtifact {
                                        blake3_hash: summary.blake3_hash.clone(),
                                        file_path: summary.file_path.clone(),
                                    }];
                                    if let Some((entry, prov)) = rocky_core::reuse::build_records(
                                        &model_ir,
                                        run_id,
                                        &upstreams,
                                        &outputs,
                                        Utc::now(),
                                    ) {
                                        reuse_index_entries.push(entry);
                                        reuse_provenance.push(prov);
                                    }
                                }
                                // Record THIS model's content identity only
                                // after the resolve above, and only for an
                                // unpartitioned (complete-hash) write, so a
                                // downstream can resolve it as a STRONG
                                // upstream without inheriting a partial hash.
                                reuse_outputs.insert(
                                    target_table_full_name.clone(),
                                    summary.blake3_hash.clone(),
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
                // Per-model decision (reporting-only). time_interval is never
                // skip-eligible, so it is suppressed in the skip-gate loop above
                // and owns its `Build` entry here. Gated on the skip gate being
                // active (reuse does not apply to time_interval) so a default
                // run's output stays byte-identical.
                if gate.config().is_active() {
                    output
                        .model_decisions
                        .push(crate::output::ModelDecisionOutput {
                            model: model_name.to_string(),
                            decision: crate::output::ModelDecision::Build,
                            reason: "time_interval: built (not skip-eligible)".to_string(),
                        });
                }
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
                Ok(mut materialization) => {
                    let model_duration_ms = materialization.duration_ms;
                    // Stamp the gate's logic key + upstream signatures so the
                    // next run can compare against this build. `skip_states`
                    // is index-aligned with the (post-gate) `matched` set;
                    // `None` when the gate is off or this model wasn't
                    // skip-eligible.
                    if let Some(state) = skip_states.get(idx).and_then(Clone::clone) {
                        materialization.skip_internal = Some(state);
                    }
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

    // Flush the auditable-reuse spine in one transaction at end-of-run, only
    // on the success path (a failed run returns early above and discards the
    // accumulator — only fully-successful builds are indexed).
    // Best-effort: a write failure logs and continues, mirroring the
    // `record_artifact` posture — the run itself is already durable.
    if reuse_enabled
        && !(reuse_index_entries.is_empty() && reuse_provenance.is_empty())
        && let Some(store) = state_store
    {
        if let Err(e) = store.record_reuse_spine(&reuse_index_entries, &reuse_provenance) {
            warn!(
                error = %e,
                entries = reuse_index_entries.len(),
                "failed to persist auditable-reuse spine (run still successful; \
                 index/provenance may be incomplete)"
            );
        } else {
            info!(
                entries = reuse_index_entries.len(),
                "recorded auditable-reuse input-match spine"
            );
        }
    }

    info!(models = models_executed, "model execution complete");
    Ok(())
}

/// Build the static `read-identity → target catalog.schema.table` lookup the
/// reuse read-set resolver consults.
///
/// Keys a model only by identities that bind UNAMBIGUOUSLY to it:
///
/// - its lowercased bare model NAME (the compiler rejects duplicate model
///   names at `project.rs`, so a 1-part read resolves to exactly one model);
/// - its lowercased fully-qualified `catalog.schema.table` target identity (a
///   3-part read names a single catalog).
///
/// The catalog-less `schema.table` form is deliberately **omitted**: two
/// models targeting `cat1.marts.orders` and `cat2.marts.orders` share the same
/// `marts.orders` and the compiler permits that (it only rejects duplicate
/// model names). Keying on `schema.table` would let a catalog-ambiguous 2-part
/// read bind to an arbitrary one of them and inherit its content hash under a
/// `strong` label — the exact false-strong this resolver must refuse. A 2-part
/// read therefore finds no key here and is left unresolved (see
/// [`resolve_read_set_content_upstreams`]).
fn build_reuse_target_by_model<'a, I>(models: I) -> std::collections::HashMap<String, String>
where
    I: IntoIterator<Item = (&'a str, &'a rocky_core::models::TargetConfig)>,
{
    let mut map = std::collections::HashMap::new();
    for (name, t) in models {
        let target_full = format!("{}.{}.{}", t.catalog, t.schema, t.table);
        map.insert(target_full.to_lowercase(), target_full.clone());
        map.insert(name.to_lowercase(), target_full);
    }
    map
}

/// Compute the **consumer-side per-column baseline** for a content-addressed
/// model `D` at build time: for every upstream `U` that `D` *provably* consumes,
/// the columns `consumed(D, U)` paired with `U`'s producer output-column hashes
/// as observed during this run.
///
/// This is the record-only half of T2's per-column skip substrate (schema v13).
/// It is **consumer-side and local** (design Fork 2): it records what `D` read
/// from each upstream *at this build*, reconstructible from the stored snapshot
/// plus the current run — never a producer-now-vs-producer-prior inference.
/// Nothing consults the result yet; the skip gate is a later phase.
///
/// Fails **closed** to `None` (no baseline ⇒ a safe rebuild in the later gate,
/// never a wrong skip) whenever the consumed set can't be proven complete
/// ([`rocky_sql::consumed_columns`] returns
/// [`ForceBuild`](rocky_sql::consumed_columns::ConsumedColumns::ForceBuild)).
///
/// Arguments:
/// - `sql` — `D`'s model SQL.
/// - `target_by_model` — read-by identity (lowercased bare name or 3-part
///   `catalog.schema.table`) → the producing model's target full name, from
///   [`build_reuse_target_by_model`].
/// - `built_output_hashes` — target full name → that upstream's producer
///   output-column hashes recorded earlier in this run.
///
/// Each returned [`UpstreamSig`](rocky_core::state::UpstreamSig) carries only
/// the column baseline (`max_ts` / `row_count` are `None` — freshness is the
/// plain-strategy gate's concern, not this path). An upstream's
/// `consumed_column_hashes` is:
/// - `Some(hashes)` — the consumed columns resolved to a producer that recorded
///   hashes this run and **every** consumed column matched (case-insensitively,
///   since [`consumed_columns`](rocky_sql::consumed_columns) lowercases column
///   names while the producer keeps the original Arrow field name);
/// - `None` — the read didn't resolve to a project model (a raw source, or a
///   catalog-ambiguous 2-part name absent from `target_by_model`), the upstream
///   recorded no hashes this run (not built content-addressed, reused, or
///   partitioned), or a consumed column had no matching producer hash. Recorded
///   honestly so the later gate sees "consumed, but no column baseline ⇒ build".
fn compute_consumer_baseline(
    sql: &str,
    target_by_model: &std::collections::HashMap<String, String>,
    built_output_hashes: &std::collections::HashMap<String, Vec<rocky_core::state::ColumnHash>>,
) -> Option<Vec<rocky_core::state::UpstreamSig>> {
    use rocky_sql::consumed_columns::{ConsumedColumns, consumed_columns};

    let ConsumedColumns::Complete(consumed) = consumed_columns(sql) else {
        // Consumed set not provably complete ⇒ no baseline (fail closed).
        return None;
    };

    let mut sigs = Vec::with_capacity(consumed.len());
    for (source_key, cols) in &consumed {
        let (upstream_key, consumed_column_hashes) = match target_by_model.get(source_key) {
            Some(target_full) => {
                // Filter the upstream's producer hashes to the consumed columns.
                // Every consumed column must resolve to a producer hash, else
                // this upstream can't back a column-level skip (fail closed to
                // `None`). Case-insensitive: `cols` are lowercased,
                // `ColumnHash.column` is the original Arrow field name.
                let hashes = built_output_hashes.get(target_full).and_then(|producer| {
                    let by_col: std::collections::HashMap<String, &rocky_core::state::ColumnHash> =
                        producer
                            .iter()
                            .map(|ch| (ch.column.to_lowercase(), ch))
                            .collect();
                    let mut filtered = Vec::with_capacity(cols.len());
                    for col in cols {
                        filtered.push((*by_col.get(col)?).clone());
                    }
                    Some(filtered)
                });
                (target_full.clone(), hashes)
            }
            // Unresolved read (raw source / catalog-ambiguous): no column
            // baseline for it, keyed by the name as written.
            None => (source_key.clone(), None),
        };
        sigs.push(rocky_core::state::UpstreamSig {
            upstream_key,
            max_ts: None,
            row_count: None,
            consumed_column_hashes,
        });
    }
    Some(sigs)
}

/// Resolve a content-addressed model's immediate upstreams to STRONG
/// content-hash identities for the auditable-reuse spine.
///
/// Enumerates **every table the model reads** from its SQL lineage — not just
/// its project-model dependencies — and resolves each to the content blake3 a
/// project model produced earlier in this run. The read set is trusted only
/// when [`rocky_sql::lineage_complete::lineage_is_provably_complete`] holds
/// (a plain `SELECT` over bare tables, no CTEs/subqueries that the lineage
/// walk could silently drop); any other shape returns `None` so an
/// un-enumerable read can never be mistaken for "no raw-source reads". This is
/// the same completeness fail-safe `skip_gate` uses.
///
/// Only an UNAMBIGUOUS read resolves: a 1-part bare name (unique per project)
/// or a 3-part `catalog.schema.table` (names a single catalog). A 2-part
/// `schema.table` read is **catalog-ambiguous** — it binds to the session
/// default catalog at runtime and `classify_table_ref` already treats it as an
/// external source with no DAG edge — so it is refused here (returns `None` for
/// that read ⇒ the whole model is left UNINDEXED, the same fail-safe as a
/// raw-source read). Without this guard a 2-part read could collide with a
/// project model in a *different* catalog and inherit its content hash under a
/// `strong` label.
///
/// Returns `Some(_)` **only when every** read table maps (via
/// `target_by_model`, keyed by lowercased bare name and full 3-part identity)
/// to a producing model whose unpartitioned content hash is in
/// `outputs_by_target`. A read of a raw source (absent from `target_by_model`),
/// a catalog-ambiguous 2-part read, a model not written content-addressed, or a
/// model not built this run leaves the read unresolved ⇒ `None` ⇒ the model is
/// not indexed. A model that reads nothing resolves to an empty (vacuously
/// strong) set. This keeps a `strong` label honest: a mixed-input or ambiguous
/// model is never mislabeled — it is simply deferred.
/// Recompute a content-addressed model's decision-time `input_hash` for the
/// fail-closed reuse gate, using the **same** all-strong upstream chain +
/// `skip_hash` + target identity that spine population
/// ([`rocky_core::reuse::build_records`]) folds into the key it stores. This
/// symmetry is load-bearing: the gate can only hit a prior run's `INPUT_INDEX`
/// entry when the inputs genuinely match.
///
/// Returns `None` — a guaranteed BUILD (clause 3) — when the read set does not
/// resolve to an all-strong content-hash chain (a raw source, a mixed/heuristic
/// or ambiguous read, a model not built this run) or the model cannot be
/// canonicalised (`skip_hash` is `None`). A `None` here is the same fail-safe
/// that keeps a `strong` label honest on the population side.
fn compute_decision_input_hash(
    model_ir: &rocky_ir::ModelIr,
    target_by_model: &std::collections::HashMap<String, String>,
    outputs_by_target: &std::collections::HashMap<String, String>,
) -> Option<String> {
    let upstreams =
        resolve_read_set_content_upstreams(&model_ir.sql, target_by_model, outputs_by_target)?;
    let skip_hash = model_ir.skip_hash()?.to_hex().to_string();
    let target_identity = format!(
        "{}.{}.{}",
        model_ir.target.catalog, model_ir.target.schema, model_ir.target.table
    );
    Some(
        rocky_core::reuse::compute_input_hash(&skip_hash, &target_identity, &upstreams)
            .to_hex()
            .to_string(),
    )
}

fn resolve_read_set_content_upstreams(
    sql: &str,
    target_by_model: &std::collections::HashMap<String, String>,
    outputs_by_target: &std::collections::HashMap<String, String>,
) -> Option<Vec<rocky_core::reuse::UpstreamIdentity>> {
    // Fail-safe: only trust the read-set enumeration for SQL shapes where the
    // lineage walk provably surfaces every upstream.
    if !rocky_sql::lineage_complete::lineage_is_provably_complete(sql) {
        return None;
    }
    let lineage = rocky_sql::lineage::extract_lineage(sql).ok()?;
    let read_tables: Vec<String> = lineage
        .source_tables
        .iter()
        .map(|t| t.name.to_lowercase())
        .collect();
    // Any unparseable "(subquery)" marker means the read set is incomplete.
    if read_tables.iter().any(|t| t == "(subquery)") {
        return None;
    }
    rocky_core::reuse::resolve_content_upstreams(&read_tables, |table| {
        // Only an UNAMBIGUOUS read may resolve to a project model's content
        // hash: a 1-part bare name or a 3-part `catalog.schema.table`. A 2-part
        // `schema.table` read is catalog-ambiguous — refuse it (and defensively
        // any other arity) so it can never be bound to an arbitrary
        // same-`schema.table` model in a different catalog under a strong
        // label. `target_by_model` carries no 2-part keys either; the explicit
        // guard is the load-bearing fail-safe, the key omission is defence in
        // depth.
        match table.matches('.').count() {
            0 | 2 => {}
            _ => return None,
        }
        let target = target_by_model.get(table)?;
        let blake3_hash = outputs_by_target.get(target)?;
        Some((target.clone(), blake3_hash.clone()))
    })
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

/// Build the [`TagTarget`] for a model's `[governance.tags]`, dispatching on
/// its materialization strategy.
///
/// View-materialized models (`strategy = "view"`) have no physical table, so
/// their tags must be applied with `ALTER VIEW ... SET TAGS` —
/// [`TagTarget::View`]. Every other strategy (full-refresh, incremental,
/// merge, materialized-view, dynamic-table, …) produces a table securable and
/// maps to [`TagTarget::Table`]. Pure so the dispatch is unit-testable without
/// a warehouse.
/// Apply each model's `[governance.tags]` to its target securable after the
/// models materialize.
///
/// Dispatch is strategy-aware via [`governance_tag_target`]: view-format
/// models (`strategy = "view"`) tag via `ALTER VIEW ... SET TAGS`, everything
/// else via `ALTER TABLE ... SET TAGS`. Models with no `[governance.tags]`
/// are skipped — Unity Catalog rejects an empty `SET TAGS ()`. The
/// `NoopGovernanceAdapter` (DuckDB and other non-governed targets) silently
/// succeeds, so this is a reachable no-op there.
///
/// Best-effort: a tag failure warns but never aborts the run, mirroring the
/// classification/retention governance posture on the replication path. The
/// model set is re-compiled here (mirroring the classification/masking
/// reconcile idiom) rather than threading resolved configs out of
/// [`execute_models`], keeping that hot path's signature unchanged.
///
/// `only_model` scopes application to a single built model: `Some(name)` for
/// the `rocky run --model <X>` path (which builds only `X`, so tagging
/// unrelated, unbuilt models would target tables this run never created),
/// `None` for the full-DAG transformation and replication `--all`/`--models`
/// paths (which build every model).
pub(crate) async fn apply_model_governance_tags(
    models_dir: &Path,
    governance_adapter: &dyn GovernanceAdapter,
    rocky_cfg: &rocky_core::config::RockyConfig,
    run_vars: &rocky_core::run_vars::RunVars,
    only_model: Option<&str>,
) {
    let governance_compile =
        rocky_compiler::compile::compile(&rocky_compiler::compile::CompilerConfig {
            models_dir: models_dir.to_path_buf(),
            contracts_dir: None,
            source_schemas: std::collections::HashMap::new(),
            source_column_info: std::collections::HashMap::new(),
            mask: rocky_cfg.mask.clone(),
            allow_unmasked: rocky_cfg.classifications.allow_unmasked.clone(),
            project_freshness_default: rocky_cfg.freshness.has_default(),
            // Resolve `@var()` on the governance reconcile compile too, so it
            // doesn't re-read raw markers and spuriously report every variable
            // as missing.
            run_vars: run_vars.clone(),
        });
    match governance_compile {
        Ok(gov_compile) => {
            for model in &gov_compile.project.models {
                if let Some(name) = only_model
                    && model.config.name != name
                {
                    continue;
                }
                let tags = &model.config.governance.tags;
                if tags.is_empty() {
                    continue;
                }
                let target = governance_tag_target(
                    &model.config.strategy,
                    &model.config.target.catalog,
                    &model.config.target.schema,
                    &model.config.target.table,
                );
                if let Err(e) = governance_adapter.set_tags(&target, tags).await {
                    warn!(
                        model = %model.config.name,
                        error = %e,
                        "apply model governance tags failed"
                    );
                }
            }
        }
        Err(e) => {
            warn!(error = %e, "governance compile failed; skipping model tag application");
        }
    }
}

pub(crate) fn governance_tag_target(
    strategy: &rocky_core::models::StrategyConfig,
    catalog: &str,
    schema: &str,
    name: &str,
) -> TagTarget {
    match strategy {
        rocky_core::models::StrategyConfig::View => TagTarget::View {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            view: name.to_string(),
        },
        _ => TagTarget::Table {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            table: name.to_string(),
        },
    }
}

/// True when `model` uses a "plain" single-statement transformation
/// strategy — i.e. anything except the two special paths
/// (`content_addressed`, `time_interval`) that the serial loop handles
/// inline. Only plain models are eligible for intra-layer concurrent
/// execution via [`execute_one_plain_model`].
pub(crate) fn is_plain_strategy(model: &rocky_core::models::Model) -> bool {
    !matches!(
        model.config.strategy,
        rocky_core::models::StrategyConfig::ContentAddressed { .. }
            | rocky_core::models::StrategyConfig::TimeInterval { .. }
    )
}

/// Build the fully-typed [`ModelIr`] for a model by lowering its config and
/// then enriching `typed_columns` from the typechecker's per-model schemas —
/// the same merge `project_ir_from_compile` does. The bare
/// `Model::to_model_ir()` leaves `typed_columns` empty, which would force
/// [`ModelIr::skip_hash`] to the never-equal `None` sentinel; merging the
/// typed columns is what lets the skip gate canonicalise a model's logic.
///
/// The IR is built in its **static** form (`TimeInterval.window == None`), the
/// call-time contract `skip_hash` requires.
fn typed_model_ir(
    model: &rocky_core::models::Model,
    typed_models: &indexmap::IndexMap<String, Vec<rocky_compiler::types::TypedColumn>>,
    surrogate_keys: &HashMap<String, Vec<rocky_core::models::SurrogateKeySpec>>,
    dialect: &dyn rocky_core::traits::SqlDialect,
) -> rocky_ir::ModelIr {
    let mut ir = model.to_model_ir();
    if let Some(cols) = typed_models.get(&model.config.name) {
        ir.typed_columns = cols.clone();
    }
    // Mirror the materialization-time SQL wrap so the skip-hash reflects a
    // declared surrogate key: a model that *gains* a key changes its IR and
    // re-materializes instead of being skipped as unchanged.
    if let Some(specs) = surrogate_keys.get(&model.config.name) {
        rocky_core::models::apply_surrogate_keys(&mut ir, specs, dialect);
    }
    ir
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
    let mut model_ir = model.to_model_ir();
    // Enrich typed columns from the typechecker (bare `to_model_ir()` leaves
    // them empty) so a `merge` strategy with an implicit all-columns update set
    // resolves to an explicit per-column list at SQL-gen. Mirrors
    // `typed_model_ir` / `project_ir_from_compile`. Without it, BigQuery emits
    // an invalid `UPDATE SET target = source` and Snowflake/DuckDB reject
    // `UPDATE SET *` for a transformation merge with no explicit columns.
    if let Some(cols) = exec_ctx.typed_models.get(model_name) {
        model_ir.typed_columns = cols.clone();
    }
    // Inject declared surrogate-key columns by wrapping the model's SELECT.
    // Transformation models materialize their raw SQL directly (never the
    // replication-only metadata-column path), so the wrap is what surfaces the
    // computed column into the CTAS / merge-source schema.
    if let Some(specs) = exec_ctx.surrogate_keys.get(model_name) {
        rocky_core::models::apply_surrogate_keys(&mut model_ir, specs, dialect);
    }
    let target_ref = dialect
        .format_table_ref(
            &model_ir.target.catalog,
            &model_ir.target.schema,
            &model_ir.target.table,
        )
        .map_err(anyhow::Error::from)?;

    let model_started_at = Utc::now();
    let mut bytes_scanned_acc: Option<u64> = None;
    let mut bytes_written_acc: Option<u64> = None;
    let mut job_ids_acc: Vec<String> = Vec::new();

    // First-run bootstrap. Strategies that mutate an existing target
    // (Merge, Incremental, DeleteInsert, Microbatch) all assume the target
    // table already exists — a MERGE/INSERT/DELETE against a missing table
    // fails with "table not found". On first run we probe via
    // `describe_table().is_ok()` and, if absent, create the target through
    // `generate_transformation_initial_ddl`, which honors the model's
    // lakehouse `format` + `format_options` (e.g. `USING ICEBERG`,
    // `PARTITIONED BY`, `TBLPROPERTIES`). Without this the first-run create
    // fell back to the warehouse default and an incremental Iceberg mart
    // silently lost its declared format.
    //
    // The initial DDL is a populated CTAS (`CREATE TABLE ... AS <model SQL>`),
    // so it already loads the full result set. We therefore split the
    // strategies into two groups:
    //
    //   * Merge — populate the target, then run the MERGE. The MERGE upserts
    //     on the unique key, so re-applying it over the just-loaded rows is
    //     idempotent. Behavior is unchanged from before this wiring.
    //   * Incremental / DeleteInsert / Microbatch — the CTAS *is* the first
    //     load. Running the subsequent append (`INSERT INTO`) or
    //     delete+insert over the same source would double-load (Incremental /
    //     Microbatch) or redundantly rewrite (DeleteInsert) the just-created
    //     rows. We capture the CTAS stats and skip the strategy statements on
    //     this first run; subsequent runs find the target present and take the
    //     normal incremental path.
    let strategy_mutates_existing_target = matches!(
        model_ir.materialization,
        rocky_ir::MaterializationStrategy::Merge { .. }
            | rocky_ir::MaterializationStrategy::Incremental { .. }
            | rocky_ir::MaterializationStrategy::DeleteInsert { .. }
            | rocky_ir::MaterializationStrategy::Microbatch { .. }
    );
    // Merge keeps its populate-then-MERGE flow; the other three populate and
    // skip the strategy exec to avoid a double load.
    let bootstrap_is_the_load = !matches!(
        model_ir.materialization,
        rocky_ir::MaterializationStrategy::Merge { .. }
    );
    let mut skip_strategy_exec = false;
    if strategy_mutates_existing_target {
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
                bootstrap_is_the_load,
                "transformation target does not exist — bootstrapping from model schema"
            );
            for ddl in &initial_ddls {
                if bootstrap_is_the_load {
                    // The CTAS loads the data, so capture its stats into the
                    // model's `bytes_scanned` / `bytes_written` output.
                    match warehouse.execute_statement_with_stats(ddl).await {
                        Ok(stats) => {
                            bytes_scanned_acc =
                                accumulate_bytes(bytes_scanned_acc, stats.bytes_scanned);
                            bytes_written_acc =
                                accumulate_bytes(bytes_written_acc, stats.bytes_written);
                            if let Some(jid) = stats.job_id {
                                job_ids_acc.push(jid);
                            }
                        }
                        Err(e) => {
                            return Err(anyhow::Error::from(e).context(format!(
                                "bootstrap of '{target_ref}' for model '{model_name}' failed"
                            )));
                        }
                    }
                } else {
                    // Merge: the bootstrap only needs to materialize the
                    // table; the MERGE that follows does the load and reports
                    // its own stats.
                    warehouse.execute_statement(ddl).await.map_err(|e| {
                        anyhow::Error::from(e).context(format!(
                            "bootstrap of '{target_ref}' for model '{model_name}' failed"
                        ))
                    })?;
                }
            }
            skip_strategy_exec = bootstrap_is_the_load;
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
        skip_strategy_exec,
        "executing model"
    );
    // Capture per-statement stats so derived models report the
    // same `bytes_scanned` / `bytes_written` shape that replication
    // tables already have. Skipped on a first-run bootstrap that already
    // loaded the data via CTAS (Incremental / DeleteInsert / Microbatch).
    if !skip_strategy_exec {
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
                    return Err(
                        anyhow::Error::from(e).context(format!("model '{model_name}' failed"))
                    );
                }
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
        // Transformation models carry no tenant dimension (their asset
        // key is `[catalog, schema, table]`, not a source-schema parse).
        tenant: None,
        job_ids: job_ids_acc,
        // Stamped by the gate in `execute_models` after a successful build
        // when `--skip-unchanged` is enabled; `None` keeps default-off
        // behavior byte-identical.
        skip_internal: None,
        recipe_identity: Some(crate::output::recipe_identity_internal(
            &model_ir,
            warehouse.dialect().name(),
        )),
        // Not the content-addressed write path — no in-process column bytes.
        output_column_hashes: None,
        // Consumer baseline is content-addressed-path only.
        consumed_column_baseline: None,
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
    // already complete (sequential), so on an adapter that supports
    // concurrent execution it's safe to fan out per-partition execution:
    // each partition is independent SQL, the state-store writes serialize
    // through redb's single writer lock, and warehouse-query parallelism is
    // what the user asked for.
    //
    // But every partition writes to the *same* target table, so an adapter
    // without concurrent execution (DuckDB, or any serial-only adapter)
    // cannot run concurrent partition writes — its single connection would
    // either serialize them anyway or fail. We therefore force the limit to
    // 1 when `supports_concurrent_execution()` is false, mirroring the
    // model-path gate in `execute_models`. DuckDB backfill stays serial
    // regardless of `--parallel`.
    //
    // We use `futures::stream::buffer_unordered` rather than
    // tokio::task::JoinSet because the per-partition futures don't need
    // to be Send / 'static — they're polled in the same task as
    // `execute_time_interval_model`, just up to N at a time. This avoids
    // having to wrap warehouse and state_ref in Arc.
    let parallel_limit = if warehouse.supports_concurrent_execution() {
        (partition_opts.parallel as usize).max(1)
    } else {
        1
    };
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
            // time_interval models carry no tenant dimension.
            tenant: None,
            job_ids: job_ids_acc,
            // time_interval is excluded from the v1 skip gate.
            skip_internal: None,
            // Recipe hash from the STATIC IR (window=None) so it is
            // partition-invariant across a model's partitions.
            recipe_identity: Some(crate::output::recipe_identity_internal(
                &model.to_model_ir(),
                warehouse.dialect().name(),
            )),
            // Not the content-addressed write path — no in-process column bytes.
            output_column_hashes: None,
            // Consumer baseline is content-addressed-path only.
            consumed_column_baseline: None,
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
pub(crate) fn build_replication_strategy_with_override(
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

    // The recognized arms below must stay in lockstep with
    // `rocky_core::config::RECOGNIZED_REPLICATION_STRATEGIES`, which the
    // `rocky validate` V035 lint uses to flag a strategy that silently falls
    // through the `_` arm to full_refresh. The
    // `recognized_replication_strategies_match_builder` test pins the relationship.
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
    prune_enabled: bool,
) -> Result<TableOutcome> {
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

    // --- Skip-unchanged pruning (config `prune_unchanged`, off by default) ---
    //
    // Before doing any copy work, ask the adapter for a cheap change-marker on
    // the source (a `DESCRIBE DETAIL`-derived marker on Databricks; `None` on
    // adapters without one → never prune). If it matches the marker recorded at
    // the last successful copy, the source is unchanged: skip the copy, drift
    // check, and data checks entirely, emitting no materialization. The
    // orchestrator's `satisfy_empty_outputs` stamps the 0-row continuity signal
    // for the selected-but-unmaterialized key. A metadata-fetch error is
    // treated as `None` (copy) so a transient failure never causes a false
    // skip. `live_marker` is threaded to the success path and recorded there.
    let live_marker = if prune_enabled {
        match warehouse.source_change_marker(&source_table).await {
            Ok(marker) => marker,
            Err(e) => {
                tracing::warn!(
                    table = %task.table_name,
                    error = %e,
                    "source_change_marker failed; copying this table (no prune)"
                );
                None
            }
        }
    } else {
        None
    };
    if let Some(live) = &live_marker {
        let recorded = {
            let guard = state.lock().await;
            guard
                .get_source_marker(&target_table.state_key())
                .ok()
                .flatten()
        };
        if recorded.as_deref() == Some(live.as_str()) {
            return Ok(TableOutcome::Pruned(PrunedTable {
                asset_key,
                source_schema: task.source_schema.clone(),
                table_name: task.table_name.clone(),
            }));
        }
    }

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

    // Idempotent full refresh on dialects whose CTAS isn't `CREATE OR
    // REPLACE` (Trino): drop the target first so a re-run doesn't fail with
    // "table already exists". Issued as a separate statement because Trino's
    // REST API runs one statement per request. Other dialects skip this —
    // their CTAS already replaces atomically.
    if strategy_name == "full_refresh" && dialect.full_refresh_needs_predrop() {
        let target_ref = dialect.format_table_ref(
            &target_table.catalog,
            &target_table.schema,
            &target_table.table,
        )?;
        let drop_sql = dialect.drop_table_sql(&target_ref);
        debug!(
            table = target_table.full_name(),
            sql = drop_sql.as_str(),
            "pre-drop for idempotent full refresh"
        );
        warehouse
            .execute_statement(&drop_sql)
            .await
            .map_err(anyhow::Error::from)?;
    }

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

    // Record the source change-marker captured at the prune decision point
    // (not a post-copy re-read): if the source advanced mid-copy the recorded
    // marker stays behind, so the next run copies again rather than skipping a
    // change. Written only on this success path — a copy failure early-returns
    // above, leaving the prior marker so the table is retried.
    if let Some(marker) = &live_marker {
        let recorded = state
            .lock()
            .await
            .set_source_marker(&target_table.state_key(), marker);
        if let Err(e) = recorded {
            tracing::warn!(
                table = %task.table_name,
                error = %e,
                "failed to record source change-marker; table will re-copy next run"
            );
        }
    }

    Ok(TableOutcome::Materialized(Box::new(TableResult {
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
            // Tenant lifted from the discover-time schema-pattern
            // `{tenant}` component on the task. `None` for non-tenant
            // patterns; carried onto the persisted ModelExecution.
            tenant: task.tenant.clone(),
            job_ids: exec_stats.job_id.clone().into_iter().collect(),
            // Replication materializations are not gated by --skip-unchanged
            // in v1 (the gate covers transformation models).
            skip_internal: None,
            recipe_identity: Some(crate::output::recipe_identity_internal(
                &model_ir,
                dialect.name(),
            )),
            // Not the content-addressed write path — no in-process column bytes.
            output_column_hashes: None,
            // Consumer baseline is content-addressed-path only.
            consumed_column_baseline: None,
        },
        drift_checked: true,
        drift_detected: drift_action,
        column_match_check,
        source_batch_ref,
        target_batch_ref,
        freshness_batch_ref,
        asset_key,
        target_full_name: target_table_full_name,
        target_ref: TableRef {
            catalog: target_table.catalog.clone(),
            schema: target_table.schema.clone(),
            table: target_table.table.clone(),
        },
        deferred_tags,
        deferred_watermark,
    })))
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
    result: Result<(usize, Result<TableOutcome, anyhow::Error>), tokio::task::JoinError>,
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
        Ok((_, Ok(TableOutcome::Pruned(pruned)))) => {
            if let Some(t) = &throttle {
                t.on_success();
                adjust_semaphore(t, semaphore, semaphore_capacity);
            }
            record_pruned(output, pruned);
        }
        Ok((_, Ok(TableOutcome::Materialized(tr)))) => {
            let tr = *tr;
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
    fn governance_tag_target_dispatches_view_strategy_to_view() {
        use rocky_core::models::StrategyConfig;
        let target =
            governance_tag_target(&StrategyConfig::View, "warehouse", "marts", "fct_orders");
        match target {
            TagTarget::View {
                catalog,
                schema,
                view,
            } => {
                assert_eq!(catalog, "warehouse");
                assert_eq!(schema, "marts");
                assert_eq!(view, "fct_orders");
            }
            other => panic!("view strategy must map to TagTarget::View, got {other:?}"),
        }
    }

    #[test]
    fn governance_tag_target_dispatches_non_view_strategies_to_table() {
        use rocky_core::models::StrategyConfig;
        // Every non-view strategy — including materialized_view — produces a
        // table securable and must dispatch to `ALTER TABLE ... SET TAGS`.
        for strategy in [
            StrategyConfig::FullRefresh,
            StrategyConfig::MaterializedView,
            StrategyConfig::Incremental {
                timestamp_column: "ts".into(),
            },
        ] {
            let target = governance_tag_target(&strategy, "warehouse", "marts", "fct_orders");
            assert!(
                matches!(target, TagTarget::Table { .. }),
                "non-view strategy {strategy:?} must map to TagTarget::Table"
            );
        }
    }

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

        let surrogate_keys = HashMap::new();
        let ctx = ExecutionContext {
            typed_models: &typed_models,
            model_timings: &model_timings,
            surrogate_keys: &surrogate_keys,
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
        let surrogate_keys = HashMap::new();
        let ctx = ExecutionContext {
            typed_models: &typed_models,
            model_timings: &model_timings,
            surrogate_keys: &surrogate_keys,
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
            model_decisions: vec![],
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
            &DeferOptions::default(),
            &SkipRunOptions::default(),
            &rocky_core::run_vars::RunVars::new(),
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
            &DeferOptions::default(),
            &SkipRunOptions::default(),
            &rocky_core::run_vars::RunVars::new(),
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

    #[test]
    fn recognized_replication_strategies_match_builder() {
        use rocky_core::config::RECOGNIZED_REPLICATION_STRATEGIES as RECOGNIZED;

        // The validate lint (V035) trusts this const to decide whether a
        // strategy string silently falls back to full_refresh. Pin it to the
        // builder so the two can never drift.
        assert_eq!(
            RECOGNIZED,
            &[
                "incremental",
                "merge",
                "view",
                "materialized_view",
                "dynamic_table",
                "full_refresh"
            ],
            "RECOGNIZED_REPLICATION_STRATEGIES changed — update \
             build_replication_strategy_with_override and this guard together"
        );

        for s in RECOGNIZED {
            let toml = if *s == "merge" {
                format!("strategy = \"{s}\"\nmerge_keys = [\"id\"]\n")
            } else {
                format!("strategy = \"{s}\"\n")
            };
            let pipeline = parse_pipeline(&toml);
            match (*s, build_replication_strategy(&pipeline)) {
                ("full_refresh", Ok(MaterializationStrategy::FullRefresh)) => {}
                ("dynamic_table", Err(_)) => {}
                (_, Ok(MaterializationStrategy::FullRefresh)) => {
                    panic!("recognized strategy {s} silently fell back to full_refresh")
                }
                (_, Ok(_)) => {}
                (other, res) => panic!("unexpected result for {other}: {res:?}"),
            }
        }

        // A value the lint must flag: not recognized, silently full_refresh.
        let bogus = parse_pipeline(r#"strategy = "time_interval""#);
        assert!(matches!(
            build_replication_strategy(&bogus),
            Ok(MaterializationStrategy::FullRefresh)
        ));
        assert!(!RECOGNIZED.contains(&"time_interval"));
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

    /// `run_status_exit_result` maps the derived run status onto the CLI
    /// exit-code contract: Success → Ok, PartialFailure → the
    /// `PartialFailure` sentinel (exit 2), Failure → an error (exit 1).
    #[test]
    fn run_status_exit_result_maps_status_to_exit_contract() {
        // Clean run → Ok.
        let mut ok = RunOutput::new(String::new(), 0, 1);
        ok.tables_copied = 1;
        assert!(super::run_status_exit_result(&ok, "r").is_ok());

        // Some progress + a failure → PartialFailure sentinel (exit 2).
        let mut partial = RunOutput::new(String::new(), 0, 1);
        partial.tables_copied = 1;
        partial.tables_failed = 1;
        let err = super::run_status_exit_result(&partial, "r").unwrap_err();
        assert!(
            err.downcast_ref::<PartialFailure>().is_some(),
            "PartialFailure must map to the exit-2 sentinel, got: {err:#}"
        );

        // No progress + a failure → a plain error (exit 1).
        let mut total = RunOutput::new(String::new(), 0, 1);
        total.tables_failed = 1;
        let err = super::run_status_exit_result(&total, "r").unwrap_err();
        assert!(err.downcast_ref::<PartialFailure>().is_none());
    }

    /// Build a parallel-copy `TableError` for the merge tests below.
    fn copy_table_error(asset: &str) -> TableError {
        TableError {
            asset_key: vec![asset.to_string()],
            error: format!("copy of {asset} failed"),
            task_index: None,
            failure_kind: crate::output::FailureKind::QueryRejected,
            cooldown_seconds: None,
        }
    }

    /// Seed a `RunOutput` with the compile-error bookkeeping `execute_models`
    /// records on the replication path before the parallel-copy merge runs:
    /// one E020 entry on `errors` + a bumped `tables_failed`.
    fn output_with_compile_error() -> RunOutput {
        let mut out = RunOutput::new(String::new(), 0, 1);
        out.tables_failed = 1;
        out.errors.push(crate::output::TableErrorOutput {
            asset_key: vec!["broken".to_string()],
            error: "[E020] time_interval time_column absent".to_string(),
            failure_kind: crate::output::FailureKind::CompileError,
            cooldown_seconds: None,
        });
        out
    }

    /// The replication-path merge preserves `execute_models`' compile-error
    /// entries instead of clobbering them with the parallel-copy errors. This
    /// is the regression guard for the silent-skip on the replication +
    /// `--models` path: a compile-erroring model under a replication pipeline
    /// must survive into `tables_failed` / `errors` and flip `status` off
    /// `Success`, even when a source copy also failed.
    #[test]
    fn merge_preserves_compile_errors_alongside_copy_errors() {
        let mut out = output_with_compile_error();
        // A model materialized (progress) so the run is a PartialFailure.
        out.tables_copied = 1;
        let copy_errors = vec![copy_table_error("orders")];

        super::merge_replication_compile_and_copy_errors(&mut out, &copy_errors);

        // The compile failure was NOT dropped: both error kinds survive and
        // the count folds in (1 compile + 1 copy).
        assert_eq!(out.tables_failed, 2, "compile + copy failures both counted");
        assert!(
            out.errors.iter().any(
                |e| e.failure_kind == crate::output::FailureKind::CompileError
                    && e.error.contains("E020")
            ),
            "the E020 compile error must survive the merge: {:?}",
            out.errors
        );
        assert!(
            out.errors
                .iter()
                .any(|e| e.failure_kind == crate::output::FailureKind::QueryRejected),
            "the parallel-copy error must also be present: {:?}",
            out.errors
        );
        // Status is stamped off the merged tallies (progress + failures).
        assert!(matches!(
            out.status,
            rocky_core::state::RunStatus::PartialFailure
        ));
    }

    /// A compile-only failure (no source copy failed) on the replication path
    /// still flips `status` off `Success` and keeps a non-zero `tables_failed`
    /// after the merge — the caller's guard then maps it to a non-zero exit.
    #[test]
    fn merge_compile_only_failure_is_not_success() {
        let mut out = output_with_compile_error();
        let no_copy_errors: Vec<TableError> = Vec::new();

        super::merge_replication_compile_and_copy_errors(&mut out, &no_copy_errors);

        assert_eq!(out.tables_failed, 1, "the compile failure is preserved");
        assert_eq!(out.errors.len(), 1);
        assert_eq!(
            out.errors[0].failure_kind,
            crate::output::FailureKind::CompileError
        );
        // No progress + a failure → total Failure, never Success.
        assert!(matches!(out.status, rocky_core::state::RunStatus::Failure));
    }

    /// A single model that emits **two** compile-error diagnostics produces two
    /// `errors` entries sharing one asset_key but only one `tables_failed`
    /// bump (the per-model compile path dedups the count via a model set). The
    /// merge counts distinct models, not raw entries, so `tables_failed` stays
    /// `1`. Guards against re-inflating the count to `errors.len()` — the
    /// single-entry tests above pass either way.
    #[test]
    fn merge_counts_distinct_models_not_diagnostics() {
        let mut out = RunOutput::new(String::new(), 0, 1);
        // One model ("broken") with two distinct error diagnostics → two
        // entries, one model. Mirrors `execute_models`' per-diagnostic push
        // against its per-model `tables_failed` bump.
        out.tables_failed = 1;
        for code in ["E020", "E021"] {
            out.errors.push(crate::output::TableErrorOutput {
                asset_key: vec!["broken".to_string()],
                error: format!("[{code}] compile error on broken"),
                failure_kind: crate::output::FailureKind::CompileError,
                cooldown_seconds: None,
            });
        }
        let no_copy_errors: Vec<TableError> = Vec::new();

        super::merge_replication_compile_and_copy_errors(&mut out, &no_copy_errors);

        // Distinct failing models == 1, even though two diagnostics survive.
        assert_eq!(
            out.tables_failed, 1,
            "one model with two diagnostics counts once, not twice"
        );
        assert_eq!(out.errors.len(), 2, "both diagnostics are preserved");
        assert!(matches!(out.status, rocky_core::state::RunStatus::Failure));
    }

    /// A clean replication run (no compile errors, no copy errors) stays
    /// `Success` after the merge — no false failure regression.
    #[test]
    fn merge_clean_run_stays_success() {
        let mut out = RunOutput::new(String::new(), 0, 1);
        out.tables_copied = 3;
        let no_errors: Vec<TableError> = Vec::new();

        super::merge_replication_compile_and_copy_errors(&mut out, &no_errors);

        assert_eq!(out.tables_failed, 0);
        assert!(out.errors.is_empty());
        assert!(matches!(out.status, rocky_core::state::RunStatus::Success));
    }

    /// A copy-only failure (no compile errors) behaves as before the fix:
    /// the copy errors populate `errors` / `tables_failed` and drive the
    /// status, with nothing spuriously preserved.
    #[test]
    fn merge_copy_only_failure_behaves_as_before() {
        let mut out = RunOutput::new(String::new(), 0, 1);
        out.tables_copied = 2;
        let copy_errors = vec![copy_table_error("a"), copy_table_error("b")];

        super::merge_replication_compile_and_copy_errors(&mut out, &copy_errors);

        assert_eq!(out.tables_failed, 2);
        assert_eq!(out.errors.len(), 2);
        assert!(
            out.errors
                .iter()
                .all(|e| e.failure_kind == crate::output::FailureKind::QueryRejected),
            "no phantom compile errors: {:?}",
            out.errors
        );
        assert!(matches!(
            out.status,
            rocky_core::state::RunStatus::PartialFailure
        ));
    }

    /// End-to-end: a model declaring a `[[surrogate_key]]` block materializes a
    /// table that carries the injected, dialect-resolved hash column.
    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn surrogate_key_column_is_materialized() {
        let dir = tempfile::tempdir().unwrap();
        let models = dir.path().join("models");
        std::fs::create_dir_all(&models).unwrap();
        std::fs::write(models.join("keyed.sql"), "SELECT 1 AS id\n").unwrap();
        std::fs::write(
            models.join("keyed.toml"),
            "[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"\"\nschema = \"main\"\n\n[[surrogate_key]]\nname = \"sk\"\ncolumns = [\"id\"]\n",
        )
        .unwrap();
        let db_path = dir.path().join("t.duckdb");
        let (_out, result) = run_models_against_duckdb(&models, &db_path, false, 1).await;
        result.expect("run should succeed");

        // The materialized table carries the injected surrogate-key column.
        let conn = rocky_duckdb::DuckDbConnector::open(&db_path).expect("open db");
        let r = conn
            .execute_sql("SELECT sk FROM main.keyed")
            .expect("query sk");
        assert_eq!(r.rows.len(), 1);
        let sk = r.rows[0][0].as_str().unwrap_or_default();
        assert_eq!(sk.len(), 32, "md5 hex should be 32 chars, got {sk:?}");
    }

    /// Write a `[strategy] time_interval` model whose `time_column` is
    /// absent from its SELECT output — guaranteed E020 at type-check.
    #[cfg(feature = "duckdb")]
    fn write_e020_model(dir: &std::path::Path, name: &str) {
        // Output schema is `{id}`; `time_column = "ts"` is not present → E020.
        std::fs::write(dir.join(format!("{name}.sql")), "SELECT 1 AS id\n")
            .expect("write model sql");
        std::fs::write(
            dir.join(format!("{name}.toml")),
            "[strategy]\ntype = \"time_interval\"\ntime_column = \"ts\"\ngranularity = \"day\"\nfirst_partition = \"2026-01-01\"\nlookback = 0\n\n[target]\ncatalog = \"\"\nschema = \"main\"\n",
        )
        .expect("write model toml");
    }

    /// A model that fails to compile (E020) is a first-class run failure,
    /// not a silent skip: it is counted in `tables_failed`, surfaced in
    /// `errors` with `FailureKind::CompileError` + the diagnostic, and the
    /// clean sibling model still materializes — so the run is a
    /// `PartialFailure`, not a green run that dropped data.
    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn compile_error_model_is_a_partial_failure_not_a_silent_skip() {
        let dir = tempfile::tempdir().unwrap();
        let models = dir.path().join("models");
        std::fs::create_dir_all(&models).unwrap();
        // One clean model + one E020 model, independent of each other.
        write_plain_model(&models, "good", "SELECT 7 AS id");
        write_e020_model(&models, "broken");

        let db_path = dir.path().join("t.duckdb");
        let (output, result) = run_models_against_duckdb(&models, &db_path, false, 1).await;
        // execute_models itself returns Ok — the failure is carried in the
        // populated `output`, so the caller can still emit the JSON payload
        // before mapping status to a non-zero exit code.
        result.expect("execute_models should return Ok (failure carried in output)");

        // The broken model counts once toward `tables_failed` even if it
        // raised several diagnostics.
        assert_eq!(output.tables_failed, 1, "the broken model counts as failed");
        assert!(!output.errors.is_empty(), "diagnostics surfaced in errors");
        // Every surfaced error belongs to the broken model and is tagged as
        // a compile failure; at least one carries the E020 code.
        assert!(output.errors.iter().all(|e| {
            e.asset_key == vec!["broken".to_string()]
                && e.failure_kind == crate::output::FailureKind::CompileError
        }));
        assert!(
            output.errors.iter().any(|e| e.error.contains("E020")),
            "an E020 diagnostic is present: {:?}",
            output.errors
        );

        // The clean model still materialized.
        let mat_names: Vec<String> = output
            .materializations
            .iter()
            .map(|m| m.asset_key.last().cloned().unwrap_or_default())
            .collect();
        assert_eq!(mat_names, vec!["good".to_string()]);

        // Status is PartialFailure (some progress + a failure).
        assert!(matches!(
            output.derive_run_status(),
            rocky_core::state::RunStatus::PartialFailure
        ));

        // The good table exists; the broken one was never created.
        let conn = rocky_duckdb::DuckDbConnector::open(&db_path).expect("open db");
        assert_eq!(
            conn.execute_sql("SELECT id FROM main.good")
                .unwrap()
                .rows
                .len(),
            1
        );
        assert!(
            conn.execute_sql("SELECT id FROM main.broken").is_err(),
            "the compile-failed model must not have been materialized"
        );
    }

    /// When every model fails to compile, the run is a total `Failure`:
    /// nothing materializes, `tables_failed` is bumped, and the diagnostic
    /// is surfaced in `errors`.
    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn all_models_compile_error_is_a_total_failure() {
        let dir = tempfile::tempdir().unwrap();
        let models = dir.path().join("models");
        std::fs::create_dir_all(&models).unwrap();
        write_e020_model(&models, "broken");

        let db_path = dir.path().join("t.duckdb");
        let (output, result) = run_models_against_duckdb(&models, &db_path, false, 1).await;
        result.expect("execute_models returns Ok with the failure in output");

        assert_eq!(output.tables_failed, 1);
        assert!(output.materializations.is_empty());
        assert_eq!(
            output.errors[0].failure_kind,
            crate::output::FailureKind::CompileError
        );
        assert!(matches!(
            output.derive_run_status(),
            rocky_core::state::RunStatus::Failure
        ));
    }

    /// Write a plain model carrying a `[governance.tags]` block.
    #[cfg(feature = "duckdb")]
    fn write_tagged_model(dir: &std::path::Path, name: &str, sql: &str) {
        std::fs::write(dir.join(format!("{name}.sql")), format!("{sql}\n"))
            .expect("write model sql");
        std::fs::write(
            dir.join(format!("{name}.toml")),
            "[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"\"\nschema = \"main\"\n\n[governance.tags]\ndomain = \"finance\"\n",
        )
        .expect("write model toml");
    }

    /// Counting [`GovernanceAdapter`] test double — records the table/view
    /// name of every `set_tags` call so a test can assert exactly which
    /// models were tagged. Every other trait method is an inert `Ok`.
    #[cfg(feature = "duckdb")]
    #[derive(Default)]
    struct CountingGovernance {
        tagged: std::sync::Mutex<Vec<String>>,
    }

    #[cfg(feature = "duckdb")]
    #[async_trait::async_trait]
    impl rocky_core::traits::GovernanceAdapter for CountingGovernance {
        async fn set_tags(
            &self,
            target: &TagTarget,
            _tags: &std::collections::BTreeMap<String, String>,
        ) -> rocky_core::traits::AdapterResult<()> {
            let name = match target {
                TagTarget::Table { table, .. } => table.clone(),
                TagTarget::View { view, .. } => view.clone(),
                TagTarget::Catalog(catalog) => catalog.clone(),
                TagTarget::Schema { schema, .. } => schema.clone(),
            };
            self.tagged.lock().unwrap().push(name);
            Ok(())
        }
        async fn get_grants(
            &self,
            _target: &rocky_ir::ir::GrantTarget,
        ) -> rocky_core::traits::AdapterResult<Vec<rocky_ir::ir::Grant>> {
            Ok(vec![])
        }
        async fn apply_grants(
            &self,
            _grants: &[rocky_ir::ir::Grant],
        ) -> rocky_core::traits::AdapterResult<()> {
            Ok(())
        }
        async fn revoke_grants(
            &self,
            _grants: &[rocky_ir::ir::Grant],
        ) -> rocky_core::traits::AdapterResult<()> {
            Ok(())
        }
        async fn bind_workspace(
            &self,
            _catalog: &str,
            _workspace_id: u64,
            _binding_type: &str,
        ) -> rocky_core::traits::AdapterResult<()> {
            Ok(())
        }
        async fn set_isolation(
            &self,
            _catalog: &str,
            _enabled: bool,
        ) -> rocky_core::traits::AdapterResult<()> {
            Ok(())
        }
    }

    /// `apply_model_governance_tags` reaches the apply loop and applies each
    /// tagged model's `[governance.tags]` — and the `only_model` filter scopes
    /// application to the single built model, never the whole project. This is
    /// the regression guard for the `rocky run --model <X>` and replication
    /// `--all` paths, where per-model tags were previously dropped entirely.
    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn apply_model_governance_tags_respects_only_model_filter() {
        let dir = tempfile::tempdir().unwrap();
        let models = dir.path().join("models");
        std::fs::create_dir_all(&models).unwrap();
        // Two tagged models + one untagged.
        write_tagged_model(&models, "alpha", "SELECT 1 AS id");
        write_tagged_model(&models, "beta", "SELECT 2 AS id");
        write_plain_model(&models, "gamma", "SELECT 3 AS id");

        // Minimal config — the helper only reads `mask`, `classifications`,
        // and `freshness`, which a bare DuckDB config leaves at defaults.
        std::fs::write(
            dir.path().join("rocky.toml"),
            "[adapter]\ntype = \"duckdb\"\npath = \"t.duckdb\"\n",
        )
        .unwrap();
        let cfg = rocky_core::config::parse_rocky_config(&dir.path().join("rocky.toml"))
            .expect("parse minimal config");
        let vars = rocky_core::run_vars::RunVars::new();

        // `None` (full-DAG / replication `--all`): every tagged model is
        // tagged; the untagged one is skipped.
        let gov = CountingGovernance::default();
        super::apply_model_governance_tags(&models, &gov, &cfg, &vars, None).await;
        let mut tagged = gov.tagged.lock().unwrap().clone();
        tagged.sort();
        assert_eq!(
            tagged,
            vec!["alpha".to_string(), "beta".to_string()],
            "None filter tags every model with [governance.tags], skips untagged"
        );

        // `Some(alpha)`: exactly one `set_tags`, for alpha only — beta must
        // NOT be tagged even though it carries tags (it isn't built this run).
        let gov = CountingGovernance::default();
        super::apply_model_governance_tags(&models, &gov, &cfg, &vars, Some("alpha")).await;
        assert_eq!(
            *gov.tagged.lock().unwrap(),
            vec!["alpha".to_string()],
            "only_model scopes application to the built model"
        );

        // `Some(gamma)`: gamma has no tags, so zero `set_tags`.
        let gov = CountingGovernance::default();
        super::apply_model_governance_tags(&models, &gov, &cfg, &vars, Some("gamma")).await;
        assert!(
            gov.tagged.lock().unwrap().is_empty(),
            "an untagged target issues no set_tags"
        );
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
            &DeferOptions::default(),
            super::SkipGateConfig::off(),
            false,
            &rocky_core::run_vars::RunVars::new(),
        )
        .await;
        (output, result)
    }

    /// End-to-end run-path coverage for `--var`: a model whose SQL references
    /// `@var(region)` materializes only the rows matching the supplied value
    /// when a non-empty `RunVars` is threaded through `execute_models`.
    ///
    /// This guards the run-path threading specifically — the compiler-level
    /// substitution is covered separately in `rocky-compiler`, but only an
    /// execution test catches a regression where `execute_models` is handed an
    /// empty `RunVars` (which would turn `@var()` into an E028 compile error
    /// and silently skip the model).
    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn run_var_substituted_and_materialized() {
        use rocky_core::traits::WarehouseAdapter;
        use rocky_duckdb::adapter::DuckDbWarehouseAdapter;

        let tmp = tempfile::TempDir::new().expect("temp dir");
        let models_dir = tmp.path().join("models");
        std::fs::create_dir(&models_dir).expect("mkdir models");
        let db_path = tmp.path().join("var.duckdb");

        // Seed a source table with mixed regions.
        {
            let seed = DuckDbWarehouseAdapter::open(&db_path).expect("open duckdb for seeding");
            seed.execute_statement(
                "CREATE TABLE main.base AS SELECT * FROM (VALUES \
                 (1, 'us'), (2, 'eu'), (3, 'us'), (4, 'apac')) AS t(id, region)",
            )
            .await
            .unwrap();
        }

        // Model filters on `@var(region)`.
        std::fs::write(
            models_dir.join("filtered.sql"),
            "SELECT id, region FROM main.base WHERE region = '@var(region)'\n",
        )
        .unwrap();
        std::fs::write(
            models_dir.join("filtered.toml"),
            "[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"\"\nschema = \"main\"\n",
        )
        .unwrap();

        let adapter = DuckDbWarehouseAdapter::open(&db_path).expect("open duckdb");
        let opts = PartitionRunOptions::default();
        let mut output = RunOutput::new(String::new(), 0, 1);
        let run_vars = rocky_core::run_vars::RunVars::parse_pairs(["region=us"]).unwrap();
        super::execute_models(
            &models_dir,
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
            &DeferOptions::default(),
            super::SkipGateConfig::off(),
            false,
            &run_vars,
        )
        .await
        .expect("run with --var should succeed");

        // The target table holds only the `us` rows (ids 1 and 3).
        let conn = rocky_duckdb::DuckDbConnector::open(&db_path).expect("open db");
        let r = conn
            .execute_sql("SELECT id FROM main.filtered ORDER BY id")
            .expect("query filtered");
        let ids: Vec<&str> = r
            .rows
            .iter()
            .map(|row| row[0].as_str().unwrap_or_default())
            .collect();
        assert_eq!(
            ids,
            vec!["1", "3"],
            "only region='us' rows survive the @var filter"
        );
    }

    /// `rocky run --model down --defer`: the selected model `down` reads its
    /// unbuilt upstream `up` from the defer (production) schema, and only
    /// `down` is materialized.
    ///
    /// Discriminator: `up` exists *only* in the defer schema (`prod`) with a
    /// known row set, plus a decoy `up` with different rows in the working
    /// schema (`main`). If defer works, `main.down` reflects the `prod` rows
    /// and the decoy is never read; if it silently fell back to the working
    /// schema, the row count would differ. We also assert `prod.up` is NOT
    /// re-materialized (its rows are left exactly as seeded).
    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn defer_resolves_unbuilt_upstream_to_prod_schema() {
        use rocky_core::traits::WarehouseAdapter;
        use rocky_duckdb::adapter::DuckDbWarehouseAdapter;

        let tmp = tempfile::TempDir::new().expect("temp dir");
        let models_dir = tmp.path().join("models");
        std::fs::create_dir(&models_dir).expect("mkdir models");
        let db_path = tmp.path().join("defer.duckdb");

        // Seed the warehouse: `prod.up` has the real upstream rows; `main.up`
        // is a decoy with a *different* row count that defer must NOT read.
        {
            let seed = DuckDbWarehouseAdapter::open(&db_path).expect("open duckdb for seeding");
            seed.execute_statement("CREATE SCHEMA IF NOT EXISTS prod")
                .await
                .unwrap();
            // 3 real upstream rows in the defer schema.
            seed.execute_statement(
                "CREATE TABLE prod.up AS SELECT * FROM (VALUES (1), (2), (3)) AS t(id)",
            )
            .await
            .unwrap();
            // Decoy with a single row in the working (main) schema. If defer
            // wrongly resolved to `main`, `down` would have 1 row, not 3.
            seed.execute_statement("CREATE TABLE main.up AS SELECT 99 AS id")
                .await
                .unwrap();
        }

        // Model `up` targets the prod schema (its production home). Model
        // `down` targets `main` and selects from the bare upstream `up`.
        std::fs::write(models_dir.join("up.sql"), "SELECT id FROM prod.up\n").unwrap();
        std::fs::write(
            models_dir.join("up.toml"),
            "[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"\"\nschema = \"prod\"\ntable = \"up\"\n",
        )
        .unwrap();
        std::fs::write(models_dir.join("down.sql"), "SELECT id FROM up\n").unwrap();
        std::fs::write(
            models_dir.join("down.toml"),
            "[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"\"\nschema = \"main\"\ntable = \"down\"\n",
        )
        .unwrap();

        let adapter = DuckDbWarehouseAdapter::open(&db_path).expect("open duckdb");
        let opts = PartitionRunOptions::default();
        let defer_opts = DeferOptions {
            enabled: true,
            defer_to: None,
        };
        let mut output = RunOutput::new(String::new(), 0, 1);

        super::execute_models(
            &models_dir,
            &adapter as &dyn rocky_core::traits::WarehouseAdapter,
            None,
            &opts,
            "test-defer",
            Some("down"), // build only `down`
            &mut output,
            None,
            None,
            &rocky_core::config::SchemaCacheConfig::default(),
            false,
            &defer_opts,
            super::SkipGateConfig::off(),
            false,
            &rocky_core::run_vars::RunVars::new(),
        )
        .await
        .expect("deferred run must succeed");

        // Only `down` was materialized — `up` was deferred, not built.
        assert_eq!(
            output.materializations.len(),
            1,
            "exactly one model (down) should materialize: {:?}",
            output
                .materializations
                .iter()
                .map(|m| &m.asset_key)
                .collect::<Vec<_>>()
        );
        assert_eq!(
            output.materializations[0].asset_key,
            vec![String::new(), "main".to_string(), "down".to_string()],
            "the materialized model must be main.down"
        );

        // DuckDB COUNT(*) may serialize as a JSON number or string depending
        // on the adapter path; accept both, matching the existing tests.
        let count_of = |v: &serde_json::Value| -> Option<i64> {
            v.as_i64()
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        };

        // `main.down` must reflect the defer-schema (prod) rows: 3, not the
        // decoy's 1.
        let verify = DuckDbWarehouseAdapter::open(&db_path).expect("reopen");
        let r = verify
            .execute_query("SELECT COUNT(*) FROM main.down")
            .await
            .expect("main.down must exist");
        assert_eq!(
            count_of(&r.rows[0][0]),
            Some(3),
            "down must read the 3 rows from prod.up (defer schema), not the decoy in main.up"
        );

        // The deferred upstream `prod.up` was never re-materialized by this
        // run — it still holds exactly its seeded 3 rows.
        let up = verify
            .execute_query("SELECT COUNT(*) FROM prod.up")
            .await
            .expect("prod.up must still exist");
        assert_eq!(
            count_of(&up.rows[0][0]),
            Some(3),
            "the deferred upstream prod.up is untouched"
        );
    }

    /// Control: WITHOUT `--defer`, the same `--model down` run fails because
    /// the bare `up` ref resolves against the working schema where the table
    /// the model expects does not exist as a queryable bare name in a CTAS
    /// targeting `main`. This pins that defer is what makes the cross-schema
    /// read work — the behavior is genuinely off by default.
    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn without_defer_unbuilt_upstream_is_not_rewritten() {
        use rocky_duckdb::adapter::DuckDbWarehouseAdapter;

        let tmp = tempfile::TempDir::new().expect("temp dir");
        let models_dir = tmp.path().join("models");
        std::fs::create_dir(&models_dir).expect("mkdir models");
        let db_path = tmp.path().join("nodefer.duckdb");

        {
            let seed = DuckDbWarehouseAdapter::open(&db_path).expect("open duckdb for seeding");
            seed.execute_statement("CREATE SCHEMA IF NOT EXISTS prod")
                .await
                .unwrap();
            seed.execute_statement(
                "CREATE TABLE prod.up AS SELECT * FROM (VALUES (1), (2), (3)) AS t(id)",
            )
            .await
            .unwrap();
        }

        std::fs::write(models_dir.join("up.sql"), "SELECT id FROM prod.up\n").unwrap();
        std::fs::write(
            models_dir.join("up.toml"),
            "[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"\"\nschema = \"prod\"\ntable = \"up\"\n",
        )
        .unwrap();
        std::fs::write(models_dir.join("down.sql"), "SELECT id FROM up\n").unwrap();
        std::fs::write(
            models_dir.join("down.toml"),
            "[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"\"\nschema = \"main\"\ntable = \"down\"\n",
        )
        .unwrap();

        let adapter = DuckDbWarehouseAdapter::open(&db_path).expect("open duckdb");
        let opts = PartitionRunOptions::default();
        let mut output = RunOutput::new(String::new(), 0, 1);

        // No defer: the bare `up` is not rewritten, and `main.up` does not
        // exist, so building `down` alone fails on the unresolved upstream.
        let result = super::execute_models(
            &models_dir,
            &adapter as &dyn rocky_core::traits::WarehouseAdapter,
            None,
            &opts,
            "test-no-defer",
            Some("down"),
            &mut output,
            None,
            None,
            &rocky_core::config::SchemaCacheConfig::default(),
            false,
            &DeferOptions::default(),
            super::SkipGateConfig::off(),
            false,
            &rocky_core::run_vars::RunVars::new(),
        )
        .await;

        assert!(
            result.is_err(),
            "without --defer the unbuilt bare upstream must not resolve, so the run fails"
        );
        assert!(
            output.materializations.is_empty(),
            "no model should materialize when the upstream is unresolved"
        );
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

    /// (d) Partition-backfill regression: a `time_interval` model backfilled
    /// over a multi-partition range with `--parallel 4` must stay serial on a
    /// DuckDB adapter (concurrency unsupported) and still materialize every
    /// partition correctly. This guards the backfill-path concurrency gate in
    /// `execute_time_interval_model`: without it, the default `--parallel 4`
    /// would fan out concurrent writes to the *same* target table, which a
    /// serial-only adapter cannot do. The forced-serial backfill must produce
    /// the identical result to `--parallel 1`.
    #[cfg(feature = "duckdb")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn duckdb_partition_backfill_stays_serial_with_parallel_flag() {
        use rocky_core::traits::WarehouseAdapter;
        use rocky_duckdb::adapter::DuckDbWarehouseAdapter;

        let tmp = tempfile::TempDir::new().expect("temp dir");
        let models_dir = tmp.path().join("models");
        std::fs::create_dir(&models_dir).expect("mkdir models");

        // A `time_interval` model materializing into `main` (always present),
        // partitioned by day on the derived `order_date`. Mirrors the
        // partition-checksum POC that backs the dagster `partition/run_backfill`
        // fixture.
        std::fs::write(
            models_dir.join("fct_daily_orders.sql"),
            "SELECT CAST(order_at AS DATE) AS order_date, customer_id, \
             COUNT(*) AS order_count, SUM(amount) AS revenue \
             FROM raw__orders.orders \
             WHERE order_at >= @start_date AND order_at < @end_date \
             GROUP BY 1, 2\n",
        )
        .unwrap();
        std::fs::write(
            models_dir.join("fct_daily_orders.toml"),
            "depends_on = []\n\n\
             [[sources]]\ncatalog = \"\"\nschema = \"raw__orders\"\ntable = \"orders\"\n\n\
             [strategy]\ntype = \"time_interval\"\ntime_column = \"order_date\"\n\
             granularity = \"day\"\nfirst_partition = \"2026-04-04\"\nlookback = 0\n\n\
             [target]\ncatalog = \"\"\nschema = \"main\"\n",
        )
        .unwrap();

        // Seed a raw source spanning five daily partitions (2026-04-04 ..
        // 2026-04-08) into a fresh DB at `db`.
        async fn seed_source(db: &std::path::Path) {
            let seed = DuckDbWarehouseAdapter::open(db).expect("open duckdb for seeding");
            seed.execute_statement("CREATE SCHEMA IF NOT EXISTS raw__orders")
                .await
                .unwrap();
            seed.execute_statement(
                "CREATE TABLE raw__orders.orders AS SELECT * FROM (VALUES \
                 (1, 1, TIMESTAMP '2026-04-04 09:00:00', 100.0), \
                 (2, 2, TIMESTAMP '2026-04-04 14:00:00',  50.0), \
                 (3, 1, TIMESTAMP '2026-04-05 10:00:00', 200.0), \
                 (4, 2, TIMESTAMP '2026-04-06 11:00:00',  75.0), \
                 (5, 2, TIMESTAMP '2026-04-06 16:00:00',  25.0), \
                 (6, 1, TIMESTAMP '2026-04-07 08:00:00', 300.0), \
                 (7, 1, TIMESTAMP '2026-04-07 13:00:00', 150.0), \
                 (8, 2, TIMESTAMP '2026-04-07 18:00:00',  80.0), \
                 (9, 1, TIMESTAMP '2026-04-08 12:00:00', 500.0)) \
                 AS t(order_id, customer_id, order_at, amount)",
            )
            .await
            .unwrap();
        }

        // Run the inclusive backfill range against a freshly-seeded DB. The
        // serial-only DuckDB adapter (concurrency unsupported, the production
        // default) must force the run serial regardless of `--parallel`.
        async fn run_backfill(
            models_dir: &std::path::Path,
            db: &std::path::Path,
            parallel: u32,
        ) -> RunOutput {
            seed_source(db).await;
            let adapter = DuckDbWarehouseAdapter::open(db)
                .expect("open duckdb")
                .with_concurrent_for_test(false);
            let opts = PartitionRunOptions {
                from: Some("2026-04-04".into()),
                to: Some("2026-04-08".into()),
                parallel,
                ..Default::default()
            };
            let mut output = RunOutput::new(String::new(), 0, parallel.max(1) as usize);
            super::execute_models(
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
                &DeferOptions::default(),
                super::SkipGateConfig::off(),
                false,
                &rocky_core::run_vars::RunVars::new(),
            )
            .await
            .expect("backfill run must succeed on serial DuckDB under --parallel");
            output
        }

        // Read the total revenue the backfill materialized into `main`.
        async fn total_revenue(db: &std::path::Path) -> f64 {
            let v = DuckDbWarehouseAdapter::open(db).expect("reopen for verify");
            let r = v
                .execute_query("SELECT CAST(SUM(revenue) AS DOUBLE) FROM main.fct_daily_orders")
                .await
                .expect("query materialized fact");
            r.rows
                .first()
                .and_then(|row| row.first())
                .and_then(|c| c.as_str())
                .and_then(|s| s.parse::<f64>().ok())
                .expect("a numeric total revenue")
        }

        let parallel_db = tmp.path().join("backfill_parallel.duckdb");
        let serial_db = tmp.path().join("backfill_serial.duckdb");
        let parallel_out = run_backfill(&models_dir, &parallel_db, 4).await;
        let serial_out = run_backfill(&models_dir, &serial_db, 1).await;

        let parallel_summary = parallel_out
            .partition_summaries
            .iter()
            .find(|s| s.model == "fct_daily_orders")
            .expect("partition summary for the --parallel 4 run");
        let serial_summary = serial_out
            .partition_summaries
            .iter()
            .find(|s| s.model == "fct_daily_orders")
            .expect("partition summary for the --parallel 1 run");

        // All five partitions (2026-04-04 .. 2026-04-08, inclusive) must
        // succeed with none failed — the forced-serial path materialized the
        // whole range despite `--parallel 4`.
        assert_eq!(
            parallel_summary.partitions_succeeded, 5,
            "all five partitions must materialize on the forced-serial backfill path"
        );
        assert_eq!(
            parallel_summary.partitions_failed, 0,
            "no partition may fail when --parallel is forced serial on DuckDB"
        );
        // `--parallel 4` must be byte-for-byte equivalent to `--parallel 1`.
        assert_eq!(
            parallel_summary.partitions_succeeded, serial_summary.partitions_succeeded,
            "--parallel 4 must materialize the same partitions as --parallel 1 on DuckDB"
        );
        assert_eq!(
            total_revenue(&parallel_db).await,
            total_revenue(&serial_db).await,
            "parallel and serial backfills must materialize identical data"
        );
    }

    // -----------------------------------------------------------------------
    // `--skip-unchanged` model-skip gate (B2 ∧ B3)
    //
    // DuckDB-backed, no credentials. Each test seeds a raw source + a model,
    // runs once to establish a baseline (persisting the RunRecord so the next
    // run can read the prior skip_hash / upstream_freshness), then runs again
    // under a chosen `SkipGateConfig` and asserts the build-vs-skip verdict.
    // The verdict is observed via `output.materializations` (a built model
    // appears) and `output.tables_skipped`. "Target not rewritten" is checked
    // with a sentinel row inserted between runs: a skip leaves it intact, a
    // rebuild (full_refresh CTAS) drops it.
    // -----------------------------------------------------------------------

    /// An active gate with the given tolerances (feature on, no force-rebuild,
    /// not a shadow run).
    #[cfg(feature = "duckdb")]
    fn active_gate(rowcount_fallback: bool, lag_tolerance_seconds: u64) -> SkipGateConfig {
        SkipGateConfig {
            feature_enabled: true,
            force_rebuild: false,
            rowcount_fallback,
            lag_tolerance_seconds,
            shadow_or_branch: false,
        }
    }

    /// Run `execute_models` against `db_path` with a real state store and the
    /// given gate config, then persist the run as a `RunRecord` so a
    /// subsequent run can read this run's `skip_hash` / `upstream_freshness`
    /// baseline. Returns the `RunOutput`.
    #[cfg(feature = "duckdb")]
    async fn run_with_gate(
        models_dir: &std::path::Path,
        db_path: &std::path::Path,
        state_store: &StateStore,
        gate: SkipGateConfig,
        run_id: &str,
        persist_status: rocky_core::state::RunStatus,
    ) -> RunOutput {
        use rocky_duckdb::adapter::DuckDbWarehouseAdapter;

        let adapter = DuckDbWarehouseAdapter::open(db_path).expect("open duckdb");
        let opts = PartitionRunOptions::default();
        let mut output = RunOutput::new(String::new(), 0, 1);
        super::execute_models(
            models_dir,
            &adapter as &dyn rocky_core::traits::WarehouseAdapter,
            Some(state_store),
            &opts,
            run_id,
            None,
            &mut output,
            None,
            None,
            &rocky_core::config::SchemaCacheConfig::default(),
            false,
            &DeferOptions::default(),
            gate,
            false,
            &rocky_core::run_vars::RunVars::new(),
        )
        .await
        .expect("gate run must succeed");

        // Persist the run so the next run sees this baseline. We override the
        // per-model status to `persist_status` so the "prior failed" tests can
        // record a failed baseline even though the materialization succeeded.
        //
        // `started_at` is derived from the trailing run-number so successive
        // runs are strictly ordered (list_runs sorts by started_at desc): a
        // wall-clock `now()` could tie at microsecond resolution and make
        // "which run is latest" ambiguous.
        let seq: i64 = run_id
            .rsplit('-')
            .next()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let started = chrono::DateTime::<Utc>::from_timestamp(1_700_000_000 + seq * 60, 0)
            .expect("valid timestamp");
        let mut record = output.to_run_record(
            run_id,
            started,
            started,
            "cfg".to_string(),
            rocky_core::state::RunTrigger::Manual,
            rocky_core::state::RunStatus::Success,
            RunRecordAudit::test_sentinels(),
        );
        if persist_status == rocky_core::state::RunStatus::Failure {
            for m in &mut record.models_executed {
                m.status = "failed".to_string();
            }
        }
        state_store.record_run(&record).expect("record run");
        output
    }

    /// True when `output` materialized `model` this run (i.e. it built).
    #[cfg(feature = "duckdb")]
    fn built(output: &RunOutput, model: &str) -> bool {
        output
            .materializations
            .iter()
            .any(|m| m.asset_key.last().map(String::as_str) == Some(model))
    }

    /// The `(decision, reason)` the run surfaced for `model`, if any. Used by
    /// the gate tests to assert the per-model decision entry mirrors what the
    /// gate actually decided (not just the build-vs-skip outcome).
    #[cfg(feature = "duckdb")]
    fn decision_for<'a>(
        output: &'a RunOutput,
        model: &str,
    ) -> Option<(crate::output::ModelDecision, &'a str)> {
        output
            .model_decisions
            .iter()
            .find(|d| d.model == model)
            .map(|d| (d.decision, d.reason.as_str()))
    }

    /// Write a full_refresh model `name.sql`/`name.toml` into `main`, with an
    /// optional `[skip]` block appended to the sidecar.
    #[cfg(feature = "duckdb")]
    fn write_model_with_skip(dir: &std::path::Path, name: &str, sql: &str, skip_block: &str) {
        std::fs::write(dir.join(format!("{name}.sql")), format!("{sql}\n")).unwrap();
        std::fs::write(
            dir.join(format!("{name}.toml")),
            format!(
                "[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"\"\nschema = \"main\"\ntable = \"{name}\"\n{skip_block}"
            ),
        )
        .unwrap();
    }

    /// Seed a raw `main.src` table with `n` integer rows.
    #[cfg(feature = "duckdb")]
    async fn seed_src(db_path: &std::path::Path, n: u32) {
        use rocky_core::traits::WarehouseAdapter;
        use rocky_duckdb::adapter::DuckDbWarehouseAdapter;
        let seed = DuckDbWarehouseAdapter::open(db_path).expect("seed open");
        seed.execute_statement("CREATE SCHEMA IF NOT EXISTS main")
            .await
            .unwrap();
        seed.execute_statement("DROP TABLE IF EXISTS main.src")
            .await
            .unwrap();
        let values: Vec<String> = (1..=n).map(|i| format!("({i})")).collect();
        seed.execute_statement(&format!(
            "CREATE TABLE main.src AS SELECT * FROM (VALUES {}) AS t(id)",
            values.join(", ")
        ))
        .await
        .unwrap();
    }

    /// Insert a sentinel row into a model's target table so a later run can
    /// detect whether the target was rewritten (a full_refresh CTAS would drop
    /// the sentinel; a skip leaves it).
    #[cfg(feature = "duckdb")]
    async fn insert_sentinel(db_path: &std::path::Path, table: &str, sentinel: i64) {
        use rocky_core::traits::WarehouseAdapter;
        use rocky_duckdb::adapter::DuckDbWarehouseAdapter;
        let a = DuckDbWarehouseAdapter::open(db_path).expect("sentinel open");
        a.execute_statement(&format!("INSERT INTO main.{table} VALUES ({sentinel})"))
            .await
            .unwrap();
    }

    /// Count rows in `main.<table>`.
    #[cfg(feature = "duckdb")]
    async fn count_rows(db_path: &std::path::Path, table: &str) -> i64 {
        use rocky_core::traits::WarehouseAdapter;
        use rocky_duckdb::adapter::DuckDbWarehouseAdapter;
        let a = DuckDbWarehouseAdapter::open(db_path).expect("count open");
        let r = a
            .execute_query(&format!("SELECT COUNT(*) FROM main.{table}"))
            .await
            .unwrap();
        r.rows[0][0]
            .as_i64()
            .or_else(|| r.rows[0][0].as_str().and_then(|s| s.parse().ok()))
            .unwrap()
    }

    /// Both unchanged ⇒ SKIP, and the target is NOT rewritten.
    ///
    /// Uses the rowcount fallback (a full_refresh model reading a raw source
    /// has no tracked timestamp column, so rowcount is the available B3
    /// signal). A sentinel row inserted after the baseline build survives the
    /// second run iff the model was skipped.
    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn gate_both_unchanged_skips_and_does_not_rewrite_target() {
        let tmp = tempfile::TempDir::new().unwrap();
        let models_dir = tmp.path().join("models");
        std::fs::create_dir(&models_dir).unwrap();
        let db = tmp.path().join("g.duckdb");
        let state = StateStore::open(&tmp.path().join("state")).unwrap();

        seed_src(&db, 3).await;
        write_model_with_skip(&models_dir, "agg", "SELECT id FROM main.src", "");

        // Baseline build.
        let out1 = run_with_gate(
            &models_dir,
            &db,
            &state,
            active_gate(true, 0),
            "run-1",
            rocky_core::state::RunStatus::Success,
        )
        .await;
        assert!(built(&out1, "agg"), "first run must build (no baseline)");

        // Sentinel: if the second run skips, this row remains (4 rows); if it
        // rebuilds, the CTAS drops it (back to 3 rows).
        insert_sentinel(&db, "agg", 999).await;
        assert_eq!(count_rows(&db, "agg").await, 4);

        // Second run: logic + source rowcount both unchanged ⇒ SKIP.
        let out2 = run_with_gate(
            &models_dir,
            &db,
            &state,
            active_gate(true, 0),
            "run-2",
            rocky_core::state::RunStatus::Success,
        )
        .await;
        assert!(!built(&out2, "agg"), "both-unchanged must skip");
        assert_eq!(out2.tables_skipped, 1, "skip must be counted");
        assert_eq!(
            count_rows(&db, "agg").await,
            4,
            "a skip must NOT rewrite the target — sentinel row survives"
        );
        // The per-model decision entry must report Skip with the matching
        // reason — the surfaced justification mirrors the gate's actual path.
        assert_eq!(
            decision_for(&out2, "agg"),
            Some((
                crate::output::ModelDecision::Skip,
                "logic and upstream data unchanged since last build"
            )),
            "skip decision + reason must be surfaced"
        );
        // The first (baseline) run built because there was no prior baseline.
        assert_eq!(
            decision_for(&out1, "agg"),
            Some((
                crate::output::ModelDecision::Build,
                "no prior successful build to compare against"
            )),
            "first build must report the no-prior-baseline reason"
        );
        // A default (gate-off) run surfaces NO decisions — byte-identical.
        let tmp_off = tempfile::TempDir::new().unwrap();
        let db_off = tmp_off.path().join("off.duckdb");
        let state_off = StateStore::open(&tmp_off.path().join("state")).unwrap();
        seed_src(&db_off, 3).await;
        let out_off = run_with_gate(
            &models_dir,
            &db_off,
            &state_off,
            SkipGateConfig::off(),
            "run-off",
            rocky_core::state::RunStatus::Success,
        )
        .await;
        assert!(
            out_off.model_decisions.is_empty(),
            "a gate-off run must surface no per-model decisions"
        );
    }

    /// Logic changed (a different WHERE predicate) ⇒ BUILD even when upstream
    /// data is unchanged.
    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn gate_logic_changed_builds() {
        let tmp = tempfile::TempDir::new().unwrap();
        let models_dir = tmp.path().join("models");
        std::fs::create_dir(&models_dir).unwrap();
        let db = tmp.path().join("g.duckdb");
        let state = StateStore::open(&tmp.path().join("state")).unwrap();

        seed_src(&db, 5).await;
        write_model_with_skip(
            &models_dir,
            "agg",
            "SELECT id FROM main.src WHERE id > 1",
            "",
        );
        run_with_gate(
            &models_dir,
            &db,
            &state,
            active_gate(true, 0),
            "run-1",
            rocky_core::state::RunStatus::Success,
        )
        .await;

        // Change the predicate — same source, different logic.
        write_model_with_skip(
            &models_dir,
            "agg",
            "SELECT id FROM main.src WHERE id > 2",
            "",
        );
        let out2 = run_with_gate(
            &models_dir,
            &db,
            &state,
            active_gate(true, 0),
            "run-2",
            rocky_core::state::RunStatus::Success,
        )
        .await;
        assert!(built(&out2, "agg"), "a changed predicate must rebuild");
        assert_eq!(
            decision_for(&out2, "agg"),
            Some((
                crate::output::ModelDecision::Build,
                "model logic changed since last build"
            )),
            "a changed predicate must surface the B2 (logic-changed) reason"
        );
    }

    /// Upstream rowcount changed ⇒ BUILD (rowcount-fallback signal).
    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn gate_upstream_rowcount_changed_builds() {
        let tmp = tempfile::TempDir::new().unwrap();
        let models_dir = tmp.path().join("models");
        std::fs::create_dir(&models_dir).unwrap();
        let db = tmp.path().join("g.duckdb");
        let state = StateStore::open(&tmp.path().join("state")).unwrap();

        seed_src(&db, 3).await;
        write_model_with_skip(&models_dir, "agg", "SELECT id FROM main.src", "");
        run_with_gate(
            &models_dir,
            &db,
            &state,
            active_gate(true, 0),
            "run-1",
            rocky_core::state::RunStatus::Success,
        )
        .await;

        // Append a source row — rowcount moves from 3 to 4.
        seed_src(&db, 4).await;
        let out2 = run_with_gate(
            &models_dir,
            &db,
            &state,
            active_gate(true, 0),
            "run-2",
            rocky_core::state::RunStatus::Success,
        )
        .await;
        assert!(
            built(&out2, "agg"),
            "a changed upstream rowcount must rebuild"
        );
        assert_eq!(
            decision_for(&out2, "agg"),
            Some((
                crate::output::ModelDecision::Build,
                "upstream data may have changed since last build"
            )),
            "a changed upstream rowcount must surface the B3 (upstream-changed) reason"
        );
    }

    /// Upstream `MAX(ts)` advanced ⇒ BUILD (watermark signal, via an
    /// incremental-strategy timestamp column on a ts-bearing source).
    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn gate_upstream_watermark_advanced_builds() {
        use rocky_core::traits::WarehouseAdapter;
        use rocky_duckdb::adapter::DuckDbWarehouseAdapter;

        let tmp = tempfile::TempDir::new().unwrap();
        let models_dir = tmp.path().join("models");
        std::fs::create_dir(&models_dir).unwrap();
        let db = tmp.path().join("g.duckdb");
        let state = StateStore::open(&tmp.path().join("state")).unwrap();

        // Source with a timestamp column, plus a pre-created incremental
        // target (the incremental strategy appends; it does not CTAS).
        {
            let s = DuckDbWarehouseAdapter::open(&db).unwrap();
            s.execute_statement("CREATE SCHEMA IF NOT EXISTS main")
                .await
                .unwrap();
            s.execute_statement(
                "CREATE TABLE main.ev AS SELECT * FROM (VALUES \
                 (1, TIMESTAMP '2024-01-01 00:00:00')) AS t(id, ts)",
            )
            .await
            .unwrap();
            s.execute_statement("CREATE TABLE main.agg AS SELECT * FROM main.ev WHERE 1=0")
                .await
                .unwrap();
        }
        // Incremental strategy so the gate tracks `ts` for the MAX(ts) probe.
        std::fs::write(models_dir.join("agg.sql"), "SELECT id, ts FROM main.ev\n").unwrap();
        std::fs::write(
            models_dir.join("agg.toml"),
            "[strategy]\ntype = \"incremental\"\ntimestamp_column = \"ts\"\n\n[target]\ncatalog = \"\"\nschema = \"main\"\ntable = \"agg\"\n",
        )
        .unwrap();

        run_with_gate(
            &models_dir,
            &db,
            &state,
            active_gate(false, 0),
            "run-1",
            rocky_core::state::RunStatus::Success,
        )
        .await;

        // Advance MAX(ts) by inserting a later-timestamped row.
        {
            let s = DuckDbWarehouseAdapter::open(&db).unwrap();
            s.execute_statement("INSERT INTO main.ev VALUES (2, TIMESTAMP '2024-06-01 00:00:00')")
                .await
                .unwrap();
        }
        let out2 = run_with_gate(
            &models_dir,
            &db,
            &state,
            active_gate(false, 0),
            "run-2",
            rocky_core::state::RunStatus::Success,
        )
        .await;
        assert!(
            built(&out2, "agg"),
            "an advanced upstream MAX(ts) must rebuild"
        );
    }

    /// A model whose `FROM` is a subquery hides its real upstreams from the
    /// lineage extractor (it records the opaque `(subquery)` marker). The gate
    /// cannot enumerate the true sources, so it must BUILD even when the
    /// underlying data changed — never skip against an upstream it never
    /// examined. This is the load-bearing fail-safe for incomplete lineage.
    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn gate_subquery_source_always_builds() {
        let tmp = tempfile::TempDir::new().unwrap();
        let models_dir = tmp.path().join("models");
        std::fs::create_dir(&models_dir).unwrap();
        let db = tmp.path().join("g.duckdb");
        let state = StateStore::open(&tmp.path().join("state")).unwrap();

        seed_src(&db, 3).await;
        // Subquery in FROM — the extractor sees only `(subquery)`, not main.src.
        write_model_with_skip(
            &models_dir,
            "agg",
            "SELECT id FROM (SELECT id FROM main.src) t",
            "",
        );
        run_with_gate(
            &models_dir,
            &db,
            &state,
            active_gate(true, 0),
            "run-1",
            rocky_core::state::RunStatus::Success,
        )
        .await;

        // Change the underlying source rowcount. A correct gate must rebuild
        // (it cannot prove the hidden upstream is unchanged); a buggy gate that
        // dropped the subquery marker would wrongly skip on stale data.
        seed_src(&db, 5).await;
        let out2 = run_with_gate(
            &models_dir,
            &db,
            &state,
            active_gate(true, 0),
            "run-2",
            rocky_core::state::RunStatus::Success,
        )
        .await;
        assert!(
            built(&out2, "agg"),
            "a subquery-in-FROM model must always build — upstreams aren't enumerable"
        );
    }

    /// First run (no prior successful execution) ⇒ BUILD.
    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn gate_first_run_builds() {
        let tmp = tempfile::TempDir::new().unwrap();
        let models_dir = tmp.path().join("models");
        std::fs::create_dir(&models_dir).unwrap();
        let db = tmp.path().join("g.duckdb");
        let state = StateStore::open(&tmp.path().join("state")).unwrap();

        seed_src(&db, 3).await;
        write_model_with_skip(&models_dir, "agg", "SELECT id FROM main.src", "");
        let out = run_with_gate(
            &models_dir,
            &db,
            &state,
            active_gate(true, 0),
            "run-1",
            rocky_core::state::RunStatus::Success,
        )
        .await;
        assert!(built(&out, "agg"), "first run has no baseline ⇒ must build");
        assert_eq!(out.tables_skipped, 0);
    }

    /// Prior run failed (latest execution status != success) ⇒ BUILD, even
    /// though logic + data are unchanged. Covers the stricter variant: a
    /// matching-hash success exists *earlier*, but the latest is a failure.
    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn gate_prior_failed_builds_even_with_older_matching_success() {
        let tmp = tempfile::TempDir::new().unwrap();
        let models_dir = tmp.path().join("models");
        std::fs::create_dir(&models_dir).unwrap();
        let db = tmp.path().join("g.duckdb");
        let state = StateStore::open(&tmp.path().join("state")).unwrap();

        seed_src(&db, 3).await;
        write_model_with_skip(&models_dir, "agg", "SELECT id FROM main.src", "");

        // run-1: an older success with a matching skip_hash.
        run_with_gate(
            &models_dir,
            &db,
            &state,
            active_gate(true, 0),
            "run-1",
            rocky_core::state::RunStatus::Success,
        )
        .await;

        // Inject a *later* run whose latest `agg` execution FAILED (status !=
        // success, no skip_hash) — exactly what a real failed run records. The
        // older success (run-1) still carries a matching hash.
        let now = chrono::DateTime::<Utc>::from_timestamp(1_700_000_000 + 120, 0).unwrap();
        let failed = rocky_core::state::RunRecord {
            run_id: "run-2-failed".to_string(),
            started_at: now,
            finished_at: now,
            status: rocky_core::state::RunStatus::Failure,
            models_executed: vec![rocky_core::state::ModelExecution {
                model_name: "agg".to_string(),
                started_at: now,
                finished_at: now,
                duration_ms: 0,
                rows_affected: None,
                status: "failed".to_string(),
                sql_hash: String::new(),
                skip_hash: None,
                upstream_freshness: None,
                bytes_scanned: None,
                bytes_written: None,
                tenant: None,
                recipe_hash: None,
                input_hash: None,
                input_proof_class: None,
                env_hash: None,
                hash_scheme: None,
                output_column_hashes: None,
            }],
            trigger: rocky_core::state::RunTrigger::Manual,
            config_hash: "cfg".to_string(),
            triggering_identity: None,
            session_source: rocky_core::state::SessionSource::Cli,
            git_commit: None,
            git_branch: None,
            idempotency_key: None,
            target_catalog: None,
            hostname: "test".to_string(),
            rocky_version: "test".to_string(),
        };
        state.record_run(&failed).unwrap();

        // run-3: the latest `agg` execution is the failure ⇒ must build, never
        // skip back to the older success.
        let out3 = run_with_gate(
            &models_dir,
            &db,
            &state,
            active_gate(true, 0),
            "run-3",
            rocky_core::state::RunStatus::Success,
        )
        .await;
        assert!(
            built(&out3, "agg"),
            "a latest-failed baseline must rebuild even with an older matching success"
        );
    }

    /// Feature off (default gate) ⇒ BUILD even when both unchanged.
    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn gate_feature_off_builds() {
        let tmp = tempfile::TempDir::new().unwrap();
        let models_dir = tmp.path().join("models");
        std::fs::create_dir(&models_dir).unwrap();
        let db = tmp.path().join("g.duckdb");
        let state = StateStore::open(&tmp.path().join("state")).unwrap();

        seed_src(&db, 3).await;
        write_model_with_skip(&models_dir, "agg", "SELECT id FROM main.src", "");
        run_with_gate(
            &models_dir,
            &db,
            &state,
            active_gate(true, 0),
            "run-1",
            rocky_core::state::RunStatus::Success,
        )
        .await;

        // Second run with the gate OFF — must build despite unchanged inputs.
        let out2 = run_with_gate(
            &models_dir,
            &db,
            &state,
            SkipGateConfig::off(),
            "run-2",
            rocky_core::state::RunStatus::Success,
        )
        .await;
        assert!(built(&out2, "agg"), "gate off ⇒ always build");
        assert_eq!(out2.tables_skipped, 0);
    }

    /// `--force-rebuild` ⇒ BUILD even when both unchanged and the feature is
    /// on.
    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn gate_force_rebuild_builds() {
        let tmp = tempfile::TempDir::new().unwrap();
        let models_dir = tmp.path().join("models");
        std::fs::create_dir(&models_dir).unwrap();
        let db = tmp.path().join("g.duckdb");
        let state = StateStore::open(&tmp.path().join("state")).unwrap();

        seed_src(&db, 3).await;
        write_model_with_skip(&models_dir, "agg", "SELECT id FROM main.src", "");
        run_with_gate(
            &models_dir,
            &db,
            &state,
            active_gate(true, 0),
            "run-1",
            rocky_core::state::RunStatus::Success,
        )
        .await;

        let forced = SkipGateConfig {
            feature_enabled: true,
            force_rebuild: true,
            rowcount_fallback: true,
            lag_tolerance_seconds: 0,
            shadow_or_branch: false,
        };
        let out2 = run_with_gate(
            &models_dir,
            &db,
            &state,
            forced,
            "run-2",
            rocky_core::state::RunStatus::Success,
        )
        .await;
        assert!(built(&out2, "agg"), "--force-rebuild must always build");
    }

    /// A non-deterministic model (`RANDOM()`) is never auto-skipped, even when
    /// both look unchanged — and the SAME model with `[skip] deterministic =
    /// true` becomes eligible and skips.
    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn gate_non_deterministic_never_skips_unless_declared() {
        // Arm 1: non-deterministic SQL ⇒ never skip.
        {
            let tmp = tempfile::TempDir::new().unwrap();
            let models_dir = tmp.path().join("models");
            std::fs::create_dir(&models_dir).unwrap();
            let db = tmp.path().join("g.duckdb");
            let state = StateStore::open(&tmp.path().join("state")).unwrap();

            seed_src(&db, 3).await;
            write_model_with_skip(
                &models_dir,
                "agg",
                "SELECT id, RANDOM() AS r FROM main.src",
                "",
            );
            run_with_gate(
                &models_dir,
                &db,
                &state,
                active_gate(true, 0),
                "run-1",
                rocky_core::state::RunStatus::Success,
            )
            .await;
            let out2 = run_with_gate(
                &models_dir,
                &db,
                &state,
                active_gate(true, 0),
                "run-2",
                rocky_core::state::RunStatus::Success,
            )
            .await;
            assert!(
                built(&out2, "agg"),
                "non-deterministic SQL must never auto-skip"
            );
        }
        // Arm 2: same SQL, but the owner declares it deterministic ⇒ eligible.
        {
            let tmp = tempfile::TempDir::new().unwrap();
            let models_dir = tmp.path().join("models");
            std::fs::create_dir(&models_dir).unwrap();
            let db = tmp.path().join("g.duckdb");
            let state = StateStore::open(&tmp.path().join("state")).unwrap();

            seed_src(&db, 3).await;
            write_model_with_skip(
                &models_dir,
                "agg",
                "SELECT id, RANDOM() AS r FROM main.src",
                "\n[skip]\ndeterministic = true\n",
            );
            run_with_gate(
                &models_dir,
                &db,
                &state,
                active_gate(true, 0),
                "run-1",
                rocky_core::state::RunStatus::Success,
            )
            .await;
            let out2 = run_with_gate(
                &models_dir,
                &db,
                &state,
                active_gate(true, 0),
                "run-2",
                rocky_core::state::RunStatus::Success,
            )
            .await;
            assert!(
                !built(&out2, "agg"),
                "[skip] deterministic = true must re-enable skip eligibility"
            );
        }
    }

    /// `[skip] eligible = false` forces a model to always build.
    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn gate_eligible_false_forces_build() {
        let tmp = tempfile::TempDir::new().unwrap();
        let models_dir = tmp.path().join("models");
        std::fs::create_dir(&models_dir).unwrap();
        let db = tmp.path().join("g.duckdb");
        let state = StateStore::open(&tmp.path().join("state")).unwrap();

        seed_src(&db, 3).await;
        write_model_with_skip(
            &models_dir,
            "agg",
            "SELECT id FROM main.src",
            "\n[skip]\neligible = false\n",
        );
        run_with_gate(
            &models_dir,
            &db,
            &state,
            active_gate(true, 0),
            "run-1",
            rocky_core::state::RunStatus::Success,
        )
        .await;
        let out2 = run_with_gate(
            &models_dir,
            &db,
            &state,
            active_gate(true, 0),
            "run-2",
            rocky_core::state::RunStatus::Success,
        )
        .await;
        assert!(
            built(&out2, "agg"),
            "[skip] eligible = false must always build"
        );
    }

    /// DAG recursion: when an upstream Rocky model is rebuilt this run, a
    /// downstream whose own logic + raw inputs are unchanged still rebuilds.
    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn gate_dag_recursion_downstream_builds_when_upstream_rebuilt() {
        let tmp = tempfile::TempDir::new().unwrap();
        let models_dir = tmp.path().join("models");
        std::fs::create_dir(&models_dir).unwrap();
        let db = tmp.path().join("g.duckdb");
        let state = StateStore::open(&tmp.path().join("state")).unwrap();

        seed_src(&db, 3).await;
        // up reads the raw source; down reads up (a Rocky-model upstream).
        write_model_with_skip(&models_dir, "up", "SELECT id FROM main.src", "");
        write_model_with_skip(&models_dir, "down", "SELECT id FROM up", "");
        run_with_gate(
            &models_dir,
            &db,
            &state,
            active_gate(true, 0),
            "run-1",
            rocky_core::state::RunStatus::Success,
        )
        .await;

        // Change ONLY up's logic so up rebuilds; down's own logic is unchanged.
        write_model_with_skip(
            &models_dir,
            "up",
            "SELECT id FROM main.src WHERE id > 0",
            "",
        );
        let out2 = run_with_gate(
            &models_dir,
            &db,
            &state,
            active_gate(true, 0),
            "run-2",
            rocky_core::state::RunStatus::Success,
        )
        .await;
        assert!(built(&out2, "up"), "up's changed logic must rebuild it");
        assert!(
            built(&out2, "down"),
            "down must rebuild because its upstream up was rebuilt this run (B3 recursion)"
        );
    }

    /// `lag_tolerance_seconds`: a sub-tolerance MAX(ts) movement is treated as
    /// unchanged (SKIP) only when a tolerance is configured; an above-tolerance
    /// movement always builds; the default tolerance 0 builds on any movement.
    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn gate_lag_tolerance_absorbs_small_movement() {
        use rocky_core::traits::WarehouseAdapter;
        use rocky_duckdb::adapter::DuckDbWarehouseAdapter;

        async fn setup(db: &std::path::Path) {
            let s = DuckDbWarehouseAdapter::open(db).unwrap();
            s.execute_statement("CREATE SCHEMA IF NOT EXISTS main")
                .await
                .unwrap();
            s.execute_statement("DROP TABLE IF EXISTS main.ev")
                .await
                .unwrap();
            s.execute_statement(
                "CREATE TABLE main.ev AS SELECT * FROM (VALUES \
                 (1, TIMESTAMP '2024-01-01 00:00:00')) AS t(id, ts)",
            )
            .await
            .unwrap();
            // Pre-create the incremental target (append strategy, no CTAS).
            s.execute_statement("DROP TABLE IF EXISTS main.agg")
                .await
                .unwrap();
            s.execute_statement("CREATE TABLE main.agg AS SELECT * FROM main.ev WHERE 1=0")
                .await
                .unwrap();
        }
        fn write_inc(models_dir: &std::path::Path) {
            std::fs::write(models_dir.join("agg.sql"), "SELECT id, ts FROM main.ev\n").unwrap();
            std::fs::write(
                models_dir.join("agg.toml"),
                "[strategy]\ntype = \"incremental\"\ntimestamp_column = \"ts\"\n\n[target]\ncatalog = \"\"\nschema = \"main\"\ntable = \"agg\"\n",
            )
            .unwrap();
        }

        // Movement of 30s, tolerance 60s ⇒ within tolerance ⇒ SKIP.
        {
            let tmp = tempfile::TempDir::new().unwrap();
            let models_dir = tmp.path().join("models");
            std::fs::create_dir(&models_dir).unwrap();
            let db = tmp.path().join("g.duckdb");
            let state = StateStore::open(&tmp.path().join("state")).unwrap();
            setup(&db).await;
            write_inc(&models_dir);
            run_with_gate(
                &models_dir,
                &db,
                &state,
                active_gate(false, 60),
                "run-1",
                rocky_core::state::RunStatus::Success,
            )
            .await;
            let s = DuckDbWarehouseAdapter::open(&db).unwrap();
            s.execute_statement("INSERT INTO main.ev VALUES (2, TIMESTAMP '2024-01-01 00:00:30')")
                .await
                .unwrap();
            let out2 = run_with_gate(
                &models_dir,
                &db,
                &state,
                active_gate(false, 60),
                "run-2",
                rocky_core::state::RunStatus::Success,
            )
            .await;
            assert!(
                !built(&out2, "agg"),
                "a 30s move under a 60s tolerance must skip"
            );
        }
        // Movement of 30s, default tolerance 0 ⇒ any movement ⇒ BUILD.
        {
            let tmp = tempfile::TempDir::new().unwrap();
            let models_dir = tmp.path().join("models");
            std::fs::create_dir(&models_dir).unwrap();
            let db = tmp.path().join("g.duckdb");
            let state = StateStore::open(&tmp.path().join("state")).unwrap();
            setup(&db).await;
            write_inc(&models_dir);
            run_with_gate(
                &models_dir,
                &db,
                &state,
                active_gate(false, 0),
                "run-1",
                rocky_core::state::RunStatus::Success,
            )
            .await;
            let s = DuckDbWarehouseAdapter::open(&db).unwrap();
            s.execute_statement("INSERT INTO main.ev VALUES (2, TIMESTAMP '2024-01-01 00:00:30')")
                .await
                .unwrap();
            let out2 = run_with_gate(
                &models_dir,
                &db,
                &state,
                active_gate(false, 0),
                "run-2",
                rocky_core::state::RunStatus::Success,
            )
            .await;
            assert!(
                built(&out2, "agg"),
                "default tolerance 0 must build on any movement"
            );
        }
    }

    /// Parallel parity: the same both-unchanged scenario yields an identical
    /// skip verdict under `--parallel 1` and `--parallel N` (the gate sits at
    /// the single shared site feeding both paths).
    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn gate_parallel_parity() {
        use rocky_duckdb::adapter::DuckDbWarehouseAdapter;

        async fn scenario(concurrent: bool, parallel: u32) -> RunOutput {
            let tmp = tempfile::TempDir::new().unwrap();
            let models_dir = tmp.path().join("models");
            std::fs::create_dir(&models_dir).unwrap();
            let db = tmp.path().join("g.duckdb");
            let state = StateStore::open(&tmp.path().join("state")).unwrap();
            seed_src(&db, 3).await;
            write_model_with_skip(&models_dir, "agg", "SELECT id FROM main.src", "");
            run_with_gate(
                &models_dir,
                &db,
                &state,
                active_gate(true, 0),
                "run-1",
                rocky_core::state::RunStatus::Success,
            )
            .await;

            // Second run via the chosen parallelism, gate active.
            let adapter = DuckDbWarehouseAdapter::open(&db)
                .unwrap()
                .with_concurrent_for_test(concurrent);
            let opts = PartitionRunOptions {
                parallel,
                ..Default::default()
            };
            let mut output = RunOutput::new(String::new(), 0, parallel.max(1) as usize);
            super::execute_models(
                &models_dir,
                &adapter as &dyn rocky_core::traits::WarehouseAdapter,
                Some(&state),
                &opts,
                "run-2",
                None,
                &mut output,
                None,
                None,
                &rocky_core::config::SchemaCacheConfig::default(),
                false,
                &DeferOptions::default(),
                active_gate(true, 0),
                false,
                &rocky_core::run_vars::RunVars::new(),
            )
            .await
            .unwrap();
            output
        }

        let serial = scenario(false, 1).await;
        let parallel = scenario(true, 4).await;
        assert_eq!(
            serial.tables_skipped, parallel.tables_skipped,
            "skip verdict must be identical under --parallel 1 and N"
        );
        assert_eq!(
            serial.tables_skipped, 1,
            "both runs must skip the unchanged model"
        );
    }

    /// Forward-deserialization: a prior `ModelExecution` lacking the skip
    /// fields (so `skip_hash == None`) forces a BUILD even when logic + data
    /// are unchanged — clause D fails. We simulate the legacy baseline by
    /// recording a success whose `skip_hash` was stripped.
    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn gate_pre_field_baseline_builds() {
        let tmp = tempfile::TempDir::new().unwrap();
        let models_dir = tmp.path().join("models");
        std::fs::create_dir(&models_dir).unwrap();
        let db = tmp.path().join("g.duckdb");
        let state = StateStore::open(&tmp.path().join("state")).unwrap();

        seed_src(&db, 3).await;
        write_model_with_skip(&models_dir, "agg", "SELECT id FROM main.src", "");

        // Build once, then strip the skip fields from the persisted record to
        // mimic a pre-gate (forward-deserialized) baseline.
        run_with_gate(
            &models_dir,
            &db,
            &state,
            active_gate(true, 0),
            "run-1",
            rocky_core::state::RunStatus::Success,
        )
        .await;
        let mut runs = state.list_runs(10).unwrap();
        let mut rec = runs.pop().unwrap();
        for m in &mut rec.models_executed {
            m.skip_hash = None;
            m.upstream_freshness = None;
        }
        state.record_run(&rec).unwrap();

        let out2 = run_with_gate(
            &models_dir,
            &db,
            &state,
            active_gate(true, 0),
            "run-2",
            rocky_core::state::RunStatus::Success,
        )
        .await;
        assert!(
            built(&out2, "agg"),
            "a baseline lacking skip_hash (pre-gate) must rebuild — clause D fails"
        );
    }

    // -----------------------------------------------------------------------
    // Incomplete-lineage fail-safe matrix.
    //
    // The lineage walk only enumerates top-level bare `FROM`/`JOIN` tables; it
    // silently drops PIVOT/UNNEST/nested-join table-factors and never descends
    // into sub-queries. A model built on any of those hides a real upstream
    // from the gate, so it must ALWAYS build — never skip against a source it
    // never examined. Each test mutates the hidden/dropped source between runs
    // and asserts the second run still BUILDS, mirroring
    // `gate_subquery_source_always_builds`.
    // -----------------------------------------------------------------------

    /// `FROM t PIVOT(...)`: the PIVOT table-factor is dropped by the lineage
    /// walk, so `main.monthly_sales` would be invisible to the enumeration. The
    /// model must build even after the pivoted source changes. (Belt-and-braces
    /// here: a PIVOT query's IR has no stable `skip_hash`, so clause D would
    /// already force a build; the completeness whitelist makes the build
    /// independent of that. The fix-isolating guard is the unit test
    /// `rocky_sql::lineage_complete::tests::pivot_source_is_incomplete`.)
    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn gate_pivot_source_always_builds() {
        use rocky_core::traits::WarehouseAdapter;
        use rocky_duckdb::adapter::DuckDbWarehouseAdapter;

        let tmp = tempfile::TempDir::new().unwrap();
        let models_dir = tmp.path().join("models");
        std::fs::create_dir(&models_dir).unwrap();
        let db = tmp.path().join("g.duckdb");
        let state = StateStore::open(&tmp.path().join("state")).unwrap();

        async fn seed_sales(db: &std::path::Path, jan: i64, feb: i64) {
            let s = DuckDbWarehouseAdapter::open(db).unwrap();
            s.execute_statement("CREATE SCHEMA IF NOT EXISTS main")
                .await
                .unwrap();
            s.execute_statement("DROP TABLE IF EXISTS main.monthly_sales")
                .await
                .unwrap();
            s.execute_statement(&format!(
                "CREATE TABLE main.monthly_sales AS SELECT * FROM (VALUES \
                 ('jan', {jan}), ('feb', {feb})) AS t(month, amount)"
            ))
            .await
            .unwrap();
        }

        seed_sales(&db, 10, 20).await;
        write_model_with_skip(
            &models_dir,
            "agg",
            "SELECT * FROM main.monthly_sales PIVOT(SUM(amount) FOR month IN ('jan', 'feb'))",
            "",
        );
        run_with_gate(
            &models_dir,
            &db,
            &state,
            active_gate(true, 0),
            "run-1",
            rocky_core::state::RunStatus::Success,
        )
        .await;

        // Change the pivoted source values (rowcount steady). The PIVOT
        // table-factor is not provably complete, so the gate refuses to skip.
        seed_sales(&db, 999, 888).await;
        let out2 = run_with_gate(
            &models_dir,
            &db,
            &state,
            active_gate(true, 0),
            "run-2",
            rocky_core::state::RunStatus::Success,
        )
        .await;
        assert!(
            built(&out2, "agg"),
            "a PIVOT model hides its source from lineage — it must always build"
        );
    }

    /// `CROSS JOIN UNNEST(...)`: the UNNEST table function parses as a
    /// table-valued `Table { args: Some(..) }`, which the completeness
    /// whitelist rejects. Even though the row source `main.events` is itself
    /// enumerable, the model must build on any change. (Belt-and-braces here:
    /// the lineage walk also surfaces a phantom `UNNEST` "table" whose
    /// `COUNT(*)` probe fails, which would already force a build; the
    /// completeness whitelist makes the build independent of that. The
    /// fix-isolating guard is the unit test
    /// `rocky_sql::lineage_complete::tests::unnest_in_from_is_incomplete`.)
    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn gate_unnest_source_always_builds() {
        use rocky_core::traits::WarehouseAdapter;
        use rocky_duckdb::adapter::DuckDbWarehouseAdapter;

        let tmp = tempfile::TempDir::new().unwrap();
        let models_dir = tmp.path().join("models");
        std::fs::create_dir(&models_dir).unwrap();
        let db = tmp.path().join("g.duckdb");
        let state = StateStore::open(&tmp.path().join("state")).unwrap();

        async fn seed_events(db: &std::path::Path, rows: &str) {
            let s = DuckDbWarehouseAdapter::open(db).unwrap();
            s.execute_statement("CREATE SCHEMA IF NOT EXISTS main")
                .await
                .unwrap();
            s.execute_statement("DROP TABLE IF EXISTS main.events")
                .await
                .unwrap();
            s.execute_statement(&format!(
                "CREATE TABLE main.events AS SELECT * FROM (VALUES {rows}) AS t(id, items)"
            ))
            .await
            .unwrap();
        }

        seed_events(&db, "(1, [10, 20]), (2, [30])").await;
        write_model_with_skip(
            &models_dir,
            "agg",
            "SELECT id, e FROM main.events CROSS JOIN UNNEST(items) AS t(e)",
            "",
        );
        run_with_gate(
            &models_dir,
            &db,
            &state,
            active_gate(true, 0),
            "run-1",
            rocky_core::state::RunStatus::Success,
        )
        .await;

        // Change the unnested arrays. The UNNEST table function disqualifies
        // the model from skipping, so it must rebuild.
        seed_events(&db, "(1, [10, 20, 30]), (2, [30, 40])").await;
        let out2 = run_with_gate(
            &models_dir,
            &db,
            &state,
            active_gate(true, 0),
            "run-2",
            rocky_core::state::RunStatus::Success,
        )
        .await;
        assert!(
            built(&out2, "agg"),
            "an UNNEST model is not provably complete — it must always build"
        );
    }

    /// `FROM (a JOIN b)`: the parenthesised join parses as a
    /// `TableFactor::NestedJoin`, dropped by the lineage walk. The model must
    /// build after either nested source changes.
    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn gate_nested_join_source_always_builds() {
        use rocky_core::traits::WarehouseAdapter;
        use rocky_duckdb::adapter::DuckDbWarehouseAdapter;

        let tmp = tempfile::TempDir::new().unwrap();
        let models_dir = tmp.path().join("models");
        std::fs::create_dir(&models_dir).unwrap();
        let db = tmp.path().join("g.duckdb");
        let state = StateStore::open(&tmp.path().join("state")).unwrap();

        async fn seed_pair(db: &std::path::Path, a_rows: u32, b_rows: u32) {
            let s = DuckDbWarehouseAdapter::open(db).unwrap();
            s.execute_statement("CREATE SCHEMA IF NOT EXISTS main")
                .await
                .unwrap();
            for (tbl, n) in [("a", a_rows), ("b", b_rows)] {
                s.execute_statement(&format!("DROP TABLE IF EXISTS main.{tbl}"))
                    .await
                    .unwrap();
                let values: Vec<String> = (1..=n).map(|i| format!("({i})")).collect();
                s.execute_statement(&format!(
                    "CREATE TABLE main.{tbl} AS SELECT * FROM (VALUES {}) AS t(id)",
                    values.join(", ")
                ))
                .await
                .unwrap();
            }
        }

        seed_pair(&db, 3, 3).await;
        write_model_with_skip(
            &models_dir,
            "agg",
            "SELECT a.id FROM (main.a a JOIN main.b b ON a.id = b.id)",
            "",
        );
        run_with_gate(
            &models_dir,
            &db,
            &state,
            active_gate(true, 0),
            "run-1",
            rocky_core::state::RunStatus::Success,
        )
        .await;

        // Grow one nested source. The NestedJoin factor hid both tables from
        // lineage (the enumeration returned an empty source set), so the
        // completeness whitelist refuses to skip and the gate builds. (Without
        // the whitelist this model would wrongly SKIP — it is one of the two
        // cases that genuinely depend on this fix.)
        seed_pair(&db, 5, 3).await;
        let out2 = run_with_gate(
            &models_dir,
            &db,
            &state,
            active_gate(true, 0),
            "run-2",
            rocky_core::state::RunStatus::Success,
        )
        .await;
        assert!(
            built(&out2, "agg"),
            "a nested-join FROM hides its sources — it must always build"
        );
    }

    /// `... WHERE id IN (SELECT dim_id FROM facts)`: the hidden source `facts`
    /// lives only inside the `IN` sub-query, which the lineage walk never
    /// descends into. With `rowcount_fallback` on and `dim` held steady, a gate
    /// that ignored the sub-query would prove `dim` unchanged and wrongly skip;
    /// the completeness whitelist rejects the sub-query so the model builds when
    /// `facts` changes.
    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn gate_in_subquery_source_always_builds() {
        use rocky_core::traits::WarehouseAdapter;
        use rocky_duckdb::adapter::DuckDbWarehouseAdapter;

        let tmp = tempfile::TempDir::new().unwrap();
        let models_dir = tmp.path().join("models");
        std::fs::create_dir(&models_dir).unwrap();
        let db = tmp.path().join("g.duckdb");
        let state = StateStore::open(&tmp.path().join("state")).unwrap();

        async fn seed_dim(db: &std::path::Path) {
            let s = DuckDbWarehouseAdapter::open(db).unwrap();
            s.execute_statement("CREATE SCHEMA IF NOT EXISTS main")
                .await
                .unwrap();
            s.execute_statement("DROP TABLE IF EXISTS main.dim")
                .await
                .unwrap();
            s.execute_statement(
                "CREATE TABLE main.dim AS SELECT * FROM (VALUES (1), (2), (3)) AS t(id)",
            )
            .await
            .unwrap();
        }
        async fn seed_facts(db: &std::path::Path, n: u32) {
            let s = DuckDbWarehouseAdapter::open(db).unwrap();
            s.execute_statement("DROP TABLE IF EXISTS main.facts")
                .await
                .unwrap();
            let values: Vec<String> = (1..=n).map(|i| format!("({})", i % 3 + 1)).collect();
            s.execute_statement(&format!(
                "CREATE TABLE main.facts AS SELECT * FROM (VALUES {}) AS t(dim_id)",
                values.join(", ")
            ))
            .await
            .unwrap();
        }

        // `dim` stays steady across both runs; only the hidden `facts` source
        // (reachable only through the IN sub-query) moves.
        seed_dim(&db).await;
        seed_facts(&db, 2).await;
        write_model_with_skip(
            &models_dir,
            "agg",
            "SELECT id FROM main.dim WHERE id IN (SELECT dim_id FROM main.facts)",
            "",
        );
        run_with_gate(
            &models_dir,
            &db,
            &state,
            active_gate(true, 0),
            "run-1",
            rocky_core::state::RunStatus::Success,
        )
        .await;

        // Change only `facts`. The visible `dim` is unchanged, so a gate that
        // ignored the IN sub-query would wrongly skip on stale data; the
        // completeness whitelist forces a build. (This is the sharpest of the
        // four — `dim`'s rowcount is steady and `rowcount_fallback` is on, so
        // the gate would prove B3 against `dim` alone and skip; the build is
        // driven by the completeness check rejecting the sub-query, not by the
        // freshness probe. One of the two cases that genuinely depend on this
        // fix.)
        seed_facts(&db, 6).await;
        let out2 = run_with_gate(
            &models_dir,
            &db,
            &state,
            active_gate(true, 0),
            "run-2",
            rocky_core::state::RunStatus::Success,
        )
        .await;
        assert!(
            built(&out2, "agg"),
            "an IN-subquery source is invisible to lineage — the model must always build"
        );
    }

    // -- reuse spine: catalog-ambiguous read must not false-strong -----------
    //
    // These exercise the *production* lookup builder + resolver
    // (`build_reuse_target_by_model` / `resolve_read_set_content_upstreams`),
    // not a hand-rolled map, so the "no catalog-less `schema.table` key"
    // guarantee is genuinely under test.

    fn target_cfg(catalog: &str, schema: &str, table: &str) -> rocky_core::models::TargetConfig {
        rocky_core::models::TargetConfig {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
        }
    }

    #[test]
    fn reuse_target_map_omits_catalog_less_schema_table_key() {
        // Two models share `marts.orders` across distinct catalogs — exactly
        // the collision the compiler permits (it only rejects duplicate model
        // NAMES). The map must carry no `marts.orders` key, only the bare names
        // and the full 3-part identities.
        let t1 = target_cfg("cat1", "marts", "orders");
        let t2 = target_cfg("cat2", "marts", "orders");
        let map = build_reuse_target_by_model([("orders_a", &t1), ("orders_b", &t2)]);

        assert!(
            !map.contains_key("marts.orders"),
            "a catalog-less schema.table key would let a 2-part read false-strong"
        );
        assert_eq!(map.get("orders_a"), Some(&"cat1.marts.orders".to_string()));
        assert_eq!(map.get("orders_b"), Some(&"cat2.marts.orders".to_string()));
        assert_eq!(
            map.get("cat1.marts.orders"),
            Some(&"cat1.marts.orders".to_string())
        );
        assert_eq!(
            map.get("cat2.marts.orders"),
            Some(&"cat2.marts.orders".to_string())
        );
    }

    #[test]
    fn reuse_two_part_ambiguous_read_leaves_model_unindexed() {
        // (a) Two models cat1.marts.orders + cat2.marts.orders both built this
        // run; a downstream content-addressed model reads the catalog-ambiguous
        // 2-part `FROM marts.orders`. It must NOT resolve to either upstream's
        // content hash — the whole model is left UNINDEXED (None).
        let t1 = target_cfg("cat1", "marts", "orders");
        let t2 = target_cfg("cat2", "marts", "orders");
        let target_by_model = build_reuse_target_by_model([("orders_a", &t1), ("orders_b", &t2)]);

        let mut outputs = std::collections::HashMap::new();
        outputs.insert("cat1.marts.orders".to_string(), "hash-cat1".to_string());
        outputs.insert("cat2.marts.orders".to_string(), "hash-cat2".to_string());

        let resolved = resolve_read_set_content_upstreams(
            "SELECT id FROM marts.orders",
            &target_by_model,
            &outputs,
        );
        assert!(
            resolved.is_none(),
            "a catalog-ambiguous 2-part read must leave the downstream unindexed, \
             not bind it to an arbitrary upstream's hash under a strong label"
        );
    }

    #[test]
    fn reuse_three_part_qualified_read_resolves_to_named_catalog() {
        // (b) A fully-qualified 3-part read names a single catalog and must
        // resolve to THAT catalog's content hash — the legitimate strong chain
        // still works.
        let t1 = target_cfg("cat1", "marts", "orders");
        let t2 = target_cfg("cat2", "marts", "orders");
        let target_by_model = build_reuse_target_by_model([("orders_a", &t1), ("orders_b", &t2)]);

        let mut outputs = std::collections::HashMap::new();
        outputs.insert("cat1.marts.orders".to_string(), "hash-cat1".to_string());
        outputs.insert("cat2.marts.orders".to_string(), "hash-cat2".to_string());

        let resolved = resolve_read_set_content_upstreams(
            "SELECT id FROM cat1.marts.orders",
            &target_by_model,
            &outputs,
        )
        .expect("a fully-qualified 3-part read resolves");
        assert_eq!(resolved.len(), 1);
        match &resolved[0] {
            rocky_core::reuse::UpstreamIdentity::Content {
                upstream_key,
                blake3_hash,
            } => {
                assert_eq!(upstream_key, "cat1.marts.orders");
                assert_eq!(
                    blake3_hash, "hash-cat1",
                    "must bind cat1's hash, not cat2's"
                );
            }
            other => panic!("expected a Content identity, got {other:?}"),
        }
    }

    #[test]
    fn reuse_one_part_bare_name_read_still_resolves() {
        // (c) A 1-part bare model-name read still resolves (names are unique
        // per project, so there is no ambiguity to refuse).
        let t1 = target_cfg("cat1", "marts", "orders");
        let target_by_model = build_reuse_target_by_model([("orders_a", &t1)]);

        let mut outputs = std::collections::HashMap::new();
        outputs.insert("cat1.marts.orders".to_string(), "hash-cat1".to_string());

        let resolved = resolve_read_set_content_upstreams(
            "SELECT id FROM orders_a",
            &target_by_model,
            &outputs,
        )
        .expect("a 1-part bare-name read resolves");
        assert_eq!(resolved.len(), 1);
        match &resolved[0] {
            rocky_core::reuse::UpstreamIdentity::Content {
                upstream_key,
                blake3_hash,
            } => {
                assert_eq!(upstream_key, "cat1.marts.orders");
                assert_eq!(blake3_hash, "hash-cat1");
            }
            other => panic!("expected a Content identity, got {other:?}"),
        }
    }

    // -- consumer-side per-column baseline (T2 P2, record-only) --------------
    //
    // These drive the PRODUCTION baseline computation
    // (`compute_consumer_baseline`) over the PRODUCTION completeness guard
    // (`rocky_sql::consumed_columns`) + the production resolver map
    // (`build_reuse_target_by_model`), on real SQL — not a hand-faked consumed
    // set — so the guard, the FROM→upstream resolution, and the
    // case-insensitive producer-hash filter are all genuinely under test.

    fn ch(column: &str, hash: &str) -> rocky_core::state::ColumnHash {
        rocky_core::state::ColumnHash {
            column: column.to_string(),
            hash: hash.to_string(),
        }
    }

    #[test]
    fn consumer_baseline_records_only_consumed_columns_case_insensitively() {
        // Upstream `u` produced three columns (ORIGINAL Arrow-field casing);
        // the downstream projects `id`, filters on `name`, and never reads
        // `email`. The baseline must carry `id` + `name` (consumed) and NOT
        // `email`, matching case-insensitively (consumed_columns lowercases;
        // the producer keeps original case) — the exact bug that would silently
        // record an empty baseline and make the feature inert.
        let t_u = target_cfg("cat", "sch", "u");
        let target_by_model = build_reuse_target_by_model([("u", &t_u)]);

        let mut built = std::collections::HashMap::new();
        built.insert(
            "cat.sch.u".to_string(),
            vec![
                ch("ID", "h_id"),
                ch("Name", "h_name"),
                ch("Email", "h_email"),
            ],
        );

        let sigs = compute_consumer_baseline(
            "SELECT id FROM u WHERE name = 'x'",
            &target_by_model,
            &built,
        )
        .expect("a provably-complete consumed set yields a baseline");

        assert_eq!(sigs.len(), 1);
        assert_eq!(sigs[0].upstream_key, "cat.sch.u");
        // Freshness fields are not this path's concern.
        assert_eq!(sigs[0].max_ts, None);
        assert_eq!(sigs[0].row_count, None);
        let cols = sigs[0]
            .consumed_column_hashes
            .as_ref()
            .expect("consumed columns resolved to producer hashes");
        assert_eq!(
            cols,
            &vec![ch("ID", "h_id"), ch("Name", "h_name")],
            "records the producer hashes for the consumed columns (id + name), \
             excludes the never-read `email`, matched case-insensitively"
        );
    }

    #[test]
    fn consumer_baseline_fails_closed_on_incomplete_consumed_set() {
        // A `SELECT *` can't enumerate consumed columns ⇒ the guard force-builds
        // ⇒ no baseline (None), which forces a safe rebuild in the later gate.
        let t_u = target_cfg("cat", "sch", "u");
        let target_by_model = build_reuse_target_by_model([("u", &t_u)]);
        let mut built = std::collections::HashMap::new();
        built.insert("cat.sch.u".to_string(), vec![ch("id", "h_id")]);

        assert!(
            compute_consumer_baseline("SELECT * FROM u", &target_by_model, &built).is_none(),
            "an un-enumerable consumed set must yield no baseline (fail closed)"
        );
    }

    #[test]
    fn consumer_baseline_none_for_upstream_without_producer_hashes() {
        // The downstream provably consumes `u.id`, but `u` recorded no producer
        // hashes this run (not built content-addressed, reused, or partitioned).
        // The upstream is recorded with `consumed_column_hashes: None` — honest
        // "consumed, but no column baseline ⇒ the later gate must build".
        let t_u = target_cfg("cat", "sch", "u");
        let target_by_model = build_reuse_target_by_model([("u", &t_u)]);
        let built = std::collections::HashMap::new(); // u produced no hashes

        let sigs = compute_consumer_baseline("SELECT id FROM u", &target_by_model, &built)
            .expect("consumed set is complete even when the upstream has no hashes");
        assert_eq!(sigs.len(), 1);
        assert_eq!(sigs[0].upstream_key, "cat.sch.u");
        assert_eq!(sigs[0].consumed_column_hashes, None);
    }

    #[test]
    fn consumer_baseline_none_when_a_consumed_column_is_missing_from_producer() {
        // The downstream consumes `id` + `name`, but the producer recorded only
        // `id` (a rename or a stale hash). Every consumed column must resolve or
        // the upstream can't back a column skip — fail closed to `None`.
        let t_u = target_cfg("cat", "sch", "u");
        let target_by_model = build_reuse_target_by_model([("u", &t_u)]);
        let mut built = std::collections::HashMap::new();
        built.insert("cat.sch.u".to_string(), vec![ch("id", "h_id")]);

        let sigs =
            compute_consumer_baseline("SELECT id, name FROM u", &target_by_model, &built).unwrap();
        assert_eq!(sigs.len(), 1);
        assert_eq!(
            sigs[0].consumed_column_hashes, None,
            "a consumed column absent from the producer's hashes ⇒ no baseline for that upstream"
        );
    }

    /// Runtime regression: a first run of an incremental transformation
    /// model against a missing target must bootstrap the table (via
    /// `generate_transformation_initial_ddl`) and load the source **exactly
    /// once** — the populated CTAS is the load, so the subsequent `INSERT INTO`
    /// is skipped. Before this wiring the first run hit `INSERT INTO` against a
    /// nonexistent table and errored; a naive fix that ran the INSERT after the
    /// CTAS would double-load. This drives the real `execute_one_plain_model`
    /// runtime path on in-memory DuckDB (format = None, so dialect-independent
    /// of the lakehouse DDL — what's proven here is the skip, not the format).
    ///
    /// The model SQL carries no watermark filter (transformation incrementals
    /// own their own filtering), so a second run re-selects the full source and
    /// appends it — proving the table is reused, not recreated, and that the
    /// skip only fires on the bootstrap run.
    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn incremental_transformation_first_run_loads_source_once_then_appends() {
        use std::time::Instant;

        use rocky_core::models::load_model_pair;
        use rocky_core::traits::WarehouseAdapter;
        use rocky_duckdb::adapter::DuckDbWarehouseAdapter;
        use rocky_duckdb::dialect::DuckDbSqlDialect;

        let adapter = DuckDbWarehouseAdapter::in_memory().unwrap();
        for ddl in [
            "CREATE SCHEMA src",
            "CREATE SCHEMA tgt",
            "CREATE TABLE src.events (id INTEGER, region VARCHAR)",
            "INSERT INTO src.events VALUES (1, 'a'), (2, 'b'), (3, 'c')",
        ] {
            adapter.execute_statement(ddl).await.unwrap();
        }
        let source_rows: i64 = 3;

        // Build a real `Model` from a sidecar + SQL pair so the runtime path
        // (`Model::to_model_ir` → `execute_one_plain_model`) is exercised
        // end-to-end rather than hand-assembling the IR.
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(
            dir.path().join("fct_events.toml"),
            r#"
name = "fct_events"

[strategy]
type = "incremental"
timestamp_column = "id"

[target]
catalog = ""
schema = "tgt"
table = "fct_events"
"#,
        )
        .unwrap();
        std::fs::write(
            dir.path().join("fct_events.sql"),
            "SELECT id, region FROM src.events",
        )
        .unwrap();
        let model = load_model_pair(
            &dir.path().join("fct_events.sql"),
            &dir.path().join("fct_events.toml"),
            None,
        )
        .expect("load incremental model");

        let dialect = DuckDbSqlDialect;
        let typed_models = indexmap::IndexMap::new();
        let model_timings = std::collections::HashMap::new();
        let surrogate_keys = std::collections::HashMap::new();
        let exec_ctx = super::ExecutionContext {
            typed_models: &typed_models,
            model_timings: &model_timings,
            surrogate_keys: &surrogate_keys,
        };

        async fn count_target(adapter: &DuckDbWarehouseAdapter) -> i64 {
            let r = adapter
                .execute_query("SELECT COUNT(*) FROM tgt.fct_events")
                .await
                .expect("count query");
            let cell = &r.rows[0][0];
            cell.as_i64()
                .or_else(|| cell.as_str().and_then(|s| s.parse::<i64>().ok()))
                .expect("count parses as i64")
        }

        // --- First run: target missing → bootstrap CTAS, INSERT skipped. ---
        super::execute_one_plain_model(
            &model,
            &adapter as &dyn WarehouseAdapter,
            &dialect as &dyn rocky_core::traits::SqlDialect,
            "fct_events",
            Instant::now(),
            exec_ctx,
        )
        .await
        .expect("first run bootstraps without error");
        assert_eq!(
            count_target(&adapter).await,
            source_rows,
            "first run must load the source exactly once (no double-load)"
        );

        // --- Second run: target exists → normal incremental append. ---
        super::execute_one_plain_model(
            &model,
            &adapter as &dyn WarehouseAdapter,
            &dialect as &dyn rocky_core::traits::SqlDialect,
            "fct_events",
            Instant::now(),
            exec_ctx,
        )
        .await
        .expect("second run appends without error");
        assert_eq!(
            count_target(&adapter).await,
            source_rows * 2,
            "second run reuses the existing table and appends through the INSERT path"
        );
    }
}
