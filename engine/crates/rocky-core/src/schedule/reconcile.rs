//! The one-shot tick: evaluate all standing demand once, then execute what is
//! due through the claim state machine.
//!
//! [`tick_once`] is the reconciler's single orchestration point. It is called by
//! the `rocky tick` CLI and, later, by a resident loop; both pass the clock in
//! as `now` — the core reads no wall clock, so every time-dependent path is
//! deterministic under test. Children are launched through an injected
//! [`Spawner`], so the whole reconciler runs without a built binary.
//!
//! The pass, in order:
//!
//! 1. Take the tick lock (`.rocky/tick.lock`). A fresh holder means another
//!    reconciler owns this pass — skip with `tick_in_progress`. A holder whose
//!    heartbeat has gone stale is wedged — proceed via the override, letting the
//!    claim state machine keep correctness (the lock is contention-avoidance,
//!    never the correctness boundary).
//! 2. Resolve every scheduled pipeline in scope; an invalid schedule fails
//!    closed to a skip (it never fires).
//! 3. Order the pipelines topologically by their `after` edges, so a pipeline
//!    that becomes due *because an upstream ran earlier in this same tick* is
//!    evaluated after that upstream.
//! 4. For each pipeline, evaluate its demand against live state (re-read each
//!    time, so within-tick upstream successes are visible), then execute the due
//!    demand through the claim state machine: claim before spawn, in-tick retry
//!    while budget remains, terminal transition on exhaustion, and a
//!    budget-aware resolver for a claim left `submitted` by a crashed owner.
//!
//! Every "do not run" outcome is an explicit, recorded reason on the
//! [`TickReport`]. Config or state faults fail closed — the tick runs nothing
//! and surfaces a [`TickError`].

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use chrono::{DateTime, Duration, Utc};
use thiserror::Error;

use crate::config::RockyConfig;
use crate::state::{StateError, StateStore};

use super::claim::{
    ClaimCas, ClaimRecord, ClaimState, DemandKind, PostAttempt, PreSpawn, Resolved,
    TerminalOutcome, decide_post_attempt, decide_pre_spawn, decide_resolver, parse_claim_key,
    sweep_terminal_claim,
};
use super::demand::{
    Demand, EvaluatedPipeline, HistoryError, ResolvedSchedule, RunHistoryView, RunSuccess,
    ScheduleConfigError, ScheduleStateView, SkipReason, SourceSkip, evaluate_one, resolve_schedule,
};
use super::lock::{TickAcquire, TickLock};
use super::record::{ScheduleStateMutation, ScheduleStateRecord};
use super::spawn::{Drain, RunTriggerKind, SpawnRequest, Spawner};
use super::spool::{self, WebhookKind};

/// A stuck `submitted` claim with no run record is held for this grace before
/// its attempt is treated as lost (the owner crashed between committing the
/// claim and writing a record). `now` is injected, so the grace is deterministic
/// under test and a crash never stalls a standing demand indefinitely.
const RECOVERY_GRACE: Duration = Duration::minutes(5);

/// A held tick lock whose heartbeat is older than this is a wedged owner: a
/// later tick proceeds via the wedge override rather than skipping forever.
pub const LOCK_TAKEOVER_AFTER: std::time::Duration = std::time::Duration::from_secs(300);

/// How often the lease heartbeat is refreshed while a child runs. Comfortably
/// under the 60s staleness bound so a long but healthy run is never mistaken for
/// a wedged one.
const HEARTBEAT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

/// Defensive bound on the per-demand claim/retry loop. The frozen transitions
/// converge in at most `1 + retry.max` spawns plus a bounded resolver prelude;
/// exceeding this is a state-machine bug, never a reachable schedule.
const MAX_DEMAND_ITERATIONS: usize = 256;

/// Inputs to a single tick that come from the CLI/serve wrapper rather than the
/// reconciler core.
#[derive(Debug, Clone)]
pub struct TickOptions {
    /// Evaluate and report, but execute nothing and write no state.
    pub dry_run: bool,
    /// Restrict evaluation to a single pipeline (`--pipeline`); `None` = every
    /// scheduled pipeline.
    pub pipeline_filter: Option<String>,
    /// The config file path passed to each spawned `rocky run -c <path>`.
    pub config_path: PathBuf,
    /// The `.rocky` directory that holds the tick lock and its heartbeat.
    pub rocky_dir: PathBuf,
    /// The tick span's W3C trace context, propagated to each child so the run's
    /// own trace connects to the tick. `None` when tracing is inactive.
    pub trace_context: Option<crate::schedule::TraceContext>,
    /// Per-pipeline member-model freshness budgets, in seconds. Each entry
    /// holds the `max_lag_seconds` of a pipeline's member models that declare
    /// one; freshness resolution takes the MIN (the tightest budget wins),
    /// falling back to the project `[freshness].expected_lag_seconds` when a
    /// pipeline has no entry (the reconciler core loads no models — that is the
    /// wrapper's job). A pipeline absent from the map resolves exactly as
    /// before: project default only.
    pub member_budgets: BTreeMap<String, Vec<u64>>,
    /// The resolved state-store path, forwarded to each child as
    /// `--state-path <path>` so the child `rocky run` opens the SAME file the
    /// reconciler read demand from and writes its run record where the resolver
    /// and `after`/`freshness` will find it. Absolute (the CLI wrapper
    /// absolutizes). The reconciler also re-opens this path between phases —
    /// see [`tick_once`]'s store lifecycle.
    pub state_path: PathBuf,
    /// The drain signal (serve mode). When raised mid-tick, the reconciler stops
    /// evaluating *further* pipelines after the current one (it does not abandon a
    /// running child — the spawner drains that), so a shutdown never starts new
    /// work. Default (`Drain::default()`, never raised) is the CLI `rocky tick`
    /// behavior: every pipeline in scope is evaluated. Shared (an `Arc` clone)
    /// with the serve loop and its [`SubprocessSpawner`].
    pub drain: Drain,
}

/// How many times the reconciler re-opens the state store after a child before
/// deferring the demand to the next tick. Mirrors `state_sync::acquire_publish_lock`.
const REOPEN_RETRY_ATTEMPTS: u32 = 5;
/// Linear backoff base between reopen attempts. `StateStore::open` already
/// retries redb's own flock internally (5 × 50ms), so this only spaces out the
/// rarer advisory-lock (`LockHeldByOther`) contention.
const REOPEN_RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(20);

/// Contention — another `rocky` process holds the state store — as opposed to a
/// genuine fault. Both the advisory writer lock (`LockHeldByOther`) and redb's
/// own file flock (`Busy`, already retried inside `StateStore::open`) are
/// expected when a `rocky run` (a tick's own child, or a manual run) is live;
/// they map to a busy-skip, never a hard error. Every other `StateError` is a
/// fault and fails the tick closed.
fn is_state_contention(e: &StateError) -> bool {
    matches!(
        e,
        StateError::LockHeldByOther { .. } | StateError::Busy { .. }
    )
}

/// Open the store for the start of a tick. A dry run opens read-write like a
/// real tick (its writes are all `!dry_run`-gated, so it mutates no scheduler
/// state; a read-only open buys nothing — redb's flock is exclusive
/// read-or-write — and is not truly non-mutating anyway). Contention returns
/// `Ok(None)`: the whole tick busy-skips with exit 0. A genuine fault propagates.
fn open_tick_store(state_path: &Path) -> Result<Option<StateStore>, TickError> {
    match StateStore::open(state_path) {
        Ok(store) => Ok(Some(store)),
        Err(e) if is_state_contention(&e) => Ok(None),
        Err(e) => Err(TickError::State(e)),
    }
}

/// Outcome of re-opening the store after a child's window.
enum StoreReopen {
    /// The store is open again; the tick continues.
    Reopened,
    /// Contention outlasted the retry envelope — the demand's post-run
    /// bookkeeping is deferred to the next tick's stuck-claim resolution.
    Busy,
}

/// The tick's owned handle on the state store.
///
/// Owned, never borrowed from the caller, so the whole `StateStore` (its redb
/// flock AND its advisory writer lock) can be dropped around each child spawn
/// and re-opened afterwards: the child `rocky run` opens the same file and must
/// find both locks free. The tick lock (`.rocky/tick.lock`) is what keeps ticks
/// mutually exclusive while the state store is released — releasing it never
/// weakens tick exclusion, and the claim state machine is the run-in-flight
/// guard.
struct PhaseStore {
    path: PathBuf,
    inner: Option<StateStore>,
}

impl PhaseStore {
    /// The open store. Panics between [`close`](Self::close) and a successful
    /// [`reopen`](Self::reopen) — that is a lifecycle bug in this module, never
    /// a reachable schedule.
    fn store(&self) -> &StateStore {
        self.inner
            .as_ref()
            .expect("state store is open outside a child-spawn window")
    }

    /// Release the store — both locks — for a child's window. Pure RAII: dropping
    /// the `StateStore` value releases them.
    fn close(&mut self) {
        self.inner = None;
    }

    /// Re-open the store read-write after a child, retrying briefly on
    /// contention. A genuine fault propagates as [`TickError::State`]; exhausted
    /// contention is [`StoreReopen::Busy`], never an error.
    async fn reopen(&mut self) -> Result<StoreReopen, TickError> {
        for attempt in 1..=REOPEN_RETRY_ATTEMPTS {
            match StateStore::open(&self.path) {
                Ok(store) => {
                    self.inner = Some(store);
                    return Ok(StoreReopen::Reopened);
                }
                Err(e) if is_state_contention(&e) => {
                    if attempt < REOPEN_RETRY_ATTEMPTS {
                        tokio::time::sleep(REOPEN_RETRY_DELAY * attempt).await;
                    }
                }
                Err(e) => return Err(TickError::State(e)),
            }
        }
        Ok(StoreReopen::Busy)
    }
}

/// A demand that executed this tick — a child ran to a terminal outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutedDemand {
    /// The pipeline that ran.
    pub pipeline: String,
    /// The source that made it due.
    pub source: DemandKind,
    /// The logical timestamp of the demand.
    pub logical_ts: DateTime<Utc>,
    /// The submission id of the attempt whose outcome was recorded (the last
    /// attempt on an exhausted retry cycle).
    pub submission_id: String,
    /// The child's exit code.
    pub exit_code: i32,
    /// The mapped terminal outcome.
    pub outcome: TerminalOutcome,
    /// Total submissions made for this demand (the claim's monotonic audit
    /// counter at the terminal transition).
    pub attempts: u32,
}

/// Why a demand did not execute this tick. Mirrors the frozen tick-contract
/// reason set so the CLI output can render each as a stable string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TickSkipReason {
    /// No demand from this source right now.
    NotDue,
    /// The schedule is disabled (`enabled = false`).
    Disabled,
    /// The pipeline is paused at runtime (`ScheduleStateRecord.paused`) —
    /// the state-level hold that reaches a RUNNING scheduler, unlike a
    /// config edit. Recorded every tick until resumed.
    Paused,
    /// A claim is `submitted` for this key (a run is in flight, or its crashed
    /// owner is still within the recovery grace) — do not spawn over it.
    InFlight,
    /// More than one cron occurrence elapsed under `catchup = "skip"`: the anchor
    /// advanced, nothing ran.
    CatchupSkipped {
        /// Number of missed occurrences (capped for reporting).
        missed: u32,
    },
    /// A standing demand suppressed by the failure backoff ladder.
    FailureBackoff {
        /// When the demand becomes eligible again.
        resume_at: DateTime<Utc>,
    },
    /// A standing demand suppressed by the fixed partial backoff.
    PartialBackoff {
        /// When the demand becomes eligible again.
        resume_at: DateTime<Utc>,
    },
    /// The demand's key is already terminal (`completed`), or its run was
    /// observed and finalized by this tick's resolver — it will not run again.
    Dedup,
    /// A spooled webhook demand targeted a pipeline no longer in config: it was
    /// finalized without running (never left pending forever).
    ConfigError,
    /// The run history could not be read (store fault or a corrupt row), so the
    /// standing demand was skipped rather than misclassified — fail-closed, and
    /// surfaced loudly rather than silently dropped.
    HistoryUnavailable,
    /// The state store was held by another `rocky` process past the bounded
    /// reopen retry, so this demand's post-run bookkeeping (or the whole tick,
    /// when the store could not be opened at all) was deferred to the next tick.
    /// Exit 0 — normal contention with a live run, not a fault.
    StateBusy,
}

/// A per-demand skip record for the tick report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkippedDemand {
    /// The pipeline the skip belongs to.
    pub pipeline: String,
    /// The demand source, when the skip is source-specific. `None` for a
    /// pipeline-level or resolution-level skip.
    pub source: Option<DemandKind>,
    /// Why it was skipped.
    pub reason: TickSkipReason,
}

/// The result of one tick: what was evaluated, executed, and skipped.
#[derive(Debug, Clone, Default)]
pub struct TickReport {
    /// Per-pipeline evaluation snapshots (sources considered, the chosen demand,
    /// anchor movements, per-source skips).
    pub evaluated: Vec<EvaluatedPipeline>,
    /// Demands that ran to a terminal outcome this tick.
    pub executed: Vec<ExecutedDemand>,
    /// Demands suppressed this tick, each with a reason.
    pub skipped: Vec<SkippedDemand>,
    /// The whole tick was skipped because a live reconciler already holds the
    /// lock (`tick_in_progress`).
    pub skipped_whole_tick: bool,
    /// The tick proceeded via the wedge override (a stale-heartbeat lock holder);
    /// correctness fell back to the claim state machine.
    pub lock_overridden: bool,
    /// The tick never opened the state store (another `rocky` process held it),
    /// or was cut short mid-pass because the store could not be re-opened after
    /// a child. Exit-0 semantics, like `skipped_whole_tick`; the deferred work is
    /// picked up by the next tick's stuck-claim resolution.
    pub state_busy: bool,
    /// The tick stopped evaluating early because the drain was raised mid-pass
    /// (serve shutdown). Pipelines already evaluated ran normally; the remainder
    /// are left for the next process to pick up. Always `false` for `rocky tick`
    /// (its drain is never raised).
    pub drained: bool,
}

/// A fault that fails the whole tick closed — it runs nothing.
#[derive(Debug, Error)]
pub enum TickError {
    /// A state-store read or write failed.
    #[error("state store error during tick: {0}")]
    State(#[from] StateError),
    /// The tick lock could not be opened or created (a genuine filesystem fault,
    /// not mere contention).
    #[error("failed to acquire tick lock in {dir}: {source}")]
    Lock {
        /// The `.rocky` directory involved.
        dir: String,
        /// The underlying I/O error.
        source: std::io::Error,
    },
    /// The `after` graph among scheduled pipelines has a cycle — refused rather
    /// than run in an ambiguous order.
    #[error("schedule dependency cycle among pipelines: {0}")]
    Cycle(String),
    /// The per-demand loop exceeded its defensive bound — a state-machine bug.
    #[error("reconciler loop bound exceeded for demand {0}")]
    LoopBound(String),
}

/// Evaluate all standing demand once and execute what is due.
///
/// `now` is always injected — the reconciler core reads no wall clock. Children
/// run through `spawner`, so the logic is testable without a binary.
///
/// # Errors
///
/// Returns [`TickError`] when the tick cannot proceed safely: a state fault, a
/// lock I/O fault, or an `after` cycle. In every such case the tick runs
/// nothing (fail closed).
pub async fn tick_once(
    config: &RockyConfig,
    state_path: &Path,
    now: DateTime<Utc>,
    spawner: &dyn Spawner,
    opts: &TickOptions,
) -> Result<TickReport, TickError> {
    let mut report = TickReport::default();

    // 1. Mutual exclusion. A dry run makes no writes and spawns nothing, so it
    //    never contends for the lock (a preview must not be starved). A real
    //    tick takes the flock: a fresh holder means skip, a wedged holder (stale
    //    heartbeat) means proceed via the override.
    let lock = if opts.dry_run {
        None
    } else {
        match TickLock::try_acquire(&opts.rocky_dir).map_err(|e| TickError::Lock {
            dir: opts.rocky_dir.display().to_string(),
            source: e,
        })? {
            TickAcquire::Acquired(held) => Some(held),
            TickAcquire::Busy { heartbeat_age } => {
                // A missing/unreadable heartbeat is treated as fresh (skip) — the
                // override is reserved for a demonstrably stale holder, never a
                // race we cannot read.
                let wedged = heartbeat_age.is_some_and(|age| age > LOCK_TAKEOVER_AFTER);
                if wedged {
                    report.lock_overridden = true;
                    None
                } else {
                    report.skipped_whole_tick = true;
                    return Ok(report);
                }
            }
        }
    };

    // 2. Open the store — AFTER the tick lock, so tick-vs-tick contention resolves
    //    to the graceful skip above rather than dying here. A live `rocky run`
    //    (this tick's own child from a prior pass, or a manual run) holding the
    //    store is a whole-tick busy-skip (exit 0), not a fault. The store is owned
    //    (not borrowed) so it can be released around each child spawn — the child
    //    opens the same file and must find both locks free.
    let Some(store) = open_tick_store(state_path)? else {
        report.state_busy = true;
        return Ok(report);
    };
    let mut phase = PhaseStore {
        path: state_path.to_path_buf(),
        inner: Some(store),
    };

    // 3. Resolve schedules in scope; invalid ones fail closed to skips.
    let (schedules, resolve_skips) = resolve_all_schedules(
        config,
        opts.pipeline_filter.as_deref(),
        &opts.member_budgets,
    );
    report.skipped.extend(resolve_skips);

    // 4. Topological order by `after`, so a same-tick upstream success can make a
    //    downstream due. A cycle is an infra error — fail closed.
    let order = topological_order(&schedules)?;

    // 5. Evaluate + execute each pipeline in order against live state.
    for name in &order {
        // Drain (serve shutdown): stop evaluating *further* pipelines. A child
        // already running for an earlier pipeline this tick is drained by the
        // spawner, not abandoned here; we simply start no new work. A dry run
        // never drains (it spawns nothing and the serve loop never dry-runs).
        if !opts.dry_run && opts.drain.is_signalled() {
            report.drained = true;
            break;
        }
        let schedule = &schedules[name];
        if let Some(held) = &lock {
            let _ = held.heartbeat();
        }
        if !opts.dry_run {
            phase.store().touch_schedule_evaluated(name, now)?;
        }

        let ev = {
            let store = phase.store();
            let state_view = StoreState(store);
            let history_view = StoreHistory(store);
            evaluate_one(schedule, &state_view, &history_view, now)
        };

        // Persist non-firing anchor movements (first-sight init, catch-up skip).
        if !opts.dry_run {
            if let Some(at) = ev.anchor_init {
                phase.store().advance_schedule_fire_anchor(name, at)?;
            }
            if let Some(at) = ev.catchup_advance {
                phase.store().advance_schedule_fire_anchor(name, at)?;
            }
        }

        for skip in &ev.skips {
            if let Some(mapped) = map_skip(name, skip) {
                report.skipped.push(mapped);
            }
        }
        report.evaluated.push(ev.clone());

        let Some(demand) = ev.due else {
            continue;
        };
        if opts.dry_run {
            // Reported as due via `evaluated`; a dry run executes nothing.
            continue;
        }
        match execute_demand(
            &demand,
            schedule,
            &mut phase,
            now,
            spawner,
            opts,
            lock.as_ref(),
        )
        .await?
        {
            ExecOutcome::Executed(ed) => report.executed.push(ed),
            ExecOutcome::Skipped(sd) => report.skipped.push(sd),
            ExecOutcome::StoreBusy(sd) => {
                // The store is closed and could not be re-opened; nothing further
                // can be evaluated safely this pass.
                report.skipped.push(sd);
                report.state_busy = true;
                return Ok(report);
            }
        }
    }

    // 6. Consume durable webhook demands from the spool. Each spooled demand is
    //    claimed (at most once) and its child spawned, then the spool file is
    //    disposed. A dry run spawns nothing, so it never touches the spool.
    if !opts.dry_run {
        let consume = consume_webhook_demands(
            config,
            &schedules,
            &mut phase,
            now,
            spawner,
            opts,
            lock.as_ref(),
            &mut report,
        )
        .await?;
        if consume == WebhookConsume::StoreBusy {
            report.state_busy = true;
            return Ok(report);
        }
    }

    // 7. Sweep orphaned `submitted` claims — a prior tick's deferred or crashed
    //    terminal transaction — independent of whether their demand is due again
    //    (a cron occurrence advances its anchor on submission, so its claim is
    //    otherwise never re-evaluated). Only on a healthy real tick with the
    //    store still open.
    if !opts.dry_run {
        sweep_orphan_claims(&schedules, phase.store(), &opts.rocky_dir, now)?;
        // Bound the id-dedup window: drop `.done` tombstones past their TTL.
        if let Err(e) = spool::sweep_tombstones(&spool::spool_dir(&opts.rocky_dir), now) {
            tracing::warn!(error = %e, "webhook tombstone sweep failed");
        }
    }

    Ok(report)
}

/// Whether the webhook consume phase completed or bailed on store contention.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WebhookConsume {
    /// Every pending demand was processed (or deferred to a later tick).
    Done,
    /// The store could not be re-opened after a child ran; the caller must end
    /// the pass. Any un-processed demand stays spooled for the next tick.
    StoreBusy,
}

/// Build the schedule a webhook demand runs under: the pipeline's resolved
/// schedule when it has one (so a webhook run inherits the scheduler-level
/// timeout), else a minimal enabled schedule. `retry_max` is forced to `0` in
/// both cases — a webhook is one-shot (consumed by being attempted), so a
/// crashed attempt is finalized as a failure, never re-run.
fn webhook_schedule(
    schedules: &BTreeMap<String, ResolvedSchedule>,
    pipeline: &str,
) -> ResolvedSchedule {
    match schedules.get(pipeline) {
        Some(base) => ResolvedSchedule {
            retry_max: 0,
            ..base.clone()
        },
        None => ResolvedSchedule {
            pipeline: pipeline.to_string(),
            enabled: true,
            cron: None,
            after: Vec::new(),
            freshness_budget: None,
            catchup: super::demand::Catchup::Skip,
            retry_max: 0,
            timeout: None,
        },
    }
}

/// Consume the durable webhook spool: for each pending demand, drive its claim
/// to a terminal state (spawning the child) and then dispose of the file.
///
/// The reconciler is the single-threaded dedup authority. For a
/// [`WebhookKind::Id`] demand it consults the `.done` tombstone *before*
/// claiming, so an accept-side race (a redelivery landing a fresh file after
/// consumption) can never cause a second run — the tombstone drops the
/// duplicate. A pipeline no longer in config is finalized and disposed loudly so
/// it never lingers forever; an unparseable file is quarantined, never silently
/// deleted.
#[allow(clippy::too_many_arguments)]
async fn consume_webhook_demands(
    config: &RockyConfig,
    schedules: &BTreeMap<String, ResolvedSchedule>,
    phase: &mut PhaseStore,
    now: DateTime<Utc>,
    spawner: &dyn Spawner,
    opts: &TickOptions,
    lock: Option<&TickLock>,
    report: &mut TickReport,
) -> Result<WebhookConsume, TickError> {
    let files = match spool::list_pending_files(&opts.rocky_dir) {
        Ok(f) => f,
        Err(e) => {
            tracing::warn!(error = %e, "scanning the webhook spool failed; skipping this tick");
            return Ok(WebhookConsume::Done);
        }
    };

    for path in files {
        // Stop starting new webhook work once a drain is raised, exactly like the
        // per-pipeline loop; already-spooled demands survive to the next tick.
        if opts.drain.is_signalled() {
            report.drained = true;
            break;
        }

        let demand_record = match spool::read_pending(&path) {
            Ok(d) => d,
            Err(e) => {
                match spool::quarantine(&path, now) {
                    Ok(dest) => tracing::warn!(
                        error = %e,
                        quarantined = %dest.display(),
                        "corrupt webhook spool file quarantined; not processed"
                    ),
                    Err(qe) => tracing::error!(
                        error = %qe,
                        file = %path.display(),
                        "failed to quarantine a corrupt webhook spool file"
                    ),
                }
                continue;
            }
        };

        // A pipeline removed from config: finalize (never run) and dispose loudly
        // so an orphaned demand does not linger forever.
        if !config.pipelines.contains_key(&demand_record.pipeline) {
            tracing::warn!(
                pipeline = %demand_record.pipeline,
                demand_uid = %demand_record.demand_uid,
                "webhook demand targets a pipeline absent from config; finalizing without running"
            );
            if let Err(e) = spool::dispose(&path, demand_record.kind) {
                tracing::error!(error = %e, "failed to dispose a config-orphaned webhook demand");
            }
            report.skipped.push(SkippedDemand {
                pipeline: demand_record.pipeline.clone(),
                source: Some(DemandKind::Webhook),
                reason: TickSkipReason::ConfigError,
            });
            continue;
        }

        // The runtime hold gates webhooks too — a paused pipeline DEFERS its
        // spooled demands rather than executing them (the bypass the #1334
        // red team confirmed) and rather than dropping them: the file stays
        // PENDING, unconsumed, so an accepted (202'd) delivery fires on
        // resume instead of vanishing. Recorded as a skip each tick, like
        // every other suppressed source. A cursor read failure falls through
        // to normal consumption — fail-open on the READ here would hold every
        // webhook hostage to a transient store error, and the pause check
        // re-runs next tick.
        let paused = phase
            .store()
            .get_schedule_state(&demand_record.pipeline)
            .ok()
            .flatten()
            .is_some_and(|cursor| cursor.paused);
        if paused {
            report.skipped.push(SkippedDemand {
                pipeline: demand_record.pipeline.clone(),
                source: Some(DemandKind::Webhook),
                reason: TickSkipReason::Paused,
            });
            continue;
        }

        // Id-dedup authority: a tombstone means this delivery id already ran.
        // Drop the (duplicate) pending file without running.
        if demand_record.kind == WebhookKind::Id && spool::is_tombstoned(&path) {
            if let Err(e) = spool::drop_pending(&path) {
                tracing::warn!(error = %e, "failed to drop a duplicate webhook demand");
            }
            continue;
        }

        let schedule = webhook_schedule(schedules, &demand_record.pipeline);
        let logical_ts = DateTime::parse_from_rfc3339(&demand_record.received_at)
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or(now);
        let demand = Demand {
            pipeline: demand_record.pipeline.clone(),
            source: DemandKind::Webhook,
            logical_ts,
            claim_discriminator: Some(demand_record.demand_uid.clone()),
        };

        match execute_demand(&demand, &schedule, phase, now, spawner, opts, lock).await? {
            ExecOutcome::Executed(ed) => {
                report.executed.push(ed);
                if let Err(e) = spool::dispose(&path, demand_record.kind) {
                    tracing::error!(error = %e, "failed to dispose a consumed webhook demand");
                }
            }
            ExecOutcome::Skipped(sd) => {
                let terminal = matches!(sd.reason, TickSkipReason::Dedup);
                report.skipped.push(sd);
                if terminal {
                    // The claim reached (or was already) terminal — dispose.
                    if let Err(e) = spool::dispose(&path, demand_record.kind) {
                        tracing::error!(error = %e, "failed to dispose a finalized webhook demand");
                    }
                }
                // Otherwise (in-flight / within crash grace): leave the file for
                // the next tick to re-resolve.
            }
            ExecOutcome::StoreBusy(sd) => {
                report.skipped.push(sd);
                return Ok(WebhookConsume::StoreBusy);
            }
        }
    }

    Ok(WebhookConsume::Done)
}

/// A read-only [`ScheduleStateView`] backed by the live store.
struct StoreState<'a>(&'a StateStore);

impl ScheduleStateView for StoreState<'_> {
    fn get(&self, pipeline: &str) -> ScheduleStateRecord {
        self.0
            .get_schedule_state(pipeline)
            .ok()
            .flatten()
            .unwrap_or_default()
    }
}

/// A read-only [`RunHistoryView`] backed by the live store. Each query re-reads
/// the store, so a child that ran earlier in the tick is visible immediately.
struct StoreHistory<'a>(&'a StateStore);

impl RunHistoryView for StoreHistory<'_> {
    fn latest_successful_run(&self, pipeline: &str) -> Result<Option<RunSuccess>, HistoryError> {
        // A read fault (store error or a corrupt `run_history` row) is surfaced
        // as an error, NOT swallowed to "no success" — the caller fails closed
        // and records a skip, so a bad row can neither false-skip an `after`
        // demand nor false-fire a `freshness` one.
        match self.0.latest_successful_run(pipeline) {
            Ok(run) => Ok(run.map(|r| RunSuccess {
                started_at: r.started_at,
                finished_at: r.finished_at,
            })),
            Err(e) => Err(HistoryError(e.to_string())),
        }
    }
}

/// Resolve every scheduled pipeline in scope. An unresolvable schedule (bad
/// cron/timezone/catchup, or `freshness = true` with no budget) fails closed to
/// a skip — `rocky validate` surfaces the reason; the reconciler never fires it.
fn resolve_all_schedules(
    config: &RockyConfig,
    filter: Option<&str>,
    member_budgets: &BTreeMap<String, Vec<u64>>,
) -> (BTreeMap<String, ResolvedSchedule>, Vec<SkippedDemand>) {
    let mut resolved = BTreeMap::new();
    let mut skips = Vec::new();
    // Iteration is over the detailed BTreeMap (sorted by pipeline name). That
    // matches the pre-split order: the old loop iterated `config.pipelines`,
    // which `toml` (no `preserve_order` feature) already deserializes in sorted
    // key order — so both produce the same skip order. `resolved` is a BTreeMap
    // either way.
    for (name, outcome) in resolve_schedules_detailed(config, filter, member_budgets) {
        match outcome {
            Ok(rs) => {
                resolved.insert(name, rs);
            }
            Err(_) => skips.push(SkippedDemand {
                pipeline: name,
                source: None,
                reason: TickSkipReason::NotDue,
            }),
        }
    }
    (resolved, skips)
}

/// Resolve every scheduled pipeline in scope, **keeping the config error**.
///
/// The reconciler's own view of "what is scheduled" — including the V044 load
/// exclusion and the project-timezone/freshness-budget defaulting — so a status
/// consumer reports the set that can actually fire. Iterating
/// `config.pipelines` + `schedule()` directly does NOT: it includes load
/// pipelines, which the reconciler skips, so such a consumer would report a
/// pipeline as scheduled that will never run.
///
/// [`resolve_all_schedules`] is a thin lossy split over this, preserving the
/// tick path's existing behavior of collapsing every config error to a skip.
pub fn resolve_schedules_detailed(
    config: &RockyConfig,
    filter: Option<&str>,
    member_budgets: &BTreeMap<String, Vec<u64>>,
) -> BTreeMap<String, Result<ResolvedSchedule, ScheduleConfigError>> {
    let default_tz = config.schedule.timezone.as_str();
    let project_lag = config.freshness.expected_lag_seconds;
    let mut out = BTreeMap::new();
    for (name, pipeline) in &config.pipelines {
        if let Some(f) = filter
            && name.as_str() != f
        {
            continue;
        }
        // Load pipelines are not schedulable — a load re-ingests every discovered
        // file each run (not incremental), so a scheduled load would duplicate
        // data. `rocky validate` rejects this (V044); the reconciler skips it too
        // so an unvalidated config never fires one.
        if pipeline.as_load().is_some() {
            continue;
        }
        let Some(schedule_cfg) = pipeline.schedule() else {
            continue;
        };
        // Freshness budget = MIN over the pipeline's member models'
        // `max_lag_seconds` (loaded by the CLI/serve wrapper and passed in via
        // `member_budgets`), falling back to the project default when a pipeline
        // has no member budgets. An absent entry is the empty slice, which
        // `resolve_freshness_budget` maps to the project default — the exact
        // pre-wiring behavior, so non-freshness pipelines are unaffected.
        let members = member_budgets
            .get(name)
            .map_or(&[][..], |lags| lags.as_slice());
        out.insert(
            name.clone(),
            resolve_schedule(name, schedule_cfg, default_tz, members, project_lag),
        );
    }
    out
}

/// Order the scheduled pipelines so every `after` upstream precedes its
/// dependents. Only edges among *scheduled* pipelines matter — an unscheduled
/// `after` target runs externally and contributes run history, not tick order.
///
/// Kahn's algorithm with a lexicographic tiebreak for determinism. A cycle
/// leaves nodes unprocessed and yields [`TickError::Cycle`].
fn topological_order(
    schedules: &BTreeMap<String, ResolvedSchedule>,
) -> Result<Vec<String>, TickError> {
    let names: BTreeSet<String> = schedules.keys().cloned().collect();
    let mut indegree: BTreeMap<String, usize> = names.iter().map(|n| (n.clone(), 0)).collect();
    let mut adj: BTreeMap<String, Vec<String>> =
        names.iter().map(|n| (n.clone(), Vec::new())).collect();
    for (name, sched) in schedules {
        for up in &sched.after {
            if names.contains(up) {
                adj.get_mut(up)
                    .expect("adjacency seeded for every name")
                    .push(name.clone());
                *indegree
                    .get_mut(name)
                    .expect("indegree seeded for every name") += 1;
            }
        }
    }
    let mut ready: BTreeSet<String> = indegree
        .iter()
        .filter(|(_, d)| **d == 0)
        .map(|(n, _)| n.clone())
        .collect();
    let mut order: Vec<String> = Vec::with_capacity(names.len());
    while let Some(n) = ready.iter().next().cloned() {
        ready.remove(&n);
        for m in adj
            .get(&n)
            .expect("adjacency seeded for every name")
            .clone()
        {
            let d = indegree
                .get_mut(&m)
                .expect("indegree seeded for every name");
            *d -= 1;
            if *d == 0 {
                ready.insert(m);
            }
        }
        order.push(n);
    }
    if order.len() != names.len() {
        let done: BTreeSet<&String> = order.iter().collect();
        let remaining: Vec<String> = names
            .iter()
            .filter(|n| !done.contains(n))
            .cloned()
            .collect();
        return Err(TickError::Cycle(remaining.join(", ")));
    }
    Ok(order)
}

/// Map a per-source evaluation skip to a report entry. A `NotDue`/`Superseded`
/// skip is elided — it is not a suppression (it appears in `evaluated`).
fn map_skip(pipeline: &str, skip: &SourceSkip) -> Option<SkippedDemand> {
    let reason = match &skip.reason {
        SkipReason::NotDue | SkipReason::Superseded => return None,
        SkipReason::Disabled => TickSkipReason::Disabled,
        SkipReason::Paused => TickSkipReason::Paused,
        SkipReason::CatchupSkipped { missed } => TickSkipReason::CatchupSkipped { missed: *missed },
        SkipReason::FailureBackoff { resume_at } => TickSkipReason::FailureBackoff {
            resume_at: *resume_at,
        },
        SkipReason::PartialBackoff { resume_at } => TickSkipReason::PartialBackoff {
            resume_at: *resume_at,
        },
        // Recorded, never elided — a read fault must be visible in the report.
        SkipReason::HistoryError => TickSkipReason::HistoryUnavailable,
    };
    Some(SkippedDemand {
        pipeline: pipeline.to_string(),
        source: Some(skip.source),
        reason,
    })
}

/// The claim storage key for a demand: `<pipeline>\u{1f}<source>\u{1f}<disc>`.
///
/// The discriminator is the demand's `claim_discriminator` when set (a webhook's
/// minted `demand_uid`) or its `logical_ts` otherwise (the cron occurrence /
/// standing reference instant). [`parse_claim_key`](crate::schedule::parse_claim_key)
/// accepts either form.
fn claim_key(demand: &Demand) -> String {
    let disc = demand
        .claim_discriminator
        .clone()
        .unwrap_or_else(|| demand.logical_ts.to_rfc3339());
    format!(
        "{}\u{1f}{}\u{1f}{}",
        demand.pipeline,
        demand.source.as_str(),
        disc
    )
}

fn new_submission_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// The cursor mutation that rides with a first submission's claim CAS: a cron
/// submission advances the catch-up anchor; a standing submission records only
/// the attempt time.
fn submit_mutation(demand: &Demand, run_id: &str, now: DateTime<Utc>) -> ScheduleStateMutation {
    match demand.source {
        DemandKind::Cron => ScheduleStateMutation::SubmitCron {
            logical_ts: demand.logical_ts,
            attempt_at: now,
            run_id: run_id.to_string(),
        },
        DemandKind::After | DemandKind::Freshness | DemandKind::Webhook => {
            ScheduleStateMutation::SubmitStanding {
                attempt_at: now,
                run_id: run_id.to_string(),
            }
        }
    }
}

fn skip(demand: &Demand, reason: TickSkipReason) -> SkippedDemand {
    SkippedDemand {
        pipeline: demand.pipeline.clone(),
        source: Some(demand.source),
        reason,
    }
}

fn to_std_timeout(d: Duration) -> std::time::Duration {
    d.to_std().unwrap_or(std::time::Duration::ZERO)
}

/// The outcome of executing (or declining to execute) one due demand.
enum ExecOutcome {
    Executed(ExecutedDemand),
    Skipped(SkippedDemand),
    /// The store could not be re-opened after the child ran. The claim stays
    /// `submitted{id_n}` and the child's run record exists, so the next tick's
    /// stuck-claim resolution record-joins it and applies the outcome. The
    /// caller must end the pass — it no longer holds a store.
    StoreBusy(SkippedDemand),
}

/// Execute one due demand through the claim state machine: claim an absent or
/// re-claimable key, resolve a stuck one, or skip a terminal one.
#[allow(clippy::too_many_arguments)]
async fn execute_demand(
    demand: &Demand,
    schedule: &ResolvedSchedule,
    phase: &mut PhaseStore,
    now: DateTime<Utc>,
    spawner: &dyn Spawner,
    opts: &TickOptions,
    lock: Option<&TickLock>,
) -> Result<ExecOutcome, TickError> {
    let key = claim_key(demand);
    let mut iterations = 0;
    loop {
        iterations += 1;
        if iterations > MAX_DEMAND_ITERATIONS {
            return Err(TickError::LoopBound(key.clone()));
        }
        let observed = phase.store().get_schedule_claim(&key)?;
        let fresh_id = new_submission_id();
        match decide_pre_spawn(observed.as_ref(), fresh_id.clone(), now) {
            PreSpawn::SkipCompleted => {
                return Ok(ExecOutcome::Skipped(skip(demand, TickSkipReason::Dedup)));
            }
            PreSpawn::ResolveStuck => {
                let current = observed.expect("ResolveStuck only arises from a stored claim");
                // Pre-spawn only — no child runs here, so the store stays open.
                match resolve_stuck(
                    &key,
                    demand,
                    schedule,
                    &current,
                    phase.store(),
                    &opts.rocky_dir,
                    now,
                )? {
                    // Released and re-claimable this tick — loop to the pre-spawn
                    // decision, which re-claims (continuing the retry budget).
                    StuckResolution::ReClaimable => continue,
                    StuckResolution::Finished => {
                        return Ok(ExecOutcome::Skipped(skip(demand, TickSkipReason::Dedup)));
                    }
                    StuckResolution::InFlight => {
                        return Ok(ExecOutcome::Skipped(skip(demand, TickSkipReason::InFlight)));
                    }
                }
            }
            PreSpawn::Claim(new_claim) => {
                let mutation = submit_mutation(demand, &fresh_id, now);
                match phase.store().schedule_claim_cas(
                    &key,
                    observed.as_ref(),
                    &new_claim,
                    &demand.pipeline,
                    &mutation,
                )? {
                    // Lost the claim race — stand down for this demand this pass.
                    ClaimCas::Lost(_) => {
                        return Ok(ExecOutcome::Skipped(skip(demand, TickSkipReason::InFlight)));
                    }
                    ClaimCas::Won => {
                        return execute_claimed(
                            &key, demand, schedule, new_claim, fresh_id, phase, now, spawner, opts,
                            lock,
                        )
                        .await;
                    }
                }
            }
        }
    }
}

/// After winning a claim, spawn the child and apply the post-attempt transition:
/// an in-tick retry while budget remains, or a terminal transition on
/// exhaustion. Never sleeps between attempts.
#[allow(clippy::too_many_arguments)]
async fn execute_claimed(
    key: &str,
    demand: &Demand,
    schedule: &ResolvedSchedule,
    mut claim: ClaimRecord,
    mut submission_id: String,
    phase: &mut PhaseStore,
    now: DateTime<Utc>,
    spawner: &dyn Spawner,
    opts: &TickOptions,
    lock: Option<&TickLock>,
) -> Result<ExecOutcome, TickError> {
    let mut iterations = 0;
    loop {
        iterations += 1;
        if iterations > MAX_DEMAND_ITERATIONS {
            return Err(TickError::LoopBound(key.to_string()));
        }
        // Refresh the lease before spawning.
        if let Some(held) = lock {
            let _ = held.heartbeat();
        }
        let request = SpawnRequest {
            pipeline: demand.pipeline.clone(),
            config_path: opts.config_path.clone(),
            state_path: opts.state_path.clone(),
            submission_id: submission_id.clone(),
            trace_context: opts.trace_context.clone(),
            timeout: schedule.timeout.map(to_std_timeout),
            trigger: RunTriggerKind::for_source(demand.source),
        };
        // Release the store — both locks — for the child's whole window: the
        // child `rocky run` opens the SAME state file, and the redb + advisory
        // locks are exclusive per machine. The claim CAS that got us here has
        // already committed, so nothing is lost by dropping the handle; closing
        // BEFORE the spawn also guarantees no lock fd reaches the child.
        phase.close();
        // Keep the lease fresh *while* the child runs — the heartbeat mtime is
        // the liveness signal a peer reads to tell a live holder from a wedged
        // one, so a long run must refresh it under 60s or a later tick would
        // wedge-override and double-fire. The heartbeat touches the tick lock,
        // not the (now-closed) store. The timer is a side-effect only; the
        // reconciler's logic clock is still the injected `now`.
        let run_fut = spawner.run(&request);
        tokio::pin!(run_fut);
        let run_outcome = loop {
            tokio::select! {
                outcome = &mut run_fut => break outcome,
                _ = tokio::time::sleep(HEARTBEAT_INTERVAL) => {
                    if let Some(held) = lock {
                        let _ = held.heartbeat();
                    }
                }
            }
        };
        // Take the store back to observe and decide. Contention here means a
        // third `rocky` process slipped in between the child's release and our
        // reopen: defer — the claim stays `submitted{submission_id}` and the
        // child's run record lets the next tick's stuck-claim resolution finish.
        if matches!(phase.reopen().await?, StoreReopen::Busy) {
            return Ok(ExecOutcome::StoreBusy(skip(
                demand,
                TickSkipReason::StateBusy,
            )));
        }
        let outcome = TerminalOutcome::from_exit_code(run_outcome.exit_code);
        let next_id = new_submission_id();
        // On shutdown, do not start another attempt. A drain raised during this
        // attempt means the child was likely SIGTERM'd by the spawner (→ nonzero
        // exit → `Failure`), and retrying would CAS a new submission and respawn a
        // fresh child *after* shutdown was requested — once per remaining retry,
        // each granted its own drain grace, so the drain would no longer be bounded
        // by a single `drain_timeout`. Forcing `retry_max = 0` here routes a
        // would-be retry through the existing, separately-tested Terminal path
        // (`budget_remains(cycle_attempts ≥ 1, 0)` is false), so this attempt
        // finalizes exactly once and nothing respawns. Consequence (failure-mode
        // ledger): a drain-interrupted run finalizes as failed and does not
        // auto-resume on restart; shutdown stays bounded to one in-flight child.
        let effective_retry_max = if opts.drain.is_signalled() {
            0
        } else {
            schedule.retry_max
        };
        match decide_post_attempt(
            &claim,
            outcome,
            demand.source,
            effective_retry_max,
            next_id.clone(),
            now,
        ) {
            PostAttempt::Retry(retry_claim) => {
                // A fresh submission — record its attempt time; the anchor (for
                // cron) already advanced on the first submission this cycle.
                let mutation = ScheduleStateMutation::SubmitStanding {
                    attempt_at: now,
                    run_id: next_id.clone(),
                };
                match phase.store().schedule_claim_cas(
                    key,
                    Some(&claim),
                    &retry_claim,
                    &demand.pipeline,
                    &mutation,
                )? {
                    ClaimCas::Lost(_) => {
                        return Ok(ExecOutcome::Skipped(skip(demand, TickSkipReason::InFlight)));
                    }
                    ClaimCas::Won => {
                        claim = retry_claim;
                        submission_id = next_id;
                        // Loop top closes the store again before the respawn.
                        continue;
                    }
                }
            }
            PostAttempt::Terminal {
                claim: terminal_claim,
                bookkeeping,
            } => {
                // Two different counters, deliberately: the tick's public
                // `ExecutedDemand.attempts` is the FROZEN contract's total-
                // submissions audit counter; the incident bundle reports the
                // attempts consumed by THIS demand cycle.
                let attempts = terminal_claim.attempts;
                let bundle_attempts = terminal_claim.cycle_attempts;
                let mutation = ScheduleStateMutation::Terminal {
                    outcome: bookkeeping.outcome,
                    cf_delta: bookkeeping.cf_delta,
                };
                match phase.store().schedule_claim_cas(
                    key,
                    Some(&claim),
                    &terminal_claim,
                    &demand.pipeline,
                    &mutation,
                )? {
                    ClaimCas::Lost(_) => {
                        return Ok(ExecOutcome::Skipped(skip(demand, TickSkipReason::InFlight)));
                    }
                    ClaimCas::Won => {
                        // Incident bundle for a Failure OR Partial (partial
                        // trips partial backoff exactly like failure trips
                        // failure backoff) — unless the SPAWNER ended the
                        // attempt for a shutdown drain. The discriminator is
                        // the spawner's own record of what it did, not a
                        // re-sample of the drain flag: a child that failed on
                        // its own and merely coincided with a drain still
                        // records its incident.
                        if !run_outcome.drain_interrupted {
                            // Post-CAS read of the cursor the mutation just
                            // updated; a read error yields `None`, never a
                            // fabricated zero.
                            let consecutive_failures = phase
                                .store()
                                .get_schedule_state(&demand.pipeline)
                                .ok()
                                .flatten()
                                .map(|c| c.consecutive_failures);
                            record_incident(
                                &opts.rocky_dir,
                                &demand.pipeline,
                                demand.source,
                                outcome,
                                Some(demand.logical_ts),
                                &submission_id,
                                Some(run_outcome.exit_code),
                                bundle_attempts,
                                consecutive_failures,
                                now,
                            );
                        }
                        // Return with the store OPEN — it feeds the next
                        // pipeline's evaluation (the same-tick cascade contract).
                        return Ok(ExecOutcome::Executed(ExecutedDemand {
                            pipeline: demand.pipeline.clone(),
                            source: demand.source,
                            logical_ts: demand.logical_ts,
                            submission_id,
                            exit_code: run_outcome.exit_code,
                            outcome,
                            attempts,
                        }));
                    }
                }
            }
        }
    }
}

/// Emit an incident bundle for a scheduler claim that finalized as `Failure`
/// or `Partial` — the shared seam behind the direct child-completion arm and
/// every recovery path that terminalizes a claim (stuck resolver,
/// missing-record grace, orphan sweep). An incident's visibility must not
/// depend on WHICH seam happened to observe the failure.
///
/// `Success` is a no-op. Facts a seam cannot know arrive as `None` and stay
/// `None` — never a fabricated zero. Write failures warn and never fail the
/// tick: the outcome is already committed, and diagnostics that break the
/// thing they diagnose are worse than none.
#[allow(clippy::too_many_arguments)]
fn record_incident(
    rocky_dir: &Path,
    pipeline: &str,
    source: crate::schedule::claim::DemandKind,
    outcome: TerminalOutcome,
    logical_ts: Option<DateTime<Utc>>,
    submission_id: &str,
    exit_code: Option<i32>,
    attempts: u32,
    consecutive_failures: Option<u32>,
    now: DateTime<Utc>,
) {
    let outcome_str = match outcome {
        TerminalOutcome::Failure => "failure",
        TerminalOutcome::Partial => "partial",
        TerminalOutcome::Success => return,
    };
    let bundle = crate::schedule::incidents::IncidentBundle {
        incident_version: 1,
        recorded_at: now,
        pipeline: pipeline.to_string(),
        source: source.as_str().to_string(),
        outcome: outcome_str.to_string(),
        logical_ts,
        submission_id: submission_id.to_string(),
        exit_code,
        attempts,
        consecutive_failures,
        pointers: vec![
            format!("rocky history --output json  # run {submission_id}"),
            format!("GET /api/v1/jobs/{submission_id}  # job state (resident scheduler only)"),
            "GET /api/v1/schedule  # cursors + claims (resident scheduler only)".to_string(),
        ],
    };
    match crate::schedule::incidents::write_incident(rocky_dir, &bundle) {
        Ok(path) => tracing::warn!(
            pipeline = %pipeline,
            outcome = %outcome_str,
            incident = %path.display(),
            "scheduled run failed; incident bundle written"
        ),
        Err(e) => tracing::warn!(
            pipeline = %pipeline,
            outcome = %outcome_str,
            error = %e,
            "scheduled run failed AND the incident bundle could not be written"
        ),
    }
}

/// Resolve orphaned `submitted` claims independent of whether their demand is
/// due again.
///
/// A claim left `submitted` after its own tick — a reopen-after-child contention
/// ([`ExecOutcome::StoreBusy`]) or a crash after the child wrote its run record —
/// has a run record but no terminal transition. The per-pipeline loop only
/// resolves a stuck claim when it re-evaluates that key, but a cron occurrence
/// advances its anchor on submission (so its key never recurs) and a satisfied
/// `after`/`freshness` demand goes not-due; either way the loop never revisits
/// the orphan, so its `submitted` claim would leak (until the 7-day GC) and, for
/// a standing demand, keep the key un-re-claimable. The sweep record-joins each
/// such claim and drives its CLAIM to a terminal state.
///
/// **Claim-lifecycle only — no per-pipeline scalars.** The sweep deliberately
/// applies `ScheduleStateMutation::None`: it never touches `consecutive_failures`
/// or `last_attempt_outcome`. Those are per-pipeline, and the authoritative
/// updater is the demand-loop path, which processes the *current* demand. A
/// pipeline may combine sources, so a newer cron success can land this tick while
/// an older standing-demand orphan is still `submitted`; applying the orphan's
/// (older) outcome to the shared scalars would corrupt them out of order —
/// overwriting the newer result and its retry cycle. Resolving only the claim is
/// enough: the orphaned occurrence never re-runs, and a still-standing demand
/// re-claims a fresh cycle on its next due tick. (Consequence, documented: an
/// orphaned run's `consecutive_failures` delta is not counted — its outcome is
/// still visible in `rocky history`; a still-due standing failure is instead
/// resolved by the demand loop's own [`resolve_stuck`], which does apply it.)
///
/// Record-present only: a `submitted` claim with NO run record is a genuine
/// crash before the child recorded anything; it is left to the demand-path grace
/// ([`resolve_missing_record`]) plus the 7-day claim GC, never force-terminated
/// here. Restricting to record-present claims is also what makes the sweep
/// double-resolve-safe — a claim already resolved is `released`/`completed`, not
/// `submitted`.
fn sweep_orphan_claims(
    schedules: &BTreeMap<String, ResolvedSchedule>,
    store: &StateStore,
    rocky_dir: &Path,
    now: DateTime<Utc>,
) -> Result<(), TickError> {
    for (key, claim) in store.list_schedule_claims()? {
        if !matches!(claim.state, ClaimState::Submitted) {
            continue;
        }
        // The discriminator is deliberately ignored here: it is a timestamp for
        // cron/after/freshness but a `demand_uid` for a webhook, and the sweep
        // needs neither.
        let Some((pipeline, source, _)) = parse_claim_key(&key) else {
            continue;
        };
        // Only sweep claims for pipelines in this tick's scope (honors
        // `--pipeline`).
        if !schedules.contains_key(&pipeline) {
            continue;
        }
        let Some(outcome) = store
            .find_terminal_run_by_submission_id(&claim.submission_id)?
            .and_then(|run| run.status.terminal_outcome())
        else {
            continue;
        };
        // Compute the terminal claim DIRECTLY (cron/webhook → completed; standing
        // success → completed, standing failure/partial → released), bypassing the
        // resolver's retry-budget logic: an orphan has already run to a terminal
        // outcome, so it is unconditionally terminal, and a serde-defaulted
        // `cycle_attempts = 0` must not divert it to `ReleasedBudgetOpen` and leak
        // the claim. CAS it with a `None` cursor mutation: the sweep never touches
        // the per-pipeline scalars (see the note above). A lost CAS is a no-op
        // (nothing else mutates claims while this tick holds the lock).
        let terminal = sweep_terminal_claim(&claim, source, outcome, now);
        let cas = store.schedule_claim_cas(
            &key,
            Some(&claim),
            &terminal,
            &pipeline,
            &ScheduleStateMutation::None,
        )?;
        // An orphaned failure is still a failure — without a bundle here it
        // would be invisible everywhere but `rocky history`. The sweep never
        // touches the per-pipeline scalars, so `consecutive_failures` is
        // honestly `None`, and the occurrence is not reconstructed
        // (`logical_ts: None`).
        if matches!(cas, ClaimCas::Won) && !matches!(outcome, TerminalOutcome::Success) {
            record_incident(
                rocky_dir,
                &pipeline,
                source,
                outcome,
                None,
                &claim.submission_id,
                None,
                claim.cycle_attempts,
                None,
                now,
            );
        }
    }
    Ok(())
}

/// How a stuck `submitted` claim was resolved.
enum StuckResolution {
    /// Released with the budget continuing — re-claimable this tick.
    ReClaimable,
    /// The run was observed terminal and finalized — do not spawn.
    Finished,
    /// The outcome is not yet knowable (a live concurrent run, or within the
    /// crash grace) — skip without spawning.
    InFlight,
}

/// Resolve a claim left `submitted` by a crashed owner. Budget-aware: the
/// resolver recomputes from the run record (joined by `submission_id`), or, when
/// no record exists, applies a bounded grace before treating the attempt as
/// lost — so configured retries survive a crash and a standing demand never
/// stalls indefinitely.
fn resolve_stuck(
    key: &str,
    demand: &Demand,
    schedule: &ResolvedSchedule,
    current: &ClaimRecord,
    store: &StateStore,
    rocky_dir: &Path,
    now: DateTime<Utc>,
) -> Result<StuckResolution, TickError> {
    let outcome = store
        .find_terminal_run_by_submission_id(&current.submission_id)?
        .and_then(|run| run.status.terminal_outcome());
    let Some(outcome) = outcome else {
        return resolve_missing_record(key, demand, schedule, current, store, rocky_dir, now);
    };
    let resolved = decide_resolver(current, outcome, demand.source, schedule.retry_max, now);
    let resolution = commit_resolution(key, &demand.pipeline, current, resolved, store)?;
    // The claim just finalized a failure this tick's spawn never saw (a prior
    // owner crashed after the child recorded its run). Same incident contract
    // as the direct arm; the child's exit code is unknowable here — the run
    // record carries the outcome, not the code.
    if matches!(resolution, StuckResolution::Finished)
        && !matches!(outcome, TerminalOutcome::Success)
    {
        let consecutive_failures = store
            .get_schedule_state(&demand.pipeline)
            .ok()
            .flatten()
            .map(|c| c.consecutive_failures);
        record_incident(
            rocky_dir,
            &demand.pipeline,
            demand.source,
            outcome,
            Some(demand.logical_ts),
            &current.submission_id,
            None,
            current.cycle_attempts,
            consecutive_failures,
            now,
        );
    }
    Ok(resolution)
}

/// Resolve a stuck claim with no run record: stamp the grace on first sight,
/// then — once the grace elapses with still no record — treat the attempt as a
/// lost failure under the same budget rule as an observed failure.
fn resolve_missing_record(
    key: &str,
    demand: &Demand,
    schedule: &ResolvedSchedule,
    current: &ClaimRecord,
    store: &StateStore,
    rocky_dir: &Path,
    now: DateTime<Utc>,
) -> Result<StuckResolution, TickError> {
    let swept = current
        .first_swept_at
        .as_deref()
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&Utc));
    match swept {
        None => {
            let mut updated = current.clone();
            updated.first_swept_at = Some(now.to_rfc3339());
            // Either outcome of this CAS means "skip this tick": on win the grace
            // has begun; on loss another reconciler owns the resolution.
            store.schedule_claim_cas(
                key,
                Some(current),
                &updated,
                &demand.pipeline,
                &ScheduleStateMutation::None,
            )?;
            Ok(StuckResolution::InFlight)
        }
        Some(swept_at) if now - swept_at >= RECOVERY_GRACE => {
            let resolved = decide_resolver(
                current,
                TerminalOutcome::Failure,
                demand.source,
                schedule.retry_max,
                now,
            );
            let resolution = commit_resolution(key, &demand.pipeline, current, resolved, store)?;
            // A lost attempt (crash before any run record) that just
            // exhausted its budget is a failure nobody else will surface.
            if matches!(resolution, StuckResolution::Finished) {
                let consecutive_failures = store
                    .get_schedule_state(&demand.pipeline)
                    .ok()
                    .flatten()
                    .map(|c| c.consecutive_failures);
                record_incident(
                    rocky_dir,
                    &demand.pipeline,
                    demand.source,
                    TerminalOutcome::Failure,
                    Some(demand.logical_ts),
                    &current.submission_id,
                    None,
                    current.cycle_attempts,
                    consecutive_failures,
                    now,
                );
            }
            Ok(resolution)
        }
        Some(_) => Ok(StuckResolution::InFlight),
    }
}

/// Commit a resolver decision via a claim CAS, mapping the outcome to a
/// [`StuckResolution`]. A lost CAS means another reconciler moved the claim —
/// skip.
fn commit_resolution(
    key: &str,
    pipeline: &str,
    expected: &ClaimRecord,
    resolved: Resolved,
    store: &StateStore,
) -> Result<StuckResolution, TickError> {
    match resolved {
        Resolved::ReleasedBudgetOpen(released) => {
            match store.schedule_claim_cas(
                key,
                Some(expected),
                &released,
                pipeline,
                &ScheduleStateMutation::None,
            )? {
                ClaimCas::Won => Ok(StuckResolution::ReClaimable),
                ClaimCas::Lost(_) => Ok(StuckResolution::InFlight),
            }
        }
        Resolved::Terminal { claim, bookkeeping } => {
            let mutation = ScheduleStateMutation::Terminal {
                outcome: bookkeeping.outcome,
                cf_delta: bookkeeping.cf_delta,
            };
            match store.schedule_claim_cas(key, Some(expected), &claim, pipeline, &mutation)? {
                ClaimCas::Won => Ok(StuckResolution::Finished),
                ClaimCas::Lost(_) => Ok(StuckResolution::InFlight),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schedule::claim::ClaimState;
    use crate::schedule::lock::{TickAcquire, TickLock};
    use crate::schedule::spawn::{CapturingSpawner, RunOutcome, SpawnRequest};
    use crate::state::{RunRecord, RunStatus, RunTrigger, SessionSource, StateStore};
    use async_trait::async_trait;
    use chrono::TimeZone;
    use std::sync::Mutex;

    // --- fixtures ------------------------------------------------------------

    /// `raw` runs externally (no schedule), `staging` runs after it.
    const AFTER_ONLY: &str = r#"
[adapter.db]
type = "duckdb"

[pipeline.raw]
type = "transformation"
[pipeline.raw.target]
adapter = "db"

[pipeline.staging]
type = "transformation"
[pipeline.staging.target]
adapter = "db"
[pipeline.staging.schedule]
after = ["raw"]
"#;

    /// `staging` runs after `raw`, with an in-tick retry budget of two.
    const AFTER_RETRY2: &str = r#"
[adapter.db]
type = "duckdb"

[pipeline.raw]
type = "transformation"
[pipeline.raw.target]
adapter = "db"

[pipeline.staging]
type = "transformation"
[pipeline.staging.target]
adapter = "db"
[pipeline.staging.schedule]
after = ["raw"]
retry = { max = 2 }
"#;

    /// A single cron-scheduled pipeline.
    const CRON_ONLY: &str = r#"
[adapter.db]
type = "duckdb"

[pipeline.raw]
type = "transformation"
[pipeline.raw.target]
adapter = "db"
[pipeline.raw.schedule]
cron = "0 3 * * *"
"#;

    /// A cron-scheduled pipeline with an in-tick retry budget of two.
    const CRON_RETRY2: &str = r#"
[adapter.db]
type = "duckdb"

[pipeline.raw]
type = "transformation"
[pipeline.raw.target]
adapter = "db"
[pipeline.raw.schedule]
cron = "0 3 * * *"
retry = { max = 2 }
"#;

    /// `raw` cron, `staging` after `raw` — the same-tick cascade.
    const CRON_AND_AFTER: &str = r#"
[adapter.db]
type = "duckdb"

[pipeline.raw]
type = "transformation"
[pipeline.raw.target]
adapter = "db"
[pipeline.raw.schedule]
cron = "0 3 * * *"

[pipeline.staging]
type = "transformation"
[pipeline.staging.target]
adapter = "db"
[pipeline.staging.schedule]
after = ["raw"]
"#;

    fn cfg(toml_str: &str) -> RockyConfig {
        toml::from_str(toml_str).expect("valid rocky.toml")
    }

    /// The split of `resolve_all_schedules` into `resolve_schedules_detailed`
    /// must not change the `skipped` Vec order (it flows verbatim into
    /// `TickOutput.skipped`). The order is sorted by pipeline name — the old
    /// loop iterated `config.pipelines`, which `toml` (no `preserve_order`)
    /// deserializes in sorted key order, and the new BTreeMap iteration is
    /// sorted too. Declaring `zzz` before `aaa` proves the input order does not
    /// leak through: both land under the sorted `[aaa, zzz]`.
    #[test]
    fn resolve_all_schedules_skips_are_sorted_not_declaration_order() {
        let config = cfg(r#"
[adapter.db]
type = "duckdb"

[pipeline.zzz]
type = "transformation"
[pipeline.zzz.target]
adapter = "db"
[pipeline.zzz.schedule]
cron = "not a cron"

[pipeline.aaa]
type = "transformation"
[pipeline.aaa.target]
adapter = "db"
[pipeline.aaa.schedule]
cron = "also invalid"
"#);
        let (resolved, skips) = resolve_all_schedules(&config, None, &BTreeMap::new());
        assert!(
            resolved.is_empty(),
            "both crons are invalid → nothing resolves"
        );
        let order: Vec<&str> = skips.iter().map(|s| s.pipeline.as_str()).collect();
        assert_eq!(
            order,
            vec!["aaa", "zzz"],
            "skip order is sorted by pipeline name"
        );
    }

    fn at(y: i32, mo: u32, d: u32, h: u32, mi: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, mo, d, h, mi, 0).unwrap()
    }

    fn temp_env() -> (std::path::PathBuf, tempfile::TempDir, TickOptions) {
        let dir = tempfile::tempdir().unwrap();
        let state_path = dir.path().join("state.redb");
        let opts = TickOptions {
            dry_run: false,
            pipeline_filter: None,
            config_path: dir.path().join("rocky.toml"),
            rocky_dir: dir.path().join(".rocky"),
            trace_context: None,
            member_budgets: BTreeMap::new(),
            state_path: state_path.clone(),
            drain: Drain::default(),
        };
        (state_path, dir, opts)
    }

    /// Open the state store at `state_path`, run `f`, then drop the handle.
    ///
    /// Under the new store lifecycle `tick_once` opens the store ITSELF from
    /// `state_path` (releasing it around each child spawn and reopening after), so
    /// no test may hold a live handle across a tick — the exclusive advisory lock
    /// would make the tick busy-skip. Every seed and every post-tick inspection is
    /// therefore scoped to an open-and-drop through this helper.
    fn with_store<T>(state_path: &std::path::Path, f: impl FnOnce(&StateStore) -> T) -> T {
        let store = StateStore::open(state_path).unwrap();
        f(&store)
    }

    fn make_run(
        pipeline: &str,
        submission_id: Option<&str>,
        status: RunStatus,
        started: DateTime<Utc>,
        finished: DateTime<Utc>,
    ) -> RunRecord {
        RunRecord {
            run_id: format!("run-{}", uuid::Uuid::new_v4()),
            started_at: started,
            finished_at: finished,
            status,
            models_executed: Vec::new(),
            trigger: RunTrigger::Schedule,
            config_hash: "hash".to_string(),
            triggering_identity: None,
            session_source: SessionSource::default(),
            git_commit: None,
            git_branch: None,
            idempotency_key: None,
            target_catalog: None,
            hostname: "test".to_string(),
            rocky_version: "test".to_string(),
            check_outcomes: Vec::new(),
            pipeline: Some(pipeline.to_string()),
            submission_id: submission_id.map(String::from),
        }
    }

    fn seed_run(
        store: &StateStore,
        pipeline: &str,
        submission_id: Option<&str>,
        status: RunStatus,
        started: DateTime<Utc>,
        finished: DateTime<Utc>,
    ) {
        store
            .record_run(&make_run(
                pipeline,
                submission_id,
                status,
                started,
                finished,
            ))
            .unwrap();
    }

    /// Insert a fresh `submitted` claim for a coordinate demand, as a crashed
    /// owner would have left it.
    fn seed_submitted_claim(
        store: &StateStore,
        pipeline: &str,
        source: DemandKind,
        logical_ts: DateTime<Utc>,
        submission_id: &str,
        transitioned_at: DateTime<Utc>,
    ) -> String {
        let key = claim_key(&Demand {
            pipeline: pipeline.to_string(),
            source,
            logical_ts,
            claim_discriminator: None,
        });
        let claim = ClaimRecord::new_submitted(submission_id.to_string(), transitioned_at);
        let cas = store
            .schedule_claim_cas(&key, None, &claim, pipeline, &ScheduleStateMutation::None)
            .unwrap();
        assert!(matches!(cas, ClaimCas::Won), "seed claim insert must win");
        key
    }

    /// A spawner that does exactly what a real child does: it opens the state
    /// store at `state_path` ITSELF and writes its run record through its own
    /// handle. That open is the regression assertion — before the store-lifecycle
    /// fix the parent held the store across the spawn, so the child's open failed
    /// with `LockHeldByOther`. Recording each request also lets a same-tick
    /// `after` downstream observe an upstream that ran earlier in the tick.
    struct OpeningSpawner {
        state_path: std::path::PathBuf,
        captured: Mutex<Vec<SpawnRequest>>,
        exit_code: i32,
        finished_at: DateTime<Utc>,
    }

    #[async_trait]
    impl Spawner for OpeningSpawner {
        async fn run(&self, request: &SpawnRequest) -> RunOutcome {
            self.captured.lock().unwrap().push(request.clone());
            let store = StateStore::open(&self.state_path)
                .expect("child window: the parent must have released the state store");
            let status = match self.exit_code {
                0 => RunStatus::Success,
                2 => RunStatus::PartialFailure,
                _ => RunStatus::Failure,
            };
            seed_run(
                &store,
                &request.pipeline,
                Some(&request.submission_id),
                status,
                self.finished_at,
                self.finished_at,
            );
            RunOutcome {
                exit_code: self.exit_code,
                pid: Some(4242),
                drain_interrupted: false,
            }
        }
    }

    fn rt() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    // --- integration: seeded upstream success drives the downstream ----------

    #[test]
    fn after_upstream_success_runs_downstream() {
        let (state_path, _dir, opts) = temp_env();
        let config = cfg(AFTER_ONLY);
        // A seeded fake upstream success (as `rocky run --pipeline raw` would).
        with_store(&state_path, |store| {
            seed_run(
                store,
                "raw",
                None,
                RunStatus::Success,
                at(2026, 5, 2, 3, 5),
                at(2026, 5, 2, 3, 10),
            );
        });
        let spawner = CapturingSpawner::new(0);
        let report = rt()
            .block_on(tick_once(
                &config,
                &state_path,
                at(2026, 5, 2, 4, 0),
                &spawner,
                &opts,
            ))
            .unwrap();

        assert_eq!(spawner.run_count(), 1, "staging runs via after");
        let runs = spawner.runs_for("staging");
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].request.pipeline, "staging");
        assert!(!runs[0].request.submission_id.is_empty());
        assert_eq!(report.executed.len(), 1);
        assert_eq!(report.executed[0].pipeline, "staging");
        assert_eq!(report.executed[0].source, DemandKind::After);
        assert_eq!(report.executed[0].outcome, TerminalOutcome::Success);

        // Re-tick at the same instant: the after claim is completed, so nothing
        // re-fires (idempotent).
        let report2 = rt()
            .block_on(tick_once(
                &config,
                &state_path,
                at(2026, 5, 2, 4, 0),
                &spawner,
                &opts,
            ))
            .unwrap();
        assert_eq!(
            spawner.run_count(),
            1,
            "completed after demand never re-fires"
        );
        assert!(report2.executed.is_empty());
    }

    // --- cron first-sight then fire ------------------------------------------

    #[test]
    fn cron_first_sight_then_fires_next_occurrence() {
        let (state_path, _dir, opts) = temp_env();
        let config = cfg(CRON_ONLY);
        let spawner = CapturingSpawner::new(0);

        // First sight anchors at `now`, fires nothing.
        rt().block_on(tick_once(
            &config,
            &state_path,
            at(2026, 5, 1, 12, 0),
            &spawner,
            &opts,
        ))
        .unwrap();
        assert_eq!(spawner.run_count(), 0, "first sight does not fire");
        with_store(&state_path, |store| {
            assert!(
                store
                    .get_schedule_state("raw")
                    .unwrap()
                    .unwrap()
                    .last_fire_logical_ts
                    .is_some(),
                "anchor recorded"
            );
        });

        // Next day past 03:00 → the occurrence fires exactly once.
        let report = rt()
            .block_on(tick_once(
                &config,
                &state_path,
                at(2026, 5, 2, 4, 0),
                &spawner,
                &opts,
            ))
            .unwrap();
        assert_eq!(spawner.run_count(), 1);
        assert_eq!(report.executed[0].source, DemandKind::Cron);
        assert_eq!(report.executed[0].logical_ts, at(2026, 5, 2, 3, 0));
        with_store(&state_path, |store| {
            assert_eq!(
                store
                    .get_schedule_state("raw")
                    .unwrap()
                    .unwrap()
                    .last_fire_logical_ts
                    .as_deref(),
                Some(at(2026, 5, 2, 3, 0).to_rfc3339()).as_deref(),
                "anchor advanced to the fired occurrence"
            );
        });

        // Same instant again → dedup, no re-fire.
        rt().block_on(tick_once(
            &config,
            &state_path,
            at(2026, 5, 2, 4, 0),
            &spawner,
            &opts,
        ))
        .unwrap();
        assert_eq!(spawner.run_count(), 1, "dedup: occurrence never re-fires");
    }

    // --- same-tick after cascade (topo order + re-evaluation) ----------------

    #[test]
    fn same_tick_after_cascade() {
        let (state_path, _dir, opts) = temp_env();
        let config = cfg(CRON_AND_AFTER);
        // Anchor raw's cron via a first sight.
        let cap = CapturingSpawner::new(0);
        rt().block_on(tick_once(
            &config,
            &state_path,
            at(2026, 5, 1, 12, 0),
            &cap,
            &opts,
        ))
        .unwrap();

        // A spawner that opens the store itself and writes a success for each
        // child, so staging can observe raw within the same tick.
        let rec = OpeningSpawner {
            state_path: state_path.clone(),
            captured: Mutex::new(Vec::new()),
            exit_code: 0,
            finished_at: at(2026, 5, 2, 3, 5),
        };
        rt().block_on(tick_once(
            &config,
            &state_path,
            at(2026, 5, 2, 4, 0),
            &rec,
            &opts,
        ))
        .unwrap();

        let caps = rec.captured.lock().unwrap();
        assert_eq!(
            caps.len(),
            2,
            "raw fires (cron), then staging fires (after)"
        );
        assert_eq!(caps[0].pipeline, "raw", "topo order: upstream first");
        assert_eq!(caps[1].pipeline, "staging");
    }

    // --- regression ii: a live submitted claim blocks a second reconciler ----

    #[test]
    fn preexisting_submitted_claim_blocks_second_reconciler() {
        let (state_path, _dir, opts) = temp_env();
        let config = cfg(AFTER_ONLY);
        with_store(&state_path, |store| {
            seed_run(
                store,
                "raw",
                None,
                RunStatus::Success,
                at(2026, 5, 2, 3, 5),
                at(2026, 5, 2, 3, 10),
            );
            // Reconciler A already claimed the after demand (no run record yet).
            seed_submitted_claim(
                store,
                "staging",
                DemandKind::After,
                at(2026, 5, 2, 3, 10),
                "owned-by-a",
                at(2026, 5, 2, 3, 50),
            );
        });
        let spawner = CapturingSpawner::new(0);
        let report = rt()
            .block_on(tick_once(
                &config,
                &state_path,
                at(2026, 5, 2, 4, 0),
                &spawner,
                &opts,
            ))
            .unwrap();
        assert_eq!(
            spawner.run_count(),
            0,
            "must not spawn over a live submitted claim"
        );
        assert!(
            report
                .skipped
                .iter()
                .any(|s| s.pipeline == "staging" && s.reason == TickSkipReason::InFlight),
            "reported as in_flight"
        );
    }

    // --- regression iii: crash between child-exit and terminal txn -----------

    #[test]
    fn stuck_submitted_resolved_from_run_record() {
        let (state_path, _dir, opts) = temp_env();
        let config = cfg(AFTER_ONLY);
        let key = with_store(&state_path, |store| {
            seed_run(
                store,
                "raw",
                None,
                RunStatus::Success,
                at(2026, 5, 2, 3, 5),
                at(2026, 5, 2, 3, 10),
            );
            let key = seed_submitted_claim(
                store,
                "staging",
                DemandKind::After,
                at(2026, 5, 2, 3, 10),
                "sub-1",
                at(2026, 5, 2, 3, 50),
            );
            // The crashed owner's run DID record — a terminal failure under sub-1.
            seed_run(
                store,
                "staging",
                Some("sub-1"),
                RunStatus::Failure,
                at(2026, 5, 2, 3, 50),
                at(2026, 5, 2, 3, 51),
            );
            // Mirror the submission's cursor write (as the original claim CAS would).
            store
                .put_schedule_state(
                    "staging",
                    &ScheduleStateRecord {
                        last_attempt_at: Some(at(2026, 5, 2, 3, 50).to_rfc3339()),
                        last_submitted_run_id: Some("sub-1".to_string()),
                        ..Default::default()
                    },
                )
                .unwrap();
            key
        });

        let spawner = CapturingSpawner::new(0);
        // retry.max = 0 → the failure is exhaustion → terminal, no re-run.
        rt().block_on(tick_once(
            &config,
            &state_path,
            at(2026, 5, 2, 3, 52),
            &spawner,
            &opts,
        ))
        .unwrap();
        assert_eq!(
            spawner.run_count(),
            0,
            "resolver finalizes, never re-runs here"
        );

        with_store(&state_path, |store| {
            let cursor = store.get_schedule_state("staging").unwrap().unwrap();
            assert_eq!(cursor.consecutive_failures, 1, "failure recorded once");
            assert_eq!(cursor.last_attempt_outcome.as_deref(), Some("failure"));
            let claim = store.get_schedule_claim(&key).unwrap().unwrap();
            assert!(
                matches!(claim.state, ClaimState::Released),
                "standing failure releases (re-claimable after backoff)"
            );
            assert!(!claim.budget_open, "exhausted release closes the cycle");
        });

        // A follow-up tick within the 5-minute failure backoff is suppressed.
        let report2 = rt()
            .block_on(tick_once(
                &config,
                &state_path,
                at(2026, 5, 2, 3, 54),
                &spawner,
                &opts,
            ))
            .unwrap();
        assert_eq!(spawner.run_count(), 0);
        // The resolver-finalized failure produced exactly ONE bundle — and the
        // follow-up tick did not double-write it (the claim is Released, not
        // Submitted, so resolve_stuck never re-fires).
        let bundles: Vec<_> = std::fs::read_dir(opts.rocky_dir.join("incidents"))
            .unwrap()
            .filter_map(Result::ok)
            .collect();
        assert_eq!(bundles.len(), 1, "one bundle, no double-write");
        let bundle: crate::schedule::incidents::IncidentBundle =
            serde_json::from_slice(&std::fs::read(bundles[0].path()).unwrap()).unwrap();
        assert_eq!(bundle.pipeline, "staging");
        assert_eq!(bundle.source, "after");
        assert_eq!(bundle.submission_id, "sub-1");
        assert_eq!(bundle.exit_code, None, "resolved from the run record");
        assert_eq!(
            bundle.consecutive_failures,
            Some(1),
            "the resolver applied the delta before the bundle read it"
        );
        assert!(
            report2.skipped.iter().any(|s| s.pipeline == "staging"
                && matches!(s.reason, TickSkipReason::FailureBackoff { .. })),
            "honors the cross-tick backoff"
        );
    }

    // --- regression iv: crash between claim-commit and spawn (cron) -----------

    #[test]
    fn crash_between_claim_and_spawn_never_refires_cron() {
        let (state_path, _dir, opts) = temp_env();
        let config = cfg(CRON_ONLY);
        // A prior tick claimed the 03:00 occurrence (anchor advanced) then died
        // before spawning: a submitted claim with no run record.
        let occ = at(2026, 5, 2, 3, 0);
        with_store(&state_path, |store| {
            store.advance_schedule_fire_anchor("raw", occ).unwrap();
            seed_submitted_claim(store, "raw", DemandKind::Cron, occ, "crashed", occ);
        });

        let spawner = CapturingSpawner::new(0);
        // The occurrence's anchor already advanced, so it is not due — a bounded
        // miss, never a duplicate.
        rt().block_on(tick_once(
            &config,
            &state_path,
            at(2026, 5, 2, 4, 0),
            &spawner,
            &opts,
        ))
        .unwrap();
        assert_eq!(spawner.run_count(), 0, "missed occurrence must not re-fire");

        // The next occurrence fires normally.
        let report = rt()
            .block_on(tick_once(
                &config,
                &state_path,
                at(2026, 5, 3, 4, 0),
                &spawner,
                &opts,
            ))
            .unwrap();
        assert_eq!(spawner.run_count(), 1, "next occurrence fires");
        assert_eq!(report.executed[0].logical_ts, at(2026, 5, 3, 3, 0));
    }

    // --- regression vi: in-tick retry through the reconciler -----------------

    #[test]
    fn in_tick_retry_runs_configured_attempts() {
        let (state_path, _dir, opts) = temp_env();
        let config = cfg(CRON_RETRY2);
        let cap = CapturingSpawner::new(0);
        // attempt 1 fails, 2 fails, 3 succeeds.
        cap.script("raw", [1, 1, 0]);
        rt().block_on(tick_once(
            &config,
            &state_path,
            at(2026, 5, 1, 12, 0),
            &cap,
            &opts,
        ))
        .unwrap();
        let report = rt()
            .block_on(tick_once(
                &config,
                &state_path,
                at(2026, 5, 2, 4, 0),
                &cap,
                &opts,
            ))
            .unwrap();

        assert_eq!(cap.runs_for("raw").len(), 3, "three in-tick attempts");
        assert_eq!(report.executed.len(), 1);
        assert_eq!(report.executed[0].outcome, TerminalOutcome::Success);
        assert_eq!(report.executed[0].attempts, 3);
        let subs: std::collections::HashSet<_> = cap
            .runs_for("raw")
            .iter()
            .map(|c| c.request.submission_id.clone())
            .collect();
        assert_eq!(subs.len(), 3, "each attempt has a distinct submission id");
    }

    // --- regression: shutdown during an in-tick retry must not respawn -------

    /// A spawner that raises a shared drain the instant it runs, then returns a
    /// failure exit code — modelling a child SIGTERM'd by a server shutdown while
    /// it was running (a killed child exits nonzero). `retry.max > 0` would
    /// normally make that failure retry.
    struct DrainOnRunSpawner {
        drain: Drain,
        runs: std::sync::atomic::AtomicU32,
    }

    #[async_trait]
    impl Spawner for DrainOnRunSpawner {
        async fn run(&self, _request: &SpawnRequest) -> RunOutcome {
            self.runs.fetch_add(1, std::sync::atomic::Ordering::Release);
            // Shutdown lands while this attempt is in flight, and the
            // spawner terminates the child for it (the drain-kill path).
            self.drain.signal();
            RunOutcome {
                exit_code: 1,
                pid: Some(4242),
                drain_interrupted: true,
            }
        }
    }

    #[test]
    fn shutdown_during_in_tick_retry_finalizes_without_respawn() {
        let (state_path, _dir, mut opts) = temp_env();
        // A shared drain: the spawner raises it mid-attempt; the reconciler reads
        // it when deciding whether to retry.
        let drain = Drain::new();
        opts.drain = drain.clone();
        // `retry.max = 2` ⇒ up to THREE attempts absent the fix.
        let config = cfg(CRON_RETRY2);
        let spawner = DrainOnRunSpawner {
            drain,
            runs: std::sync::atomic::AtomicU32::new(0),
        };

        // Tick 1: first sight of the cron anchors, nothing fires (so the drain is
        // not raised yet).
        rt().block_on(tick_once(
            &config,
            &state_path,
            at(2026, 5, 1, 12, 0),
            &spawner,
            &opts,
        ))
        .unwrap();
        assert_eq!(
            spawner.runs.load(std::sync::atomic::Ordering::Acquire),
            0,
            "first sight anchors, no fire",
        );

        // Tick 2: the occurrence is due → attempt 1 runs and raises the drain. The
        // reconciler must NOT spawn attempts 2 and 3 after shutdown was requested.
        let report = rt()
            .block_on(tick_once(
                &config,
                &state_path,
                at(2026, 5, 2, 4, 0),
                &spawner,
                &opts,
            ))
            .unwrap();

        assert_eq!(
            spawner.runs.load(std::sync::atomic::Ordering::Acquire),
            1,
            "a drain raised mid-attempt finalizes the run; it must not respawn a retry",
        );
        assert_eq!(report.executed.len(), 1);
        assert_eq!(report.executed[0].outcome, TerminalOutcome::Failure);
        assert_eq!(
            report.executed[0].attempts, 1,
            "the single attempt is terminal"
        );
        // A drain-interrupted failure is NOT an incident: a graceful shutdown
        // must not litter `.rocky/incidents/`.
        assert!(
            !opts.rocky_dir.join("incidents").exists()
                || std::fs::read_dir(opts.rocky_dir.join("incidents"))
                    .unwrap()
                    .next()
                    .is_none(),
            "no incident bundle on drain"
        );
    }

    /// The tick's public `attempts` and the incident bundle's `attempts` are
    /// DIFFERENT counters and must stay decoupled: total submissions (the
    /// frozen tick contract) vs attempts within this demand cycle. A standing
    /// demand that fails across two cycles makes them diverge — 2 vs 1.
    #[test]
    fn tick_attempts_stay_total_while_bundle_attempts_are_cycle_scoped() {
        let (state_path, _dir, opts) = temp_env();
        let config = cfg(AFTER_ONLY);
        // Cycle 1: raw succeeds, staging fails (released with failure backoff).
        with_store(&state_path, |store| {
            seed_run(
                store,
                "raw",
                None,
                RunStatus::Success,
                at(2026, 5, 2, 3, 5),
                at(2026, 5, 2, 3, 10),
            );
        });
        let spawner = CapturingSpawner::new(0);
        spawner.script("staging", [1, 1]);
        let report1 = rt()
            .block_on(tick_once(
                &config,
                &state_path,
                at(2026, 5, 2, 3, 20),
                &spawner,
                &opts,
            ))
            .unwrap();
        assert_eq!(report1.executed.len(), 1);
        assert_eq!(report1.executed[0].attempts, 1, "first submission overall");

        // Cycle 2 (past the failure backoff): re-claimed — a fresh cycle, but
        // the SECOND submission overall.
        let report2 = rt()
            .block_on(tick_once(
                &config,
                &state_path,
                at(2026, 5, 2, 3, 40),
                &spawner,
                &opts,
            ))
            .unwrap();
        assert_eq!(report2.executed.len(), 1, "{:?}", report2.skipped);
        assert_eq!(
            report2.executed[0].attempts, 2,
            "the public tick contract reports TOTAL submissions"
        );

        // The newest bundle reports the cycle's attempts: 1, not 2.
        let mut bundles: Vec<_> = std::fs::read_dir(opts.rocky_dir.join("incidents"))
            .unwrap()
            .filter_map(Result::ok)
            .map(|e| e.path())
            .collect();
        bundles.sort();
        assert_eq!(bundles.len(), 2, "one bundle per failed cycle");
        let read = |p: &std::path::PathBuf| -> crate::schedule::incidents::IncidentBundle {
            serde_json::from_slice(&std::fs::read(p).unwrap()).unwrap()
        };
        let (first, newest) = (read(&bundles[0]), read(&bundles[1]));
        assert_ne!(
            first.submission_id, newest.submission_id,
            "two distinct failed submissions"
        );
        assert_eq!(
            newest.submission_id, report2.executed[0].submission_id,
            "the newest bundle is cycle 2's"
        );
        assert_eq!(
            newest.attempts, 1,
            "the bundle reports attempts within THIS cycle"
        );
    }

    /// A child that failed ON ITS OWN while a drain happened to be raised:
    /// the spawner did not terminate it, so its failure is genuine and its
    /// incident must be recorded. Discriminating on the spawner's own
    /// `drain_interrupted` record — not a re-sample of the drain flag — is
    /// what makes this hold.
    #[test]
    fn a_genuine_failure_coinciding_with_a_drain_still_records_its_incident() {
        struct FailThenDrainSpawner {
            drain: Drain,
        }
        #[async_trait]
        impl Spawner for FailThenDrainSpawner {
            async fn run(&self, request: &SpawnRequest) -> RunOutcome {
                // The child exits 1 on its own; shutdown lands right after.
                let store = StateStore::open(&request.state_path).unwrap();
                seed_run(
                    &store,
                    &request.pipeline,
                    Some(&request.submission_id),
                    RunStatus::Failure,
                    at(2026, 5, 2, 3, 0),
                    at(2026, 5, 2, 3, 1),
                );
                drop(store);
                self.drain.signal();
                RunOutcome {
                    exit_code: 1,
                    pid: Some(4242),
                    drain_interrupted: false,
                }
            }
        }

        let (state_path, _dir, mut opts) = temp_env();
        let config = cfg(CRON_ONLY);
        let drain = Drain::default();
        opts.drain = drain.clone();
        let spawner = FailThenDrainSpawner { drain };
        // Tick 1 anchors the cron (first sight never fires); tick 2 fires it.
        rt().block_on(tick_once(
            &config,
            &state_path,
            at(2026, 5, 2, 2, 0),
            &spawner,
            &opts,
        ))
        .unwrap();
        rt().block_on(tick_once(
            &config,
            &state_path,
            at(2026, 5, 2, 3, 0),
            &spawner,
            &opts,
        ))
        .unwrap();

        let bundles: Vec<_> = std::fs::read_dir(opts.rocky_dir.join("incidents"))
            .unwrap()
            .filter_map(Result::ok)
            .collect();
        assert_eq!(
            bundles.len(),
            1,
            "a genuine failure is an incident even during shutdown"
        );
    }

    // --- regression vii: crash-safe retry budget (standing demand) -----------

    #[test]
    fn crash_safe_retry_budget_reclaims_and_runs_attempt_two() {
        let (state_path, _dir, opts) = temp_env();
        let config = cfg(AFTER_RETRY2);
        let key = with_store(&state_path, |store| {
            seed_run(
                store,
                "raw",
                None,
                RunStatus::Success,
                at(2026, 5, 2, 3, 5),
                at(2026, 5, 2, 3, 10),
            );
            // attempt 1 ran, failed, then the reconciler crashed before the retry
            // CAS: a submitted claim (cycle_attempts = 1) + a failure record.
            let key = seed_submitted_claim(
                store,
                "staging",
                DemandKind::After,
                at(2026, 5, 2, 3, 10),
                "att-1",
                at(2026, 5, 2, 3, 50),
            );
            seed_run(
                store,
                "staging",
                Some("att-1"),
                RunStatus::Failure,
                at(2026, 5, 2, 3, 50),
                at(2026, 5, 2, 3, 51),
            );
            store
                .put_schedule_state(
                    "staging",
                    &ScheduleStateRecord {
                        last_attempt_at: Some(at(2026, 5, 2, 3, 50).to_rfc3339()),
                        ..Default::default()
                    },
                )
                .unwrap();
            key
        });

        let spawner = CapturingSpawner::new(0); // attempt 2 succeeds
        let report = rt()
            .block_on(tick_once(
                &config,
                &state_path,
                at(2026, 5, 2, 3, 52),
                &spawner,
                &opts,
            ))
            .unwrap();

        assert_eq!(
            spawner.run_count(),
            1,
            "budget-open resolver re-claims and fires attempt 2 in the same tick"
        );
        assert_eq!(report.executed.len(), 1);
        assert_eq!(report.executed[0].outcome, TerminalOutcome::Success);
        with_store(&state_path, |store| {
            let claim = store.get_schedule_claim(&key).unwrap().unwrap();
            assert!(claim.is_completed(), "attempt 2 succeeded → completed");
            assert_eq!(
                claim.cycle_attempts, 2,
                "the cycle ordinal continued across the crash"
            );
        });
    }

    // --- lock: tick_in_progress ----------------------------------------------

    #[test]
    fn tick_in_progress_skips_whole_tick() {
        let (state_path, _dir, opts) = temp_env();
        let config = cfg(CRON_ONLY);
        std::fs::create_dir_all(&opts.rocky_dir).unwrap();
        let TickAcquire::Acquired(_held) = TickLock::try_acquire(&opts.rocky_dir).unwrap() else {
            panic!("expected to acquire the lock");
        };
        let spawner = CapturingSpawner::new(0);
        let report = rt()
            .block_on(tick_once(
                &config,
                &state_path,
                at(2026, 5, 2, 4, 0),
                &spawner,
                &opts,
            ))
            .unwrap();
        assert!(report.skipped_whole_tick, "a fresh holder means skip");
        assert_eq!(spawner.run_count(), 0);
    }

    // --- dry run: no writes, no spawns ---------------------------------------

    #[test]
    fn dry_run_writes_nothing_and_spawns_nothing() {
        let (state_path, _dir, opts) = temp_env();
        let config = cfg(CRON_ONLY);
        // Anchor set so the cron would otherwise be due.
        let before = with_store(&state_path, |store| {
            store
                .advance_schedule_fire_anchor("raw", at(2026, 5, 1, 3, 0))
                .unwrap();
            store.get_schedule_state("raw").unwrap()
        });

        let spawner = CapturingSpawner::new(0);
        let dry = TickOptions {
            dry_run: true,
            ..opts.clone()
        };
        let report = rt()
            .block_on(tick_once(
                &config,
                &state_path,
                at(2026, 5, 2, 4, 0),
                &spawner,
                &dry,
            ))
            .unwrap();

        assert_eq!(spawner.run_count(), 0, "dry run spawns nothing");
        assert!(report.executed.is_empty());
        assert!(
            report
                .evaluated
                .iter()
                .any(|e| e.pipeline == "raw" && e.due.is_some()),
            "still reports the demand as due"
        );
        with_store(&state_path, |store| {
            assert_eq!(
                before,
                store.get_schedule_state("raw").unwrap(),
                "dry run writes no state"
            );
        });
    }

    // --- fail-closed on a corrupt run_history row (F4/F5), end to end ---------

    const FRESHNESS_ONLY: &str = r#"
[freshness]
expected_lag_seconds = 3600

[adapter.db]
type = "duckdb"

[pipeline.raw]
type = "transformation"
[pipeline.raw.target]
adapter = "db"
[pipeline.raw.schedule]
freshness = true
"#;

    #[test]
    fn after_corrupt_history_row_fails_closed_no_spawn() {
        let (state_path, _dir, opts) = temp_env();
        let config = cfg(AFTER_ONLY);
        with_store(&state_path, |store| {
            // A genuine upstream success — `after` would be due...
            seed_run(
                store,
                "raw",
                None,
                RunStatus::Success,
                at(2026, 5, 2, 3, 5),
                at(2026, 5, 2, 3, 10),
            );
            // ...but a corrupt row in run_history makes the scan error.
            store.insert_corrupt_run_history_row("run-corrupt").unwrap();
        });

        let spawner = CapturingSpawner::new(0);
        let report = rt()
            .block_on(tick_once(
                &config,
                &state_path,
                at(2026, 5, 2, 4, 0),
                &spawner,
                &opts,
            ))
            .unwrap();

        assert_eq!(
            spawner.run_count(),
            0,
            "a history read fault must never spawn on data it could not read"
        );
        assert!(
            report
                .skipped
                .iter()
                .any(|s| s.pipeline == "staging" && s.reason == TickSkipReason::HistoryUnavailable),
            "the fault is recorded loudly, not a silent drop: {:?}",
            report.skipped
        );
    }

    #[test]
    fn freshness_corrupt_history_row_does_not_fire_epoch() {
        let (state_path, _dir, opts) = temp_env();
        let config = cfg(FRESHNESS_ONLY);
        // No successful run at all + a corrupt row: freshness must NOT read the
        // fault as "never ran" and fire the epoch occurrence.
        with_store(&state_path, |store| {
            store.insert_corrupt_run_history_row("run-corrupt").unwrap();
        });

        let spawner = CapturingSpawner::new(0);
        let report = rt()
            .block_on(tick_once(
                &config,
                &state_path,
                at(2026, 5, 2, 4, 0),
                &spawner,
                &opts,
            ))
            .unwrap();

        assert_eq!(
            spawner.run_count(),
            0,
            "a read fault must not fire the epoch occurrence"
        );
        assert!(
            report
                .skipped
                .iter()
                .any(|s| s.pipeline == "raw" && s.reason == TickSkipReason::HistoryUnavailable),
            "recorded fail-closed: {:?}",
            report.skipped
        );
    }

    // --- F3: member-model freshness budget narrows the due window (wired in
    //         PR2; the reconciler core consumes `TickOptions::member_budgets`).
    #[test]
    fn freshness_member_budget_narrows_due_window() {
        // Project default is 3600s (FRESHNESS_ONLY). Last success finished 1800s
        // before `now`, so under the project budget the run is still fresh.
        let now = at(2026, 5, 2, 4, 0);
        let last_success_finished = at(2026, 5, 2, 3, 30); // now − 1800s
        let config = cfg(FRESHNESS_ONLY);

        // (a) No member budgets ⇒ project default (3600s) applies ⇒ 1800 < 3600
        //     ⇒ NOT stale ⇒ no spawn. This is the pre-wiring behavior.
        {
            let (state_path, _dir, opts) = temp_env();
            with_store(&state_path, |store| {
                seed_run(
                    store,
                    "raw",
                    None,
                    RunStatus::Success,
                    at(2026, 5, 2, 3, 25),
                    last_success_finished,
                );
            });
            let spawner = CapturingSpawner::new(0);
            let report = rt()
                .block_on(tick_once(&config, &state_path, now, &spawner, &opts))
                .unwrap();
            assert_eq!(
                spawner.run_count(),
                0,
                "under the project budget the run is still fresh: {:?}",
                report.evaluated
            );
        }

        // (b) A tighter member budget (600s) via `member_budgets` ⇒ MIN wins ⇒
        //     1800 > 600 ⇒ stale ⇒ the freshness demand fires. This is the wiring
        //     the empty-slice default previously suppressed (silent under-fire).
        {
            let (state_path, _dir, mut opts) = temp_env();
            opts.member_budgets
                .insert("raw".to_string(), vec![600, 7200]);
            with_store(&state_path, |store| {
                seed_run(
                    store,
                    "raw",
                    None,
                    RunStatus::Success,
                    at(2026, 5, 2, 3, 25),
                    last_success_finished,
                );
            });
            let spawner = CapturingSpawner::new(0);
            let report = rt()
                .block_on(tick_once(&config, &state_path, now, &spawner, &opts))
                .unwrap();
            assert_eq!(
                spawner.run_count(),
                1,
                "the tightest member budget (600s) makes the run stale and due: {:?}",
                report.evaluated
            );
        }
    }

    // --- owned-store lifecycle: a sibling handle busy-skips the whole tick ----

    #[test]
    fn state_store_held_elsewhere_busy_skips_whole_tick() {
        let (state_path, _dir, opts) = temp_env();
        let config = cfg(CRON_ONLY);
        // A concurrent `rocky run` already holds the state store's exclusive
        // advisory lock. The tick takes its lock, then finds the store busy and
        // skips the whole pass (exit 0) rather than faulting.
        let held = StateStore::open(&state_path).unwrap();
        let spawner = CapturingSpawner::new(0);
        let report = rt()
            .block_on(tick_once(
                &config,
                &state_path,
                at(2026, 5, 2, 4, 0),
                &spawner,
                &opts,
            ))
            .unwrap();
        assert!(report.state_busy, "a busy store defers the whole tick");
        assert!(
            report.evaluated.is_empty() && report.executed.is_empty(),
            "nothing is evaluated or executed when the store never opened"
        );
        assert_eq!(spawner.run_count(), 0, "no child runs over a busy store");
        drop(held);
    }

    // --- owned-store lifecycle: the child can open the released store ----------

    #[test]
    fn child_window_can_open_state_store() {
        let (state_path, _dir, opts) = temp_env();
        let config = cfg(CRON_ONLY);

        // First sight anchors raw's cron, fires nothing.
        let anchor = CapturingSpawner::new(0);
        rt().block_on(tick_once(
            &config,
            &state_path,
            at(2026, 5, 1, 12, 0),
            &anchor,
            &opts,
        ))
        .unwrap();

        // The occurrence fires. `OpeningSpawner` opens the SAME state store itself
        // and writes its run record — which only succeeds because the parent
        // released the store around the spawn. Pre-fix (parent held the store),
        // this open failed with `LockHeldByOther`; this test pinpoints the flock
        // regression.
        let spawner = OpeningSpawner {
            state_path: state_path.clone(),
            captured: Mutex::new(Vec::new()),
            exit_code: 0,
            finished_at: at(2026, 5, 2, 3, 5),
        };
        let report = rt()
            .block_on(tick_once(
                &config,
                &state_path,
                at(2026, 5, 2, 4, 0),
                &spawner,
                &opts,
            ))
            .unwrap();

        assert_eq!(
            spawner.captured.lock().unwrap().len(),
            1,
            "exactly one spawn"
        );
        assert_eq!(report.executed.len(), 1);
        assert_eq!(report.executed[0].source, DemandKind::Cron);
        assert_eq!(report.executed[0].outcome, TerminalOutcome::Success);
        // The run record the child wrote through its own handle is durable and
        // visible after the tick.
        with_store(&state_path, |store| {
            assert!(
                store.latest_successful_run("raw").unwrap().is_some(),
                "the child's run record is visible post-tick"
            );
        });
    }

    // --- sweep: an orphaned submitted claim is resolved off the demand path ----

    #[test]
    fn sweep_resolves_orphaned_submitted_claim() {
        let (state_path, _dir, opts) = temp_env();
        let config = cfg(CRON_ONLY);
        // A prior tick claimed the 03:00 occurrence (anchor advanced) and its
        // child recorded a terminal FAILURE, but the terminal claim transition
        // never landed — an orphaned `submitted` claim WITH a matching run record.
        let occ = at(2026, 5, 2, 3, 0);
        let key = with_store(&state_path, |store| {
            store.advance_schedule_fire_anchor("raw", occ).unwrap();
            let key = seed_submitted_claim(store, "raw", DemandKind::Cron, occ, "orphan-1", occ);
            seed_run(
                store,
                "raw",
                Some("orphan-1"),
                RunStatus::Failure,
                occ,
                at(2026, 5, 2, 3, 1),
            );
            key
        });

        // now = 04:00: the anchor already advanced past the 03:00 occurrence, so
        // the cron demand is NOT due — the per-demand loop never re-evaluates this
        // key and `resolve_stuck` never runs. Any resolution is therefore
        // attributable ONLY to `sweep_orphan_claims`.
        let spawner = CapturingSpawner::new(0);
        let report = rt()
            .block_on(tick_once(
                &config,
                &state_path,
                at(2026, 5, 2, 4, 0),
                &spawner,
                &opts,
            ))
            .unwrap();
        assert_eq!(
            spawner.run_count(),
            0,
            "nothing is due; the sweep never spawns"
        );
        assert!(report.executed.is_empty());

        with_store(&state_path, |store| {
            // The sweep hardcodes retry_max = 0, so cron + failure lands terminal
            // `completed`, not `released` — the orphaned occurrence never re-runs.
            let claim = store.get_schedule_claim(&key).unwrap().unwrap();
            assert!(
                matches!(
                    claim.state,
                    ClaimState::Completed {
                        outcome: TerminalOutcome::Failure
                    }
                ),
                "the sweep drove the cron orphan terminal: {:?}",
                claim.state
            );
            // The sweep is claim-lifecycle-only: it does NOT touch the
            // per-pipeline `consecutive_failures` scalar (that is owned by the
            // demand-loop path, so an orphan's older outcome can't overwrite a
            // newer one out of order). The cursor stays as seeded.
            let cursor = store.get_schedule_state("raw").unwrap().unwrap();
            assert_eq!(
                cursor.consecutive_failures, 0,
                "the sweep leaves consecutive_failures untouched"
            );
        });

        // The orphaned failure is still an incident — the sweep emits the
        // bundle the crashed owner never could. Facts the sweep cannot know
        // are None, never fabricated.
        let bundles: Vec<_> = std::fs::read_dir(opts.rocky_dir.join("incidents"))
            .unwrap()
            .filter_map(Result::ok)
            .collect();
        assert_eq!(bundles.len(), 1, "exactly one bundle for the orphan");
        let bundle: crate::schedule::incidents::IncidentBundle =
            serde_json::from_slice(&std::fs::read(bundles[0].path()).unwrap()).unwrap();
        assert_eq!(bundle.pipeline, "raw");
        assert_eq!(bundle.outcome, "failure");
        assert_eq!(bundle.submission_id, "orphan-1");
        assert_eq!(bundle.exit_code, None, "no child was observed");
        assert_eq!(
            bundle.consecutive_failures, None,
            "the sweep does not touch scalars, so it must not report them"
        );
        assert_eq!(bundle.logical_ts, None);
    }

    // --- webhook ingress (at-most-once spool consumption) --------------------

    /// A no-schedule pipeline a webhook can target (plus a second one for the
    /// two-pipelines dedup case).
    const WEBHOOK_TARGETS: &str = r#"
[adapter.db]
type = "duckdb"

[pipeline.raw]
type = "transformation"
[pipeline.raw.target]
adapter = "db"

[pipeline.other]
type = "transformation"
[pipeline.other.target]
adapter = "db"
"#;

    fn webhook_now() -> DateTime<Utc> {
        at(2026, 5, 2, 3, 0)
    }

    /// The `demand_uid` minted by an accept, read back off the single pending
    /// file so a test can seed a matching claim.
    fn accept_and_read_uid(
        rocky_dir: &std::path::Path,
        pipeline: &str,
        kind: crate::schedule::spool::WebhookKind,
        token: &str,
    ) -> String {
        let outcome = crate::schedule::spool::accept(
            rocky_dir,
            pipeline,
            kind,
            token,
            "bodyhash",
            webhook_now(),
        )
        .unwrap();
        match outcome {
            crate::schedule::spool::AcceptOutcome::Created(uid) => uid,
            crate::schedule::spool::AcceptOutcome::Duplicate => panic!("expected a fresh accept"),
        }
    }

    /// The #1334 red team's confirmed bypass, pinned at the tick level: a
    /// paused pipeline must DEFER its spooled webhook demands — no execution,
    /// the pending file untouched (an accepted 202'd delivery is never
    /// dropped), a `paused` skip recorded — and resuming fires exactly the
    /// deferred demand. The demand-level pause tests structurally cannot
    /// catch this: webhook consumption constructs its demand outside
    /// `evaluate_one`'s gate.
    #[test]
    fn a_paused_pipeline_defers_spooled_webhooks_and_resume_fires_them() {
        let (state_path, _dir, opts) = temp_env();
        let config = cfg(WEBHOOK_TARGETS);
        accept_and_read_uid(&opts.rocky_dir, "raw", spool::WebhookKind::Id, "evt-hold");
        with_store(&state_path, |store| {
            store.set_schedule_paused("raw", true).unwrap();
        });

        let spawner = CapturingSpawner::new(0);
        let report = rt()
            .block_on(tick_once(
                &config,
                &state_path,
                webhook_now(),
                &spawner,
                &opts,
            ))
            .unwrap();

        assert_eq!(spawner.run_count(), 0, "a paused pipeline must not fire");
        assert!(report.executed.is_empty());
        assert!(
            report.skipped.iter().any(|s| s.pipeline == "raw"
                && s.source == Some(DemandKind::Webhook)
                && s.reason == TickSkipReason::Paused),
            "the deferral must be recorded, not silent: {:?}",
            report.skipped
        );
        assert_eq!(
            spool::list_pending_files(&opts.rocky_dir).unwrap().len(),
            1,
            "the accepted delivery must stay PENDING — deferred, never dropped"
        );

        // Resume → the deferred demand fires exactly once.
        with_store(&state_path, |store| {
            store.set_schedule_paused("raw", false).unwrap();
        });
        let report2 = rt()
            .block_on(tick_once(
                &config,
                &state_path,
                webhook_now(),
                &spawner,
                &opts,
            ))
            .unwrap();
        assert_eq!(spawner.run_count(), 1, "resume fires the deferred demand");
        assert_eq!(report2.executed.len(), 1);
        assert_eq!(report2.executed[0].source, DemandKind::Webhook);
    }

    /// A spooled webhook demand fires its pipeline exactly once and disposes the
    /// file to a `.done` tombstone (id kind) — this is also the 202-durability
    /// case: the file is present, the server "crashed", the next tick executes.
    #[test]
    fn webhook_id_demand_fires_once_and_tombstones() {
        let (state_path, _dir, opts) = temp_env();
        let config = cfg(WEBHOOK_TARGETS);
        accept_and_read_uid(&opts.rocky_dir, "raw", spool::WebhookKind::Id, "evt-1");

        let spawner = CapturingSpawner::new(0);
        let report = rt()
            .block_on(tick_once(
                &config,
                &state_path,
                webhook_now(),
                &spawner,
                &opts,
            ))
            .unwrap();

        assert_eq!(
            spawner.run_count(),
            1,
            "the spooled webhook demand runs once"
        );
        let runs = spawner.runs_for("raw");
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].request.trigger, RunTriggerKind::Webhook);
        assert_eq!(report.executed.len(), 1);
        assert_eq!(report.executed[0].source, DemandKind::Webhook);

        // Consumed: no pending file remains, a `.done` tombstone was left.
        assert!(
            spool::list_pending_files(&opts.rocky_dir)
                .unwrap()
                .is_empty()
        );

        // Re-tick at the same instant: the tombstone dedups, nothing re-fires.
        let report2 = rt()
            .block_on(tick_once(
                &config,
                &state_path,
                webhook_now(),
                &spawner,
                &opts,
            ))
            .unwrap();
        assert_eq!(spawner.run_count(), 1, "an id demand never runs twice");
        assert!(report2.executed.is_empty());
    }

    /// A failed webhook child (exit 1) is consumed with NO requeue — at-most-once
    /// means one attempt, and the failure is visible in the report/history.
    #[test]
    fn webhook_failed_child_is_consumed_without_requeue() {
        let (state_path, _dir, opts) = temp_env();
        let config = cfg(WEBHOOK_TARGETS);
        accept_and_read_uid(&opts.rocky_dir, "raw", spool::WebhookKind::Id, "evt-1");

        let spawner = CapturingSpawner::new(0);
        spawner.script("raw", [1]); // the child exits 1 (failure)
        let report = rt()
            .block_on(tick_once(
                &config,
                &state_path,
                webhook_now(),
                &spawner,
                &opts,
            ))
            .unwrap();

        assert_eq!(
            spawner.run_count(),
            1,
            "one attempt, no retry (retry_max=0)"
        );
        assert_eq!(report.executed.len(), 1);
        assert_eq!(report.executed[0].outcome, TerminalOutcome::Failure);
        assert!(
            spool::list_pending_files(&opts.rocky_dir)
                .unwrap()
                .is_empty()
        );
        // A genuine failure writes exactly one incident bundle with the raw
        // facts — pipeline, source, exit — and pointer commands, no prose.
        let bundles: Vec<_> = std::fs::read_dir(opts.rocky_dir.join("incidents"))
            .expect("incidents dir exists after a failure")
            .filter_map(Result::ok)
            .collect();
        assert_eq!(bundles.len(), 1, "exactly one incident for one failure");
        let bundle: crate::schedule::incidents::IncidentBundle =
            serde_json::from_slice(&std::fs::read(bundles[0].path()).unwrap()).unwrap();
        assert_eq!(bundle.pipeline, "raw");
        assert_eq!(bundle.source, "webhook");
        assert_eq!(bundle.exit_code, Some(1));
        assert!(!bundle.pointers.is_empty());

        // Re-tick: the claim is completed{failure}, so it never re-runs.
        rt().block_on(tick_once(
            &config,
            &state_path,
            webhook_now(),
            &spawner,
            &opts,
        ))
        .unwrap();
        assert_eq!(spawner.run_count(), 1, "a failed webhook is not requeued");
    }

    /// Crash between the claim CAS and disposal: the claim is `submitted` and the
    /// child already recorded a terminal run, but the spool file is still present.
    /// The next tick idempotently completes disposal with EXACTLY ONE run and no
    /// new spawn.
    #[test]
    fn webhook_crash_after_claim_self_heals_to_exactly_one_run() {
        let (state_path, _dir, opts) = temp_env();
        let config = cfg(WEBHOOK_TARGETS);
        let uid = accept_and_read_uid(&opts.rocky_dir, "raw", spool::WebhookKind::Id, "evt-1");

        // Simulate the crash: a submitted claim for this demand's key + a terminal
        // run the (now-dead) child recorded, both present, plus the spool file.
        let key = claim_key(&Demand {
            pipeline: "raw".to_string(),
            source: DemandKind::Webhook,
            logical_ts: webhook_now(),
            claim_discriminator: Some(uid.clone()),
        });
        with_store(&state_path, |store| {
            let claim = ClaimRecord::new_submitted("sub-crash".to_string(), webhook_now());
            store
                .schedule_claim_cas(&key, None, &claim, "raw", &ScheduleStateMutation::None)
                .unwrap();
            seed_run(
                store,
                "raw",
                Some("sub-crash"),
                RunStatus::Success,
                webhook_now(),
                webhook_now(),
            );
        });

        let spawner = CapturingSpawner::new(0);
        rt().block_on(tick_once(
            &config,
            &state_path,
            webhook_now(),
            &spawner,
            &opts,
        ))
        .unwrap();

        // No NEW spawn — the recorded run is joined and the claim finalized.
        assert_eq!(
            spawner.run_count(),
            0,
            "the recorded run is honored, not re-run"
        );
        with_store(&state_path, |store| {
            let claim = store.get_schedule_claim(&key).unwrap().unwrap();
            assert!(claim.is_completed(), "the crashed claim was finalized");
        });
        // Disposed — no pending file lingers.
        assert!(
            spool::list_pending_files(&opts.rocky_dir)
                .unwrap()
                .is_empty()
        );
    }

    /// The reconciler is the id-dedup authority: a pending file whose key already
    /// has a `.done` tombstone (a redelivery landing after consumption) is dropped
    /// WITHOUT running, closing the accept-side race.
    #[test]
    fn webhook_pending_with_existing_tombstone_is_dropped_not_run() {
        let (state_path, _dir, opts) = temp_env();
        let config = cfg(WEBHOOK_TARGETS);
        // First delivery: accept + consume → a `.done` tombstone.
        accept_and_read_uid(&opts.rocky_dir, "raw", spool::WebhookKind::Id, "evt-1");
        let spawner = CapturingSpawner::new(0);
        rt().block_on(tick_once(
            &config,
            &state_path,
            webhook_now(),
            &spawner,
            &opts,
        ))
        .unwrap();
        assert_eq!(spawner.run_count(), 1);

        // Simulate the accept-side race: a fresh pending file at the SAME key
        // (a different uid) materializes after the tombstone exists. Derive the
        // key path from the tombstone (strip `.done`) — the on-disk key is a
        // hash, so a test must not hand-encode it.
        let spool_path = spool::spool_dir(&opts.rocky_dir);
        let tombstone = std::fs::read_dir(&spool_path)
            .unwrap()
            .filter_map(Result::ok)
            .find(|e| e.file_name().to_string_lossy().ends_with(".done"))
            .expect("the first consume left a .done tombstone")
            .path();
        let racing = tombstone.with_extension(""); // `<hex>.done` -> `<hex>`
        let record = serde_json::json!({
            "demand_uid": "racing-uid",
            "pipeline": "raw",
            "kind": "id",
            "token": "evt-1",
            "received_at": webhook_now().to_rfc3339(),
            "body_hash": "h",
        });
        std::fs::write(&racing, serde_json::to_vec(&record).unwrap()).unwrap();

        rt().block_on(tick_once(
            &config,
            &state_path,
            webhook_now(),
            &spawner,
            &opts,
        ))
        .unwrap();
        assert_eq!(
            spawner.run_count(),
            1,
            "the tombstoned duplicate must not run"
        );
        assert!(
            spool::list_pending_files(&opts.rocky_dir)
                .unwrap()
                .is_empty()
        );
    }

    /// A body-keyed demand leaves no tombstone, so an identical body delivered
    /// again after consumption is a NEW demand that fires again.
    #[test]
    fn webhook_body_demand_fires_again_after_consumption() {
        let (state_path, _dir, opts) = temp_env();
        let config = cfg(WEBHOOK_TARGETS);
        let spawner = CapturingSpawner::new(0);

        accept_and_read_uid(&opts.rocky_dir, "raw", spool::WebhookKind::Body, "bodyhash");
        rt().block_on(tick_once(
            &config,
            &state_path,
            webhook_now(),
            &spawner,
            &opts,
        ))
        .unwrap();
        assert_eq!(spawner.run_count(), 1);
        assert!(
            spool::list_pending_files(&opts.rocky_dir)
                .unwrap()
                .is_empty()
        );

        // Same body again, after consumption → fires again.
        accept_and_read_uid(&opts.rocky_dir, "raw", spool::WebhookKind::Body, "bodyhash");
        rt().block_on(tick_once(
            &config,
            &state_path,
            webhook_now(),
            &spawner,
            &opts,
        ))
        .unwrap();
        assert_eq!(
            spawner.run_count(),
            2,
            "a body demand re-fires post-consumption"
        );
    }

    /// The same body delivered to two different pipelines is two demands; both
    /// fire.
    #[test]
    fn webhook_same_body_two_pipelines_both_fire() {
        let (state_path, _dir, opts) = temp_env();
        let config = cfg(WEBHOOK_TARGETS);
        accept_and_read_uid(&opts.rocky_dir, "raw", spool::WebhookKind::Body, "bh");
        accept_and_read_uid(&opts.rocky_dir, "other", spool::WebhookKind::Body, "bh");

        let spawner = CapturingSpawner::new(0);
        rt().block_on(tick_once(
            &config,
            &state_path,
            webhook_now(),
            &spawner,
            &opts,
        ))
        .unwrap();
        assert_eq!(spawner.run_count(), 2);
        assert_eq!(spawner.runs_for("raw").len(), 1);
        assert_eq!(spawner.runs_for("other").len(), 1);
    }

    /// A demand for a pipeline removed from config is finalized (never run) and
    /// disposed loudly — never left pending forever.
    #[test]
    fn webhook_demand_for_unknown_pipeline_is_finalized_not_run() {
        let (state_path, _dir, opts) = temp_env();
        let config = cfg(WEBHOOK_TARGETS);
        // "ghost" is not in the config.
        accept_and_read_uid(&opts.rocky_dir, "ghost", spool::WebhookKind::Id, "evt-1");

        let spawner = CapturingSpawner::new(0);
        let report = rt()
            .block_on(tick_once(
                &config,
                &state_path,
                webhook_now(),
                &spawner,
                &opts,
            ))
            .unwrap();
        assert_eq!(
            spawner.run_count(),
            0,
            "an unknown-pipeline demand never runs"
        );
        assert!(
            report
                .skipped
                .iter()
                .any(|s| s.reason == TickSkipReason::ConfigError),
            "the removed-pipeline demand is reported as a config_error skip"
        );
        assert!(
            spool::list_pending_files(&opts.rocky_dir)
                .unwrap()
                .is_empty(),
            "the orphaned demand is disposed, never left pending"
        );
    }

    /// A corrupt spool file is quarantined (never silently deleted) and the tick
    /// continues.
    #[test]
    fn webhook_corrupt_file_is_quarantined_and_tick_continues() {
        let (state_path, _dir, opts) = temp_env();
        let config = cfg(WEBHOOK_TARGETS);
        let spool_path = spool::spool_dir(&opts.rocky_dir);
        std::fs::create_dir_all(&spool_path).unwrap();
        std::fs::write(spool_path.join("raw__id__bad"), b"not json").unwrap();
        // A good demand alongside the corrupt one still runs.
        accept_and_read_uid(&opts.rocky_dir, "raw", spool::WebhookKind::Id, "good");

        let spawner = CapturingSpawner::new(0);
        rt().block_on(tick_once(
            &config,
            &state_path,
            webhook_now(),
            &spawner,
            &opts,
        ))
        .unwrap();
        assert_eq!(spawner.run_count(), 1, "the good demand still runs");
        assert!(
            spool::count_corrupt(&opts.rocky_dir).unwrap() >= 1,
            "the corrupt file was quarantined, not deleted"
        );
        assert!(
            spool::list_pending_files(&opts.rocky_dir)
                .unwrap()
                .is_empty()
        );
    }

    /// A delivery id shaped like a reserved control affix (`x.corrupt-9`) is
    /// spooled, consumed, and RUN — not hidden from the scan or miscounted as
    /// corrupt (red-team F2, consume side).
    #[test]
    fn webhook_affix_shaped_delivery_id_runs() {
        let (state_path, _dir, opts) = temp_env();
        let config = cfg(WEBHOOK_TARGETS);
        accept_and_read_uid(
            &opts.rocky_dir,
            "raw",
            spool::WebhookKind::Id,
            "x.corrupt-9",
        );

        let spawner = CapturingSpawner::new(0);
        rt().block_on(tick_once(
            &config,
            &state_path,
            webhook_now(),
            &spawner,
            &opts,
        ))
        .unwrap();
        assert_eq!(
            spawner.run_count(),
            1,
            "an affix-shaped delivery id still runs"
        );
        assert_eq!(
            spool::count_corrupt(&opts.rocky_dir).unwrap(),
            0,
            "an affix-shaped id must not raise a false corrupt alarm"
        );
        assert!(
            spool::list_pending_files(&opts.rocky_dir)
                .unwrap()
                .is_empty(),
            "the affix-shaped demand was consumed"
        );
    }
}
