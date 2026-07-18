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
    TerminalOutcome, decide_post_attempt, decide_pre_spawn, decide_resolver, sweep_terminal_claim,
};
use super::demand::{
    Demand, EvaluatedPipeline, HistoryError, ResolvedSchedule, RunHistoryView, RunSuccess,
    ScheduleStateView, SkipReason, SourceSkip, evaluate_one, resolve_schedule,
};
use super::lock::{TickAcquire, TickLock};
use super::record::{ScheduleStateMutation, ScheduleStateRecord};
use super::spawn::{Drain, SpawnRequest, Spawner};

/// A stuck `submitted` claim with no run record is held for this grace before
/// its attempt is treated as lost (the owner crashed between committing the
/// claim and writing a record). `now` is injected, so the grace is deterministic
/// under test and a crash never stalls a standing demand indefinitely.
const RECOVERY_GRACE: Duration = Duration::minutes(5);

/// A held tick lock whose heartbeat is older than this is a wedged owner: a
/// later tick proceeds via the wedge override rather than skipping forever.
const LOCK_TAKEOVER_AFTER: std::time::Duration = std::time::Duration::from_secs(300);

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
    /// The tick span's `traceparent`, propagated to each child so the run's own
    /// trace connects to the tick. `None` when tracing is inactive.
    pub traceparent: Option<String>,
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

    // 6. Sweep orphaned `submitted` claims — a prior tick's deferred or crashed
    //    terminal transaction — independent of whether their demand is due again
    //    (a cron occurrence advances its anchor on submission, so its claim is
    //    otherwise never re-evaluated). Only on a healthy real tick with the
    //    store still open.
    if !opts.dry_run {
        sweep_orphan_claims(&schedules, phase.store(), now)?;
    }

    Ok(report)
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
    let default_tz = config.schedule.timezone.as_str();
    let project_lag = config.freshness.expected_lag_seconds;
    let mut resolved = BTreeMap::new();
    let mut skips = Vec::new();
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
        match resolve_schedule(name, schedule_cfg, default_tz, members, project_lag) {
            Ok(rs) => {
                resolved.insert(name.clone(), rs);
            }
            Err(_) => {
                skips.push(SkippedDemand {
                    pipeline: name.clone(),
                    source: None,
                    reason: TickSkipReason::NotDue,
                });
            }
        }
    }
    (resolved, skips)
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

/// The claim storage key for a coordinate-derived demand:
/// `<pipeline>\u{1f}<source>\u{1f}<logical_ts>`.
fn claim_key(pipeline: &str, source: DemandKind, logical_ts: DateTime<Utc>) -> String {
    format!(
        "{pipeline}\u{1f}{}\u{1f}{}",
        source.as_str(),
        logical_ts.to_rfc3339()
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
    let key = claim_key(&demand.pipeline, demand.source, demand.logical_ts);
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
                match resolve_stuck(&key, demand, schedule, &current, phase.store(), now)? {
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
            traceparent: opts.traceparent.clone(),
            timeout: schedule.timeout.map(to_std_timeout),
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
        match decide_post_attempt(
            &claim,
            outcome,
            demand.source,
            schedule.retry_max,
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
                let attempts = terminal_claim.attempts;
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
    now: DateTime<Utc>,
) -> Result<(), TickError> {
    for (key, claim) in store.list_schedule_claims()? {
        if !matches!(claim.state, ClaimState::Submitted) {
            continue;
        }
        let Some((pipeline, source)) = parse_claim_key(&key) else {
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
        let _ = store.schedule_claim_cas(
            &key,
            Some(&claim),
            &terminal,
            &pipeline,
            &ScheduleStateMutation::None,
        )?;
    }
    Ok(())
}

/// Parse a claim key `<pipeline>\u{1f}<source>\u{1f}<logical_ts>` back into its
/// pipeline and source. Returns `None` for an unrecognized shape — the sweep
/// skips a key it cannot interpret rather than acting on it.
fn parse_claim_key(key: &str) -> Option<(String, DemandKind)> {
    let mut parts = key.split('\u{1f}');
    let pipeline = parts.next()?.to_string();
    let source = match parts.next()? {
        "cron" => DemandKind::Cron,
        "after" => DemandKind::After,
        "freshness" => DemandKind::Freshness,
        "webhook" => DemandKind::Webhook,
        _ => return None,
    };
    Some((pipeline, source))
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
    now: DateTime<Utc>,
) -> Result<StuckResolution, TickError> {
    let outcome = store
        .find_terminal_run_by_submission_id(&current.submission_id)?
        .and_then(|run| run.status.terminal_outcome());
    let Some(outcome) = outcome else {
        return resolve_missing_record(key, demand, schedule, current, store, now);
    };
    let resolved = decide_resolver(current, outcome, demand.source, schedule.retry_max, now);
    commit_resolution(key, &demand.pipeline, current, resolved, store)
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
            commit_resolution(key, &demand.pipeline, current, resolved, store)
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
            traceparent: None,
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
        let key = claim_key(pipeline, source, logical_ts);
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
    }
}
