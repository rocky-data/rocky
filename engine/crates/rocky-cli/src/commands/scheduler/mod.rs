//! `rocky serve --scheduler` — the resident reconciler loop.
//!
//! A single tokio task runs `tick_once` on a clock-driven cadence: every
//! `poll_interval` it re-reads the config, evaluates every scheduled pipeline's
//! standing demand, and runs what is due — the same reconciler core as
//! `rocky tick`, re-hosted inside a long-lived process. The only genuinely new
//! risk class over `rocky tick` is the resident timer, so the loop is built to
//! be deterministically testable (an injected [`Clock`]) and to fail safe:
//!
//! - a config parse error on one iteration is logged and skipped — never fatal,
//!   never runs stale demands;
//! - runs go through the persisted jobs model ([`JobsModelSpawner`]) so
//!   `GET /api/v1/jobs/{id}` reports scheduler runs and a restart is honest;
//! - the app-level mutation permit is held for the tick so scheduler runs and
//!   API `run`/`apply` jobs coordinate with a clean `409` rather than colliding
//!   on the redb flock;
//! - on shutdown the loop drains: it stops evaluating new demands and gives a
//!   running child `drain_timeout` to finish before terminating it.
//!
//! Correctness against a second reconciler (another `serve --scheduler`, or a
//! cron-driven `rocky tick`) is inherited whole from `tick_once`: the tick lock,
//! its wedge override, and the claim state machine (O1-DESIGN §5).

pub mod clock;
pub mod spawner;

pub use clock::{Clock, TokioClock};
pub use spawner::JobsModelSpawner;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use rocky_core::config::load_rocky_config;
use rocky_core::schedule::{Drain, Spawner, TickOptions, TickReport, tick_once};
use rocky_server::state::ServerState;

use crate::api::state_path_for;
use crate::commands::tick::build_member_budgets;

/// The default poll interval between ticks (Hugo-overridable, O1-DESIGN §10).
pub const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(15);

/// The default drain grace on shutdown.
pub const DEFAULT_DRAIN_TIMEOUT: Duration = Duration::from_secs(60);

/// The mutation-permit holder id while a scheduler tick is mutating. A sentinel:
/// individual runs are recorded as jobs keyed by their `submission_id`; this id
/// only ever surfaces as the `running_job_id` of a `409 mutation_in_progress`
/// returned to an API `run`/`apply` submitted mid-tick.
const SCHEDULER_PERMIT_HOLDER: &str = "scheduler";

/// Tunables for the resident scheduler loop.
#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    /// How long the loop sleeps between ticks.
    pub poll_interval: Duration,
    /// How long a running child may keep going after shutdown before it is
    /// terminated (the drain grace).
    pub drain_timeout: Duration,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            poll_interval: DEFAULT_POLL_INTERVAL,
            drain_timeout: DEFAULT_DRAIN_TIMEOUT,
        }
    }
}

/// The injected-clock millis of the loop's most recent iteration start, for the
/// self-watchdog. A wedged loop stops updating this; the supervisor notices.
fn now_millis(clock: &dyn Clock) -> u64 {
    clock.now().timestamp_millis().max(0) as u64
}

/// Spawn the resident reconciler as a background task with the production
/// [`TokioClock`], returning its handle. The handle resolves when the loop stops
/// — after `shutdown` is raised and the in-flight tick has drained.
pub fn spawn_scheduler(
    state: Arc<ServerState>,
    config_path: PathBuf,
    sched: SchedulerConfig,
    shutdown: Drain,
) -> tokio::task::JoinHandle<()> {
    let clock: Arc<dyn Clock> = Arc::new(TokioClock);
    let heartbeat = Arc::new(AtomicU64::new(now_millis(&*clock)));
    // Runs go through the persisted jobs model; the spawner shares the shutdown
    // signal so a running child drains on shutdown.
    let spawner: Arc<dyn Spawner> = Arc::new(JobsModelSpawner::new(
        Arc::clone(&state),
        shutdown.clone(),
        sched.drain_timeout,
    ));
    tokio::spawn(run_scheduler(
        state,
        config_path,
        sched,
        clock,
        shutdown,
        heartbeat,
        spawner,
    ))
}

/// Run the resident reconciler until `shutdown` is raised.
///
/// `heartbeat` is stamped with the injected-clock time at the top of every
/// iteration; the self-watchdog reads it to detect a wedged loop. `shutdown` is
/// shared with the HTTP server's graceful-shutdown future and doubles as the
/// drain signal handed to `tick_once` and the spawner.
#[allow(clippy::too_many_arguments)]
pub async fn run_scheduler(
    state: Arc<ServerState>,
    config_path: PathBuf,
    sched: SchedulerConfig,
    clock: Arc<dyn Clock>,
    shutdown: Drain,
    heartbeat: Arc<AtomicU64>,
    spawner: Arc<dyn Spawner>,
) {
    // The `.rocky` dir (tick lock; and, in PR 2, the webhook spool) is anchored
    // to the config file's directory — the project root — not the process cwd,
    // so a `serve --scheduler` and a cron `rocky tick` launched from different
    // cwds contend on the same lock.
    let rocky_dir = config_path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .join(".rocky");
    let state_path = std::path::absolute(state_path_for(&state))
        .unwrap_or_else(|_| state_path_for(&state));

    tracing::info!(
        poll_interval_s = sched.poll_interval.as_secs(),
        config = %config_path.display(),
        "scheduler loop started",
    );

    loop {
        if shutdown.is_signalled() {
            break;
        }
        heartbeat.store(now_millis(&*clock), Ordering::Relaxed);

        let drained = run_one_tick(
            &state,
            &config_path,
            &state_path,
            &rocky_dir,
            &*clock,
            &shutdown,
            &*spawner,
        )
        .await;

        // A drain observed mid-tick (or a shutdown that landed since) ends the
        // loop; the running child, if any, was already drained by the spawner.
        if drained || shutdown.is_signalled() {
            break;
        }

        tokio::select! {
            biased;
            () = shutdown.signalled() => break,
            () = clock.sleep(sched.poll_interval) => {}
        }
    }

    tracing::info!("scheduler loop stopped");
}

/// One reconciliation pass. Returns `true` iff the tick drained mid-pass
/// (shutdown), signalling the loop to stop.
///
/// Every failure mode here is non-fatal to the loop: a config parse error, a
/// permit held by an API job, or a tick fault each skip this iteration and the
/// loop lives on. The reconciler never runs stale demands — the config is
/// re-read fresh every iteration (F11 §4.7).
#[allow(clippy::too_many_arguments)]
async fn run_one_tick(
    state: &Arc<ServerState>,
    config_path: &Path,
    state_path: &Path,
    rocky_dir: &Path,
    clock: &dyn Clock,
    shutdown: &Drain,
    spawner: &dyn Spawner,
) -> bool {
    // Re-read config each iteration. A parse error skips THIS iteration only —
    // the loop must never die on a bad edit, and must never run stale demands.
    let config = match load_rocky_config(config_path) {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(
                error = %e,
                config = %config_path.display(),
                "scheduler: config error this iteration; skipping (loop stays alive)",
            );
            return false;
        }
    };

    // Option C: hold the app-level mutation permit for the whole tick, so a
    // scheduler run and an API `run`/`apply` never collide on the redb flock —
    // whichever is second gets a clean `409`/skip. Held by an API job ⇒ skip this
    // tick; the demands re-derive next tick (no claim is taken, nothing burned).
    let _permit = match state.mutation_permit.try_acquire(SCHEDULER_PERMIT_HOLDER) {
        Ok(guard) => guard,
        Err(holder) => {
            tracing::debug!(
                running_job_id = %holder,
                "scheduler: a mutating job holds the permit; skipping this tick",
            );
            return false;
        }
    };

    let member_budgets = build_member_budgets(&config, config_path, None);
    let opts = TickOptions {
        dry_run: false,
        pipeline_filter: None,
        config_path: config_path.to_path_buf(),
        rocky_dir: rocky_dir.to_path_buf(),
        // A connected `scheduler.tick → run` trace is wired with the telemetry
        // step; today each child's run trace stands alone.
        traceparent: None,
        member_budgets,
        state_path: state_path.to_path_buf(),
        // The shutdown signal doubles as the tick's drain: raised mid-tick, the
        // reconciler stops evaluating and the spawner drains the running child.
        drain: shutdown.clone(),
    };

    let now = clock.now();
    match tick_once(&config, state_path, now, spawner, &opts).await {
        Ok(report) => {
            log_tick(&report);
            report.drained
        }
        Err(e) => {
            // A tick fault (state I/O, an `after` cycle) fails THIS tick closed;
            // the loop continues and the next tick re-evaluates.
            tracing::error!(error = %e, "scheduler: tick faulted; loop continues");
            false
        }
    }
}

/// Emit a structured summary of a completed tick. (Real OTel instruments are
/// added with the telemetry step; the fields here are the metric surface.)
fn log_tick(report: &TickReport) {
    if report.skipped_whole_tick {
        tracing::debug!("scheduler: tick skipped (another reconciler holds the lock)");
        return;
    }
    let executed = report.executed.len();
    let due = report.evaluated.iter().filter(|e| e.due.is_some()).count();
    let failed = report
        .executed
        .iter()
        .filter(|e| e.outcome != rocky_core::schedule::TerminalOutcome::Success)
        .count();
    tracing::info!(
        due,
        executed,
        failed,
        skipped = report.skipped.len(),
        lock_overridden = report.lock_overridden,
        state_busy = report.state_busy,
        drained = report.drained,
        "scheduler: tick complete",
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Mutex;

    use async_trait::async_trait;
    use chrono::{DateTime, Duration as ChronoDuration, TimeZone, Utc};
    use rocky_core::schedule::CapturingSpawner;
    use rocky_server::state::ServerState;
    use tokio::sync::Notify;

    /// A cron pipeline that is due every minute — so, at a 60s poll, each tick
    /// after the first-sight anchor fires exactly one occurrence.
    const CRON_EVERY_MINUTE: &str = r#"
[adapter.db]
type = "duckdb"

[pipeline.raw]
type = "transformation"
[pipeline.raw.target]
adapter = "db"
[pipeline.raw.schedule]
cron = "* * * * *"
"#;

    /// A deterministic clock: `now()` reads a manually-advanced instant, and
    /// `sleep(d)` parks until `advance` moves the clock past its wake time.
    ///
    /// `park_count` is a MONOTONIC count of sleeps entered — one per completed
    /// tick, since each tick is followed by the poll sleep. A test drives the
    /// loop N steps by `advance`-ing then awaiting `wait_for_park(N)`: because
    /// the count only reaches N once the Nth sleep has begun (i.e. the Nth tick
    /// finished), the test never races the loop or reads a stale earlier park.
    struct FakeClock {
        now: Mutex<DateTime<Utc>>,
        /// Notified on every `advance`, to wake parked sleepers.
        wake: Notify,
        /// Notified each time a sleep is entered, so a test can await a target
        /// park count.
        parked: Notify,
        park_count: AtomicU64,
    }

    impl FakeClock {
        fn new(start: DateTime<Utc>) -> Arc<Self> {
            Arc::new(Self {
                now: Mutex::new(start),
                wake: Notify::new(),
                parked: Notify::new(),
                park_count: AtomicU64::new(0),
            })
        }

        fn advance(&self, secs: i64) {
            {
                let mut n = self.now.lock().unwrap();
                *n += ChronoDuration::seconds(secs);
            }
            self.wake.notify_waiters();
        }

        /// Resolve once the loop has entered at least `n` poll sleeps — i.e. has
        /// completed at least `n` ticks.
        async fn wait_for_park(&self, n: u64) {
            loop {
                let notified = self.parked.notified();
                if self.park_count.load(Ordering::Acquire) >= n {
                    return;
                }
                notified.await;
            }
        }
    }

    #[async_trait]
    impl Clock for FakeClock {
        fn now(&self) -> DateTime<Utc> {
            *self.now.lock().unwrap()
        }

        async fn sleep(&self, dur: Duration) {
            let target = self.now() + ChronoDuration::from_std(dur).unwrap();
            // Record the park (this tick is done) BEFORE waiting, so a waiter can
            // observe it, then block until the clock is advanced past `target`.
            self.park_count.fetch_add(1, Ordering::Release);
            self.parked.notify_waiters();
            loop {
                let notified = self.wake.notified();
                if self.now() >= target {
                    break;
                }
                notified.await;
            }
        }
    }

    /// A temp project whose `rocky.toml` holds `config`. Returns the dir (kept
    /// alive for the test) and the config path.
    fn temp_project(config: &str) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("rocky.toml");
        std::fs::write(&config_path, config).unwrap();
        (dir, config_path)
    }

    /// A config-less server state rooted at `models_dir` — enough for the
    /// mutation permit and state-path resolution the loop needs.
    fn test_state(models_dir: &Path) -> Arc<ServerState> {
        ServerState::with_auth(models_dir.to_path_buf(), None, None, None, Vec::new())
    }

    fn at(y: i32, mo: u32, d: u32, h: u32, mi: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, mo, d, h, mi, 0).unwrap()
    }

    /// The loop fires a due cron demand once per tick after the first-sight
    /// anchor, with no drift: N iterations ⇒ exactly N−1 fires (the first tick
    /// only anchors), each an occurrence of the every-minute schedule.
    #[tokio::test]
    async fn fires_due_cron_each_tick_without_drift() {
        let (dir, config_path) = temp_project(CRON_EVERY_MINUTE);
        let state = test_state(dir.path());
        let clock = FakeClock::new(at(2026, 5, 2, 3, 0));
        let capture = Arc::new(CapturingSpawner::new(0));
        let shutdown = Drain::new();
        let heartbeat = Arc::new(AtomicU64::new(0));

        let clock_dyn: Arc<dyn Clock> = clock.clone();
        let spawner: Arc<dyn Spawner> = capture.clone();
        let task = tokio::spawn(run_scheduler(
            Arc::clone(&state),
            config_path,
            SchedulerConfig {
                poll_interval: Duration::from_secs(60),
                drain_timeout: Duration::from_secs(60),
            },
            clock_dyn,
            shutdown.clone(),
            heartbeat,
            spawner,
        ));

        // Iteration 1 (now = 03:00): first sight ⇒ anchor, no fire.
        clock.wait_for_park(1).await;
        assert_eq!(capture.run_count(), 0, "first sight must not fire");

        // Nine more ticks, one minute apart ⇒ nine fires, one per occurrence.
        for i in 1..=9u64 {
            clock.advance(60);
            clock.wait_for_park(i + 1).await;
            assert_eq!(
                capture.run_count(),
                i as usize,
                "tick {i} fires exactly one occurrence (no drift, no double)",
            );
        }

        shutdown.signal();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("loop exits promptly on shutdown")
            .unwrap();

        assert_eq!(capture.runs_for("raw").len(), 9);
    }

    /// A config parse error on one iteration skips that tick and leaves the loop
    /// alive: after the config is repaired, the next tick fires normally.
    #[tokio::test]
    async fn config_error_iteration_skips_but_loop_survives() {
        let (dir, config_path) = temp_project(CRON_EVERY_MINUTE);
        let state = test_state(dir.path());
        let clock = FakeClock::new(at(2026, 5, 2, 3, 0));
        let capture = Arc::new(CapturingSpawner::new(0));
        let shutdown = Drain::new();
        let heartbeat = Arc::new(AtomicU64::new(0));

        let clock_dyn: Arc<dyn Clock> = clock.clone();
        let spawner: Arc<dyn Spawner> = capture.clone();
        let task = tokio::spawn(run_scheduler(
            Arc::clone(&state),
            config_path.clone(),
            SchedulerConfig {
                poll_interval: Duration::from_secs(60),
                drain_timeout: Duration::from_secs(60),
            },
            clock_dyn,
            shutdown.clone(),
            heartbeat,
            spawner,
        ));

        // Tick 1 anchors; tick 2 fires once.
        clock.wait_for_park(1).await;
        clock.advance(60);
        clock.wait_for_park(2).await;
        assert_eq!(capture.run_count(), 1);

        // Corrupt the config, then tick: the iteration is skipped, no new fire,
        // and — crucially — the loop is still running (it parks again).
        std::fs::write(&config_path, "this is not valid toml : : :").unwrap();
        clock.advance(60);
        clock.wait_for_park(3).await;
        assert_eq!(capture.run_count(), 1, "a bad-config tick fires nothing");

        // Repair the config: the very next tick fires again — the loop lived.
        std::fs::write(&config_path, CRON_EVERY_MINUTE).unwrap();
        clock.advance(60);
        clock.wait_for_park(4).await;
        assert_eq!(capture.run_count(), 2, "loop recovered after config repair");

        shutdown.signal();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("loop exits promptly on shutdown")
            .unwrap();
    }

    /// A held mutation permit (an API `run`/`apply` in flight) makes the tick
    /// skip cleanly — no run, no burned claim — and the loop keeps ticking; once
    /// the permit frees, the next tick fires.
    #[tokio::test]
    async fn mutation_permit_held_skips_tick() {
        let (dir, config_path) = temp_project(CRON_EVERY_MINUTE);
        let state = test_state(dir.path());
        let clock = FakeClock::new(at(2026, 5, 2, 3, 0));
        let capture = Arc::new(CapturingSpawner::new(0));
        let shutdown = Drain::new();
        let heartbeat = Arc::new(AtomicU64::new(0));

        let clock_dyn: Arc<dyn Clock> = clock.clone();
        let spawner: Arc<dyn Spawner> = capture.clone();
        let task = tokio::spawn(run_scheduler(
            Arc::clone(&state),
            config_path,
            SchedulerConfig {
                poll_interval: Duration::from_secs(60),
                drain_timeout: Duration::from_secs(60),
            },
            clock_dyn,
            shutdown.clone(),
            heartbeat,
            spawner,
        ));

        // Anchor tick.
        clock.wait_for_park(1).await;

        // Simulate an API job holding the permit across the next tick.
        let guard = state
            .mutation_permit
            .try_acquire("api-job-1")
            .expect("permit free");
        clock.advance(60);
        clock.wait_for_park(2).await;
        assert_eq!(
            capture.run_count(),
            0,
            "a held permit skips the tick — no run, no burned claim",
        );

        // Release the permit: the next tick fires the (still-due) occurrence.
        drop(guard);
        clock.advance(60);
        clock.wait_for_park(3).await;
        assert_eq!(capture.run_count(), 1, "tick fires once the permit frees");

        shutdown.signal();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("loop exits promptly on shutdown")
            .unwrap();
    }
}
