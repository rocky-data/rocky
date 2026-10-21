//! Wait for a running source sync to finish (`[...discovery.sync] wait = true`).
//!
//! Read-only: the loop only asks the source adapter for connector sync state
//! ([`DiscoveryAdapter::source_sync_states`]). It never starts a sync.
//!
//! Which sync did a connector finish? A source vendor may give no sync id and
//! no sync start time. So the loop records each connector's `succeeded_at` and
//! `failed_at` from the poll that first saw it syncing. A later poll that sees
//! the connector idle and a timestamp that moved past that record has the
//! outcome: `succeeded_at` moved is success, `failed_at` moved is failure, and
//! when both moved the later one wins. Both values come from the same API, so
//! no clock on this machine takes part in the comparison.

use std::future::Future;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::Serialize;
use tracing::warn;

use crate::traits::{AdapterResult, DiscoveryAdapter};

/// Idle polls without a timestamp change that the loop tolerates after a sync
/// ends. The vendor may flip the sync state a moment before it updates
/// `succeeded_at` / `failed_at`.
const SETTLE_POLLS: u32 = 3;

/// Consecutive failed status polls that end the wait with an error.
const MAX_CONSECUTIVE_POLL_ERRORS: u32 = 3;

/// Sync state of one source connector at one poll.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceSyncState {
    pub id: String,
    pub schema: String,
    pub source_type: String,
    /// A sync is running right now.
    pub syncing: bool,
    /// The connector is paused (no sync runs on its own).
    pub paused: bool,
    pub succeeded_at: Option<DateTime<Utc>>,
    pub failed_at: Option<DateTime<Utc>>,
}

/// How the wait for one connector ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SourceSyncOutcome {
    /// The sync ended and `succeeded_at` moved.
    Succeeded,
    /// The sync ended and `failed_at` moved.
    Failed,
    /// The connector was still syncing when the wait ran out.
    Timeout,
    /// The sync ended but neither timestamp moved, or the connector vanished.
    Unknown,
    /// A shutdown signal ended the wait first.
    Interrupted,
}

impl SourceSyncOutcome {
    /// The `snake_case` name used in the run JSON.
    pub fn as_str(self) -> &'static str {
        match self {
            SourceSyncOutcome::Succeeded => "succeeded",
            SourceSyncOutcome::Failed => "failed",
            SourceSyncOutcome::Timeout => "timeout",
            SourceSyncOutcome::Unknown => "unknown",
            SourceSyncOutcome::Interrupted => "interrupted",
        }
    }

    /// `true` for every outcome except [`SourceSyncOutcome::Succeeded`].
    /// `on_sync_failure` decides what a bad outcome does.
    pub fn is_bad(self) -> bool {
        !matches!(self, SourceSyncOutcome::Succeeded)
    }
}

/// Result for one connector that was syncing when the wait began.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceSyncResult {
    pub id: String,
    pub schema: String,
    pub outcome: SourceSyncOutcome,
    /// Whole seconds from the start of the wait to this outcome.
    pub waited_secs: u64,
    /// The connector's `succeeded_at` after the wait.
    pub last_sync_at: Option<DateTime<Utc>>,
}

/// Timing for [`wait_for_source_syncs`].
#[derive(Debug, Clone, Copy)]
pub struct SourceSyncWaitOptions {
    /// Longest wait for the whole phase.
    pub timeout: Duration,
    /// Pause between status polls.
    pub poll_interval: Duration,
}

/// Outcome of the wait phase.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SourceSyncReport {
    /// One row per connector that was syncing at the start. Empty when
    /// nothing was running.
    pub syncs: Vec<SourceSyncResult>,
    /// The wait ended on the cancellation future.
    pub interrupted: bool,
}

impl SourceSyncReport {
    /// Results whose outcome is not a success.
    pub fn bad(&self) -> impl Iterator<Item = &SourceSyncResult> {
        self.syncs.iter().filter(|r| r.outcome.is_bad())
    }
}

enum Phase {
    Waiting,
    /// Idle with no timestamp change; counts the polls seen so far.
    Settling(u32),
    Done(SourceSyncOutcome),
}

struct Tracked {
    id: String,
    schema: String,
    base_succeeded: Option<DateTime<Utc>>,
    base_failed: Option<DateTime<Utc>>,
    last_sync_at: Option<DateTime<Utc>>,
    phase: Phase,
    waited_secs: u64,
}

fn moved(now: Option<DateTime<Utc>>, base: Option<DateTime<Utc>>) -> bool {
    match (now, base) {
        (Some(n), Some(b)) => n > b,
        (Some(_), None) => true,
        (None, _) => false,
    }
}

/// Wait until every connector that is syncing now has stopped syncing.
///
/// `include` limits the wait to the connectors the run will read (its
/// `--filter`). Paused connectors are never waited for.
///
/// Returns `Ok(None)` when the adapter does not report sync state. Returns
/// `Err` when the first poll fails, or when status polls fail
/// [`MAX_CONSECUTIVE_POLL_ERRORS`] times in a row.
///
/// `cancel` is polled together with the poll sleep. When it completes the
/// loop stops at once and marks every open connector `Interrupted`.
pub async fn wait_for_source_syncs(
    adapter: &dyn DiscoveryAdapter,
    schema_prefix: &str,
    include: &(dyn Fn(&SourceSyncState) -> bool + Sync),
    opts: SourceSyncWaitOptions,
    cancel: impl Future<Output = ()>,
) -> AdapterResult<Option<SourceSyncReport>> {
    let Some(first) = adapter.source_sync_states(schema_prefix).await? else {
        return Ok(None);
    };
    let started = Instant::now();
    let mut tracked: Vec<Tracked> = first
        .iter()
        .filter(|s| s.syncing && !s.paused && include(s))
        .map(|s| Tracked {
            id: s.id.clone(),
            schema: s.schema.clone(),
            base_succeeded: s.succeeded_at,
            base_failed: s.failed_at,
            last_sync_at: s.succeeded_at,
            phase: Phase::Waiting,
            waited_secs: 0,
        })
        .collect();

    let mut interrupted = false;
    let mut errors_in_a_row = 0u32;
    tokio::pin!(cancel);

    while tracked
        .iter()
        .any(|t| matches!(t.phase, Phase::Waiting | Phase::Settling(_)))
    {
        let elapsed = started.elapsed();
        if elapsed >= opts.timeout {
            for t in open(&mut tracked) {
                t.phase = Phase::Done(SourceSyncOutcome::Timeout);
                t.waited_secs = elapsed.as_secs();
            }
            break;
        }
        let pause = opts.poll_interval.min(opts.timeout - elapsed);
        tokio::select! {
            () = &mut cancel => {
                let elapsed = started.elapsed();
                for t in open(&mut tracked) {
                    t.phase = Phase::Done(SourceSyncOutcome::Interrupted);
                    t.waited_secs = elapsed.as_secs();
                }
                interrupted = true;
                break;
            }
            () = tokio::time::sleep(pause) => {}
        }

        let states = match adapter.source_sync_states(schema_prefix).await {
            Ok(Some(states)) => {
                errors_in_a_row = 0;
                states
            }
            Ok(None) => break,
            Err(e) => {
                errors_in_a_row += 1;
                if errors_in_a_row >= MAX_CONSECUTIVE_POLL_ERRORS {
                    return Err(e);
                }
                warn!(error = %e, "source sync status poll failed; will poll again");
                continue;
            }
        };
        let waited = started.elapsed().as_secs();
        for t in open(&mut tracked) {
            let Some(state) = states.iter().find(|s| s.id == t.id) else {
                t.phase = Phase::Done(SourceSyncOutcome::Unknown);
                t.waited_secs = waited;
                continue;
            };
            t.last_sync_at = state.succeeded_at;
            if state.syncing {
                t.phase = Phase::Waiting;
                continue;
            }
            let ok = moved(state.succeeded_at, t.base_succeeded);
            let bad = moved(state.failed_at, t.base_failed);
            let outcome = match (ok, bad) {
                (true, false) => Some(SourceSyncOutcome::Succeeded),
                (false, true) => Some(SourceSyncOutcome::Failed),
                (true, true) => Some(if state.failed_at > state.succeeded_at {
                    SourceSyncOutcome::Failed
                } else {
                    SourceSyncOutcome::Succeeded
                }),
                (false, false) => None,
            };
            match outcome {
                Some(o) => {
                    t.phase = Phase::Done(o);
                    t.waited_secs = waited;
                }
                None => {
                    let seen = match t.phase {
                        Phase::Settling(n) => n + 1,
                        _ => 1,
                    };
                    if seen > SETTLE_POLLS {
                        t.phase = Phase::Done(SourceSyncOutcome::Unknown);
                        t.waited_secs = waited;
                    } else {
                        t.phase = Phase::Settling(seen);
                    }
                }
            }
        }
    }

    let syncs = tracked
        .into_iter()
        .map(|t| SourceSyncResult {
            outcome: match t.phase {
                Phase::Done(o) => o,
                // The loop leaves only through a `Done` marking, or through
                // `Ok(None)`, where the adapter stopped answering.
                Phase::Waiting | Phase::Settling(_) => SourceSyncOutcome::Unknown,
            },
            id: t.id,
            schema: t.schema,
            waited_secs: t.waited_secs,
            last_sync_at: t.last_sync_at,
        })
        .collect();
    Ok(Some(SourceSyncReport { syncs, interrupted }))
}

fn open(tracked: &mut [Tracked]) -> impl Iterator<Item = &mut Tracked> {
    tracked
        .iter_mut()
        .filter(|t| matches!(t.phase, Phase::Waiting | Phase::Settling(_)))
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use chrono::TimeZone;

    use super::*;
    use crate::source::DiscoveryResult;
    use crate::traits::AdapterError;

    /// Replays a script of poll answers; the last answer repeats.
    struct Scripted {
        polls: Mutex<VecDeque<Vec<SourceSyncState>>>,
        calls: Mutex<u32>,
    }

    impl Scripted {
        fn new(polls: Vec<Vec<SourceSyncState>>) -> Self {
            Self {
                polls: Mutex::new(polls.into()),
                calls: Mutex::new(0),
            }
        }
        fn calls(&self) -> u32 {
            *self.calls.lock().unwrap()
        }
    }

    #[async_trait]
    impl DiscoveryAdapter for Scripted {
        async fn discover(&self, _p: &str) -> AdapterResult<DiscoveryResult> {
            Ok(DiscoveryResult::ok(vec![]))
        }
        async fn source_sync_states(
            &self,
            _p: &str,
        ) -> AdapterResult<Option<Vec<SourceSyncState>>> {
            *self.calls.lock().unwrap() += 1;
            let mut q = self.polls.lock().unwrap();
            if q.len() > 1 {
                Ok(q.pop_front())
            } else {
                Ok(q.front().cloned())
            }
        }
    }

    fn at(secs: i64) -> Option<DateTime<Utc>> {
        Some(Utc.timestamp_opt(1_800_000_000 + secs, 0).unwrap())
    }

    fn state(
        id: &str,
        syncing: bool,
        ok: Option<DateTime<Utc>>,
        bad: Option<DateTime<Utc>>,
    ) -> SourceSyncState {
        SourceSyncState {
            id: id.into(),
            schema: format!("src__{id}"),
            source_type: "shopify".into(),
            syncing,
            paused: false,
            succeeded_at: ok,
            failed_at: bad,
        }
    }

    fn opts(timeout_ms: u64) -> SourceSyncWaitOptions {
        SourceSyncWaitOptions {
            timeout: Duration::from_millis(timeout_ms),
            poll_interval: Duration::from_millis(5),
        }
    }

    fn all(_: &SourceSyncState) -> bool {
        true
    }

    async fn run(adapter: &Scripted, o: SourceSyncWaitOptions) -> SourceSyncReport {
        wait_for_source_syncs(adapter, "src__", &all, o, std::future::pending())
            .await
            .unwrap()
            .unwrap()
    }

    #[tokio::test]
    async fn idle_connectors_cost_one_poll() {
        let a = Scripted::new(vec![vec![state("a", false, at(0), None)]]);
        let report = run(&a, opts(1000)).await;
        assert!(report.syncs.is_empty());
        assert_eq!(a.calls(), 1);
    }

    #[tokio::test]
    async fn adapter_without_sync_state_returns_none() {
        struct Plain;
        #[async_trait]
        impl DiscoveryAdapter for Plain {
            async fn discover(&self, _p: &str) -> AdapterResult<DiscoveryResult> {
                Ok(DiscoveryResult::ok(vec![]))
            }
        }
        let r = wait_for_source_syncs(&Plain, "", &all, opts(100), std::future::pending())
            .await
            .unwrap();
        assert!(r.is_none());
    }

    #[tokio::test]
    async fn sync_that_ends_with_new_succeeded_at_is_success() {
        let a = Scripted::new(vec![
            vec![state("a", true, at(0), None)],
            vec![state("a", true, at(0), None)],
            vec![state("a", false, at(60), None)],
        ]);
        let report = run(&a, opts(2000)).await;
        assert_eq!(report.syncs.len(), 1);
        assert_eq!(report.syncs[0].outcome, SourceSyncOutcome::Succeeded);
        assert_eq!(report.syncs[0].last_sync_at, at(60));
        assert_eq!(a.calls(), 3);
    }

    #[tokio::test]
    async fn sync_that_ends_with_new_failed_at_is_failure() {
        let a = Scripted::new(vec![
            vec![state("a", true, at(0), None)],
            vec![state("a", false, at(0), at(60))],
        ]);
        let report = run(&a, opts(2000)).await;
        assert_eq!(report.syncs[0].outcome, SourceSyncOutcome::Failed);
    }

    #[tokio::test]
    async fn when_both_timestamps_move_the_later_one_wins() {
        let a = Scripted::new(vec![
            vec![state("a", true, at(0), at(0))],
            vec![state("a", false, at(50), at(60))],
        ]);
        assert_eq!(
            run(&a, opts(2000)).await.syncs[0].outcome,
            SourceSyncOutcome::Failed
        );
        let b = Scripted::new(vec![
            vec![state("a", true, at(0), at(0))],
            vec![state("a", false, at(70), at(60))],
        ]);
        assert_eq!(
            run(&b, opts(2000)).await.syncs[0].outcome,
            SourceSyncOutcome::Succeeded
        );
    }

    #[tokio::test]
    async fn an_old_failed_at_does_not_fail_a_new_success() {
        let a = Scripted::new(vec![
            vec![state("a", true, at(0), at(-500))],
            vec![state("a", false, at(60), at(-500))],
        ]);
        assert_eq!(
            run(&a, opts(2000)).await.syncs[0].outcome,
            SourceSyncOutcome::Succeeded
        );
    }

    #[tokio::test]
    async fn timestamp_lag_is_tolerated_for_settle_polls() {
        // Idle with no change for 2 polls, then succeeded_at moves.
        let a = Scripted::new(vec![
            vec![state("a", true, at(0), None)],
            vec![state("a", false, at(0), None)],
            vec![state("a", false, at(0), None)],
            vec![state("a", false, at(60), None)],
        ]);
        assert_eq!(
            run(&a, opts(2000)).await.syncs[0].outcome,
            SourceSyncOutcome::Succeeded
        );
    }

    #[tokio::test]
    async fn idle_with_no_timestamp_change_is_unknown() {
        let a = Scripted::new(vec![
            vec![state("a", true, at(0), None)],
            vec![state("a", false, at(0), None)],
        ]);
        let report = run(&a, opts(5000)).await;
        assert_eq!(report.syncs[0].outcome, SourceSyncOutcome::Unknown);
        // 1 first poll + the idle polls: the first sets Settling(1), the
        // fourth exceeds SETTLE_POLLS.
        assert_eq!(a.calls(), 1 + SETTLE_POLLS + 1);
    }

    #[tokio::test]
    async fn still_syncing_at_the_deadline_is_timeout() {
        let a = Scripted::new(vec![vec![state("a", true, at(0), None)]]);
        let report = run(&a, opts(60)).await;
        assert_eq!(report.syncs[0].outcome, SourceSyncOutcome::Timeout);
        assert!(!report.interrupted);
    }

    #[tokio::test]
    async fn timeout_does_not_overshoot_by_a_poll_interval() {
        let a = Scripted::new(vec![vec![state("a", true, at(0), None)]]);
        let o = SourceSyncWaitOptions {
            timeout: Duration::from_millis(100),
            poll_interval: Duration::from_secs(30),
        };
        let t = Instant::now();
        let report = run(&a, o).await;
        assert!(t.elapsed() < Duration::from_secs(5), "{:?}", t.elapsed());
        assert_eq!(report.syncs[0].outcome, SourceSyncOutcome::Timeout);
    }

    #[tokio::test]
    async fn cancel_interrupts_the_wait() {
        let a = Scripted::new(vec![vec![state("a", true, at(0), None)]]);
        let o = SourceSyncWaitOptions {
            timeout: Duration::from_secs(3600),
            poll_interval: Duration::from_secs(30),
        };
        let cancel = async { tokio::time::sleep(Duration::from_millis(30)).await };
        let t = Instant::now();
        let report = wait_for_source_syncs(&a, "", &all, o, cancel)
            .await
            .unwrap()
            .unwrap();
        assert!(t.elapsed() < Duration::from_secs(5));
        assert!(report.interrupted);
        assert_eq!(report.syncs[0].outcome, SourceSyncOutcome::Interrupted);
    }

    #[tokio::test]
    async fn paused_and_filtered_connectors_are_not_waited_for() {
        let mut paused = state("p", true, at(0), None);
        paused.paused = true;
        let a = Scripted::new(vec![vec![paused, state("skip", true, at(0), None)]]);
        let only_other = |s: &SourceSyncState| s.id != "skip";
        let report = wait_for_source_syncs(&a, "", &only_other, opts(100), std::future::pending())
            .await
            .unwrap()
            .unwrap();
        assert!(report.syncs.is_empty());
        assert_eq!(a.calls(), 1);
    }

    #[tokio::test]
    async fn a_vanished_connector_is_unknown() {
        let a = Scripted::new(vec![
            vec![state("a", true, at(0), None)],
            vec![state("other", false, at(0), None)],
        ]);
        assert_eq!(
            run(&a, opts(2000)).await.syncs[0].outcome,
            SourceSyncOutcome::Unknown
        );
    }

    #[tokio::test]
    async fn one_bad_poll_is_survived_three_in_a_row_fail() {
        struct Flaky(Mutex<u32>, u32);
        #[async_trait]
        impl DiscoveryAdapter for Flaky {
            async fn discover(&self, _p: &str) -> AdapterResult<DiscoveryResult> {
                Ok(DiscoveryResult::ok(vec![]))
            }
            async fn source_sync_states(
                &self,
                _p: &str,
            ) -> AdapterResult<Option<Vec<SourceSyncState>>> {
                let mut n = self.0.lock().unwrap();
                *n += 1;
                match *n {
                    1 => Ok(Some(vec![state("a", true, at(0), None)])),
                    k if k <= 1 + self.1 => Err(AdapterError::msg("boom")),
                    _ => Ok(Some(vec![state("a", false, at(60), None)])),
                }
            }
        }
        let ok = wait_for_source_syncs(
            &Flaky(Mutex::new(0), 2),
            "",
            &all,
            opts(2000),
            std::future::pending(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(ok.syncs[0].outcome, SourceSyncOutcome::Succeeded);
        let err = wait_for_source_syncs(
            &Flaky(Mutex::new(0), 3),
            "",
            &all,
            opts(2000),
            std::future::pending(),
        )
        .await;
        assert!(err.is_err());
    }
}
