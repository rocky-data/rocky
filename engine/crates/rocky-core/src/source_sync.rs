//! Wait for a running source sync to finish, and optionally start one
//! (`[...discovery.sync] wait = true`, `trigger = true`).
//!
//! Wait only ([`wait_for_source_syncs`]): the loop asks the source adapter
//! for connector sync state ([`DiscoveryAdapter::source_sync_states`]) and
//! never starts a sync.
//!
//! Which sync did a connector finish? A source vendor may give no sync id and
//! no sync start time. So the wait records each connector's `succeeded_at` and
//! `failed_at` from the poll that first saw it syncing. A later poll that sees
//! the connector idle and a timestamp that moved past that record has the
//! outcome: `succeeded_at` moved is success, `failed_at` moved is failure, and
//! when both moved the later one wins. Both values come from the same API, so
//! no clock on this machine takes part in the comparison.
//!
//! Trigger ([`trigger_and_wait_for_source_syncs`]): needs both opt-ins, the
//! adapter owner's and the pipeline's. Per connector:
//!
//! ```text
//!   paused ---------------------------------> outcome `paused`, no request
//!   syncing now --> wait for it to end (not counted) --+
//!   idle -----------------------------------------------+
//!                                                         v
//!        baseline = succeeded_at / failed_at of that idle read
//!        one POST (never `force`); T_pre = the later of its `Date` and the idle read's
//!        poll: syncing seen, then idle
//!          succeeded_at moved past the baseline AND > T_pre + 5 s --> succeeded
//!          failed_at    moved past the baseline AND > T_pre + 5 s --> failed
//!                                              (the later of the two wins)
//! ```
//!
//! Two tests must both pass before a timestamp counts as the end of the sync
//! Rocky started. The first is that it moved past the baseline: both values
//! come from the vendor API, so no clock takes part. The second is that it is
//! later than `T_pre` plus 5 seconds. `T_pre` is the later of the `Date`
//! header of the answer to the POST and the `Date` of the idle read, so a
//! scheduled sync that began and ended before the POST (for example while the
//! POST waited out a 429) is not counted. When neither has a `Date`, this
//! machine's clock stands in. The moved test still applies then, so a clock
//! difference alone can never make a sync look like ours.
//!
//! The 5 second margin covers the gap between the server clock behind `T_pre`
//! and the clock behind `succeeded_at`. It can reject a real sync that ends
//! within 5 seconds of the POST. Such a sync ends as `unknown`, at once when
//! its timestamp moved to a time after `T_pre` but inside the margin, and after
//! the settle polls when a poll saw it syncing first. A sync that never shows
//! and never moves ends as `timeout` after the full wait. All of these are failures: it fails closed.
//!
//! One window stays open. A sync that starts after the idle read and before
//! Rocky's POST is answered, and is still running then, is counted as
//! Rocky's: with `force = false` Fivetran starts no second sync, so the POST
//! joins the running one, and Rocky cannot tell the two apart. That window can
//! last minutes when earlier connectors hit 429 backoff.
//!
//! A request whose answer is unknown (a timeout, a 5xx) is treated as sent:
//! the row stays `triggered`, and the wait goes on like it does for a slow
//! start, so a sync that starts later is still waited out. One more read right
//! after that answer moves the baseline and `T_pre`, so a sync that ended
//! between the idle read and the request is not counted; if that read fails,
//! the connector ends `unknown`. If that read shows a sync running, that sync
//! is counted as Rocky's: the open window above, met on this path too. Rocky's
//! own sync, if it ends before that read, is missed and the row ends
//! `timeout` (safe, but slow). A 409 is taken as "the connector is busy": the
//! connector is waited for and the request is sent again when it is idle.
//! After a second 409 (counted over the whole run) no request is sent; the
//! connector ends `trigger_failed` after `SETTLE_POLLS` idle reads in a row,
//! because the status can lag behind the sync the 409 announced.

use std::future::Future;
use std::time::{Duration, Instant};

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use schemars::JsonSchema;
use serde::Serialize;
use thiserror::Error;
use tracing::warn;

use crate::traits::{AdapterResult, DiscoveryAdapter};

/// Idle polls without a timestamp change that the loop tolerates after a sync
/// ends. The vendor may flip the sync state a moment before it updates
/// `succeeded_at` / `failed_at`.
const SETTLE_POLLS: u32 = 3;

/// Consecutive failed status polls that end the wait with an error.
const MAX_CONSECUTIVE_POLL_ERRORS: u32 = 3;

/// Busy (409) answers after which a connector gets no more sync requests in
/// one run. It counts across the whole run, not in a row.
const MAX_CONFLICTS: u32 = 2;

/// How far past `T_pre` a timestamp has to be before it counts as the end of
/// the sync Rocky started. Covers the clock difference between the server time
/// behind `T_pre` and the clock behind the vendor's `succeeded_at`.
const IDENTITY_MARGIN_SECS: i64 = 5;

/// Whether a vendor timestamp shows an end after `t_pre`, with the margin.
fn ends_after(timestamp: Option<DateTime<Utc>>, t_pre: DateTime<Utc>) -> bool {
    timestamp.is_some_and(|t| t > t_pre + ChronoDuration::seconds(IDENTITY_MARGIN_SECS))
}

/// What the trigger path knows when it sends the request: the lower bound for
/// the end of the sync, and the timestamps of the read that showed the
/// connector idle.
#[derive(Debug, Clone, Copy)]
struct Identity {
    t_pre: DateTime<Utc>,
    base_succeeded: Option<DateTime<Utc>>,
    base_failed: Option<DateTime<Utc>>,
}

impl Identity {
    fn of(state: &SourceSyncState) -> Self {
        Self {
            t_pre: idle_seen_at(state),
            base_succeeded: state.succeeded_at,
            base_failed: state.failed_at,
        }
    }

    /// The single place the sync identity is decided: a timestamp is the end
    /// of the sync Rocky started only if it moved past the baseline (no clock
    /// involved) and is later than `t_pre` plus the margin.
    fn ended(&self, now: Option<DateTime<Utc>>, base: Option<DateTime<Utc>>) -> bool {
        moved(now, base) && ends_after(now, self.t_pre)
    }

    /// A timestamp moved past the baseline and landed after `t_pre` but not
    /// past the margin. The clocks cannot say whether that is the sync Rocky
    /// started, and it will not move again. A timestamp from before `t_pre` is
    /// a sync that ended before the request, so it does not count here.
    fn inside_margin(&self, state: &SourceSyncState) -> bool {
        let inside = |now: Option<DateTime<Utc>>, base: Option<DateTime<Utc>>| {
            moved(now, base) && now.is_some_and(|t| t > self.t_pre) && !ends_after(now, self.t_pre)
        };
        inside(state.succeeded_at, self.base_succeeded) || inside(state.failed_at, self.base_failed)
    }

    fn succeeded(&self, state: &SourceSyncState) -> bool {
        self.ended(state.succeeded_at, self.base_succeeded)
    }

    fn failed(&self, state: &SourceSyncState) -> bool {
        self.ended(state.failed_at, self.base_failed)
    }
}

/// Why the source refused or could not take a sync request.
#[derive(Debug, Error)]
pub enum SourceSyncTriggerError {
    /// The discovery adapter has no way to start a sync.
    #[error("this discovery adapter cannot start syncs")]
    Unsupported,
    /// The adapter's owner did not opt in (`allow_sync_trigger`).
    #[error("the adapter does not allow Rocky to start syncs: set allow_sync_trigger = true on it")]
    NotAllowed,
    /// The source refused the key (HTTP 403). The text names the fix.
    #[error("{0}")]
    Forbidden(String),
    /// The source refused the request, or it never went out (a 400 or 404, a
    /// 429 that outlasted the retries, a connect error). No sync started
    /// because of it, so the caller does not read the connector again.
    #[error("{0}")]
    Failed(String),
    /// The source answered that the connector is busy (HTTP 409). The vendor
    /// docs do not say whether that means "a sync is already running", so the
    /// caller takes the safe side: it treats the connector as syncing, waits
    /// for it to be idle, does not count that sync as its own, and asks once
    /// more. After a second busy answer it sends nothing more and ends the
    /// connector as `TriggerFailed` after several idle reads in a row.
    #[error("{0}")]
    Conflict(String),
    /// The request may have reached the source and its answer is unknown (a
    /// timeout, a server error, a connection lost mid-request). The caller
    /// treats it as sent. It reads the connector once more and moves the
    /// identity baseline and `t_pre` to that read, so a sync that ended before
    /// it is not counted. If that read fails, the connector ends `unknown`.
    #[error("{0}")]
    Unknown(String),
}

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
    /// The source server's clock when it answered this read (the HTTP `Date`
    /// header). `None` when the adapter cannot tell; the trigger then falls
    /// back to this machine's clock.
    pub observed_at: Option<DateTime<Utc>>,
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
    /// The status reads failed, so the wait was abandoned. The result's
    /// `detail` names the error.
    Error,
    /// The connector is paused, so no sync was started. Not a failure: a
    /// person paused it on purpose, and Rocky never unpauses.
    Paused,
    /// Rocky could not start the sync. The result's `detail` names the cause.
    TriggerFailed,
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
            SourceSyncOutcome::Error => "error",
            SourceSyncOutcome::Paused => "paused",
            SourceSyncOutcome::TriggerFailed => "trigger_failed",
        }
    }

    /// `true` for every outcome except [`SourceSyncOutcome::Succeeded`] and
    /// [`SourceSyncOutcome::Paused`]. `on_sync_failure` decides what a bad
    /// outcome does.
    pub fn is_bad(self) -> bool {
        !matches!(
            self,
            SourceSyncOutcome::Succeeded | SourceSyncOutcome::Paused
        )
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
    /// The error text behind an [`SourceSyncOutcome::Error`] or
    /// [`SourceSyncOutcome::TriggerFailed`] outcome.
    pub detail: Option<String>,
    /// Rocky sent the request that started this sync.
    pub triggered: bool,
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
    detail: Option<String>,
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
/// `Err` when the first poll fails (no connector is known yet). When status
/// polls fail [`MAX_CONSECUTIVE_POLL_ERRORS`] times in a row the wait is
/// abandoned, and every connector still open ends as
/// [`SourceSyncOutcome::Error`] with the error text in its `detail`, so the
/// report still names the connectors.
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
            detail: None,
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
                    let elapsed = started.elapsed().as_secs();
                    let text = e.to_string();
                    for t in open(&mut tracked) {
                        t.phase = Phase::Done(SourceSyncOutcome::Error);
                        t.waited_secs = elapsed;
                        t.detail = Some(text.clone());
                    }
                    break;
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
            detail: t.detail,
            triggered: false,
        })
        .collect();
    Ok(Some(SourceSyncReport { syncs, interrupted }))
}

/// Tells the caller that a sync request went out. Used for the run's event.
#[derive(Debug, Clone)]
pub struct SourceSyncTriggered {
    pub id: String,
    pub schema: String,
    /// `T_pre`: the server time at which the connector was last seen idle.
    pub idle_seen_at: DateTime<Utc>,
}

enum TriggerPhase {
    /// A sync was running at the first read. Wait for it to end; it is not
    /// counted as the sync Rocky asked for.
    AwaitIdle,
    /// Seen idle (see [`Identity`]). The request goes out next.
    Ready(Identity),
    /// The request went out.
    Running {
        id: Identity,
        /// A read has shown the connector syncing since the request.
        seen_syncing: bool,
        /// Idle reads with no new end past the identity test since a sync was
        /// seen.
        settle: u32,
    },
    Done(SourceSyncOutcome),
}

struct TriggerTracked {
    id: String,
    schema: String,
    phase: TriggerPhase,
    triggered: bool,
    last_sync_at: Option<DateTime<Utc>>,
    waited_secs: u64,
    detail: Option<String>,
    /// Busy (409) answers so far; see [`MAX_CONFLICTS`].
    conflicts: u32,
    /// Idle reads in a row after the last busy answer that stopped requests.
    idle_reads: u32,
}

impl TriggerTracked {
    fn is_open(&self) -> bool {
        !matches!(self.phase, TriggerPhase::Done(_))
    }

    fn finish(&mut self, outcome: SourceSyncOutcome, waited_secs: u64) {
        self.phase = TriggerPhase::Done(outcome);
        self.waited_secs = waited_secs;
    }
}

/// `T_pre` for a connector read as idle: the server's clock when it answered.
/// Without one, this machine's clock stands in (and a warning says so).
fn idle_seen_at(state: &SourceSyncState) -> DateTime<Utc> {
    state.observed_at.unwrap_or_else(|| {
        warn!(
            connector = %state.id,
            "the source sent no server time; using this machine's clock for the sync identity check"
        );
        Utc::now()
    })
}

/// After a request whose answer is unknown, read the connector once more.
/// `None` when the read fails or the connector is not in the answer.
async fn read_after_unknown(
    adapter: &dyn DiscoveryAdapter,
    schema_prefix: &str,
    id: &str,
) -> Option<SourceSyncState> {
    let Ok(Some(states)) = adapter.source_sync_states(schema_prefix).await else {
        return None;
    };
    states.into_iter().find(|s| s.id == id)
}

/// Start a sync on every selected connector and wait until each has ended.
///
/// Needs both opt-ins; the caller checks the pipeline's, and the adapter's
/// [`DiscoveryAdapter::trigger_source_sync`] checks its owner's. For each
/// connector `include` accepts:
///
/// - a paused connector gets the `Paused` outcome and no request;
/// - a connector that is syncing now is waited for first, and that sync is
///   not counted;
/// - once a read shows the connector idle, one request starts a sync, and the
///   sync ends when a timestamp moves past the idle read's value and past
///   `T_pre` plus the margin (see the module docs).
///
/// A refused key (403) stops further requests; the rest end as
/// `TriggerFailed`. A request the source refused
/// ([`SourceSyncTriggerError::Failed`]) ends as `TriggerFailed` and is never
/// counted as started, whatever a later read shows. A request with an unknown
/// answer ([`SourceSyncTriggerError::Unknown`]) may have been sent: the row
/// stays `triggered` and the wait goes on, so it ends `timeout` or `unknown`,
/// never `TriggerFailed`. A busy answer ([`SourceSyncTriggerError::Conflict`])
/// sends the connector back to waiting for idle. `on_trigger` runs right after
/// each request that was accepted or may have been sent.
///
/// Returns `Ok(None)` when the adapter does not report sync state, and `Err`
/// when the first read fails. Later read failures end the wait like
/// [`wait_for_source_syncs`] does. `cancel` ends the wait at once, also while
/// a request is in flight.
pub async fn trigger_and_wait_for_source_syncs(
    adapter: &dyn DiscoveryAdapter,
    schema_prefix: &str,
    include: &(dyn Fn(&SourceSyncState) -> bool + Sync),
    opts: SourceSyncWaitOptions,
    on_trigger: &(dyn Fn(&SourceSyncTriggered) + Sync),
    cancel: impl Future<Output = ()>,
) -> AdapterResult<Option<SourceSyncReport>> {
    let Some(first) = adapter.source_sync_states(schema_prefix).await? else {
        return Ok(None);
    };
    let started = Instant::now();
    let mut tracked: Vec<TriggerTracked> = first
        .iter()
        .filter(|s| include(s))
        .map(|s| TriggerTracked {
            id: s.id.clone(),
            schema: s.schema.clone(),
            phase: if s.paused {
                TriggerPhase::Done(SourceSyncOutcome::Paused)
            } else if s.syncing {
                TriggerPhase::AwaitIdle
            } else {
                TriggerPhase::Ready(Identity::of(s))
            },
            triggered: false,
            last_sync_at: s.succeeded_at,
            waited_secs: 0,
            detail: None,
            conflicts: 0,
            idle_reads: 0,
        })
        .collect();
    for t in tracked
        .iter()
        .filter(|t| matches!(t.phase, TriggerPhase::Done(SourceSyncOutcome::Paused)))
    {
        warn!(
            connector = %t.id,
            "connector is paused: no sync started (Rocky never unpauses); the run reads what it last landed"
        );
    }

    let mut interrupted = false;
    let mut errors_in_a_row = 0u32;
    let mut refused: Option<String> = None;
    tokio::pin!(cancel);

    loop {
        if !tracked.iter().any(TriggerTracked::is_open) {
            break;
        }
        let elapsed = started.elapsed();
        if elapsed >= opts.timeout {
            for t in tracked.iter_mut().filter(|t| t.is_open()) {
                t.finish(SourceSyncOutcome::Timeout, elapsed.as_secs());
            }
            break;
        }

        // Start the syncs that are ready.
        for t in &mut tracked {
            let TriggerPhase::Ready(identity) = t.phase else {
                continue;
            };
            let waited = started.elapsed().as_secs();
            if let Some(message) = &refused {
                t.detail = Some(format!("not started: {message}"));
                t.finish(SourceSyncOutcome::TriggerFailed, waited);
                continue;
            }
            let sent = tokio::select! {
                biased;
                () = &mut cancel => None,
                result = adapter.trigger_source_sync(&t.id) => Some(result),
            };
            let Some(result) = sent else {
                // The request was in flight when the signal came. It may have
                // reached the source, so the row says so and does not claim
                // that nothing was sent.
                t.triggered = true;
                t.detail = Some(
                    "interrupted while the sync request was in flight; the request may have \
                     been sent and a sync may be running"
                        .to_string(),
                );
                interrupted = true;
                break;
            };
            // `Some((seen_syncing, identity))` when a sync may be ours.
            let started_sync = match result {
                // The `Date` of the answer is the lower bound for the end of
                // the sync: the sync cannot have started before the request.
                // It never lowers the bound the idle read already set.
                Ok(post_date) => Some((
                    false,
                    Identity {
                        t_pre: post_date.map_or(identity.t_pre, |d| d.max(identity.t_pre)),
                        ..identity
                    },
                )),
                Err(SourceSyncTriggerError::Unknown(message)) => {
                    // A timeout or a server error: the request may have
                    // reached the source. Treat it as sent and keep waiting
                    // for a sync to start. The request went out after the idle
                    // read, so a sync may have ended in between (a scheduled
                    // one, during backoff): move the baseline and `t_pre` to
                    // one more read so that sync is not counted.
                    t.detail = Some(format!(
                        "{message}; the request may have been sent and a sync may still start"
                    ));
                    match read_after_unknown(adapter, schema_prefix, &t.id).await {
                        Some(state) => Some((
                            state.syncing,
                            Identity {
                                t_pre: idle_seen_at(&state).max(identity.t_pre),
                                base_succeeded: state.succeeded_at,
                                base_failed: state.failed_at,
                            },
                        )),
                        // Without that read nothing can tell Rocky's sync
                        // from one that ended before it: stop here. The row
                        // stays `triggered`, so the run does not read.
                        None => {
                            t.triggered = true;
                            on_trigger(&SourceSyncTriggered {
                                id: t.id.clone(),
                                schema: t.schema.clone(),
                                idle_seen_at: identity.t_pre,
                            });
                            t.detail = Some(format!(
                                "{message}; the request may have been sent and the connector \
                                 could not be read again"
                            ));
                            t.finish(SourceSyncOutcome::Unknown, waited);
                            continue;
                        }
                    }
                }
                // The connector is busy: wait it out and do not count that
                // sync. After the first busy answer Rocky asks once more;
                // after the second it sends nothing more, and the connector
                // ends `TriggerFailed` once a read shows it idle, so the run
                // never reads while that sync writes.
                Err(SourceSyncTriggerError::Conflict(message)) => {
                    t.conflicts += 1;
                    t.detail = Some(if t.conflicts >= MAX_CONFLICTS {
                        format!(
                            "{message}; busy answer {} times, no sync started; waiting for the \
                             connector to be idle",
                            t.conflicts
                        )
                    } else {
                        format!("{message}; the connector is treated as syncing and is waited for")
                    });
                    t.phase = TriggerPhase::AwaitIdle;
                    continue;
                }
                // A refusal: no sync started because of it. A sync that
                // Fivetran started on its own is not ours.
                Err(error) => {
                    if matches!(error, SourceSyncTriggerError::Forbidden(_)) {
                        refused = Some(error.to_string());
                    }
                    t.detail = Some(error.to_string());
                    None
                }
            };
            match started_sync {
                Some((seen_syncing, id)) => {
                    t.triggered = true;
                    t.phase = TriggerPhase::Running {
                        id,
                        seen_syncing,
                        settle: 0,
                    };
                    on_trigger(&SourceSyncTriggered {
                        id: t.id.clone(),
                        schema: t.schema.clone(),
                        idle_seen_at: id.t_pre,
                    });
                }
                None => t.finish(SourceSyncOutcome::TriggerFailed, waited),
            }
        }
        if interrupted {
            let elapsed = started.elapsed().as_secs();
            for t in tracked.iter_mut().filter(|t| t.is_open()) {
                t.finish(SourceSyncOutcome::Interrupted, elapsed);
            }
            break;
        }
        if !tracked.iter().any(TriggerTracked::is_open) {
            break;
        }

        let elapsed = started.elapsed();
        if elapsed >= opts.timeout {
            continue;
        }
        let pause = opts.poll_interval.min(opts.timeout - elapsed);
        tokio::select! {
            () = &mut cancel => {
                let elapsed = started.elapsed().as_secs();
                for t in tracked.iter_mut().filter(|t| t.is_open()) {
                    t.finish(SourceSyncOutcome::Interrupted, elapsed);
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
                    let elapsed = started.elapsed().as_secs();
                    let text = e.to_string();
                    for t in tracked.iter_mut().filter(|t| t.is_open()) {
                        t.finish(SourceSyncOutcome::Error, elapsed);
                        t.detail = Some(text.clone());
                    }
                    break;
                }
                warn!(error = %e, "source sync status poll failed; will poll again");
                continue;
            }
        };
        let waited = started.elapsed().as_secs();
        for t in tracked.iter_mut().filter(|t| t.is_open()) {
            let Some(state) = states.iter().find(|s| s.id == t.id) else {
                t.finish(SourceSyncOutcome::Unknown, waited);
                continue;
            };
            t.last_sync_at = state.succeeded_at;
            match t.phase {
                TriggerPhase::AwaitIdle => {
                    if state.paused {
                        t.finish(SourceSyncOutcome::Paused, waited);
                    } else if t.conflicts >= MAX_CONFLICTS {
                        // Busy twice: no more requests. The 409 says a sync
                        // probably runs, and the status can lag behind it, so
                        // only `SETTLE_POLLS` idle reads in a row end the
                        // connector; a syncing read starts the count again.
                        if state.syncing {
                            t.idle_reads = 0;
                        } else {
                            t.idle_reads += 1;
                            if t.idle_reads >= SETTLE_POLLS {
                                t.finish(SourceSyncOutcome::TriggerFailed, waited);
                            }
                        }
                    } else if !state.syncing {
                        t.detail = None;
                        t.phase = TriggerPhase::Ready(Identity::of(state));
                    }
                }
                TriggerPhase::Running {
                    id,
                    seen_syncing,
                    settle,
                } => {
                    if state.syncing {
                        t.phase = TriggerPhase::Running {
                            id,
                            seen_syncing: true,
                            settle: 0,
                        };
                        continue;
                    }
                    let ok = id.succeeded(state);
                    let bad = id.failed(state);
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
                            t.detail = None;
                            t.finish(o, waited);
                        }
                        // Idle with nothing past `t_pre`. Before any read has
                        // shown the sync, this is a slow start: keep waiting
                        // until the deadline. But a timestamp that already
                        // moved to a time after `t_pre`, yet not past the
                        // margin, is a sync that ended too soon to count and
                        // will not move again: end it now.
                        None if !seen_syncing && id.inside_margin(state) => {
                            t.finish(SourceSyncOutcome::Unknown, waited);
                        }
                        None if !seen_syncing => {}
                        None if settle >= SETTLE_POLLS => {
                            t.finish(SourceSyncOutcome::Unknown, waited);
                        }
                        None => {
                            t.phase = TriggerPhase::Running {
                                id,
                                seen_syncing,
                                settle: settle + 1,
                            };
                        }
                    }
                }
                TriggerPhase::Ready(_) | TriggerPhase::Done(_) => {}
            }
        }
    }

    let syncs = tracked
        .into_iter()
        .map(|t| SourceSyncResult {
            outcome: match t.phase {
                TriggerPhase::Done(o) => o,
                // The loop leaves only through a `Done` marking, or through
                // `Ok(None)`, where the adapter stopped answering.
                TriggerPhase::AwaitIdle | TriggerPhase::Ready(_) | TriggerPhase::Running { .. } => {
                    SourceSyncOutcome::Unknown
                }
            },
            id: t.id,
            schema: t.schema,
            waited_secs: t.waited_secs,
            last_sync_at: t.last_sync_at,
            detail: t.detail,
            triggered: t.triggered,
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
            observed_at: None,
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
        // Three polls in a row fail: the wait is abandoned, and the report
        // still names the connector, with the error text.
        let abandoned = wait_for_source_syncs(
            &Flaky(Mutex::new(0), 3),
            "",
            &all,
            opts(2000),
            std::future::pending(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(abandoned.syncs.len(), 1);
        assert_eq!(abandoned.syncs[0].id, "a");
        assert_eq!(abandoned.syncs[0].outcome, SourceSyncOutcome::Error);
        assert!(
            abandoned.syncs[0]
                .detail
                .as_deref()
                .is_some_and(|d| d.contains("boom")),
            "{:?}",
            abandoned.syncs[0].detail
        );
        assert!(abandoned.bad().next().is_some());
    }

    // --- trigger ---

    /// Replays poll answers like [`Scripted`], and records the order of reads
    /// and requests so a test can see a request went out between two reads.
    struct TriggerScripted {
        polls: Mutex<VecDeque<Vec<SourceSyncState>>>,
        log: Mutex<Vec<String>>,
        trigger_results: Mutex<VecDeque<Result<Option<DateTime<Utc>>, SourceSyncTriggerError>>>,
    }

    impl TriggerScripted {
        fn new(polls: Vec<Vec<SourceSyncState>>) -> Self {
            Self {
                polls: Mutex::new(polls.into()),
                log: Mutex::new(Vec::new()),
                trigger_results: Mutex::new(VecDeque::new()),
            }
        }
        fn with_trigger_results(
            self,
            results: Vec<Result<Option<DateTime<Utc>>, SourceSyncTriggerError>>,
        ) -> Self {
            *self.trigger_results.lock().unwrap() = results.into();
            self
        }
        fn log(&self) -> Vec<String> {
            self.log.lock().unwrap().clone()
        }
        fn requests(&self) -> Vec<String> {
            self.log()
                .into_iter()
                .filter_map(|l| l.strip_prefix("trigger ").map(str::to_string))
                .collect()
        }
    }

    #[async_trait]
    impl DiscoveryAdapter for TriggerScripted {
        async fn discover(&self, _p: &str) -> AdapterResult<DiscoveryResult> {
            Ok(DiscoveryResult::ok(vec![]))
        }
        async fn source_sync_states(
            &self,
            _p: &str,
        ) -> AdapterResult<Option<Vec<SourceSyncState>>> {
            self.log.lock().unwrap().push("poll".into());
            let mut q = self.polls.lock().unwrap();
            if q.len() > 1 {
                Ok(q.pop_front())
            } else {
                Ok(q.front().cloned())
            }
        }
        async fn trigger_source_sync(
            &self,
            id: &str,
        ) -> Result<Option<DateTime<Utc>>, SourceSyncTriggerError> {
            self.log.lock().unwrap().push(format!("trigger {id}"));
            self.trigger_results
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(Ok(None))
        }
    }

    /// A state read at server time `obs`.
    fn seen(
        id: &str,
        syncing: bool,
        ok: Option<DateTime<Utc>>,
        bad: Option<DateTime<Utc>>,
        obs: i64,
    ) -> SourceSyncState {
        SourceSyncState {
            observed_at: at(obs),
            ..state(id, syncing, ok, bad)
        }
    }

    fn no_event(_: &SourceSyncTriggered) {}

    async fn trigger_run(adapter: &TriggerScripted, timeout_ms: u64) -> SourceSyncReport {
        trigger_and_wait_for_source_syncs(
            adapter,
            "src__",
            &all,
            opts(timeout_ms),
            &no_event,
            std::future::pending(),
        )
        .await
        .unwrap()
        .unwrap()
    }

    #[tokio::test]
    async fn a_triggered_sync_that_ends_past_the_margin_is_a_success() {
        let a = TriggerScripted::new(vec![
            vec![seen("a", false, at(-500), None, 0)],
            vec![seen("a", true, at(-500), None, 10)],
            vec![seen("a", false, at(40), None, 60)],
        ]);
        let report = trigger_run(&a, 2000).await;
        assert_eq!(report.syncs.len(), 1);
        assert_eq!(report.syncs[0].outcome, SourceSyncOutcome::Succeeded);
        assert!(report.syncs[0].triggered);
        assert_eq!(report.syncs[0].last_sync_at, at(40));
        // One read, then exactly one request, then the polls.
        assert_eq!(a.requests(), ["a"]);
        assert_eq!(a.log()[..2], ["poll", "trigger a"]);
    }

    /// The identity rule, at its edge: `succeeded_at` must be STRICTLY past
    /// `T_pre + 5 s`.
    #[tokio::test]
    async fn the_margin_is_strict_five_seconds_past_the_idle_read() {
        let script = |ended: i64| {
            TriggerScripted::new(vec![
                vec![seen("a", false, at(-500), None, 100)],
                vec![seen("a", true, at(-500), None, 110)],
                vec![seen("a", false, at(ended), None, 120)],
            ])
        };
        // Exactly T_pre + 5 s: not past the margin. Never accepted.
        let at_edge = trigger_run(&script(105), 5000).await;
        assert_eq!(at_edge.syncs[0].outcome, SourceSyncOutcome::Unknown);
        assert!(at_edge.syncs[0].triggered);
        // One second more: accepted.
        let past = trigger_run(&script(106), 5000).await;
        assert_eq!(past.syncs[0].outcome, SourceSyncOutcome::Succeeded);
    }

    /// An old `succeeded_at` (before `T_pre`) is never taken for the new sync.
    #[tokio::test]
    async fn an_end_before_the_idle_read_is_never_accepted() {
        let a = TriggerScripted::new(vec![
            vec![seen("a", false, at(50), None, 100)],
            vec![seen("a", true, at(50), None, 110)],
            vec![seen("a", false, at(50), None, 120)],
        ]);
        let report = trigger_run(&a, 5000).await;
        assert_eq!(report.syncs[0].outcome, SourceSyncOutcome::Unknown);
    }

    #[tokio::test]
    async fn a_triggered_sync_that_fails_is_a_failure_and_the_later_timestamp_wins() {
        let failed = TriggerScripted::new(vec![
            vec![seen("a", false, at(-500), at(-400), 0)],
            vec![seen("a", true, at(-500), at(-400), 10)],
            vec![seen("a", false, at(-500), at(40), 60)],
        ]);
        assert_eq!(
            trigger_run(&failed, 2000).await.syncs[0].outcome,
            SourceSyncOutcome::Failed
        );
        let both = TriggerScripted::new(vec![
            vec![seen("a", false, at(-500), at(-400), 0)],
            vec![seen("a", true, at(-500), at(-400), 10)],
            vec![seen("a", false, at(50), at(40), 60)],
        ]);
        assert_eq!(
            trigger_run(&both, 2000).await.syncs[0].outcome,
            SourceSyncOutcome::Succeeded
        );
    }

    /// A sync running at the first read is waited for and NOT counted. The
    /// request goes out only after a read shows the connector idle, and `T_pre`
    /// is that read's server time, so the old sync's end can never pass.
    #[tokio::test]
    async fn a_running_sync_is_waited_out_before_the_request_and_never_counted() {
        let a = TriggerScripted::new(vec![
            vec![seen("a", true, at(-500), None, 0)],
            vec![seen("a", true, at(-500), None, 10)],
            // The old sync ended at 90; the idle read is at server time 100.
            vec![seen("a", false, at(90), None, 100)],
            vec![seen("a", true, at(90), None, 110)],
            vec![seen("a", false, at(160), None, 170)],
        ]);
        let report = trigger_run(&a, 5000).await;
        assert_eq!(report.syncs[0].outcome, SourceSyncOutcome::Succeeded);
        assert_eq!(report.syncs[0].last_sync_at, at(160));
        let log = a.log();
        let request = log.iter().position(|l| l == "trigger a").unwrap();
        // Three reads (syncing, syncing, idle) come before the request.
        assert_eq!(log[..request], ["poll", "poll", "poll"]);
        assert_eq!(a.requests().len(), 1);
    }

    #[tokio::test]
    async fn the_old_sync_ending_at_the_idle_read_is_unknown_not_a_success() {
        // After the request, only the old sync's end (90) is on record.
        let a = TriggerScripted::new(vec![
            vec![seen("a", true, at(-500), None, 0)],
            vec![seen("a", false, at(90), None, 100)],
            vec![seen("a", true, at(90), None, 110)],
            vec![seen("a", false, at(90), None, 120)],
        ]);
        let report = trigger_run(&a, 5000).await;
        assert_eq!(report.syncs[0].outcome, SourceSyncOutcome::Unknown);
    }

    /// Before any read has shown the new sync, an idle connector with no new
    /// timestamp is a slow start, not an outcome: the wait runs to the deadline.
    #[tokio::test]
    async fn a_sync_that_never_shows_before_the_deadline_is_a_timeout() {
        let a = TriggerScripted::new(vec![vec![seen("a", false, at(-500), None, 0)]]);
        let report = trigger_run(&a, 100).await;
        assert_eq!(report.syncs[0].outcome, SourceSyncOutcome::Timeout);
        assert!(report.syncs[0].triggered);
    }

    #[tokio::test]
    async fn a_paused_connector_gets_no_request_and_is_not_a_failure() {
        let mut paused = seen("p", false, at(-500), None, 0);
        paused.paused = true;
        let a = TriggerScripted::new(vec![vec![paused]]);
        let report = trigger_run(&a, 1000).await;
        assert_eq!(report.syncs[0].outcome, SourceSyncOutcome::Paused);
        assert!(!report.syncs[0].triggered);
        assert!(a.requests().is_empty());
        assert!(report.bad().next().is_none());
    }

    #[tokio::test]
    async fn only_the_connectors_the_filter_selects_get_a_request() {
        let a = TriggerScripted::new(vec![
            vec![
                seen("keep", false, at(-500), None, 0),
                seen("skip", false, at(-500), None, 0),
            ],
            vec![
                seen("keep", true, at(-500), None, 10),
                seen("skip", false, at(-500), None, 10),
            ],
            vec![
                seen("keep", false, at(40), None, 60),
                seen("skip", false, at(-500), None, 60),
            ],
        ]);
        let only_keep = |s: &SourceSyncState| s.id == "keep";
        let report = trigger_and_wait_for_source_syncs(
            &a,
            "",
            &only_keep,
            opts(2000),
            &no_event,
            std::future::pending(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(report.syncs.len(), 1);
        assert_eq!(a.requests(), ["keep"]);
    }

    /// A 403 is a failure with the vendor-facing message, and no further
    /// request goes out on a key that was refused.
    #[tokio::test]
    async fn a_refused_key_fails_with_its_message_and_stops_the_requests() {
        let a = TriggerScripted::new(vec![vec![
            seen("a", false, at(-500), None, 0),
            seen("b", false, at(-500), None, 0),
        ]])
        .with_trigger_results(vec![Err(SourceSyncTriggerError::Forbidden(
            "this key cannot start syncs".into(),
        ))]);
        let report = trigger_run(&a, 1000).await;
        assert_eq!(a.requests(), ["a"], "no request after a 403");
        assert_eq!(report.syncs[0].outcome, SourceSyncOutcome::TriggerFailed);
        assert_eq!(
            report.syncs[0].detail.as_deref(),
            Some("this key cannot start syncs")
        );
        assert_eq!(report.syncs[1].outcome, SourceSyncOutcome::TriggerFailed);
        assert!(
            report.syncs[1]
                .detail
                .as_deref()
                .is_some_and(|d| d.starts_with("not started"))
        );
        assert!(!report.syncs[0].triggered && !report.syncs[1].triggered);
    }

    #[tokio::test]
    async fn an_adapter_that_cannot_start_syncs_fails_every_connector() {
        let a = TriggerScripted::new(vec![vec![seen("a", false, at(-500), None, 0)]])
            .with_trigger_results(vec![Err(SourceSyncTriggerError::NotAllowed)]);
        let report = trigger_run(&a, 1000).await;
        assert_eq!(report.syncs[0].outcome, SourceSyncOutcome::TriggerFailed);
        assert!(!report.syncs[0].triggered);
    }

    /// A request with an unknown answer (timeout, 5xx) is treated as sent:
    /// a sync that the re-read shows is accepted, and one that never shows
    /// ends as a timeout, not as a failed request.
    #[tokio::test]
    async fn an_unknown_request_answer_is_checked_with_one_more_read() {
        let started = TriggerScripted::new(vec![
            vec![seen("a", false, at(-500), None, 0)],
            // The re-read after the failed request: it syncs.
            vec![seen("a", true, at(-500), None, 5)],
            vec![seen("a", false, at(60), None, 70)],
        ])
        .with_trigger_results(vec![Err(SourceSyncTriggerError::Unknown("timeout".into()))]);
        let report = trigger_run(&started, 2000).await;
        assert_eq!(report.syncs[0].outcome, SourceSyncOutcome::Succeeded);
        assert!(report.syncs[0].triggered);
        // One request only: an unknown answer is never retried.
        assert_eq!(started.requests().len(), 1);

        // The re-read shows no sync and no sync ever shows: the request may
        // have been sent, so the row stays triggered and waits out the deadline.
        let idle = TriggerScripted::new(vec![vec![seen("a", false, at(-500), None, 0)]])
            .with_trigger_results(vec![Err(SourceSyncTriggerError::Unknown(
                "HTTP 502".into(),
            ))]);
        let report = trigger_run(&idle, 100).await;
        assert_eq!(report.syncs[0].outcome, SourceSyncOutcome::Timeout);
        assert!(report.syncs[0].triggered);
        let detail = report.syncs[0].detail.as_deref().unwrap();
        assert!(
            detail.contains("HTTP 502") && detail.contains("may have been sent"),
            "{detail}"
        );
        assert_eq!(idle.requests().len(), 1);
    }

    /// The re-read after an unknown answer finds the connector still idle
    /// (`scheduled`); the sync starts a moment later and is waited out.
    #[tokio::test]
    async fn a_sync_that_starts_after_an_unknown_answer_is_waited_out() {
        let a = TriggerScripted::new(vec![
            vec![seen("a", false, at(-500), None, 0)],
            // The re-read right after the 502: nothing yet.
            vec![seen("a", false, at(-500), None, 5)],
            vec![seen("a", true, at(-500), None, 10)],
            vec![seen("a", false, at(60), None, 70)],
        ])
        .with_trigger_results(vec![Err(SourceSyncTriggerError::Unknown(
            "HTTP 502".into(),
        ))]);
        let report = trigger_run(&a, 5000).await;
        assert_eq!(report.syncs[0].outcome, SourceSyncOutcome::Succeeded);
        assert!(report.syncs[0].triggered);
        assert_eq!(report.syncs[0].detail, None);
        assert_eq!(a.requests().len(), 1);
    }

    /// After an unknown answer, a sync that ended between the idle read and
    /// the request (a scheduled one, during backoff) must not count as
    /// Rocky's: the re-read moves the baseline and `t_pre`.
    #[tokio::test]
    async fn a_sync_that_ended_before_an_unknown_answer_is_not_counted() {
        let a = TriggerScripted::new(vec![
            vec![seen("a", false, at(-500), None, 0)],
            // The re-read after the 504: a scheduled sync ended at 60.
            vec![seen("a", false, at(60), None, 70)],
        ])
        .with_trigger_results(vec![Err(SourceSyncTriggerError::Unknown(
            "HTTP 504".into(),
        ))]);
        let report = trigger_run(&a, 200).await;
        assert_ne!(report.syncs[0].outcome, SourceSyncOutcome::Succeeded);
        assert_eq!(report.syncs[0].outcome, SourceSyncOutcome::Timeout);
        assert!(report.syncs[0].triggered);
    }

    /// After an unknown answer, a re-read that cannot find the connector ends
    /// it as `unknown` at once, still marked as triggered.
    #[tokio::test]
    async fn an_unknown_answer_with_no_re_read_ends_unknown() {
        let a = TriggerScripted::new(vec![vec![seen("a", false, at(-500), None, 0)], vec![]])
            .with_trigger_results(vec![Err(SourceSyncTriggerError::Unknown(
                "HTTP 502".into(),
            ))]);
        let report = trigger_run(&a, 60_000).await;
        assert_eq!(report.syncs[0].outcome, SourceSyncOutcome::Unknown);
        assert!(report.syncs[0].triggered);
        assert_eq!(a.requests().len(), 1);
    }

    /// A second busy answer stops the requests: no request on every poll.
    #[tokio::test]
    async fn a_second_busy_answer_ends_the_connector() {
        let a = TriggerScripted::new(vec![vec![seen("a", false, at(-500), None, 0)]])
            .with_trigger_results(vec![
                Err(SourceSyncTriggerError::Conflict("HTTP 409".into())),
                Err(SourceSyncTriggerError::Conflict("HTTP 409".into())),
                Err(SourceSyncTriggerError::Conflict("HTTP 409".into())),
            ]);
        let report = trigger_run(&a, 5000).await;
        assert_eq!(report.syncs[0].outcome, SourceSyncOutcome::TriggerFailed);
        assert!(!report.syncs[0].triggered);
        assert_eq!(a.requests().len(), 2);
    }

    /// After a second busy answer, a sync that is running is waited out: the
    /// connector ends only once a read shows it idle, so the run (even under
    /// `warn`) never reads while that sync writes.
    #[tokio::test]
    async fn after_a_second_busy_answer_a_running_sync_is_waited_out() {
        let a = TriggerScripted::new(vec![
            vec![seen("a", false, at(-500), None, 0)],
            vec![seen("a", false, at(-500), None, 10)],
            // After the second 409: a sync is running for two polls.
            vec![seen("a", true, at(-500), None, 20)],
            vec![seen("a", true, at(-500), None, 30)],
            vec![seen("a", false, at(35), None, 40)],
        ])
        .with_trigger_results(vec![
            Err(SourceSyncTriggerError::Conflict("HTTP 409".into())),
            Err(SourceSyncTriggerError::Conflict("HTTP 409".into())),
        ]);
        let report = trigger_run(&a, 5000).await;
        assert_eq!(report.syncs[0].outcome, SourceSyncOutcome::TriggerFailed);
        assert!(!report.syncs[0].triggered);
        assert_eq!(report.syncs[0].last_sync_at, at(35));
        assert!(
            report.syncs[0]
                .detail
                .as_deref()
                .is_some_and(|d| d.contains("busy answer 2 times")),
            "{:?}",
            report.syncs[0].detail
        );
        assert_eq!(a.requests().len(), 2);
    }

    /// After a second busy answer, one idle read is not enough: the status
    /// can lag behind the sync the 409 announced. A sync that shows up after
    /// one idle read is still waited out.
    #[tokio::test]
    async fn after_a_second_busy_answer_one_idle_read_is_not_enough() {
        let a = TriggerScripted::new(vec![
            vec![seen("a", false, at(-500), None, 0)],
            vec![seen("a", false, at(-500), None, 10)],
            // Idle once (lag), then the sync shows, then it ends.
            vec![seen("a", false, at(-500), None, 20)],
            vec![seen("a", true, at(-500), None, 30)],
            vec![seen("a", false, at(35), None, 40)],
        ])
        .with_trigger_results(vec![
            Err(SourceSyncTriggerError::Conflict("HTTP 409".into())),
            Err(SourceSyncTriggerError::Conflict("HTTP 409".into())),
        ]);
        let report = trigger_run(&a, 5000).await;
        assert_eq!(report.syncs[0].outcome, SourceSyncOutcome::TriggerFailed);
        assert_eq!(report.syncs[0].last_sync_at, at(35));

        // A syncing read starts the count again: two idle reads, a syncing
        // one, then idle. Without the reset the connector would end on the
        // first idle read after the sync, before the later end at 65.
        let b = TriggerScripted::new(vec![
            vec![seen("a", false, at(-500), None, 0)],
            vec![seen("a", false, at(-500), None, 10)],
            vec![seen("a", false, at(-500), None, 20)],
            vec![seen("a", false, at(-500), None, 30)],
            vec![seen("a", true, at(-500), None, 40)],
            vec![seen("a", false, at(45), None, 50)],
            vec![seen("a", true, at(45), None, 55)],
            vec![seen("a", false, at(65), None, 70)],
        ])
        .with_trigger_results(vec![
            Err(SourceSyncTriggerError::Conflict("HTTP 409".into())),
            Err(SourceSyncTriggerError::Conflict("HTTP 409".into())),
        ]);
        let report = trigger_run(&b, 5000).await;
        assert_eq!(report.syncs[0].outcome, SourceSyncOutcome::TriggerFailed);
        assert_eq!(report.syncs[0].last_sync_at, at(65));
    }

    /// After a second busy answer, a paused read ends the connector `paused`,
    /// and a deadline hit while it is still busy ends it `timeout`.
    #[tokio::test]
    async fn after_a_second_busy_answer_pause_and_deadline_end_the_connector() {
        let busy = || {
            vec![
                Err(SourceSyncTriggerError::Conflict("HTTP 409".into())),
                Err(SourceSyncTriggerError::Conflict("HTTP 409".into())),
            ]
        };
        let paused = TriggerScripted::new(vec![
            vec![seen("a", false, at(-500), None, 0)],
            vec![seen("a", false, at(-500), None, 10)],
            vec![SourceSyncState {
                paused: true,
                ..seen("a", false, at(-500), None, 20)
            }],
        ])
        .with_trigger_results(busy());
        assert_eq!(
            trigger_run(&paused, 5000).await.syncs[0].outcome,
            SourceSyncOutcome::Paused
        );

        let stuck = TriggerScripted::new(vec![
            vec![seen("a", false, at(-500), None, 0)],
            vec![seen("a", false, at(-500), None, 10)],
            vec![seen("a", true, at(-500), None, 20)],
        ])
        .with_trigger_results(busy());
        let report = trigger_run(&stuck, 200).await;
        assert_eq!(report.syncs[0].outcome, SourceSyncOutcome::Timeout);
        assert!(!report.syncs[0].triggered);
        assert_eq!(stuck.requests().len(), 2);
    }

    /// A timestamp that moved but is not past `T_pre` plus the margin, with no
    /// poll having seen the sync, ends at once. It must not wait the full
    /// timeout.
    #[tokio::test]
    async fn a_moved_end_inside_the_margin_with_no_sync_seen_ends_promptly_as_unknown() {
        let a = TriggerScripted::new(vec![
            vec![seen("a", false, at(-500), None, 100)],
            // Moved past the baseline, 3 s after T_pre: inside the margin.
            vec![seen("a", false, at(103), None, 110)],
        ]);
        let t = Instant::now();
        let report = trigger_run(&a, 60_000).await;
        assert!(t.elapsed() < Duration::from_secs(5), "{:?}", t.elapsed());
        assert_eq!(report.syncs[0].outcome, SourceSyncOutcome::Unknown);
        assert!(report.syncs[0].triggered);
    }

    /// An earlier POST `Date` never lowers `T_pre` below the idle read's.
    #[tokio::test]
    async fn an_earlier_post_date_does_not_lower_t_pre() {
        let script = |ended: i64| {
            TriggerScripted::new(vec![
                vec![seen("a", false, at(-500), None, 100)],
                vec![seen("a", true, at(-500), None, 110)],
                vec![seen("a", false, at(ended), None, 120)],
            ])
            .with_trigger_results(vec![Ok(at(50))])
        };
        // Past the POST date plus the margin, but not past the idle read's.
        let early = trigger_run(&script(103), 5000).await;
        assert_eq!(early.syncs[0].outcome, SourceSyncOutcome::Unknown);
        let late = trigger_run(&script(106), 5000).await;
        assert_eq!(late.syncs[0].outcome, SourceSyncOutcome::Succeeded);
    }

    /// A 409 means the connector is busy: it is waited for, not counted, and
    /// the request is sent again once it is idle.
    #[tokio::test]
    async fn a_busy_answer_waits_the_sync_out_and_asks_again() {
        let a = TriggerScripted::new(vec![
            vec![seen("a", false, at(-500), None, 0)],
            vec![seen("a", true, at(-500), None, 10)],
            // The busy sync ended at 90; idle read at 100.
            vec![seen("a", false, at(90), None, 100)],
            vec![seen("a", true, at(90), None, 110)],
            vec![seen("a", false, at(160), None, 170)],
        ])
        .with_trigger_results(vec![
            Err(SourceSyncTriggerError::Conflict("HTTP 409".into())),
            Ok(None),
        ]);
        let report = trigger_run(&a, 5000).await;
        assert_eq!(report.syncs[0].outcome, SourceSyncOutcome::Succeeded);
        assert_eq!(report.syncs[0].last_sync_at, at(160));
        assert!(report.syncs[0].triggered);
        assert_eq!(a.requests().len(), 2);
    }

    #[tokio::test]
    async fn each_request_calls_the_event_hook_once() {
        let a = TriggerScripted::new(vec![
            vec![seen("a", false, at(-500), None, 0)],
            vec![seen("a", true, at(-500), None, 10)],
            vec![seen("a", false, at(40), None, 60)],
        ]);
        let seen_ids = Mutex::new(Vec::new());
        let hook = |t: &SourceSyncTriggered| {
            seen_ids
                .lock()
                .unwrap()
                .push((t.id.clone(), t.idle_seen_at));
        };
        let report = trigger_and_wait_for_source_syncs(
            &a,
            "",
            &all,
            opts(2000),
            &hook,
            std::future::pending(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(report.syncs[0].outcome, SourceSyncOutcome::Succeeded);
        let ids = seen_ids.lock().unwrap().clone();
        assert_eq!(ids.len(), 1);
        assert_eq!(ids[0].0, "a");
        assert_eq!(Some(ids[0].1), at(0));
    }

    /// A signal while the request is in flight: it may have been sent, so the
    /// row does not claim that nothing went out.
    #[tokio::test]
    async fn a_signal_during_the_request_does_not_claim_nothing_was_sent() {
        let a = TriggerScripted::new(vec![vec![seen("a", false, at(-500), None, 0)]]);
        let report = trigger_and_wait_for_source_syncs(
            &a,
            "",
            &all,
            opts(2000),
            &no_event,
            std::future::ready(()),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(report.interrupted);
        let row = &report.syncs[0];
        assert_eq!(row.outcome, SourceSyncOutcome::Interrupted);
        assert!(row.triggered, "unknown is not false");
        assert!(
            row.detail
                .as_deref()
                .is_some_and(|d| d.contains("may have been sent")),
            "{:?}",
            row.detail
        );
    }

    /// The POST answer's `Date` replaces the idle read's as `T_pre`: an end
    /// before it is not ours, an end after it is.
    #[tokio::test]
    async fn the_post_date_is_the_lower_bound_for_the_end() {
        let script = |ended: i64| {
            TriggerScripted::new(vec![
                vec![seen("a", false, at(-500), None, 0)],
                vec![seen("a", true, at(-500), None, 10)],
                vec![seen("a", false, at(ended), None, 120)],
            ])
            .with_trigger_results(vec![Ok(at(100))])
        };
        // Moved past the baseline and past the idle read, but before the POST.
        let before = trigger_run(&script(50), 5000).await;
        assert_eq!(before.syncs[0].outcome, SourceSyncOutcome::Unknown);
        let after = trigger_run(&script(110), 5000).await;
        assert_eq!(after.syncs[0].outcome, SourceSyncOutcome::Succeeded);
    }

    /// An end that did not move past the baseline is never ours, whatever the
    /// clocks say.
    #[tokio::test]
    async fn an_unmoved_timestamp_is_never_ours_even_past_the_margin() {
        let a = TriggerScripted::new(vec![
            vec![seen("a", false, at(50), None, 0)],
            vec![seen("a", true, at(50), None, 10)],
            vec![seen("a", false, at(50), None, 20)],
        ]);
        let report = trigger_run(&a, 5000).await;
        assert_eq!(report.syncs[0].outcome, SourceSyncOutcome::Unknown);
    }

    /// A refused request (`Failed`) is not re-read: a sync that started on its
    /// own is not ours.
    #[tokio::test]
    async fn a_refused_request_is_not_re_read_and_not_triggered() {
        let a = TriggerScripted::new(vec![
            vec![seen("a", false, at(-500), None, 0)],
            vec![seen("a", true, at(-500), None, 5)],
        ])
        .with_trigger_results(vec![Err(SourceSyncTriggerError::Failed("HTTP 409".into()))]);
        let report = trigger_run(&a, 1000).await;
        assert_eq!(report.syncs[0].outcome, SourceSyncOutcome::TriggerFailed);
        assert!(!report.syncs[0].triggered);
        assert_eq!(a.log(), ["poll", "trigger a"], "no re-read after a refusal");
    }

    #[tokio::test]
    async fn a_signal_ends_a_trigger_wait_at_once() {
        let a = TriggerScripted::new(vec![vec![seen("a", true, at(-500), None, 0)]]);
        let o = SourceSyncWaitOptions {
            timeout: Duration::from_secs(3600),
            poll_interval: Duration::from_secs(30),
        };
        let cancel = async { tokio::time::sleep(Duration::from_millis(30)).await };
        let t = Instant::now();
        let report = trigger_and_wait_for_source_syncs(&a, "", &all, o, &no_event, cancel)
            .await
            .unwrap()
            .unwrap();
        assert!(t.elapsed() < Duration::from_secs(5));
        assert!(report.interrupted);
        assert_eq!(report.syncs[0].outcome, SourceSyncOutcome::Interrupted);
    }
}
