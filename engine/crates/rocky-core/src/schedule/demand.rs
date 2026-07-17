//! Pure demand evaluation: given resolved schedules, the schedule cursors, and
//! the run history, decide what is due — with no clock read and no side effect.
//!
//! The three demand sources, per the frozen semantics:
//!
//! - **cron** (§4.1): due at each occurrence after the catch-up anchor. First
//!   sight records the anchor at `now` and does NOT fire. The anchor advances on
//!   submission regardless of outcome, so a failed occurrence waits for the next
//!   occurrence, not the next tick. Catch-up is `skip` or `latest` only.
//! - **after** (§4.2): due once every upstream has a successful run that
//!   completed *after this pipeline's own latest success started*. The
//!   started-vs-completed asymmetry is load-bearing — it closes the
//!   stale-forever overlap where an upstream succeeds while my unrelated run is
//!   in flight. A partial (exit-2) upstream never counts.
//! - **freshness** (§4.3): due once this pipeline's own run-staleness exceeds
//!   its budget. Run-staleness only — the reconciler never queries the
//!   warehouse.
//!
//! Standing demands (`after`, `freshness`) are additionally subject to the
//! always-on cross-tick throttle ([`super::record::ScheduleStateRecord::standing_throttle`]);
//! cron is throttled by its own occurrence granularity and is never
//! backoff-suppressed.

use chrono::{DateTime, Duration, Utc};
use chrono_tz::Tz;

use super::claim::DemandKind;
use super::occurrence::{OccurrenceError, next_occurrence, previous_or_equal_occurrence};
use super::record::{ScheduleStateRecord, Throttle};

/// Catch-up policy for a cron schedule with more than one missed occurrence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Catchup {
    /// Advance the anchor to the latest missed occurrence and run nothing.
    Skip,
    /// Fire one demand at the latest missed occurrence.
    Latest,
}

/// A resolved, validated schedule for one pipeline — the input to evaluation.
///
/// Produced by [`resolve_schedule`], which parses the cron/timezone/catch-up
/// and resolves the freshness budget. A resolution error is a config error the
/// reconciler surfaces as a skip (fail closed), never a fire.
#[derive(Debug, Clone)]
pub struct ResolvedSchedule {
    /// The pipeline name.
    pub pipeline: String,
    /// Whether the schedule is enabled (`enabled = true`).
    pub enabled: bool,
    /// The parsed cron expression and its timezone, when a cron is configured.
    pub cron: Option<(String, Tz)>,
    /// Upstream pipelines for the `after` trigger.
    pub after: Vec<String>,
    /// The resolved freshness budget, when `freshness = true` and a budget was
    /// resolvable. `None` disables the freshness trigger.
    pub freshness_budget: Option<Duration>,
    /// Catch-up policy.
    pub catchup: Catchup,
    /// In-tick retry budget (`retry.max`).
    pub retry_max: u32,
    /// Scheduler-level timeout, when `timeout_minutes > 0`.
    pub timeout: Option<Duration>,
}

/// A config error found while resolving a schedule. Mirrors the validation
/// codes so the reconciler and `rocky validate` agree on what is invalid.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScheduleConfigError {
    /// The cron expression did not parse.
    BadCron(String),
    /// The timezone name is not a known IANA zone.
    BadTimezone(String),
    /// `catchup` was neither `skip` nor `latest`. `"all"` is called out
    /// specifically.
    BadCatchup(String),
    /// `freshness = true` but no budget was resolvable anywhere.
    FreshnessNoBudget,
}

/// Resolve the freshness budget for a pipeline: the minimum of its member
/// models' declared `max_lag_seconds`, falling back to the project
/// `expected_lag_seconds`. `member_max_lags` holds only the members that
/// declare a budget (empty for pipeline types with no such models, e.g.
/// replication), so the project default is the honest budget there.
pub fn resolve_freshness_budget(
    member_max_lags: &[u64],
    project_expected_lag_seconds: Option<u64>,
) -> Option<u64> {
    if let Some(min) = member_max_lags.iter().copied().min() {
        Some(min)
    } else {
        project_expected_lag_seconds
    }
}

/// Resolve a raw [`ScheduleConfig`](crate::config::ScheduleConfig) into a
/// validated [`ResolvedSchedule`], or the first config error found.
///
/// `default_timezone` is the project `[schedule].timezone`; `member_max_lags`
/// are the freshness budgets of the pipeline's member models (empty when the
/// type has none); `project_expected_lag_seconds` is the project freshness
/// default.
///
/// # Errors
///
/// Returns a [`ScheduleConfigError`] for a malformed cron, an unknown timezone,
/// an invalid `catchup`, or `freshness = true` with no resolvable budget.
pub fn resolve_schedule(
    pipeline: &str,
    schedule: &crate::config::ScheduleConfig,
    default_timezone: &str,
    member_max_lags: &[u64],
    project_expected_lag_seconds: Option<u64>,
) -> Result<ResolvedSchedule, ScheduleConfigError> {
    let cron = match &schedule.cron {
        None => None,
        Some(expr) => {
            // Validate the expression parses.
            super::occurrence::parse_cron(expr)
                .map_err(|_| ScheduleConfigError::BadCron(expr.clone()))?;
            let tz_name = schedule.timezone.as_deref().unwrap_or(default_timezone);
            let tz: Tz = tz_name
                .parse()
                .map_err(|_| ScheduleConfigError::BadTimezone(tz_name.to_string()))?;
            Some((expr.clone(), tz))
        }
    };

    let catchup = match schedule.catchup.as_str() {
        "skip" => Catchup::Skip,
        "latest" => Catchup::Latest,
        other => return Err(ScheduleConfigError::BadCatchup(other.to_string())),
    };

    let freshness_budget = if schedule.freshness {
        match resolve_freshness_budget(member_max_lags, project_expected_lag_seconds) {
            Some(secs) => Some(Duration::seconds(secs as i64)),
            None => return Err(ScheduleConfigError::FreshnessNoBudget),
        }
    } else {
        None
    };

    let timeout = if schedule.timeout_minutes > 0 {
        Some(Duration::minutes(schedule.timeout_minutes as i64))
    } else {
        None
    };

    let _ = pipeline;
    Ok(ResolvedSchedule {
        pipeline: pipeline.to_string(),
        enabled: schedule.enabled,
        cron,
        after: schedule.after.clone(),
        freshness_budget,
        catchup,
        retry_max: schedule.retry.max,
        timeout,
    })
}

/// A concrete due demand: a `(pipeline, source, logical_ts)` instance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Demand {
    /// The pipeline to run.
    pub pipeline: String,
    /// The source that made it due.
    pub source: DemandKind,
    /// The logical timestamp: the cron occurrence, the max upstream success
    /// completion, or the staleness reference instant.
    pub logical_ts: DateTime<Utc>,
}

/// Why a source did not produce a demand this tick. Every "do not run" path is
/// an explicit, recorded reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SkipReason {
    /// No demand from this source right now.
    NotDue,
    /// The schedule is disabled (`enabled = false`).
    Disabled,
    /// More than one cron occurrence elapsed and `catchup = "skip"`: the anchor
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
    /// A higher-priority source already produced the demand this tick.
    Superseded,
    /// The run history could not be read (store fault or a corrupt row), so a
    /// standing demand was skipped rather than misclassified. Fail-closed: never
    /// a false-skip (`after`) or false-fire (`freshness`).
    HistoryError,
}

/// A per-source skip record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceSkip {
    /// The source that was skipped.
    pub source: DemandKind,
    /// Why.
    pub reason: SkipReason,
}

/// The evaluation of one pipeline: the single demand chosen to run (by source
/// priority cron > after > freshness), any anchor updates to persist without
/// firing, and per-source skip reasons.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvaluatedPipeline {
    /// The pipeline name.
    pub pipeline: String,
    /// The demand to run this tick, if any.
    pub due: Option<Demand>,
    /// First-sight cron anchor to initialize (set without firing).
    pub anchor_init: Option<DateTime<Utc>>,
    /// Cron anchor to advance on a catch-up skip (set without firing).
    pub catchup_advance: Option<DateTime<Utc>>,
    /// Per-source skip reasons.
    pub skips: Vec<SourceSkip>,
}

/// A pipeline's latest successful run, reduced to the two instants the
/// reconciler needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunSuccess {
    /// When the run started (the `after` comparison baseline).
    pub started_at: DateTime<Utc>,
    /// When the run finished (the upstream-satisfaction / staleness instant).
    pub finished_at: DateTime<Utc>,
}

/// Read-only view of the schedule cursors.
pub trait ScheduleStateView {
    /// The cursor for a pipeline, or its default when never evaluated.
    fn get(&self, pipeline: &str) -> ScheduleStateRecord;
}

/// A run-history read that could not be completed (a store fault or a corrupt
/// row). It is **never** conflated with "no successful run": a read error must
/// fail closed — skip the demand loudly — rather than misread as "never ran",
/// which would false-skip an `after` demand or false-fire a `freshness` one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryError(pub String);

impl std::fmt::Display for HistoryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "run-history read failed: {}", self.0)
    }
}

impl std::error::Error for HistoryError {}

/// Read-only view of the run history, per pipeline.
pub trait RunHistoryView {
    /// The most recent *successful* run for a pipeline, `Ok(None)` when there is
    /// genuinely none.
    ///
    /// # Errors
    ///
    /// Returns [`HistoryError`] when the history cannot be read (store fault or
    /// a corrupt row). Callers MUST fail closed on this — never treat it as "no
    /// successful run".
    fn latest_successful_run(&self, pipeline: &str) -> Result<Option<RunSuccess>, HistoryError>;
}

/// A stable reference instant for a freshness claim when the pipeline has never
/// succeeded — the Unix epoch, so the claim key does not rotate each tick.
fn never_ran_reference() -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp(0, 0).expect("epoch is a valid instant")
}

/// The outcome of evaluating a cron source.
enum CronEval {
    NotDue,
    InitAnchor(DateTime<Utc>),
    Due(DateTime<Utc>),
    CatchupSkip {
        anchor_to: DateTime<Utc>,
        missed: u32,
    },
}

fn evaluate_cron(
    expr: &str,
    tz: Tz,
    catchup: Catchup,
    state: &ScheduleStateRecord,
    now: DateTime<Utc>,
) -> Result<CronEval, OccurrenceError> {
    let Some(anchor) = state.fire_anchor() else {
        // First sight: record the anchor at `now`, do not fire.
        return Ok(CronEval::InitAnchor(now));
    };
    let next1 = next_occurrence(expr, tz, anchor)?;
    if next1 > now {
        return Ok(CronEval::NotDue);
    }
    // At least one occurrence elapsed. Peek one more to detect catch-up.
    let next2 = next_occurrence(expr, tz, next1)?;
    if next2 > now {
        // Exactly one occurrence is due.
        return Ok(CronEval::Due(next1));
    }
    // More than one occurrence elapsed. The latest at-or-before `now`:
    let latest = previous_or_equal_occurrence(expr, tz, now)?;
    // A capped count for reporting.
    let mut missed = 1u32;
    let mut cursor = next1;
    while missed < 1000 {
        let n = next_occurrence(expr, tz, cursor)?;
        if n > now {
            break;
        }
        missed += 1;
        cursor = n;
    }
    match catchup {
        Catchup::Skip => Ok(CronEval::CatchupSkip {
            anchor_to: latest,
            missed,
        }),
        Catchup::Latest => Ok(CronEval::Due(latest)),
    }
}

/// Whether `after` is due, and if so the logical timestamp
/// (`max(upstream success completions)`).
///
/// # Errors
///
/// Propagates a [`HistoryError`] from any history read — the caller fails closed
/// (a read error is never a "not due").
fn evaluate_after(
    pipeline: &str,
    upstreams: &[String],
    history: &dyn RunHistoryView,
) -> Result<Option<DateTime<Utc>>, HistoryError> {
    if upstreams.is_empty() {
        return Ok(None);
    }
    // My baseline: the start of my latest success. If I never succeeded, every
    // upstream success counts (baseline = epoch).
    let my_baseline = history
        .latest_successful_run(pipeline)?
        .map(|r| r.started_at)
        .unwrap_or_else(never_ran_reference);

    let mut max_completion: Option<DateTime<Utc>> = None;
    for upstream in upstreams {
        let Some(u) = history.latest_successful_run(upstream)? else {
            // An upstream with no success at all — not due.
            return Ok(None);
        };
        // The upstream must have completed AFTER my latest success started
        // (completion-vs-start asymmetry closes the overlap stale-forever case).
        if u.finished_at <= my_baseline {
            return Ok(None);
        }
        max_completion = Some(match max_completion {
            Some(m) if m >= u.finished_at => m,
            _ => u.finished_at,
        });
    }
    Ok(max_completion)
}

/// Whether `freshness` is due, and if so the (stable) logical timestamp.
///
/// # Errors
///
/// Propagates a [`HistoryError`] — a read error must NOT be read as "never ran"
/// (which would fire the epoch occurrence). The caller fails closed.
fn evaluate_freshness(
    pipeline: &str,
    budget: Duration,
    history: &dyn RunHistoryView,
    now: DateTime<Utc>,
) -> Result<Option<DateTime<Utc>>, HistoryError> {
    match history.latest_successful_run(pipeline)? {
        None => Ok(Some(never_ran_reference())),
        Some(r) => {
            if now - r.finished_at > budget {
                Ok(Some(r.finished_at))
            } else {
                Ok(None)
            }
        }
    }
}

/// Evaluate every scheduled pipeline against the cursors and run history at
/// `now`. Pure — no clock read, no writes. The reconciler applies the results.
///
/// # Errors
///
/// Returns [`OccurrenceError`] only if a cron occurrence search fails on a
/// schedule that already parsed (a pathological pattern). A per-pipeline cron
/// evaluation error is surfaced by the caller as a fail-closed skip.
pub fn evaluate_demands(
    schedules: &[ResolvedSchedule],
    state: &dyn ScheduleStateView,
    history: &dyn RunHistoryView,
    now: DateTime<Utc>,
) -> Vec<EvaluatedPipeline> {
    schedules
        .iter()
        .map(|s| evaluate_one(s, state, history, now))
        .collect()
}

/// Evaluate a single pipeline. A cron search error becomes a fail-closed
/// `NotDue` cron skip (never a fire).
pub fn evaluate_one(
    schedule: &ResolvedSchedule,
    state: &dyn ScheduleStateView,
    history: &dyn RunHistoryView,
    now: DateTime<Utc>,
) -> EvaluatedPipeline {
    let mut out = EvaluatedPipeline {
        pipeline: schedule.pipeline.clone(),
        due: None,
        anchor_init: None,
        catchup_advance: None,
        skips: Vec::new(),
    };

    if !schedule.enabled {
        out.skips.push(SourceSkip {
            source: DemandKind::Cron,
            reason: SkipReason::Disabled,
        });
        return out;
    }

    let cursor = state.get(&schedule.pipeline);

    // --- cron (highest priority) --------------------------------------------
    if let Some((expr, tz)) = &schedule.cron {
        match evaluate_cron(expr, *tz, schedule.catchup, &cursor, now) {
            Ok(CronEval::InitAnchor(at)) => {
                out.anchor_init = Some(at);
                out.skips.push(SourceSkip {
                    source: DemandKind::Cron,
                    reason: SkipReason::NotDue,
                });
            }
            Ok(CronEval::NotDue) => {
                out.skips.push(SourceSkip {
                    source: DemandKind::Cron,
                    reason: SkipReason::NotDue,
                });
            }
            Ok(CronEval::CatchupSkip { anchor_to, missed }) => {
                out.catchup_advance = Some(anchor_to);
                out.skips.push(SourceSkip {
                    source: DemandKind::Cron,
                    reason: SkipReason::CatchupSkipped { missed },
                });
            }
            Ok(CronEval::Due(logical_ts)) => {
                out.due = Some(Demand {
                    pipeline: schedule.pipeline.clone(),
                    source: DemandKind::Cron,
                    logical_ts,
                });
            }
            Err(_) => {
                // Fail closed: a cron search error never fires.
                out.skips.push(SourceSkip {
                    source: DemandKind::Cron,
                    reason: SkipReason::NotDue,
                });
            }
        }
    }

    // --- after --------------------------------------------------------------
    // A history read error fails CLOSED: record the skip, never fire on data we
    // could not read (a false "not due" would silently drop a due demand).
    if !schedule.after.is_empty() {
        match evaluate_after(&schedule.pipeline, &schedule.after, history) {
            Ok(Some(logical_ts)) if out.due.is_none() => {
                match apply_standing_throttle(&cursor, now) {
                    Ok(()) => {
                        out.due = Some(Demand {
                            pipeline: schedule.pipeline.clone(),
                            source: DemandKind::After,
                            logical_ts,
                        });
                    }
                    Err(reason) => out.skips.push(SourceSkip {
                        source: DemandKind::After,
                        reason,
                    }),
                }
            }
            Ok(Some(_)) => out.skips.push(SourceSkip {
                source: DemandKind::After,
                reason: SkipReason::Superseded,
            }),
            Ok(None) => out.skips.push(SourceSkip {
                source: DemandKind::After,
                reason: SkipReason::NotDue,
            }),
            Err(_) => out.skips.push(SourceSkip {
                source: DemandKind::After,
                reason: SkipReason::HistoryError,
            }),
        }
    }

    // --- freshness ----------------------------------------------------------
    // A history read error fails CLOSED: skip rather than treat "unreadable" as
    // "never ran" (which would fire — and re-fire — the epoch occurrence).
    if let Some(budget) = schedule.freshness_budget {
        match evaluate_freshness(&schedule.pipeline, budget, history, now) {
            Ok(Some(logical_ts)) if out.due.is_none() => {
                match apply_standing_throttle(&cursor, now) {
                    Ok(()) => {
                        out.due = Some(Demand {
                            pipeline: schedule.pipeline.clone(),
                            source: DemandKind::Freshness,
                            logical_ts,
                        });
                    }
                    Err(reason) => out.skips.push(SourceSkip {
                        source: DemandKind::Freshness,
                        reason,
                    }),
                }
            }
            Ok(Some(_)) => out.skips.push(SourceSkip {
                source: DemandKind::Freshness,
                reason: SkipReason::Superseded,
            }),
            Ok(None) => out.skips.push(SourceSkip {
                source: DemandKind::Freshness,
                reason: SkipReason::NotDue,
            }),
            Err(_) => out.skips.push(SourceSkip {
                source: DemandKind::Freshness,
                reason: SkipReason::HistoryError,
            }),
        }
    }

    out
}

/// Map the standing throttle to `Ok` (clear) or a skip reason.
fn apply_standing_throttle(
    cursor: &ScheduleStateRecord,
    now: DateTime<Utc>,
) -> Result<(), SkipReason> {
    match cursor.standing_throttle(now) {
        Throttle::Clear => Ok(()),
        Throttle::FailureBackoff { resume_at } => Err(SkipReason::FailureBackoff { resume_at }),
        Throttle::PartialBackoff { resume_at } => Err(SkipReason::PartialBackoff { resume_at }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use std::collections::HashMap;

    fn ts(y: i32, mo: u32, d: u32, h: u32, mi: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, mo, d, h, mi, 0).unwrap()
    }

    #[derive(Default)]
    struct States(HashMap<String, ScheduleStateRecord>);
    impl ScheduleStateView for States {
        fn get(&self, pipeline: &str) -> ScheduleStateRecord {
            self.0.get(pipeline).cloned().unwrap_or_default()
        }
    }

    #[derive(Default)]
    struct History(HashMap<String, RunSuccess>);
    impl RunHistoryView for History {
        fn latest_successful_run(
            &self,
            pipeline: &str,
        ) -> Result<Option<RunSuccess>, HistoryError> {
            Ok(self.0.get(pipeline).copied())
        }
    }

    /// A history view that faults for a specific pipeline — models a corrupt
    /// `run_history` row so the fail-closed path is testable at this layer.
    struct FaultyHistory {
        ok: History,
        fault_for: &'static str,
    }
    impl RunHistoryView for FaultyHistory {
        fn latest_successful_run(
            &self,
            pipeline: &str,
        ) -> Result<Option<RunSuccess>, HistoryError> {
            if pipeline == self.fault_for {
                return Err(HistoryError("corrupt run_history row (test)".into()));
            }
            self.ok.latest_successful_run(pipeline)
        }
    }

    fn cron_schedule(name: &str, catchup: Catchup) -> ResolvedSchedule {
        ResolvedSchedule {
            pipeline: name.to_string(),
            enabled: true,
            cron: Some(("0 3 * * *".to_string(), Tz::UTC)),
            after: vec![],
            freshness_budget: None,
            catchup,
            retry_max: 0,
            timeout: None,
        }
    }

    // --- cron ----------------------------------------------------------------

    #[test]
    fn cron_first_sight_records_anchor_and_does_not_fire() {
        let s = cron_schedule("raw", Catchup::Latest);
        let states = States::default();
        let history = History::default();
        let e = evaluate_one(&s, &states, &history, ts(2026, 5, 1, 12, 0));
        assert!(e.due.is_none(), "first sight must not fire");
        assert_eq!(e.anchor_init, Some(ts(2026, 5, 1, 12, 0)));
    }

    #[test]
    fn cron_fires_once_after_anchor() {
        let s = cron_schedule("raw", Catchup::Latest);
        let mut states = States::default();
        states.0.insert(
            "raw".into(),
            ScheduleStateRecord {
                last_fire_logical_ts: Some(ts(2026, 5, 1, 3, 0).to_rfc3339()),
                ..Default::default()
            },
        );
        let history = History::default();
        // Next day 03:00 has elapsed by 04:00.
        let e = evaluate_one(&s, &states, &history, ts(2026, 5, 2, 4, 0));
        assert_eq!(
            e.due.map(|d| (d.source, d.logical_ts)),
            Some((DemandKind::Cron, ts(2026, 5, 2, 3, 0)))
        );
    }

    #[test]
    fn cron_catchup_latest_fires_only_the_most_recent_missed() {
        let s = cron_schedule("raw", Catchup::Latest);
        let mut states = States::default();
        states.0.insert(
            "raw".into(),
            ScheduleStateRecord {
                last_fire_logical_ts: Some(ts(2026, 5, 1, 3, 0).to_rfc3339()),
                ..Default::default()
            },
        );
        let history = History::default();
        // Three days later — 3 occurrences missed; latest is 05-04 03:00.
        let e = evaluate_one(&s, &states, &history, ts(2026, 5, 4, 12, 0));
        assert_eq!(e.due.unwrap().logical_ts, ts(2026, 5, 4, 3, 0));
    }

    #[test]
    fn cron_catchup_skip_advances_anchor_and_runs_nothing() {
        let s = cron_schedule("raw", Catchup::Skip);
        let mut states = States::default();
        states.0.insert(
            "raw".into(),
            ScheduleStateRecord {
                last_fire_logical_ts: Some(ts(2026, 5, 1, 3, 0).to_rfc3339()),
                ..Default::default()
            },
        );
        let history = History::default();
        let e = evaluate_one(&s, &states, &history, ts(2026, 5, 4, 12, 0));
        assert!(e.due.is_none());
        assert_eq!(e.catchup_advance, Some(ts(2026, 5, 4, 3, 0)));
        assert!(matches!(
            e.skips
                .iter()
                .find(|k| k.source == DemandKind::Cron)
                .map(|k| &k.reason),
            Some(SkipReason::CatchupSkipped { missed: 3 })
        ));
    }

    #[test]
    fn cron_now_earlier_than_anchor_does_not_fire() {
        // Anchor monotonicity: a `now` before the anchor is a no-op.
        let s = cron_schedule("raw", Catchup::Latest);
        let mut states = States::default();
        states.0.insert(
            "raw".into(),
            ScheduleStateRecord {
                last_fire_logical_ts: Some(ts(2026, 5, 10, 3, 0).to_rfc3339()),
                ..Default::default()
            },
        );
        let history = History::default();
        let e = evaluate_one(&s, &states, &history, ts(2026, 5, 2, 4, 0));
        assert!(e.due.is_none(), "now before anchor must not fire");
    }

    // --- after ---------------------------------------------------------------

    fn after_schedule(name: &str, upstream: &str) -> ResolvedSchedule {
        ResolvedSchedule {
            pipeline: name.to_string(),
            enabled: true,
            cron: None,
            after: vec![upstream.to_string()],
            freshness_budget: None,
            catchup: Catchup::Latest,
            retry_max: 0,
            timeout: None,
        }
    }

    #[test]
    fn after_due_when_upstream_completed_after_my_start() {
        let s = after_schedule("staging", "raw");
        let states = States::default();
        let mut history = History::default();
        // My last success started at 10:00; upstream finished at 11:00 (after).
        history.0.insert(
            "staging".into(),
            RunSuccess {
                started_at: ts(2026, 5, 1, 10, 0),
                finished_at: ts(2026, 5, 1, 10, 30),
            },
        );
        history.0.insert(
            "raw".into(),
            RunSuccess {
                started_at: ts(2026, 5, 1, 10, 45),
                finished_at: ts(2026, 5, 1, 11, 0),
            },
        );
        let e = evaluate_one(&s, &states, &history, ts(2026, 5, 1, 12, 0));
        assert_eq!(
            e.due.map(|d| (d.source, d.logical_ts)),
            Some((DemandKind::After, ts(2026, 5, 1, 11, 0)))
        );
    }

    #[test]
    fn after_overlap_upstream_finished_before_my_start_not_due() {
        // Upstream finished BEFORE my latest success started -> already consumed.
        let s = after_schedule("staging", "raw");
        let states = States::default();
        let mut history = History::default();
        history.0.insert(
            "staging".into(),
            RunSuccess {
                started_at: ts(2026, 5, 1, 11, 0),
                finished_at: ts(2026, 5, 1, 11, 30),
            },
        );
        history.0.insert(
            "raw".into(),
            RunSuccess {
                started_at: ts(2026, 5, 1, 10, 0),
                finished_at: ts(2026, 5, 1, 10, 30),
            },
        );
        let e = evaluate_one(&s, &states, &history, ts(2026, 5, 1, 12, 0));
        assert!(e.due.is_none());
    }

    #[test]
    fn after_overlap_upstream_finished_during_my_run_still_due_next_tick() {
        // The started-vs-completed trap: my run started 10:00, finished 11:30;
        // upstream finished 11:00 (during my run). My completion (11:30) is
        // LATER than the upstream, but my START (10:00) is earlier, so the
        // upstream data was NOT seen by my run -> still due.
        let s = after_schedule("staging", "raw");
        let states = States::default();
        let mut history = History::default();
        history.0.insert(
            "staging".into(),
            RunSuccess {
                started_at: ts(2026, 5, 1, 10, 0),
                finished_at: ts(2026, 5, 1, 11, 30),
            },
        );
        history.0.insert(
            "raw".into(),
            RunSuccess {
                started_at: ts(2026, 5, 1, 10, 45),
                finished_at: ts(2026, 5, 1, 11, 0),
            },
        );
        let e = evaluate_one(&s, &states, &history, ts(2026, 5, 1, 12, 0));
        assert_eq!(
            e.due.map(|d| d.source),
            Some(DemandKind::After),
            "must still be due — compares completion vs my START"
        );
    }

    #[test]
    fn after_not_due_when_an_upstream_never_succeeded() {
        let s = after_schedule("staging", "raw");
        let states = States::default();
        let history = History::default(); // no runs at all
        let e = evaluate_one(&s, &states, &history, ts(2026, 5, 1, 12, 0));
        assert!(e.due.is_none());
    }

    #[test]
    fn after_never_ran_downstream_fires_on_first_upstream_success() {
        let s = after_schedule("staging", "raw");
        let states = States::default();
        let mut history = History::default();
        // staging never succeeded; raw has a success.
        history.0.insert(
            "raw".into(),
            RunSuccess {
                started_at: ts(2026, 5, 1, 10, 0),
                finished_at: ts(2026, 5, 1, 10, 30),
            },
        );
        let e = evaluate_one(&s, &states, &history, ts(2026, 5, 1, 12, 0));
        assert_eq!(e.due.map(|d| d.source), Some(DemandKind::After));
    }

    // --- throttle on standing demands ---------------------------------------

    #[test]
    fn after_due_but_failure_backoff_suppresses() {
        let s = after_schedule("staging", "raw");
        let mut states = States::default();
        states.0.insert(
            "staging".into(),
            ScheduleStateRecord {
                last_attempt_at: Some(ts(2026, 5, 1, 11, 30).to_rfc3339()),
                last_attempt_outcome: Some("failure".into()),
                consecutive_failures: 1, // 5 min backoff
                ..Default::default()
            },
        );
        let mut history = History::default();
        history.0.insert(
            "raw".into(),
            RunSuccess {
                started_at: ts(2026, 5, 1, 10, 0),
                finished_at: ts(2026, 5, 1, 11, 0),
            },
        );
        // now is 11:32 — within the 5-min window from 11:30.
        let e = evaluate_one(&s, &states, &history, ts(2026, 5, 1, 11, 32));
        assert!(
            e.due.is_none(),
            "failure backoff suppresses the standing demand"
        );
        assert!(matches!(
            e.skips
                .iter()
                .find(|k| k.source == DemandKind::After)
                .map(|k| &k.reason),
            Some(SkipReason::FailureBackoff { .. })
        ));
        // After the window, it fires.
        let e2 = evaluate_one(&s, &states, &history, ts(2026, 5, 1, 11, 36));
        assert_eq!(e2.due.map(|d| d.source), Some(DemandKind::After));
    }

    #[test]
    fn perma_partial_throttles_at_sixty_minutes() {
        // A reliably-partial standing pipeline must NOT re-run every tick.
        let s = after_schedule("staging", "raw");
        let mut states = States::default();
        states.0.insert(
            "staging".into(),
            ScheduleStateRecord {
                last_attempt_at: Some(ts(2026, 5, 1, 11, 0).to_rfc3339()),
                last_attempt_outcome: Some("partial".into()),
                consecutive_failures: 0,
                ..Default::default()
            },
        );
        let mut history = History::default();
        history.0.insert(
            "raw".into(),
            RunSuccess {
                started_at: ts(2026, 5, 1, 9, 0),
                finished_at: ts(2026, 5, 1, 10, 0),
            },
        );
        let e = evaluate_one(&s, &states, &history, ts(2026, 5, 1, 11, 30));
        assert!(matches!(
            e.skips
                .iter()
                .find(|k| k.source == DemandKind::After)
                .map(|k| &k.reason),
            Some(SkipReason::PartialBackoff { .. })
        ));
    }

    // --- freshness -----------------------------------------------------------

    #[test]
    fn freshness_due_when_stale_beyond_budget() {
        let s = ResolvedSchedule {
            pipeline: "raw".into(),
            enabled: true,
            cron: None,
            after: vec![],
            freshness_budget: Some(Duration::hours(1)),
            catchup: Catchup::Latest,
            retry_max: 0,
            timeout: None,
        };
        let states = States::default();
        let mut history = History::default();
        history.0.insert(
            "raw".into(),
            RunSuccess {
                started_at: ts(2026, 5, 1, 8, 0),
                finished_at: ts(2026, 5, 1, 9, 0),
            },
        );
        // 2h later -> stale beyond a 1h budget.
        let e = evaluate_one(&s, &states, &history, ts(2026, 5, 1, 11, 0));
        assert_eq!(
            e.due.map(|d| (d.source, d.logical_ts)),
            Some((DemandKind::Freshness, ts(2026, 5, 1, 9, 0)))
        );
        // Within budget -> not due.
        let e2 = evaluate_one(&s, &states, &history, ts(2026, 5, 1, 9, 30));
        assert!(e2.due.is_none());
    }

    // --- priority + disabled -------------------------------------------------

    #[test]
    fn cron_takes_priority_over_after() {
        let s = ResolvedSchedule {
            pipeline: "raw".into(),
            enabled: true,
            cron: Some(("0 3 * * *".into(), Tz::UTC)),
            after: vec!["up".into()],
            freshness_budget: None,
            catchup: Catchup::Latest,
            retry_max: 0,
            timeout: None,
        };
        let mut states = States::default();
        states.0.insert(
            "raw".into(),
            ScheduleStateRecord {
                last_fire_logical_ts: Some(ts(2026, 5, 1, 3, 0).to_rfc3339()),
                ..Default::default()
            },
        );
        let mut history = History::default();
        history.0.insert(
            "up".into(),
            RunSuccess {
                started_at: ts(2026, 5, 2, 0, 0),
                finished_at: ts(2026, 5, 2, 1, 0),
            },
        );
        let e = evaluate_one(&s, &states, &history, ts(2026, 5, 2, 4, 0));
        assert_eq!(e.due.map(|d| d.source), Some(DemandKind::Cron));
        assert!(
            e.skips
                .iter()
                .any(|k| k.source == DemandKind::After && k.reason == SkipReason::Superseded)
        );
    }

    #[test]
    fn disabled_schedule_produces_no_demand() {
        let mut s = cron_schedule("raw", Catchup::Latest);
        s.enabled = false;
        let mut states = States::default();
        states.0.insert(
            "raw".into(),
            ScheduleStateRecord {
                last_fire_logical_ts: Some(ts(2026, 5, 1, 3, 0).to_rfc3339()),
                ..Default::default()
            },
        );
        let e = evaluate_one(&s, &states, &History::default(), ts(2026, 6, 1, 4, 0));
        assert!(e.due.is_none());
        assert!(e.skips.iter().any(|k| k.reason == SkipReason::Disabled));
    }

    // --- freshness budget resolution ----------------------------------------

    #[test]
    fn freshness_budget_prefers_member_min_then_project() {
        assert_eq!(
            resolve_freshness_budget(&[3600, 1800, 7200], Some(600)),
            Some(1800),
            "min of members"
        );
        assert_eq!(
            resolve_freshness_budget(&[], Some(600)),
            Some(600),
            "falls back to project"
        );
        assert_eq!(resolve_freshness_budget(&[], None), None, "unresolvable");
    }

    // --- fail-closed on a history read fault (F4/F5) -------------------------

    #[test]
    fn after_history_read_error_fails_closed_no_false_skip() {
        // The upstream's run-history read faults. A due `after` must NOT be
        // silently dropped as "not due" — it records a HistoryError skip and
        // fires nothing.
        let s = after_schedule("staging", "raw");
        let states = States::default();
        let mut ok = History::default();
        // Even though `raw` genuinely succeeded, the read for it faults.
        ok.0.insert(
            "raw".into(),
            RunSuccess {
                started_at: ts(2026, 5, 1, 10, 0),
                finished_at: ts(2026, 5, 1, 11, 0),
            },
        );
        let history = FaultyHistory {
            ok,
            fault_for: "raw",
        };
        let e = evaluate_one(&s, &states, &history, ts(2026, 5, 1, 12, 0));
        assert!(e.due.is_none(), "a read fault must never fire");
        assert!(
            e.skips
                .iter()
                .any(|k| k.source == DemandKind::After && k.reason == SkipReason::HistoryError),
            "the fault is recorded, not elided: {:?}",
            e.skips
        );
    }

    #[test]
    fn freshness_history_read_error_fails_closed_no_false_fire() {
        // The pipeline's own run-history read faults. Freshness must NOT read
        // the fault as "never ran" and fire the epoch occurrence.
        let s = ResolvedSchedule {
            pipeline: "raw".into(),
            enabled: true,
            cron: None,
            after: vec![],
            freshness_budget: Some(Duration::hours(1)),
            catchup: Catchup::Latest,
            retry_max: 0,
            timeout: None,
        };
        let states = States::default();
        let history = FaultyHistory {
            ok: History::default(),
            fault_for: "raw",
        };
        let e = evaluate_one(&s, &states, &history, ts(2026, 5, 1, 12, 0));
        assert!(
            e.due.is_none(),
            "a read fault must never fire the epoch occurrence"
        );
        assert!(
            e.skips
                .iter()
                .any(|k| k.source == DemandKind::Freshness && k.reason == SkipReason::HistoryError),
            "the fault is recorded: {:?}",
            e.skips
        );
    }
}
