//! The per-pipeline schedule cursor and the cross-tick throttle math.
//!
//! [`ScheduleStateRecord`] is the durable cursor for one scheduled pipeline: the
//! catch-up anchor, the last attempt time and outcome, and the consecutive
//! failure count that drives the always-on backoff. Its anchors are
//! **monotonic** — [`ScheduleStateRecord::advance_fire_anchor`] and
//! [`ScheduleStateRecord::record_attempt`] never move a cursor backwards, so a
//! `--now` earlier than the anchor is a no-op by construction.

use chrono::{DateTime, Duration, Utc};

use super::claim::{CfDelta, TerminalOutcome};

/// The per-pipeline schedule cursor stored under the pipeline name.
///
/// Every field is serde-defaulted so a record written by an older binary — or a
/// future one that adds a field — round-trips cleanly without a blob migration.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ScheduleStateRecord {
    /// RFC3339; updated on every tick that evaluates this pipeline. A stale
    /// value is how `doctor` detects a dead timer.
    #[serde(default)]
    pub last_evaluated_at: Option<String>,
    /// RFC3339; the last cron occurrence **submitted** — the catch-up anchor.
    /// Advances on submission regardless of the run's outcome. Monotonic.
    #[serde(default)]
    pub last_fire_logical_ts: Option<String>,
    /// RFC3339; the last submission time for any source — drives the cross-tick
    /// failure/partial backoff. Monotonic.
    #[serde(default)]
    pub last_attempt_at: Option<String>,
    /// The `run_id`/submission of the most recent submission, for operator
    /// cross-reference.
    #[serde(default)]
    pub last_submitted_run_id: Option<String>,
    /// Exit-1/lost failures increment this once per exhausted demand; a success
    /// resets it; a partial (exit 2) does neither.
    #[serde(default)]
    pub consecutive_failures: u32,
    /// `"success"` | `"partial"` | `"failure"` — the last terminal outcome,
    /// consulted by the outcome-aware throttle.
    #[serde(default)]
    pub last_attempt_outcome: Option<String>,
}

/// The cross-tick backoff ceiling: failure backoff never exceeds this.
const MAX_FAILURE_BACKOFF: Duration = Duration::hours(6);
/// The base unit of the exponential failure backoff.
const BASE_FAILURE_BACKOFF: Duration = Duration::minutes(5);
/// The fixed partial-success backoff.
const PARTIAL_BACKOFF: Duration = Duration::minutes(60);

/// The suppression a standing demand is under right now, if any.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Throttle {
    /// Not suppressed — the demand may proceed.
    Clear,
    /// Suppressed by the failure backoff ladder until `resume_at`.
    FailureBackoff {
        /// When the demand becomes eligible again.
        resume_at: DateTime<Utc>,
    },
    /// Suppressed by the fixed partial backoff until `resume_at`.
    PartialBackoff {
        /// When the demand becomes eligible again.
        resume_at: DateTime<Utc>,
    },
}

impl ScheduleStateRecord {
    fn last_attempt(&self) -> Option<DateTime<Utc>> {
        self.last_attempt_at
            .as_deref()
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
            .map(|dt| dt.with_timezone(&Utc))
    }

    /// The catch-up anchor as an absolute instant, if set.
    pub fn fire_anchor(&self) -> Option<DateTime<Utc>> {
        self.last_fire_logical_ts
            .as_deref()
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
            .map(|dt| dt.with_timezone(&Utc))
    }

    /// The failure-backoff duration for the current `consecutive_failures`:
    /// `min(5min * 2^(cf - 1), 6h)`. Returns `None` when `cf == 0`.
    fn failure_backoff(&self) -> Option<Duration> {
        if self.consecutive_failures == 0 {
            return None;
        }
        // 5min * 2^(cf-1), saturating at the ceiling. Cap the shift so 2^n never
        // overflows before the min() clamps it.
        let shift = (self.consecutive_failures - 1).min(32);
        let scaled = BASE_FAILURE_BACKOFF
            .checked_mul(1i32.checked_shl(shift).unwrap_or(i32::MAX))
            .unwrap_or(MAX_FAILURE_BACKOFF);
        Some(scaled.min(MAX_FAILURE_BACKOFF))
    }

    /// The cross-tick throttle a **standing** demand (`after`/`freshness`) is
    /// under at `now`, from `last_attempt_outcome` + `last_attempt_at`.
    ///
    /// Cron demands are naturally throttled by their own occurrence granularity
    /// and must NOT be passed here — suppressing a cron occurrence via this
    /// ladder would drop a legitimate fire.
    ///
    /// - last attempt failed (`consecutive_failures > 0`): suppressed until
    ///   `last_attempt_at + min(5min * 2^(cf-1), 6h)`.
    /// - last attempt partial (exit 2): suppressed until `last_attempt_at + 60min`.
    ///   Load-bearing: a partial is not a success, so the standing demand stays
    ///   due — without this a reliably-partial pipeline re-runs every tick.
    /// - last attempt succeeded: no suppression.
    pub fn standing_throttle(&self, now: DateTime<Utc>) -> Throttle {
        let Some(last_attempt) = self.last_attempt() else {
            return Throttle::Clear;
        };
        match self.last_attempt_outcome.as_deref() {
            Some("failure") => {
                let Some(backoff) = self.failure_backoff() else {
                    return Throttle::Clear;
                };
                let resume_at = last_attempt + backoff;
                if now < resume_at {
                    Throttle::FailureBackoff { resume_at }
                } else {
                    Throttle::Clear
                }
            }
            Some("partial") => {
                let resume_at = last_attempt + PARTIAL_BACKOFF;
                if now < resume_at {
                    Throttle::PartialBackoff { resume_at }
                } else {
                    Throttle::Clear
                }
            }
            _ => Throttle::Clear,
        }
    }

    /// Advance the catch-up fire anchor to `logical_ts`, monotonically: a value
    /// at or before the current anchor is ignored. This is the only writer of
    /// `last_fire_logical_ts`.
    pub fn advance_fire_anchor(&mut self, logical_ts: DateTime<Utc>) {
        match self.fire_anchor() {
            Some(current) if logical_ts <= current => {}
            _ => self.last_fire_logical_ts = Some(logical_ts.to_rfc3339()),
        }
    }

    /// Record a submission at `attempt_at` for `run_id`, advancing
    /// `last_attempt_at` monotonically. This is the only writer of
    /// `last_attempt_at`.
    pub fn record_submission(&mut self, attempt_at: DateTime<Utc>, run_id: &str) {
        let advance = match self.last_attempt() {
            Some(prev) => attempt_at > prev,
            None => true,
        };
        if advance {
            self.last_attempt_at = Some(attempt_at.to_rfc3339());
        }
        self.last_submitted_run_id = Some(run_id.to_string());
    }

    /// Apply terminal bookkeeping: move `consecutive_failures` per `cf_delta`
    /// and stamp `last_attempt_outcome`.
    pub fn apply_bookkeeping(&mut self, outcome: TerminalOutcome, cf_delta: CfDelta) {
        match cf_delta {
            CfDelta::Increment => {
                self.consecutive_failures = self.consecutive_failures.saturating_add(1)
            }
            CfDelta::Reset => self.consecutive_failures = 0,
            CfDelta::Unchanged => {}
        }
        self.last_attempt_outcome = Some(outcome.as_str().to_string());
    }
}

/// A mutation applied to a pipeline's [`ScheduleStateRecord`] atomically with a
/// claim compare-and-swap (see `StateStore::schedule_claim_cas`), so the cursor
/// and the claim always move together — a crash can never leave a claim without
/// its anchor advance, or vice versa.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScheduleStateMutation {
    /// No cursor change (an in-tick retry that only rewrites the claim, for
    /// instance).
    None,
    /// A cron submission: advance the catch-up anchor to `logical_ts` and record
    /// the submission time and run id.
    SubmitCron {
        /// The occurrence being submitted — the new anchor value.
        logical_ts: DateTime<Utc>,
        /// The submission time.
        attempt_at: DateTime<Utc>,
        /// The launched run's submission id.
        run_id: String,
    },
    /// A standing (`after`/`freshness`) submission: record the submission time
    /// and run id only (no anchor — standing demands have no cron anchor).
    SubmitStanding {
        /// The submission time.
        attempt_at: DateTime<Utc>,
        /// The launched run's submission id.
        run_id: String,
    },
    /// A terminal outcome: apply the `consecutive_failures`/outcome bookkeeping.
    Terminal {
        /// The terminal outcome.
        outcome: TerminalOutcome,
        /// How `consecutive_failures` moves.
        cf_delta: CfDelta,
    },
}

impl ScheduleStateMutation {
    /// Apply this mutation to `rec` in place.
    pub fn apply(&self, rec: &mut ScheduleStateRecord) {
        match self {
            ScheduleStateMutation::None => {}
            ScheduleStateMutation::SubmitCron {
                logical_ts,
                attempt_at,
                run_id,
            } => {
                rec.advance_fire_anchor(*logical_ts);
                rec.record_submission(*attempt_at, run_id);
            }
            ScheduleStateMutation::SubmitStanding { attempt_at, run_id } => {
                rec.record_submission(*attempt_at, run_id);
            }
            ScheduleStateMutation::Terminal { outcome, cf_delta } => {
                rec.apply_bookkeeping(*outcome, *cf_delta);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn at(h: u32, mi: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 5, 1, h, mi, 0).unwrap()
    }

    #[test]
    fn failure_backoff_ladder_doubles_and_caps_at_six_hours() {
        let mut r = ScheduleStateRecord {
            last_attempt_at: Some(at(0, 0).to_rfc3339()),
            last_attempt_outcome: Some("failure".into()),
            ..Default::default()
        };
        // cf=1 -> 5min, cf=2 -> 10min, cf=3 -> 20min ...
        r.consecutive_failures = 1;
        assert_eq!(
            r.standing_throttle(at(0, 4)),
            Throttle::FailureBackoff {
                resume_at: at(0, 5)
            }
        );
        assert_eq!(
            r.standing_throttle(at(0, 5)),
            Throttle::Clear,
            "resume at boundary is inclusive-clear"
        );
        r.consecutive_failures = 2;
        assert_eq!(
            r.standing_throttle(at(0, 9)),
            Throttle::FailureBackoff {
                resume_at: at(0, 10)
            }
        );
        r.consecutive_failures = 3;
        assert_eq!(
            r.standing_throttle(at(0, 19)),
            Throttle::FailureBackoff {
                resume_at: at(0, 20)
            }
        );
        // Large cf saturates at the 6h ceiling, never overflows.
        r.consecutive_failures = 100;
        assert_eq!(
            r.standing_throttle(at(5, 0)),
            Throttle::FailureBackoff {
                resume_at: at(6, 0)
            }
        );
        assert_eq!(r.standing_throttle(at(6, 0)), Throttle::Clear);
    }

    #[test]
    fn partial_backoff_is_fixed_sixty_minutes() {
        // regression: the perma-partial hot loop must throttle at 60m.
        let r = ScheduleStateRecord {
            last_attempt_at: Some(at(0, 0).to_rfc3339()),
            last_attempt_outcome: Some("partial".into()),
            // partial does not touch consecutive_failures
            consecutive_failures: 0,
            ..Default::default()
        };
        assert_eq!(
            r.standing_throttle(at(0, 59)),
            Throttle::PartialBackoff {
                resume_at: at(1, 0)
            }
        );
        assert_eq!(r.standing_throttle(at(1, 0)), Throttle::Clear);
    }

    #[test]
    fn success_is_never_throttled() {
        let r = ScheduleStateRecord {
            last_attempt_at: Some(at(0, 0).to_rfc3339()),
            last_attempt_outcome: Some("success".into()),
            ..Default::default()
        };
        assert_eq!(r.standing_throttle(at(0, 1)), Throttle::Clear);
    }

    #[test]
    fn anchors_are_monotonic() {
        let mut r = ScheduleStateRecord::default();
        r.advance_fire_anchor(at(3, 0));
        assert_eq!(r.fire_anchor(), Some(at(3, 0)));
        // A backwards value is ignored.
        r.advance_fire_anchor(at(1, 0));
        assert_eq!(
            r.fire_anchor(),
            Some(at(3, 0)),
            "anchor must not move backwards"
        );
        r.advance_fire_anchor(at(4, 0));
        assert_eq!(r.fire_anchor(), Some(at(4, 0)));

        r.record_submission(at(3, 0), "run-a");
        r.record_submission(at(1, 0), "run-b"); // backwards attempt time ignored
        assert_eq!(
            r.last_attempt_at.as_deref(),
            Some(at(3, 0).to_rfc3339()).as_deref(),
            "last_attempt_at must not move backwards"
        );
    }

    #[test]
    fn bookkeeping_moves_cf_correctly() {
        let mut r = ScheduleStateRecord::default();
        r.apply_bookkeeping(TerminalOutcome::Failure, CfDelta::Increment);
        r.apply_bookkeeping(TerminalOutcome::Failure, CfDelta::Increment);
        assert_eq!(r.consecutive_failures, 2);
        r.apply_bookkeeping(TerminalOutcome::Partial, CfDelta::Unchanged);
        assert_eq!(
            r.consecutive_failures, 2,
            "partial neither increments nor resets"
        );
        assert_eq!(r.last_attempt_outcome.as_deref(), Some("partial"));
        r.apply_bookkeeping(TerminalOutcome::Success, CfDelta::Reset);
        assert_eq!(r.consecutive_failures, 0);
    }
}
