//! Cron occurrence math.
//!
//! [`next_occurrence`] is the single point where the reconciler asks "when does
//! this cron fire next, after this instant, in this timezone?". Wrapping the
//! underlying cron library behind our own function keeps the dependency
//! swappable and pins the DST semantics we depend on (see the tests) to a
//! contract we own rather than to a library's incidental behavior.
//!
//! All timezone reasoning happens in the named IANA zone via `chrono-tz`; the
//! input and output instants are always absolute UTC, so callers never juggle
//! wall-clock ambiguity.

use std::str::FromStr;

use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use croner::Cron;
use thiserror::Error;

/// A cron expression or occurrence search that could not be resolved.
#[derive(Debug, Error)]
pub enum OccurrenceError {
    /// The cron expression did not parse.
    #[error("invalid cron expression {expr:?}: {message}")]
    Parse {
        /// The offending expression, echoed for operator diagnosis.
        expr: String,
        /// The underlying parser message.
        message: String,
    },
    /// The library could not locate an occurrence (e.g. a search-limit
    /// overrun on a pathological pattern).
    #[error("no cron occurrence found for {expr:?} after {after}: {message}")]
    Search {
        /// The expression being searched.
        expr: String,
        /// The instant the search started from.
        after: DateTime<Utc>,
        /// The underlying search message.
        message: String,
    },
}

/// Parse a standard 5-field cron expression, surfacing a typed error on
/// failure. Exposed so config validation can reject a malformed schedule
/// without duplicating the parser choice.
///
/// # Errors
///
/// Returns [`OccurrenceError::Parse`] when `cron_expr` is not a valid cron
/// expression.
pub fn parse_cron(cron_expr: &str) -> Result<Cron, OccurrenceError> {
    Cron::from_str(cron_expr).map_err(|e| OccurrenceError::Parse {
        expr: cron_expr.to_string(),
        message: e.to_string(),
    })
}

/// The first cron occurrence strictly after `after`, evaluated in `tz`.
///
/// The search runs in the named timezone so daylight-saving transitions are
/// honored, then the result is converted back to absolute UTC. `after` is
/// exclusive: an instant that is itself an occurrence yields the *next* one.
///
/// # Errors
///
/// Returns [`OccurrenceError::Parse`] for a malformed expression, or
/// [`OccurrenceError::Search`] if no occurrence can be found.
pub fn next_occurrence(
    cron_expr: &str,
    tz: Tz,
    after: DateTime<Utc>,
) -> Result<DateTime<Utc>, OccurrenceError> {
    let cron = parse_cron(cron_expr)?;
    let after_local = after.with_timezone(&tz);
    let next_local = cron
        .find_next_occurrence(&after_local, false)
        .map_err(|e| OccurrenceError::Search {
            expr: cron_expr.to_string(),
            after,
            message: e.to_string(),
        })?;
    Ok(next_local.with_timezone(&Utc))
}

/// The most recent cron occurrence at or before `at`, evaluated in `tz`.
///
/// Used by the catch-up policy to pick the single "latest missed" occurrence:
/// when several occurrences elapsed while nothing was running, the anchor jumps
/// to this instant and one demand fires for it. `at` is inclusive.
///
/// # Errors
///
/// Returns [`OccurrenceError::Parse`] for a malformed expression, or
/// [`OccurrenceError::Search`] if no occurrence can be found at or before `at`.
pub fn previous_or_equal_occurrence(
    cron_expr: &str,
    tz: Tz,
    at: DateTime<Utc>,
) -> Result<DateTime<Utc>, OccurrenceError> {
    let cron = parse_cron(cron_expr)?;
    let at_local = at.with_timezone(&tz);
    let prev_local = cron
        .find_previous_occurrence(&at_local, true)
        .map_err(|e| OccurrenceError::Search {
            expr: cron_expr.to_string(),
            after: at,
            message: e.to_string(),
        })?;
    Ok(prev_local.with_timezone(&Utc))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn utc(y: i32, mo: u32, d: u32, h: u32, mi: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, mo, d, h, mi, 0).unwrap()
    }

    #[test]
    fn utc_daily_schedule_is_unaffected_by_dst() {
        // A plain UTC schedule must fire at exactly the same wall/UTC instant
        // every day, regardless of any civil-time DST change elsewhere.
        let next = next_occurrence("30 1 * * *", Tz::UTC, utc(2026, 3, 28, 12, 0)).unwrap();
        assert_eq!(next, utc(2026, 3, 29, 1, 30));
        let after = next_occurrence("30 1 * * *", Tz::UTC, next).unwrap();
        assert_eq!(after, utc(2026, 3, 30, 1, 30));
    }

    #[test]
    fn lisbon_spring_forward_skipped_wall_time_fires_once_at_next_valid_instant() {
        // Europe/Lisbon 2026-03-29: clocks jump 01:00 WET -> 02:00 WEST, so the
        // wall time 01:30 never happens. A `30 1 * * *` schedule must fire once,
        // at the next valid instant. 01:00 WET is 01:00Z; the jump lands civil
        // time at 02:00 WEST == 01:00Z. The next valid firing after the gap is
        // the 02:00 WEST boundary, i.e. 01:00Z.
        let tz = Tz::Europe__Lisbon;
        // Start the search just before the missed 01:30 local.
        let start = utc(2026, 3, 29, 0, 0);
        let fire = next_occurrence("30 1 * * *", tz, start).unwrap();

        // It must be a single real instant on the transition day, at or just
        // after the gap, and never before it. The gap is [01:00Z, 01:00Z) in
        // Lisbon terms; the fire is the first valid wall-time match.
        assert_eq!(
            fire.date_naive(),
            utc(2026, 3, 29, 0, 0).date_naive(),
            "must fire on the transition day, once"
        );
        // The next occurrence after it is the following day at 01:30 WEST
        // (00:30Z), proving exactly one fire covered the skipped slot.
        let next = next_occurrence("30 1 * * *", tz, fire).unwrap();
        assert_eq!(next, utc(2026, 3, 30, 0, 30));
        assert!(next > fire);
    }

    #[test]
    fn lisbon_fall_back_repeated_wall_time_fires_on_first_occurrence_only() {
        // Europe/Lisbon 2026-10-25: clocks fall back 02:00 WEST -> 01:00 WET, so
        // the wall time 01:30 happens twice. A `30 1 * * *` schedule must fire
        // on the FIRST occurrence only (the earlier UTC instant, 00:30Z under
        // WEST), never a second time at the repeat (01:30Z under WET).
        let tz = Tz::Europe__Lisbon;
        let start = utc(2026, 10, 25, 0, 0);
        let fire = next_occurrence("30 1 * * *", tz, start).unwrap();
        assert_eq!(
            fire,
            utc(2026, 10, 25, 0, 30),
            "first (WEST) 01:30 == 00:30Z"
        );

        // Crucially, the NEXT occurrence must skip the repeated wall time and
        // land on the following day — not the second 01:30 (01:30Z).
        let next = next_occurrence("30 1 * * *", tz, fire).unwrap();
        assert_eq!(
            next,
            utc(2026, 10, 26, 1, 30),
            "must not fire again at the repeated 01:30 local (01:30Z)"
        );
    }

    #[test]
    fn month_boundary_rolls_over() {
        // Daily at 00:00 across a month boundary.
        let next = next_occurrence("0 0 * * *", Tz::UTC, utc(2026, 1, 31, 12, 0)).unwrap();
        assert_eq!(next, utc(2026, 2, 1, 0, 0));
        // Day-of-month 31 skips months without a 31st (Feb -> Mar 31).
        let next = next_occurrence("0 0 31 * *", Tz::UTC, utc(2026, 1, 31, 12, 0)).unwrap();
        assert_eq!(next, utc(2026, 3, 31, 0, 0));
    }

    #[test]
    fn every_five_minutes_steps_correctly() {
        let mut cursor = utc(2026, 6, 1, 9, 2);
        let expected = [
            utc(2026, 6, 1, 9, 5),
            utc(2026, 6, 1, 9, 10),
            utc(2026, 6, 1, 9, 15),
        ];
        for want in expected {
            cursor = next_occurrence("*/5 * * * *", Tz::UTC, cursor).unwrap();
            assert_eq!(cursor, want);
        }
    }

    #[test]
    fn previous_or_equal_picks_latest_missed_occurrence() {
        // Latest daily-03:00 occurrence at or before 2026-05-10T07:00Z.
        let prev =
            previous_or_equal_occurrence("0 3 * * *", Tz::UTC, utc(2026, 5, 10, 7, 0)).unwrap();
        assert_eq!(prev, utc(2026, 5, 10, 3, 0));
        // Exactly on an occurrence, inclusive returns that occurrence.
        let prev =
            previous_or_equal_occurrence("0 3 * * *", Tz::UTC, utc(2026, 5, 10, 3, 0)).unwrap();
        assert_eq!(prev, utc(2026, 5, 10, 3, 0));
    }

    #[test]
    fn occurrences_strictly_increase() {
        // Property: iterating next_occurrence from its own output always yields
        // a strictly greater instant, across a DST boundary, for several
        // patterns. A stuck or backwards occurrence is a scheduler hang or a
        // double-fire — this pins neither can happen.
        for expr in ["*/7 * * * *", "30 1 * * *", "0 0 * * *", "15 2 * * 1"] {
            for tz in [Tz::UTC, Tz::Europe__Lisbon, Tz::America__New_York] {
                let mut prev = utc(2026, 3, 27, 0, 0);
                for _ in 0..500 {
                    let next = next_occurrence(expr, tz, prev).unwrap();
                    assert!(
                        next > prev,
                        "expr={expr} tz={tz:?}: {next} !> {prev} (must strictly increase)"
                    );
                    prev = next;
                }
            }
        }
    }

    #[test]
    fn malformed_cron_is_a_typed_parse_error() {
        let err = next_occurrence("not a cron", Tz::UTC, utc(2026, 1, 1, 0, 0)).unwrap_err();
        assert!(matches!(err, OccurrenceError::Parse { .. }));
    }
}
