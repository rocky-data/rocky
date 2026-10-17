//! Delivery timing: when a product's output is due, and whether it arrived
//! on time.
//!
//! # When a delivery is due
//!
//! A product declares `deliver_by`, a cron expression. Each occurrence is a
//! deadline. A delivery that finishes at `t` covers the first deadline at or
//! after `t`. So the deadline still open after a delivery is the occurrence
//! after the one it covered.
//!
//! The deadline a new delivery serves:
//!
//! - If the open deadline already passed when the delivery finished, the
//!   delivery serves that open deadline, and it is late.
//! - Otherwise it serves the first deadline at or after its own finish, and
//!   it is on time.
//! - With no previous delivery, there is no open deadline to miss, so the
//!   first delivery is on time.
//!
//! Anchoring on the previous delivery, not on the run's start, keeps a late
//! run honest. A run that starts after today's deadline still serves today's
//! deadline, because nothing delivered it yet. Anchoring on the start would
//! hand it tomorrow's deadline and record it as on time.
//!
//! ```text
//!   deliver_by = "0 6 * * *"
//!
//!   day 1 05:40  delivered    covers day 1 06:00; open deadline: day 2 06:00
//!   day 2 07:30  delivered    day 2 06:00 passed: due day 2 06:00, late 5400 s
//! ```
//!
//! Lateness is wall-clock time from the deadline to the moment the model
//! finished. Only runs that count as production carry a deadline.

use chrono::{DateTime, Utc};

use crate::product::spec::DeliverBy;
use crate::schedule::occurrence::next_occurrence;
use crate::state::{RunRecord, StateError, StateStore, UnrecordedScope};

/// A product's delivery deadline, keyed by the model it delivers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductDeadline {
    /// The product name.
    pub product: String,
    /// The Rocky model the product delivers.
    pub output_model: String,
    /// The deadline schedule.
    pub deliver_by: DeliverBy,
}

/// The first `deliver_by` occurrence strictly after `anchor`.
///
/// `None` when the cron library finds no occurrence. Parsing already
/// validated the expression, so in practice this is always `Some`.
#[must_use]
pub fn due_after(deliver_by: &DeliverBy, anchor: DateTime<Utc>) -> Option<DateTime<Utc>> {
    next_occurrence(&deliver_by.cron, deliver_by.tz, anchor).ok()
}

/// The first `deliver_by` occurrence at or after `at`: the deadline a
/// delivery finishing at `at` covers.
#[must_use]
pub fn covered_by(deliver_by: &DeliverBy, at: DateTime<Utc>) -> Option<DateTime<Utc>> {
    // Occurrences fall on whole minutes, so stepping back one second and
    // searching strictly after finds an occurrence equal to `at`. A
    // sub-second `at` can land just past the occurrence found that way.
    let candidate = due_after(deliver_by, at - chrono::Duration::seconds(1))?;
    if candidate >= at {
        Some(candidate)
    } else {
        due_after(deliver_by, at)
    }
}

/// The deadline still open after a delivery that finished at
/// `last_delivered`: the occurrence after the one that delivery covered.
#[must_use]
pub fn open_deadline_after(
    deliver_by: &DeliverBy,
    last_delivered: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    due_after(deliver_by, covered_by(deliver_by, last_delivered)?)
}

/// The deadline a delivery finishing at `finished` serves, given the
/// previous delivery's finish. See the module docs.
#[must_use]
pub fn deadline_served(
    deliver_by: &DeliverBy,
    previous_delivered: Option<DateTime<Utc>>,
    finished: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    if let Some(previous) = previous_delivered {
        let open = open_deadline_after(deliver_by, previous)?;
        if open < finished {
            return Some(open);
        }
    }
    covered_by(deliver_by, finished)
}

/// Milliseconds from `from` to `to`, or `None` when `to` is earlier.
fn elapsed_ms(from: DateTime<Utc>, to: DateTime<Utc>) -> Option<u64> {
    (to - from)
        .to_std()
        .ok()
        .map(|elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
}

/// Fill the delivery-timing fields of a run record before it is written.
///
/// - [`crate::state::ModelExecution::wait_for_upstream_ms`] on every
///   execution: model start minus run start.
/// - [`RunRecord::queued_ms`]: run start minus the logical timestamp of the
///   schedule claim that launched it, found by `submission_id`.
/// - [`crate::state::ModelExecution::due_at`] on each successful execution
///   of a model that some product in `deadlines` delivers, when the run
///   counts as production. See the module docs for which deadline.
///
/// Call it before the record is written: the previous delivery is read from
/// `store`, and this record must not be its own previous delivery.
///
/// # Errors
///
/// Returns a [`StateError`] when the store cannot be read.
pub fn stamp_delivery_timing(
    record: &mut RunRecord,
    store: &StateStore,
    deadlines: &[ProductDeadline],
) -> Result<(), StateError> {
    let run_started = record.started_at;
    for execution in &mut record.models_executed {
        execution.wait_for_upstream_ms = elapsed_ms(run_started, execution.started_at);
    }

    if let Some(submission_id) = record.submission_id.as_deref() {
        let logical_ts = store
            .list_schedule_claims()?
            .into_iter()
            .filter(|(_, claim)| claim.submission_id == submission_id)
            .find_map(|(key, _)| crate::schedule::parse_claim_key(&key).and_then(|(_, _, ts)| ts));
        record.queued_ms = logical_ts.and_then(|due| elapsed_ms(due, run_started));
    }

    if deadlines.is_empty() || !record.counts_as_production(UnrecordedScope::Exclude) {
        return Ok(());
    }
    for execution in &mut record.models_executed {
        if execution.status != "success" {
            continue;
        }
        let model = execution.rocky_model_name().to_string();
        let Some(deadline) = deadlines.iter().find(|d| d.output_model == model) else {
            continue;
        };
        let previous = store
            .last_delivery(&model, UnrecordedScope::Exclude)?
            .map(|previous| previous.finished_at);
        execution.due_at = deadline_served(&deadline.deliver_by, previous, execution.finished_at);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn utc(y: i32, mo: u32, d: u32, h: u32, mi: u32) -> DateTime<Utc> {
        use chrono::TimeZone;
        Utc.with_ymd_and_hms(y, mo, d, h, mi, 0).unwrap()
    }

    fn six_am() -> DeliverBy {
        DeliverBy {
            cron: "0 6 * * *".to_string(),
            tz: chrono_tz::Tz::UTC,
        }
    }

    #[test]
    fn due_after_is_strictly_after_the_anchor() {
        assert_eq!(
            due_after(&six_am(), utc(2026, 5, 1, 5, 0)),
            Some(utc(2026, 5, 1, 6, 0))
        );
        assert_eq!(
            due_after(&six_am(), utc(2026, 5, 1, 6, 0)),
            Some(utc(2026, 5, 2, 6, 0))
        );
    }

    #[test]
    fn deliver_by_reads_in_its_timezone() {
        let lisbon = DeliverBy {
            cron: "0 6 * * *".to_string(),
            tz: chrono_tz::Tz::Europe__Lisbon,
        };
        // Lisbon is UTC+1 in summer: 06:00 local is 05:00 UTC.
        assert_eq!(
            due_after(&lisbon, utc(2026, 7, 1, 0, 0)),
            Some(utc(2026, 7, 1, 5, 0))
        );
    }

    #[test]
    fn covered_by_includes_an_exact_occurrence() {
        let d = six_am();
        assert_eq!(
            covered_by(&d, utc(2026, 5, 1, 6, 0)),
            Some(utc(2026, 5, 1, 6, 0))
        );
        assert_eq!(
            covered_by(&d, utc(2026, 5, 1, 5, 59)),
            Some(utc(2026, 5, 1, 6, 0))
        );
        let just_after = utc(2026, 5, 1, 6, 0) + chrono::Duration::milliseconds(300);
        assert_eq!(covered_by(&d, just_after), Some(utc(2026, 5, 2, 6, 0)));
    }

    #[test]
    fn deadline_served_cases() {
        let d = six_am();
        // First delivery: nothing was open, so it covers the next deadline.
        assert_eq!(
            deadline_served(&d, None, utc(2026, 5, 1, 7, 30)),
            Some(utc(2026, 5, 2, 6, 0))
        );
        // On time the next day.
        assert_eq!(
            deadline_served(&d, Some(utc(2026, 5, 1, 5, 40)), utc(2026, 5, 2, 5, 50)),
            Some(utc(2026, 5, 2, 6, 0))
        );
        // Late the next day: the open deadline passed before it finished.
        assert_eq!(
            deadline_served(&d, Some(utc(2026, 5, 1, 5, 40)), utc(2026, 5, 2, 7, 30)),
            Some(utc(2026, 5, 2, 6, 0))
        );
        // A second delivery the same day covers the next deadline, on time.
        assert_eq!(
            deadline_served(&d, Some(utc(2026, 5, 1, 5, 40)), utc(2026, 5, 1, 12, 0)),
            Some(utc(2026, 5, 2, 6, 0))
        );
        // A missed day: the delivery serves the oldest open deadline.
        assert_eq!(
            deadline_served(&d, Some(utc(2026, 5, 1, 5, 40)), utc(2026, 5, 3, 7, 0)),
            Some(utc(2026, 5, 2, 6, 0))
        );
    }

    fn bare_record(started: DateTime<Utc>, submission_id: Option<&str>) -> RunRecord {
        serde_json::from_value(serde_json::json!({
            "run_id": "r",
            "started_at": started,
            "finished_at": started,
            "status": crate::state::RunStatus::Success,
            "models_executed": [],
            "trigger": crate::state::RunTrigger::Schedule,
            "config_hash": "c",
            "submission_id": submission_id,
            "run_scope": crate::state::RunScope::Production,
        }))
        .expect("a minimal run record")
    }

    #[test]
    fn queued_ms_joins_the_claim_by_submission_id() {
        let dir = tempfile::tempdir().unwrap();
        let store = StateStore::open(&dir.path().join("state.redb")).unwrap();
        let logical = utc(2026, 5, 1, 6, 0);
        let key = format!("p\u{1f}cron\u{1f}{}", logical.to_rfc3339());
        let claim =
            crate::schedule::claim::ClaimRecord::new_submitted("sub-1".to_string(), logical);
        store
            .schedule_claim_cas(
                &key,
                None,
                &claim,
                "p",
                &crate::schedule::record::ScheduleStateMutation::None,
            )
            .unwrap();

        let mut record = bare_record(utc(2026, 5, 1, 6, 3), Some("sub-1"));
        stamp_delivery_timing(&mut record, &store, &[]).unwrap();
        assert_eq!(record.queued_ms, Some(3 * 60 * 1000));

        let mut by_hand = bare_record(utc(2026, 5, 1, 6, 3), None);
        stamp_delivery_timing(&mut by_hand, &store, &[]).unwrap();
        assert_eq!(
            by_hand.queued_ms, None,
            "a run started by hand was not queued"
        );

        let mut other = bare_record(utc(2026, 5, 1, 6, 3), Some("sub-unknown"));
        stamp_delivery_timing(&mut other, &store, &[]).unwrap();
        assert_eq!(other.queued_ms, None, "no claim, no queue time");
    }
}
