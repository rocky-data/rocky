//! Delivery timing: when a product's output is due, and whether it arrived
//! on time.
//!
//! # When a delivery is due
//!
//! A product declares `deliver_by`, a cron expression. Each occurrence is a
//! deadline. Each delivery records the deadline it served. It also covers
//! every deadline that had passed when it finished, so a late delivery
//! catches up. The deadline still open after a delivery is the occurrence
//! after the latest one it covered.
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
//! A late delivery reports one lateness, against the oldest missed
//! deadline. The deadlines it caught up on are not reported separately.
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
//!
//! # How long a delivery takes
//!
//! [`estimate_durations`] reads the last [`ESTIMATE_WINDOW`] production
//! durations and returns their p50 and p90. With fewer than
//! [`ESTIMATE_MIN_RUNS`] it returns no band, and no verdict may rest on it.
//! It has no day-of-week model.

use chrono::{DateTime, Utc};

use crate::product::spec::DeliverBy;
use crate::schedule::occurrence::{next_occurrence, previous_or_equal_occurrence};
use crate::state::{RunRecord, StateError, StateStore, UnrecordedScope};

/// How many recent production durations the estimate reads.
pub const ESTIMATE_WINDOW: usize = 20;

/// The fewest durations the estimate needs before it reports a band.
pub const ESTIMATE_MIN_RUNS: usize = 5;

/// How many of the newest durations the growth check compares.
pub const GROWTH_RUNS: usize = 5;

/// A p50/p90 band over recent durations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurationBand {
    /// Median duration in milliseconds.
    pub p50_ms: u64,
    /// 90th-percentile duration in milliseconds. Raised to the slowest of
    /// the newest runs when they all ran slower than before; see
    /// [`Self::growth`].
    pub p90_ms: u64,
    /// True when the newest [`GROWTH_RUNS`] durations all exceed the p90 of
    /// the older durations, so `p90_ms` is the largest of those newest runs.
    pub growth: bool,
}

/// The duration estimate: the band, plus the sample it rests on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurationEstimate {
    /// How many durations the estimate read.
    pub n: usize,
    /// The most durations it reads ([`ESTIMATE_WINDOW`]).
    pub window: usize,
    /// The band. `None` when `n` is below [`ESTIMATE_MIN_RUNS`]: too few
    /// runs to say anything.
    pub band: Option<DurationBand>,
}

/// Nearest-rank percentile of a sorted, non-empty slice.
fn percentile(sorted: &[u64], pct: usize) -> u64 {
    let rank = (pct * sorted.len()).div_ceil(100).max(1);
    sorted[rank - 1]
}

/// Estimate p50 and p90 from durations listed newest first.
///
/// Reads at most [`ESTIMATE_WINDOW`] durations. Below [`ESTIMATE_MIN_RUNS`]
/// the band is `None`. Percentiles are nearest-rank.
///
/// **Growth.** When the newest [`GROWTH_RUNS`] durations all exceed the p90
/// of the older durations in the window, the runs have grown slower and the
/// window's own p90 lags behind. Then `p90_ms` is the largest of the newest
/// [`GROWTH_RUNS`]. The comparison must use the older durations: over the
/// whole window of 20, at most two values sit above the p90, so "the newest
/// five all exceed it" could never be true. The check needs at least
/// [`GROWTH_RUNS`] older durations, so it starts at 10 runs: against one or
/// two older values almost any five runs would read as growth. Growth never
/// lowers the p90: it is the larger of the window's p90 and the slowest of
/// the newest five.
#[must_use]
pub fn estimate_durations(newest_first: &[u64]) -> DurationEstimate {
    let sample = &newest_first[..newest_first.len().min(ESTIMATE_WINDOW)];
    let n = sample.len();
    if n < ESTIMATE_MIN_RUNS {
        return DurationEstimate {
            n,
            window: ESTIMATE_WINDOW,
            band: None,
        };
    }
    let mut sorted = sample.to_vec();
    sorted.sort_unstable();
    let p50_ms = percentile(&sorted, 50);
    let mut p90_ms = percentile(&sorted, 90);

    // `split_at` panics unless the sample holds `GROWTH_RUNS` entries.
    const _: () = assert!(ESTIMATE_MIN_RUNS >= GROWTH_RUNS);
    let (newest, older) = sample.split_at(GROWTH_RUNS);
    let mut growth = false;
    if older.len() >= GROWTH_RUNS {
        let mut older_sorted = older.to_vec();
        older_sorted.sort_unstable();
        let older_p90 = percentile(&older_sorted, 90);
        if newest.iter().all(|duration| *duration > older_p90) {
            growth = true;
            p90_ms = p90_ms.max(newest.iter().copied().max().unwrap_or(p90_ms));
        }
    }
    DurationEstimate {
        n,
        window: ESTIMATE_WINDOW,
        band: Some(DurationBand {
            p50_ms,
            p90_ms,
            growth,
        }),
    }
}

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

/// A previous delivery: when it finished, and the deadline its run recorded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LastDelivery {
    /// When the model finished.
    pub finished_at: DateTime<Utc>,
    /// The deadline the run recorded for it, when it recorded one.
    pub due_at: Option<DateTime<Utc>>,
}

/// The latest deadline a delivery satisfied: the one it served, or a later
/// one that had already passed when it finished. A late delivery catches up
/// on every deadline before its finish.
///
/// A delivery with no recorded deadline (written before this field existed)
/// is taken to have covered the first deadline at or after its finish.
fn last_covered(deliver_by: &DeliverBy, delivery: LastDelivery) -> Option<DateTime<Utc>> {
    let served = match delivery.due_at {
        Some(due) => due,
        None => covered_by(deliver_by, delivery.finished_at)?,
    };
    let passed =
        previous_or_equal_occurrence(&deliver_by.cron, deliver_by.tz, delivery.finished_at).ok();
    Some(passed.map_or(served, |passed| passed.max(served)))
}

/// The deadline still open after `last`: the occurrence after the latest one
/// it covered.
#[must_use]
pub fn open_deadline_after(deliver_by: &DeliverBy, last: LastDelivery) -> Option<DateTime<Utc>> {
    due_after(deliver_by, last_covered(deliver_by, last)?)
}

/// The deadline a delivery finishing at `finished` serves, given the
/// previous delivery. See the module docs.
#[must_use]
pub fn deadline_served(
    deliver_by: &DeliverBy,
    previous: Option<LastDelivery>,
    finished: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    // A previous delivery with no recorded deadline (written before the
    // product declared `deliver_by`, or before this field existed) is no
    // baseline: judging from it would charge every deadline since then, often
    // days, to this delivery.
    if let Some(previous) = previous.filter(|previous| previous.due_at.is_some()) {
        let open = open_deadline_after(deliver_by, previous)?;
        if open < finished {
            return Some(open);
        }
    }
    covered_by(deliver_by, finished)
}

/// Seconds from `due` to `finished`, rounded up: a delivery a fraction of a
/// second late is late by one second, never by zero. Zero or negative is on
/// time.
#[must_use]
pub fn late_by_seconds(due: DateTime<Utc>, finished: DateTime<Utc>) -> i64 {
    (finished - due).num_milliseconds().div_euclid(1000)
        + i64::from((finished - due).num_milliseconds().rem_euclid(1000) != 0)
}

/// The delivery state a status surface reports for one deadline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryVerdict {
    /// The deadline the latest delivery served, when the run recorded one.
    pub last_due_at: Option<DateTime<Utc>>,
    /// When the latest delivery finished.
    pub last_delivered_at: Option<DateTime<Utc>>,
    /// Whether the latest delivery met its deadline. `None` when no
    /// delivery recorded a deadline.
    pub on_time: Option<bool>,
    /// How late the latest delivery was, in seconds; `0` when on time.
    /// `None` when no delivery recorded a deadline.
    pub late_by_seconds: Option<u64>,
    /// The deadline the next delivery must meet: the one still open after
    /// the latest delivery, or the first at or after `now` when nothing was
    /// delivered yet. A time in the past means the next delivery is already
    /// late.
    pub next_due_at: Option<DateTime<Utc>>,
    /// Whether the p90 duration is longer than the time left before
    /// `next_due_at`. `None` when the estimate has no band (fewer than
    /// [`ESTIMATE_MIN_RUNS`] runs). A deadline that already passed leaves
    /// less than no time, so it reads `true`.
    pub at_risk: Option<bool>,
}

/// Judge one deadline from the latest delivery and a duration estimate.
///
/// `on_time` and `late_by_seconds` are facts about the latest delivery and
/// need no estimate. Only `at_risk` rests on the estimate.
#[must_use]
pub fn delivery_verdict(
    deliver_by: &DeliverBy,
    last: Option<LastDelivery>,
    estimate: &DurationEstimate,
    now: DateTime<Utc>,
) -> DeliveryVerdict {
    let late = last.and_then(|last| {
        last.due_at
            .map(|due| late_by_seconds(due, last.finished_at))
    });
    let next_due_at = match last {
        Some(last) => open_deadline_after(deliver_by, last),
        None => covered_by(deliver_by, now),
    };
    let at_risk = match (estimate.band, next_due_at) {
        (Some(band), Some(due)) => {
            let left_ms = (due - now).num_milliseconds();
            Some(i128::from(band.p90_ms) > i128::from(left_ms))
        }
        (None, _) | (_, None) => None,
    };
    DeliveryVerdict {
        last_due_at: last.and_then(|last| last.due_at),
        last_delivered_at: last.map(|last| last.finished_at),
        on_time: late.map(|late| late <= 0),
        late_by_seconds: late.map(|late| u64::try_from(late).unwrap_or(0)),
        next_due_at,
        at_risk,
    }
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
    // One history read per delivered model, not per execution: a partitioned
    // model records one execution per partition.
    let mut previous_by_model: std::collections::BTreeMap<String, Option<LastDelivery>> =
        std::collections::BTreeMap::new();
    for execution in &mut record.models_executed {
        if execution.status != "success" {
            continue;
        }
        let model = execution.rocky_model_name().to_string();
        let Some(deadline) = deadlines.iter().find(|d| d.output_model == model) else {
            continue;
        };
        let previous = match previous_by_model.get(&model) {
            Some(previous) => *previous,
            None => {
                let previous = store
                    .last_delivery(&model, UnrecordedScope::Exclude, Some(&record.run_id))?
                    .map(|previous| LastDelivery {
                        finished_at: previous.finished_at,
                        due_at: previous.due_at,
                    });
                previous_by_model.insert(model, previous);
                previous
            }
        };
        execution.due_at = deadline_served(&deadline.deliver_by, previous, execution.finished_at);
    }
    Ok(())
}

/// A deadline a pipeline serves itself: its next due time and the product.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectDeadline {
    /// The product whose deadline this is.
    pub product: String,
    /// The deadline the next delivery must meet. `None` when the cron finds
    /// no occurrence; the latest start is then unknown.
    pub due_at: Option<DateTime<Utc>>,
}

/// One pipeline in the `[schedule] after` graph, as [`latest_starts`] reads
/// it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ChainNode {
    /// The pipelines this one runs after (its `[schedule] after`).
    pub after: Vec<String>,
    /// The p90 of the pipeline's run time, in milliseconds. `None` when the
    /// estimate has too few runs.
    pub p90_ms: Option<u64>,
    /// The most urgent deadline the pipeline serves itself, if any.
    pub deadline: Option<DirectDeadline>,
    /// True when the pipeline may serve a deadline but cannot build it: its
    /// models could not all be read, so the next run builds nothing, and
    /// some product declares a deadline: neither config nor run history can
    /// show that it builds no deadlined model. The node then counts as
    /// feeding a
    /// deadline whose start-by is unknown.
    pub serves_unknown: bool,
}

/// Why a latest start is unknown.
///
/// Ordered by declaration so a sorted list reads the same every time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum UnknownReason {
    /// A pipeline on the way has no p90: fewer than 5 successful production
    /// runs. Its name is in `missing_estimate`.
    MissingEstimate,
    /// A deadline's cron finds no next occurrence.
    NoNextDeadline,
    /// A pipeline on the way could not have all its models read, so it may
    /// serve a deadline it cannot build. See [`ChainNode::serves_unknown`].
    ModelsUnreadable,
    /// The `after` graph has a cycle on the way to a deadline.
    Cycle,
    /// Subtracting a p90 from the start-by time leaves the date range.
    Overflow,
}

/// The latest time a pipeline can start and still meet every deadline it
/// feeds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LatestStart {
    /// Every node on every chain to a deadline has a p90.
    Known {
        /// The latest start.
        at: DateTime<Utc>,
        /// The product whose deadline gives the minimum.
        product: String,
        /// That product's next deadline.
        due_at: DateTime<Utc>,
    },
    /// Some term on a chain is unknown, so there is no answer. Never a guess.
    Unknown {
        /// Every reason found on the chains, sorted, without repeats. Never
        /// empty.
        reasons: Vec<UnknownReason>,
        /// The pipelines on the chains whose p90 is unknown, sorted. Empty
        /// unless `reasons` holds [`UnknownReason::MissingEstimate`].
        missing_estimate: Vec<String>,
    },
}

/// Derive the latest start of every pipeline that feeds a deadline.
///
/// The rule, walking `after` edges backwards from each pipeline that serves
/// a deadline:
///
/// ```text
///   latest_start(P) = min( next_due_at(P)                 if P serves one,
///                          latest_start(C) for each C that runs after P
///                                          and feeds a deadline )
///                     - p90(P)
/// ```
///
/// A pipeline feeds a deadline when it serves one, has
/// [`ChainNode::serves_unknown`] set, or some pipeline that runs after it,
/// directly or through others, does. Every other pipeline is absent from the
/// result, so a cycle in `after` with no deadline downstream gives no entry.
///
/// One unknown term makes the result unknown, with every reason found (see
/// [`UnknownReason`]): a p90 missing on any node of any chain, a deadline
/// with no next occurrence, a node with unreadable models, a cycle in `after`
/// (the reconciler refuses one), or a subtraction that leaves the date range.
/// Taking the minimum over the known terms only would be a guess.
///
/// A deadline only derives this value. It never adds an `after` edge and
/// never moves a cron.
#[must_use]
pub fn latest_starts(
    nodes: &std::collections::BTreeMap<String, ChainNode>,
) -> std::collections::BTreeMap<String, LatestStart> {
    use std::collections::{BTreeMap, BTreeSet};

    // Every node that feeds a deadline: the deadline nodes and everything
    // upstream of them along `after`.
    let mut feeding: BTreeSet<&str> = BTreeSet::new();
    let mut stack: Vec<&str> = nodes
        .iter()
        .filter(|(_, node)| node.deadline.is_some() || node.serves_unknown)
        .map(|(name, _)| name.as_str())
        .collect();
    while let Some(name) = stack.pop() {
        if !feeding.insert(name) {
            continue;
        }
        if let Some(node) = nodes.get(name) {
            stack.extend(node.after.iter().map(String::as_str));
        }
    }

    let mut consumers: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for (name, node) in nodes {
        if !feeding.contains(name.as_str()) {
            continue;
        }
        for upstream in &node.after {
            consumers
                .entry(upstream.as_str())
                .or_default()
                .push(name.as_str());
        }
    }

    fn unknown(reason: UnknownReason) -> LatestStart {
        LatestStart::Unknown {
            reasons: vec![reason],
            missing_estimate: Vec::new(),
        }
    }

    // Only called on a node in `feeding`, and only walks consumers that are
    // in it, so every call has a deadline downstream and returns an answer.
    fn walk<'a>(
        name: &'a str,
        nodes: &'a BTreeMap<String, ChainNode>,
        consumers: &BTreeMap<&'a str, Vec<&'a str>>,
        memo: &mut BTreeMap<&'a str, LatestStart>,
        in_progress: &mut BTreeSet<&'a str>,
    ) -> LatestStart {
        if let Some(done) = memo.get(name) {
            return done.clone();
        }
        if !in_progress.insert(name) {
            return unknown(UnknownReason::Cycle);
        }
        let node = nodes.get(name);
        let mut reasons: BTreeSet<UnknownReason> = BTreeSet::new();
        let mut missing: BTreeSet<String> = BTreeSet::new();
        // The earliest start-by of everything this node feeds.
        let mut earliest: Option<(DateTime<Utc>, String, DateTime<Utc>)> = None;
        let mut consider = |at: DateTime<Utc>, product: &str, due: DateTime<Utc>| {
            if earliest.as_ref().is_none_or(|(held, _, _)| at < *held) {
                earliest = Some((at, product.to_string(), due));
            }
        };

        if let Some(deadline) = node.and_then(|node| node.deadline.as_ref()) {
            match deadline.due_at {
                Some(due) => consider(due, &deadline.product, due),
                None => {
                    reasons.insert(UnknownReason::NoNextDeadline);
                }
            }
        }
        if node.is_some_and(|node| node.serves_unknown) {
            reasons.insert(UnknownReason::ModelsUnreadable);
        }
        for consumer in consumers.get(name).into_iter().flatten() {
            match walk(consumer, nodes, consumers, memo, in_progress) {
                LatestStart::Known {
                    at,
                    product,
                    due_at,
                } => consider(at, &product, due_at),
                LatestStart::Unknown {
                    reasons: theirs,
                    missing_estimate,
                } => {
                    reasons.extend(theirs);
                    missing.extend(missing_estimate);
                }
            }
        }
        in_progress.remove(name);

        let p90_ms = node.and_then(|node| node.p90_ms);
        if p90_ms.is_none() {
            reasons.insert(UnknownReason::MissingEstimate);
            missing.insert(name.to_string());
        }
        let result = match (p90_ms, earliest) {
            (Some(p90_ms), Some((at, product, due_at))) if reasons.is_empty() => {
                let p90 = chrono::Duration::milliseconds(i64::try_from(p90_ms).unwrap_or(i64::MAX));
                match at.checked_sub_signed(p90) {
                    Some(at) => LatestStart::Known {
                        at,
                        product,
                        due_at,
                    },
                    None => unknown(UnknownReason::Overflow),
                }
            }
            _ => {
                // A node in `feeding` with no reason and no start-by cannot
                // occur: it serves a deadline, has `serves_unknown`, or has a
                // consumer that answered. Should it ever, it is unknown, not
                // a time.
                if reasons.is_empty() {
                    reasons.insert(UnknownReason::NoNextDeadline);
                }
                LatestStart::Unknown {
                    reasons: reasons.into_iter().collect(),
                    missing_estimate: missing.into_iter().collect(),
                }
            }
        };
        memo.insert(name, result.clone());
        result
    }

    let mut memo: BTreeMap<&str, LatestStart> = BTreeMap::new();
    let mut in_progress: BTreeSet<&str> = BTreeSet::new();
    let mut out = BTreeMap::new();
    for name in nodes.keys() {
        if feeding.contains(name.as_str()) {
            let result = walk(name, nodes, &consumers, &mut memo, &mut in_progress);
            out.insert(name.clone(), result);
        }
    }
    out
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

    // ----- the duration estimate -----

    #[test]
    fn cold_start_has_no_band() {
        for n in 0..ESTIMATE_MIN_RUNS {
            let durations = vec![1_000; n];
            let estimate = estimate_durations(&durations);
            assert_eq!(estimate.n, n);
            assert_eq!(estimate.window, ESTIMATE_WINDOW);
            assert_eq!(estimate.band, None, "{n} runs must give no band");
        }
    }

    #[test]
    fn five_runs_are_enough_for_a_band() {
        let estimate = estimate_durations(&[500, 400, 300, 200, 100]);
        assert_eq!(estimate.n, 5);
        let band = estimate.band.expect("five runs give a band");
        assert_eq!(band.p50_ms, 300);
        assert_eq!(band.p90_ms, 500);
        assert!(!band.growth, "no older runs to compare against");
    }

    #[test]
    fn a_stable_series_gives_its_own_percentiles() {
        // 20 runs, newest first: 1..=20 seconds, shuffled so the newest five
        // are not the slowest.
        let newest_first: Vec<u64> = [
            7, 3, 12, 1, 18, 20, 5, 9, 14, 2, 16, 11, 4, 19, 8, 13, 6, 17, 10, 15,
        ]
        .iter()
        .map(|s| s * 1_000)
        .collect();
        let estimate = estimate_durations(&newest_first);
        assert_eq!(estimate.n, 20);
        assert_eq!(estimate.window, ESTIMATE_WINDOW);
        let band = estimate.band.expect("band");
        assert_eq!(band.p50_ms, 10_000, "nearest-rank p50 of 1..=20");
        assert_eq!(band.p90_ms, 18_000, "nearest-rank p90 of 1..=20");
        assert!(!band.growth);
    }

    #[test]
    fn only_the_newest_window_is_read() {
        let mut newest_first = vec![1_000; ESTIMATE_WINDOW];
        // Older than the window: must not move the band.
        newest_first.extend(vec![1_000_000; 10]);
        let estimate = estimate_durations(&newest_first);
        assert_eq!(estimate.n, ESTIMATE_WINDOW);
        assert_eq!(estimate.band.expect("band").p90_ms, 1_000);
    }

    #[test]
    fn a_growth_series_uses_the_largest_of_the_newest_five() {
        // 15 older runs near 10 s, then the 5 newest runs, all slower.
        let mut newest_first = vec![31_000, 34_000, 30_000, 33_000, 32_000];
        newest_first.extend((0..15).map(|i| 9_000 + i * 100));
        let band = estimate_durations(&newest_first).band.expect("band");
        assert!(band.growth, "the newest five all exceed the older p90");
        assert_eq!(band.p90_ms, 34_000);
        // The window's own nearest-rank p90 is 32 s: two of the newest runs
        // are slower than it, so it would under-state the next run.
        let mut sorted = newest_first.clone();
        sorted.sort_unstable();
        assert_eq!(percentile(&sorted, 90), 32_000);
    }

    #[test]
    fn growth_needs_five_older_runs() {
        // Nine runs: the newest five all exceed the four older ones, but
        // four older values are too few to call it growth.
        let newest_first = [50, 60, 70, 80, 90, 10, 11, 12, 13];
        let band = estimate_durations(&newest_first).band.expect("band");
        assert!(!band.growth);
        assert_eq!(band.p90_ms, 90, "the window's own nearest-rank p90");
    }

    #[test]
    fn growth_never_lowers_the_p90() {
        // One large older spike, and the newest five still exceed the older
        // p90. The growth p90 must not fall below the window's own.
        let mut newest_first = vec![31_000, 34_000, 30_000, 33_000, 32_000];
        newest_first.extend((0..14).map(|i| 9_000 + i * 100));
        newest_first.push(500_000);
        let mut sorted = newest_first.clone();
        sorted.sort_unstable();
        let window_p90 = percentile(&sorted, 90);
        let band = estimate_durations(&newest_first).band.expect("band");
        assert!(band.p90_ms >= window_p90);
    }

    #[test]
    fn one_fast_run_among_the_newest_five_is_not_growth() {
        let mut newest_first = vec![31_000, 34_000, 5_000, 33_000, 32_000];
        newest_first.extend((0..15).map(|i| 9_000 + i * 100));
        let band = estimate_durations(&newest_first).band.expect("band");
        assert!(!band.growth);
        assert_eq!(band.p90_ms, 32_000, "the window's own nearest-rank p90");
    }

    // ----- deadlines -----

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

    fn previous(finished: DateTime<Utc>, due: DateTime<Utc>) -> Option<LastDelivery> {
        Some(LastDelivery {
            finished_at: finished,
            due_at: Some(due),
        })
    }

    #[test]
    fn deadline_served_cases() {
        let d = six_am();
        let day1_on_time = previous(utc(2026, 5, 1, 5, 40), utc(2026, 5, 1, 6, 0));
        // First delivery: nothing was open, so it covers the next deadline.
        assert_eq!(
            deadline_served(&d, None, utc(2026, 5, 1, 7, 30)),
            Some(utc(2026, 5, 2, 6, 0))
        );
        // On time the next day.
        assert_eq!(
            deadline_served(&d, day1_on_time, utc(2026, 5, 2, 5, 50)),
            Some(utc(2026, 5, 2, 6, 0))
        );
        // Late the next day: the open deadline passed before it finished.
        assert_eq!(
            deadline_served(&d, day1_on_time, utc(2026, 5, 2, 7, 30)),
            Some(utc(2026, 5, 2, 6, 0))
        );
        // A second delivery the same day covers the next deadline, on time.
        assert_eq!(
            deadline_served(&d, day1_on_time, utc(2026, 5, 1, 12, 0)),
            Some(utc(2026, 5, 2, 6, 0))
        );
        // A missed day: the delivery serves the oldest open deadline.
        assert_eq!(
            deadline_served(&d, day1_on_time, utc(2026, 5, 3, 7, 0)),
            Some(utc(2026, 5, 2, 6, 0))
        );
    }

    #[test]
    fn a_late_delivery_does_not_cover_the_next_deadline() {
        let d = six_am();
        // Day 2's delivery landed at 07:30, late for day 2 06:00.
        let day2_late = previous(utc(2026, 5, 2, 7, 30), utc(2026, 5, 2, 6, 0));
        assert_eq!(
            open_deadline_after(&d, day2_late.unwrap()),
            Some(utc(2026, 5, 3, 6, 0))
        );
        // So day 3 at 07:00 is late again, not on time for day 4.
        assert_eq!(
            deadline_served(&d, day2_late, utc(2026, 5, 3, 7, 0)),
            Some(utc(2026, 5, 3, 6, 0))
        );
        // And day 3 at 05:50 is on time.
        assert_eq!(
            deadline_served(&d, day2_late, utc(2026, 5, 3, 5, 50)),
            Some(utc(2026, 5, 3, 6, 0))
        );
    }

    #[test]
    fn a_late_delivery_catches_up_on_every_passed_deadline() {
        let d = six_am();
        // Day 3 at 07:00 served day 2 06:00 late; day 3 06:00 had passed too.
        let caught_up = previous(utc(2026, 5, 3, 7, 0), utc(2026, 5, 2, 6, 0));
        assert_eq!(
            open_deadline_after(&d, caught_up.unwrap()),
            Some(utc(2026, 5, 4, 6, 0))
        );
        assert_eq!(
            deadline_served(&d, caught_up, utc(2026, 5, 4, 5, 50)),
            Some(utc(2026, 5, 4, 6, 0)),
            "day 4 on time is not charged with day 3"
        );
    }

    #[test]
    fn a_delivery_without_a_recorded_deadline_is_no_baseline() {
        let d = six_am();
        // Five days old and recorded before deadlines existed: judging from
        // it would charge four missed days to this delivery.
        let unrecorded = Some(LastDelivery {
            finished_at: utc(2026, 5, 1, 5, 40),
            due_at: None,
        });
        assert_eq!(
            deadline_served(&d, unrecorded, utc(2026, 5, 6, 7, 30)),
            Some(utc(2026, 5, 7, 6, 0)),
            "treated like a first delivery"
        );
    }

    #[test]
    fn a_fraction_of_a_second_late_is_late() {
        let due = utc(2026, 5, 1, 6, 0);
        let ms = chrono::Duration::milliseconds;
        assert_eq!(late_by_seconds(due, due), 0);
        assert_eq!(late_by_seconds(due, due + ms(300)), 1);
        assert_eq!(late_by_seconds(due, due - ms(300)), 0);
        assert_eq!(late_by_seconds(due, due + ms(5_400_000)), 5_400);
        assert_eq!(late_by_seconds(due, due - ms(1_200_000)), -1_200);
    }

    // ----- the verdict -----

    fn known(p90_ms: u64) -> DurationEstimate {
        DurationEstimate {
            n: 5,
            window: ESTIMATE_WINDOW,
            band: Some(DurationBand {
                p50_ms: p90_ms / 2,
                p90_ms,
                growth: false,
            }),
        }
    }

    fn delivered(finished: DateTime<Utc>, due: DateTime<Utc>) -> Option<LastDelivery> {
        Some(LastDelivery {
            finished_at: finished,
            due_at: Some(due),
        })
    }

    #[test]
    fn verdict_on_time() {
        let verdict = delivery_verdict(
            &six_am(),
            delivered(utc(2026, 5, 1, 5, 40), utc(2026, 5, 1, 6, 0)),
            &known(60_000),
            utc(2026, 5, 1, 12, 0),
        );
        assert_eq!(verdict.on_time, Some(true));
        assert_eq!(verdict.late_by_seconds, Some(0));
        assert_eq!(verdict.next_due_at, Some(utc(2026, 5, 2, 6, 0)));
        assert_eq!(verdict.at_risk, Some(false));
    }

    #[test]
    fn verdict_late() {
        let verdict = delivery_verdict(
            &six_am(),
            delivered(utc(2026, 5, 1, 7, 30), utc(2026, 5, 1, 6, 0)),
            &known(60_000),
            utc(2026, 5, 1, 12, 0),
        );
        assert_eq!(verdict.on_time, Some(false));
        assert_eq!(verdict.late_by_seconds, Some(5_400));
    }

    #[test]
    fn verdict_at_risk_when_p90_exceeds_time_left() {
        // Delivered on day 1, so the open deadline is day 2 06:00. It is
        // 05:30 on day 2 and the p90 is one hour.
        let verdict = delivery_verdict(
            &six_am(),
            delivered(utc(2026, 5, 1, 5, 40), utc(2026, 5, 1, 6, 0)),
            &known(3_600_000),
            utc(2026, 5, 2, 5, 30),
        );
        assert_eq!(verdict.next_due_at, Some(utc(2026, 5, 2, 6, 0)));
        assert_eq!(verdict.at_risk, Some(true));
    }

    #[test]
    fn a_delivery_that_already_landed_today_is_not_at_risk() {
        // Delivered at 05:20 on day 2; at 05:30 the open deadline is day 3.
        let verdict = delivery_verdict(
            &six_am(),
            delivered(utc(2026, 5, 2, 5, 20), utc(2026, 5, 2, 6, 0)),
            &known(3_600_000),
            utc(2026, 5, 2, 5, 30),
        );
        assert_eq!(verdict.next_due_at, Some(utc(2026, 5, 3, 6, 0)));
        assert_eq!(verdict.at_risk, Some(false));
    }

    #[test]
    fn no_band_means_no_risk_verdict() {
        let verdict = delivery_verdict(
            &six_am(),
            None,
            &estimate_durations(&[1, 2, 3]),
            utc(2026, 5, 2, 5, 59),
        );
        assert_eq!(verdict.at_risk, None);
        assert_eq!(verdict.on_time, None);
        assert_eq!(verdict.late_by_seconds, None);
        assert_eq!(verdict.next_due_at, Some(utc(2026, 5, 2, 6, 0)));
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

    /// A run that built `model`: the run starts at `started`, the model
    /// finishes `minutes` later.
    fn delivery_run(
        id: &str,
        started: DateTime<Utc>,
        minutes: i64,
        status: &str,
        scope: crate::state::RunScope,
    ) -> RunRecord {
        let finished = started + chrono::Duration::minutes(minutes);
        let mut record = bare_record(started, None);
        record.run_id = id.to_string();
        record.finished_at = finished;
        record.pipeline = Some("revenue".to_string());
        record.run_scope = Some(scope);
        record.models_executed = vec![
            serde_json::from_value(serde_json::json!({
                "model_name": "revenue_daily",
                "started_at": started,
                "finished_at": finished,
                "duration_ms": 1,
                "rows_affected": null,
                "status": status,
                "sql_hash": "h",
                "bytes_scanned": null,
                "bytes_written": null,
            }))
            .unwrap(),
        ];
        record
    }

    #[test]
    fn the_store_estimate_reads_production_successes_only() {
        use crate::state::RunScope;
        let dir = tempfile::tempdir().unwrap();
        let store = StateStore::open(&dir.path().join("state.redb")).unwrap();
        for day in 1..=4 {
            store
                .record_run(&delivery_run(
                    &format!("prod-{day}"),
                    utc(2026, 5, day, 5, 0),
                    10 * i64::from(day),
                    "success",
                    RunScope::Production,
                ))
                .unwrap();
        }
        // Four production runs: below the minimum, no band.
        let estimate = store
            .get_duration_estimate("revenue_daily", UnrecordedScope::Exclude)
            .unwrap();
        assert_eq!((estimate.n, estimate.band), (4, None));

        // A shadow run and a failed execution do not count.
        store
            .record_run(&delivery_run(
                "shadow",
                utc(2026, 5, 5, 5, 0),
                500,
                "success",
                RunScope::Shadow { schema: None },
            ))
            .unwrap();
        store
            .record_run(&delivery_run(
                "failed",
                utc(2026, 5, 6, 5, 0),
                500,
                "failed",
                RunScope::Production,
            ))
            .unwrap();
        let estimate = store
            .get_duration_estimate("revenue_daily", UnrecordedScope::Exclude)
            .unwrap();
        assert_eq!(estimate.n, 4, "shadow and failed builds are not deliveries");

        store
            .record_run(&delivery_run(
                "prod-7",
                utc(2026, 5, 7, 5, 0),
                50,
                "success",
                RunScope::Production,
            ))
            .unwrap();
        let estimate = store
            .get_duration_estimate("revenue_daily", UnrecordedScope::Exclude)
            .unwrap();
        assert_eq!(estimate.n, 5);
        let band = estimate.band.expect("five production runs give a band");
        assert_eq!(band.p50_ms, 30 * 60 * 1000);
        assert_eq!(band.p90_ms, 50 * 60 * 1000);

        let pipeline = store
            .get_pipeline_duration_estimate("revenue", UnrecordedScope::Exclude)
            .unwrap();
        // The failed execution's run still has status Success here, so the
        // pipeline sees six production runs; the shadow run stays out.
        assert_eq!(pipeline.n, 6);
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

    fn lisbon_0130() -> DeliverBy {
        DeliverBy {
            cron: "30 1 * * *".to_string(),
            tz: chrono_tz::Tz::Europe__Lisbon,
        }
    }

    fn at(y: i32, mo: u32, d: u32, h: u32, mi: u32) -> LastDelivery {
        LastDelivery {
            finished_at: utc(y, mo, d, h, mi),
            due_at: None,
        }
    }

    /// 2026-03-29, Europe/Lisbon: 01:00 WET jumps to 02:00 WEST, so 01:30
    /// local does not exist. The cron library moves that day's deadline to
    /// 02:00 local (01:00 UTC). Defensible but surprising: the deadline is
    /// the end of the gap, one occurrence that day, none skipped.
    #[test]
    fn spring_forward_moves_a_nonexistent_deadline_to_the_end_of_the_gap() {
        let d = lisbon_0130();
        let prev_due = utc(2026, 3, 28, 1, 30);
        let gap_due = utc(2026, 3, 29, 1, 0);
        let next_due = utc(2026, 3, 30, 0, 30);
        assert_eq!(due_after(&d, prev_due), Some(gap_due));
        assert_eq!(due_after(&d, gap_due), Some(next_due));

        let previous = Some(LastDelivery {
            finished_at: utc(2026, 3, 28, 1, 10),
            due_at: Some(prev_due),
        });
        // Early and exactly on the moved deadline: serves it.
        for finished in [utc(2026, 3, 29, 0, 50), gap_due] {
            assert_eq!(deadline_served(&d, previous, finished), Some(gap_due));
        }
        // After it: late for the moved deadline, never for the next.
        let late = utc(2026, 3, 29, 1, 10);
        assert_eq!(deadline_served(&d, previous, late), Some(gap_due));
        let late_delivery = LastDelivery {
            finished_at: late,
            due_at: Some(gap_due),
        };
        assert_eq!(open_deadline_after(&d, late_delivery), Some(next_due));
        // No recorded deadline: it covered the first deadline at or after
        // its finish, one per day.
        assert_eq!(
            open_deadline_after(&d, at(2026, 3, 29, 0, 50)),
            Some(next_due)
        );
        assert_eq!(
            open_deadline_after(&d, at(2026, 3, 29, 1, 10)),
            Some(utc(2026, 3, 31, 0, 30))
        );
    }

    /// 2026-10-25, Europe/Lisbon: 02:00 WEST falls back to 01:00 WET, so
    /// 01:30 local happens twice (00:30 UTC and 01:30 UTC). The cron library
    /// yields the first only. Defensible but surprising: one deadline that
    /// day at 00:30 UTC; the repeated 01:30 is not a second deadline.
    #[test]
    fn fall_back_counts_a_repeated_deadline_once() {
        let d = lisbon_0130();
        let prev_due = utc(2026, 10, 24, 0, 30);
        let first = utc(2026, 10, 25, 0, 30);
        let next_due = utc(2026, 10, 26, 1, 30);
        assert_eq!(due_after(&d, prev_due), Some(first));
        assert_eq!(due_after(&d, first), Some(next_due));

        let previous = Some(LastDelivery {
            finished_at: utc(2026, 10, 24, 0, 20),
            due_at: Some(prev_due),
        });
        assert_eq!(
            deadline_served(&d, previous, utc(2026, 10, 25, 0, 20)),
            Some(first)
        );
        // Finishing between the two 01:30s, or after both: late for the
        // first, and the repeated 01:30 UTC is neither skipped into nor
        // counted again.
        for finished in [
            utc(2026, 10, 25, 0, 45),
            utc(2026, 10, 25, 1, 10),
            utc(2026, 10, 25, 1, 45),
        ] {
            assert_eq!(deadline_served(&d, previous, finished), Some(first));
            let delivery = LastDelivery {
                finished_at: finished,
                due_at: Some(first),
            };
            assert_eq!(open_deadline_after(&d, delivery), Some(next_due));
            let unrecorded = LastDelivery {
                finished_at: finished,
                due_at: None,
            };
            // Covers the first deadline at or after its finish: the 26th.
            assert_eq!(
                open_deadline_after(&d, unrecorded),
                Some(utc(2026, 10, 27, 1, 30))
            );
        }
    }

    #[test]
    fn a_run_persisted_twice_is_not_its_own_previous_delivery() {
        use crate::state::RunScope;
        let dir = tempfile::tempdir().unwrap();
        let store = StateStore::open(&dir.path().join("state.redb")).unwrap();
        let deadlines = [ProductDeadline {
            product: "revenue".to_string(),
            output_model: "revenue_daily".to_string(),
            deliver_by: six_am(),
        }];
        // Day 1 delivered on time, due 06:00.
        let mut day1 = delivery_run(
            "day-1",
            utc(2026, 5, 1, 5, 0),
            30,
            "success",
            RunScope::Production,
        );
        stamp_delivery_timing(&mut day1, &store, &deadlines).unwrap();
        store.record_run(&day1).unwrap();
        // Day 2 finishes at 07:30: late for the 06:00 deadline.
        let mut day2 = delivery_run(
            "day-2",
            utc(2026, 5, 2, 7, 0),
            30,
            "success",
            RunScope::Production,
        );
        stamp_delivery_timing(&mut day2, &store, &deadlines).unwrap();
        let due = utc(2026, 5, 2, 6, 0);
        assert_eq!(day2.models_executed[0].due_at, Some(due));
        // The run is persisted, then persisted again under the same id (a
        // failed `verify_after`). The second stamp must stay late.
        store.record_run(&day2).unwrap();
        stamp_delivery_timing(&mut day2, &store, &deadlines).unwrap();
        assert_eq!(
            day2.models_executed[0].due_at,
            Some(due),
            "a second stamp must not re-cover the next deadline"
        );
        assert!(day2.models_executed[0].finished_at > due);
    }

    // ----- the latest start -----

    const MIN: u64 = 60_000;

    fn node(
        after: &[&str],
        p90_min: Option<u64>,
        deadline: Option<(&str, DateTime<Utc>)>,
    ) -> ChainNode {
        ChainNode {
            after: after.iter().map(|s| (*s).to_string()).collect(),
            p90_ms: p90_min.map(|m| m * MIN),
            deadline: deadline.map(|(product, due)| DirectDeadline {
                product: product.to_string(),
                due_at: Some(due),
            }),
            serves_unknown: false,
        }
    }

    fn graph(nodes: Vec<(&str, ChainNode)>) -> std::collections::BTreeMap<String, ChainNode> {
        nodes
            .into_iter()
            .map(|(name, node)| (name.to_string(), node))
            .collect()
    }

    /// `raw` feeds two products: `revenue` due 06:00 (p90 10 min) and
    /// `costs` due 04:00 (p90 30 min). The earlier start-by wins:
    /// min(05:50, 03:30) - 20 min = 03:10, bound by `costs`.
    #[test]
    fn the_earliest_of_two_downstream_deadlines_wins() {
        let nodes = graph(vec![
            ("raw", node(&[], Some(20), None)),
            (
                "revenue",
                node(
                    &["raw"],
                    Some(10),
                    Some(("revenue_daily", utc(2026, 5, 6, 6, 0))),
                ),
            ),
            (
                "costs",
                node(
                    &["raw"],
                    Some(30),
                    Some(("costs_daily", utc(2026, 5, 6, 4, 0))),
                ),
            ),
            ("unrelated", node(&[], Some(5), None)),
        ]);
        let starts = latest_starts(&nodes);
        assert_eq!(
            starts.get("raw"),
            Some(&LatestStart::Known {
                at: utc(2026, 5, 6, 3, 10),
                product: "costs_daily".to_string(),
                due_at: utc(2026, 5, 6, 4, 0),
            })
        );
        assert_eq!(
            starts.get("revenue"),
            Some(&LatestStart::Known {
                at: utc(2026, 5, 6, 5, 50),
                product: "revenue_daily".to_string(),
                due_at: utc(2026, 5, 6, 6, 0),
            })
        );
        assert!(
            !starts.contains_key("unrelated"),
            "a pipeline that feeds no deadline has no latest start"
        );
    }

    /// A node that serves a deadline AND feeds a later one downstream takes
    /// the min of both terms.
    #[test]
    fn a_direct_deadline_and_a_downstream_one_take_the_min() {
        let nodes = graph(vec![
            (
                "mid",
                node(&[], Some(10), Some(("early", utc(2026, 5, 6, 5, 0)))),
            ),
            (
                "last",
                node(&["mid"], Some(10), Some(("late", utc(2026, 5, 6, 9, 0)))),
            ),
        ]);
        let starts = latest_starts(&nodes);
        let Some(LatestStart::Known { at, product, .. }) = starts.get("mid") else {
            panic!("{starts:?}");
        };
        assert_eq!(*at, utc(2026, 5, 6, 4, 50));
        assert_eq!(product, "early");
    }

    /// One missing p90 anywhere on a chain makes every node above it
    /// unknown, even when another chain from the same node is known.
    #[test]
    fn a_missing_p90_on_any_chain_makes_the_latest_start_unknown() {
        let nodes = graph(vec![
            ("raw", node(&[], Some(20), None)),
            (
                "revenue",
                node(
                    &["raw"],
                    Some(10),
                    Some(("revenue_daily", utc(2026, 5, 6, 6, 0))),
                ),
            ),
            (
                "costs",
                node(&["raw"], None, Some(("costs_daily", utc(2026, 5, 6, 4, 0)))),
            ),
        ]);
        let starts = latest_starts(&nodes);
        let unknown_costs = LatestStart::Unknown {
            reasons: vec![UnknownReason::MissingEstimate],
            missing_estimate: vec!["costs".to_string()],
        };
        assert_eq!(starts.get("raw"), Some(&unknown_costs));
        assert_eq!(starts.get("costs"), Some(&unknown_costs));
        assert!(matches!(
            starts.get("revenue"),
            Some(LatestStart::Known { .. })
        ));

        // The node's own p90 missing counts the same way.
        let nodes = graph(vec![
            ("raw", node(&[], None, None)),
            (
                "revenue",
                node(
                    &["raw"],
                    Some(10),
                    Some(("revenue_daily", utc(2026, 5, 6, 6, 0))),
                ),
            ),
        ]);
        assert_eq!(
            latest_starts(&nodes).get("raw"),
            Some(&LatestStart::Unknown {
                reasons: vec![UnknownReason::MissingEstimate],
                missing_estimate: vec!["raw".to_string()],
            })
        );
    }

    /// A cycle on the way to a deadline cannot be walked: it reads as
    /// unknown with the reason `Cycle`, not as a number.
    #[test]
    fn a_cycle_reads_as_unknown() {
        let nodes = graph(vec![
            (
                "a",
                node(&["b"], Some(10), Some(("p", utc(2026, 5, 6, 6, 0)))),
            ),
            ("b", node(&["a"], Some(10), None)),
        ]);
        let starts = latest_starts(&nodes);
        for name in ["a", "b"] {
            let Some(LatestStart::Unknown {
                reasons,
                missing_estimate,
            }) = starts.get(name)
            else {
                panic!("{name}: {starts:?}");
            };
            assert_eq!(reasons, &vec![UnknownReason::Cycle], "{name}");
            assert!(missing_estimate.is_empty(), "{name}");
        }
    }

    /// A cycle with no deadline anywhere downstream feeds nothing: no entry,
    /// not an unknown one.
    #[test]
    fn a_cycle_with_no_deadline_gives_no_entry() {
        let nodes = graph(vec![
            ("a", node(&["b"], Some(10), None)),
            ("b", node(&["a"], Some(10), None)),
            ("p", node(&[], Some(10), Some(("p", utc(2026, 5, 6, 6, 0))))),
        ]);
        let starts = latest_starts(&nodes);
        assert!(!starts.contains_key("a"), "{starts:?}");
        assert!(!starts.contains_key("b"), "{starts:?}");
        assert!(matches!(starts.get("p"), Some(LatestStart::Known { .. })));
    }

    /// A p90 that pushes the start-by time out of the date range is
    /// `Overflow`, never a wrapped time.
    #[test]
    fn a_subtraction_out_of_range_reads_as_overflow() {
        let nodes = graph(vec![(
            "p",
            node(&[], Some(10), Some(("p", DateTime::<Utc>::MIN_UTC))),
        )]);
        assert_eq!(
            latest_starts(&nodes).get("p"),
            Some(&LatestStart::Unknown {
                reasons: vec![UnknownReason::Overflow],
                missing_estimate: Vec::new(),
            })
        );
    }

    /// A deadline with no next occurrence is `NoNextDeadline`; a node whose
    /// models could not be read is `ModelsUnreadable`. Both carry upstream,
    /// beside a missing p90, each reason listed once, sorted.
    #[test]
    fn every_unknown_reason_on_the_way_is_listed() {
        let mut no_next = node(&["raw"], Some(10), None);
        no_next.deadline = Some(DirectDeadline {
            product: "p".to_string(),
            due_at: None,
        });
        let mut unreadable = node(&["raw"], None, None);
        unreadable.serves_unknown = true;
        let nodes = graph(vec![
            ("raw", node(&[], Some(20), None)),
            ("no_next", no_next),
            ("unreadable", unreadable),
        ]);
        let starts = latest_starts(&nodes);
        assert_eq!(
            starts.get("raw"),
            Some(&LatestStart::Unknown {
                reasons: vec![
                    UnknownReason::MissingEstimate,
                    UnknownReason::NoNextDeadline,
                    UnknownReason::ModelsUnreadable,
                ],
                missing_estimate: vec!["unreadable".to_string()],
            })
        );
        assert_eq!(
            starts.get("no_next"),
            Some(&LatestStart::Unknown {
                reasons: vec![UnknownReason::NoNextDeadline],
                missing_estimate: Vec::new(),
            })
        );
    }
}
