//! The per-demand claim state machine — the soundness heart of the reconciler.
//!
//! Every due demand takes a **claim** before its child is spawned. The claim is
//! a small record in redb, transitioned by compare-and-swap inside a write
//! transaction, so two reconcilers racing the same demand cannot both spawn:
//! redb serializes writers, exactly one lands the `submitted` transition, and a
//! reconciler whose CAS fails has *lost ownership* and stands down.
//!
//! This module holds only the **pure decisions**. The redb CAS that applies
//! them lives on [`crate::state::StateStore`]; keeping the decision logic here,
//! free of any transaction, is what makes the state machine exhaustively
//! unit-testable — the regression matrix below is the executable spec.
//!
//! The three decision points:
//!
//! - [`decide_pre_spawn`] — before spawning: claim an absent or re-claimable
//!   key (`released`), refuse to spawn over a live `submitted` (resolve it
//!   first) or a terminal `completed`.
//! - [`decide_post_attempt`] — after a child returns *in the same tick*: either
//!   an in-tick retry transition (`submitted{id_n} -> submitted{id_n+1}`) while
//!   budget remains, or a terminal transition on exhaustion.
//! - [`decide_resolver`] — for a `submitted` claim re-encountered on a later
//!   tick (its owner crashed): budget-aware, it continues the retry budget
//!   across the crash rather than finalizing after one of N attempts.
//!
//! A claim is `submitted` (a run is in flight for this key), `released` (the
//! attempt was voided or is retryable — the key may be re-claimed), or
//! `completed` (a terminal run was observed — the key must NEVER run again).
//! Conflating `released` and `completed` is the crash-duplicate bug: recovery
//! re-promotes anything it reads as merely released.

use chrono::{DateTime, Utc};

/// The demand source a claim belongs to. Governs the terminal transition: a
/// spent cron/webhook demand is `completed` (its next fire is a new key), while
/// a standing `after`/`freshness` demand that failed goes back to `released`
/// so its next backoff window can re-claim it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DemandKind {
    /// A cron occurrence. The anchor advances on submission; the next
    /// occurrence is a fresh key.
    Cron,
    /// An `after` dependency trigger. Standing until its own run succeeds.
    After,
    /// A freshness (run-staleness) trigger. Standing until a run succeeds.
    Freshness,
    /// A webhook delivery (reserved for the resident-loop phase). One-shot:
    /// consumed by being attempted.
    Webhook,
}

impl DemandKind {
    /// The stable source token used in the claim storage key.
    pub fn as_str(self) -> &'static str {
        match self {
            DemandKind::Cron => "cron",
            DemandKind::After => "after",
            DemandKind::Freshness => "freshness",
            DemandKind::Webhook => "webhook",
        }
    }

    /// Whether this demand is *standing* — a source that keeps demanding until a
    /// run actually succeeds (`after`, `freshness`). Cron and webhook are
    /// one-shot per key.
    pub fn is_standing(self) -> bool {
        matches!(self, DemandKind::After | DemandKind::Freshness)
    }
}

/// The terminal outcome of an attempt, from the child's exit code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminalOutcome {
    /// Exit 0.
    Success,
    /// Exit 2 — a partial success. Does NOT satisfy downstream `after`; does
    /// clear the emitter's own cron demand for that occurrence; is never
    /// retried; neither increments nor resets `consecutive_failures`.
    Partial,
    /// Exit 1 / any other — an infrastructure failure. Retryable within budget;
    /// increments `consecutive_failures` on exhaustion.
    Failure,
}

impl TerminalOutcome {
    /// Map a child process exit code to an outcome.
    pub fn from_exit_code(code: i32) -> Self {
        match code {
            0 => TerminalOutcome::Success,
            2 => TerminalOutcome::Partial,
            _ => TerminalOutcome::Failure,
        }
    }

    /// The string recorded in `ScheduleStateRecord.last_attempt_outcome`.
    pub fn as_str(self) -> &'static str {
        match self {
            TerminalOutcome::Success => "success",
            TerminalOutcome::Partial => "partial",
            TerminalOutcome::Failure => "failure",
        }
    }
}

/// The lifecycle state of a claim.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", tag = "state")]
pub enum ClaimState {
    /// A run is (or was) in flight for this key under `submission_id`.
    Submitted,
    /// The attempt was voided or is retryable — this key may be re-claimed.
    /// `budget_open` on the record decides whether the re-claim continues the
    /// current retry cycle or starts a fresh one.
    Released,
    /// A terminal run was observed and this key is finished — it must NEVER run
    /// again.
    Completed {
        /// The observed terminal outcome.
        outcome: TerminalOutcome,
    },
}

/// A claim record: the authoritative, per-key mutable state of the reconciler.
///
/// Every field beyond the first three is serde-defaulted so a record written by
/// an older binary — or a future one that adds a field — round-trips cleanly.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ClaimRecord {
    /// The lifecycle state (`submitted` / `released` / `completed`).
    #[serde(flatten)]
    pub state: ClaimState,
    /// The current attempt's submission id (a fresh UUID per submission). The
    /// precise join key between a demand and *its* run record.
    pub submission_id: String,
    /// Monotonic audit counter of total submissions on this key. Never consulted
    /// by any budget guard.
    #[serde(default)]
    pub attempts: u32,
    /// The authoritative retry-budget ordinal: submissions in the current retry
    /// cycle INCLUDING the one in flight. Initialized to 1 on insert. The sole
    /// input to every budget guard (`cycle_attempts < 1 + retry.max`).
    #[serde(default)]
    pub cycle_attempts: u32,
    /// Set on every `released` transition: `true` = the retry cycle continues
    /// (a re-claim does `cycle_attempts + 1`); `false` = the cycle is over
    /// (exhaustion/backoff), so a re-claim resets `cycle_attempts` to 1.
    #[serde(default)]
    pub budget_open: bool,
    /// The spawned child's PID, recorded for the recovery sweep's liveness
    /// probe. `None` until a child is spawned (or on platforms without a probe).
    #[serde(default)]
    pub child_pid: Option<u32>,
    /// The spawned child's process start time, paired with `child_pid` to make
    /// a later liveness probe PID-reuse-proof. Populated by the recovery phase.
    #[serde(default)]
    pub child_start_time: Option<u64>,
    /// When a stuck `submitted` claim was first re-encountered by a sweep, so a
    /// bounded grace can elapse before it is force-resolved. RFC3339.
    #[serde(default)]
    pub first_swept_at: Option<String>,
    /// The instant of the last transition. RFC3339. Drives the 7-day GC of
    /// terminal claims.
    #[serde(default)]
    pub last_transition_at: Option<String>,
}

impl ClaimRecord {
    /// A fresh `submitted` claim starting a new retry cycle (`cycle_attempts`
    /// = 1, `attempts` = 1).
    pub fn new_submitted(submission_id: String, now: DateTime<Utc>) -> Self {
        Self {
            state: ClaimState::Submitted,
            submission_id,
            attempts: 1,
            cycle_attempts: 1,
            budget_open: false,
            child_pid: None,
            child_start_time: None,
            first_swept_at: None,
            last_transition_at: Some(now.to_rfc3339()),
        }
    }

    /// True when the claim is terminal (`completed`).
    pub fn is_completed(&self) -> bool {
        matches!(self.state, ClaimState::Completed { .. })
    }
}

/// Parse a claim storage key back into its parts.
///
/// The key is `<pipeline>\u{1f}<source>\u{1f}<discriminator>`. Returns `None`
/// only for a genuinely unrecognized shape — the orphan sweep skips a key it
/// cannot interpret rather than acting on it.
///
/// The discriminator is **not always a timestamp**: it is the `logical_ts`
/// (RFC3339) for cron/after/freshness, but a minted `demand_uid` for a webhook
/// demand. So it is returned as an `Option` and parsed only when it really is
/// an instant. Treating an unparseable discriminator as a malformed key would
/// make [`crate::schedule::sweep_orphan_claims`] skip every webhook claim,
/// leaving a `submitted` webhook claim stuck forever — precisely the class of
/// leak that sweep exists to clear.
pub fn parse_claim_key(key: &str) -> Option<(String, DemandKind, Option<DateTime<Utc>>)> {
    let mut parts = key.split('\u{1f}');
    let pipeline = parts.next()?.to_string();
    let source = match parts.next()? {
        "cron" => DemandKind::Cron,
        "after" => DemandKind::After,
        "freshness" => DemandKind::Freshness,
        "webhook" => DemandKind::Webhook,
        _ => return None,
    };
    let logical_ts = parts.next().and_then(|d| {
        DateTime::parse_from_rfc3339(d)
            .ok()
            .map(|dt| dt.with_timezone(&Utc))
    });
    Some((pipeline, source, logical_ts))
}

/// The outcome of a claim compare-and-swap against the store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClaimCas {
    /// The CAS matched the observed prior and the new claim (plus any cursor
    /// mutation) was committed. The caller owns the demand and may spawn.
    Won,
    /// The stored claim no longer equals what was observed — another reconciler
    /// moved it. The caller has lost ownership: no spawn, no further writes for
    /// this demand this pass. Carries the current stored claim, if any.
    Lost(Option<ClaimRecord>),
}

/// Whether the retry budget has room for another attempt, given the count of
/// submissions already made in this cycle (including the in-flight one) and the
/// configured maximum number of *additional* re-submissions.
///
/// `cycle_attempts < 1 + retry_max`. With `retry_max = 0`, a freshly-inserted
/// claim (`cycle_attempts = 1`) reads `1 < 1` = false — the first attempt is
/// the only one.
pub fn budget_remains(cycle_attempts: u32, retry_max: u32) -> bool {
    cycle_attempts < 1 + retry_max
}

/// The pre-spawn decision for a due demand, given the claim currently stored
/// (if any) and a freshly-minted submission id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PreSpawn {
    /// Claim the key: CAS-insert this `submitted` record, then spawn. The CAS
    /// asserts the stored claim still equals what was observed; a lost CAS is a
    /// stand-down (no spawn).
    Claim(ClaimRecord),
    /// The key is already `submitted` — do not spawn over it; run the resolver
    /// against the identity table first.
    ResolveStuck,
    /// The key is `completed` — terminal, never spawn.
    SkipCompleted,
}

/// Decide the pre-spawn transition. Pure — the CAS that applies it is on the
/// state store.
///
/// - absent ⇒ insert `submitted` (`cycle_attempts = 1`).
/// - `released{budget_open = true}` ⇒ re-claim continuing the cycle
///   (`cycle_attempts + 1`).
/// - `released{budget_open = false}` ⇒ re-claim resetting the cycle
///   (`cycle_attempts = 1`).
/// - `submitted` ⇒ [`PreSpawn::ResolveStuck`].
/// - `completed` ⇒ [`PreSpawn::SkipCompleted`].
pub fn decide_pre_spawn(
    observed: Option<&ClaimRecord>,
    fresh_submission_id: String,
    now: DateTime<Utc>,
) -> PreSpawn {
    match observed {
        None => PreSpawn::Claim(ClaimRecord::new_submitted(fresh_submission_id, now)),
        Some(prior) => match &prior.state {
            ClaimState::Completed { .. } => PreSpawn::SkipCompleted,
            ClaimState::Submitted => PreSpawn::ResolveStuck,
            ClaimState::Released => {
                let cycle_attempts = if prior.budget_open {
                    prior.cycle_attempts + 1
                } else {
                    1
                };
                PreSpawn::Claim(ClaimRecord {
                    state: ClaimState::Submitted,
                    submission_id: fresh_submission_id,
                    attempts: prior.attempts + 1,
                    cycle_attempts,
                    budget_open: false,
                    child_pid: None,
                    child_start_time: None,
                    first_swept_at: None,
                    last_transition_at: Some(now.to_rfc3339()),
                })
            }
        },
    }
}

/// How a terminal transition should move `consecutive_failures`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CfDelta {
    /// Failure on exhaustion — `consecutive_failures += 1`.
    Increment,
    /// Success — `consecutive_failures = 0`.
    Reset,
    /// Partial — neither increment nor reset.
    Unchanged,
}

/// Bookkeeping to apply to the pipeline's `ScheduleStateRecord` alongside a
/// terminal claim transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Bookkeeping {
    /// The outcome recorded in `last_attempt_outcome`.
    pub outcome: TerminalOutcome,
    /// How `consecutive_failures` moves.
    pub cf_delta: CfDelta,
}

/// The decision after an in-tick attempt returns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PostAttempt {
    /// Budget remains after a failure — the in-tick retry transition
    /// `submitted{id_n} -> submitted{id_n+1}`. Spawn again immediately; no
    /// `consecutive_failures`/backoff bookkeeping yet.
    Retry(ClaimRecord),
    /// The demand is exhausted — apply the terminal claim transition together
    /// with the bookkeeping, in one write.
    Terminal {
        /// The terminal claim record to store.
        claim: ClaimRecord,
        /// The `ScheduleStateRecord` bookkeeping to apply in the same write.
        bookkeeping: Bookkeeping,
    },
}

/// Compute the terminal claim state for a demand kind + outcome (frozen table).
///
/// - cron / webhook: any outcome ⇒ `completed` (the next fire is a new key).
/// - after / freshness: success ⇒ `completed`; failure | partial ⇒ `released`
///   with `budget_open = false` (the demand is still standing and must be
///   re-claimable when its backoff elapses).
fn terminal_state(kind: DemandKind, outcome: TerminalOutcome) -> ClaimState {
    match kind {
        DemandKind::Cron | DemandKind::Webhook => ClaimState::Completed { outcome },
        DemandKind::After | DemandKind::Freshness => match outcome {
            TerminalOutcome::Success => ClaimState::Completed { outcome },
            TerminalOutcome::Failure | TerminalOutcome::Partial => ClaimState::Released,
        },
    }
}

fn bookkeeping_for(outcome: TerminalOutcome) -> Bookkeeping {
    let cf_delta = match outcome {
        TerminalOutcome::Success => CfDelta::Reset,
        TerminalOutcome::Failure => CfDelta::Increment,
        TerminalOutcome::Partial => CfDelta::Unchanged,
    };
    Bookkeeping { outcome, cf_delta }
}

fn terminal_claim(
    current: &ClaimRecord,
    kind: DemandKind,
    outcome: TerminalOutcome,
    now: DateTime<Utc>,
) -> ClaimRecord {
    let state = terminal_state(kind, outcome);
    ClaimRecord {
        state,
        submission_id: current.submission_id.clone(),
        attempts: current.attempts,
        cycle_attempts: current.cycle_attempts,
        // A terminal `released` (standing demand, failure/partial) closes the
        // cycle so the next backoff-driven re-claim resets `cycle_attempts`.
        budget_open: false,
        child_pid: current.child_pid,
        child_start_time: current.child_start_time,
        first_swept_at: None,
        last_transition_at: Some(now.to_rfc3339()),
    }
}

/// The terminal claim state for an orphaned run, computed DIRECTLY from the
/// source + outcome — no retry-budget logic.
///
/// The stuck-claim sweep uses this instead of [`decide_resolver`]: the resolver
/// consults the retry budget (`budget_remains(cycle_attempts, retry_max)`), and
/// a claim whose `cycle_attempts` is a serde-defaulted `0` would take the
/// `ReleasedBudgetOpen` branch even at `retry_max = 0` (`0 < 1`), leaving the
/// orphan un-terminalized. An orphan has already run to a terminal outcome, so
/// it is unconditionally terminal regardless of budget; this computes exactly
/// that (cron/webhook → completed; standing success → completed, standing
/// failure/partial → released), so the sweep never leaks a claim.
pub fn sweep_terminal_claim(
    current: &ClaimRecord,
    kind: DemandKind,
    outcome: TerminalOutcome,
    now: DateTime<Utc>,
) -> ClaimRecord {
    terminal_claim(current, kind, outcome, now)
}

/// Decide the transition after an in-tick attempt returns with `outcome`.
///
/// A `failure` with budget remaining is an in-tick [`PostAttempt::Retry`]; any
/// other outcome — success, partial, or an exhausted failure — is terminal.
pub fn decide_post_attempt(
    current: &ClaimRecord,
    outcome: TerminalOutcome,
    kind: DemandKind,
    retry_max: u32,
    fresh_submission_id: String,
    now: DateTime<Utc>,
) -> PostAttempt {
    let retryable =
        outcome == TerminalOutcome::Failure && budget_remains(current.cycle_attempts, retry_max);
    if retryable {
        return PostAttempt::Retry(ClaimRecord {
            state: ClaimState::Submitted,
            submission_id: fresh_submission_id,
            attempts: current.attempts + 1,
            cycle_attempts: current.cycle_attempts + 1,
            budget_open: false,
            child_pid: None,
            child_start_time: None,
            first_swept_at: None,
            last_transition_at: Some(now.to_rfc3339()),
        });
    }
    PostAttempt::Terminal {
        claim: terminal_claim(current, kind, outcome, now),
        bookkeeping: bookkeeping_for(outcome),
    }
}

/// The resolver decision for a stuck `submitted` claim re-encountered on a later
/// tick (its owner crashed between spawn and the next transition).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Resolved {
    /// A failure with budget remaining: the retry budget continues across the
    /// crash. Store this `released{budget_open = true}` with NO
    /// `consecutive_failures`/backoff — the next tick re-claims and continues
    /// the cycle. This is what the plain terminal table would have wrongly
    /// suppressed one state over.
    ReleasedBudgetOpen(ClaimRecord),
    /// The attempt is exhausted (or a success/partial). Apply the terminal
    /// transition + bookkeeping, exactly as the in-tick exhaustion path would.
    Terminal {
        /// The terminal claim record to store.
        claim: ClaimRecord,
        /// The bookkeeping to apply in the same write.
        bookkeeping: Bookkeeping,
    },
}

/// Decide how to resolve a stuck `submitted` claim, given the outcome the
/// resolver derived (from a terminal run record carrying the claim's
/// `submission_id`, or a verified-dead child treated as `failure`).
///
/// Budget-aware: a `failure` with `cycle_attempts < 1 + retry_max` continues the
/// budget cross-tick ([`Resolved::ReleasedBudgetOpen`], no `consecutive_failures`);
/// an exhausted failure — or a partial/success — is terminal. Routing a
/// budget-remaining failure straight to the terminal table would silently
/// suppress configured retries across a crash.
pub fn decide_resolver(
    current: &ClaimRecord,
    outcome: TerminalOutcome,
    kind: DemandKind,
    retry_max: u32,
    now: DateTime<Utc>,
) -> Resolved {
    let budget_open_failure =
        outcome == TerminalOutcome::Failure && budget_remains(current.cycle_attempts, retry_max);
    if budget_open_failure {
        return Resolved::ReleasedBudgetOpen(ClaimRecord {
            state: ClaimState::Released,
            submission_id: current.submission_id.clone(),
            attempts: current.attempts,
            // Preserve the cycle ordinal so the next re-claim (budget_open =
            // true) continues it (`cycle_attempts + 1`).
            cycle_attempts: current.cycle_attempts,
            budget_open: true,
            child_pid: current.child_pid,
            child_start_time: current.child_start_time,
            first_swept_at: None,
            last_transition_at: Some(now.to_rfc3339()),
        });
    }
    Resolved::Terminal {
        claim: terminal_claim(current, kind, outcome, now),
        bookkeeping: bookkeeping_for(outcome),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 5, 1, 0, 0, 0).unwrap()
    }

    fn submitted(id: &str, attempts: u32, cycle: u32) -> ClaimRecord {
        ClaimRecord {
            state: ClaimState::Submitted,
            submission_id: id.to_string(),
            attempts,
            cycle_attempts: cycle,
            budget_open: false,
            child_pid: None,
            child_start_time: None,
            first_swept_at: None,
            last_transition_at: Some(now().to_rfc3339()),
        }
    }

    // --- claim-key parsing --------------------------------------------------

    #[test]
    fn parse_claim_key_reads_a_timestamped_key() {
        let (pipeline, source, ts) =
            parse_claim_key("sales\u{1f}cron\u{1f}2026-05-01T00:00:00+00:00").expect("parses");
        assert_eq!(pipeline, "sales");
        assert_eq!(source, DemandKind::Cron);
        assert_eq!(ts, Some(now()));
    }

    /// A webhook claim key's third component is a minted `demand_uid`, NOT a
    /// timestamp. Parsing must still succeed with `logical_ts: None` — a parser
    /// that rejected the key outright would make `sweep_orphan_claims` skip
    /// every webhook claim, leaving a `submitted` claim stuck forever.
    #[test]
    fn parse_claim_key_accepts_a_webhook_uid_discriminator() {
        let (pipeline, source, ts) =
            parse_claim_key("sales\u{1f}webhook\u{1f}0b9c1d2e-3f40-4a5b-8c6d-7e8f90a1b2c3")
                .expect("a webhook key is well-formed, not malformed");
        assert_eq!(pipeline, "sales");
        assert_eq!(source, DemandKind::Webhook);
        assert_eq!(ts, None, "a demand_uid is not an instant");
    }

    #[test]
    fn parse_claim_key_rejects_an_unknown_source() {
        assert!(parse_claim_key("sales\u{1f}telepathy\u{1f}whenever").is_none());
        assert!(parse_claim_key("sales").is_none());
    }

    // --- budget guard (attempt-accounting matrix, regression viii) ---------

    #[test]
    fn budget_guard_off_by_one_seed() {
        // retry.max = 0: a freshly-inserted claim (cycle_attempts = 1) has NO
        // budget — the first failure is exhaustion. A 0-seeded counter would
        // read 0 < 1 and fire a phantom second run.
        assert!(!budget_remains(1, 0), "seed=1, max=0 must be exhausted");
        // retry.max = 2: cycles 1 and 2 have budget, cycle 3 does not.
        assert!(budget_remains(1, 2));
        assert!(budget_remains(2, 2));
        assert!(!budget_remains(3, 2));
    }

    // --- pre-spawn -----------------------------------------------------------

    #[test]
    fn pre_spawn_absent_inserts_submitted_seeded_at_one() {
        let d = decide_pre_spawn(None, "s1".into(), now());
        let PreSpawn::Claim(rec) = d else {
            panic!("expected Claim")
        };
        assert_eq!(rec.state, ClaimState::Submitted);
        assert_eq!(rec.attempts, 1);
        assert_eq!(rec.cycle_attempts, 1);
    }

    #[test]
    fn pre_spawn_completed_never_spawns() {
        let prior = ClaimRecord {
            state: ClaimState::Completed {
                outcome: TerminalOutcome::Success,
            },
            ..submitted("s1", 1, 1)
        };
        assert_eq!(
            decide_pre_spawn(Some(&prior), "s2".into(), now()),
            PreSpawn::SkipCompleted
        );
    }

    #[test]
    fn pre_spawn_submitted_resolves_never_spawns_over() {
        let prior = submitted("s1", 1, 1);
        assert_eq!(
            decide_pre_spawn(Some(&prior), "s2".into(), now()),
            PreSpawn::ResolveStuck
        );
    }

    #[test]
    fn pre_spawn_released_budget_open_continues_cycle() {
        // regression viii: crash-recovery re-claim CONTINUES the cycle ordinal.
        let prior = ClaimRecord {
            state: ClaimState::Released,
            budget_open: true,
            attempts: 1,
            cycle_attempts: 1,
            ..submitted("s1", 1, 1)
        };
        let PreSpawn::Claim(rec) = decide_pre_spawn(Some(&prior), "s2".into(), now()) else {
            panic!("expected Claim")
        };
        assert_eq!(rec.cycle_attempts, 2, "continue the cycle");
        assert_eq!(rec.attempts, 2);
        assert_eq!(rec.submission_id, "s2", "fresh submission id");
    }

    #[test]
    fn pre_spawn_released_budget_closed_resets_cycle() {
        // regression viii: after an EXHAUSTED release + backoff, the next
        // cycle's re-claim RESETS cycle_attempts = 1 (full retry budget again).
        let prior = ClaimRecord {
            state: ClaimState::Released,
            budget_open: false,
            attempts: 3,
            cycle_attempts: 3,
            ..submitted("s1", 3, 3)
        };
        let PreSpawn::Claim(rec) = decide_pre_spawn(Some(&prior), "s4".into(), now()) else {
            panic!("expected Claim")
        };
        assert_eq!(rec.cycle_attempts, 1, "reset the cycle");
        assert_eq!(rec.attempts, 4, "attempts stays monotonic");
    }

    // --- in-tick post-attempt (regression vi) --------------------------------

    #[test]
    fn post_attempt_failure_with_budget_retries_in_tick() {
        // cron retry=2, attempt 1 fails -> RETRY transition, not terminal.
        let cur = submitted("s1", 1, 1);
        let d = decide_post_attempt(
            &cur,
            TerminalOutcome::Failure,
            DemandKind::Cron,
            2,
            "s2".into(),
            now(),
        );
        let PostAttempt::Retry(rec) = d else {
            panic!("expected Retry")
        };
        assert_eq!(rec.submission_id, "s2");
        assert_eq!(rec.cycle_attempts, 2);
        assert_eq!(rec.state, ClaimState::Submitted);
    }

    #[test]
    fn post_attempt_failure_exhausted_is_terminal_completed_for_cron() {
        let cur = submitted("s3", 3, 3);
        let d = decide_post_attempt(
            &cur,
            TerminalOutcome::Failure,
            DemandKind::Cron,
            2,
            "s4".into(),
            now(),
        );
        let PostAttempt::Terminal { claim, bookkeeping } = d else {
            panic!("expected Terminal")
        };
        assert_eq!(
            claim.state,
            ClaimState::Completed {
                outcome: TerminalOutcome::Failure
            }
        );
        assert_eq!(bookkeeping.cf_delta, CfDelta::Increment);
    }

    #[test]
    fn post_attempt_retry_max_zero_is_immediately_terminal() {
        // regression viii: retry.max=0 -> first failure is exhaustion, exactly
        // one run ever.
        let cur = submitted("s1", 1, 1);
        let d = decide_post_attempt(
            &cur,
            TerminalOutcome::Failure,
            DemandKind::After,
            0,
            "s2".into(),
            now(),
        );
        assert!(
            matches!(d, PostAttempt::Terminal { .. }),
            "no phantom retry at max=0"
        );
    }

    #[test]
    fn post_attempt_after_failure_releases_not_completes() {
        // Routing an after-failure to `completed` would kill retries one state
        // over — it must be `released{budget_open=false}`.
        let cur = submitted("s1", 1, 1);
        let d = decide_post_attempt(
            &cur,
            TerminalOutcome::Failure,
            DemandKind::After,
            0,
            "s2".into(),
            now(),
        );
        let PostAttempt::Terminal { claim, .. } = d else {
            panic!("terminal")
        };
        assert_eq!(claim.state, ClaimState::Released);
        assert!(!claim.budget_open, "exhausted release closes the cycle");
    }

    #[test]
    fn post_attempt_after_success_completes() {
        // regression v: after SUCCEEDS -> completed (terminal, never re-run).
        let cur = submitted("s1", 1, 1);
        let d = decide_post_attempt(
            &cur,
            TerminalOutcome::Success,
            DemandKind::After,
            0,
            "s2".into(),
            now(),
        );
        let PostAttempt::Terminal { claim, bookkeeping } = d else {
            panic!("terminal")
        };
        assert_eq!(
            claim.state,
            ClaimState::Completed {
                outcome: TerminalOutcome::Success
            }
        );
        assert_eq!(bookkeeping.cf_delta, CfDelta::Reset);
    }

    #[test]
    fn post_attempt_partial_is_terminal_never_retried_cf_unchanged() {
        let cur = submitted("s1", 1, 1);
        // Even with budget, partial is never retried.
        let d = decide_post_attempt(
            &cur,
            TerminalOutcome::Partial,
            DemandKind::After,
            5,
            "s2".into(),
            now(),
        );
        let PostAttempt::Terminal { claim, bookkeeping } = d else {
            panic!("terminal")
        };
        assert_eq!(
            claim.state,
            ClaimState::Released,
            "standing demand stays claimable"
        );
        assert_eq!(
            bookkeeping.cf_delta,
            CfDelta::Unchanged,
            "partial neither increments nor resets cf"
        );
    }

    #[test]
    fn post_attempt_cron_partial_completes() {
        // Partial clears the emitter's own cron demand for that occurrence.
        let cur = submitted("s1", 1, 1);
        let d = decide_post_attempt(
            &cur,
            TerminalOutcome::Partial,
            DemandKind::Cron,
            0,
            "s2".into(),
            now(),
        );
        let PostAttempt::Terminal { claim, .. } = d else {
            panic!("terminal")
        };
        assert_eq!(
            claim.state,
            ClaimState::Completed {
                outcome: TerminalOutcome::Partial
            }
        );
    }

    // --- resolver (regression iii, vii) --------------------------------------

    #[test]
    fn resolver_failure_with_budget_releases_budget_open_no_cf() {
        // regression vii: crash between attempt-1 failure and retry CAS.
        // Resolver sees failure + budget remaining -> released{budget_open=true},
        // NO consecutive_failures. The next tick re-claims and fires attempt 2.
        let cur = submitted("s1", 1, 1);
        let r = decide_resolver(&cur, TerminalOutcome::Failure, DemandKind::Cron, 2, now());
        let Resolved::ReleasedBudgetOpen(rec) = r else {
            panic!("expected ReleasedBudgetOpen")
        };
        assert_eq!(rec.state, ClaimState::Released);
        assert!(rec.budget_open);
        assert_eq!(rec.cycle_attempts, 1, "cycle preserved for the +1 re-claim");
    }

    #[test]
    fn resolver_failure_exhausted_is_terminal_with_cf() {
        let cur = submitted("s3", 3, 3);
        let r = decide_resolver(&cur, TerminalOutcome::Failure, DemandKind::After, 2, now());
        let Resolved::Terminal { claim, bookkeeping } = r else {
            panic!("terminal")
        };
        assert_eq!(claim.state, ClaimState::Released);
        assert!(!claim.budget_open);
        assert_eq!(bookkeeping.cf_delta, CfDelta::Increment);
    }

    #[test]
    fn resolver_success_is_terminal_completed() {
        // regression iii: record exists (success) -> outcome applied, completed.
        let cur = submitted("s1", 1, 1);
        let r = decide_resolver(&cur, TerminalOutcome::Success, DemandKind::After, 2, now());
        let Resolved::Terminal { claim, bookkeeping } = r else {
            panic!("terminal")
        };
        assert_eq!(
            claim.state,
            ClaimState::Completed {
                outcome: TerminalOutcome::Success
            }
        );
        assert_eq!(bookkeeping.cf_delta, CfDelta::Reset);
    }

    #[test]
    fn resolver_partial_never_continues_budget() {
        let cur = submitted("s1", 1, 1);
        let r = decide_resolver(&cur, TerminalOutcome::Partial, DemandKind::After, 5, now());
        assert!(
            matches!(r, Resolved::Terminal { .. }),
            "partial is never budget-continued"
        );
    }

    #[test]
    fn full_retry_cycle_never_exceeds_configured_submissions() {
        // regression viii: retry.max=2 -> at most 3 total submissions across the
        // cycle. Walk the in-tick path: fail, fail, fail(exhausted).
        let mut cur = ClaimRecord::new_submitted("s1".into(), now());
        assert_eq!(cur.cycle_attempts, 1);
        // attempt 1 fails
        let PostAttempt::Retry(c2) = decide_post_attempt(
            &cur,
            TerminalOutcome::Failure,
            DemandKind::Cron,
            2,
            "s2".into(),
            now(),
        ) else {
            panic!()
        };
        cur = c2;
        assert_eq!(cur.cycle_attempts, 2);
        // attempt 2 fails
        let PostAttempt::Retry(c3) = decide_post_attempt(
            &cur,
            TerminalOutcome::Failure,
            DemandKind::Cron,
            2,
            "s3".into(),
            now(),
        ) else {
            panic!()
        };
        cur = c3;
        assert_eq!(cur.cycle_attempts, 3);
        // attempt 3 fails -> exhausted (3 total submissions)
        let PostAttempt::Terminal { .. } = decide_post_attempt(
            &cur,
            TerminalOutcome::Failure,
            DemandKind::Cron,
            2,
            "s4".into(),
            now(),
        ) else {
            panic!("attempt 3 must be terminal — never a 4th submission")
        };
    }
}
