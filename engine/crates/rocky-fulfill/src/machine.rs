//! Pure decisions for the fulfillment reconciler — the FF-DESIGN D6
//! transition table, verbatim, as side-effect-free functions.
//!
//! This module follows the `rocky_core::schedule::claim` discipline: it
//! holds ONLY pure decisions over `(observed record, event, now)` — no
//! I/O, no transactions, no clock reads. The redb CAS that applies a
//! decision lives on `rocky_core::state::StateStore::fulfill_state_cas`,
//! driven by [`crate::step`]. Keeping the decision logic here, free of
//! any transaction, is what makes the state machine exhaustively
//! unit-testable — the test matrix below is the executable spec, one
//! test per D6 table row.
//!
//! Two decision surfaces:
//!
//! - [`decide_ownership`] — who may drive the record: claim a released
//!   record, stand down from a live owner, take over a dead one
//!   (PID + start-time liveness, PID-reuse-proof), and the
//!   grace-stamped takeover for owners that cannot be probed
//!   ([`FULFILL_RECOVERY_GRACE`], the `first_swept_at` pattern). This
//!   replaces `--break-lock`.
//! - [`decide`] — the D6 table: given the state the record is in and
//!   what the runner just observed, the next fenced transition and/or
//!   the next task.

use chrono::{DateTime, Duration, SecondsFormat, Utc};
use rocky_core::fulfill::{DraftingRound, FulfillState, FulfillStateRecord};

/// Driver dispatches allowed per task cycle (elicitation or drafting)
/// before the product blocks. FF-DESIGN D6 rules, frozen.
pub const MAX_COMPILE_ITERS: u32 = 8;

/// Verify-red → repair-drafting cycles allowed before the product
/// blocks. FF-DESIGN D6 rules, frozen.
pub const MAX_REPAIR_ROUNDS: u32 = 3;

/// Grace between first observing a non-probeable owner stamp and taking
/// the record over (the `schedule::reconcile` `RECOVERY_GRACE` pattern).
/// A DEFINITIVELY dead owner (no such pid, or the pid was reused by a
/// process with a different start time) is taken over immediately — the
/// grace exists for the indefinite arm only.
pub const FULFILL_RECOVERY_GRACE: Duration = Duration::minutes(5);

/// `needs_input` reason: the spec candidate awaits `approve-spec`.
pub const REASON_SPEC_APPROVAL: &str = "spec_approval";
/// `needs_input` reason: the `[policy]` posture needs a human edit.
pub const REASON_POLICY: &str = "policy";
/// `needs_input` reason: the proposed plan awaits `rocky review`.
pub const REASON_PLAN_APPROVAL: &str = "plan_approval";

// ---------------------------------------------------------------------------
// Ownership
// ---------------------------------------------------------------------------

/// What the liveness probe said about a recorded owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnerLiveness {
    /// The pid exists and its start time matches the record: the owner
    /// process is alive.
    Alive,
    /// Definitive: the pid does not exist, or it exists with a different
    /// start time (the pid was reused by an unrelated process).
    Dead,
    /// The probe could not answer (platform without a probe, transient
    /// read failure). Never treated as dead — the grace path applies.
    Indefinite(String),
}

/// The identity of the process asking to drive the loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelfIdentity {
    /// This process's pid.
    pub pid: u32,
    /// This process's start time, in the platform probe's own unit.
    pub start_time: u64,
}

/// The ownership decision for one loop entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnershipDecision {
    /// CAS this record in (expected prior = what was observed, `None`
    /// when absent): the record is unowned (or brand new) and this
    /// process claims it.
    Claim(FulfillStateRecord),
    /// The record is already stamped with this exact process. Proceed.
    AlreadyOwned,
    /// A LIVE owner holds the record: print who, exit clean, write
    /// nothing.
    StandDown {
        /// The live owner.
        owner_pid: u32,
    },
    /// The owner cannot be probed and no grace is running: CAS this
    /// record (which stamps `first_swept_at`) and stop — takeover
    /// becomes possible once [`FULFILL_RECOVERY_GRACE`] elapses.
    StampGrace(FulfillStateRecord),
    /// The owner cannot be probed and the grace is still running: stop,
    /// write nothing, say how long remains.
    WaitGrace {
        /// Whole seconds until the grace elapses.
        remaining_seconds: i64,
    },
    /// The owner is definitively dead (or the grace elapsed on a
    /// non-probeable one): CAS this record, which stamps this process as
    /// the new owner. The dead owner's next CAS — if it was merely
    /// paused, not dead — fails on the changed prior, so two drivers can
    /// never both write.
    TakeOver(FulfillStateRecord),
}

/// Decide who may drive the record. Pure — the CAS that applies a
/// `Claim`/`TakeOver`/`StampGrace` is on the state store, and a lost CAS
/// means another process moved first: stand down.
///
/// `liveness` is the probe result for the RECORDED owner and must be
/// `None` exactly when the record carries no owner stamp.
///
/// # Takeover coexistence — the fenced-or-idempotent invariant
///
/// A grace takeover (an ALIVE but unprobeable owner outlives the grace)
/// creates a window where the old loop may still complete an in-flight
/// side effect before its next CAS loses. Every side-effect class it
/// can produce in that window is covered, and none of the coverage
/// depends on the liveness probe being right:
///
/// - **State/journal writes** — every write is CAS-on-observed-prior;
///   the takeover moved the record, so the old owner's next write LOSES
///   and it stands down. Fenced.
/// - **A propose completing late** — the plan file persists, but the
///   old owner cannot journal `proposed` (its CAS loses), the loop
///   never applies a plan it did not pin, and an orphaned plan reaches
///   the warehouse only through the engine's own marker + digest gates.
///   Worst case: one extra never-approved plan in the review queue.
/// - **An apply completing late** — the warehouse write rides the
///   idempotency key pinned on the SHARED record lineage. The new owner
///   resumes `applying` as `applying_unknown` and asks the
///   authoritative receipt: a live claim PARKS it; a completed one
///   resolves to `applied` without re-running. Keyed + receipt-resolved
///   (both arms integration-proven).
/// - **A candidate write completing late** — tmp+rename, last writer
///   wins, and the candidate is untrusted-by-definition until
///   `approve-spec` digests the bytes it is approving and prints that
///   digest; the approval CAS names one winner. Detected at the next
///   authority transition.
/// - **The old worker group** — the taken-over record carries
///   `driver_pgid` + leader start time; the new owner sweeps a
///   still-live group before dispatching its own worker. The approve
///   verb refuses a new-digest approval while any state is mid-step or
///   a worker group is stamped, so no record replacement can drop the
///   stamp (or the pinned key) out from under this argument.
/// - **Transcripts / outbox files** — advisory: no gate ever trusts
///   them, and elicitation bytes are digest-checked on hand-off.
///
/// Anything outside these classes is a NEW cross-domain boundary and
/// must state its own protocol before it lands (the E12 rule).
pub fn decide_ownership(
    observed: Option<&FulfillStateRecord>,
    liveness: Option<&OwnerLiveness>,
    me: SelfIdentity,
    product_id: &str,
    now: DateTime<Utc>,
) -> OwnershipDecision {
    let Some(record) = observed else {
        // First run: no record. Claim by inserting `init` with our stamp.
        let mut fresh = FulfillStateRecord::new(
            FulfillState::Init,
            product_id.to_string(),
            None,
            Some(stamp(now)),
        );
        fresh.owner_pid = Some(me.pid);
        fresh.owner_start_time = Some(me.start_time);
        return OwnershipDecision::Claim(fresh);
    };
    let Some(owner_pid) = record.owner_pid else {
        // Released record (every clean stop clears the stamp): claim it.
        let mut claimed = record.clone();
        claimed.owner_pid = Some(me.pid);
        claimed.owner_start_time = Some(me.start_time);
        claimed.first_swept_at = None;
        claimed.updated_at = Some(stamp(now));
        return OwnershipDecision::Claim(claimed);
    };
    if owner_pid == me.pid && record.owner_start_time == Some(me.start_time) {
        return OwnershipDecision::AlreadyOwned;
    }
    match liveness {
        Some(OwnerLiveness::Alive) => OwnershipDecision::StandDown { owner_pid },
        Some(OwnerLiveness::Dead) => {
            let mut taken = record.clone();
            taken.owner_pid = Some(me.pid);
            taken.owner_start_time = Some(me.start_time);
            taken.first_swept_at = None;
            taken.updated_at = Some(stamp(now));
            OwnershipDecision::TakeOver(taken)
        }
        Some(OwnerLiveness::Indefinite(_)) | None => {
            // `None` here means the record has a stamp the caller could
            // not probe at all — treat exactly like an indefinite probe.
            match parse_stamp(record.first_swept_at.as_deref()) {
                None => {
                    let mut stamped = record.clone();
                    stamped.first_swept_at = Some(stamp(now));
                    OwnershipDecision::StampGrace(stamped)
                }
                Some(swept_at) if now - swept_at >= FULFILL_RECOVERY_GRACE => {
                    let mut taken = record.clone();
                    taken.owner_pid = Some(me.pid);
                    taken.owner_start_time = Some(me.start_time);
                    taken.first_swept_at = None;
                    taken.updated_at = Some(stamp(now));
                    OwnershipDecision::TakeOver(taken)
                }
                Some(swept_at) => OwnershipDecision::WaitGrace {
                    remaining_seconds: (FULFILL_RECOVERY_GRACE - (now - swept_at)).num_seconds(),
                },
            }
        }
    }
}

// ---------------------------------------------------------------------------
// The D6 events
// ---------------------------------------------------------------------------

/// The posture verdict (`rocky product verify`), summarized.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PostureStatus {
    /// The frozen `propose_only` posture holds.
    Pass,
    /// A human must paste the corrected `[policy]` block.
    NeedsInput {
        /// The paste-ready block.
        paste_block: String,
        /// Why, in plain language.
        reason: String,
    },
    /// A non-posture failure (identity collision, unresolvable tag).
    Fail {
        /// Why, in plain language.
        reason: String,
    },
}

/// The governed propose outcome, summarized for the decision table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProposeSummary {
    /// The plan persisted, policy allowed.
    Written {
        /// The persisted plan id.
        plan_id: String,
    },
    /// The plan persisted, review pending.
    ReviewRequired {
        /// The persisted plan id.
        plan_id: String,
        /// The gating policy verdict, rendered.
        refusal: String,
    },
    /// Nothing persisted; the policy denied.
    Denied {
        /// The denying policy verdict, rendered (model + rule + reason).
        refusal: String,
    },
}

/// The typed apply outcome, summarized for the decision table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApplySummary {
    /// The plan executed.
    Applied {
        /// The apply's run id, when the path forces one.
        run_id: Option<String>,
    },
    /// The idempotency key already holds a terminal success.
    SkippedIdempotent {
        /// The run that already satisfied the key.
        prior_run_id: String,
    },
    /// The idempotency key holds a live in-flight claim.
    SkippedInFlight {
        /// The run currently holding the claim.
        prior_run_id: String,
    },
    /// The apply failed with an error a retry will not fix unattended.
    Failed {
        /// The rendered error.
        error: String,
    },
}

/// The authoritative receipt lookup, summarized for the decision table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReceiptSummary {
    /// A terminal `Succeeded` receipt exists.
    Succeeded {
        /// The run that succeeded.
        run_id: String,
    },
    /// A terminal `Failed` receipt exists (retry is dedup-safe).
    Failed {
        /// The run that failed.
        run_id: String,
    },
    /// The key is claimed by a live (or apparently live) run.
    InFlight {
        /// The claiming run.
        run_id: String,
    },
    /// The authoritative store holds no record for the key.
    NoRecord,
    /// The backend cannot answer authoritatively — a human resolves.
    CannotAnswer {
        /// Why, in plain language.
        reason: String,
    },
}

/// Why a reading of the declared checks could not be trusted.
///
/// The verdict alone is not enough to tell an operator what to do: the
/// causes share one `unevaluable` verdict but need three different
/// remedies, and one of them needs a different LANDING STATE as well.
/// Carrying the cause is what keeps the stop from printing a command
/// that cannot fix the condition it just reported.
///
/// [`decide_observation`] matches every variant explicitly. A new cause
/// therefore has to state its own landing and remedy at the point it is
/// added; it cannot inherit the custody one, which would print "restore
/// the file you changed" at someone whose problem no restore can fix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnevaluableCause {
    /// The checks on disk are not the ones this generation VERIFIED.
    ///
    /// Re-running the loop re-reads the same diverged files and reports
    /// the same thing forever, so the stop must not offer it bare. The
    /// remedy is a RESTORE: put back what changed, and the digests match
    /// again. No engine verb adopts the change instead — `rocky product
    /// compile` refuses sidecar drift outright, and on a
    /// `test_definitions.toml` edit it re-lowers the sidecar without
    /// touching the definitions, so the expansion still diverges. Nor can
    /// the loop adopt it: the only route into `verifying` is from
    /// `merged` (the `Merged` arm of [`decide`] is the sole transition
    /// into it), and no post-apply event re-enters `merged` in the SAME
    /// generation, so nothing after an apply can pin a new digest.
    ///
    /// To keep the change it belongs in the product spec — but not from
    /// here, and not every change fits. A first custody divergence lands
    /// the record back at `applied`, and `rocky product approve` refuses
    /// `applied`, pinned against the real verb by
    /// `approving_refuses_at_applied_and_permits_at_observing` in
    /// rocky-cli. So the order is: restore, re-run until the loop leaves
    /// `applied`, and only then change and approve the spec — which
    /// starts a new generation that pins at its own verify.
    ///
    /// WHAT FITS is narrower than "the spec", and the message says so.
    /// `output.checks` is `Vec<String>` — opaque SQL booleans
    /// (`product::spec::OutputSpec`) — and the lowering turns every one
    /// of them into an `expression` test at `severity = "error"`
    /// (`product::lowering::generated_tests`). A `not_null` comes from
    /// an `output.columns` entry with `nullable = false`. And
    /// `output.grain` lowers to exactly ONE uniqueness check: `unique`
    /// on a single grain column, or `composite` + `kind = "unique"`
    /// over several.
    ///
    /// That last one is the correction. The message used to list
    /// `unique` flat among the shapes with no spec spelling, which is
    /// false — the declared grain IS a uniqueness check, and the
    /// lowering emits it in both arms. What has no spelling is
    /// uniqueness that is not the grain: a `unique` on some other
    /// column, a second one, or a `composite` whose `kind` is not
    /// `unique`. Alongside it: another typed shape
    /// (`row_count_range`, `accepted_values`, `in_range`,
    /// `regex_match`, `relationships`), a `warning` severity, a
    /// `filter`, or a `[[use_test]]` reference. For those the restore
    /// is the whole remedy. Pinned from the lowering side by
    /// `spec_checks_lower_only_to_error_severity_expression_tests` and
    /// the composite-grain arm in rocky-core, so teaching `checks` a
    /// new shape fails a test that names this message.
    CheckCustody,
    /// The pinned digest and the recomputed one were taken under
    /// DIFFERENT preimage schemes, so they were never comparable.
    ///
    /// Not a custody divergence, and it must never be reported as one.
    /// Nothing on disk is in doubt: the generation was pinned by a build
    /// whose `CheckSetPreimage` covered different fields, and this build
    /// cannot reproduce that value. The custody remedy is a RESTORE, and
    /// no restore changes a hash algorithm — printing it here is the
    /// exact "instruction that cannot resolve what it was printed for"
    /// the custody arm exists to avoid. It would also land at `applied`,
    /// where `rocky product approve` is refused, so the operator would
    /// have neither of the two routes out.
    ///
    /// This one lands `blocked` instead, which is a state a human can
    /// act on: `rocky fulfill <product> --retry` re-enters at
    /// `spec_approved` (the `Blocked` arm of [`decide`]) and the fresh
    /// generation pins its own digest at its own `verifying`, under the
    /// current scheme. That is the printed remedy, and
    /// `a_digest_from_an_older_scheme_blocks_with_a_remedy_that_works`
    /// drives it end to end.
    ///
    /// (Reading `product.rs`, `rocky product approve` should also be
    /// accepted here — `blocked` is in its stop set, not its in-flight
    /// set. Read from the code, NOT exercised by a test, and not the
    /// command the stop prints. Stated as an observation rather than a
    /// guarantee.)
    ///
    /// Reachable today only from a record written by an intermediate
    /// build of this work package: `checks_digest` does not exist on
    /// `main`, so no released binary has ever written an untagged one.
    /// It is here for the NEXT preimage change, which the rule on
    /// `CheckSetPreimage` positively invites, and which would otherwise
    /// strand every generation a released build had pinned.
    CheckSchemeChanged,
    /// The reading itself failed, or checks errored. Re-running can
    /// genuinely resolve this one: the warehouse may answer next time.
    Unreadable,
}

/// What the runner observed, for the state the record is in.
///
/// Each variant is produced by exactly one gathering step in
/// [`crate::step`]; [`decide`] maps `(state, event)` onto the D6 exits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    /// `init`: what the candidate spec surface shows.
    CandidateSurface {
        /// The working `products/<name>.toml` digest, when it exists and
        /// parses.
        candidate_digest: Option<String>,
    },
    /// An elicitation driver task finished and (on success) the RUNNER
    /// completed the confined candidate write.
    ElicitationFinished {
        /// The digest of the candidate the runner wrote, on success.
        written_digest: Option<String>,
        /// The worker's questions for the human, surfaced in the stop.
        questions: Vec<String>,
        /// The failure, when the task failed.
        error: Option<String>,
    },
    /// `needs_input(spec_approval)` re-entry: candidate vs approval.
    ApprovalSurface {
        /// The current working-candidate digest, when it parses.
        candidate_digest: Option<String>,
        /// The currently approved digest, when an approval exists.
        approved_digest: Option<String>,
    },
    /// `spec_approved` entry: the snapshot re-verification.
    SnapshotVerify {
        /// Whether snapshot bytes re-digest to the approval record.
        snapshot_ok: bool,
        /// Detail for the tamper stop.
        detail: String,
    },
    /// The posture verification verdict.
    PostureVerified(PostureStatus),
    /// Phase-A lowering result.
    PhaseAResult {
        /// Whether Phase A committed.
        ok: bool,
        /// The reject/error detail on failure.
        detail: String,
    },
    /// A drafting (or repair) driver task finished.
    DraftingFinished {
        /// The failure, when the task failed. `None` = worker exited 0
        /// and the group was killed with no survivors.
        error: Option<String>,
    },
    /// The Phase-A byte-verification against the committed manifest.
    ArtifactCheck {
        /// Non-empty = tampered artifacts, rendered.
        problems: Vec<String>,
    },
    /// Phase-B merge result.
    PhaseBResult {
        /// Whether Phase B committed.
        ok: bool,
        /// The reject/error detail on failure.
        detail: String,
    },
    /// The runner's own verification bundle at `verifying`.
    VerifyBundle {
        /// `compile_output` had no errors.
        compile_green: bool,
        /// The model executed and its unit tests passed. This does NOT
        /// cover the product's declared data checks — see
        /// `tests_deferred`.
        test_green: bool,
        /// `product verify` still passes (policy-check agreement).
        posture_green: bool,
        /// The committed manifest is total and byte-clean.
        manifest_total: bool,
        /// How many of the product's declared data checks were NOT
        /// evaluated. They run against a materialised table, and verify
        /// runs before apply, so at this point none of them can run.
        ///
        /// `None` when this bundle carries no count of its own —
        /// either the sidecar could not be read, or the bundle is the
        /// synthesized propose-failure one, whose pass already
        /// journaled the authoritative count. Distinct from `Some(0)`,
        /// which positively claims there are none.
        ///
        /// Reported, never gated: deferred is not a failure, so this
        /// field is deliberately absent from the green pattern below.
        tests_deferred: Option<usize>,
        /// Digest over the EXPANDED check set this bundle verified, when
        /// it could be computed. Recorded on the green transition and
        /// re-checked at observation, so an edit to a shared
        /// `test_definitions.toml` cannot change what runs without the
        /// loop noticing. `None` when this build or this bundle could not
        /// ask the loader — which makes observation hold, never pass.
        checks_digest: Option<String>,
        /// Rendered detail. Carries the deferred-checks note on the
        /// paths that counted, plus the red legs' reasons when there
        /// are any.
        detail: String,
    },
    /// The governed propose ran.
    Proposed {
        /// The outcome, summarized.
        outcome: ProposeSummary,
        /// The persisted plan payload's spec digest (read back from the
        /// plan store, never trusted from memory), when one persisted.
        plan_payload_digest: Option<String>,
        /// The CURRENTLY approved digest, for the supersession check.
        approved_digest: Option<String>,
        /// The idempotency key the plan was built with.
        idempotency_key: String,
    },
    /// The review-marker poll at `proposed` / `needs_input(plan_approval)`.
    MarkerPoll {
        /// The marker approves this plan.
        reviewed: bool,
        /// The marker is malformed / names another plan (rendered), and
        /// polling must surface an error rather than wait forever.
        invalid: Option<String>,
        /// The plan payload's spec digest.
        plan_payload_digest: Option<String>,
        /// The currently approved digest.
        approved_digest: Option<String>,
    },
    /// The pre-apply digest recompute from the SNAPSHOT bytes.
    PreApply {
        /// The digest recomputed from the snapshot file, when readable
        /// and intact.
        recomputed_digest: Option<String>,
        /// The plan payload's spec digest.
        plan_payload_digest: Option<String>,
        /// Whether the snapshot bytes still match the approval record.
        snapshot_ok: bool,
    },
    /// The typed apply finished.
    ApplyFinished(ApplySummary),
    /// The authoritative receipt lookup answered.
    ReceiptResolved(ReceiptSummary),
    /// The post-apply observation ran.
    ObservationDone {
        /// Scoped `rocky test` was green.
        test_green: bool,
        /// The staleness verdict: `None` = no freshness spec / no rows.
        staleness_ok: Option<bool>,
        /// Rendered findings, journaled.
        detail: String,
    },
    /// The product's declared data checks were read against the APPLIED
    /// output — the checks the verify bundle could only report deferred,
    /// finally evaluable because the table now exists.
    ObservationChecks {
        /// Checks that failed at `severity = "error"`. The only signal
        /// that routes to a repair round: positive evidence that the
        /// live output contradicts something the product declared about
        /// itself.
        failed: usize,
        /// Checks whose execution errored. NOT a failure — the runner
        /// could not tell whether the data is right, and guessing in
        /// either direction is worse than holding.
        errored: usize,
        /// Checks that failed at `severity = "warning"`. Reported, never
        /// routed: a warning is by definition not a defect the product
        /// declared as disqualifying.
        warned: usize,
        /// Declared checks that produced no verdict at all. `None` when
        /// the read failed outright, so even the count is unknown —
        /// distinct from `Some(0)`, which positively claims every
        /// declared check was evaluated (the #1495 rule, applied to the
        /// observation side).
        deferred: Option<usize>,
        /// The rendered evidence: which checks, and what they measured.
        /// This is what reaches the repair worker, so it stays pure
        /// evidence — the staleness/test reading rides beside it.
        detail: String,
        /// The staleness/test reading from earlier in the same pass,
        /// already journaled, carried only so the stop message reports
        /// the whole observation.
        prior_detail: String,
        /// Why the reading is incomplete, when it is. `None` on a
        /// reading that evaluated everything it declared.
        cause: Option<UnevaluableCause>,
    },
    /// `blocked` re-entry with `--retry`.
    RetryRequested,
    /// A plain re-entry with nothing new observed (resume dispatch).
    Reentry,
}

// ---------------------------------------------------------------------------
// The decisions
// ---------------------------------------------------------------------------

/// A task the runner performs next; its outcome is the next [`Event`].
///
/// Not `Copy`: [`Self::ObserveChecks`] carries the reading that came
/// before it, because the two halves of an observation must end in ONE
/// message to the human. Splitting them into two tasks is what gives the
/// crash seam between them a real resting point; letting the second half
/// forget the first is what would quietly drop the staleness finding out
/// of the stop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskKind {
    /// Dispatch the elicitation driver task, then do the confined
    /// candidate write.
    Elicit,
    /// Run `product verify` (posture, tags, collisions).
    VerifyPosture,
    /// Run Phase-A lowering through the staged commit.
    RunPhaseA,
    /// Byte-verify every Phase-A artifact against the committed
    /// manifest hashes.
    ByteVerifyPhaseA,
    /// Dispatch the drafting driver task (kill group + assert no
    /// survivors before returning).
    Draft,
    /// Dispatch the repair driver task (same supervision).
    Repair,
    /// Dispatch the data-repair driver task: same supervision, same
    /// reopened drafting window, different brief — the worker is handed
    /// the failing check and what it measured, not a compiler error.
    DataRepair,
    /// Run Phase-B metadata merge through the staged commit.
    RunPhaseB,
    /// Run the runner's own verification bundle (compile, test,
    /// posture, manifest).
    VerifyBundle,
    /// The controlled propose through the governed helper.
    Propose,
    /// Poll the review marker (typed status).
    PollMarker,
    /// Recompute the digest from the snapshot bytes (pre-apply gate).
    PreApplyCheck,
    /// The typed in-process apply with `expect_spec_digest`.
    Apply,
    /// The authoritative receipt lookup for the pinned key.
    LookupReceipt,
    /// The post-apply observation (scoped test + staleness read).
    Observe,
    /// Read the product's declared data checks against the applied
    /// output. Separate from [`Self::Observe`] so each event has exactly
    /// one producer, and so the crash seam between the two is real.
    ObserveChecks {
        /// The staleness/test reading already journaled this pass,
        /// carried so the final stop reports the whole observation
        /// rather than only its second half.
        prior_detail: String,
    },
}

/// A stop: the loop can go no further without a human (or an external
/// event). Every stop prints the state, why, and the exact next command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stop {
    /// Why the loop stopped, in plain language.
    pub message: String,
    /// The exact command that unblocks it, when one exists.
    pub next_command: Option<String>,
}

/// What the runner must do with a decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    /// CAS to `record` (journaling `event`), then keep stepping.
    Advance {
        /// The complete new record (the CAS target).
        record: FulfillStateRecord,
        /// The journal row's event text.
        event: String,
    },
    /// CAS to `record`, then perform `task` (whose outcome is the next
    /// event). Used where the attempt must be counted BEFORE dispatch.
    AdvanceAndAct {
        /// The complete new record (the CAS target).
        record: FulfillStateRecord,
        /// The journal row's event text.
        event: String,
        /// The task to perform after the CAS commits.
        task: TaskKind,
    },
    /// CAS to `record`, then stop with the ask.
    AdvanceAndStop {
        /// The complete new record (the CAS target).
        record: FulfillStateRecord,
        /// The journal row's event text.
        event: String,
        /// The printed ask.
        stop: Stop,
    },
    /// No write; perform the task.
    Act(TaskKind),
    /// No write; stop with the ask.
    Halt(Stop),
}

/// RFC3339 seconds-precision stamp (the approve verb's format).
fn stamp(now: DateTime<Utc>) -> String {
    now.to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn parse_stamp(raw: Option<&str>) -> Option<DateTime<Utc>> {
    raw.and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&Utc))
}

/// Clone `observed` into a new record in `state`, stamped `now`.
/// Pins and counters carry over unless the caller mutates them.
fn to_state(
    observed: &FulfillStateRecord,
    state: FulfillState,
    now: DateTime<Utc>,
) -> FulfillStateRecord {
    let mut next = observed.clone();
    next.state = state;
    next.updated_at = Some(stamp(now));
    next
}

/// The blocked record for `reason`, pins cleared.
fn blocked(
    observed: &FulfillStateRecord,
    reason: String,
    now: DateTime<Utc>,
) -> FulfillStateRecord {
    let mut next = to_state(observed, FulfillState::Blocked { reason }, now);
    next.plan_id = None;
    next.idempotency_key = None;
    next
}

fn blocked_stop(
    record: FulfillStateRecord,
    event: String,
    product: &str,
    reason: &str,
) -> Decision {
    // The stop surfaces the RECORD's blocked reason (which carries any
    // `tampered:` classification), so the printed message and the
    // persisted state never tell two different stories.
    let message = match &record.state {
        FulfillState::Blocked { reason } => format!("blocked: {reason}"),
        _ => format!("blocked: {reason}"),
    };
    Decision::AdvanceAndStop {
        record,
        event,
        stop: Stop {
            message,
            next_command: Some(format!("rocky fulfill {product} --retry")),
        },
    }
}

/// The product's bare name (the `product:` key form, stripped).
fn product_name(observed: &FulfillStateRecord) -> &str {
    observed
        .product_id
        .strip_prefix("product:")
        .unwrap_or(&observed.product_id)
}

/// Decide the next move — the FF-DESIGN D6 transition table.
///
/// `observed` is the record THIS process owns (ownership settled by
/// [`decide_ownership`] first); `event` is what the runner just
/// observed or completed for the state the record is in. An event that
/// cannot occur in the record's state is an internal error and halts
/// with a diagnostic — it never silently proceeds.
pub fn decide(observed: &FulfillStateRecord, event: Event, now: DateTime<Utc>) -> Decision {
    let product = product_name(observed).to_string();
    match &observed.state {
        // ------------------------------------------------------------------
        // init | first run | lock acquired; no state file
        //   → elicitation task → needs_input(spec_approval) (candidate written)
        // ------------------------------------------------------------------
        FulfillState::Init => match event {
            Event::CandidateSurface {
                candidate_digest: Some(digest),
            } => {
                // A candidate already exists (human-authored, or a prior
                // elicitation's write landed before the crash): adopt it.
                let mut next = to_state(
                    observed,
                    FulfillState::NeedsInput {
                        reason: REASON_SPEC_APPROVAL.to_string(),
                        payload: digest.clone(),
                    },
                    now,
                );
                next.drafting_attempts = 0;
                Decision::AdvanceAndStop {
                    record: next,
                    event: format!("candidate adopted ({digest})"),
                    stop: Stop {
                        message: format!(
                            "candidate spec products/{product}.toml ({digest}) awaits approval"
                        ),
                        next_command: Some(format!("rocky fulfill approve-spec {product}")),
                    },
                }
            }
            Event::CandidateSurface {
                candidate_digest: None,
            } => dispatch_elicitation(observed, &product, "elicitation attempts exhausted", now),
            Event::ElicitationFinished {
                written_digest: Some(digest),
                questions,
                ..
            } => {
                // The normative table's init row goes straight to
                // needs_input(spec_approval) once the candidate is
                // written; the stop carries the worker's questions.
                let mut next = to_state(
                    observed,
                    FulfillState::NeedsInput {
                        reason: REASON_SPEC_APPROVAL.to_string(),
                        payload: digest.clone(),
                    },
                    now,
                );
                next.drafting_attempts = 0;
                let mut message = format!(
                    "candidate spec products/{product}.toml written ({digest}); awaiting approval"
                );
                if !questions.is_empty() {
                    message.push_str("\nworker questions:\n");
                    for q in &questions {
                        message.push_str(&format!("  - {q}\n"));
                    }
                }
                Decision::AdvanceAndStop {
                    record: next,
                    event: format!("candidate written ({digest})"),
                    stop: Stop {
                        message,
                        next_command: Some(format!("rocky fulfill approve-spec {product}")),
                    },
                }
            }
            Event::ElicitationFinished {
                written_digest: None,
                error,
                ..
            } => {
                // A failed attempt re-dispatches through the SAME
                // persisted-budget gate as the first one: the retry's
                // increment is CAS'd onto the record, never a local
                // counter, so a crash-loop (or a lying driver) can burn
                // at most `MAX_COMPILE_ITERS` dispatches total.
                let error =
                    error.unwrap_or_else(|| "elicitation produced no candidate".to_string());
                dispatch_elicitation(observed, &product, &error, now)
            }
            other => internal_mismatch(observed, &other),
        },

        // ------------------------------------------------------------------
        // elicited: candidate written → needs_input(spec_approval)
        // ------------------------------------------------------------------
        FulfillState::Elicited => match event {
            Event::CandidateSurface {
                candidate_digest: Some(digest),
            } => {
                let next = to_state(
                    observed,
                    FulfillState::NeedsInput {
                        reason: REASON_SPEC_APPROVAL.to_string(),
                        payload: digest.clone(),
                    },
                    now,
                );
                Decision::AdvanceAndStop {
                    record: next,
                    event: "awaiting spec approval".to_string(),
                    stop: Stop {
                        message: format!(
                            "candidate spec products/{product}.toml ({digest}) awaits approval"
                        ),
                        next_command: Some(format!("rocky fulfill approve-spec {product}")),
                    },
                }
            }
            Event::CandidateSurface {
                candidate_digest: None,
            } => {
                // The candidate vanished between the write and this step:
                // start over.
                let mut next = to_state(observed, FulfillState::Init, now);
                next.drafting_attempts = 0;
                Decision::Advance {
                    record: next,
                    event: "candidate missing; restarting".to_string(),
                }
            }
            other => internal_mismatch(observed, &other),
        },

        // ------------------------------------------------------------------
        // needs_input(reason, payload)
        // ------------------------------------------------------------------
        FulfillState::NeedsInput { reason, payload } => match (reason.as_str(), event) {
            // approve-spec CAS ok → spec_approved (the approve verb moves
            // the state itself; observing it here covers records that
            // carry a matching approval but were left in needs_input).
            (
                REASON_SPEC_APPROVAL,
                Event::ApprovalSurface {
                    candidate_digest,
                    approved_digest,
                },
            ) => match (candidate_digest, approved_digest) {
                (Some(cand), Some(approved)) if cand == approved => {
                    let mut next = to_state(observed, FulfillState::SpecApproved, now);
                    next.spec_digest = Some(approved.clone());
                    Decision::Advance {
                        record: next,
                        event: format!("approval observed ({approved})"),
                    }
                }
                (Some(cand), _) if cand != *payload => {
                    // Candidate edited while waiting: stay, show the NEW
                    // digest (D6: "candidate edited → stays (new digest
                    // shown)").
                    let next = to_state(
                        observed,
                        FulfillState::NeedsInput {
                            reason: REASON_SPEC_APPROVAL.to_string(),
                            payload: cand.clone(),
                        },
                        now,
                    );
                    Decision::AdvanceAndStop {
                        record: next,
                        event: format!("candidate revised ({cand})"),
                        stop: Stop {
                            message: format!(
                                "candidate spec revised ({cand}); approval still pending"
                            ),
                            next_command: Some(format!("rocky fulfill approve-spec {product}")),
                        },
                    }
                }
                (Some(cand), _) => Decision::Halt(Stop {
                    message: format!("candidate spec ({cand}) awaits approval"),
                    next_command: Some(format!("rocky fulfill approve-spec {product}")),
                }),
                (None, _) => {
                    // Candidate deleted: restart from init (re-elicit).
                    let mut next = to_state(observed, FulfillState::Init, now);
                    next.drafting_attempts = 0;
                    Decision::Advance {
                        record: next,
                        event: "candidate removed; restarting".to_string(),
                    }
                }
            },
            // policy: rerun re-verifies the posture.
            (REASON_POLICY, Event::PostureVerified(PostureStatus::Pass)) => {
                let next = to_state(observed, FulfillState::SpecApproved, now);
                Decision::Advance {
                    record: next,
                    event: "posture verified".to_string(),
                }
            }
            (
                REASON_POLICY,
                Event::PostureVerified(PostureStatus::NeedsInput {
                    paste_block,
                    reason,
                }),
            ) => Decision::Halt(Stop {
                message: format!(
                    "policy posture still needs a human edit: {reason}\n\
                         paste this into rocky.toml, then re-run:\n\n{paste_block}"
                ),
                next_command: Some(format!("rocky fulfill {product}")),
            }),
            (REASON_POLICY, Event::PostureVerified(PostureStatus::Fail { reason })) => {
                let record = blocked(observed, reason.clone(), now);
                blocked_stop(
                    record,
                    "posture verification failed".to_string(),
                    &product,
                    &reason,
                )
            }
            // plan_approval: rerun re-checks the marker; a re-approved
            // snapshot meanwhile supersedes.
            (
                REASON_PLAN_APPROVAL,
                Event::MarkerPoll {
                    reviewed,
                    invalid,
                    plan_payload_digest,
                    approved_digest,
                },
            ) => decide_marker(
                observed,
                &product,
                reviewed,
                invalid,
                plan_payload_digest,
                approved_digest,
                now,
            ),
            (_, other) => internal_mismatch(observed, &other),
        },

        // ------------------------------------------------------------------
        // spec_approved | snapshot exists, journal sha matches |
        //   verify pass → Phase-A lower → lowered_contract;
        //   verify fail → needs_input(policy)
        // ------------------------------------------------------------------
        FulfillState::SpecApproved => match event {
            Event::SnapshotVerify {
                snapshot_ok: false,
                detail,
            } => {
                let record = blocked(observed, format!("tampered: {detail}"), now);
                blocked_stop(
                    record,
                    "snapshot tamper detected".to_string(),
                    &product,
                    &detail,
                )
            }
            Event::SnapshotVerify {
                snapshot_ok: true, ..
            } => Decision::Act(TaskKind::VerifyPosture),
            Event::PostureVerified(PostureStatus::Pass) => Decision::Act(TaskKind::RunPhaseA),
            Event::PostureVerified(PostureStatus::NeedsInput {
                paste_block,
                reason,
            }) => {
                let next = to_state(
                    observed,
                    FulfillState::NeedsInput {
                        reason: REASON_POLICY.to_string(),
                        payload: paste_block.clone(),
                    },
                    now,
                );
                Decision::AdvanceAndStop {
                    record: next,
                    event: "posture needs input".to_string(),
                    stop: Stop {
                        message: format!(
                            "policy posture needs a human edit: {reason}\n\
                             paste this into rocky.toml, then re-run:\n\n{paste_block}"
                        ),
                        next_command: Some(format!("rocky fulfill {product}")),
                    },
                }
            }
            Event::PostureVerified(PostureStatus::Fail { reason }) => {
                let record = blocked(observed, reason.clone(), now);
                blocked_stop(record, "verification failed".to_string(), &product, &reason)
            }
            Event::PhaseAResult { ok: true, .. } => {
                let mut next = to_state(observed, FulfillState::LoweredContract, now);
                next.drafting_attempts = 0;
                Decision::Advance {
                    record: next,
                    event: "phase A committed".to_string(),
                }
            }
            Event::PhaseAResult { ok: false, detail } => {
                let record = blocked(observed, format!("phase A rejected: {detail}"), now);
                blocked_stop(record, "phase A rejected".to_string(), &product, &detail)
            }
            other => internal_mismatch(observed, &other),
        },

        // ------------------------------------------------------------------
        // lowered_contract | Phase-A artifacts hashed into manifest |
        //   driver drafting task → drafting
        // ------------------------------------------------------------------
        FulfillState::LoweredContract => match event {
            // The first pass over a freshly lowered contract is always a
            // draft — there is no verification to repair yet.
            Event::Reentry => dispatch_drafting(observed, &product, DraftingRound::Draft, now),
            other => internal_mismatch(observed, &other),
        },

        // ------------------------------------------------------------------
        // drafting | worker-profile driver configured |
        //   driver exit 0 → merged path; failure/timeout ≤ retries → retry;
        //   else blocked
        // ------------------------------------------------------------------
        FulfillState::Drafting => match event {
            Event::DraftingFinished { error: None } => Decision::Act(TaskKind::ByteVerifyPhaseA),
            Event::DraftingFinished { error: Some(error) } => {
                if observed.drafting_attempts >= MAX_COMPILE_ITERS {
                    let record = blocked(
                        observed,
                        format!("drafting failed: {error} (attempts exhausted)"),
                        now,
                    );
                    blocked_stop(
                        record,
                        "drafting budget exhausted".to_string(),
                        &product,
                        &error,
                    )
                } else {
                    // A retry stays in the round the machine is already
                    // in: retrying a failed REPAIR driver with the
                    // drafting brief would hand the worker the wrong
                    // task (#1493).
                    dispatch_drafting(observed, &product, observed.drafting_round, now)
                }
            }
            // merged | all Phase-A artifact bytes re-verified against
            // manifest hashes (drift → blocked(tampered)) — checked
            // BEFORE the merged transition.
            Event::ArtifactCheck { problems } if problems.is_empty() => {
                Decision::Act(TaskKind::RunPhaseB)
            }
            Event::ArtifactCheck { problems } => {
                let detail = problems.join("; ");
                let record = blocked(observed, format!("tampered: {detail}"), now);
                blocked_stop(
                    record,
                    "phase A tamper detected".to_string(),
                    &product,
                    &detail,
                )
            }
            Event::PhaseBResult { ok: true, .. } => {
                let next = to_state(observed, FulfillState::Merged, now);
                Decision::Advance {
                    record: next,
                    event: "phase B committed".to_string(),
                }
            }
            Event::PhaseBResult { ok: false, detail } => {
                let record = blocked(observed, format!("phase B rejected: {detail}"), now);
                blocked_stop(record, "phase B rejected".to_string(), &product, &detail)
            }
            // Resume with a drafting task possibly in flight from a dead
            // owner: the dispatcher sweeps the recorded group first, and
            // the interrupted dispatch consumed its attempt already.
            Event::Reentry => {
                if observed.drafting_attempts >= MAX_COMPILE_ITERS {
                    let record = blocked(
                        observed,
                        format!(
                            "drafting interrupted with attempts exhausted \
                             ({MAX_COMPILE_ITERS} of max_compile_iters)"
                        ),
                        now,
                    );
                    blocked_stop(
                        record,
                        "drafting budget exhausted".to_string(),
                        &product,
                        "drafting attempts exhausted",
                    )
                } else {
                    // The crash-resume arm. The round comes off the
                    // RECORD, so a repair that crashed between its own
                    // CAS and its worker dispatch resumes as a repair —
                    // same brief, same budget — instead of silently
                    // downgrading to a draft (#1493).
                    dispatch_drafting(observed, &product, observed.drafting_round, now)
                }
            }
            other => internal_mismatch(observed, &other),
        },

        // ------------------------------------------------------------------
        // merged → verifying
        // ------------------------------------------------------------------
        FulfillState::Merged => match event {
            Event::Reentry => {
                let next = to_state(observed, FulfillState::Verifying, now);
                Decision::Advance {
                    record: next,
                    event: "verifying".to_string(),
                }
            }
            other => internal_mismatch(observed, &other),
        },

        // ------------------------------------------------------------------
        // verifying | own compile+test green; policy check agrees;
        //   manifest total | → controlled propose → proposed;
        //   red ≤ repair rounds → drafting (the repair dispatch reopens
        //   the window: merged generation byte-verified in full, then
        //   the manifest demoted to Phase A through the staged commit —
        //   #1493); else blocked
        // ------------------------------------------------------------------
        FulfillState::Verifying => match event {
            Event::Reentry => Decision::Act(TaskKind::VerifyBundle),
            // `tests_deferred` is deliberately NOT in this pattern:
            // deferred checks are reported, never gated, so any count
            // still proposes. The green verdict is journaled first —
            // without this row the bundle would leave no trace at all,
            // and "verify green" would be a claim with no record of
            // what green did and did not cover.
            // `checks_digest: Some(_)` IS in this pattern, unlike
            // `tests_deferred`. A generation whose check set could not be
            // digested cannot be pinned, and an unpinnable generation is
            // doomed rather than degraded: nothing after apply can
            // re-enter `verifying` to pin one, so observation would hold
            // terminally on a divergence it can never resolve. Failing
            // the bundle here costs a repair round and, if it persists,
            // a `blocked` a human can act on — which is what an
            // unverifiable generation should cost.
            Event::VerifyBundle {
                compile_green: true,
                test_green: true,
                posture_green: true,
                manifest_total: true,
                checks_digest: Some(checks_digest),
                detail,
                ..
            } => {
                // The green verdict is where the executed check set is
                // pinned: this is the last point the runner validated the
                // model, and everything after it (propose, the human
                // review window, apply) must run the SAME checks.
                let mut next = to_state(observed, FulfillState::Verifying, now);
                next.checks_digest = Some(checks_digest);
                Decision::AdvanceAndAct {
                    record: next,
                    event: verify_green_event(&detail),
                    task: TaskKind::Propose,
                }
            }
            Event::VerifyBundle { detail, .. } => {
                if observed.repair_rounds >= MAX_REPAIR_ROUNDS {
                    let record = blocked(
                        observed,
                        format!(
                            "verification red after {MAX_REPAIR_ROUNDS} repair rounds: {detail}"
                        ),
                        now,
                    );
                    blocked_stop(
                        record,
                        "repair budget exhausted".to_string(),
                        &product,
                        &detail,
                    )
                } else {
                    let mut next = to_state(observed, FulfillState::Drafting, now);
                    next.repair_rounds = observed.repair_rounds + 1;
                    next.drafting_attempts = 1;
                    // Persist the round WITH the transition that decides
                    // it: this CAS and the worker dispatch are separate
                    // steps, and a crash between them must resume as a
                    // repair (#1493).
                    next.drafting_round = DraftingRound::Repair;
                    Decision::AdvanceAndAct {
                        record: next,
                        event: format!("repair round {} ({detail})", observed.repair_rounds + 1),
                        task: round_task(DraftingRound::Repair),
                    }
                }
            }
            Event::Proposed {
                outcome,
                plan_payload_digest,
                approved_digest,
                idempotency_key,
            } => decide_proposed(
                observed,
                &product,
                outcome,
                plan_payload_digest,
                approved_digest,
                idempotency_key,
                now,
            ),
            other => internal_mismatch(observed, &other),
        },

        // ------------------------------------------------------------------
        // proposed | plan payload digest == snapshot digest (else
        //   superseded) | marker present+valid → plan_approved;
        //   absent → needs_input(plan_approval)
        // ------------------------------------------------------------------
        FulfillState::Proposed => match event {
            Event::MarkerPoll {
                reviewed,
                invalid,
                plan_payload_digest,
                approved_digest,
            } => decide_marker(
                observed,
                &product,
                reviewed,
                invalid,
                plan_payload_digest,
                approved_digest,
                now,
            ),
            other => internal_mismatch(observed, &other),
        },

        // ------------------------------------------------------------------
        // plan_approved | marker parses, plan_id matches |
        //   journal applying (digest + idempotency key pinned) → applying
        // ------------------------------------------------------------------
        FulfillState::PlanApproved => match event {
            Event::Reentry => {
                let next = to_state(observed, FulfillState::Applying, now);
                // The in-process transition CARRIES the pre-apply task; a
                // cold gather at `applying` yields Reentry instead, which
                // is exactly the crash-resume arm (→ applying_unknown).
                Decision::AdvanceAndAct {
                    record: next,
                    event: format!(
                        "applying (plan {}, key {})",
                        observed.plan_id.as_deref().unwrap_or("<unpinned>"),
                        observed.idempotency_key.as_deref().unwrap_or("<unpinned>")
                    ),
                    task: TaskKind::PreApplyCheck,
                }
            }
            other => internal_mismatch(observed, &other),
        },

        // ------------------------------------------------------------------
        // applying | pre-apply digest recompute from snapshot ok; fenced
        //   write ok | apply ok → applied; crash → next run applying_unknown
        // ------------------------------------------------------------------
        FulfillState::Applying => match event {
            Event::PreApply {
                snapshot_ok: false, ..
            } => {
                let record = blocked(
                    observed,
                    "tampered: snapshot bytes no longer match the approval record".to_string(),
                    now,
                );
                blocked_stop(
                    record,
                    "snapshot tamper detected".to_string(),
                    &product,
                    "snapshot bytes no longer match the approval record",
                )
            }
            Event::PreApply {
                recomputed_digest,
                plan_payload_digest,
                snapshot_ok: true,
            } => match (recomputed_digest, plan_payload_digest) {
                (Some(snap), Some(plan)) if snap == plan => Decision::Act(TaskKind::Apply),
                (snap, plan) => {
                    let old = plan.unwrap_or_else(|| "<none>".to_string());
                    let new = snap.unwrap_or_else(|| "<none>".to_string());
                    let mut next = to_state(
                        observed,
                        FulfillState::Superseded {
                            old_digest: old.clone(),
                            new_digest: new.clone(),
                        },
                        now,
                    );
                    next.plan_id = None;
                    next.idempotency_key = None;
                    Decision::Advance {
                        record: next,
                        event: format!("superseded at pre-apply ({old} -> {new})"),
                    }
                }
            },
            Event::ApplyFinished(ApplySummary::Applied { run_id }) => {
                let next = to_state(observed, FulfillState::Applied, now);
                Decision::Advance {
                    record: next,
                    event: format!(
                        "applied (run {})",
                        run_id.unwrap_or_else(|| "<none>".to_string())
                    ),
                }
            }
            // skipped_idempotent resolves via the receipt — never assumed.
            Event::ApplyFinished(ApplySummary::SkippedIdempotent { .. }) => {
                Decision::Act(TaskKind::LookupReceipt)
            }
            // skipped_in_flight KEEPS applying (RTE-2).
            Event::ApplyFinished(ApplySummary::SkippedInFlight { prior_run_id }) => {
                Decision::Halt(Stop {
                    message: format!(
                        "an apply for this plan's idempotency key is already in flight \
                         (run {prior_run_id}); re-run once it settles"
                    ),
                    next_command: Some(format!("rocky fulfill {product}")),
                })
            }
            Event::ApplyFinished(ApplySummary::Failed { error }) => {
                let record = blocked(observed, format!("apply failed: {error}"), now);
                blocked_stop(record, "apply failed".to_string(), &product, &error)
            }
            Event::ReceiptResolved(lookup) => decide_receipt(observed, &product, lookup, now),
            // Resume found `applying` begun with no terminal record.
            Event::Reentry => {
                let next = to_state(observed, FulfillState::ApplyingUnknown, now);
                Decision::Advance {
                    record: next,
                    event: "resume found applying with no terminal record".to_string(),
                }
            }
            other => internal_mismatch(observed, &other),
        },

        // ------------------------------------------------------------------
        // applying_unknown | journal shows applying, no terminal |
        //   receipt for the pinned key → applied; none → retry apply
        //   (dedup-safe); backend cannot answer → stays, for a human
        // ------------------------------------------------------------------
        FulfillState::ApplyingUnknown => match event {
            Event::Reentry => Decision::Act(TaskKind::LookupReceipt),
            Event::ReceiptResolved(lookup) => decide_receipt(observed, &product, lookup, now),
            other => internal_mismatch(observed, &other),
        },

        // ------------------------------------------------------------------
        // applied      | apply result | observation → observing
        // observing    | runner       | staleness/test findings journaled
        // observed_failing | red checks | re-read → repair round, or blocked
        //
        // One arm for all three: every entry does the SAME two readings,
        // in the same order, and the verdict — not the state it was read
        // from — decides where the record lands. That is what makes
        // "a crash mid-observation re-reads the checks" structural
        // rather than a property of one code path: `Reentry` from any of
        // the three observes again, and nothing carries the last verdict
        // forward.
        //
        // The staleness/test reading is journaled where it happens, and
        // only then are the DECLARED checks read. Landing `observing` on
        // the first event would record health before the one signal that
        // can contradict it had been looked at.
        // ------------------------------------------------------------------
        FulfillState::Applied | FulfillState::Observing | FulfillState::ObservedFailing => {
            match event {
                Event::Reentry => Decision::Act(TaskKind::Observe),
                Event::ObservationDone {
                    test_green,
                    staleness_ok,
                    detail,
                } => Decision::AdvanceAndAct {
                    record: to_state(observed, observed.state.clone(), now),
                    event: observation_event(test_green, staleness_ok, &detail),
                    task: TaskKind::ObserveChecks {
                        prior_detail: detail,
                    },
                },
                Event::ObservationChecks {
                    failed,
                    errored,
                    warned,
                    deferred,
                    detail,
                    prior_detail,
                    cause,
                } => decide_observation_checks(
                    observed,
                    &product,
                    failed,
                    errored,
                    warned,
                    deferred,
                    &detail,
                    &prior_detail,
                    cause.as_ref(),
                    now,
                ),
                other => internal_mismatch(observed, &other),
            }
        }

        // ------------------------------------------------------------------
        // superseded | old/new digests journaled |
        //   re-enters at spec_approved with the new snapshot
        // ------------------------------------------------------------------
        FulfillState::Superseded { new_digest, .. } => match event {
            Event::Reentry => {
                let mut next = to_state(observed, FulfillState::SpecApproved, now);
                next.spec_digest = Some(new_digest.clone());
                next.plan_id = None;
                next.idempotency_key = None;
                next.drafting_attempts = 0;
                next.repair_rounds = 0;
                // A new approved generation inherits no budget and no
                // evidence: the checks it declares may not even be the
                // checks the old one failed.
                next.data_repair_rounds = 0;
                next.observation_detail = None;
                Decision::Advance {
                    record: next,
                    event: format!("re-entering at spec_approved ({new_digest})"),
                }
            }
            other => internal_mismatch(observed, &other),
        },

        // ------------------------------------------------------------------
        // blocked(reason) | manual `rocky fulfill <product> --retry`
        // ------------------------------------------------------------------
        FulfillState::Blocked { reason } => match event {
            Event::RetryRequested => {
                let re_entry = if observed.spec_digest.is_some() {
                    FulfillState::SpecApproved
                } else {
                    FulfillState::Init
                };
                let mut next = to_state(observed, re_entry, now);
                next.plan_id = None;
                next.idempotency_key = None;
                next.drafting_attempts = 0;
                next.repair_rounds = 0;
                // A human asked for another attempt after reading the
                // remedy, so both budgets refill and the stale evidence
                // goes — the next observation reads the checks fresh.
                next.data_repair_rounds = 0;
                next.observation_detail = None;
                // The check-set pin goes with it. The re-entry is at
                // `spec_approved`, and the only route to an observation
                // from there runs through `verifying`, which pins a
                // fresh digest — so this is behaviour-neutral today.
                // It is cleared anyway so no value from an older
                // preimage scheme can outlive the retry that was
                // printed to escape it, and so any future path that
                // reached observation without re-verifying would find
                // `None` and hold, rather than compare against a stale
                // pin.
                next.checks_digest = None;
                Decision::Advance {
                    record: next,
                    event: "manual retry".to_string(),
                }
            }
            Event::Reentry => Decision::Halt(Stop {
                message: format!("blocked: {reason}"),
                next_command: Some(format!("rocky fulfill {product} --retry")),
            }),
            other => internal_mismatch(observed, &other),
        },
    }
}

/// The shared marker-poll decision for `proposed` and
/// `needs_input(plan_approval)` — one rule, two waiting states.
fn decide_marker(
    observed: &FulfillStateRecord,
    product: &str,
    reviewed: bool,
    invalid: Option<String>,
    plan_payload_digest: Option<String>,
    approved_digest: Option<String>,
    now: DateTime<Utc>,
) -> Decision {
    let plan_id = observed
        .plan_id
        .clone()
        .unwrap_or_else(|| "<unpinned>".to_string());
    if let Some(why) = invalid {
        // A malformed marker never approves and never waits silently.
        return Decision::Halt(Stop {
            message: format!("review marker for plan {plan_id} is invalid: {why}"),
            next_command: Some(format!("rocky review {plan_id} --approve")),
        });
    }
    // Snapshot re-approved while the plan waited → superseded (D6).
    let superseded = matches!(
        (&plan_payload_digest, &approved_digest),
        (Some(plan), Some(approved)) if plan != approved
    );
    if superseded {
        let old = plan_payload_digest.unwrap_or_default();
        let new = approved_digest.unwrap_or_default();
        let event = format!("superseded while awaiting review ({old} -> {new})");
        let mut next = to_state(
            observed,
            FulfillState::Superseded {
                old_digest: old,
                new_digest: new,
            },
            now,
        );
        next.plan_id = None;
        next.idempotency_key = None;
        return Decision::Advance {
            record: next,
            event,
        };
    }
    if reviewed {
        let next = to_state(observed, FulfillState::PlanApproved, now);
        return Decision::Advance {
            record: next,
            event: format!("review marker observed (plan {plan_id})"),
        };
    }
    // Absent marker from `proposed` → needs_input(plan_approval); from
    // needs_input it stays (no rewrite of an identical ask).
    match &observed.state {
        FulfillState::NeedsInput { .. } => Decision::Halt(Stop {
            message: format!(
                "plan {plan_id} awaits human review; after approving, re-run: rocky fulfill {product}"
            ),
            next_command: Some(format!("rocky review {plan_id} --approve")),
        }),
        _ => {
            let next = to_state(
                observed,
                FulfillState::NeedsInput {
                    reason: REASON_PLAN_APPROVAL.to_string(),
                    payload: plan_id.clone(),
                },
                now,
            );
            Decision::AdvanceAndStop {
                record: next,
                event: "awaiting plan review".to_string(),
                stop: Stop {
                    message: format!(
                        "plan {plan_id} awaits human review; after approving, re-run: rocky fulfill {product}"
                    ),
                    next_command: Some(format!("rocky review {plan_id} --approve")),
                },
            }
        }
    }
}

/// The propose-outcome decision at `verifying`.
/// The post-apply verdict on the product's declared data checks — the
/// F3 routing decision.
///
/// Three landings, and which one is reached depends only on the reading:
///
/// ```text
///   clean        → observing              (budget reset, evidence cleared)
///   unevaluable  → applied                (from applied/observing)
///                → observed_failing       (from observed_failing — no new
///                                          news does not clear old news)
///   failing      → observed_failing       first sighting: record and stop
///                → drafting(data_repair)  confirmed: spend one round
///                → blocked                budget exhausted, naming the check
/// ```
///
/// The two-step failing path is deliberate. The bad data is ALREADY
/// applied, so nothing is gained by racing: the first red is recorded in
/// a state a human can see, and only a second reading — a fresh read from
/// a fresh invocation, never the stored verdict — spends a repair round.
/// A transient warehouse blip therefore cannot burn budget, and the
/// "post-data-red pre-repair" crash seam has a real resting state to
/// resume from.
///
/// Forward-only: no arm here rolls anything back. The repaired output
/// re-enters at drafting and leaves through the same propose → human
/// review → apply gates as any other change.
#[allow(clippy::too_many_arguments)]
fn decide_observation_checks(
    observed: &FulfillStateRecord,
    product: &str,
    failed: usize,
    errored: usize,
    warned: usize,
    deferred: Option<usize>,
    detail: &str,
    prior_detail: &str,
    cause: Option<&UnevaluableCause>,
    now: DateTime<Utc>,
) -> Decision {
    let verdict = classify_checks(failed, errored, deferred);
    // One observation, one message. The staleness/test reading is
    // already in the journal; repeating it here is what keeps a single
    // `rocky fulfill` stop from reporting half of what it looked at.
    let whole = if prior_detail.is_empty() {
        detail.to_string()
    } else {
        format!("{prior_detail} | {detail}")
    };
    // `applied` on the pass that first reaches it, `live` thereafter —
    // the pre-F3 wording, kept because it is the difference between
    // "this just shipped" and "this is running".
    let standing = match &observed.state {
        FulfillState::Applied => "applied",
        _ => "live",
    };
    let event = observation_checks_event(&verdict, failed, errored, warned, deferred, detail);
    match verdict {
        // Every declared check ran, none failed. This is the ONLY path
        // to `observing`, and it is where the data-repair budget resets:
        // the cycle closed, so the next red starts from a full ceiling.
        CheckVerdict::Clean => {
            let mut next = to_state(observed, FulfillState::Observing, now);
            next.data_repair_rounds = 0;
            next.observation_detail = None;
            Decision::AdvanceAndStop {
                record: next,
                event,
                stop: Stop {
                    message: format!("product {product} is {standing}; {whole}"),
                    next_command: None,
                },
            }
        }
        // Something could not be evaluated. Never `observing`: claiming
        // health on checks that did not run is the exact dishonesty this
        // work package exists to remove. Never a repair round either —
        // "cannot tell" is not "the data is wrong", and rewriting a
        // model on a suspicion spends a live-table cycle on a guess.
        //
        // This is where the loop diverges from `rocky test
        // --declarative`, which exits non-zero when a check errors. The
        // CLI is reporting to a human who will read the error; the loop
        // is deciding whether to rewrite a model, and the honest answer
        // to an unreadable check is to stop and say so.
        CheckVerdict::Unevaluable => {
            // A digest pinned under an older PREIMAGE SCHEME is handled
            // before anything else, because it is the one cause whose
            // landing state differs. Everything below assumes the two
            // digests were comparable and one of them moved; this one
            // says they were never comparable at all.
            //
            // It cannot land at `applied` like the others. That state is
            // the honest "observation not concluded" holding pattern for
            // conditions a later run can resolve — and this one cannot:
            // re-running recomputes the same current-scheme digest and
            // compares it against the same old-scheme value, forever.
            // `applied` is also in `rocky product approve`'s in-flight
            // refusal set, so the operator would be left with no route
            // out at all. `blocked` has two: `--retry` re-enters at
            // `spec_approved` and the fresh generation pins its own
            // digest at its own verify, and `approve` is accepted from
            // the stop set.
            if matches!(cause, Some(UnevaluableCause::CheckSchemeChanged)) {
                let reason = format!(
                    "{detail} — nothing on disk changed, and no restore alters a hash \
                     algorithm. Re-running this generation cannot resolve it either: only \
                     a fresh generation pins a digest, at its own verify"
                );
                let record = blocked(observed, reason, now);
                return blocked_stop(
                    record,
                    "check-set digest scheme changed under an applied generation".to_string(),
                    product,
                    detail,
                );
            }
            let landing = match &observed.state {
                // No new news does not clear old news: a product already
                // known to be failing stays failing.
                FulfillState::ObservedFailing => FulfillState::ObservedFailing,
                // "Applied, observation not concluded" — the honest
                // state, and the one whose re-entry re-reads.
                _ => FulfillState::Applied,
            };
            let next = to_state(observed, landing, now);
            // One verdict, three causes, three remedies. Printing "run
            // the loop again" for a custody divergence would name a
            // command that re-reads the same diverged file and reports
            // the same thing forever — an instruction that cannot
            // resolve what it was printed for is worse than no
            // instruction.
            // The custody remedy is a RESTORE, not a command, and saying
            // so is the only honest option available.
            //
            // `rocky product compile` was offered here and does not work
            // for either drift class: it refuses outright on sidecar
            // drift (`phase-a-tampered`), and on a `test_definitions.toml`
            // edit it re-lowers the sidecar without touching the
            // definitions, so the expansion still diverges. Nor can the
            // loop adopt the edit — the only route into `verifying` is
            // from `merged`, and an applied product can never reach it
            // again, so no post-apply path can pin a new digest.
            //
            // What remains true: undo the edit and the digests match, or
            // put the change in the spec and approve it, which writes a
            // fresh record at `spec_approved` (outside this table, in the
            // approve verb) and re-pins at that generation's own verify.
            //
            // Those two are SEQUENTIAL, not alternatives. This arm lands
            // the record back at `applied` (see `landing` above), and
            // `rocky product approve` refuses every in-flight state,
            // `applied` included — grounded by
            // `approving_refuses_at_applied_and_permits_at_observing` in
            // rocky-cli, which drives the real verb from both states. So
            // the spec route does not open until the restore has let the
            // loop finish observing and leave `applied`. The message
            // prints that order: a remedy whose second half is refused
            // from the state it is printed in is not a remedy.
            //
            //   custody stop ──▶ applied ──(approve REFUSED)
            //        │
            //        └─ restore ─▶ rocky fulfill ─▶ observing / observed_failing
            //                                              │
            //                                              └─ approve is accepted here
            //
            // `rocky fulfill` is named because it IS the command that
            // resolves this — after the restore, which the message states
            // first so the order is not a guess.
            //
            // The spec route is also QUALIFIED, because it does not fit
            // every divergence. `output.checks` is a list of opaque SQL
            // boolean strings and the lowering turns each one into an
            // `expression` test at `severity = "error"` — so a changed
            // `row_count_range`, a `warning` severity, a `filter`, or a
            // `[[use_test]]` reference has no spec spelling at all.
            // Naming `checks` bare told an operator to carry a change
            // the field cannot hold, which is the same defect as naming
            // a command that cannot run. See `UnevaluableCause` for the
            // grounding and the lowering-side pin.
            //
            // The QUALIFICATION itself then over-corrected, and that is
            // the same defect with the sign flipped. It listed `unique`
            // flat among the unspellable shapes, but `output.grain`
            // lowers straight to one — `unique` on a single grain
            // column, `composite` + `kind = "unique"` over several
            // (`product::lowering::generated_tests`, pinned there in
            // both arms). What has no spelling is uniqueness that is
            // NOT the declared grain. An over-claim about our own gate
            // and an under-claim about our own spec both send an
            // operator down a route the code does not support, so the
            // sentence is checked in both directions by
            // `applied_unevaluable_holds_and_names_the_restore`.
            let (next_command, remedy) = match cause {
                Some(UnevaluableCause::CheckCustody) => (
                    format!("rocky fulfill {product}"),
                    " — restore the file you changed and re-run; the loop cannot adopt an \
                     edit here, because nothing after an apply can re-verify a new set of \
                     checks. To keep the change instead, take it in this order: restore, \
                     re-run until the loop leaves `applied` (`observing` when the checks \
                     pass, `observed_failing` when one is genuinely red), and only then put \
                     the change in the product spec and approve the spec again. Approving \
                     is refused while the state is `applied`, so that order is not \
                     optional. Check first that the spec can hold your change: \
                     `output.checks` takes a SQL boolean and always lowers it to an \
                     error-severity `expression` test, a not-null comes from an \
                     `output.columns` entry with `nullable = false`, and `output.grain` \
                     lowers to exactly one uniqueness check — `unique` on a single grain \
                     column, or `composite` with `kind = \"unique\"` over several. Anything \
                     else has no spec spelling: a uniqueness check that is not the declared \
                     grain, another typed shape such as `row_count_range`, a `warning` \
                     severity, a `filter`, or a `[[use_test]]` reference. For those the \
                     restore is the whole remedy"
                        .to_string(),
                ),
                // Enumerated, not defaulted. A `_ =>` here is how a
                // fourth cause would silently acquire "re-run the loop"
                // — the remedy that is right only when re-running can
                // change the answer. `CheckSchemeChanged` is listed and
                // unreachable because it returned above; writing it out
                // means a variant added later has to come to this match
                // and choose.
                Some(UnevaluableCause::CheckSchemeChanged)
                | Some(UnevaluableCause::Unreadable)
                | None => (format!("rocky fulfill {product}"), String::new()),
            };
            Decision::AdvanceAndStop {
                record: next,
                event,
                stop: Stop {
                    message: format!(
                        "product {product} is applied, but its declared data checks could not \
                         be evaluated, so nothing here says the output is right: {whole}{remedy}"
                    ),
                    next_command: Some(next_command),
                },
            }
        }
        CheckVerdict::Failing => {
            // One ceiling, two counters. `repair_rounds` cannot serve
            // here: `decide_proposed` resets it to 0 on every successful
            // propose, and a data-red cycle proposes every lap, so the
            // bound would never bind. See `FulfillStateRecord::
            // data_repair_rounds`.
            if observed.data_repair_rounds >= MAX_REPAIR_ROUNDS {
                let record = blocked(
                    observed,
                    format!(
                        "the applied output still fails its declared data checks after \
                         {MAX_REPAIR_ROUNDS} repair rounds: {detail}"
                    ),
                    now,
                );
                return blocked_stop(
                    record,
                    format!("data repair budget exhausted: {detail}"),
                    product,
                    detail,
                );
            }
            match &observed.state {
                // Confirmed by a second, independent reading: spend one
                // round. The evidence is refreshed from THIS reading —
                // the worker must act on what is true now, not on what
                // was true when the red was first recorded.
                FulfillState::ObservedFailing => {
                    let mut next = to_state(observed, FulfillState::Drafting, now);
                    next.data_repair_rounds = observed.data_repair_rounds + 1;
                    next.drafting_attempts = 1;
                    // Persisted WITH the transition that decided them, so
                    // a crash before the worker starts resumes into the
                    // same round carrying the same evidence (#1493's
                    // lesson, applied to the data-red path).
                    next.drafting_round = DraftingRound::DataRepair;
                    next.observation_detail = Some(truncate_detail(detail));
                    Decision::AdvanceAndAct {
                        record: next,
                        event: format!(
                            "data repair round {} ({detail})",
                            observed.data_repair_rounds + 1
                        ),
                        task: round_task(DraftingRound::DataRepair),
                    }
                }
                // First sighting: record it and stop. The state says
                // plainly that the live output is failing its own
                // declared checks — it is never a healthy `observing`.
                _ => {
                    let mut next = to_state(observed, FulfillState::ObservedFailing, now);
                    next.observation_detail = Some(truncate_detail(detail));
                    Decision::AdvanceAndStop {
                        record: next,
                        event,
                        stop: Stop {
                            message: format!(
                                "product {product} is applied, and the applied output is failing \
                                 its own declared data checks: {whole} — re-run to confirm the \
                                 reading and start a repair round (the repaired model goes back \
                                 through review before it applies)"
                            ),
                            next_command: Some(format!("rocky fulfill {product}")),
                        },
                    }
                }
            }
        }
    }
}

fn decide_proposed(
    observed: &FulfillStateRecord,
    product: &str,
    outcome: ProposeSummary,
    plan_payload_digest: Option<String>,
    approved_digest: Option<String>,
    idempotency_key: String,
    now: DateTime<Utc>,
) -> Decision {
    match outcome {
        // ProposeOutcome::Written | ReviewRequired both → proposed.
        ProposeSummary::Written { plan_id } | ProposeSummary::ReviewRequired { plan_id, .. } => {
            // Post-propose stale-spec refusal (D1 point 1): the persisted
            // plan's payload digest vs the CURRENT approval.
            let superseded = matches!(
                (&plan_payload_digest, &approved_digest),
                (Some(plan), Some(approved)) if plan != approved
            );
            if superseded {
                let old = plan_payload_digest.unwrap_or_default();
                let new = approved_digest.unwrap_or_default();
                let event = format!("superseded at post-propose ({old} -> {new})");
                let mut next = to_state(
                    observed,
                    FulfillState::Superseded {
                        old_digest: old,
                        new_digest: new,
                    },
                    now,
                );
                next.plan_id = None;
                next.idempotency_key = None;
                return Decision::Advance {
                    record: next,
                    event,
                };
            }
            let mut next = to_state(observed, FulfillState::Proposed, now);
            next.plan_id = Some(plan_id.clone());
            next.idempotency_key = Some(idempotency_key);
            next.repair_rounds = 0;
            Decision::Advance {
                record: next,
                event: format!("proposed (plan {plan_id})"),
            }
        }
        // Denied → blocked, NAMING the policy (nothing persisted).
        ProposeSummary::Denied { refusal } => {
            let record = blocked(
                observed,
                format!("policy denied the propose: {refusal}"),
                now,
            );
            blocked_stop(
                record,
                "propose denied by policy".to_string(),
                product,
                &refusal,
            )
        }
    }
}

/// The receipt-lookup decision, shared by `applying` (a
/// `skipped_idempotent` resolution) and `applying_unknown` (resume).
///
/// Only a terminal `Succeeded` receipt journals `applied`. `InFlight`
/// and `CannotAnswer` keep the state where it is, for a human or a
/// later settle — never a blind retry. `NoRecord`/`Failed` make a retry
/// dedup-safe under the pinned key.
fn decide_receipt(
    observed: &FulfillStateRecord,
    product: &str,
    lookup: ReceiptSummary,
    now: DateTime<Utc>,
) -> Decision {
    match lookup {
        ReceiptSummary::Succeeded { run_id } => {
            let next = to_state(observed, FulfillState::Applied, now);
            Decision::Advance {
                record: next,
                event: format!("receipt found: applied (run {run_id})"),
            }
        }
        ReceiptSummary::NoRecord => retry_or_park(observed, "no receipt", now),
        ReceiptSummary::Failed { run_id } => {
            retry_or_park(observed, &format!("prior apply failed (run {run_id})"), now)
        }
        ReceiptSummary::InFlight { run_id } => match &observed.state {
            FulfillState::ApplyingUnknown => Decision::Halt(Stop {
                message: format!(
                    "an apply is (or appears) in flight under the pinned key (run {run_id}); \
                     re-run once it settles"
                ),
                next_command: Some(format!("rocky fulfill {product}")),
            }),
            _ => {
                let next = to_state(observed, FulfillState::ApplyingUnknown, now);
                Decision::AdvanceAndStop {
                    record: next,
                    event: format!("apply in flight under the pinned key (run {run_id})"),
                    stop: Stop {
                        message: format!(
                            "an apply is in flight under the pinned key (run {run_id}); \
                             re-run once it settles"
                        ),
                        next_command: Some(format!("rocky fulfill {product}")),
                    },
                }
            }
        },
        ReceiptSummary::CannotAnswer { reason } => match &observed.state {
            FulfillState::ApplyingUnknown => Decision::Halt(Stop {
                message: format!(
                    "the idempotency backend cannot answer authoritatively: {reason}\n\
                     the state stays applying_unknown for a human"
                ),
                next_command: None,
            }),
            _ => {
                let next = to_state(observed, FulfillState::ApplyingUnknown, now);
                Decision::AdvanceAndStop {
                    record: next,
                    event: "receipt lookup cannot answer authoritatively".to_string(),
                    stop: Stop {
                        message: format!(
                            "the idempotency backend cannot answer authoritatively: {reason}\n\
                             the state stays applying_unknown for a human"
                        ),
                        next_command: None,
                    },
                }
            }
        },
    }
}

/// The `NoRecord`/`Failed` receipt arms. From `applying_unknown` the
/// retry is dedup-safe: re-run the pre-apply gate then the apply,
/// in-process. From `applying` the same answer is a CONTRADICTION —
/// the engine just said `skipped_idempotent` (a Succeeded receipt
/// exists) and the lookup disagrees — so the state parks at
/// `applying_unknown` for a human instead of retrying into an
/// inconsistent store.
fn retry_or_park(observed: &FulfillStateRecord, detail: &str, now: DateTime<Utc>) -> Decision {
    match &observed.state {
        FulfillState::ApplyingUnknown => {
            let next = to_state(observed, FulfillState::Applying, now);
            Decision::AdvanceAndAct {
                record: next,
                event: format!("{detail}; retrying apply under the pinned key"),
                task: TaskKind::PreApplyCheck,
            }
        }
        _ => {
            let next = to_state(observed, FulfillState::ApplyingUnknown, now);
            Decision::AdvanceAndStop {
                record: next,
                event: format!("receipt contradicts skipped_idempotent ({detail})"),
                stop: Stop {
                    message: format!(
                        "the apply was deflected as already-satisfied, but the \
                         authoritative receipt lookup says {detail} — resolve by \
                         hand before re-running"
                    ),
                    next_command: None,
                },
            }
        }
    }
}

/// The elicitation dispatch: count the attempt BEFORE dispatch onto the
/// PERSISTED record (the claim.rs cycle discipline — a crash mid-task
/// still consumed budget), or block once `MAX_COMPILE_ITERS` dispatches
/// are spent. `failure_detail` names the latest failure in the blocked
/// reason.
fn dispatch_elicitation(
    observed: &FulfillStateRecord,
    product: &str,
    failure_detail: &str,
    now: DateTime<Utc>,
) -> Decision {
    if observed.drafting_attempts >= MAX_COMPILE_ITERS {
        let record = blocked(
            observed,
            format!(
                "elicitation failed {MAX_COMPILE_ITERS} times (max_compile_iters): \
                 {failure_detail}"
            ),
            now,
        );
        return blocked_stop(
            record,
            "elicitation budget exhausted".to_string(),
            product,
            failure_detail,
        );
    }
    let mut next = to_state(observed, FulfillState::Init, now);
    next.drafting_attempts = observed.drafting_attempts + 1;
    Decision::AdvanceAndAct {
        record: next,
        event: format!("elicitation attempt {}", observed.drafting_attempts + 1),
        task: TaskKind::Elicit,
    }
}

/// The drafting dispatch: count the attempt BEFORE dispatch, then act.
///
/// The round is PERSISTED on the record it advances to, so a crash
/// between this transition and the worker's dispatch resumes as the same
/// round rather than silently downgrading a repair to a draft (#1493).
fn dispatch_drafting(
    observed: &FulfillStateRecord,
    product: &str,
    round: DraftingRound,
    now: DateTime<Utc>,
) -> Decision {
    let task = round_task(round);
    if observed.drafting_attempts >= MAX_COMPILE_ITERS {
        let record = blocked(
            observed,
            format!("drafting failed {MAX_COMPILE_ITERS} times (max_compile_iters)"),
            now,
        );
        return blocked_stop(
            record,
            "drafting budget exhausted".to_string(),
            product,
            "drafting attempts exhausted",
        );
    }
    let mut next = to_state(observed, FulfillState::Drafting, now);
    next.drafting_attempts = observed.drafting_attempts + 1;
    next.drafting_round = round;
    Decision::AdvanceAndAct {
        record: next,
        event: format!("{} attempt {}", round.tag(), observed.drafting_attempts + 1),
        task,
    }
}

/// The task that performs `round`. The one mapping from the persisted
/// round to the dispatched task, so a resume and a fresh decision can
/// never disagree.
fn round_task(round: DraftingRound) -> TaskKind {
    match round {
        DraftingRound::Draft => TaskKind::Draft,
        DraftingRound::Repair => TaskKind::Repair,
        DraftingRound::DataRepair => TaskKind::DataRepair,
    }
}

/// The longest `observation_detail` the record will carry.
///
/// The evidence is warehouse-shaped: a model can declare many checks and
/// every one of them can fail at once. The record is compare-and-set as a
/// whole on every transition, so an unbounded string is an unbounded cost
/// on every subsequent write. Truncation is visible, never silent — the
/// marker says what was dropped.
const MAX_OBSERVATION_DETAIL: usize = 4000;

/// Cap the observed evidence at [`MAX_OBSERVATION_DETAIL`], on a char
/// boundary, with a marker naming the loss.
///
/// Pure so the boundary behaviour is pinned by test rather than by
/// reading: slicing a multi-byte string at a byte index panics, and the
/// evidence is the one field in the record built from data the loop does
/// not author.
fn truncate_detail(detail: &str) -> String {
    if detail.len() <= MAX_OBSERVATION_DETAIL {
        return detail.to_string();
    }
    let mut end = MAX_OBSERVATION_DETAIL;
    while end > 0 && !detail.is_char_boundary(end) {
        end -= 1;
    }
    format!(
        "{}… [evidence truncated at {MAX_OBSERVATION_DETAIL} bytes]",
        &detail[..end]
    )
}

/// The three-way verdict on a reading of the declared data checks.
///
/// Named rather than inlined because the ORDER of the arms is the whole
/// decision: positive evidence of failure outranks a partial reading, and
/// a partial reading outranks a clean tally. Inverting the last two would
/// let "we could not evaluate anything" render as health.
#[derive(Debug, Clone, PartialEq, Eq)]
enum CheckVerdict {
    /// At least one declared check failed at `severity = "error"`.
    Failing,
    /// Nothing failed, but the reading is incomplete — some check
    /// errored, or the read could not evaluate everything it declared.
    Unevaluable,
    /// Every declared check was evaluated and none failed. Warnings may
    /// be present; a warning is not a defect.
    Clean,
}

/// Classify a check reading. See [`CheckVerdict`] for why the order is
/// load-bearing.
fn classify_checks(failed: usize, errored: usize, deferred: Option<usize>) -> CheckVerdict {
    if failed > 0 {
        return CheckVerdict::Failing;
    }
    if errored > 0 || deferred != Some(0) {
        return CheckVerdict::Unevaluable;
    }
    CheckVerdict::Clean
}

/// The journal event for one reading of the declared data checks.
fn observation_checks_event(
    verdict: &CheckVerdict,
    failed: usize,
    errored: usize,
    warned: usize,
    deferred: Option<usize>,
    detail: &str,
) -> String {
    let head = match verdict {
        CheckVerdict::Failing => "declared data checks FAILING",
        CheckVerdict::Unevaluable => "declared data checks not evaluable",
        CheckVerdict::Clean => "declared data checks green",
    };
    let deferred = match deferred {
        Some(n) => n.to_string(),
        None => "unknown".to_string(),
    };
    format!(
        "observation: {head} ({failed} failed, {errored} errored, {warned} warned, \
         {deferred} unevaluated): {detail}"
    )
}

/// The ONE wording for unevaluated declared data checks.
///
/// The verify bundle runs before apply, so the target table does not
/// exist yet and the model sidecar's declared checks cannot run.
/// Saying "deferred" keeps the claim true: they did not pass and they
/// did not fail.
///
/// `None` when nothing is deferred, so a caller never renders an empty
/// or zero-valued clause.
pub fn deferred_note(tests_deferred: usize) -> Option<String> {
    if tests_deferred == 0 {
        return None;
    }
    let checks = if tests_deferred == 1 {
        "check"
    } else {
        "checks"
    };
    Some(format!(
        "{tests_deferred} declared data {checks} deferred \
         (not evaluable before the model is materialized)"
    ))
}

/// The wording when the declared checks could not even be counted.
///
/// Still deferred — nothing ran — but the count is withheld rather
/// than guessed, because a number nobody read is not evidence.
pub fn uncounted_deferred_note(why: &str) -> String {
    format!(
        "declared data checks deferred (not evaluable before the model \
         is materialized); count unavailable: {why}"
    )
}

/// The journal event for an all-green verify bundle.
///
/// `detail` is the bundle's own rendering; on the green path it is the
/// deferred-checks note (every red leg is what pushes anything else).
/// Carrying it verbatim is what stops "verify green" from being a bare
/// claim in the journal.
fn verify_green_event(detail: &str) -> String {
    if detail.is_empty() {
        "verify green".to_string()
    } else {
        format!("verify green: {detail}")
    }
}

fn observation_event(test_green: bool, staleness_ok: Option<bool>, detail: &str) -> String {
    let staleness = match staleness_ok {
        Some(true) => "fresh",
        Some(false) => "STALE",
        None => "no freshness budget",
    };
    format!(
        "observation: tests {}, staleness {staleness} ({detail})",
        if test_green { "green" } else { "RED" }
    )
}

/// An event that cannot occur in this state is a runner bug: halt with
/// a diagnostic. This arm REJECTS — it never silently proceeds — so the
/// per-state matches stay honest without a 16×18 cross-product.
fn internal_mismatch(observed: &FulfillStateRecord, event: &Event) -> Decision {
    Decision::Halt(Stop {
        message: format!(
            "internal error: event {event:?} cannot occur in state '{}' — this is a \
             rocky-fulfill bug, not a project problem",
            observed.state.tag()
        ),
        next_command: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, 19, 12, 0, 0).unwrap()
    }

    fn rec(state: FulfillState) -> FulfillStateRecord {
        FulfillStateRecord::new(state, "product:revenue_daily".to_string(), None, None)
    }

    fn me() -> SelfIdentity {
        SelfIdentity {
            pid: 4242,
            start_time: 1_000_000,
        }
    }

    // =====================================================================
    // decide_ownership — claim / stand-down / takeover / grace
    // =====================================================================

    #[test]
    fn ownership_absent_record_claims_init_with_our_stamp() {
        let d = decide_ownership(None, None, me(), "product:revenue_daily", now());
        let OwnershipDecision::Claim(record) = d else {
            panic!("expected Claim, got {d:?}");
        };
        assert_eq!(record.state, FulfillState::Init);
        assert_eq!(record.owner_pid, Some(4242));
        assert_eq!(record.owner_start_time, Some(1_000_000));
        assert_eq!(record.product_id, "product:revenue_daily");
    }

    #[test]
    fn ownership_released_record_claims_immediately() {
        let mut prior = rec(FulfillState::Proposed);
        prior.first_swept_at = Some(stamp(now())); // stale mark must clear
        let d = decide_ownership(Some(&prior), None, me(), "product:revenue_daily", now());
        let OwnershipDecision::Claim(record) = d else {
            panic!("expected Claim, got {d:?}");
        };
        assert_eq!(record.state, FulfillState::Proposed, "state untouched");
        assert_eq!(record.owner_pid, Some(4242));
        assert!(record.first_swept_at.is_none(), "grace mark cleared");
    }

    #[test]
    fn ownership_self_stamp_is_already_owned() {
        let mut prior = rec(FulfillState::Drafting);
        prior.owner_pid = Some(4242);
        prior.owner_start_time = Some(1_000_000);
        let d = decide_ownership(Some(&prior), None, me(), "product:revenue_daily", now());
        assert_eq!(d, OwnershipDecision::AlreadyOwned);
    }

    #[test]
    fn ownership_live_owner_stands_down() {
        let mut prior = rec(FulfillState::Drafting);
        prior.owner_pid = Some(99);
        prior.owner_start_time = Some(5);
        let d = decide_ownership(
            Some(&prior),
            Some(&OwnerLiveness::Alive),
            me(),
            "product:revenue_daily",
            now(),
        );
        assert_eq!(d, OwnershipDecision::StandDown { owner_pid: 99 });
    }

    #[test]
    fn ownership_dead_owner_is_taken_over_immediately() {
        let mut prior = rec(FulfillState::Applying);
        prior.owner_pid = Some(99);
        prior.owner_start_time = Some(5);
        prior.first_swept_at = Some(stamp(now()));
        let d = decide_ownership(
            Some(&prior),
            Some(&OwnerLiveness::Dead),
            me(),
            "product:revenue_daily",
            now(),
        );
        let OwnershipDecision::TakeOver(record) = d else {
            panic!("expected TakeOver, got {d:?}");
        };
        assert_eq!(record.owner_pid, Some(4242));
        assert_eq!(record.owner_start_time, Some(1_000_000));
        assert!(record.first_swept_at.is_none());
        assert_eq!(record.state, FulfillState::Applying, "state untouched");
    }

    #[test]
    fn ownership_unprobeable_owner_stamps_the_grace_first() {
        let mut prior = rec(FulfillState::Drafting);
        prior.owner_pid = Some(99);
        let d = decide_ownership(
            Some(&prior),
            Some(&OwnerLiveness::Indefinite("no probe".to_string())),
            me(),
            "product:revenue_daily",
            now(),
        );
        let OwnershipDecision::StampGrace(record) = d else {
            panic!("expected StampGrace, got {d:?}");
        };
        assert_eq!(record.first_swept_at, Some(stamp(now())));
        assert_eq!(record.owner_pid, Some(99), "the stamp stays until takeover");
    }

    #[test]
    fn ownership_unprobeable_owner_waits_out_the_grace() {
        let mut prior = rec(FulfillState::Drafting);
        prior.owner_pid = Some(99);
        prior.first_swept_at = Some(stamp(now() - Duration::seconds(30)));
        let d = decide_ownership(
            Some(&prior),
            Some(&OwnerLiveness::Indefinite("no probe".to_string())),
            me(),
            "product:revenue_daily",
            now(),
        );
        let OwnershipDecision::WaitGrace { remaining_seconds } = d else {
            panic!("expected WaitGrace, got {d:?}");
        };
        assert_eq!(remaining_seconds, FULFILL_RECOVERY_GRACE.num_seconds() - 30);
    }

    #[test]
    fn ownership_unprobeable_owner_is_taken_over_after_the_grace() {
        let mut prior = rec(FulfillState::Drafting);
        prior.owner_pid = Some(99);
        prior.first_swept_at = Some(stamp(now() - FULFILL_RECOVERY_GRACE));
        let d = decide_ownership(
            Some(&prior),
            Some(&OwnerLiveness::Indefinite("no probe".to_string())),
            me(),
            "product:revenue_daily",
            now(),
        );
        let OwnershipDecision::TakeOver(record) = d else {
            panic!("expected TakeOver, got {d:?}");
        };
        assert_eq!(record.owner_pid, Some(4242));
        assert!(record.first_swept_at.is_none());
    }

    // =====================================================================
    // init | first run | → elicitation task → needs_input(spec_approval)
    // =====================================================================

    #[test]
    fn init_with_a_candidate_adopts_it_and_asks_for_approval() {
        let d = decide(
            &rec(FulfillState::Init),
            Event::CandidateSurface {
                candidate_digest: Some("sha256:aa".into()),
            },
            now(),
        );
        let Decision::AdvanceAndStop { record, stop, .. } = d else {
            panic!("expected AdvanceAndStop, got {d:?}");
        };
        assert_eq!(
            record.state,
            FulfillState::NeedsInput {
                reason: REASON_SPEC_APPROVAL.into(),
                payload: "sha256:aa".into()
            }
        );
        assert_eq!(
            stop.next_command.as_deref(),
            Some("rocky fulfill approve-spec revenue_daily")
        );
    }

    #[test]
    fn init_without_a_candidate_counts_the_attempt_before_dispatch() {
        let d = decide(
            &rec(FulfillState::Init),
            Event::CandidateSurface {
                candidate_digest: None,
            },
            now(),
        );
        let Decision::AdvanceAndAct { record, task, .. } = d else {
            panic!("expected AdvanceAndAct, got {d:?}");
        };
        assert_eq!(record.state, FulfillState::Init);
        assert_eq!(record.drafting_attempts, 1, "pre-counted");
        assert_eq!(task, TaskKind::Elicit);
    }

    #[test]
    fn init_elicitation_budget_exhausted_blocks() {
        let mut prior = rec(FulfillState::Init);
        prior.drafting_attempts = MAX_COMPILE_ITERS;
        let d = decide(
            &prior,
            Event::CandidateSurface {
                candidate_digest: None,
            },
            now(),
        );
        let Decision::AdvanceAndStop { record, stop, .. } = d else {
            panic!("expected AdvanceAndStop, got {d:?}");
        };
        assert!(matches!(record.state, FulfillState::Blocked { .. }));
        assert_eq!(
            stop.next_command.as_deref(),
            Some("rocky fulfill revenue_daily --retry")
        );
    }

    #[test]
    fn init_elicitation_success_stops_at_needs_input_with_the_questions() {
        let mut prior = rec(FulfillState::Init);
        prior.drafting_attempts = 2;
        let d = decide(
            &prior,
            Event::ElicitationFinished {
                written_digest: Some("sha256:bb".into()),
                questions: vec!["is refunds excluded?".into()],
                error: None,
            },
            now(),
        );
        let Decision::AdvanceAndStop { record, stop, .. } = d else {
            panic!("expected AdvanceAndStop, got {d:?}");
        };
        assert_eq!(
            record.state,
            FulfillState::NeedsInput {
                reason: REASON_SPEC_APPROVAL.into(),
                payload: "sha256:bb".into()
            }
        );
        assert_eq!(record.drafting_attempts, 0, "cycle reset");
        assert!(stop.message.contains("is refunds excluded?"));
    }

    #[test]
    fn init_elicitation_failure_retries_within_budget_then_blocks() {
        // A failed attempt's retry must consume PERSISTED budget: the
        // decision carries a record with the incremented counter (the
        // caller CASes it before dispatching), never a bare Act.
        let mut prior = rec(FulfillState::Init);
        prior.drafting_attempts = 1;
        let d = decide(
            &prior,
            Event::ElicitationFinished {
                written_digest: None,
                questions: vec![],
                error: Some("driver timeout".into()),
            },
            now(),
        );
        let Decision::AdvanceAndAct { record, task, .. } = d else {
            panic!("expected AdvanceAndAct (persisted increment), got {d:?}");
        };
        assert_eq!(record.drafting_attempts, 2, "budget consumed on the record");
        assert_eq!(task, TaskKind::Elicit);

        let mut exhausted = rec(FulfillState::Init);
        exhausted.drafting_attempts = MAX_COMPILE_ITERS;
        let d = decide(
            &exhausted,
            Event::ElicitationFinished {
                written_digest: None,
                questions: vec![],
                error: Some("driver timeout".into()),
            },
            now(),
        );
        let Decision::AdvanceAndStop { record, .. } = d else {
            panic!("expected AdvanceAndStop, got {d:?}");
        };
        let FulfillState::Blocked { reason } = &record.state else {
            panic!("expected Blocked");
        };
        assert!(reason.contains("driver timeout"));
    }

    /// S2 regression: walking the DECIDED records through repeated
    /// failures blocks at exactly `MAX_COMPILE_ITERS` dispatches — the
    /// budget lives on the record the caller persists, so no failure
    /// path can loop for free.
    #[test]
    fn elicitation_failure_loop_blocks_at_exactly_the_budget() {
        let mut record = rec(FulfillState::Init);
        let mut dispatches = 0u32;
        loop {
            // Cold gather: no candidate on disk.
            let d = decide(
                &record,
                Event::CandidateSurface {
                    candidate_digest: None,
                },
                now(),
            );
            match d {
                Decision::AdvanceAndAct {
                    record: next, task, ..
                } => {
                    assert_eq!(task, TaskKind::Elicit);
                    dispatches += 1;
                    record = next;
                }
                Decision::AdvanceAndStop { record: next, .. } => {
                    assert!(
                        matches!(next.state, FulfillState::Blocked { .. }),
                        "the only stop on this path is blocked"
                    );
                    break;
                }
                other => panic!("unexpected decision {other:?}"),
            }
            // The dispatched attempt fails; the retry decision must also
            // ride the persisted counter.
            let d = decide(
                &record,
                Event::ElicitationFinished {
                    written_digest: None,
                    questions: vec![],
                    error: Some("driver exploded".into()),
                },
                now(),
            );
            match d {
                Decision::AdvanceAndAct { record: next, .. } => {
                    dispatches += 1;
                    record = next;
                }
                Decision::AdvanceAndStop { record: next, .. } => {
                    assert!(matches!(next.state, FulfillState::Blocked { .. }));
                    break;
                }
                other => panic!("unexpected decision {other:?}"),
            }
        }
        assert_eq!(
            dispatches, MAX_COMPILE_ITERS,
            "exactly max_compile_iters driver dispatches, then blocked"
        );
    }

    // =====================================================================
    // elicited (adopted/legacy records) → needs_input(spec_approval)
    // =====================================================================

    #[test]
    fn elicited_with_a_candidate_asks_for_approval() {
        let d = decide(
            &rec(FulfillState::Elicited),
            Event::CandidateSurface {
                candidate_digest: Some("sha256:cc".into()),
            },
            now(),
        );
        let Decision::AdvanceAndStop { record, .. } = d else {
            panic!("expected AdvanceAndStop, got {d:?}");
        };
        assert_eq!(
            record.state,
            FulfillState::NeedsInput {
                reason: REASON_SPEC_APPROVAL.into(),
                payload: "sha256:cc".into()
            }
        );
    }

    #[test]
    fn elicited_with_the_candidate_gone_restarts() {
        let d = decide(
            &rec(FulfillState::Elicited),
            Event::CandidateSurface {
                candidate_digest: None,
            },
            now(),
        );
        let Decision::Advance { record, .. } = d else {
            panic!("expected Advance, got {d:?}");
        };
        assert_eq!(record.state, FulfillState::Init);
    }

    // =====================================================================
    // needs_input(spec_approval) | approve CAS ok → spec_approved;
    // candidate edited → stays (new digest shown)
    // =====================================================================

    fn needs_spec_approval(payload: &str) -> FulfillStateRecord {
        rec(FulfillState::NeedsInput {
            reason: REASON_SPEC_APPROVAL.into(),
            payload: payload.into(),
        })
    }

    #[test]
    fn spec_approval_observed_advances_to_spec_approved() {
        let d = decide(
            &needs_spec_approval("sha256:aa"),
            Event::ApprovalSurface {
                candidate_digest: Some("sha256:aa".into()),
                approved_digest: Some("sha256:aa".into()),
            },
            now(),
        );
        let Decision::Advance { record, .. } = d else {
            panic!("expected Advance, got {d:?}");
        };
        assert_eq!(record.state, FulfillState::SpecApproved);
        assert_eq!(record.spec_digest.as_deref(), Some("sha256:aa"));
    }

    #[test]
    fn spec_approval_candidate_edit_stays_and_shows_the_new_digest() {
        let d = decide(
            &needs_spec_approval("sha256:aa"),
            Event::ApprovalSurface {
                candidate_digest: Some("sha256:NEW".into()),
                approved_digest: None,
            },
            now(),
        );
        let Decision::AdvanceAndStop { record, stop, .. } = d else {
            panic!("expected AdvanceAndStop, got {d:?}");
        };
        assert_eq!(
            record.state,
            FulfillState::NeedsInput {
                reason: REASON_SPEC_APPROVAL.into(),
                payload: "sha256:NEW".into()
            }
        );
        assert!(stop.message.contains("sha256:NEW"));
    }

    #[test]
    fn spec_approval_unchanged_candidate_halts_without_a_write() {
        let d = decide(
            &needs_spec_approval("sha256:aa"),
            Event::ApprovalSurface {
                candidate_digest: Some("sha256:aa".into()),
                approved_digest: None,
            },
            now(),
        );
        let Decision::Halt(stop) = d else {
            panic!("expected Halt, got {d:?}");
        };
        assert_eq!(
            stop.next_command.as_deref(),
            Some("rocky fulfill approve-spec revenue_daily")
        );
    }

    #[test]
    fn spec_approval_candidate_deleted_restarts() {
        let d = decide(
            &needs_spec_approval("sha256:aa"),
            Event::ApprovalSurface {
                candidate_digest: None,
                approved_digest: None,
            },
            now(),
        );
        let Decision::Advance { record, .. } = d else {
            panic!("expected Advance, got {d:?}");
        };
        assert_eq!(record.state, FulfillState::Init);
    }

    // =====================================================================
    // needs_input(policy) re-entry
    // =====================================================================

    fn needs_policy() -> FulfillStateRecord {
        rec(FulfillState::NeedsInput {
            reason: REASON_POLICY.into(),
            payload: String::new(),
        })
    }

    #[test]
    fn policy_pass_advances_to_spec_approved() {
        let d = decide(
            &needs_policy(),
            Event::PostureVerified(PostureStatus::Pass),
            now(),
        );
        let Decision::Advance { record, .. } = d else {
            panic!("expected Advance, got {d:?}");
        };
        assert_eq!(record.state, FulfillState::SpecApproved);
    }

    #[test]
    fn policy_still_needed_halts_with_the_paste_block() {
        let d = decide(
            &needs_policy(),
            Event::PostureVerified(PostureStatus::NeedsInput {
                paste_block: "[policy]\nversion = 1".into(),
                reason: "no [policy] block".into(),
            }),
            now(),
        );
        let Decision::Halt(stop) = d else {
            panic!("expected Halt, got {d:?}");
        };
        assert!(stop.message.contains("[policy]\nversion = 1"));
    }

    #[test]
    fn policy_hard_fail_blocks() {
        let d = decide(
            &needs_policy(),
            Event::PostureVerified(PostureStatus::Fail {
                reason: "identity collision".into(),
            }),
            now(),
        );
        let Decision::AdvanceAndStop { record, .. } = d else {
            panic!("expected AdvanceAndStop, got {d:?}");
        };
        assert!(matches!(record.state, FulfillState::Blocked { .. }));
    }

    // =====================================================================
    // spec_approved | snapshot exists, journal sha matches |
    // verify pass → Phase A → lowered_contract; verify fail → needs_input
    // =====================================================================

    #[test]
    fn spec_approved_snapshot_tamper_blocks() {
        let d = decide(
            &rec(FulfillState::SpecApproved),
            Event::SnapshotVerify {
                snapshot_ok: false,
                detail: "digest mismatch".into(),
            },
            now(),
        );
        let Decision::AdvanceAndStop { record, .. } = d else {
            panic!("expected AdvanceAndStop, got {d:?}");
        };
        let FulfillState::Blocked { reason } = &record.state else {
            panic!("expected Blocked");
        };
        assert!(reason.contains("tampered"));
    }

    #[test]
    fn spec_approved_snapshot_ok_verifies_the_posture() {
        let d = decide(
            &rec(FulfillState::SpecApproved),
            Event::SnapshotVerify {
                snapshot_ok: true,
                detail: String::new(),
            },
            now(),
        );
        assert_eq!(d, Decision::Act(TaskKind::VerifyPosture));
    }

    #[test]
    fn spec_approved_posture_pass_lowers_phase_a() {
        let d = decide(
            &rec(FulfillState::SpecApproved),
            Event::PostureVerified(PostureStatus::Pass),
            now(),
        );
        assert_eq!(d, Decision::Act(TaskKind::RunPhaseA));
    }

    #[test]
    fn spec_approved_posture_needs_input_stops_with_the_paste_block() {
        let d = decide(
            &rec(FulfillState::SpecApproved),
            Event::PostureVerified(PostureStatus::NeedsInput {
                paste_block: "[policy]".into(),
                reason: "posture drift".into(),
            }),
            now(),
        );
        let Decision::AdvanceAndStop { record, stop, .. } = d else {
            panic!("expected AdvanceAndStop, got {d:?}");
        };
        assert_eq!(
            record.state,
            FulfillState::NeedsInput {
                reason: REASON_POLICY.into(),
                payload: "[policy]".into()
            }
        );
        assert!(stop.message.contains("[policy]"));
    }

    #[test]
    fn spec_approved_posture_fail_blocks() {
        let d = decide(
            &rec(FulfillState::SpecApproved),
            Event::PostureVerified(PostureStatus::Fail {
                reason: "collision".into(),
            }),
            now(),
        );
        let Decision::AdvanceAndStop { record, .. } = d else {
            panic!("expected AdvanceAndStop, got {d:?}");
        };
        assert!(matches!(record.state, FulfillState::Blocked { .. }));
    }

    #[test]
    fn spec_approved_phase_a_commit_advances_and_resets_the_cycle() {
        let mut prior = rec(FulfillState::SpecApproved);
        prior.drafting_attempts = 3;
        let d = decide(
            &prior,
            Event::PhaseAResult {
                ok: true,
                detail: "phase A committed".into(),
            },
            now(),
        );
        let Decision::Advance { record, .. } = d else {
            panic!("expected Advance, got {d:?}");
        };
        assert_eq!(record.state, FulfillState::LoweredContract);
        assert_eq!(record.drafting_attempts, 0);
    }

    #[test]
    fn spec_approved_phase_a_reject_blocks() {
        let d = decide(
            &rec(FulfillState::SpecApproved),
            Event::PhaseAResult {
                ok: false,
                detail: "unlowerable field".into(),
            },
            now(),
        );
        let Decision::AdvanceAndStop { record, .. } = d else {
            panic!("expected AdvanceAndStop, got {d:?}");
        };
        let FulfillState::Blocked { reason } = &record.state else {
            panic!("expected Blocked");
        };
        assert!(reason.contains("unlowerable field"));
    }

    // =====================================================================
    // lowered_contract → drafting (attempt pre-counted)
    // =====================================================================

    #[test]
    fn lowered_contract_dispatches_drafting_with_a_counted_attempt() {
        let d = decide(&rec(FulfillState::LoweredContract), Event::Reentry, now());
        let Decision::AdvanceAndAct { record, task, .. } = d else {
            panic!("expected AdvanceAndAct, got {d:?}");
        };
        assert_eq!(record.state, FulfillState::Drafting);
        assert_eq!(record.drafting_attempts, 1);
        assert_eq!(task, TaskKind::Draft);
    }

    #[test]
    fn lowered_contract_with_exhausted_attempts_blocks() {
        let mut prior = rec(FulfillState::LoweredContract);
        prior.drafting_attempts = MAX_COMPILE_ITERS;
        let d = decide(&prior, Event::Reentry, now());
        let Decision::AdvanceAndStop { record, .. } = d else {
            panic!("expected AdvanceAndStop, got {d:?}");
        };
        assert!(matches!(record.state, FulfillState::Blocked { .. }));
    }

    // =====================================================================
    // drafting | driver exit 0 → merged path; failure ≤ retries → retry;
    // else blocked. merged precondition: byte-verify → blocked(tampered)
    // =====================================================================

    #[test]
    fn drafting_success_byte_verifies_phase_a_before_merging() {
        let d = decide(
            &rec(FulfillState::Drafting),
            Event::DraftingFinished { error: None },
            now(),
        );
        assert_eq!(d, Decision::Act(TaskKind::ByteVerifyPhaseA));
    }

    #[test]
    fn drafting_failure_retries_within_budget() {
        let mut prior = rec(FulfillState::Drafting);
        prior.drafting_attempts = 2;
        let d = decide(
            &prior,
            Event::DraftingFinished {
                error: Some("worker exited 1".into()),
            },
            now(),
        );
        let Decision::AdvanceAndAct { record, task, .. } = d else {
            panic!("expected AdvanceAndAct, got {d:?}");
        };
        assert_eq!(record.drafting_attempts, 3);
        assert_eq!(task, TaskKind::Draft);
    }

    #[test]
    fn drafting_failure_exhausted_blocks_with_the_error() {
        let mut prior = rec(FulfillState::Drafting);
        prior.drafting_attempts = MAX_COMPILE_ITERS;
        let d = decide(
            &prior,
            Event::DraftingFinished {
                error: Some("transcript: /t/x.log".into()),
            },
            now(),
        );
        let Decision::AdvanceAndStop { record, .. } = d else {
            panic!("expected AdvanceAndStop, got {d:?}");
        };
        let FulfillState::Blocked { reason } = &record.state else {
            panic!("expected Blocked");
        };
        assert!(reason.contains("/t/x.log"));
    }

    #[test]
    fn drafting_clean_byte_verify_runs_phase_b() {
        let d = decide(
            &rec(FulfillState::Drafting),
            Event::ArtifactCheck { problems: vec![] },
            now(),
        );
        assert_eq!(d, Decision::Act(TaskKind::RunPhaseB));
    }

    #[test]
    fn drafting_phase_a_tamper_blocks_as_tampered() {
        let d = decide(
            &rec(FulfillState::Drafting),
            Event::ArtifactCheck {
                problems: vec!["contract hash mismatch".into()],
            },
            now(),
        );
        let Decision::AdvanceAndStop { record, .. } = d else {
            panic!("expected AdvanceAndStop, got {d:?}");
        };
        let FulfillState::Blocked { reason } = &record.state else {
            panic!("expected Blocked");
        };
        assert!(reason.contains("tampered"));
        assert!(reason.contains("contract hash mismatch"));
    }

    #[test]
    fn drafting_phase_b_commit_advances_to_merged() {
        let d = decide(
            &rec(FulfillState::Drafting),
            Event::PhaseBResult {
                ok: true,
                detail: String::new(),
            },
            now(),
        );
        let Decision::Advance { record, .. } = d else {
            panic!("expected Advance, got {d:?}");
        };
        assert_eq!(record.state, FulfillState::Merged);
    }

    #[test]
    fn drafting_phase_b_reject_blocks() {
        let d = decide(
            &rec(FulfillState::Drafting),
            Event::PhaseBResult {
                ok: false,
                detail: "merge refused".into(),
            },
            now(),
        );
        let Decision::AdvanceAndStop { record, .. } = d else {
            panic!("expected AdvanceAndStop, got {d:?}");
        };
        assert!(matches!(record.state, FulfillState::Blocked { .. }));
    }

    #[test]
    fn drafting_resume_redispatches_within_budget_and_blocks_after() {
        let mut prior = rec(FulfillState::Drafting);
        prior.drafting_attempts = 1;
        let d = decide(&prior, Event::Reentry, now());
        let Decision::AdvanceAndAct { record, task, .. } = d else {
            panic!("expected AdvanceAndAct, got {d:?}");
        };
        assert_eq!(record.drafting_attempts, 2);
        assert_eq!(task, TaskKind::Draft);

        let mut exhausted = rec(FulfillState::Drafting);
        exhausted.drafting_attempts = MAX_COMPILE_ITERS;
        let d = decide(&exhausted, Event::Reentry, now());
        let Decision::AdvanceAndStop { record, .. } = d else {
            panic!("expected AdvanceAndStop, got {d:?}");
        };
        assert!(matches!(record.state, FulfillState::Blocked { .. }));
    }

    // =====================================================================
    // merged → verifying
    // =====================================================================

    #[test]
    fn merged_advances_to_verifying() {
        let d = decide(&rec(FulfillState::Merged), Event::Reentry, now());
        let Decision::Advance { record, .. } = d else {
            panic!("expected Advance, got {d:?}");
        };
        assert_eq!(record.state, FulfillState::Verifying);
    }

    // =====================================================================
    // verifying | green → controlled propose → proposed;
    // red ≤ repair rounds → drafting; else blocked
    // =====================================================================

    /// An all-green bundle reporting `tests_deferred` unevaluated
    /// declared data checks, rendered exactly as `verify_bundle` does.
    fn green_bundle_with(tests_deferred: usize) -> Event {
        Event::VerifyBundle {
            compile_green: true,
            test_green: true,
            posture_green: true,
            manifest_total: true,
            tests_deferred: Some(tests_deferred),
            detail: deferred_note(tests_deferred).unwrap_or_default(),
            checks_digest: Some("sha256:verified".to_string()),
        }
    }

    fn green_bundle() -> Event {
        green_bundle_with(0)
    }

    #[test]
    fn verifying_gathers_the_bundle_then_proposes_on_green() {
        let d = decide(&rec(FulfillState::Verifying), Event::Reentry, now());
        assert_eq!(d, Decision::Act(TaskKind::VerifyBundle));
        let d = decide(&rec(FulfillState::Verifying), green_bundle(), now());
        let Decision::AdvanceAndAct {
            record,
            task,
            event,
        } = d
        else {
            panic!("expected AdvanceAndAct (the green verdict is journaled), got {d:?}");
        };
        assert_eq!(record.state, FulfillState::Verifying);
        assert_eq!(task, TaskKind::Propose);
        assert_eq!(event, "verify green");
    }

    #[test]
    fn deferred_declared_checks_are_journaled_and_never_block_the_propose() {
        // The anti-vacuity pin for #1495. A product whose declared check
        // WOULD fail (say `revenue_eur >= 0` against negative rows) still
        // verifies green, because that check runs against a materialised
        // table and this gate runs before apply. Gating is deliberately
        // unchanged — the loop still proposes — but the journal now
        // records what green did NOT cover, with the exact count.
        let d = decide(&rec(FulfillState::Verifying), green_bundle_with(6), now());
        let Decision::AdvanceAndAct {
            record,
            task,
            event,
        } = d
        else {
            panic!("deferred checks must not change the decision, got {d:?}");
        };
        assert_eq!(task, TaskKind::Propose, "deferred is not a failure");
        assert_eq!(record.state, FulfillState::Verifying);
        assert_eq!(
            event,
            "verify green: 6 declared data checks deferred \
             (not evaluable before the model is materialized)"
        );
        assert!(
            !event.contains("passed"),
            "a deferred check must never read as passed: {event}"
        );
    }

    #[test]
    fn the_deferred_note_states_the_exact_count_and_vanishes_at_zero() {
        // No false alarm: nothing deferred renders no clause at all, and
        // the bare green event stays bare. (A real product spec always
        // lowers at least its grain test, so zero is unreachable through
        // `verify_bundle`; the helper still has to be honest at zero.)
        assert_eq!(deferred_note(0), None);
        assert_eq!(verify_green_event(""), "verify green");
        // The count is stated exactly, and reads as English at one.
        assert_eq!(
            deferred_note(1).expect("one is deferred"),
            "1 declared data check deferred \
             (not evaluable before the model is materialized)"
        );
        assert_eq!(
            deferred_note(6).expect("six are deferred"),
            "6 declared data checks deferred \
             (not evaluable before the model is materialized)"
        );
    }

    #[test]
    fn a_count_that_could_not_be_read_is_never_reported_as_zero() {
        // `Some(0)` claims "there are none"; `None` admits "nobody
        // read it". Collapsing the second into the first would be the
        // same lie in a new place.
        let note = uncounted_deferred_note("models/revenue_daily.toml does not parse: bad line 3");
        assert!(note.starts_with("declared data checks deferred"));
        assert!(note.contains("count unavailable: models/revenue_daily.toml does not parse"));
        assert!(
            !note.contains(" 0 "),
            "an unknown count must not render as zero: {note}"
        );
        let d = decide(
            &rec(FulfillState::Verifying),
            Event::VerifyBundle {
                compile_green: true,
                test_green: true,
                posture_green: true,
                manifest_total: true,
                tests_deferred: None,
                detail: note.clone(),
                checks_digest: Some("sha256:verified".to_string()),
            },
            now(),
        );
        let Decision::AdvanceAndAct { task, event, .. } = d else {
            panic!("an uncountable sidecar must not change the decision, got {d:?}");
        };
        assert_eq!(
            task,
            TaskKind::Propose,
            "not knowing is still not a failure"
        );
        assert_eq!(event, format!("verify green: {note}"));
    }

    #[test]
    fn a_model_that_fails_to_execute_is_still_red_even_with_checks_deferred() {
        // The gate this fix must NOT relax: a model that fails to run is
        // red, deferred count or not.
        let mut prior = rec(FulfillState::Verifying);
        prior.repair_rounds = 1;
        let d = decide(
            &prior,
            Event::VerifyBundle {
                compile_green: true,
                test_green: false,
                posture_green: true,
                manifest_total: true,
                tests_deferred: Some(6),
                detail: "test failures: revenue_daily: binder error".into(),
                checks_digest: Some("sha256:verified".to_string()),
            },
            now(),
        );
        let Decision::AdvanceAndAct { record, task, .. } = d else {
            panic!("a failing model must still dispatch a repair, got {d:?}");
        };
        assert_eq!(task, TaskKind::Repair, "an execution failure is still red");
        assert_eq!(record.state, FulfillState::Drafting);
    }

    #[test]
    fn verifying_red_dispatches_a_repair_round() {
        let mut prior = rec(FulfillState::Verifying);
        prior.repair_rounds = 1;
        prior.drafting_attempts = 5;
        let d = decide(
            &prior,
            Event::VerifyBundle {
                compile_green: false,
                test_green: true,
                posture_green: true,
                manifest_total: true,
                tests_deferred: Some(6),
                detail: "E012 on revenue_eur".into(),
                checks_digest: Some("sha256:verified".to_string()),
            },
            now(),
        );
        let Decision::AdvanceAndAct {
            record,
            task,
            event,
        } = d
        else {
            panic!("expected AdvanceAndAct, got {d:?}");
        };
        assert_eq!(record.state, FulfillState::Drafting);
        assert_eq!(record.repair_rounds, 2);
        assert_eq!(record.drafting_attempts, 1, "fresh drafting cycle");
        assert_eq!(task, TaskKind::Repair);
        assert!(event.contains("E012 on revenue_eur"));
        assert_eq!(
            record.drafting_round,
            DraftingRound::Repair,
            "the decided round is persisted WITH the transition, not left \
             for the dispatch to remember"
        );
    }

    // ------------- the round survives a crash and a retry (#1493) -----------

    /// The record the repair transition above leaves on disk, as a
    /// resume would read it back.
    fn crashed_mid_repair() -> FulfillStateRecord {
        let mut prior = rec(FulfillState::Verifying);
        prior.repair_rounds = 1;
        prior.drafting_attempts = 5;
        let d = decide(
            &prior,
            Event::VerifyBundle {
                compile_green: false,
                test_green: true,
                posture_green: true,
                manifest_total: true,
                tests_deferred: None,
                detail: "E012 on revenue_eur".into(),
                checks_digest: Some("sha256:verified".to_string()),
            },
            now(),
        );
        let Decision::AdvanceAndAct { record, .. } = d else {
            panic!("expected the repair transition, got {d:?}");
        };
        record
    }

    #[test]
    fn a_crash_between_the_repair_cas_and_its_worker_resumes_as_a_repair() {
        // The #1493 F2 defect: the repair transition CASes `drafting`
        // and then dispatches. A crash in between leaves the record at
        // `drafting`, and the cold resume re-enters with `Reentry`.
        // Before the fix that arm hard-coded `TaskKind::Draft`, so the
        // resumed round got the DRAFTING brief and the drafting budget
        // for a round the machine had decided was a repair.
        let resumed = crashed_mid_repair();
        assert_eq!(resumed.state, FulfillState::Drafting);

        let d = decide(&resumed, Event::Reentry, now());
        let Decision::AdvanceAndAct {
            record,
            task,
            event,
        } = d
        else {
            panic!("expected AdvanceAndAct, got {d:?}");
        };
        assert_eq!(
            task,
            TaskKind::Repair,
            "a resumed repair must dispatch the REPAIR task — the brief and \
             the budget both key off it"
        );
        assert_eq!(
            record.drafting_round,
            DraftingRound::Repair,
            "and the round stays persisted across the resume"
        );
        assert!(
            event.starts_with("repair attempt"),
            "the journal names the round it actually dispatched: {event}"
        );
    }

    #[test]
    fn a_failed_repair_driver_retries_as_a_repair_not_a_draft() {
        // Same defect, second arm: the driver-failure retry also
        // hard-coded `TaskKind::Draft`, so a repair whose worker failed
        // once came back as a plain draft.
        let resumed = crashed_mid_repair();
        let d = decide(
            &resumed,
            Event::DraftingFinished {
                error: Some("driver exit 1".into()),
            },
            now(),
        );
        let Decision::AdvanceAndAct { task, record, .. } = d else {
            panic!("expected AdvanceAndAct, got {d:?}");
        };
        assert_eq!(task, TaskKind::Repair, "a retried repair is still a repair");
        assert_eq!(record.drafting_round, DraftingRound::Repair);
    }

    #[test]
    fn a_first_draft_carrying_repair_rounds_is_still_a_draft() {
        // Why the round is PERSISTED and not derived from
        // `repair_rounds`: that counter survives into a re-approved
        // generation's first pass, so `repair_rounds > 0` does not mean
        // "this round is a repair". A derivation would mis-dispatch
        // exactly here.
        let mut lowered = rec(FulfillState::LoweredContract);
        lowered.repair_rounds = 2;
        let d = decide(&lowered, Event::Reentry, now());
        let Decision::AdvanceAndAct { task, record, .. } = d else {
            panic!("expected AdvanceAndAct, got {d:?}");
        };
        assert_eq!(
            task,
            TaskKind::Draft,
            "the first pass over a fresh contract is a draft whatever the \
             repair counter says"
        );
        assert_eq!(record.drafting_round, DraftingRound::Draft);

        // And the resume of THAT record stays a draft too.
        let d = decide(&record, Event::Reentry, now());
        let Decision::AdvanceAndAct { task, .. } = d else {
            panic!("expected AdvanceAndAct, got {d:?}");
        };
        assert_eq!(task, TaskKind::Draft);
    }

    #[test]
    fn verifying_red_after_max_repair_rounds_blocks() {
        let mut prior = rec(FulfillState::Verifying);
        prior.repair_rounds = MAX_REPAIR_ROUNDS;
        let d = decide(
            &prior,
            Event::VerifyBundle {
                compile_green: true,
                test_green: false,
                posture_green: true,
                manifest_total: true,
                tests_deferred: Some(6),
                detail: "unique(client_id,date) failed".into(),
                checks_digest: Some("sha256:verified".to_string()),
            },
            now(),
        );
        let Decision::AdvanceAndStop { record, .. } = d else {
            panic!("expected AdvanceAndStop, got {d:?}");
        };
        let FulfillState::Blocked { reason } = &record.state else {
            panic!("expected Blocked");
        };
        assert!(reason.contains("repair rounds"));
    }

    #[test]
    fn verifying_written_and_review_required_both_pin_and_advance_to_proposed() {
        for outcome in [
            ProposeSummary::Written {
                plan_id: "plan-1".into(),
            },
            ProposeSummary::ReviewRequired {
                plan_id: "plan-1".into(),
                refusal: "requires review".into(),
            },
        ] {
            let mut prior = rec(FulfillState::Verifying);
            prior.repair_rounds = 2;
            let d = decide(
                &prior,
                Event::Proposed {
                    outcome,
                    plan_payload_digest: Some("sha256:aa".into()),
                    approved_digest: Some("sha256:aa".into()),
                    idempotency_key: "product:revenue_daily@sha256:aa@7".into(),
                },
                now(),
            );
            let Decision::Advance { record, .. } = d else {
                panic!("expected Advance, got {d:?}");
            };
            assert_eq!(record.state, FulfillState::Proposed);
            assert_eq!(record.plan_id.as_deref(), Some("plan-1"));
            assert_eq!(
                record.idempotency_key.as_deref(),
                Some("product:revenue_daily@sha256:aa@7")
            );
            assert_eq!(record.repair_rounds, 0, "cycle closed");
        }
    }

    #[test]
    fn verifying_post_propose_digest_mismatch_supersedes() {
        let d = decide(
            &rec(FulfillState::Verifying),
            Event::Proposed {
                outcome: ProposeSummary::Written {
                    plan_id: "plan-1".into(),
                },
                plan_payload_digest: Some("sha256:OLD".into()),
                approved_digest: Some("sha256:NEW".into()),
                idempotency_key: "k".into(),
            },
            now(),
        );
        let Decision::Advance { record, .. } = d else {
            panic!("expected Advance, got {d:?}");
        };
        assert_eq!(
            record.state,
            FulfillState::Superseded {
                old_digest: "sha256:OLD".into(),
                new_digest: "sha256:NEW".into()
            }
        );
        assert!(record.plan_id.is_none(), "pins cleared");
    }

    #[test]
    fn verifying_denied_blocks_naming_the_policy() {
        let d = decide(
            &rec(FulfillState::Verifying),
            Event::Proposed {
                outcome: ProposeSummary::Denied {
                    refusal: "model 'revenue_daily', rule #2, deny".into(),
                },
                plan_payload_digest: None,
                approved_digest: Some("sha256:aa".into()),
                idempotency_key: "k".into(),
            },
            now(),
        );
        let Decision::AdvanceAndStop { record, stop, .. } = d else {
            panic!("expected AdvanceAndStop, got {d:?}");
        };
        let FulfillState::Blocked { reason } = &record.state else {
            panic!("expected Blocked");
        };
        assert!(reason.contains("rule #2"), "{reason}");
        assert!(stop.message.contains("rule #2"));
    }

    // =====================================================================
    // proposed | marker present+valid → plan_approved;
    // absent → needs_input(plan_approval); re-approval → superseded
    // =====================================================================

    fn proposed() -> FulfillStateRecord {
        let mut record = rec(FulfillState::Proposed);
        record.plan_id = Some("plan-1".into());
        record.idempotency_key = Some("k@1".into());
        record
    }

    #[test]
    fn proposed_marker_absent_asks_for_review() {
        let d = decide(
            &proposed(),
            Event::MarkerPoll {
                reviewed: false,
                invalid: None,
                plan_payload_digest: Some("sha256:aa".into()),
                approved_digest: Some("sha256:aa".into()),
            },
            now(),
        );
        let Decision::AdvanceAndStop { record, stop, .. } = d else {
            panic!("expected AdvanceAndStop, got {d:?}");
        };
        assert_eq!(
            record.state,
            FulfillState::NeedsInput {
                reason: REASON_PLAN_APPROVAL.into(),
                payload: "plan-1".into()
            }
        );
        assert_eq!(record.plan_id.as_deref(), Some("plan-1"), "pin carried");
        assert_eq!(
            stop.next_command.as_deref(),
            Some("rocky review plan-1 --approve")
        );
    }

    #[test]
    fn proposed_marker_present_advances_to_plan_approved() {
        let d = decide(
            &proposed(),
            Event::MarkerPoll {
                reviewed: true,
                invalid: None,
                plan_payload_digest: Some("sha256:aa".into()),
                approved_digest: Some("sha256:aa".into()),
            },
            now(),
        );
        let Decision::Advance { record, .. } = d else {
            panic!("expected Advance, got {d:?}");
        };
        assert_eq!(record.state, FulfillState::PlanApproved);
    }

    #[test]
    fn proposed_invalid_marker_halts_loudly() {
        let d = decide(
            &proposed(),
            Event::MarkerPoll {
                reviewed: false,
                invalid: Some("truncated marker".into()),
                plan_payload_digest: None,
                approved_digest: None,
            },
            now(),
        );
        let Decision::Halt(stop) = d else {
            panic!("expected Halt, got {d:?}");
        };
        assert!(stop.message.contains("truncated marker"));
    }

    #[test]
    fn proposed_reapproval_supersedes_and_clears_the_pins() {
        let d = decide(
            &proposed(),
            Event::MarkerPoll {
                reviewed: false,
                invalid: None,
                plan_payload_digest: Some("sha256:OLD".into()),
                approved_digest: Some("sha256:NEW".into()),
            },
            now(),
        );
        let Decision::Advance { record, .. } = d else {
            panic!("expected Advance, got {d:?}");
        };
        assert_eq!(
            record.state,
            FulfillState::Superseded {
                old_digest: "sha256:OLD".into(),
                new_digest: "sha256:NEW".into()
            }
        );
        assert!(record.plan_id.is_none());
        assert!(record.idempotency_key.is_none());
    }

    #[test]
    fn needs_plan_approval_reruns_the_same_marker_rule_but_halts_in_place() {
        let mut waiting = rec(FulfillState::NeedsInput {
            reason: REASON_PLAN_APPROVAL.into(),
            payload: "plan-1".into(),
        });
        waiting.plan_id = Some("plan-1".into());
        // Still absent: halt, no rewrite of the identical ask.
        let d = decide(
            &waiting,
            Event::MarkerPoll {
                reviewed: false,
                invalid: None,
                plan_payload_digest: Some("sha256:aa".into()),
                approved_digest: Some("sha256:aa".into()),
            },
            now(),
        );
        assert!(matches!(d, Decision::Halt(_)), "got {d:?}");
        // Approved: the same rule advances.
        let d = decide(
            &waiting,
            Event::MarkerPoll {
                reviewed: true,
                invalid: None,
                plan_payload_digest: Some("sha256:aa".into()),
                approved_digest: Some("sha256:aa".into()),
            },
            now(),
        );
        let Decision::Advance { record, .. } = d else {
            panic!("expected Advance, got {d:?}");
        };
        assert_eq!(record.state, FulfillState::PlanApproved);
        // Superseded while waiting: same rule as `proposed`.
        let d = decide(
            &waiting,
            Event::MarkerPoll {
                reviewed: false,
                invalid: None,
                plan_payload_digest: Some("sha256:OLD".into()),
                approved_digest: Some("sha256:NEW".into()),
            },
            now(),
        );
        let Decision::Advance { record, .. } = d else {
            panic!("expected Advance, got {d:?}");
        };
        assert!(matches!(record.state, FulfillState::Superseded { .. }));
    }

    // =====================================================================
    // plan_approved → applying (pins pinned, pre-apply task carried)
    // =====================================================================

    #[test]
    fn plan_approved_journals_applying_and_carries_the_pre_apply_task() {
        let mut prior = rec(FulfillState::PlanApproved);
        prior.plan_id = Some("plan-1".into());
        prior.idempotency_key = Some("k@1".into());
        let d = decide(&prior, Event::Reentry, now());
        let Decision::AdvanceAndAct {
            record,
            task,
            event,
        } = d
        else {
            panic!("expected AdvanceAndAct, got {d:?}");
        };
        assert_eq!(record.state, FulfillState::Applying);
        assert_eq!(record.plan_id.as_deref(), Some("plan-1"));
        assert_eq!(record.idempotency_key.as_deref(), Some("k@1"));
        assert_eq!(task, TaskKind::PreApplyCheck);
        assert!(event.contains("plan-1") && event.contains("k@1"));
    }

    // =====================================================================
    // applying | pre-apply recompute ok → apply; mismatch → superseded;
    // crash → next run applying_unknown; skipped_in_flight KEEPS the
    // state; skipped_idempotent resolves via the receipt
    // =====================================================================

    fn applying() -> FulfillStateRecord {
        let mut record = rec(FulfillState::Applying);
        record.plan_id = Some("plan-1".into());
        record.idempotency_key = Some("k@1".into());
        record
    }

    #[test]
    fn applying_pre_apply_match_applies() {
        let d = decide(
            &applying(),
            Event::PreApply {
                recomputed_digest: Some("sha256:aa".into()),
                plan_payload_digest: Some("sha256:aa".into()),
                snapshot_ok: true,
            },
            now(),
        );
        assert_eq!(d, Decision::Act(TaskKind::Apply));
    }

    #[test]
    fn applying_pre_apply_snapshot_tamper_blocks() {
        let d = decide(
            &applying(),
            Event::PreApply {
                recomputed_digest: Some("sha256:zz".into()),
                plan_payload_digest: Some("sha256:aa".into()),
                snapshot_ok: false,
            },
            now(),
        );
        let Decision::AdvanceAndStop { record, .. } = d else {
            panic!("expected AdvanceAndStop, got {d:?}");
        };
        let FulfillState::Blocked { reason } = &record.state else {
            panic!("expected Blocked");
        };
        assert!(reason.contains("tampered"));
    }

    #[test]
    fn applying_pre_apply_digest_mismatch_supersedes() {
        let d = decide(
            &applying(),
            Event::PreApply {
                recomputed_digest: Some("sha256:NEW".into()),
                plan_payload_digest: Some("sha256:OLD".into()),
                snapshot_ok: true,
            },
            now(),
        );
        let Decision::Advance { record, .. } = d else {
            panic!("expected Advance, got {d:?}");
        };
        assert_eq!(
            record.state,
            FulfillState::Superseded {
                old_digest: "sha256:OLD".into(),
                new_digest: "sha256:NEW".into()
            }
        );
        assert!(record.plan_id.is_none());
    }

    #[test]
    fn applying_success_journals_applied() {
        let d = decide(
            &applying(),
            Event::ApplyFinished(ApplySummary::Applied {
                run_id: Some("run-1".into()),
            }),
            now(),
        );
        let Decision::Advance { record, event } = d else {
            panic!("expected Advance, got {d:?}");
        };
        assert_eq!(record.state, FulfillState::Applied);
        assert!(event.contains("run-1"));
    }

    #[test]
    fn applying_skipped_idempotent_resolves_via_the_receipt_never_assumed() {
        let d = decide(
            &applying(),
            Event::ApplyFinished(ApplySummary::SkippedIdempotent {
                prior_run_id: "run-0".into(),
            }),
            now(),
        );
        assert_eq!(d, Decision::Act(TaskKind::LookupReceipt));
    }

    #[test]
    fn applying_skipped_in_flight_keeps_the_state() {
        let d = decide(
            &applying(),
            Event::ApplyFinished(ApplySummary::SkippedInFlight {
                prior_run_id: "run-9".into(),
            }),
            now(),
        );
        let Decision::Halt(stop) = d else {
            panic!("expected Halt (no state change), got {d:?}");
        };
        assert!(stop.message.contains("run-9"));
    }

    #[test]
    fn applying_typed_failure_blocks() {
        let d = decide(
            &applying(),
            Event::ApplyFinished(ApplySummary::Failed {
                error: "policy denied".into(),
            }),
            now(),
        );
        let Decision::AdvanceAndStop { record, .. } = d else {
            panic!("expected AdvanceAndStop, got {d:?}");
        };
        assert!(matches!(record.state, FulfillState::Blocked { .. }));
    }

    #[test]
    fn applying_cold_resume_enters_applying_unknown() {
        let d = decide(&applying(), Event::Reentry, now());
        let Decision::Advance { record, .. } = d else {
            panic!("expected Advance, got {d:?}");
        };
        assert_eq!(record.state, FulfillState::ApplyingUnknown);
    }

    #[test]
    fn applying_receipt_success_after_skip_is_applied() {
        let d = decide(
            &applying(),
            Event::ReceiptResolved(ReceiptSummary::Succeeded {
                run_id: "run-0".into(),
            }),
            now(),
        );
        let Decision::Advance { record, .. } = d else {
            panic!("expected Advance, got {d:?}");
        };
        assert_eq!(record.state, FulfillState::Applied);
    }

    #[test]
    fn applying_receipt_contradiction_parks_for_a_human() {
        // skipped_idempotent claimed a success exists; the authoritative
        // lookup says otherwise — park, never blind-retry.
        for lookup in [
            ReceiptSummary::NoRecord,
            ReceiptSummary::Failed {
                run_id: "run-0".into(),
            },
        ] {
            let d = decide(&applying(), Event::ReceiptResolved(lookup), now());
            let Decision::AdvanceAndStop { record, stop, .. } = d else {
                panic!("expected AdvanceAndStop, got {d:?}");
            };
            assert_eq!(record.state, FulfillState::ApplyingUnknown);
            assert!(stop.next_command.is_none(), "a human resolves");
        }
    }

    #[test]
    fn applying_receipt_in_flight_moves_to_applying_unknown_and_stops() {
        let d = decide(
            &applying(),
            Event::ReceiptResolved(ReceiptSummary::InFlight {
                run_id: "run-9".into(),
            }),
            now(),
        );
        let Decision::AdvanceAndStop { record, .. } = d else {
            panic!("expected AdvanceAndStop, got {d:?}");
        };
        assert_eq!(record.state, FulfillState::ApplyingUnknown);
    }

    // =====================================================================
    // applying_unknown | receipt → applied; none → retry (dedup-safe);
    // cannot answer → stays for a human — BOTH arms
    // =====================================================================

    fn applying_unknown() -> FulfillStateRecord {
        let mut record = rec(FulfillState::ApplyingUnknown);
        record.plan_id = Some("plan-1".into());
        record.idempotency_key = Some("k@1".into());
        record
    }

    #[test]
    fn applying_unknown_looks_up_the_receipt_on_entry() {
        let d = decide(&applying_unknown(), Event::Reentry, now());
        assert_eq!(d, Decision::Act(TaskKind::LookupReceipt));
    }

    #[test]
    fn applying_unknown_receipt_found_resolves_to_applied() {
        let d = decide(
            &applying_unknown(),
            Event::ReceiptResolved(ReceiptSummary::Succeeded {
                run_id: "run-1".into(),
            }),
            now(),
        );
        let Decision::Advance { record, .. } = d else {
            panic!("expected Advance, got {d:?}");
        };
        assert_eq!(record.state, FulfillState::Applied);
    }

    #[test]
    fn applying_unknown_no_receipt_retries_dedup_safe() {
        for lookup in [
            ReceiptSummary::NoRecord,
            ReceiptSummary::Failed {
                run_id: "run-0".into(),
            },
        ] {
            let d = decide(&applying_unknown(), Event::ReceiptResolved(lookup), now());
            let Decision::AdvanceAndAct { record, task, .. } = d else {
                panic!("expected AdvanceAndAct, got {d:?}");
            };
            assert_eq!(record.state, FulfillState::Applying);
            assert_eq!(
                task,
                TaskKind::PreApplyCheck,
                "retry re-runs the digest gate"
            );
            assert_eq!(record.idempotency_key.as_deref(), Some("k@1"), "same key");
        }
    }

    #[test]
    fn applying_unknown_in_flight_stays_put() {
        let d = decide(
            &applying_unknown(),
            Event::ReceiptResolved(ReceiptSummary::InFlight {
                run_id: "run-9".into(),
            }),
            now(),
        );
        assert!(matches!(d, Decision::Halt(_)), "got {d:?}");
    }

    #[test]
    fn applying_unknown_cannot_answer_stays_for_a_human() {
        let d = decide(
            &applying_unknown(),
            Event::ReceiptResolved(ReceiptSummary::CannotAnswer {
                reason: "s3 backend has no non-mutating read".into(),
            }),
            now(),
        );
        let Decision::Halt(stop) = d else {
            panic!("expected Halt, got {d:?}");
        };
        assert!(stop.message.contains("s3 backend"));
        assert!(stop.next_command.is_none());
    }

    // =====================================================================
    // applied → observation → observing; observing re-observes + journals
    // =====================================================================

    #[test]
    fn applied_journals_the_reading_then_reads_the_declared_checks() {
        let d = decide(&rec(FulfillState::Applied), Event::Reentry, now());
        assert_eq!(d, Decision::Act(TaskKind::Observe));
        let d = decide(
            &rec(FulfillState::Applied),
            Event::ObservationDone {
                test_green: true,
                staleness_ok: Some(true),
                detail: "lag 60s, budget 86400s".into(),
            },
            now(),
        );
        let Decision::AdvanceAndAct {
            record,
            event,
            task,
        } = d
        else {
            panic!("expected AdvanceAndAct, got {d:?}");
        };
        // The staleness/test reading is journaled where it happened, but
        // NO health is claimed yet: `observing` is not reachable until
        // the declared checks have been read.
        assert_eq!(record.state, FulfillState::Applied, "no state claimed yet");
        assert_eq!(
            task,
            TaskKind::ObserveChecks {
                prior_detail: "lag 60s, budget 86400s".to_string()
            },
            "the reading is carried forward, so the final stop reports the whole \
             observation and not only its second half"
        );
        assert!(event.contains("staleness fresh"));
    }

    #[test]
    fn observing_journals_every_observation_including_stale_ones() {
        let d = decide(&rec(FulfillState::Observing), Event::Reentry, now());
        assert_eq!(d, Decision::Act(TaskKind::Observe));
        let d = decide(
            &rec(FulfillState::Observing),
            Event::ObservationDone {
                test_green: false,
                staleness_ok: Some(false),
                detail: "lag 200000s, budget 86400s".into(),
            },
            now(),
        );
        let Decision::AdvanceAndAct {
            record,
            event,
            task,
        } = d
        else {
            panic!("expected AdvanceAndAct, got {d:?}");
        };
        assert_eq!(record.state, FulfillState::Observing, "stays observing");
        assert_eq!(
            task,
            TaskKind::ObserveChecks {
                prior_detail: "lag 200000s, budget 86400s".to_string()
            }
        );
        assert!(event.contains("tests RED"));
        assert!(event.contains("staleness STALE"));
    }

    // =====================================================================
    // F3 — the declared data checks, read against the APPLIED output
    // =====================================================================

    /// A rendered reading in the shape `step::render_check_findings`
    /// actually produces — the "N of M" prefix included.
    ///
    /// Fixtures that omit the prefix silently weaken every message
    /// assertion made against them: the assertion still passes or fails,
    /// but about a string production never emits. Shared as a constant so
    /// the exact-match assertions and the message assertions cannot drift
    /// apart.
    const CHECK_DETAIL: &str = "3 of 4 declared data checks passed; \
         revenue_daily.client_id [unique] fail (error): 4 duplicate value(s) found";

    /// The reading, with everything green unless a field says otherwise.
    fn checks(failed: usize, errored: usize, warned: usize, deferred: Option<usize>) -> Event {
        Event::ObservationChecks {
            failed,
            errored,
            warned,
            deferred,
            detail: CHECK_DETAIL.into(),
            prior_detail: "MAX(loaded_at) = t, lag 60s, budget 86400s".into(),
            cause: (errored > 0 || deferred != Some(0)).then_some(UnevaluableCause::Unreadable),
        }
    }

    /// THE REGRESSION PIN. A clean reading still lands `observing`, from
    /// both entry states, and it is the only verdict that does.
    #[test]
    fn a_clean_reading_is_the_only_path_to_observing() {
        for from in [FulfillState::Applied, FulfillState::Observing] {
            let mut prior = rec(from.clone());
            // A product that spent budget on an earlier red arrives here
            // carrying it; a closed cycle refills the ceiling.
            prior.data_repair_rounds = 2;
            prior.observation_detail = Some("stale evidence from the last red".into());
            let d = decide(&prior, checks(0, 0, 0, Some(0)), now());
            let Decision::AdvanceAndStop { record, event, .. } = d else {
                panic!("expected AdvanceAndStop from {from:?}, got {d:?}");
            };
            assert_eq!(record.state, FulfillState::Observing, "from {from:?}");
            assert_eq!(record.data_repair_rounds, 0, "the cycle closed");
            assert_eq!(
                record.observation_detail, None,
                "evidence from a red that is no longer true must not survive it"
            );
            assert!(event.contains("green"), "{event}");
        }
    }

    /// Freshness and warning-severity checks REPORT; they never route.
    ///
    /// Staleness is a scheduling fact far more often than a model defect,
    /// and a `severity = "warning"` check is by definition one the
    /// product did not declare disqualifying. Rewriting SQL on either
    /// would spend a live-table cycle on something the rewrite cannot
    /// fix.
    #[test]
    fn warnings_and_staleness_report_but_never_route_to_repair() {
        // Stale, and its model tests red — the pre-F3 signals — with
        // every declared check passing.
        let d = decide(
            &rec(FulfillState::Applied),
            Event::ObservationDone {
                test_green: false,
                staleness_ok: Some(false),
                detail: "lag 200000s, budget 86400s".into(),
            },
            now(),
        );
        assert!(
            matches!(&d, Decision::AdvanceAndAct { task, .. }
                if matches!(task, TaskKind::ObserveChecks { .. })),
            "staleness routes nowhere on its own: {d:?}"
        );
        let d = decide(&rec(FulfillState::Applied), checks(0, 0, 7, Some(0)), now());
        let Decision::AdvanceAndStop { record, event, .. } = d else {
            panic!("expected AdvanceAndStop, got {d:?}");
        };
        assert_eq!(
            record.state,
            FulfillState::Observing,
            "seven warnings are seven reports, not a defect"
        );
        assert!(
            event.contains("7 warned"),
            "the count is journaled: {event}"
        );
    }

    /// A first red is RECORDED, in a state that says what is wrong, and
    /// the loop stops there. It does not repair on one reading.
    #[test]
    fn a_first_data_red_lands_the_visibly_distinct_state_and_stops() {
        let d = decide(&rec(FulfillState::Applied), checks(1, 0, 0, Some(0)), now());
        let Decision::AdvanceAndStop {
            record,
            event,
            stop,
        } = d
        else {
            panic!("expected AdvanceAndStop, got {d:?}");
        };
        assert_eq!(record.state, FulfillState::ObservedFailing);
        assert_ne!(
            record.state,
            FulfillState::Observing,
            "a failing product must never be recorded as a healthy one"
        );
        assert_eq!(
            record.data_repair_rounds, 0,
            "recording a red spends nothing; only a confirmed one does"
        );
        assert_eq!(
            record.observation_detail.as_deref(),
            Some(CHECK_DETAIL),
            "the evidence is persisted for the worker that must act on it"
        );
        assert!(event.contains("FAILING"), "{event}");
        // The human-facing message names the check and the actual value,
        // and promises the review gate rather than a silent fix.
        assert!(
            stop.message
                .contains("failing its own declared data checks")
        );
        assert!(stop.message.contains("4 duplicate value(s) found"));
        assert!(stop.message.contains("review"));
    }

    /// Resume honesty: entering the data-red state re-READS the checks.
    /// Nothing loads the stored verdict.
    #[test]
    fn the_data_red_state_re_reads_the_checks_rather_than_assuming_the_verdict() {
        let mut prior = rec(FulfillState::ObservedFailing);
        prior.observation_detail = Some("the verdict from before the crash".into());
        assert_eq!(
            decide(&prior, Event::Reentry, now()),
            Decision::Act(TaskKind::Observe),
            "a crashed data-red resumes into a reading, never into its own last answer"
        );
        // And a re-read that comes back CLEAN releases it — a red is not
        // a trap, it is the current answer.
        let d = decide(&prior, checks(0, 0, 0, Some(0)), now());
        let Decision::AdvanceAndStop { record, .. } = d else {
            panic!("expected AdvanceAndStop, got {d:?}");
        };
        assert_eq!(record.state, FulfillState::Observing);
        assert_eq!(record.observation_detail, None);
    }

    /// A CONFIRMED red — a second, independent reading — spends exactly
    /// one round and dispatches the data-repair brief.
    #[test]
    fn a_confirmed_data_red_spends_one_round_and_dispatches_the_data_repair_task() {
        let mut prior = rec(FulfillState::ObservedFailing);
        prior.data_repair_rounds = 1;
        prior.drafting_attempts = 5;
        prior.observation_detail = Some("the FIRST reading's evidence".into());
        let d = decide(&prior, checks(2, 0, 0, Some(0)), now());
        let Decision::AdvanceAndAct {
            record,
            event,
            task,
        } = d
        else {
            panic!("expected AdvanceAndAct, got {d:?}");
        };
        assert_eq!(record.state, FulfillState::Drafting);
        assert_eq!(task, TaskKind::DataRepair);
        assert_eq!(record.data_repair_rounds, 2, "exactly one round spent");
        assert_eq!(record.repair_rounds, 0, "the verify budget is untouched");
        assert_eq!(record.drafting_attempts, 1, "a fresh compile-loop budget");
        assert_eq!(
            record.drafting_round,
            DraftingRound::DataRepair,
            "persisted WITH the deciding transition, so a crash before the \
             worker starts resumes into the same round"
        );
        assert_eq!(
            record.observation_detail.as_deref(),
            Some(CHECK_DETAIL),
            "the worker acts on what is true NOW, not on the first reading"
        );
        assert!(event.contains("data repair round 2"), "{event}");
    }

    /// An errored check is "cannot tell", and cannot tell is neither
    /// health nor a licence to rewrite a model.
    #[test]
    fn an_errored_check_holds_and_never_reads_as_health() {
        let d = decide(
            &rec(FulfillState::Observing),
            checks(0, 3, 0, Some(0)),
            now(),
        );
        let Decision::AdvanceAndStop {
            record,
            event,
            stop,
        } = d
        else {
            panic!("expected AdvanceAndStop, got {d:?}");
        };
        assert_eq!(
            record.state,
            FulfillState::Applied,
            "applied, observation not concluded — never a healthy `observing`"
        );
        assert_eq!(record.data_repair_rounds, 0, "no round is spent on a guess");
        assert!(event.contains("not evaluable"), "{event}");
        assert!(stop.message.contains("could not be evaluated"));
    }

    /// A reading that cannot even count is not a reading of zero
    /// problems — the #1495 rule, on the observation side.
    #[test]
    fn an_uncountable_reading_holds_rather_than_claiming_zero() {
        for deferred in [None, Some(2)] {
            let d = decide(
                &rec(FulfillState::Applied),
                checks(0, 0, 0, deferred),
                now(),
            );
            let Decision::AdvanceAndStop { record, event, .. } = d else {
                panic!("expected AdvanceAndStop for {deferred:?}, got {d:?}");
            };
            assert_eq!(
                record.state,
                FulfillState::Applied,
                "an incomplete reading claims nothing: {deferred:?}"
            );
            assert!(event.contains("not evaluable"), "{event}");
        }
    }

    /// No new news does not clear old news.
    #[test]
    fn an_unevaluable_reading_from_the_data_red_state_stays_failing() {
        let mut prior = rec(FulfillState::ObservedFailing);
        prior.observation_detail = Some("4 duplicate value(s) found".into());
        let d = decide(&prior, checks(0, 1, 0, Some(0)), now());
        let Decision::AdvanceAndStop { record, .. } = d else {
            panic!("expected AdvanceAndStop, got {d:?}");
        };
        assert_eq!(
            record.state,
            FulfillState::ObservedFailing,
            "a product already known to be failing is not cleared by a reading \
             that could not tell"
        );
        assert_eq!(
            record.observation_detail.as_deref(),
            Some("4 duplicate value(s) found"),
            "and the last real evidence survives"
        );
    }

    /// The ceiling binds: repeated data-reds land `blocked`, naming the
    /// check. No unbounded repair cycle against a live table.
    #[test]
    fn repeated_data_reds_exhaust_the_ceiling_and_block_naming_the_check() {
        for from in [FulfillState::Applied, FulfillState::ObservedFailing] {
            let mut prior = rec(from.clone());
            prior.data_repair_rounds = MAX_REPAIR_ROUNDS;
            let d = decide(&prior, checks(1, 0, 0, Some(0)), now());
            let Decision::AdvanceAndStop {
                record,
                event,
                stop,
            } = d
            else {
                panic!("expected AdvanceAndStop from {from:?}, got {d:?}");
            };
            let FulfillState::Blocked { reason } = &record.state else {
                panic!("expected blocked from {from:?}, got {:?}", record.state);
            };
            assert!(
                reason.contains("4 duplicate value(s) found"),
                "the block names the check that would not go green: {reason}"
            );
            assert!(reason.contains(&MAX_REPAIR_ROUNDS.to_string()), "{reason}");
            assert!(event.contains("budget exhausted"), "{event}");
            assert_eq!(
                stop.next_command.as_deref(),
                Some("rocky fulfill revenue_daily --retry"),
                "a human is the escalation, not another round"
            );
        }
    }

    /// The two budgets are separate because they RESET differently, and
    /// this is the difference that makes the data ceiling bind at all.
    ///
    /// `decide_proposed` clears `repair_rounds` on every successful
    /// propose. A data-red cycle proposes every lap (red → repair →
    /// propose → apply → red), so a shared counter would be zeroed each
    /// time and the ceiling would never be reached.
    #[test]
    fn a_propose_resets_the_verify_budget_and_never_the_data_budget() {
        let mut prior = rec(FulfillState::Verifying);
        prior.repair_rounds = 2;
        prior.data_repair_rounds = 2;
        let d = decide(
            &prior,
            Event::Proposed {
                outcome: ProposeSummary::Written {
                    plan_id: "plan-2".into(),
                },
                plan_payload_digest: None,
                approved_digest: None,
                idempotency_key: "k".into(),
            },
            now(),
        );
        let Decision::Advance { record, .. } = d else {
            panic!("expected Advance, got {d:?}");
        };
        assert_eq!(record.repair_rounds, 0, "the verify cycle closed");
        assert_eq!(
            record.data_repair_rounds, 2,
            "the data cycle has NOT closed — the output is not observed yet, \
             and zeroing here is what would make the ceiling vacuous"
        );
    }

    /// A human intervention refills both budgets and drops the evidence.
    #[test]
    fn a_retry_and_a_supersession_both_clear_the_data_budget_and_evidence() {
        let mut blocked_rec = rec(FulfillState::Blocked {
            reason: "the applied output still fails its declared data checks".into(),
        });
        blocked_rec.spec_digest = Some("sha256:aa".into());
        blocked_rec.data_repair_rounds = MAX_REPAIR_ROUNDS;
        blocked_rec.observation_detail = Some("4 duplicate value(s) found".into());
        let Decision::Advance { record, .. } = decide(&blocked_rec, Event::RetryRequested, now())
        else {
            panic!("expected Advance");
        };
        assert_eq!(record.data_repair_rounds, 0);
        assert_eq!(record.observation_detail, None);

        let mut superseded = rec(FulfillState::Superseded {
            old_digest: "sha256:aa".into(),
            new_digest: "sha256:bb".into(),
        });
        superseded.data_repair_rounds = 2;
        superseded.observation_detail = Some("4 duplicate value(s) found".into());
        let Decision::Advance { record, .. } = decide(&superseded, Event::Reentry, now()) else {
            panic!("expected Advance");
        };
        assert_eq!(record.data_repair_rounds, 0);
        assert_eq!(
            record.observation_detail, None,
            "a new generation may not even declare the check the old one failed"
        );
    }

    /// One observation, one message — and the standing word is the one
    /// the reader had before F3.
    ///
    /// Splitting the observation into two readings must not cost the
    /// human half the answer: the staleness finding is journaled by the
    /// first reading and would otherwise vanish from the stop, which is
    /// the only place most people ever look. And the first pass still
    /// says "applied" while later ones say "live" — that difference is
    /// how a reader tells a fresh ship from a running product.
    #[test]
    fn one_observation_reports_as_one_message_with_the_standing_word_intact() {
        let d = decide(&rec(FulfillState::Applied), checks(0, 0, 0, Some(0)), now());
        let Decision::AdvanceAndStop { stop, .. } = d else {
            panic!("expected AdvanceAndStop, got {d:?}");
        };
        assert!(
            stop.message.contains("is applied;"),
            "the first pass to reach it says applied: {}",
            stop.message
        );
        assert!(
            stop.message.contains("lag 60s, budget 86400s"),
            "the staleness reading must survive into the stop: {}",
            stop.message
        );
        assert!(
            stop.message.contains("declared data checks"),
            "and so must the check reading: {}",
            stop.message
        );

        let d = decide(
            &rec(FulfillState::Observing),
            checks(0, 0, 0, Some(0)),
            now(),
        );
        let Decision::AdvanceAndStop { stop, .. } = d else {
            panic!("expected AdvanceAndStop, got {d:?}");
        };
        assert!(
            stop.message.contains("is live;"),
            "a product already observed says live: {}",
            stop.message
        );

        // A red stop carries the whole observation too — the human
        // deciding whether to let a repair run needs both halves.
        let d = decide(&rec(FulfillState::Applied), checks(1, 0, 0, Some(0)), now());
        let Decision::AdvanceAndStop { stop, .. } = d else {
            panic!("expected AdvanceAndStop, got {d:?}");
        };
        assert!(stop.message.contains("lag 60s"), "{}", stop.message);
        assert!(
            stop.message.contains("4 duplicate value(s) found"),
            "{}",
            stop.message
        );
    }

    /// A hold must name a command that can actually END the hold.
    ///
    /// Both causes land the same `unevaluable` verdict, but only one is
    /// resolved by running the loop again. A custody divergence re-reads
    /// the same diverged sidecar every time, so telling the operator to
    /// re-run points them at an infinite loop — the product is not
    /// stranded by its STATE, but by never being told the way out.
    ///
    /// Asserted as the SPECIFIC command per cause. A test that only
    /// checked `next_command.is_some()` passes on the broken behaviour
    /// and proves nothing.
    #[test]
    fn each_unevaluable_cause_names_the_remedy_that_resolves_it() {
        let reading = |cause: Option<UnevaluableCause>| Event::ObservationChecks {
            failed: 0,
            errored: 0,
            warned: 0,
            deferred: None,
            // "verified", not "approved" — the set this compares against
            // is the one the generation pinned at `verifying`, and no
            // human is ever shown it (see the README paragraph and
            // `WORKER_PROFILE_TOOLS`). A fixture that says "approved"
            // teaches the wrong word to the next reader of this test.
            detail: "the declared checks on disk are not the ones this generation verified".into(),
            prior_detail: String::new(),
            cause,
        };

        // Custody divergence: a RESTORE is what puts the verified checks
        // back — no verb adopts the edit — so that is what the stop must
        // say, and it must say it before naming any command.
        let d = decide(
            &rec(FulfillState::Applied),
            reading(Some(UnevaluableCause::CheckCustody)),
            now(),
        );
        let Decision::AdvanceAndStop { stop, .. } = d else {
            panic!("expected AdvanceAndStop, got {d:?}");
        };
        assert!(
            stop.message.contains("restore the file you changed"),
            "the remedy is a RESTORE, stated before the command: {}",
            stop.message
        );
        assert!(
            stop.message.contains("cannot adopt an edit here"),
            "and the operator is told WHY the loop will not take their edit: {}",
            stop.message
        );
        assert!(
            stop.message.contains("approve the spec again"),
            "with the route for keeping the change named too: {}",
            stop.message
        );

        // AND IT IS AN ORDER, not a menu. This arm lands the record at
        // `applied`, and `rocky product approve` refuses `applied` —
        // pinned against the real verb by
        // `approving_refuses_at_applied_and_permits_at_observing` in
        // rocky-cli. So "approve the spec again" is unreachable from the
        // state this message is printed in until the restore + re-run
        // has moved the product off `applied`.
        //
        // Asserted by POSITION. Substring presence passed on the broken
        // wording too — it named both steps and implied neither order.
        let message = &stop.message;
        let at = |needle: &str| {
            message
                .find(needle)
                .unwrap_or_else(|| panic!("the remedy must contain {needle:?}: {message}"))
        };
        assert!(
            at("restore the file you changed") < at("re-run until the loop leaves `applied`"),
            "the restore comes first: {message}"
        );
        assert!(
            at("re-run until the loop leaves `applied`") < at("approve the spec again"),
            "the re-run comes before the approval, because the approval is refused at \
             `applied`: {message}"
        );
        assert!(
            message.contains("`observing`"),
            "the state the operator waits for is named, not left as a guess: {message}"
        );
        assert!(
            message.contains("Approving is refused while the state is `applied`"),
            "and WHY that order is forced is stated, so it does not read as a preference: \
             {message}"
        );

        // AND THE SPEC ROUTE IS QUALIFIED. It used to say "put the
        // change in the product spec's `checks`" flat out. `checks` is a
        // list of opaque SQL boolean strings that the lowering turns
        // into `expression` tests at `severity = "error"`
        // (`spec_checks_lower_only_to_error_severity_expression_tests`
        // in rocky-core pins that), so for a changed `row_count_range`,
        // a `warning` severity, or a `filter`, the sentence named a
        // route the field cannot carry. That is the same defect as
        // naming a command that cannot run, and it must not come back.
        assert!(
            message.contains("always lowers it to an error-severity `expression` test"),
            "what `output.checks` can carry is stated, not implied: {message}"
        );
        assert!(
            message.contains("has no spec spelling"),
            "and the shapes it CANNOT carry are called out, so the spec route is never \
             offered for a change it would silently distort: {message}"
        );
        assert!(
            at("has no spec spelling") < at("the restore is the whole remedy"),
            "with the honest fallback stated right after them: {message}"
        );
        for shape in ["row_count_range", "`warning` severity", "`filter`"] {
            assert!(
                message.contains(shape),
                "each unrepresentable shape is named — {shape} is missing: {message}"
            );
        }

        // AND THE QUALIFICATION IS ITSELF QUALIFIED — the same defect
        // with the sign flipped. Fixing the over-claim produced an
        // under-claim: the sentence listed `unique` flat among the
        // shapes with no spec spelling, when `output.grain` lowers
        // DIRECTLY to a single-column `unique` or a `composite` +
        // `kind = "unique"` (`product::lowering::generated_tests`,
        // pinned in both arms in rocky-core). So it is checked in BOTH
        // directions here: the grain's uniqueness is stated as
        // spellable, and the uniqueness that is not spellable is
        // qualified rather than the whole shape.
        assert!(
            message.contains("`output.grain` lowers to exactly one uniqueness check"),
            "the declared grain IS a uniqueness check the spec can spell, and the message \
             says so instead of listing `unique` as unreachable: {message}"
        );
        for spelled in [
            "`unique` on a single grain column",
            "`composite` with `kind = \"unique\"`",
        ] {
            assert!(
                message.contains(spelled),
                "both grain arms are named — {spelled} is missing: {message}"
            );
        }
        assert!(
            message.contains("a uniqueness check that is not the declared grain"),
            "and the uniqueness that genuinely has no spelling is the NON-grain one, stated \
             as such rather than as `unique` bare: {message}"
        );
        assert!(
            at("`output.grain` lowers to exactly one uniqueness check")
                < at("has no spec spelling"),
            "what the spec CAN hold is stated before what it cannot, so the operator reads \
             the route before the refusal: {message}"
        );

        // A transient read failure: re-running genuinely can resolve it.
        for cause in [Some(UnevaluableCause::Unreadable), None] {
            let d = decide(&rec(FulfillState::Applied), reading(cause.clone()), now());
            let Decision::AdvanceAndStop { stop, .. } = d else {
                panic!("expected AdvanceAndStop, got {d:?}");
            };
            assert_eq!(
                stop.next_command.as_deref(),
                Some("rocky fulfill revenue_daily"),
                "the warehouse may answer next time, so re-running IS the remedy ({cause:?})"
            );
            assert!(
                !stop.message.contains("restore the file you changed"),
                "and it must not tell them to undo an edit they did not make: {}",
                stop.message
            );
        }

        // AND THE SCHEME CAUSE GETS A DIFFERENT LANDING, not just
        // different words. `applied` is the holding pattern for
        // conditions a later run can resolve, and it is in `rocky
        // product approve`'s in-flight refusal set — so a cause that
        // re-running can never resolve must not land there, or the
        // operator has no exit at all. It goes to `blocked`, whose
        // `--retry` re-enters at `spec_approved` and pins a fresh
        // digest at the new generation's own verify.
        for state in [FulfillState::Applied, FulfillState::ObservedFailing] {
            let d = decide(
                &rec(state.clone()),
                reading(Some(UnevaluableCause::CheckSchemeChanged)),
                now(),
            );
            let Decision::AdvanceAndStop { record, stop, .. } = d else {
                panic!("expected AdvanceAndStop, got {d:?}");
            };
            assert!(
                matches!(record.state, FulfillState::Blocked { .. }),
                "a scheme mismatch lands where a human can act, from {state:?}: {:?}",
                record.state
            );
            assert_eq!(
                stop.next_command.as_deref(),
                Some("rocky fulfill revenue_daily --retry"),
                "and the printed command starts the generation that re-pins: {stop:?}"
            );
            assert!(
                !stop.message.contains("restore the file you changed"),
                "no restore alters a hash algorithm, so the custody remedy must not leak \
                 into this arm: {}",
                stop.message
            );
            assert!(
                !stop.message.contains("put the change in the product spec"),
                "and no spec field carries a digest scheme: {}",
                stop.message
            );
        }
    }

    /// The verdict ORDER is the decision. Positive evidence of failure
    /// outranks an incomplete reading; an incomplete reading outranks a
    /// clean tally.
    #[test]
    fn the_check_verdict_ranks_evidence_above_an_incomplete_reading() {
        // Failure wins even when the rest of the reading is unusable —
        // one proven failure is enough to act on.
        assert_eq!(
            classify_checks(1, 9, None),
            CheckVerdict::Failing,
            "a proven failure is not withheld because other checks errored"
        );
        // Incomplete outranks clean: the tally alone looks identical.
        assert_eq!(classify_checks(0, 1, Some(0)), CheckVerdict::Unevaluable);
        assert_eq!(classify_checks(0, 0, None), CheckVerdict::Unevaluable);
        assert_eq!(classify_checks(0, 0, Some(1)), CheckVerdict::Unevaluable);
        // Clean is the narrow case: everything declared ran, none failed.
        assert_eq!(classify_checks(0, 0, Some(0)), CheckVerdict::Clean);
    }

    /// The evidence is warehouse-shaped, so it is capped — and the cap
    /// is announced, never silent.
    #[test]
    fn the_evidence_is_capped_on_a_char_boundary_and_says_so() {
        let short = "revenue_daily.client_id [unique]: 4 duplicate value(s)";
        assert_eq!(truncate_detail(short), short, "short evidence is untouched");

        // Multi-byte characters straddling the cut: slicing at the raw
        // byte index would panic.
        let long = "é".repeat(MAX_OBSERVATION_DETAIL);
        let capped = truncate_detail(&long);
        assert!(capped.contains("evidence truncated"), "the loss is named");
        assert!(
            capped.starts_with("éé"),
            "the head of the evidence survives: {}",
            &capped[..8]
        );
        assert!(capped.len() < long.len());

        // And the cap is what the record actually stores.
        let mut prior = rec(FulfillState::Applied);
        prior.data_repair_rounds = 0;
        let d = decide(
            &prior,
            Event::ObservationChecks {
                failed: 1,
                errored: 0,
                warned: 0,
                deferred: Some(0),
                detail: "x".repeat(MAX_OBSERVATION_DETAIL + 500),
                prior_detail: String::new(),
                cause: None,
            },
            now(),
        );
        let Decision::AdvanceAndStop { record, .. } = d else {
            panic!("expected AdvanceAndStop, got {d:?}");
        };
        let stored = record.observation_detail.expect("evidence recorded");
        assert!(
            stored.len() < MAX_OBSERVATION_DETAIL + 100,
            "the record does not grow without bound: {} bytes",
            stored.len()
        );
    }

    // =====================================================================
    // superseded → re-enters at spec_approved with the new snapshot
    // =====================================================================

    #[test]
    fn superseded_reenters_at_spec_approved_with_the_new_digest() {
        let mut prior = rec(FulfillState::Superseded {
            old_digest: "sha256:OLD".into(),
            new_digest: "sha256:NEW".into(),
        });
        prior.plan_id = Some("plan-1".into());
        prior.drafting_attempts = 7;
        prior.repair_rounds = 2;
        let d = decide(&prior, Event::Reentry, now());
        let Decision::Advance { record, .. } = d else {
            panic!("expected Advance, got {d:?}");
        };
        assert_eq!(record.state, FulfillState::SpecApproved);
        assert_eq!(record.spec_digest.as_deref(), Some("sha256:NEW"));
        assert!(record.plan_id.is_none());
        assert_eq!(record.drafting_attempts, 0);
        assert_eq!(record.repair_rounds, 0);
    }

    // =====================================================================
    // blocked | manual --retry after the printed remedy
    // =====================================================================

    #[test]
    fn blocked_without_retry_reprints_the_remedy() {
        let d = decide(
            &rec(FulfillState::Blocked {
                reason: "phase A rejected: bad type".into(),
            }),
            Event::Reentry,
            now(),
        );
        let Decision::Halt(stop) = d else {
            panic!("expected Halt, got {d:?}");
        };
        assert!(stop.message.contains("bad type"));
        assert_eq!(
            stop.next_command.as_deref(),
            Some("rocky fulfill revenue_daily --retry")
        );
    }

    #[test]
    fn blocked_retry_reenters_at_spec_approved_when_a_digest_exists() {
        let mut prior = rec(FulfillState::Blocked { reason: "x".into() });
        prior.spec_digest = Some("sha256:aa".into());
        prior.plan_id = Some("plan-1".into());
        prior.drafting_attempts = 8;
        let d = decide(&prior, Event::RetryRequested, now());
        let Decision::Advance { record, .. } = d else {
            panic!("expected Advance, got {d:?}");
        };
        assert_eq!(record.state, FulfillState::SpecApproved);
        assert!(record.plan_id.is_none());
        assert_eq!(record.drafting_attempts, 0);
    }

    #[test]
    fn blocked_retry_without_a_digest_reenters_at_init() {
        let d = decide(
            &rec(FulfillState::Blocked { reason: "x".into() }),
            Event::RetryRequested,
            now(),
        );
        let Decision::Advance { record, .. } = d else {
            panic!("expected Advance, got {d:?}");
        };
        assert_eq!(record.state, FulfillState::Init);
    }

    // =====================================================================
    // an impossible (state, event) pair REJECTS — it never proceeds
    // =====================================================================

    #[test]
    fn an_impossible_event_halts_with_a_diagnostic() {
        let d = decide(
            &rec(FulfillState::Observing),
            Event::PreApply {
                recomputed_digest: None,
                plan_payload_digest: None,
                snapshot_ok: true,
            },
            now(),
        );
        let Decision::Halt(stop) = d else {
            panic!("expected Halt, got {d:?}");
        };
        assert!(stop.message.contains("internal error"));
        assert!(stop.message.contains("observing"));
    }
}
