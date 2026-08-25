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
    /// `blocked` re-entry with `--retry`.
    RetryRequested,
    /// A plain re-entry with nothing new observed (resume dispatch).
    Reentry,
}

// ---------------------------------------------------------------------------
// The decisions
// ---------------------------------------------------------------------------

/// A task the runner performs next; its outcome is the next [`Event`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
            Event::VerifyBundle {
                compile_green: true,
                test_green: true,
                posture_green: true,
                manifest_total: true,
                detail,
                ..
            } => Decision::AdvanceAndAct {
                record: to_state(observed, FulfillState::Verifying, now),
                event: verify_green_event(&detail),
                task: TaskKind::Propose,
            },
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
        // applied → observation checks → observing
        // ------------------------------------------------------------------
        FulfillState::Applied => match event {
            Event::Reentry => Decision::Act(TaskKind::Observe),
            Event::ObservationDone {
                test_green,
                staleness_ok,
                detail,
            } => {
                let next = to_state(observed, FulfillState::Observing, now);
                Decision::AdvanceAndStop {
                    record: next,
                    event: observation_event(test_green, staleness_ok, &detail),
                    stop: Stop {
                        message: format!("product {product} is applied; {detail}"),
                        next_command: None,
                    },
                }
            }
            other => internal_mismatch(observed, &other),
        },

        // ------------------------------------------------------------------
        // observing | staleness/test findings journaled
        // ------------------------------------------------------------------
        FulfillState::Observing => match event {
            Event::Reentry => Decision::Act(TaskKind::Observe),
            Event::ObservationDone {
                test_green,
                staleness_ok,
                detail,
            } => {
                let next = to_state(observed, FulfillState::Observing, now);
                Decision::AdvanceAndStop {
                    record: next,
                    event: observation_event(test_green, staleness_ok, &detail),
                    stop: Stop {
                        message: format!("product {product} is live; {detail}"),
                        next_command: None,
                    },
                }
            }
            other => internal_mismatch(observed, &other),
        },

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
    }
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
    fn applied_observes_then_settles_into_observing() {
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
        let Decision::AdvanceAndStop { record, event, .. } = d else {
            panic!("expected AdvanceAndStop, got {d:?}");
        };
        assert_eq!(record.state, FulfillState::Observing);
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
        let Decision::AdvanceAndStop { record, event, .. } = d else {
            panic!("expected AdvanceAndStop, got {d:?}");
        };
        assert_eq!(record.state, FulfillState::Observing, "stays observing");
        assert!(event.contains("tests RED"));
        assert!(event.contains("staleness STALE"));
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
