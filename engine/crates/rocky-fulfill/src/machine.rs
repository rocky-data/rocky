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
use rocky_core::fulfill::{FulfillState, FulfillStateRecord};

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
pub fn decide_ownership(
    observed: Option<&FulfillStateRecord>,
    liveness: Option<OwnerLiveness>,
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
        /// `test_output` had no failures.
        test_green: bool,
        /// `product verify` still passes (policy-check agreement).
        posture_green: bool,
        /// The committed manifest is total and byte-clean.
        manifest_total: bool,
        /// Rendered detail for the red path.
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
fn to_state(observed: &FulfillStateRecord, state: FulfillState, now: DateTime<Utc>) -> FulfillStateRecord {
    let mut next = observed.clone();
    next.state = state;
    next.updated_at = Some(stamp(now));
    next
}

/// The blocked record for `reason`, pins cleared.
fn blocked(observed: &FulfillStateRecord, reason: String, now: DateTime<Utc>) -> FulfillStateRecord {
    let mut next = to_state(observed, FulfillState::Blocked { reason }, now);
    next.plan_id = None;
    next.idempotency_key = None;
    next
}

fn blocked_stop(record: FulfillStateRecord, event: String, product: &str, reason: &str) -> Decision {
    Decision::AdvanceAndStop {
        record,
        event,
        stop: Stop {
            message: format!("blocked: {reason}"),
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
            } => {
                if observed.drafting_attempts >= MAX_COMPILE_ITERS {
                    let record = blocked(
                        observed,
                        format!(
                            "elicitation failed {MAX_COMPILE_ITERS} times (max_compile_iters)"
                        ),
                        now,
                    );
                    blocked_stop(record, "elicitation budget exhausted".to_string(), &product,
                        "elicitation attempts exhausted")
                } else {
                    // Count the attempt BEFORE dispatch (a crash mid-task
                    // still consumed budget — the claim.rs discipline).
                    let mut next = to_state(observed, FulfillState::Init, now);
                    next.drafting_attempts = observed.drafting_attempts + 1;
                    Decision::AdvanceAndAct {
                        record: next,
                        event: format!(
                            "elicitation attempt {}",
                            observed.drafting_attempts + 1
                        ),
                        task: TaskKind::Elicit,
                    }
                }
            }
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
                let error = error.unwrap_or_else(|| "elicitation produced no candidate".to_string());
                if observed.drafting_attempts >= MAX_COMPILE_ITERS {
                    let record = blocked(
                        observed,
                        format!("elicitation failed: {error} (attempts exhausted)"),
                        now,
                    );
                    blocked_stop(record, "elicitation failed".to_string(), &product, &error)
                } else {
                    Decision::Act(TaskKind::Elicit)
                }
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
            (REASON_SPEC_APPROVAL, Event::ApprovalSurface {
                candidate_digest,
                approved_digest,
            }) => match (candidate_digest, approved_digest) {
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
            (REASON_POLICY, Event::PostureVerified(PostureStatus::NeedsInput { paste_block, reason })) => {
                Decision::Halt(Stop {
                    message: format!(
                        "policy posture still needs a human edit: {reason}\n\
                         paste this into rocky.toml, then re-run:\n\n{paste_block}"
                    ),
                    next_command: Some(format!("rocky fulfill {product}")),
                })
            }
            (REASON_POLICY, Event::PostureVerified(PostureStatus::Fail { reason })) => {
                let record = blocked(observed, reason.clone(), now);
                blocked_stop(record, "posture verification failed".to_string(), &product, &reason)
            }
            // plan_approval: rerun re-checks the marker; a re-approved
            // snapshot meanwhile supersedes.
            (REASON_PLAN_APPROVAL, Event::MarkerPoll {
                reviewed,
                invalid,
                plan_payload_digest,
                approved_digest,
            }) => decide_marker(
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
            Event::SnapshotVerify { snapshot_ok: false, detail } => {
                let record = blocked(observed, format!("tampered: {detail}"), now);
                blocked_stop(record, "snapshot tamper detected".to_string(), &product, &detail)
            }
            Event::SnapshotVerify { snapshot_ok: true, .. } => {
                Decision::Act(TaskKind::VerifyPosture)
            }
            Event::PostureVerified(PostureStatus::Pass) => Decision::Act(TaskKind::RunPhaseA),
            Event::PostureVerified(PostureStatus::NeedsInput { paste_block, reason }) => {
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
            Event::Reentry => dispatch_drafting(observed, &product, TaskKind::Draft, now),
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
                    blocked_stop(record, "drafting budget exhausted".to_string(), &product, &error)
                } else {
                    dispatch_drafting(observed, &product, TaskKind::Draft, now)
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
                blocked_stop(record, "phase A tamper detected".to_string(), &product, &detail)
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
                    dispatch_drafting(observed, &product, TaskKind::Draft, now)
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
        //   red ≤ repair rounds → drafting; else blocked
        // ------------------------------------------------------------------
        FulfillState::Verifying => match event {
            Event::Reentry => Decision::Act(TaskKind::VerifyBundle),
            Event::VerifyBundle {
                compile_green: true,
                test_green: true,
                posture_green: true,
                manifest_total: true,
                ..
            } => Decision::Act(TaskKind::Propose),
            Event::VerifyBundle { detail, .. } => {
                if observed.repair_rounds >= MAX_REPAIR_ROUNDS {
                    let record = blocked(
                        observed,
                        format!("verification red after {MAX_REPAIR_ROUNDS} repair rounds: {detail}"),
                        now,
                    );
                    blocked_stop(record, "repair budget exhausted".to_string(), &product, &detail)
                } else {
                    let mut next = to_state(observed, FulfillState::Drafting, now);
                    next.repair_rounds = observed.repair_rounds + 1;
                    next.drafting_attempts = 1;
                    Decision::AdvanceAndAct {
                        record: next,
                        event: format!(
                            "repair round {} ({detail})",
                            observed.repair_rounds + 1
                        ),
                        task: TaskKind::Repair,
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
            Event::PreApply { snapshot_ok: false, .. } => {
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
            Event::ObservationDone { test_green, staleness_ok, detail } => {
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
            Event::ObservationDone { test_green, staleness_ok, detail } => {
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
    if let (Some(plan), Some(approved)) = (&plan_payload_digest, &approved_digest)
        && plan != approved
    {
        let mut next = to_state(
            observed,
            FulfillState::Superseded {
                old_digest: plan.clone(),
                new_digest: approved.clone(),
            },
            now,
        );
        next.plan_id = None;
        next.idempotency_key = None;
        return Decision::Advance {
            record: next,
            event: format!("superseded while awaiting review ({plan} -> {approved})"),
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
            if let (Some(plan), Some(approved)) = (&plan_payload_digest, &approved_digest)
                && plan != approved
            {
                let mut next = to_state(
                    observed,
                    FulfillState::Superseded {
                        old_digest: plan.clone(),
                        new_digest: approved.clone(),
                    },
                    now,
                );
                next.plan_id = None;
                next.idempotency_key = None;
                return Decision::Advance {
                    record: next,
                    event: format!("superseded at post-propose ({plan} -> {approved})"),
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
            let record = blocked(observed, format!("policy denied the propose: {refusal}"), now);
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
    match &lookup {
        ReceiptSummary::Succeeded { run_id } => {
            let next = to_state(observed, FulfillState::Applied, now);
            Decision::Advance {
                record: next,
                event: format!("receipt found: applied (run {run_id})"),
            }
        }
        // From `applying_unknown`, no receipt (or a terminal failure)
        // makes the retry dedup-safe: re-run the pre-apply gate then the
        // apply, in-process. From `applying` the same answer is a
        // CONTRADICTION — the engine just said `skipped_idempotent`
        // (a Succeeded receipt exists) and the lookup disagrees — so the
        // state parks at applying_unknown for a human instead of
        // retrying into an inconsistent store.
        ReceiptSummary::NoRecord | ReceiptSummary::Failed { .. } => {
            let detail = match &lookup {
                ReceiptSummary::NoRecord => "no receipt".to_string(),
                ReceiptSummary::Failed { run_id } => format!("prior apply failed (run {run_id})"),
                _ => unreachable!("outer match arm"),
            };
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
                        event: format!(
                            "receipt contradicts skipped_idempotent ({detail})"
                        ),
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

/// The drafting dispatch: count the attempt BEFORE dispatch, then act.
fn dispatch_drafting(
    observed: &FulfillStateRecord,
    product: &str,
    task: TaskKind,
    now: DateTime<Utc>,
) -> Decision {
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
    Decision::AdvanceAndAct {
        record: next,
        event: format!("drafting attempt {}", observed.drafting_attempts + 1),
        task,
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
