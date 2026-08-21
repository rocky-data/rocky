//! Store driving: every reconciler transition goes through
//! `StateStore::fulfill_state_cas` — CAS-on-observed-prior, journal row
//! in the same transaction — and ownership is settled before any
//! transition is attempted.
//!
//! A lost CAS means another process moved the record: this process
//! stops, prints who won, and writes nothing further. The stale-owner
//! takeover (grace stamp + pid/start-time liveness, decided purely in
//! [`crate::machine::decide_ownership`]) replaces any `--break-lock`
//! style manual unlock.

use std::path::Path;

use anyhow::{Context, Result, bail};
use chrono::{DateTime, SecondsFormat, Utc};
use rocky_core::fulfill::{
    FulfillCas, FulfillJournalRow, FulfillStateRecord, ProductApprovalRecord,
};
use rocky_core::state::StateStore;

use crate::machine::{OwnerLiveness, OwnershipDecision, SelfIdentity, decide_ownership};

/// The store handle for one product's reconciler records.
///
/// Opens the state store PER OPERATION rather than holding a writer:
/// redb allows one writer per state file, and the loop's engine calls
/// (status, compile, the apply core) open their own handles between our
/// transitions. A held handle would deadlock the loop against itself;
/// correctness never depended on it — every write is CAS-on-observed-
/// prior, so an interleaved writer makes our next CAS lose, which is
/// the designed stand-down.
pub struct StoreDriver {
    state_path: std::path::PathBuf,
    product_name: String,
    product_id: String,
}

/// The outcome of applying one fenced transition.
#[derive(Debug)]
pub enum Applied {
    /// The CAS won; this is the record as stored (journal seq stamped).
    Won(FulfillStateRecord),
    /// Another process moved the record. Stop; the payload names it.
    Lost {
        /// The record that won, when one exists.
        winner: Option<FulfillStateRecord>,
    },
}

/// The outcome of the ownership acquisition.
#[derive(Debug)]
pub enum Acquired {
    /// This process owns the record; drive it. Boxed: the record dwarfs
    /// the stop message, and ownership is decided once per invocation.
    Owned(Box<FulfillStateRecord>),
    /// This process must not drive the record; the message says why and
    /// what to do (printed verbatim, exit clean).
    Stopped(String),
}

impl StoreDriver {
    /// Bind to the state store at `state_path` for `product_name`. The
    /// store itself is opened per operation (see the type docs).
    pub fn open(state_path: &Path, product_name: &str) -> Result<Self> {
        Ok(Self {
            state_path: state_path.to_path_buf(),
            product_name: product_name.to_string(),
            product_id: format!("product:{product_name}"),
        })
    }

    /// The product's key form (`product:<name>`).
    pub fn product_id(&self) -> &str {
        &self.product_id
    }

    fn store(&self) -> Result<StateStore> {
        StateStore::open(&self.state_path).with_context(|| {
            format!(
                "failed to open the state store at {} for the product records",
                self.state_path.display()
            )
        })
    }

    /// Read the current record.
    pub fn read(&self) -> Result<Option<FulfillStateRecord>> {
        Ok(self.store()?.fulfill_state_get(&self.product_name)?)
    }

    /// Read the current spec-approval record (a pointer — callers must
    /// re-verify snapshot bytes against its digest before trusting them).
    pub fn approval(&self) -> Result<Option<ProductApprovalRecord>> {
        Ok(self.store()?.product_approval_get(&self.product_name)?)
    }

    /// Read the journal, in append order.
    pub fn journal(&self) -> Result<Vec<FulfillJournalRow>> {
        Ok(self.store()?.fulfill_journal_rows(&self.product_name)?)
    }

    /// Apply one fenced transition: CAS `expected → new`, journaling
    /// `event`, all in one transaction. On `Won`, the returned record is
    /// the stored one (journal seq stamped by the same arithmetic the
    /// transaction uses).
    pub fn transition(
        &self,
        expected: Option<&FulfillStateRecord>,
        new: &FulfillStateRecord,
        event: &str,
        now: DateTime<Utc>,
    ) -> Result<Applied> {
        let row = FulfillJournalRow {
            seq: 0, // allocated inside the transaction
            at: Some(now.to_rfc3339_opts(SecondsFormat::Secs, true)),
            event: event.to_string(),
            from_state: expected.map(|r| r.state.tag().to_string()),
            to_state: new.state.tag().to_string(),
            spec_digest: new.spec_digest.clone(),
            plan_id: new.plan_id.clone(),
            idempotency_key: new.idempotency_key.clone(),
        };
        match self
            .store()?
            .fulfill_state_cas(&self.product_name, expected, new, &row)?
        {
            FulfillCas::Won => {
                let mut stored = new.clone();
                // The transaction allocates seq = prior + 1 (1 on insert);
                // mirror it so the next CAS's expected-prior matches.
                stored.journal_seq = expected.map(|r| r.journal_seq + 1).unwrap_or(1);
                Ok(Applied::Won(stored))
            }
            FulfillCas::Lost { current_state, .. } => Ok(Applied::Lost {
                winner: current_state.map(|boxed| *boxed),
            }),
        }
    }

    /// Acquire ownership of the record (or learn why not). Applies the
    /// pure [`decide_ownership`] verdict via the CAS; a lost CAS at any
    /// point means another process moved first — stand down.
    pub fn acquire(&self, me: SelfIdentity, now: DateTime<Utc>) -> Result<Acquired> {
        let observed = self.read()?;
        let liveness = match &observed {
            Some(record) => match (record.owner_pid, record.owner_start_time) {
                (Some(pid), expected_start)
                    if pid != me.pid || expected_start != Some(me.start_time) =>
                {
                    Some(probe_owner(pid, expected_start))
                }
                (Some(_), _) => None, // ourselves; decide_ownership answers AlreadyOwned
                (None, _) => None,
            },
            None => None,
        };
        let decision = decide_ownership(
            observed.as_ref(),
            liveness.as_ref(),
            me,
            &self.product_id,
            now,
        );
        match decision {
            OwnershipDecision::AlreadyOwned => {
                let record = observed.expect("AlreadyOwned implies a record");
                Ok(Acquired::Owned(Box::new(record)))
            }
            OwnershipDecision::Claim(claimed) | OwnershipDecision::TakeOver(claimed) => {
                match self.transition(observed.as_ref(), &claimed, "ownership acquired", now)? {
                    Applied::Won(stored) => Ok(Acquired::Owned(Box::new(stored))),
                    Applied::Lost { winner } => Ok(Acquired::Stopped(lost_message(&winner))),
                }
            }
            OwnershipDecision::StandDown { owner_pid } => Ok(Acquired::Stopped(format!(
                "product '{}' is being driven by a live process (pid {owner_pid}); \
                 nothing was written — stop that process first if you must take over",
                self.product_name
            ))),
            OwnershipDecision::StampGrace(stamped) => {
                let message = format!(
                    "product '{}' carries an owner stamp that cannot be verified; \
                     a recovery grace has started — re-run after {} seconds to take over",
                    self.product_name,
                    crate::machine::FULFILL_RECOVERY_GRACE.num_seconds()
                );
                match self.transition(observed.as_ref(), &stamped, "recovery grace stamped", now)? {
                    Applied::Won(_) => Ok(Acquired::Stopped(message)),
                    Applied::Lost { winner } => Ok(Acquired::Stopped(lost_message(&winner))),
                }
            }
            OwnershipDecision::WaitGrace { remaining_seconds } => Ok(Acquired::Stopped(format!(
                "product '{}' carries an owner stamp that cannot be verified; \
                 the recovery grace has {remaining_seconds} seconds left — re-run after it",
                self.product_name
            ))),
        }
    }

    /// Release ownership on a clean stop: clear the owner stamp so the
    /// next invocation claims immediately (no grace). Journal-free by
    /// design? No — every write journals; the release row keeps the
    /// audit trail complete.
    pub fn release(
        &self,
        observed: &FulfillStateRecord,
        now: DateTime<Utc>,
    ) -> Result<FulfillStateRecord> {
        let mut released = observed.clone();
        released.owner_pid = None;
        released.owner_start_time = None;
        released.driver_pgid = None;
        released.driver_leader_start_time = None;
        released.updated_at = Some(now.to_rfc3339_opts(SecondsFormat::Secs, true));
        match self.transition(Some(observed), &released, "ownership released", now)? {
            Applied::Won(stored) => Ok(stored),
            Applied::Lost { winner } => {
                // Losing the release is safe: whoever moved the record owns
                // it now, and our stamp is gone either way.
                bail!("{}", lost_message(&winner))
            }
        }
    }

    /// Stamp (or clear) the live driver group on the record, so a
    /// takeover after a crash can sweep the group before dispatching a
    /// new worker.
    pub fn stamp_driver_group(
        &self,
        observed: &FulfillStateRecord,
        group: Option<(u32, u64)>,
        now: DateTime<Utc>,
    ) -> Result<FulfillStateRecord> {
        let mut stamped = observed.clone();
        match group {
            Some((pgid, leader_start)) => {
                stamped.driver_pgid = Some(pgid);
                stamped.driver_leader_start_time = Some(leader_start);
            }
            None => {
                stamped.driver_pgid = None;
                stamped.driver_leader_start_time = None;
            }
        }
        stamped.updated_at = Some(now.to_rfc3339_opts(SecondsFormat::Secs, true));
        let event = match group {
            Some((pgid, _)) => format!("driver group started (pgid {pgid})"),
            None => "driver group ended".to_string(),
        };
        match self.transition(Some(observed), &stamped, &event, now)? {
            Applied::Won(stored) => Ok(stored),
            Applied::Lost { winner } => bail!("{}", lost_message(&winner)),
        }
    }
}

/// The stand-down message for a lost CAS: name what won.
pub fn lost_message(winner: &Option<FulfillStateRecord>) -> String {
    match winner {
        Some(record) => format!(
            "another process moved this product's record (now '{}', owner pid {}); \
             nothing was written — re-run to observe the new state",
            record.state.tag(),
            record
                .owner_pid
                .map(|p| p.to_string())
                .unwrap_or_else(|| "<released>".to_string()),
        ),
        None => "another process moved this product's record (now absent); nothing was written"
            .to_string(),
    }
}

/// Probe a recorded owner: alive, definitively dead (no such pid, or the
/// pid was reused by a process with a different start time), or
/// indefinite (the probe itself failed).
fn probe_owner(pid: u32, expected_start: Option<u64>) -> OwnerLiveness {
    match process_liveness(pid) {
        Ok(None) => OwnerLiveness::Dead,
        Ok(Some(start_time)) => match expected_start {
            // A record without a start time cannot be verified against
            // reuse — but the pid exists, so the conservative answer is
            // alive (stand down; the human can stop it).
            None => OwnerLiveness::Alive,
            Some(expected) if expected == start_time => OwnerLiveness::Alive,
            Some(_) => OwnerLiveness::Dead, // the pid was reused
        },
        Err(why) => OwnerLiveness::Indefinite(why),
    }
}

/// This process's own identity for the ownership stamp.
pub fn self_identity() -> Result<SelfIdentity> {
    let pid = std::process::id();
    match process_liveness(pid) {
        Ok(Some(start_time)) => Ok(SelfIdentity { pid, start_time }),
        Ok(None) => bail!("could not read this process's own start time (probe found no process)"),
        Err(why) => bail!("could not read this process's own start time: {why}"),
    }
}

/// The start time of a live process, or `None` when no such pid exists.
///
/// Re-exported from [`rocky_core::process`], which is where the probe
/// lives so `rocky-cli` can reach it too (this crate sits above
/// `rocky-cli`, so it could not host a primitive that layer needs).
pub use rocky_core::process::process_liveness;

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn the_probe_sees_this_process_alive_with_a_stable_start_time() {
        let pid = std::process::id();
        let first = process_liveness(pid)
            .expect("probing our own pid must not fail")
            .expect("we exist");
        let second = process_liveness(pid).expect("probe").expect("still us");
        assert_eq!(first, second, "a process's start time never changes");
    }

    #[cfg(unix)]
    #[test]
    fn the_probe_answers_none_for_a_dead_pid_and_detects_reuse() {
        // Spawn a child, record its identity, let it exit, then probe.
        let mut child = std::process::Command::new("/bin/sh")
            .args(["-c", "exit 0"])
            .spawn()
            .expect("spawn");
        let pid = child.id();
        child.wait().expect("wait");
        // The pid is now dead (or, in a pathological pid-reuse race,
        // belongs to a NEW process whose start time differs from any
        // value we could have recorded). Either way the guard's answer
        // for (pid, recorded_start) must be "not the same process".
        match process_liveness(pid).expect("probe must not fail") {
            None => {} // dead, the common case
            Some(start) => {
                // Reused: the reused process cannot share our child's
                // start instant. We cannot know the child's exact start
                // value anymore, but a wrong expectation must read Dead —
                // exercise probe_owner's comparison arm directly.
                assert_eq!(
                    probe_owner(pid, Some(start.wrapping_add(1))),
                    crate::machine::OwnerLiveness::Dead,
                    "a start-time mismatch is a definitive reuse verdict"
                );
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn probe_owner_arms() {
        use crate::machine::OwnerLiveness;
        let me = std::process::id();
        let my_start = process_liveness(me).unwrap().unwrap();
        assert_eq!(probe_owner(me, Some(my_start)), OwnerLiveness::Alive);
        assert_eq!(
            probe_owner(me, Some(my_start.wrapping_add(1))),
            OwnerLiveness::Dead,
            "same pid, different start time = the pid was reused"
        );
        assert_eq!(
            probe_owner(me, None),
            OwnerLiveness::Alive,
            "an unverifiable start time on a LIVE pid stays conservative"
        );
    }
}

#[cfg(test)]
mod store_tests {
    use super::*;
    use rocky_core::fulfill::FulfillState;

    fn driver(dir: &Path) -> StoreDriver {
        StoreDriver::open(&dir.join("state.redb"), "revenue_daily").expect("bind")
    }

    #[test]
    fn acquire_claims_then_release_clears_the_stamp() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = driver(dir.path());
        let me = self_identity().expect("identity");
        let now = chrono::Utc::now();

        // Fresh product: claim inserts init with our stamp.
        let Acquired::Owned(record) = store.acquire(me, now).expect("acquire") else {
            panic!("expected Owned");
        };
        assert_eq!(record.state, FulfillState::Init);
        assert_eq!(record.owner_pid, Some(me.pid));
        assert_eq!(record.journal_seq, 1, "the claim journaled");

        // Release clears the stamp; the next acquire claims immediately
        // (no grace) — the crash-free path never waits.
        let released = store.release(&record, now).expect("release");
        assert!(released.owner_pid.is_none());
        let Acquired::Owned(again) = store.acquire(me, now).expect("re-acquire") else {
            panic!("expected Owned");
        };
        assert_eq!(again.owner_pid, Some(me.pid));

        // Same-process re-entry while stamped: AlreadyOwned, no write.
        let before = store.journal().expect("journal").len();
        let Acquired::Owned(_) = store.acquire(me, now).expect("acquire") else {
            panic!("expected Owned");
        };
        assert_eq!(
            store.journal().expect("journal").len(),
            before,
            "AlreadyOwned writes nothing"
        );
    }

    /// The step loop CHAINS the record each `Won` returns into the next
    /// CAS's expected-prior — so the returned record's `journal_seq`
    /// mirror must match the transaction's arithmetic exactly. A drifted
    /// mirror would make the very next chained CAS lose against our own
    /// write (a phantom "another process moved").
    #[test]
    fn chained_transitions_ride_the_returned_record() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = driver(dir.path());
        let me = self_identity().expect("identity");
        let now = chrono::Utc::now();
        let Acquired::Owned(record) = store.acquire(me, now).expect("acquire") else {
            panic!("expected Owned");
        };
        let mut record = *record;
        for (step, state) in [
            FulfillState::Elicited,
            FulfillState::SpecApproved,
            FulfillState::LoweredContract,
        ]
        .into_iter()
        .enumerate()
        {
            let mut next = record.clone();
            next.state = state;
            match store
                .transition(Some(&record), &next, "chained", now)
                .expect("cas answers")
            {
                Applied::Won(stored) => record = stored,
                Applied::Lost { winner } => panic!(
                    "chained CAS {step} lost against our own write: {}",
                    lost_message(&winner)
                ),
            }
        }
        assert_eq!(record.journal_seq, 4, "acquire + three chained rows");
        assert_eq!(store.journal().expect("journal").len(), 4);
    }

    #[test]
    fn a_lost_transition_stops_cleanly_and_writes_nothing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = driver(dir.path());
        let me = self_identity().expect("identity");
        let now = chrono::Utc::now();
        let Acquired::Owned(record) = store.acquire(me, now).expect("acquire") else {
            panic!("expected Owned");
        };

        // Another process moves the record between our read and write.
        let mut moved = record.clone();
        moved.state = FulfillState::SpecApproved;
        let Applied::Won(_) = store
            .transition(Some(&record), &moved, "foreign move", now)
            .expect("foreign wins")
        else {
            panic!("foreign CAS should win");
        };

        // Our CAS against the stale prior loses; the winner is named;
        // nothing of ours is journaled.
        let rows_before = store.journal().expect("journal").len();
        let mut ours = record.clone();
        ours.state = FulfillState::Elicited;
        let Applied::Lost { winner } = store
            .transition(Some(&record), &ours, "our move", now)
            .expect("cas answers")
        else {
            panic!("expected Lost");
        };
        let message = lost_message(&winner);
        assert!(message.contains("spec_approved"), "{message}");
        assert_eq!(store.journal().expect("journal").len(), rows_before);
    }

    #[test]
    fn a_dead_owner_is_taken_over_immediately_through_the_store() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = driver(dir.path());
        let now = chrono::Utc::now();

        // Seed a record owned by a process that is now dead: spawn one,
        // record its REAL identity, let it exit.
        let mut child = std::process::Command::new("/bin/sh")
            .args(["-c", "exit 0"])
            .spawn()
            .expect("spawn");
        let dead_pid = child.id();
        // The start time must be read BEFORE exit to be authentic; a
        // dead-probe answers None, so fall back to a sentinel that can
        // never match a live reuse.
        let dead_start = process_liveness(dead_pid).ok().flatten().unwrap_or(1);
        child.wait().expect("reap");

        let mut record = rocky_core::fulfill::FulfillStateRecord::new(
            FulfillState::Proposed,
            "product:revenue_daily".to_string(),
            None,
            None,
        );
        record.owner_pid = Some(dead_pid);
        record.owner_start_time = Some(dead_start);
        let Applied::Won(_) = store
            .transition(None, &record, "seeded dead owner", now)
            .expect("seed")
        else {
            panic!("seed should win");
        };

        let me = self_identity().expect("identity");
        let Acquired::Owned(taken) = store.acquire(me, now).expect("acquire") else {
            panic!("a definitively dead owner is taken over immediately");
        };
        assert_eq!(taken.owner_pid, Some(me.pid));
        assert_eq!(taken.state, FulfillState::Proposed, "state untouched");
    }

    #[cfg(unix)]
    #[test]
    fn a_live_owner_stands_the_acquire_down_without_writes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = driver(dir.path());
        let now = chrono::Utc::now();

        let mut sleeper = std::process::Command::new("/bin/sleep")
            .arg("300")
            .spawn()
            .expect("sleeper");
        let pid = sleeper.id();
        let start = process_liveness(pid).expect("probe").expect("alive");

        let mut record = rocky_core::fulfill::FulfillStateRecord::new(
            FulfillState::Drafting,
            "product:revenue_daily".to_string(),
            None,
            None,
        );
        record.owner_pid = Some(pid);
        record.owner_start_time = Some(start);
        store
            .transition(None, &record, "seeded live owner", now)
            .expect("seed");

        let me = self_identity().expect("identity");
        let rows_before = store.journal().expect("journal").len();
        let Acquired::Stopped(message) = store.acquire(me, now).expect("acquire") else {
            panic!("a live owner must stand the acquire down");
        };
        assert!(message.contains(&pid.to_string()), "{message}");
        assert_eq!(store.journal().expect("journal").len(), rows_before);

        sleeper.kill().expect("kill");
        sleeper.wait().expect("reap");
    }
}
