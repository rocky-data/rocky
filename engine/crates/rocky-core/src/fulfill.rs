//! Fulfillment records: the per-product reconciler state, its journal
//! rows, and the spec-approval record.
//!
//! These are the value types behind the `fulfill_state` and
//! `product_approvals` redb tables (see `state.rs`). The state-machine
//! DRIVER that moves a product through these states is a separate work
//! package; the record shapes ship with the schema bump so the on-disk
//! format is settled once.
//!
//! # Key layout (one table, two key forms)
//!
//! - the current record at key `product:<name>`
//!   ([`fulfill_state_key`]);
//! - append-only journal rows at `product:<name>#<seq:08>`
//!   ([`fulfill_journal_key`]), with `seq` allocated inside the same CAS
//!   transaction that writes the record — a torn record/journal pair is
//!   impossible by construction.
//!
//! # Serde discipline
//!
//! The state enum is `#[serde(tag = "state", rename_all = "snake_case")]`
//! with per-state payloads flattened into the record, and every non-key
//! field is `#[serde(default)]` so a record written by an older binary —
//! or a future one that adds a field — round-trips cleanly (the
//! `schedule::claim::ClaimRecord` discipline).

use serde::{Deserialize, Serialize};

/// The reconciler state of one product, as persisted.
///
/// The vocabulary is the fulfillment design's transition table, shipped
/// ahead of the driver so the schema is settled once. Only the approval
/// transition is written today (`rocky product approve`); every other
/// variant exists so the driver lands without another schema bump.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum FulfillState {
    /// First run; nothing recorded yet.
    Init,
    /// The elicitation task produced a candidate spec.
    Elicited,
    /// A human approved the spec snapshot ([`ProductApprovalRecord`]).
    SpecApproved,
    /// Phase A committed: the contract exists, hashed into the manifest.
    LoweredContract,
    /// A worker drafting task is (or was) in flight.
    Drafting,
    /// Phase B committed: spec-owned metadata merged into the sidecar.
    Merged,
    /// The runner is verifying the drafted model (compile, test, policy).
    Verifying,
    /// A governed plan was proposed.
    Proposed,
    /// The plan's human review marker was observed.
    PlanApproved,
    /// An apply is in flight (digest + idempotency key pinned first).
    Applying,
    /// Resume found `applying` begun with no terminal record; resolved
    /// only by an authoritative receipt lookup, never a blind re-apply.
    ApplyingUnknown,
    /// The apply completed.
    Applied,
    /// Post-apply observation (staleness, checks).
    Observing,
    /// Stopped for a human.
    NeedsInput {
        /// What is needed (e.g. `spec_approval`, `policy`).
        #[serde(default)]
        reason: String,
        /// Free-form payload for the human (e.g. the candidate digest).
        #[serde(default)]
        payload: String,
    },
    /// Stopped on an error a retry will not fix without a human.
    Blocked {
        /// Why, in plain language.
        #[serde(default)]
        reason: String,
    },
    /// The approved spec moved while work was in flight.
    Superseded {
        /// The digest the in-flight work was built from.
        #[serde(default)]
        old_digest: String,
        /// The newly approved digest.
        #[serde(default)]
        new_digest: String,
    },
}

impl FulfillState {
    /// The wire tag of this state (`"spec_approved"`, ...), for journal
    /// rows and display.
    pub fn tag(&self) -> &'static str {
        match self {
            Self::Init => "init",
            Self::Elicited => "elicited",
            Self::SpecApproved => "spec_approved",
            Self::LoweredContract => "lowered_contract",
            Self::Drafting => "drafting",
            Self::Merged => "merged",
            Self::Verifying => "verifying",
            Self::Proposed => "proposed",
            Self::PlanApproved => "plan_approved",
            Self::Applying => "applying",
            Self::ApplyingUnknown => "applying_unknown",
            Self::Applied => "applied",
            Self::Observing => "observing",
            Self::NeedsInput { .. } => "needs_input",
            Self::Blocked { .. } => "blocked",
            Self::Superseded { .. } => "superseded",
        }
    }
}

/// The current fulfillment record of one product (key `product:<name>`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FulfillStateRecord {
    /// The lifecycle state, flattened (`"state": "<tag>"` plus payload).
    #[serde(flatten)]
    pub state: FulfillState,
    /// `product:<name>` — matches the key, carried for self-description.
    #[serde(default)]
    pub product_id: String,
    /// The approved spec digest this state was reached under, if any.
    #[serde(default)]
    pub spec_digest: Option<String>,
    /// Sequence number of the newest journal row written with this
    /// record. Allocated inside the CAS transaction; the next row is
    /// `journal_seq + 1`.
    #[serde(default)]
    pub journal_seq: u64,
    /// RFC3339 instant of the last transition.
    #[serde(default)]
    pub updated_at: Option<String>,
}

/// One append-only journal row (key `product:<name>#<seq:08>`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FulfillJournalRow {
    /// This row's sequence number (also encoded in its key).
    #[serde(default)]
    pub seq: u64,
    /// RFC3339 instant of the transition.
    #[serde(default)]
    pub at: Option<String>,
    /// What happened, in plain language (e.g. `spec approved`).
    #[serde(default)]
    pub event: String,
    /// The state tag before the transition, when one existed.
    #[serde(default)]
    pub from_state: Option<String>,
    /// The state tag after the transition.
    #[serde(default)]
    pub to_state: String,
    /// The spec digest involved, when the event concerns one.
    #[serde(default)]
    pub spec_digest: Option<String>,
}

/// The spec-approval record (table `product_approvals`, key
/// `product:<name>`).
///
/// redb stores records, never payload blobs: the approved bytes live on
/// disk at [`ProductApprovalRecord::snapshot_path`], immutable and
/// digest-addressed, and every reader re-verifies file-bytes digest ==
/// record digest before trusting them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProductApprovalRecord {
    /// `product:<name>`.
    #[serde(default)]
    pub product_id: String,
    /// `sha256:<hex>` over the approved snapshot's raw bytes.
    #[serde(default)]
    pub spec_digest: String,
    /// Best-effort approver identity (the review-marker convention: git
    /// identity when available, `unknown` otherwise — an honesty caveat,
    /// not an authentication claim).
    #[serde(default)]
    pub approver: String,
    /// RFC3339 instant of the approval.
    #[serde(default)]
    pub approved_at: Option<String>,
    /// Project-root-relative POSIX path of the immutable snapshot file
    /// (`.rocky/fulfillment/<name>/approved-<digest>.toml`).
    #[serde(default)]
    pub snapshot_path: String,
}

/// The key of a product's current fulfillment record: `product:<name>`.
pub fn fulfill_state_key(product_name: &str) -> String {
    format!("product:{product_name}")
}

/// The key of one journal row: `product:<name>#<seq:08>`.
///
/// The sequence is zero-padded to 8 digits so a lexicographic key scan
/// returns rows in append order.
pub fn fulfill_journal_key(product_name: &str, seq: u64) -> String {
    format!("product:{product_name}#{seq:08}")
}

/// The outcome of a fulfillment CAS transaction.
///
/// A `Lost` means another process moved the record between the caller's
/// read and this write: stop, no further writes — the returned current
/// values are what won.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FulfillCas {
    /// The expected priors matched; every write committed atomically.
    Won,
    /// A prior did not match; nothing was written. Boxed: the records
    /// dwarf the `Won` variant, and a CAS loss is the rare arm.
    Lost {
        /// The approval record currently stored, if any.
        current_approval: Option<Box<ProductApprovalRecord>>,
        /// The fulfillment record currently stored, if any.
        current_state: Option<Box<FulfillStateRecord>>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_tags_round_trip_through_serde() {
        // The tag() spelling and the serde wire tag must agree — journal
        // rows record tag() strings and readers parse serde output.
        let states = [
            FulfillState::Init,
            FulfillState::Elicited,
            FulfillState::SpecApproved,
            FulfillState::LoweredContract,
            FulfillState::Drafting,
            FulfillState::Merged,
            FulfillState::Verifying,
            FulfillState::Proposed,
            FulfillState::PlanApproved,
            FulfillState::Applying,
            FulfillState::ApplyingUnknown,
            FulfillState::Applied,
            FulfillState::Observing,
            FulfillState::NeedsInput {
                reason: "spec_approval".to_string(),
                payload: String::new(),
            },
            FulfillState::Blocked {
                reason: "x".to_string(),
            },
            FulfillState::Superseded {
                old_digest: "sha256:a".to_string(),
                new_digest: "sha256:b".to_string(),
            },
        ];
        for state in states {
            let value = serde_json::to_value(&state).expect("serializes");
            assert_eq!(value["state"], state.tag(), "{state:?}");
            let back: FulfillState = serde_json::from_value(value).expect("parses");
            assert_eq!(back, state);
        }
    }

    #[test]
    fn a_record_with_only_a_state_forward_deserializes_with_defaults() {
        // Old-binary round-trip discipline: every non-key field defaults.
        let record: FulfillStateRecord =
            serde_json::from_str(r#"{"state": "spec_approved"}"#).expect("parses");
        assert_eq!(record.state, FulfillState::SpecApproved);
        assert_eq!(record.product_id, "");
        assert!(record.spec_digest.is_none());
        assert_eq!(record.journal_seq, 0);
        assert!(record.updated_at.is_none());
    }

    #[test]
    fn journal_keys_sort_in_append_order() {
        let keys: Vec<String> = [1u64, 2, 10, 99_999_999]
            .iter()
            .map(|seq| fulfill_journal_key("revenue_daily", *seq))
            .collect();
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(keys, sorted, "zero-padding keeps lexicographic = numeric");
        assert_eq!(keys[0], "product:revenue_daily#00000001");
    }

    #[test]
    fn journal_keys_never_collide_with_the_record_key() {
        // The `#` separator keeps journal rows outside every record key:
        // record keys are `product:<name>` and a bare identifier name can
        // never contain `#`.
        assert_eq!(fulfill_state_key("revenue_daily"), "product:revenue_daily");
        assert_ne!(
            fulfill_state_key("revenue_daily"),
            fulfill_journal_key("revenue_daily", 0)
        );
    }
}
