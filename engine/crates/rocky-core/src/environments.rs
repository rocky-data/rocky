//! Environments and their append-only publish history (RV1-P2, state schema v32).
//!
//! An **environment** (`staging`, `prod`, ...) is a named set of pointers. Each
//! pointer names one model and the output version a recorded run produced for
//! it. A **publish** moves some pointers in one step and appends one history
//! row. Nothing here touches the warehouse: the pointers are state only.
//!
//! ```text
//!   run history ──▶ publish_pointers(env, expected_head) ──┬──▶ new head  staging#N+1
//!   (output_version            one redb write txn          └──▶ history   staging|000..N+1
//!    per execution)        head != expected_head ──▶ PublishConflict (nothing written)
//! ```
//!
//! What this phase does NOT do, stated so nobody relies on it:
//!
//! - A pointer pins no data. `rocky gc`, run-history retention and Delta
//!   `VACUUM` can remove a version an environment points to. Pinning is RV1-P5.
//! - A state-only publish controls no warehouse object (no view swap, no
//!   clone). The table publish in [`crate::table_publish`] (RV1-P3) moves
//!   Delta tables, one commit per table; views are RV1-P4.
//! - There is no rollback verb and no CLI verb yet.
//!
//! Concurrency. The local store serializes writers through one redb write
//! transaction, so the head read and the head write are atomic. Across pods
//! the shared remote blob is the serialization point:
//! [`crate::state_sync::publish_pointers`] runs the transaction inside a
//! [`crate::state_sync::LedgerSeamSession`], which uploads with
//! compare-and-swap and replays the WHOLE transaction on a fresh download when
//! the blob moved. A replay that finds a different head refuses with
//! [`crate::state_sync::StateSyncError::PublishConflict`]; a replay that finds
//! the same head (an unrelated run finalized in between) succeeds.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::config::PrincipalRef;
use crate::state::{OutputVersion, RunScope, RunStatus, UnversionedReason};

/// Longest environment name, in bytes. Matches the principal id limit.
pub const ENVIRONMENT_NAME_MAX_LEN: usize = 63;

/// A validated environment name.
///
/// The grammar is `^[a-z0-9][a-z0-9._-]{0,62}$`, the same grammar as
/// [`crate::config::PrincipalId`]. It refuses `|` (the history key separator)
/// and `#` (the publish id separator), so a name can never forge another
/// environment's key or id.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct EnvironmentName(String);

impl EnvironmentName {
    /// Parse and validate a name.
    ///
    /// # Errors
    ///
    /// [`EnvironmentError::InvalidName`] for an empty, too long, or
    /// out-of-grammar name.
    pub fn parse(s: &str) -> Result<Self, EnvironmentError> {
        let bad = || EnvironmentError::InvalidName {
            name: s.to_string(),
        };
        if s.is_empty() || s.len() > ENVIRONMENT_NAME_MAX_LEN {
            return Err(bad());
        }
        let bytes = s.as_bytes();
        let lead_ok = bytes[0].is_ascii_lowercase() || bytes[0].is_ascii_digit();
        let rest_ok = bytes[1..].iter().all(|b| {
            b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'.' | b'_' | b'-')
        });
        if !lead_ok || !rest_ok {
            return Err(bad());
        }
        Ok(Self(s.to_string()))
    }

    /// The name as a string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for EnvironmentName {
    type Error = EnvironmentError;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::parse(&s)
    }
}

impl From<EnvironmentName> for String {
    fn from(n: EnvironmentName) -> Self {
        n.0
    }
}

impl std::fmt::Display for EnvironmentName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// The publish id for `seq` of `env`: `"{env}#{seq}"`.
#[must_use]
pub fn publish_id(env: &EnvironmentName, seq: u64) -> String {
    format!("{env}#{seq}")
}

/// The `publish_history` key for `seq` of `env`: `"{env}|{seq:020}"`.
///
/// The zero padding makes the lexical order the numeric order, so a prefix
/// scan over `"{env}|"` returns the history in sequence order.
#[must_use]
pub fn history_key(env: &EnvironmentName, seq: u64) -> String {
    format!("{env}|{seq:020}")
}

/// One environment pointer: a model and the output version a run recorded.
///
/// `version` is a COPY of [`crate::state::ModelExecution::output_version`],
/// so the pointer and its history survive `sweep_run_history` removing the
/// run row it came from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvPointer {
    /// The model name.
    pub model: String,
    /// The run that produced `version`.
    pub run_id: String,
    /// The recorded output version. Never [`OutputVersion::Unversioned`]
    /// when written. Read leniently: see [`PointerVersion`].
    pub version: PointerVersion,
}

/// The version an [`EnvPointer`] holds, read leniently.
///
/// A later binary can add an [`OutputVersion`] variant. A strict read would
/// then fail `get_environment`, `list_environments` and `publish_history` for
/// every row in the table. Here such a value reads as
/// [`PointerVersion::Unreadable`] and keeps its raw JSON, so the row still
/// reads and a rewrite of the row keeps the value unchanged.
///
/// Untagged, so [`PointerVersion::Known`] serializes byte-for-byte as the
/// bare [`OutputVersion`]. The wire shape is the same as a plain field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PointerVersion {
    /// A version this binary can read.
    Known(OutputVersion),
    /// A value this binary cannot parse, kept as written.
    Unreadable(serde_json::Value),
}

impl PointerVersion {
    /// The version, when this binary can read it.
    #[must_use]
    pub fn known(&self) -> Option<&OutputVersion> {
        match self {
            Self::Known(v) => Some(v),
            Self::Unreadable(_) => None,
        }
    }
}

/// The current head of one environment. Key: the environment name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvironmentRecord {
    /// The environment name.
    pub name: EnvironmentName,
    /// The sequence number of the head publish. The first publish is 1.
    pub seq: u64,
    /// The head publish id, `"{name}#{seq}"`.
    pub head_publish_id: String,
    /// Every pointer the environment holds, by model.
    pub pointers: BTreeMap<String, EnvPointer>,
    /// When the head moved.
    pub updated_at: chrono::DateTime<chrono::Utc>,
    /// Who moved the head.
    pub updated_by: PrincipalRef,
    /// The publish id of a table publish that started and has not recorded
    /// its outcome yet (RV1-P3). While it is set, every other publish to this
    /// environment is refused with [`EnvironmentError::PublishInProgress`].
    /// Omitted when `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publishing: Option<String>,
}

/// What a publish changed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PublishScope {
    /// The publish moved state pointers only. No warehouse object changed.
    StateOnly,
    /// The publish moved Delta tables, one commit per table (RV1-P3). It is
    /// NOT atomic across tables: readers can see some tables moved and others
    /// not. [`PublishRecord::tables`] says which.
    DeltaPerTable,
}

/// The table part of a [`PublishScope::DeltaPerTable`] publish.
///
/// A table publish writes two history rows:
///
/// ```text
///   env#N    Started  { planned }          head = env#N, publishing = env#N
///      ── one Delta commit per table, in `planned` order ──
///   env#N+1  Finished { started, moves }   head = env#N+1, publishing = None
/// ```
///
/// In the `Started` row, `to` is the plan, not what moved. The `Finished`
/// row's `to` holds only the models whose table now serves the version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case")]
pub enum TablePublish {
    /// The publish claimed the environment. No table had moved yet.
    Started {
        /// The models to move, in the order the tables are committed.
        planned: Vec<String>,
    },
    /// The publish ended. `moves` holds one entry per planned model, in order.
    Finished {
        /// The publish id of the `Started` row this row closes.
        started: String,
        /// What happened to each table.
        moves: Vec<TableMove>,
    },
}

/// What a table publish did to one model's table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableMove {
    /// The model name.
    pub model: String,
    /// What happened.
    pub outcome: TableMoveOutcome,
}

/// The outcome of one table move.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum TableMoveOutcome {
    /// One commit moved the table. `table_version` is that commit.
    Moved {
        /// The table name.
        table: String,
        /// The commit that made the table serve the version.
        table_version: u64,
        /// A follow-up step after the commit failed (for example the Iceberg
        /// metadata sync). The table did move.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        warning: Option<String>,
    },
    /// The table already served the version. No commit was written.
    AlreadyCurrent {
        /// The table name.
        table: String,
        /// The current table version.
        table_version: u64,
    },
    /// The move failed. The table did not move, as far as the backend can
    /// tell.
    Failed {
        /// Why.
        error: String,
    },
    /// The move's commit may have landed: its write returned an error that
    /// does not say whether the commit was stored. The pointer does not
    /// move. A retry of the publish finds the table already current when the
    /// commit did land.
    Unknown {
        /// The error the commit write returned.
        error: String,
    },
    /// An earlier move failed or ended unknown, so this one was not tried.
    NotAttempted,
}

impl TableMoveOutcome {
    /// Whether the table now serves the version.
    #[must_use]
    pub fn serves_version(&self) -> bool {
        match self {
            Self::Moved { .. } | Self::AlreadyCurrent { .. } => true,
            Self::Failed { .. } | Self::Unknown { .. } | Self::NotAttempted => false,
        }
    }
}

/// One append-only publish history row. Key: [`history_key`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublishRecord {
    /// `"{environment}#{seq}"`.
    pub publish_id: String,
    /// The environment name.
    pub environment: EnvironmentName,
    /// The sequence number. The first publish is 1.
    pub seq: u64,
    /// The head this publish replaced. `None` for the first publish.
    pub prior_publish_id: Option<String>,
    /// Who published.
    pub principal: PrincipalRef,
    /// When the publish committed (local clock of the writer).
    pub published_at: chrono::DateTime<chrono::Utc>,
    /// The prior pointers of the models this publish moved. A model the
    /// environment did not hold before is absent here.
    pub from: BTreeMap<String, EnvPointer>,
    /// The new pointers of the models this publish moved.
    pub to: BTreeMap<String, EnvPointer>,
    /// The plan this publish executes, when there is one.
    pub plan_id: Option<String>,
    /// What the publish changed.
    pub scope: PublishScope,
    /// The table part of a [`PublishScope::DeltaPerTable`] publish. Omitted
    /// for a state-only publish.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tables: Option<TablePublish>,
}

/// One pointer to publish: take `model`'s output version from run `run_id`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishSource {
    /// The model name.
    pub model: String,
    /// The run whose execution of `model` supplies the version.
    pub run_id: String,
}

/// A request to move an environment's pointers.
#[derive(Debug, Clone)]
pub struct PublishRequest {
    /// The environment.
    pub environment: EnvironmentName,
    /// The head the caller read. `None` means "create": the environment must
    /// not exist yet.
    pub expected_head: Option<String>,
    /// The pointers to move. At least one; one per model.
    pub sources: Vec<PublishSource>,
    /// Who publishes.
    pub principal: PrincipalRef,
    /// The plan this publish executes, when there is one.
    pub plan_id: Option<String>,
}

/// Why a model cannot be published.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum PublishRefusal {
    /// The named run is not in run history (never recorded, or swept).
    #[error("run {run_id:?} is not in run history")]
    RunNotFound { run_id: String },
    /// The run did not execute the model.
    #[error("run {run_id:?} did not execute the model")]
    ModelNotInRun { run_id: String },
    /// The run did not write production. A shadow or branch run writes
    /// somewhere else, so its version is not the production output.
    #[error("run {run_id:?} wrote {scope}, not production; only a production run can be published")]
    NotProduction { run_id: String, scope: String },
    /// The run did not end in `Success` or `PartialFailure`.
    #[error("run {run_id:?} ended with status {status:?}; only a successful run can be published")]
    RunNotSuccessful {
        run_id: String,
        status: crate::state::RunStatus,
    },
    /// The run tripped its error-severity check gate.
    #[error("run {run_id:?} failed its check gate")]
    CheckGateFailed { run_id: String },
    /// The run auto-applied schema drift that its `verify_after` gate did not
    /// confirm.
    #[error("run {run_id:?} failed its verify_after gate")]
    VerifyAfterFailed { run_id: String },
    /// The run executed the model, but that execution did not succeed. This
    /// is how a `PartialFailure` run refuses its failed models.
    #[error("run {run_id:?} recorded the model's execution as {status:?}, not \"success\"")]
    ExecutionNotSuccessful { run_id: String, status: String },
    /// The run executed the model more than once, so the version is ambiguous.
    ///
    /// Run history keys an execution by the LAST segment of its asset key. A
    /// `time_interval` model records one execution per partition, and a
    /// replication run can record the same table name from two schemas. Both
    /// land here. RV1-P2 refuses them; how to combine partition versions into
    /// one pointer is an RV1-P3 decision.
    #[error(
        "run {run_id:?} recorded {count} executions for the model (a partitioned time_interval \
         model, or a replicated table name shared by two schemas); partitioned and replicated \
         outputs cannot be published yet"
    )]
    AmbiguousExecution { run_id: String, count: usize },
    /// The execution recorded no output version (a failed execution, a binary
    /// older than RV1-P1b, or a value this binary cannot read).
    #[error("run {run_id:?} recorded no output version for the model")]
    NoOutputVersion { run_id: String },
    /// The execution recorded that it has no version.
    #[error("run {run_id:?} recorded the output as unversioned ({reason:?})")]
    Unversioned {
        run_id: String,
        reason: UnversionedReason,
    },
    /// The table backend of a table publish (RV1-P3) cannot move this
    /// model's table to the version. Nothing was written.
    #[error("the table backend cannot publish this version: {reason}")]
    BackendRefused { reason: String },
}

/// A publish or environment request that is wrong in itself.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum EnvironmentError {
    /// The environment name breaks the grammar.
    #[error(
        "environment name {name:?} is not valid: use lowercase letters, digits, '.', '_' and '-', \
         starting with a letter or a digit, at most 63 bytes (no '|' or '#')"
    )]
    InvalidName { name: String },
    /// The request names no pointer.
    #[error("publish to environment {environment:?} names no model")]
    EmptyPublish { environment: String },
    /// The request names one model twice.
    #[error("publish to environment {environment:?} names model {model:?} twice")]
    DuplicateModel { environment: String, model: String },
    /// A model cannot be published. Nothing was written.
    #[error("publish to environment {environment:?} refused for model {model:?}: {reason}")]
    Refused {
        environment: String,
        model: String,
        reason: PublishRefusal,
    },
    /// A history row already exists at the key. History is insert-only.
    #[error("publish history row {key:?} already exists; history is insert-only")]
    HistoryRowExists { key: String },
    /// A stored head is corrupt: its seq cannot advance.
    #[error("environment {environment:?} head seq {seq} cannot advance")]
    SeqOverflow { environment: String, seq: u64 },
    /// A table publish started and has not recorded its outcome. Its tables
    /// may be part moved. Nothing was written.
    #[error(
        "environment {environment:?} has a table publish in progress ({publish_id}); its tables \
         may be part moved. Wait for it to finish. If its process died, publish again with \
         take-over from head {publish_id}"
    )]
    PublishInProgress {
        environment: String,
        publish_id: String,
    },
    /// The outcome does not match the publish it closes.
    #[error("table publish {publish_id:?} cannot be finished: {reason}")]
    FinishMismatch { publish_id: String, reason: String },
}

/// Resolve one source against the run's recorded executions. Pure, so the
/// output-version gate is testable without a store.
pub(crate) fn resolve_pointer(
    source: &PublishSource,
    run: Option<&crate::state::RunRecord>,
) -> Result<EnvPointer, PublishRefusal> {
    let run_id = source.run_id.clone();
    let Some(run) = run else {
        return Err(PublishRefusal::RunNotFound { run_id });
    };
    // Run-level truth first: a run that did not write production, did not
    // succeed, or tripped a gate supplies no publishable version for ANY model.
    match &run.run_scope {
        Some(RunScope::Production) => {}
        other => {
            let scope = match other {
                Some(RunScope::Shadow { .. }) => "a shadow target".to_string(),
                Some(RunScope::Branch { name }) => format!("branch {name:?}"),
                // A record that cannot say where it wrote fails closed.
                _ => "an unrecorded scope".to_string(),
            };
            return Err(PublishRefusal::NotProduction { run_id, scope });
        }
    }
    match run.status {
        // PartialFailure is allowed: the per-execution check below refuses
        // every model whose own execution did not succeed.
        RunStatus::Success | RunStatus::PartialFailure => {}
        status => return Err(PublishRefusal::RunNotSuccessful { run_id, status }),
    }
    if run.check_gate_failed {
        return Err(PublishRefusal::CheckGateFailed { run_id });
    }
    if run.verify_after_failed {
        return Err(PublishRefusal::VerifyAfterFailed { run_id });
    }
    let mut matching = run
        .models_executed
        .iter()
        .filter(|e| e.model_name == source.model);
    let Some(exec) = matching.next() else {
        return Err(PublishRefusal::ModelNotInRun { run_id });
    };
    let extra = matching.count();
    if extra > 0 {
        return Err(PublishRefusal::AmbiguousExecution {
            run_id,
            count: extra + 1,
        });
    }
    if exec.status != "success" {
        return Err(PublishRefusal::ExecutionNotSuccessful {
            run_id,
            status: exec.status.clone(),
        });
    }
    match &exec.output_version {
        None => Err(PublishRefusal::NoOutputVersion { run_id }),
        Some(OutputVersion::Unversioned { reason }) => Err(PublishRefusal::Unversioned {
            run_id,
            reason: *reason,
        }),
        Some(version) => Ok(EnvPointer {
            model: source.model.clone(),
            run_id,
            version: PointerVersion::Known(version.clone()),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn environment_names_follow_the_principal_grammar() {
        for ok in ["staging", "prod", "a", "team-1.dev_x", "0env"] {
            assert!(EnvironmentName::parse(ok).is_ok(), "{ok}");
        }
        for bad in [
            "",
            "Staging",
            "stag|ing",
            "stag#1",
            "-lead",
            ".lead",
            "a b",
            "a/b",
            &"x".repeat(ENVIRONMENT_NAME_MAX_LEN + 1),
        ] {
            assert!(
                matches!(
                    EnvironmentName::parse(bad),
                    Err(EnvironmentError::InvalidName { .. })
                ),
                "{bad:?} must be refused"
            );
        }
        assert!(EnvironmentName::parse(&"x".repeat(ENVIRONMENT_NAME_MAX_LEN)).is_ok());
        // Deserialization runs the same check.
        assert!(serde_json::from_str::<EnvironmentName>("\"a|b\"").is_err());
    }

    #[test]
    fn history_keys_sort_in_sequence_order() {
        let env = EnvironmentName::parse("staging").unwrap();
        let mut keys: Vec<String> = [10_u64, 2, 1, 100]
            .iter()
            .map(|s| history_key(&env, *s))
            .collect();
        keys.sort();
        assert_eq!(
            keys,
            vec![
                history_key(&env, 1),
                history_key(&env, 2),
                history_key(&env, 10),
                history_key(&env, 100)
            ]
        );
        assert_eq!(history_key(&env, 7), "staging|00000000000000000007");
        assert_eq!(publish_id(&env, 7), "staging#7");
    }

    fn delta_run(run_id: &str) -> crate::state::RunRecord {
        crate::state::run_with_output_versions(
            run_id,
            &[(
                "orders",
                Some(OutputVersion::DeltaObserved {
                    table: "c.s.orders".into(),
                    version: 3,
                }),
            )],
        )
    }

    fn orders_from(run_id: &str) -> PublishSource {
        PublishSource {
            model: "orders".into(),
            run_id: run_id.into(),
        }
    }

    /// The baseline: a successful production run with a version publishes.
    /// Every gate test below changes ONE field of this run.
    #[test]
    fn a_successful_production_run_resolves() {
        let run = delta_run("r1");
        let pointer = resolve_pointer(&orders_from("r1"), Some(&run)).unwrap();
        assert_eq!(pointer.run_id, "r1");
    }

    /// Only a production run publishes. A shadow run, a branch run, and a
    /// record with no scope are refused before any model is looked at.
    #[test]
    fn a_run_that_did_not_write_production_is_refused() {
        for scope in [
            Some(RunScope::Shadow {
                schema: Some("s".into()),
            }),
            Some(RunScope::Branch {
                name: "pr-1".into(),
            }),
            None,
        ] {
            let mut run = delta_run("r1");
            run.run_scope = scope.clone();
            let err = resolve_pointer(&orders_from("r1"), Some(&run)).unwrap_err();
            assert!(
                matches!(&err, PublishRefusal::NotProduction { run_id, .. } if run_id == "r1"),
                "{scope:?}: {err:?}"
            );
        }
    }

    /// A run that failed, or never ran, is refused even if a model in it
    /// recorded a version.
    #[test]
    fn a_run_that_did_not_succeed_is_refused() {
        for status in [
            RunStatus::Failure,
            RunStatus::SkippedIdempotent,
            RunStatus::SkippedInFlight,
        ] {
            let mut run = delta_run("r1");
            run.status = status;
            assert_eq!(
                resolve_pointer(&orders_from("r1"), Some(&run)).unwrap_err(),
                PublishRefusal::RunNotSuccessful {
                    run_id: "r1".into(),
                    status
                }
            );
        }
    }

    /// A tripped check gate or verify_after gate refuses the whole run.
    #[test]
    fn a_run_that_tripped_a_gate_is_refused() {
        let mut run = delta_run("r1");
        run.check_gate_failed = true;
        assert_eq!(
            resolve_pointer(&orders_from("r1"), Some(&run)).unwrap_err(),
            PublishRefusal::CheckGateFailed {
                run_id: "r1".into()
            }
        );
        let mut run = delta_run("r1");
        run.verify_after_failed = true;
        assert_eq!(
            resolve_pointer(&orders_from("r1"), Some(&run)).unwrap_err(),
            PublishRefusal::VerifyAfterFailed {
                run_id: "r1".into()
            }
        );
    }

    /// A `PartialFailure` run publishes the models that succeeded and refuses
    /// the ones that failed.
    #[test]
    fn a_partial_failure_run_publishes_only_its_successful_models() {
        let mut run = delta_run("r1");
        run.status = RunStatus::PartialFailure;
        let mut failed = run.models_executed[0].clone();
        failed.model_name = "customers".into();
        failed.status = "failed".into();
        run.models_executed.push(failed);
        assert!(resolve_pointer(&orders_from("r1"), Some(&run)).is_ok());
        let customers = PublishSource {
            model: "customers".into(),
            run_id: "r1".into(),
        };
        assert_eq!(
            resolve_pointer(&customers, Some(&run)).unwrap_err(),
            PublishRefusal::ExecutionNotSuccessful {
                run_id: "r1".into(),
                status: "failed".into()
            }
        );
    }

    /// Two executions under one model name refuse, and the message names
    /// the cause so the user knows it is a P2 limit, not a broken run.
    #[test]
    fn a_partitioned_or_replicated_model_is_refused_with_its_cause() {
        let mut run = delta_run("r1");
        let second = run.models_executed[0].clone();
        run.models_executed.push(second);
        let err = resolve_pointer(&orders_from("r1"), Some(&run)).unwrap_err();
        assert_eq!(
            err,
            PublishRefusal::AmbiguousExecution {
                run_id: "r1".into(),
                count: 2
            }
        );
        let msg = err.to_string();
        assert!(msg.contains("time_interval"), "{msg}");
        assert!(msg.contains("cannot be published yet"), "{msg}");
    }

    /// A pointer whose version is a variant this binary does not know still
    /// reads, keeps its raw value, and writes it back unchanged. A known
    /// version keeps the bare `OutputVersion` wire shape.
    #[test]
    fn a_pointer_with_an_unknown_version_variant_still_reads() {
        let known = r#"{"model":"orders","run_id":"r1","version":{"kind":"delta_observed","table":"c.s.t","version":7}}"#;
        let p: EnvPointer = serde_json::from_str(known).unwrap();
        assert!(matches!(
            p.version.known(),
            Some(OutputVersion::DeltaObserved { version: 7, .. })
        ));
        assert_eq!(serde_json::to_string(&p).unwrap(), known);

        let future =
            r#"{"model":"orders","run_id":"r1","version":{"kind":"iceberg_snapshot","id":42}}"#;
        let p: EnvPointer = serde_json::from_str(future).unwrap();
        assert!(p.version.known().is_none());
        assert!(matches!(p.version, PointerVersion::Unreadable(_)));
        // Kept as written (the same JSON value; key order is not kept).
        assert_eq!(
            serde_json::to_value(&p).unwrap(),
            serde_json::from_str::<serde_json::Value>(future).unwrap()
        );

        // A whole head with one unknown pointer reads; the other stays known.
        let head = format!(
            r#"{{"name":"prod","seq":1,"head_publish_id":"prod#1","pointers":{{"a":{known},"b":{future}}},"updated_at":"2026-10-06T00:00:00Z","updated_by":{by}}}"#,
            by = serde_json::to_string(&PrincipalRef::unnamed()).unwrap()
        );
        let rec: EnvironmentRecord = serde_json::from_str(&head).unwrap();
        assert!(rec.pointers["a"].version.known().is_some());
        assert!(rec.pointers["b"].version.known().is_none());
    }

    #[test]
    fn publish_scope_wire_shape_is_state_only() {
        assert_eq!(
            serde_json::to_string(&PublishScope::StateOnly).unwrap(),
            "\"state_only\""
        );
        assert_eq!(
            serde_json::to_string(&PublishScope::DeltaPerTable).unwrap(),
            "\"delta_per_table\""
        );
    }

    /// A P2 row (no `tables`, no `publishing`) reads back with both `None`,
    /// and a state-only row still serializes without them.
    #[test]
    fn rows_without_the_table_fields_read_as_state_only() {
        let head = format!(
            r#"{{"name":"prod","seq":1,"head_publish_id":"prod#1","pointers":{{}},"updated_at":"2026-10-06T00:00:00Z","updated_by":{by}}}"#,
            by = serde_json::to_string(&PrincipalRef::unnamed()).unwrap()
        );
        let rec: EnvironmentRecord = serde_json::from_str(&head).unwrap();
        assert_eq!(rec.publishing, None);
        assert!(!serde_json::to_string(&rec).unwrap().contains("publishing"));
        let moved = TableMove {
            model: "orders".into(),
            outcome: TableMoveOutcome::Moved {
                table: "c.s.orders".into(),
                table_version: 4,
                warning: None,
            },
        };
        assert_eq!(
            serde_json::to_string(&moved).unwrap(),
            r#"{"model":"orders","outcome":{"outcome":"moved","table":"c.s.orders","table_version":4}}"#
        );
    }
}
