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
//! - A pointer controls no warehouse object (no view swap, no clone). That is
//!   RV1-P3 / RV1-P4.
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
use crate::state::{OutputVersion, UnversionedReason};

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
    /// The recorded output version. Never [`OutputVersion::Unversioned`].
    pub version: OutputVersion,
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
}

/// What a publish changed. Only `state_only` exists in RV1-P2.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PublishScope {
    /// The publish moved state pointers only. No warehouse object changed.
    StateOnly,
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
    /// The run executed the model more than once, so the version is ambiguous.
    #[error("run {run_id:?} executed the model {count} times; the version is ambiguous")]
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
    match &exec.output_version {
        None => Err(PublishRefusal::NoOutputVersion { run_id }),
        Some(OutputVersion::Unversioned { reason }) => Err(PublishRefusal::Unversioned {
            run_id,
            reason: *reason,
        }),
        Some(version) => Ok(EnvPointer {
            model: source.model.clone(),
            run_id,
            version: version.clone(),
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

    #[test]
    fn publish_scope_wire_shape_is_state_only() {
        assert_eq!(
            serde_json::to_string(&PublishScope::StateOnly).unwrap(),
            "\"state_only\""
        );
    }
}
