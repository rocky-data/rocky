//! Table publish (RV1-P3, experimental): move an environment's tables to the
//! output versions its pointers name, one table commit at a time.
//!
//! A table publish is **not atomic across tables**. Each table moves in its
//! own commit (one Delta commit per table on Databricks). Readers can see
//! some tables at the new version and others at the old one, until every
//! move lands. Rocky reports which tables moved; it never claims an
//! environment-wide switch.
//!
//! ```text
//!   begin  (CAS on the head)  env#N   Started { planned }    publishing = env#N
//!     │ head moved ──▶ PublishConflict, no table touched
//!     ▼
//!   move table 1 ──▶ move table 2 ──▶ ... (stop at the first failure)
//!     ▼
//!   finish (CAS on env#N)     env#N+1 Finished { moves }     publishing = None
//! ```
//!
//! The begin step claims the environment before any table moves, so two
//! publishers cannot both move tables: the loser's begin is a conflict.
//! While a publish is in progress, every other publish to the environment
//! is refused ([`EnvironmentError::PublishInProgress`]).
//!
//! Limits, stated so nobody relies on them:
//!
//! - A crash between two moves leaves the environment `publishing` and the
//!   history with a `Started` row and no `Finished` row. The tables that
//!   moved are not recorded. Recovery is a new publish with take-over, which
//!   moves every table again (a table already at its version writes no
//!   commit).
//! - A run that writes a table while a publish moves it is not serialized
//!   with the publish. The later commit wins.
//! - On Delta, a publish moves the model's own table. Two environments that
//!   both hold a model move the same table, and their compare-and-swap
//!   tokens are separate. Rocky does not check this yet.
//! - A pointer pins no data: `VACUUM` can remove the files of a version an
//!   environment points to.
//!
//! [`EnvironmentError::PublishInProgress`]: crate::environments::EnvironmentError::PublishInProgress

use thiserror::Error;

use crate::environments::{EnvPointer, PublishRecord, PublishRequest, TableMove, TableMoveOutcome};
use crate::state_sync::{self, LedgerSeamSession, StateSyncError};

/// A table that a backend moved, or found already at the version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableMoved {
    /// One commit moved the table, and its follow-up steps ran.
    Moved {
        /// The table name.
        table: String,
        /// The commit that made the table serve the version.
        table_version: u64,
    },
    /// The table already served the version. No commit was written. Its
    /// follow-up steps ran again.
    AlreadyCurrent {
        /// The table name.
        table: String,
        /// The current table version.
        table_version: u64,
    },
    /// The table serves the version, but a follow-up step (the Iceberg
    /// metadata sync) failed.
    SyncFailed {
        /// The table name.
        table: String,
        /// The table version that serves the version.
        table_version: u64,
        /// Whether this move wrote the commit.
        committed: bool,
        /// Why the follow-up step failed.
        error: String,
    },
}

impl From<TableMoved> for TableMoveOutcome {
    fn from(m: TableMoved) -> Self {
        match m {
            TableMoved::Moved {
                table,
                table_version,
            } => Self::Moved {
                table,
                table_version,
            },
            TableMoved::AlreadyCurrent {
                table,
                table_version,
            } => Self::AlreadyCurrent {
                table,
                table_version,
            },
            TableMoved::SyncFailed {
                table,
                table_version,
                committed,
                error,
            } => Self::SyncFailed {
                table,
                table_version,
                committed,
                error,
            },
        }
    }
}

/// The warehouse side of a table publish: makes one model's table serve the
/// version a pointer names, in at most one commit.
#[async_trait::async_trait]
pub trait TablePointerBackend: Send + Sync {
    /// Whether this backend can move `pointer`'s table. Runs inside the
    /// begin transaction, before anything is written; an `Err` refuses the
    /// whole publish with no table touched.
    ///
    /// It checks the version's shape (its kind, its files, a configured
    /// table), not whether the files still exist. A version whose files a
    /// `VACUUM` removed fails in [`Self::move_table`], after earlier tables
    /// may have moved.
    ///
    /// # Errors
    ///
    /// A reason the version cannot be published by this backend.
    fn check(&self, pointer: &EnvPointer) -> Result<(), String>;

    /// Make `pointer.model`'s table serve `pointer.version`, in at most one
    /// commit.
    ///
    /// # Errors
    ///
    /// [`TableMoveError::NotMoved`] when the backend wrote no commit;
    /// [`TableMoveError::Unknown`] when the commit write failed in a way
    /// that does not say whether the commit was stored.
    async fn move_table(&self, pointer: &EnvPointer) -> Result<TableMoved, TableMoveError>;
}

/// Why a [`TablePointerBackend::move_table`] call did not report a move.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TableMoveError {
    /// No commit was written. The table did not move.
    #[error("{0}")]
    NotMoved(String),
    /// The commit write failed, but the commit may have been stored (for
    /// example a timeout after the request was sent).
    #[error("{0}")]
    Unknown(String),
}

impl From<TableMoveError> for TableMoveOutcome {
    fn from(e: TableMoveError) -> Self {
        match e {
            TableMoveError::NotMoved(error) => Self::Failed { error },
            TableMoveError::Unknown(error) => Self::Unknown { error },
        }
    }
}

/// The result of a table publish that recorded its outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TablePublishReport {
    /// The `Started` history row.
    pub started: PublishRecord,
    /// The `Finished` history row.
    pub finished: PublishRecord,
}

impl TablePublishReport {
    /// What happened to each table, in commit order.
    #[must_use]
    pub fn moves(&self) -> &[TableMove] {
        match &self.finished.tables {
            Some(crate::environments::TablePublish::Finished { moves, .. }) => moves,
            Some(crate::environments::TablePublish::Started { .. }) | None => &[],
        }
    }

    /// Whether every planned table now serves its version to every reader.
    /// `false` means a partial publish: readers see a mix of old and new
    /// tables, or Iceberg readers miss a table whose sync failed.
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.moves().iter().all(|m| m.outcome.is_fully_served())
    }
}

/// Why a table publish did not record an outcome.
#[derive(Debug, Error)]
pub enum TablePublishError {
    /// The begin step failed. No table was touched.
    #[error("table publish refused before any table moved: {0}")]
    Begin(#[source] StateSyncError),
    /// Tables may have moved, but the outcome could not be recorded. The
    /// environment stays `publishing`; `moves` is the only record of what
    /// happened.
    #[error(
        "table publish {started} ran, but its outcome could not be recorded: {source}. The \
         environment stays marked as publishing. Moves: {moves:?}"
    )]
    RecordFailed {
        /// The `Started` publish id.
        started: String,
        /// What happened to each table.
        moves: Vec<TableMove>,
        /// The state error.
        #[source]
        source: StateSyncError,
    },
}

/// Publish an environment's tables (RV1-P3, experimental).
///
/// Claims the environment with a CAS on `request.expected_head`, moves each
/// table in model-name order through `backend`, stops at the first failed
/// move, and records the outcome. A partial publish returns `Ok` with
/// [`TablePublishReport::is_complete`] `false`; the history names every
/// moved, failed and untried table.
///
/// `take_over` starts from a head whose own table publish never finished.
/// See [`crate::state::StateStore::begin_table_publish`].
///
/// # Errors
///
/// [`TablePublishError::Begin`] when the begin step is refused (a head
/// conflict, a publish in progress, a refused pointer); no table moved.
/// [`TablePublishError::RecordFailed`] when the outcome could not be written.
pub async fn publish_tables(
    session: &LedgerSeamSession,
    request: &PublishRequest,
    backend: &dyn TablePointerBackend,
    take_over: bool,
) -> Result<TablePublishReport, TablePublishError> {
    let check = |p: &EnvPointer| backend.check(p);
    let started = state_sync::begin_table_publish(session, request, take_over, &check)
        .await
        .map_err(TablePublishError::Begin)?;

    let planned: Vec<&EnvPointer> = started.to.values().collect();
    let mut moves = Vec::with_capacity(planned.len());
    let mut failed = false;
    for pointer in planned {
        let outcome = if failed {
            TableMoveOutcome::NotAttempted
        } else {
            match backend.move_table(pointer).await {
                Ok(moved) => moved.into(),
                Err(error) => {
                    failed = true;
                    tracing::warn!(
                        environment = %request.environment,
                        model = %pointer.model,
                        %error,
                        "table publish: a table did not move (or its commit is unknown); \
                         later tables are not tried"
                    );
                    error.into()
                }
            }
        };
        moves.push(TableMove {
            model: pointer.model.clone(),
            outcome,
        });
    }

    let finished = state_sync::finish_table_publish(
        session,
        &request.environment,
        &started.publish_id,
        &moves,
        &request.principal,
    )
    .await
    .map_err(|source| TablePublishError::RecordFailed {
        started: started.publish_id.clone(),
        moves: moves.clone(),
        source,
    })?;
    Ok(TablePublishReport { started, finished })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    use tempfile::TempDir;

    use super::*;
    use crate::config::{ConcurrencyControl, PrincipalRef, StateConfig};
    use crate::environments::{
        EnvironmentError, EnvironmentName, PublishRefusal, PublishScope, PublishSource,
        TablePublish,
    };
    use crate::state::{OutputVersion, StateError, StateStore, run_with_output_versions};

    /// An in-memory stand-in for Delta tables: each table serves one version.
    #[derive(Default)]
    struct FakeTables {
        served: Mutex<BTreeMap<String, u64>>,
        calls: Mutex<Vec<String>>,
        fail_on: Option<String>,
        unknown_on: Option<String>,
        sync_fail_on: Option<String>,
        refuse: Option<String>,
    }

    impl FakeTables {
        fn failing_on(model: &str) -> Self {
            Self {
                fail_on: Some(model.to_string()),
                ..Self::default()
            }
        }
        fn calls(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }
        fn served(&self) -> BTreeMap<String, u64> {
            self.served.lock().unwrap().clone()
        }
    }

    fn table_and_version(p: &EnvPointer) -> (String, u64) {
        match p.version.known() {
            Some(OutputVersion::DeltaObserved { table, version }) => (table.clone(), *version),
            other => panic!("the fake serves delta_observed only, got {other:?}"),
        }
    }

    #[async_trait::async_trait]
    impl TablePointerBackend for FakeTables {
        fn check(&self, pointer: &EnvPointer) -> Result<(), String> {
            match &self.refuse {
                Some(m) if *m == pointer.model => Err("refused by the fake".into()),
                _ => Ok(()),
            }
        }
        async fn move_table(&self, pointer: &EnvPointer) -> Result<TableMoved, TableMoveError> {
            self.calls.lock().unwrap().push(pointer.model.clone());
            if self.fail_on.as_deref() == Some(pointer.model.as_str()) {
                return Err(TableMoveError::NotMoved("injected failure".into()));
            }
            if self.unknown_on.as_deref() == Some(pointer.model.as_str()) {
                // The commit lands, but the write reports an error.
                let (table, version) = table_and_version(pointer);
                self.served.lock().unwrap().insert(table, version);
                return Err(TableMoveError::Unknown("timed out after sending".into()));
            }
            let (table, version) = table_and_version(pointer);
            let mut served = self.served.lock().unwrap();
            if served.get(&table) == Some(&version) {
                return Ok(TableMoved::AlreadyCurrent {
                    table,
                    table_version: version,
                });
            }
            served.insert(table.clone(), version);
            if self.sync_fail_on.as_deref() == Some(pointer.model.as_str()) {
                return Ok(TableMoved::SyncFailed {
                    table,
                    table_version: version,
                    committed: true,
                    error: "sync failed".into(),
                });
            }
            Ok(TableMoved::Moved {
                table,
                table_version: version,
            })
        }
    }

    fn delta(model: &str, v: u64) -> (String, Option<OutputVersion>) {
        (
            model.to_string(),
            Some(OutputVersion::DeltaObserved {
                table: format!("c.s.{model}"),
                version: v,
            }),
        )
    }

    fn seed(store: &StateStore) {
        let r1: Vec<_> = ["a", "b", "c"].iter().map(|m| delta(m, 1)).collect();
        let r1: Vec<(&str, Option<OutputVersion>)> =
            r1.iter().map(|(m, v)| (m.as_str(), v.clone())).collect();
        store
            .record_run(&run_with_output_versions("r1", &r1))
            .unwrap();
        let r2 = delta("a", 2);
        store
            .record_run(&run_with_output_versions("r2", &[(r2.0.as_str(), r2.1)]))
            .unwrap();
    }

    fn env() -> EnvironmentName {
        EnvironmentName::parse("prod").unwrap()
    }

    fn req(expected: Option<&str>, sources: &[(&str, &str)]) -> PublishRequest {
        PublishRequest {
            environment: env(),
            expected_head: expected.map(str::to_string),
            sources: sources
                .iter()
                .map(|(model, run_id)| PublishSource {
                    model: (*model).to_string(),
                    run_id: (*run_id).to_string(),
                })
                .collect(),
            principal: PrincipalRef::unnamed(),
            plan_id: None,
        }
    }

    fn local(dir: &TempDir) -> (LedgerSeamSession, std::path::PathBuf) {
        let path = dir.path().join(".rocky-state.redb");
        seed(&StateStore::open(&path).unwrap());
        (
            LedgerSeamSession::new(&StateConfig::default(), &path, false),
            path,
        )
    }

    const ABC: &[(&str, &str)] = &[("a", "r1"), ("b", "r1"), ("c", "r1")];

    /// Every table moves; history has a Started and a Finished row; the head
    /// holds every pointer and is no longer publishing.
    #[tokio::test]
    async fn a_table_publish_moves_every_table_and_records_it() {
        let dir = TempDir::new().unwrap();
        let (session, path) = local(&dir);
        let tables = FakeTables::default();
        let report = publish_tables(&session, &req(None, ABC), &tables, false)
            .await
            .unwrap();

        assert!(report.is_complete());
        assert_eq!(report.started.publish_id, "prod#1");
        assert_eq!(report.finished.publish_id, "prod#2");
        assert_eq!(report.finished.scope, PublishScope::DeltaPerTable);
        assert_eq!(tables.calls(), vec!["a", "b", "c"]);
        assert_eq!(
            tables.served(),
            BTreeMap::from([
                ("c.s.a".to_string(), 1),
                ("c.s.b".to_string(), 1),
                ("c.s.c".to_string(), 1)
            ])
        );
        let store = StateStore::open(&path).unwrap();
        let head = store.get_environment(&env()).unwrap().unwrap();
        assert_eq!(head.head_publish_id, "prod#2");
        assert_eq!(head.publishing, None);
        assert_eq!(
            head.pointers.keys().collect::<Vec<_>>(),
            vec!["a", "b", "c"]
        );
        let history = store.publish_history(&env()).unwrap();
        assert_eq!(history.len(), 2);
        assert!(matches!(
            &history[0].tables,
            Some(TablePublish::Started { planned }) if planned == &["a", "b", "c"]
        ));
        assert_eq!(history[1], report.finished);

        // Publishing the same versions again writes no table commit.
        drop(store);
        let again = publish_tables(&session, &req(Some("prod#2"), ABC), &tables, false)
            .await
            .unwrap();
        assert!(again.is_complete());
        assert!(
            again
                .moves()
                .iter()
                .all(|m| matches!(m.outcome, TableMoveOutcome::AlreadyCurrent { .. }))
        );
    }

    /// A failure mid-way: `a` moved, `b` failed, `c` was not tried. The
    /// history says exactly that, and the head's pointers move for `a` only.
    #[tokio::test]
    async fn a_failure_mid_way_records_which_tables_moved() {
        let dir = TempDir::new().unwrap();
        let (session, path) = local(&dir);
        let tables = FakeTables::failing_on("b");
        let report = publish_tables(&session, &req(None, ABC), &tables, false)
            .await
            .unwrap();

        assert!(!report.is_complete());
        let outcomes: Vec<(&str, &TableMoveOutcome)> = report
            .moves()
            .iter()
            .map(|m| (m.model.as_str(), &m.outcome))
            .collect();
        assert!(matches!(
            outcomes[0],
            (
                "a",
                TableMoveOutcome::Moved {
                    table_version: 1,
                    ..
                }
            )
        ));
        assert!(
            matches!(outcomes[1], ("b", TableMoveOutcome::Failed { error }) if error == "injected failure")
        );
        assert!(matches!(outcomes[2], ("c", TableMoveOutcome::NotAttempted)));
        assert_eq!(tables.calls(), vec!["a", "b"], "c is never tried");
        assert_eq!(tables.served(), BTreeMap::from([("c.s.a".to_string(), 1)]));

        let store = StateStore::open(&path).unwrap();
        let head = store.get_environment(&env()).unwrap().unwrap();
        assert_eq!(head.publishing, None);
        assert_eq!(head.pointers.keys().collect::<Vec<_>>(), vec!["a"]);
        let history = store.publish_history(&env()).unwrap();
        assert_eq!(history[1].to.keys().collect::<Vec<_>>(), vec!["a"]);
        assert_eq!(history[1].from, BTreeMap::new());
        assert!(matches!(
            &history[1].tables,
            Some(TablePublish::Finished { started, moves }) if started == "prod#1" && moves.len() == 3
        ));
    }

    /// A commit write that errors after it may have landed is recorded
    /// `unknown`, not `failed`, and stops the publish. The pointer does not
    /// move. A retry finds the table already current.
    #[tokio::test]
    async fn an_ambiguous_commit_error_is_recorded_unknown_and_a_retry_reconciles_it() {
        let dir = TempDir::new().unwrap();
        let (session, path) = local(&dir);
        let tables = FakeTables {
            unknown_on: Some("a".into()),
            ..FakeTables::default()
        };
        let report = publish_tables(&session, &req(None, ABC), &tables, false)
            .await
            .unwrap();
        assert!(!report.is_complete());
        assert!(
            matches!(&report.moves()[0].outcome, TableMoveOutcome::Unknown { error } if error.contains("timed out")),
            "{:?}",
            report.moves()
        );
        assert!(matches!(
            report.moves()[1].outcome,
            TableMoveOutcome::NotAttempted
        ));
        let store = StateStore::open(&path).unwrap();
        let head = store.get_environment(&env()).unwrap().unwrap();
        assert!(head.pointers.is_empty(), "an unknown move moves no pointer");
        drop(store);

        let retry_tables = FakeTables {
            served: Mutex::new(tables.served()),
            ..FakeTables::default()
        };
        let retry = publish_tables(&session, &req(Some("prod#2"), ABC), &retry_tables, false)
            .await
            .unwrap();
        assert!(retry.is_complete());
        assert!(matches!(
            retry.moves()[0].outcome,
            TableMoveOutcome::AlreadyCurrent { .. }
        ));
    }

    /// A failed Iceberg sync is not a complete publish, but the Delta table
    /// moved: its pointer moves and later tables are still tried.
    #[tokio::test]
    async fn a_sync_failure_moves_the_pointer_but_the_publish_is_not_complete() {
        let dir = TempDir::new().unwrap();
        let (session, path) = local(&dir);
        let tables = FakeTables {
            sync_fail_on: Some("a".into()),
            ..FakeTables::default()
        };
        let report = publish_tables(&session, &req(None, ABC), &tables, false)
            .await
            .unwrap();
        assert!(!report.is_complete(), "history must not read as served");
        assert!(matches!(
            report.moves()[0].outcome,
            TableMoveOutcome::SyncFailed { .. }
        ));
        assert_eq!(tables.calls(), vec!["a", "b", "c"]);
        let store = StateStore::open(&path).unwrap();
        let head = store.get_environment(&env()).unwrap().unwrap();
        assert_eq!(
            head.pointers.keys().collect::<Vec<_>>(),
            vec!["a", "b", "c"]
        );
    }

    /// A backend refusal at begin refuses the whole publish: nothing is
    /// written and no table is touched.
    #[tokio::test]
    async fn a_backend_refusal_writes_nothing_and_moves_nothing() {
        let dir = TempDir::new().unwrap();
        let (session, path) = local(&dir);
        let tables = FakeTables {
            refuse: Some("c".into()),
            ..FakeTables::default()
        };
        let err = publish_tables(&session, &req(None, ABC), &tables, false)
            .await
            .unwrap_err();
        assert!(
            matches!(
                &err,
                TablePublishError::Begin(StateSyncError::State(StateError::Environment(
                    EnvironmentError::Refused {
                        model,
                        reason: PublishRefusal::BackendRefused { .. },
                        ..
                    }
                ))) if model == "c"
            ),
            "{err:?}"
        );
        assert!(tables.calls().is_empty());
        let store = StateStore::open(&path).unwrap();
        assert!(store.get_environment(&env()).unwrap().is_none());
        assert!(store.publish_history(&env()).unwrap().is_empty());
    }

    /// A publish whose process died after begin leaves the environment
    /// publishing. Every other publish is refused, a state-only one too,
    /// until a take-over from that head. The take-over moves the tables.
    #[tokio::test]
    async fn an_unfinished_publish_blocks_others_until_a_take_over() {
        let dir = TempDir::new().unwrap();
        let (session, path) = local(&dir);
        // The "dead" publisher: begin only.
        let started =
            state_sync::begin_table_publish(&session, &req(None, ABC), false, &|_| Ok(()))
                .await
                .unwrap();
        assert_eq!(started.publish_id, "prod#1");

        let tables = FakeTables::default();
        let err = publish_tables(&session, &req(Some("prod#1"), ABC), &tables, false)
            .await
            .unwrap_err();
        assert!(
            matches!(
                &err,
                TablePublishError::Begin(StateSyncError::State(StateError::Environment(
                    EnvironmentError::PublishInProgress { publish_id, .. }
                ))) if publish_id == "prod#1"
            ),
            "{err:?}"
        );
        let state_only =
            state_sync::publish_pointers(&session, &req(Some("prod#1"), &[("a", "r2")]))
                .await
                .unwrap_err();
        assert!(
            matches!(
                &state_only,
                StateSyncError::State(StateError::Environment(
                    EnvironmentError::PublishInProgress { .. }
                ))
            ),
            "{state_only:?}"
        );
        assert!(tables.calls().is_empty());

        let report = publish_tables(&session, &req(Some("prod#1"), ABC), &tables, true)
            .await
            .unwrap();
        assert!(report.is_complete());
        assert_eq!(report.started.prior_publish_id.as_deref(), Some("prod#1"));
        let store = StateStore::open(&path).unwrap();
        let ids: Vec<String> = store
            .publish_history(&env())
            .unwrap()
            .into_iter()
            .map(|r| r.publish_id)
            .collect();
        assert_eq!(ids, vec!["prod#1", "prod#2", "prod#3"]);
        assert_eq!(
            store.get_environment(&env()).unwrap().unwrap().publishing,
            None
        );
    }

    /// A backend that, during its first move, lets a second publisher take
    /// over the environment (as if it wrongly judged the first one dead).
    struct TakeOverDuringMove {
        session: LedgerSeamSession,
        tables: FakeTables,
    }

    #[async_trait::async_trait]
    impl TablePointerBackend for TakeOverDuringMove {
        fn check(&self, _pointer: &EnvPointer) -> Result<(), String> {
            Ok(())
        }
        async fn move_table(&self, pointer: &EnvPointer) -> Result<TableMoved, TableMoveError> {
            if self.tables.calls().is_empty() {
                state_sync::begin_table_publish(
                    &self.session,
                    &req(Some("prod#1"), &[("a", "r2")]),
                    true,
                    &|_| Ok(()),
                )
                .await
                .unwrap();
            }
            self.tables.move_table(pointer).await
        }
    }

    /// The outcome cannot be recorded when another publish took over in
    /// between. The error carries the moves, the only record of what moved,
    /// and the history gains no row for them.
    #[tokio::test]
    async fn a_take_over_mid_publish_makes_the_record_fail_with_the_moves() {
        let dir = TempDir::new().unwrap();
        let (session, path) = local(&dir);
        let backend = TakeOverDuringMove {
            session: LedgerSeamSession::new(&StateConfig::default(), &path, false),
            tables: FakeTables::default(),
        };
        let err = publish_tables(&session, &req(None, ABC), &backend, false)
            .await
            .unwrap_err();
        let TablePublishError::RecordFailed {
            started,
            moves,
            source,
        } = &err
        else {
            panic!("expected RecordFailed, got {err:?}");
        };
        assert_eq!(started, "prod#1");
        assert_eq!(moves.len(), 3);
        assert!(moves.iter().all(|m| m.outcome.serves_version()));
        assert!(
            matches!(source, StateSyncError::PublishConflict { found: Some(f), .. } if f == "prod#2"),
            "{source:?}"
        );
        let store = StateStore::open(&path).unwrap();
        let ids: Vec<String> = store
            .publish_history(&env())
            .unwrap()
            .into_iter()
            .map(|r| r.publish_id)
            .collect();
        assert_eq!(ids, vec!["prod#1", "prod#2"], "no finished row for prod#1");
        assert_eq!(
            store
                .get_environment(&env())
                .unwrap()
                .unwrap()
                .publishing
                .as_deref(),
            Some("prod#2")
        );
    }

    /// Two pods publish from the same head at once over remote state with
    /// `cas`. Exactly one wins. The loser's begin is a `PublishConflict`
    /// and its backend never moves a table, so no table move is lost or
    /// overwritten by the loser.
    #[tokio::test]
    async fn two_concurrent_table_publishes_one_wins_the_loser_moves_nothing() {
        use crate::state_sync::{FinalizeDurability, RemoteStateSession};
        let _serial = state_sync::remote_testing::serial_guard();
        let mut h = crate::test_harness::CrossPodHarness::new_s3_like();
        h.pod_a.cfg.concurrency_control = Some(ConcurrencyControl::Cas);
        h.pod_b.cfg.concurrency_control = Some(ConcurrencyControl::Cas);
        drop(StateStore::open(&h.pod_a.state_path).unwrap());
        let mut seed_session = RemoteStateSession::new(
            &h.pod_a.cfg,
            &h.pod_a.state_path,
            FinalizeDurability::Durable,
            false,
        );
        let _ = seed_session.acquire().await.unwrap();
        seed(&h.open_store(&h.pod_a));
        seed_session.finalize().await.unwrap();

        let sa = LedgerSeamSession::new(&h.pod_a.cfg, &h.pod_a.state_path, false);
        let sb = LedgerSeamSession::new(&h.pod_b.cfg, &h.pod_b.state_path, false);
        let (ta, tb) = (FakeTables::default(), FakeTables::default());
        let (qa, qb) = (req(None, &[("a", "r1")]), req(None, &[("a", "r2")]));
        let (ra, rb) = tokio::join!(
            publish_tables(&sa, &qa, &ta, false),
            publish_tables(&sb, &qb, &tb, false),
        );
        let (winner, loser_err, loser_tables) = match (ra, rb) {
            (Ok(w), Err(e)) => (w, e, &tb),
            (Err(e), Ok(w)) => (w, e, &ta),
            other => panic!("exactly one publish must win: {other:?}"),
        };
        assert!(
            matches!(
                &loser_err,
                TablePublishError::Begin(StateSyncError::PublishConflict { found: Some(_), .. })
            ),
            "{loser_err:?}"
        );
        assert!(loser_tables.calls().is_empty(), "the loser moved a table");
        assert!(winner.is_complete());

        // The remote holds the winner's two rows and nothing of the loser.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(".rocky-state.redb");
        let _ = state_sync::download_state(&h.pod_a.cfg, &path, false)
            .await
            .unwrap();
        let store = StateStore::open(&path).unwrap();
        let history = store.publish_history(&env()).unwrap();
        assert_eq!(
            history,
            vec![winner.started.clone(), winner.finished.clone()]
        );
        let head = store.get_environment(&env()).unwrap().unwrap();
        assert_eq!(head.pointers["a"], winner.finished.to["a"]);
    }
}
