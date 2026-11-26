//! Cancel the warehouse statements a forced exit would leave running.
//!
//! `rocky run` drains in-flight statements on the first signal. It stops
//! waiting on them only on a forced exit: a second signal, or the settle grace
//! has passed (`run_cancel`), or the `--watch` loop drops a stalled iteration
//! (`run_watch`). A warehouse keeps running a statement whose client went
//! away, so on that path Rocky asks each adapter to cancel what it still runs.
//!
//! ```text
//!   AdapterRegistry::from_config ──▶ track(name, adapter)     (weak reference)
//!                                          │
//!   forced exit ──▶ cancel_before_exit ────┤
//!                     for each live adapter: in_flight_statements()
//!                     for each handle:       cancel_statement(handle)
//!                     all at once, each bounded by CANCEL_ON_EXIT_BUDGET
//!                                          │
//!                                          ▼
//!                            one line per statement, then exit
//! ```
//!
//! The list is process-wide on purpose: a forced exit ends every run in the
//! process, so every statement any of them still waits on is abandoned.

use std::sync::{Arc, LazyLock, Mutex, PoisonError, Weak};
use std::time::Duration;

use rocky_core::statement_cancel::{CancelOutcome, StatementHandle};
use rocky_core::traits::WarehouseAdapter;

/// How long one cancel request may take on a forced exit. The requests run at
/// once, so this also bounds the whole step. It is short because the operator
/// asked the process to stop.
pub(crate) const CANCEL_ON_EXIT_BUDGET: Duration = Duration::from_secs(3);

/// The warehouse adapters this process built, held weakly so a finished run
/// does not keep its adapters alive.
#[derive(Default)]
struct Tracked(Mutex<Vec<(String, Weak<dyn WarehouseAdapter>)>>);

impl Tracked {
    fn track(&self, name: &str, adapter: &Arc<dyn WarehouseAdapter>) {
        let mut tracked = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        tracked.retain(|(_, weak)| weak.strong_count() > 0);
        tracked.push((name.to_string(), Arc::downgrade(adapter)));
    }

    fn live(&self) -> Vec<(String, Arc<dyn WarehouseAdapter>)> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter_map(|(name, weak)| weak.upgrade().map(|adapter| (name.clone(), adapter)))
            .collect()
    }
}

static TRACKED: LazyLock<Tracked> = LazyLock::new(Tracked::default);

/// Make `adapter` reachable from a forced exit.
pub(crate) fn track(name: &str, adapter: &Arc<dyn WarehouseAdapter>) {
    TRACKED.track(name, adapter);
}

/// What became of one cancel request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CancelResult {
    /// The warehouse accepted the request. The statement may still have been
    /// finished already, or stop a little later.
    Requested,
    /// The adapter listed the statement but cannot cancel it.
    Unsupported,
    /// The request failed. The statement may still run.
    Failed(String),
    /// No answer within [`CANCEL_ON_EXIT_BUDGET`]. The statement may still run.
    TimedOut,
}

/// One in-flight statement and what its cancel request achieved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StatementCancel {
    pub(crate) adapter: String,
    pub(crate) handle: StatementHandle,
    pub(crate) result: CancelResult,
}

/// Cancel every statement a tracked adapter still waits on, print one line
/// for each, and return the results. Call it just before a forced exit, while
/// the run's futures are still alive: dropping them removes their handles.
pub(crate) async fn cancel_before_exit(prefix: &str) -> Vec<StatementCancel> {
    let results = cancel_all(&TRACKED.live(), CANCEL_ON_EXIT_BUDGET).await;
    for cancel in &results {
        report(prefix, cancel);
    }
    results
}

/// Send one cancel request per in-flight statement of `adapters`, all at once,
/// each bounded by `budget`.
async fn cancel_all(
    adapters: &[(String, Arc<dyn WarehouseAdapter>)],
    budget: Duration,
) -> Vec<StatementCancel> {
    let requests = adapters.iter().flat_map(|(name, adapter)| {
        adapter
            .in_flight_statements()
            .into_iter()
            .map(move |handle| async move {
                let result =
                    match tokio::time::timeout(budget, adapter.cancel_statement(&handle)).await {
                        Ok(Ok(CancelOutcome::Requested)) => CancelResult::Requested,
                        Ok(Ok(CancelOutcome::Unsupported)) => CancelResult::Unsupported,
                        Ok(Err(e)) => CancelResult::Failed(
                            rocky_core::secret_registry::render_placeholders(&e.to_string()),
                        ),
                        Err(_) => CancelResult::TimedOut,
                    };
                StatementCancel {
                    adapter: name.clone(),
                    handle,
                    result,
                }
            })
    });
    futures::future::join_all(requests).await
}

/// One stderr line and one `tracing` event per statement. Stderr because the
/// process exits next, before a log pipeline could flush.
fn report(prefix: &str, cancel: &StatementCancel) {
    let StatementCancel {
        adapter,
        handle,
        result,
    } = cancel;
    match result {
        CancelResult::Requested => {
            tracing::warn!(adapter = %adapter, statement = %handle, "cancel requested on forced exit");
            eprintln!("{prefix} cancel requested for statement {handle} on adapter '{adapter}'");
        }
        CancelResult::Unsupported => {
            tracing::warn!(adapter = %adapter, statement = %handle, "adapter cannot cancel; statement may still run");
            eprintln!(
                "{prefix} adapter '{adapter}' cannot cancel statement {handle}; it may still run"
            );
        }
        CancelResult::Failed(error) => {
            tracing::warn!(adapter = %adapter, statement = %handle, error = %error, "cancel failed; statement may still run");
            eprintln!(
                "{prefix} could not cancel statement {handle} on adapter '{adapter}': {error}; it \
                 may still run"
            );
        }
        CancelResult::TimedOut => {
            tracing::warn!(adapter = %adapter, statement = %handle, "cancel got no answer in time; statement may still run");
            eprintln!(
                "{prefix} no answer to the cancel of statement {handle} on adapter '{adapter}' \
                 within {}s; it may still run",
                CANCEL_ON_EXIT_BUDGET.as_secs()
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use async_trait::async_trait;
    use rocky_core::statement_cancel::InFlightStatements;
    use rocky_core::traits::{AdapterError, AdapterResult, QueryResult, SqlDialect};
    use rocky_ir::{ColumnInfo, TableRef};

    use crate::testing::RecordingWarehouseAdapter;

    #[derive(Clone, Copy)]
    enum Answer {
        Accept,
        Fail,
        Hang,
    }

    /// Lists what its [`InFlightStatements`] holds and records each cancel.
    struct Cancellable {
        inner: RecordingWarehouseAdapter,
        in_flight: InFlightStatements,
        answer: Answer,
        cancelled: Mutex<Vec<StatementHandle>>,
    }

    impl Cancellable {
        fn new(answer: Answer) -> Self {
            Self {
                inner: RecordingWarehouseAdapter::new(&format!(
                    "statement-cancel-{}",
                    uuid::Uuid::new_v4()
                )),
                in_flight: InFlightStatements::new(),
                answer,
                cancelled: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl WarehouseAdapter for Cancellable {
        fn dialect(&self) -> &dyn SqlDialect {
            self.inner.dialect()
        }
        async fn execute_statement(&self, sql: &str) -> AdapterResult<()> {
            self.inner.execute_statement(sql).await
        }
        async fn execute_query(&self, sql: &str) -> AdapterResult<QueryResult> {
            self.inner.execute_query(sql).await
        }
        async fn describe_table(&self, table: &TableRef) -> AdapterResult<Vec<ColumnInfo>> {
            self.inner.describe_table(table).await
        }
        fn in_flight_statements(&self) -> Vec<StatementHandle> {
            self.in_flight.snapshot()
        }
        async fn cancel_statement(&self, handle: &StatementHandle) -> AdapterResult<CancelOutcome> {
            self.cancelled.lock().unwrap().push(handle.clone());
            match self.answer {
                Answer::Accept => Ok(CancelOutcome::Requested),
                Answer::Fail => Err(AdapterError::msg("cancel refused")),
                Answer::Hang => std::future::pending().await,
            }
        }
    }

    fn handles(names: &[&str]) -> Vec<StatementHandle> {
        names.iter().map(|n| StatementHandle::new(*n)).collect()
    }

    #[tokio::test]
    async fn every_in_flight_statement_is_cancelled_through_its_own_adapter() {
        let a = Arc::new(Cancellable::new(Answer::Accept));
        let b = Arc::new(Cancellable::new(Answer::Accept));
        let _a1 = a.in_flight.register(StatementHandle::new("a-1"));
        let _a2 = a.in_flight.register(StatementHandle::new("a-2"));
        let _b1 = b.in_flight.register(StatementHandle::new("b-1"));
        let adapters: Vec<(String, Arc<dyn WarehouseAdapter>)> = vec![
            ("a".to_string(), a.clone() as Arc<dyn WarehouseAdapter>),
            ("b".to_string(), b.clone() as Arc<dyn WarehouseAdapter>),
        ];

        let results = cancel_all(&adapters, Duration::from_secs(1)).await;

        assert_eq!(*a.cancelled.lock().unwrap(), handles(&["a-1", "a-2"]));
        assert_eq!(*b.cancelled.lock().unwrap(), handles(&["b-1"]));
        let summary: Vec<(String, String, CancelResult)> = results
            .into_iter()
            .map(|c| (c.adapter, c.handle.to_string(), c.result))
            .collect();
        assert_eq!(
            summary,
            vec![
                ("a".into(), "a-1".into(), CancelResult::Requested),
                ("a".into(), "a-2".into(), CancelResult::Requested),
                ("b".into(), "b-1".into(), CancelResult::Requested),
            ]
        );
    }

    #[tokio::test]
    async fn a_cancel_that_hangs_is_cut_at_the_budget_and_does_not_hold_the_others() {
        let hangs = Arc::new(Cancellable::new(Answer::Hang));
        let fails = Arc::new(Cancellable::new(Answer::Fail));
        let _h = hangs.in_flight.register(StatementHandle::new("h-1"));
        let _f = fails.in_flight.register(StatementHandle::new("f-1"));
        let adapters: Vec<(String, Arc<dyn WarehouseAdapter>)> = vec![
            ("hangs".to_string(), hangs as Arc<dyn WarehouseAdapter>),
            ("fails".to_string(), fails as Arc<dyn WarehouseAdapter>),
        ];

        let started = std::time::Instant::now();
        let results = cancel_all(&adapters, Duration::from_millis(100)).await;

        assert!(
            started.elapsed() < Duration::from_secs(2),
            "the budget must bound the step"
        );
        let by_result: Vec<CancelResult> = results.into_iter().map(|c| c.result).collect();
        assert_eq!(
            by_result,
            vec![
                CancelResult::TimedOut,
                CancelResult::Failed("cancel refused".into())
            ]
        );
    }

    #[tokio::test]
    async fn the_trait_default_lists_nothing_and_cancels_nothing() {
        let adapter = RecordingWarehouseAdapter::new(&format!(
            "statement-cancel-default-{}",
            uuid::Uuid::new_v4()
        ));
        assert!(adapter.in_flight_statements().is_empty());
        assert_eq!(
            adapter
                .cancel_statement(&StatementHandle::new("any"))
                .await
                .unwrap(),
            CancelOutcome::Unsupported
        );
        let adapters: Vec<(String, Arc<dyn WarehouseAdapter>)> =
            vec![("rec".to_string(), Arc::new(adapter))];
        assert!(
            cancel_all(&adapters, Duration::from_secs(1))
                .await
                .is_empty()
        );
    }

    #[test]
    fn a_dropped_adapter_is_no_longer_tracked() {
        let tracked = Tracked::default();
        let kept: Arc<dyn WarehouseAdapter> = Arc::new(Cancellable::new(Answer::Accept));
        let dropped: Arc<dyn WarehouseAdapter> = Arc::new(Cancellable::new(Answer::Accept));
        tracked.track("kept", &kept);
        tracked.track("dropped", &dropped);
        drop(dropped);
        let names: Vec<String> = tracked.live().into_iter().map(|(n, _)| n).collect();
        assert_eq!(names, ["kept"]);
    }
}
