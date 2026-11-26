//! Cancel the warehouse statements a forced exit would leave running.
//!
//! `rocky run` drains in-flight statements on the first signal. A forced exit
//! stops waiting on them: a second signal, or the settle grace has passed
//! (`run_cancel`), or the `--watch` loop drops a stalled iteration
//! (`run_watch`). A warehouse keeps running a statement whose client went
//! away, so on those paths Rocky asks each adapter to cancel what it still
//! runs.
//!
//! A fan-out abort inside a run (`fail_fast`, the error-rate abort) also stops
//! waiting on statements: [`abort_fan_out`] cancels the ones the run's own
//! adapter lists, then aborts the tasks. A client deadline and a poll error
//! are handled inside the Databricks and Snowflake connectors, which cancel
//! before they return. Only those two adapters list statements; every other
//! adapter lists none, so nothing is cancelled and nothing is printed for it.
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

use rocky_core::statement_cancel::{ABANDON_CANCEL_BUDGET, CancelOutcome, StatementHandle};
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

/// Every statement the tracked adapters list now, with the adapter's name.
/// What a forced exit would try to cancel.
#[cfg(test)]
pub(crate) fn tracked_statements() -> Vec<(String, StatementHandle)> {
    TRACKED
        .live()
        .into_iter()
        .flat_map(|(name, adapter)| {
            adapter
                .in_flight_statements()
                .into_iter()
                .map(move |handle| (name.clone(), handle))
        })
        .collect()
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

/// How the process ends on a forced exit. A plain `fn`, so a test can pass
/// one that records the call instead of ending the test process.
pub(crate) type ExitProcess = fn(i32);

/// End the process with `code`.
pub(crate) fn exit_process(code: i32) {
    std::process::exit(code)
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
    let requests = adapters.iter().map(|(name, adapter)| {
        cancel_handles(
            name,
            adapter.as_ref(),
            adapter.in_flight_statements(),
            budget,
        )
    });
    futures::future::join_all(requests)
        .await
        .into_iter()
        .flatten()
        .collect()
}

/// Send one cancel request per handle through `adapter`, all at once, each
/// bounded by `budget`.
async fn cancel_handles(
    name: &str,
    adapter: &dyn WarehouseAdapter,
    handles: Vec<StatementHandle>,
    budget: Duration,
) -> Vec<StatementCancel> {
    let requests = handles.into_iter().map(|handle| async move {
        let result = match tokio::time::timeout(budget, adapter.cancel_statement(&handle)).await {
            Ok(Ok(CancelOutcome::Requested)) => CancelResult::Requested,
            Ok(Ok(CancelOutcome::Unsupported)) => CancelResult::Unsupported,
            Ok(Err(e)) => CancelResult::Failed(rocky_core::secret_registry::render_placeholders(
                &e.to_string(),
            )),
            Err(_) => CancelResult::TimedOut,
        };
        StatementCancel {
            adapter: name.to_string(),
            handle,
            result,
        }
    });
    futures::future::join_all(requests).await
}

/// What [`abort_fan_out`] stops: a `JoinSet` in production. A seam so a test
/// can make the abort drop the tasks' statement guards at once, the worst
/// case for the order of the steps.
pub(crate) trait FanOut {
    /// Abort every task.
    fn abort_all(&mut self);
}

impl<T: 'static> FanOut for tokio::task::JoinSet<T> {
    fn abort_all(&mut self) {
        tokio::task::JoinSet::abort_all(self);
    }
}

/// Stop a fan-out early: abort every task in `join_set`, and cancel the
/// statements they were waiting on through `adapter`, the run's own
/// warehouse adapter.
///
/// ```text
///   snapshot adapter.in_flight_statements() ──▶ abort_all() ──▶ cancel each
/// ```
///
/// The snapshot comes first, while the tasks still hold their statements
/// (an aborted task drops its guard, which unlists the handle). The abort
/// comes before the cancels, so no task submits a new statement while they
/// are sent. Only this run's adapter is swept, never the process-wide list:
/// a sibling `--dag` sub-run's statements are not this run's to cancel.
/// Each request is bounded by [`ABANDON_CANCEL_BUDGET`]; the run goes on
/// either way, so the outcome is logged, not printed.
pub(crate) async fn abort_fan_out(
    join_set: &mut impl FanOut,
    adapter_name: &str,
    adapter: &dyn WarehouseAdapter,
) -> Vec<StatementCancel> {
    let handles = adapter.in_flight_statements();
    join_set.abort_all();
    let results = cancel_handles(adapter_name, adapter, handles, ABANDON_CANCEL_BUDGET).await;
    log_abandon_cancels("fan-out abort", &results);
    results
}

/// Cancel the statements `adapter` lists now that `before` did not list:
/// the ones a step started since `before` was taken. Call it while that
/// step's future is still alive (dropping it unlists its handles), just
/// before the future is dropped. `step` names the step in the log.
///
/// Each request is bounded by [`ABANDON_CANCEL_BUDGET`]. The outcome is
/// logged, not printed.
pub(crate) async fn cancel_started_since(
    adapter_name: &str,
    adapter: &dyn WarehouseAdapter,
    before: &[StatementHandle],
    step: &str,
) -> Vec<StatementCancel> {
    let started: Vec<StatementHandle> = adapter
        .in_flight_statements()
        .into_iter()
        .filter(|handle| !before.contains(handle))
        .collect();
    let results = cancel_handles(adapter_name, adapter, started, ABANDON_CANCEL_BUDGET).await;
    log_abandon_cancels(step, &results);
    results
}

/// One `tracing` event per cancel sent for a statement Rocky stopped waiting
/// on. `step` names what stopped waiting.
fn log_abandon_cancels(step: &str, results: &[StatementCancel]) {
    for cancel in results {
        let StatementCancel {
            adapter,
            handle,
            result,
        } = cancel;
        match result {
            CancelResult::Requested => {
                tracing::warn!(adapter = %adapter, statement = %handle, "{step}: cancel requested");
            }
            CancelResult::Unsupported => {
                tracing::warn!(adapter = %adapter, statement = %handle, "{step}: adapter cannot cancel; statement may still run");
            }
            CancelResult::Failed(error) => {
                tracing::warn!(adapter = %adapter, statement = %handle, error = %error, "{step}: cancel failed; statement may still run");
            }
            CancelResult::TimedOut => {
                tracing::warn!(adapter = %adapter, statement = %handle, "{step}: cancel got no answer in time; statement may still run");
            }
        }
    }
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

    use crate::testing::{
        CancelAnswer as Answer, CancellableWarehouseAdapter as Cancellable,
        RecordingWarehouseAdapter,
    };

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

    /// #2768: a fan-out abort cancels the statement an aborted task was
    /// waiting on, and the task is aborted.
    #[tokio::test]
    async fn a_fan_out_abort_cancels_what_its_tasks_wait_on_and_aborts_them() {
        let adapter = Arc::new(Cancellable::new(Answer::Accept));
        let mut join_set = tokio::task::JoinSet::new();
        let (registered_tx, registered_rx) = tokio::sync::oneshot::channel();
        {
            let in_flight = adapter.in_flight.clone();
            join_set.spawn(async move {
                let _guard = in_flight.register(StatementHandle::new("copy-1"));
                let _ = registered_tx.send(());
                std::future::pending::<()>().await;
            });
        }
        registered_rx.await.unwrap();

        let results = abort_fan_out(&mut join_set, "wh", adapter.as_ref()).await;

        assert_eq!(*adapter.cancelled.lock().unwrap(), handles(&["copy-1"]));
        assert_eq!(
            results,
            vec![StatementCancel {
                adapter: "wh".into(),
                handle: StatementHandle::new("copy-1"),
                result: CancelResult::Requested,
            }]
        );
        let joined = join_set.join_next().await.expect("the task").unwrap_err();
        assert!(joined.is_cancelled(), "the task must be aborted");
        assert!(adapter.in_flight.snapshot().is_empty());
    }

    /// Every early stop of the replication fan-out in `run.rs` goes through
    /// [`abort_fan_out`]. A bare `abort_all` there would drop the copies and
    /// leave their statements running. A new site must use the helper too
    /// (and bump the count).
    #[test]
    fn every_fan_out_abort_in_run_rs_cancels_its_statements() {
        let run = include_str!("commands/run.rs");
        let bare = concat!("join_set", ".abort_all()");
        assert!(
            !run.contains(bare),
            "run.rs aborts a fan-out without cancelling its statements"
        );
        assert_eq!(
            run.matches("statement_cancel::abort_fan_out(").count(),
            4,
            "the four fan-out stops: fail_fast and error rate (inline drain), \
             fail_fast on a copy error and on a panic, error rate (final drain)"
        );
    }

    /// The snapshot must come before the abort. Here the abort drops every
    /// task's statement guard at once (a real `JoinSet` drops them a little
    /// later, which hides the order). Swap the two steps in
    /// [`abort_fan_out`] and the snapshot is empty, so nothing is cancelled.
    #[tokio::test]
    async fn a_fan_out_abort_lists_the_statements_before_it_aborts() {
        struct DropsGuardsOnAbort(Vec<rocky_core::statement_cancel::InFlightGuard>);
        impl FanOut for DropsGuardsOnAbort {
            fn abort_all(&mut self) {
                self.0.clear();
            }
        }
        let adapter = Cancellable::new(Answer::Accept);
        let mut fan_out = DropsGuardsOnAbort(vec![
            adapter.in_flight.register(StatementHandle::new("copy-1")),
            adapter.in_flight.register(StatementHandle::new("copy-2")),
        ]);

        let results = abort_fan_out(&mut fan_out, "wh", &adapter).await;

        assert!(fan_out.0.is_empty(), "the tasks are aborted");
        assert!(adapter.in_flight.snapshot().is_empty());
        assert_eq!(
            *adapter.cancelled.lock().unwrap(),
            handles(&["copy-1", "copy-2"]),
            "the statements listed before the abort are cancelled"
        );
        assert_eq!(results.len(), 2);
    }

    /// The step-scoped sweep cancels only what started after `before`.
    #[tokio::test]
    async fn a_step_sweep_cancels_only_what_the_step_started() {
        let adapter = Cancellable::new(Answer::Accept);
        let _earlier = adapter.in_flight.register(StatementHandle::new("earlier"));
        let before = adapter.in_flight_statements();
        let _count = adapter.in_flight.register(StatementHandle::new("count"));

        let results = cancel_started_since("wh", &adapter, &before, "test step").await;

        assert_eq!(*adapter.cancelled.lock().unwrap(), handles(&["count"]));
        assert_eq!(results.len(), 1);
    }

    /// The one `join_set.shutdown()` in `run.rs` is the interrupted settle
    /// after the final drain. It is safe without a cancel: the drain leaves
    /// that loop only when the set is empty (`join_next` returned `None`) or
    /// after an [`abort_fan_out`] already swept and aborted it. A new
    /// shutdown site could drop live copies, so it must be reviewed (and the
    /// count bumped) or go through the helper.
    #[test]
    fn the_one_fan_out_shutdown_in_run_rs_follows_a_drain_or_a_sweep() {
        let run = include_str!("commands/run.rs");
        let shutdown = concat!("join_set", ".shutdown()");
        assert_eq!(
            run.matches(shutdown).count(),
            1,
            "a new `JoinSet::shutdown` in run.rs drops its tasks without cancelling \
             their statements"
        );
    }

    /// A fan-out abort with nothing in flight sends nothing.
    #[tokio::test]
    async fn a_fan_out_abort_with_nothing_in_flight_sends_nothing() {
        let adapter = Cancellable::new(Answer::Accept);
        let mut join_set = tokio::task::JoinSet::<()>::new();
        assert!(
            abort_fan_out(&mut join_set, "wh", &adapter)
                .await
                .is_empty()
        );
        assert!(adapter.cancelled.lock().unwrap().is_empty());
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
