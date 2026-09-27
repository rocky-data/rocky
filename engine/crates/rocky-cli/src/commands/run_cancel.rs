//! Cooperative cancellation for one `rocky run` (#1606).
//!
//! Dropping a whole-run future stops the warehouse work and the state tail at
//! once, so it cannot express "stop starting work, then settle the state". A
//! [`RunCancel`] carries that request instead:
//!
//! ```text
//!   cancel ──▶ no new table / layer / model starts
//!          ──▶ in-flight warehouse work gets IN_FLIGHT_GRACE to finish,
//!              then the run stops waiting for it (crash-parity for it alone)
//!          ──▶ the state tail settles in full (watermarks, checkpoints,
//!              run record, RemoteStateSession) ──▶ run returns `Interrupted`
//! ```
//!
//! The token reaches the run through a task-local scope, not a parameter:
//! [`RunCancel::scope`] wraps the run future, and every other caller of
//! [`super::run::run`] runs outside any scope, where the token never fires.
//! A parameter would touch every one of `run()`'s and `execute_models()`'s
//! ~70 call sites to pass a token that is never cancelled.
//!
//! Only `rocky run --watch` opens a scope today. The run checks the token at
//! "before new warehouse work" points and never inside the state tail.

use std::future::Future;
use std::time::Duration;

use tokio::sync::watch;

/// How long in-flight warehouse work may keep running after a cancel.
///
/// After this the run stops waiting for it and settles its state. For DuckDB
/// the abandoned statement keeps its blocking-pool thread, and `main`'s bounded
/// runtime shutdown is then the only thing it can delay. That bound now cuts
/// only a wedged warehouse statement, never a state commit: the run awaited
/// every state write before it returned.
pub(crate) const IN_FLIGHT_GRACE: Duration = Duration::from_secs(5);

tokio::task_local! {
    static RUN_CANCEL: RunCancel;
}

/// The run-side view of a cancel request.
#[derive(Debug, Clone)]
pub(crate) struct RunCancel {
    rx: watch::Receiver<bool>,
}

/// The side that requests the cancel. Held by the caller that owns the run.
#[derive(Debug)]
pub(crate) struct RunCancelTrigger {
    tx: watch::Sender<bool>,
}

impl RunCancel {
    /// A connected trigger and token, not yet cancelled.
    pub(crate) fn new() -> (RunCancelTrigger, RunCancel) {
        let (tx, rx) = watch::channel(false);
        (RunCancelTrigger { tx }, RunCancel { rx })
    }

    /// Run `fut` with this token as the current run's cancel.
    pub(crate) async fn scope<F: Future>(self, fut: F) -> F::Output {
        RUN_CANCEL.scope(self, fut).await
    }

    fn is_cancelled(&self) -> bool {
        *self.rx.borrow()
    }

    async fn cancelled(&self) {
        let mut rx = self.rx.clone();
        // `Err` means the trigger is gone without a cancel: it can never fire.
        if rx.wait_for(|cancelled| *cancelled).await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}

impl RunCancelTrigger {
    /// Ask the run to stop. A second call changes nothing.
    pub(crate) fn cancel(&self) {
        self.tx.send_replace(true);
    }
}

/// Whether the current run was asked to stop. `false` outside any scope.
pub(crate) fn run_cancelled() -> bool {
    RUN_CANCEL
        .try_with(RunCancel::is_cancelled)
        .unwrap_or(false)
}

/// Resolves when the current run is asked to stop. Never resolves outside a
/// scope.
pub(crate) async fn run_cancel_requested() {
    match RUN_CANCEL.try_with(Clone::clone) {
        Ok(cancel) => cancel.cancelled().await,
        Err(_) => std::future::pending().await,
    }
}

/// Resolves [`IN_FLIGHT_GRACE`] after the current run is asked to stop.
pub(crate) async fn in_flight_grace_elapsed() {
    grace_elapsed(IN_FLIGHT_GRACE).await;
}

async fn grace_elapsed(grace: Duration) {
    run_cancel_requested().await;
    tokio::time::sleep(grace).await;
}

/// Await `work`. After a cancel, stop waiting for it once the grace elapses.
///
/// `None` means `work` was dropped unfinished. Use this only around warehouse
/// work, never around a state write: dropping the future is crash-parity for
/// whatever statement it was awaiting.
pub(crate) async fn await_unless_cut<F: Future>(work: F) -> Option<F::Output> {
    await_unless_cut_after(work, IN_FLIGHT_GRACE).await
}

async fn await_unless_cut_after<F: Future>(work: F, grace: Duration) -> Option<F::Output> {
    tokio::select! {
        out = work => Some(out),
        () = grace_elapsed(grace) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn outside_a_scope_the_run_is_never_cancelled() {
        assert!(!run_cancelled());
        let pending = tokio::time::timeout(Duration::from_millis(20), run_cancel_requested());
        assert!(pending.await.is_err(), "no scope must mean no cancel");
    }

    #[tokio::test]
    async fn a_scoped_run_sees_the_trigger() {
        let (trigger, cancel) = RunCancel::new();
        cancel
            .scope(async move {
                assert!(!run_cancelled());
                trigger.cancel();
                assert!(run_cancelled());
                run_cancel_requested().await;
            })
            .await;
    }

    #[tokio::test]
    async fn in_flight_work_is_cut_only_after_the_grace() {
        let grace = Duration::from_millis(50);
        let (trigger, cancel) = RunCancel::new();
        let out = cancel
            .scope(async move {
                trigger.cancel();
                let started = std::time::Instant::now();
                let out = await_unless_cut_after(std::future::pending::<()>(), grace).await;
                assert!(started.elapsed() >= grace);
                out
            })
            .await;
        assert!(out.is_none(), "wedged work must be cut after the grace");
    }

    #[tokio::test]
    async fn without_a_cancel_work_is_never_cut() {
        let (_trigger, cancel) = RunCancel::new();
        let out = cancel
            .scope(await_unless_cut_after(
                tokio::time::sleep(Duration::from_millis(100)),
                Duration::from_millis(1),
            ))
            .await;
        assert!(
            out.is_some(),
            "the grace starts at the cancel, not at the call"
        );
    }

    #[tokio::test]
    async fn finished_work_is_never_cut() {
        let (trigger, cancel) = RunCancel::new();
        let out = cancel
            .scope(async move {
                trigger.cancel();
                await_unless_cut(async { 7 }).await
            })
            .await;
        assert_eq!(out, Some(7));
    }
}
