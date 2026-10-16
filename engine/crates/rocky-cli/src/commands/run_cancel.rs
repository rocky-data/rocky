//! The one cancellation token a `rocky run` observes (#1606).
//!
//! A shutdown signal (SIGINT, or SIGTERM on Unix) cancels the token. Each run
//! phase checks it before it starts new warehouse work, so the run reaches its
//! own settle step before the process exits 130: the run record is persisted
//! and the idempotency claim released. The remote-state session is finalized
//! (model-only, replication `--all` model phase), checkpointed then released
//! (replication fan-out), or released without upload (transformation
//! pipeline).
//!
//! ```text
//!   signal ──▶ token.cancel()
//!                 │
//!                 ├─▶ replication fan-out: stop spawning tables, drain in-flight
//!                 ├─▶ model execution: stop at the next layer or serial model
//!                 └─▶ auto-retry: retry nothing
//!                          │
//!                          ▼
//!              settle: record the run (interrupted), release claim, exit 130
//! ```
//!
//! Who owns the token decides who listens for signals:
//!
//! - **Plain `rocky run`.** No ambient token exists, so [`for_run`] makes one
//!   and spawns the listener. The listener lives exactly as long as the run.
//! - **`rocky run --watch`.** The watch loop makes the token, runs the
//!   iteration inside [`scope`], and cancels the token from its own signal
//!   arms. [`for_run`] sees the ambient token and spawns no listener. The watch
//!   loop keeps its bounded wait (`run_watch.rs`), so a stalled statement
//!   cannot hold SIGTERM forever.
//!
//! A token cannot stop a statement that has already started. So the wait is
//! bounded: past [`SIGNAL_SETTLE_GRACE`] the listener exits 130 without the
//! settle (crash parity, the pre-#1606 behaviour). The replication fan-out is
//! the one exception; see [`RunCancel::hold_unbounded`]. A second signal of
//! either kind exits 130 at once.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio_util::sync::CancellationToken;

tokio::task_local! {
    /// The token a caller that owns the signals hands to the run it drives.
    static AMBIENT: CancellationToken;
}

/// Run `fut` with `token` as the cancellation token every [`for_run`] inside
/// it adopts.
pub(crate) async fn scope<F: Future>(token: CancellationToken, fut: F) -> F::Output {
    AMBIENT.scope(token, fut).await
}

/// The token of the enclosing [`scope`], if there is one.
pub(crate) fn ambient() -> Option<CancellationToken> {
    AMBIENT.try_with(CancellationToken::clone).ok()
}

/// The cancellation token of one run, plus the signal listener it owns.
///
/// Dropping it stops the listener, so a finished run leaves no listener behind
/// in a process that runs many (`rocky serve`, the DAG runner, tests).
pub(crate) struct RunCancel {
    token: CancellationToken,
    unbounded: Arc<AtomicUsize>,
    listener: Option<tokio::task::JoinHandle<()>>,
}

impl RunCancel {
    /// The token the run's phases observe.
    pub(crate) fn token(&self) -> &CancellationToken {
        &self.token
    }

    /// Suspend the settle deadline while the returned hold lives.
    ///
    /// For the replication fan-out only, from its first spawn through the
    /// batch watermark flush; see [`SIGNAL_SETTLE_GRACE`] for why. The grace
    /// starts afresh once the last hold drops. No effect under `--watch`,
    /// whose loop owns the deadline.
    pub(crate) fn hold_unbounded(&self) -> UnboundedHold {
        self.unbounded.fetch_add(1, Ordering::SeqCst);
        UnboundedHold(Arc::clone(&self.unbounded))
    }
}

/// See [`RunCancel::hold_unbounded`].
pub(crate) struct UnboundedHold(Arc<AtomicUsize>);

impl Drop for UnboundedHold {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// How long a cancelled run may take to settle on its own before it is cut
/// short (#1606). Shared by plain `rocky run` and `rocky run --watch`.
///
/// A token cannot stop a statement already in flight, and nothing can cancel a
/// `spawn_blocking` statement, so the wait must be bounded or a stalled model
/// would make SIGTERM useless. Before #1606 a transformation run had no
/// listener and died on SIGTERM at once; this keeps it prompt within five
/// seconds while a run that is not stalled gets time to write its record and
/// release its claim.
///
/// The replication fan-out is exempt (see [`RunCancel::hold_unbounded`]): its
/// watermarks are deferred in memory and written in ONE batch after the drain
/// (`flush_run_watermarks`), not as each table finishes. Cutting the drain
/// would lose the watermark of every table already committed, so the next run
/// would re-append those deltas on adapters without watermark recovery. That
/// drain waited without a limit before #1606 and still does; a second signal
/// stops it.
pub(crate) const SIGNAL_SETTLE_GRACE: Duration = Duration::from_secs(5);

/// Poll interval for the unbounded-hold counter.
const HOLD_POLL: Duration = Duration::from_millis(50);

/// Resolve once the settle deadline has passed: [`SIGNAL_SETTLE_GRACE`] spent
/// with no unbounded hold alive. A hold taken during the grace pauses it; the
/// grace restarts in full when the hold drops.
async fn settle_deadline(unbounded: &AtomicUsize, grace: Duration) {
    loop {
        while unbounded.load(Ordering::SeqCst) > 0 {
            tokio::time::sleep(HOLD_POLL).await;
        }
        let held_again = async {
            while unbounded.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(HOLD_POLL).await;
            }
        };
        if tokio::time::timeout(grace, held_again).await.is_err() {
            return;
        }
    }
}

impl Drop for RunCancel {
    fn drop(&mut self) {
        if let Some(listener) = self.listener.take() {
            listener.abort();
        }
    }
}

/// The token for a run starting now.
///
/// Adopts the enclosing [`scope`]'s token when there is one; its owner listens
/// for signals. Otherwise makes a fresh token and spawns a listener that
/// cancels it on the first SIGINT or SIGTERM, and exits 130 on a second signal
/// of either kind or once [`SIGNAL_SETTLE_GRACE`] passes outside any
/// [`RunCancel::hold_unbounded`].
pub(crate) fn for_run() -> RunCancel {
    let unbounded = Arc::new(AtomicUsize::new(0));
    if let Some(token) = ambient() {
        return RunCancel {
            token,
            unbounded,
            listener: None,
        };
    }
    let token = CancellationToken::new();
    let listener = spawn_signal_listener(token.clone(), Arc::clone(&unbounded));
    RunCancel {
        token,
        unbounded,
        listener: Some(listener),
    }
}

/// Spawn the signal listener for a token this run owns.
///
/// The Unix streams are registered here, before the spawn, so a signal that
/// lands before the task is first polled is latched rather than lost.
fn spawn_signal_listener(
    token: CancellationToken,
    unbounded: Arc<AtomicUsize>,
) -> tokio::task::JoinHandle<()> {
    #[cfg(unix)]
    let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()).ok();
    #[cfg(unix)]
    let mut sigterm =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).ok();
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            tokio::select! {
                Some(()) = recv_or_pending(sigint.as_mut()) => {}
                Some(()) = recv_or_pending(sigterm.as_mut()) => {}
                else => return,
            }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
        eprintln!(
            "\n[rocky] shutdown signal received — finishing in-flight statements; press Ctrl-C \
             again to terminate"
        );
        token.cancel();
        // The first signal was consumed above, so only a later one resolves
        // this, of either kind.
        let second = async {
            #[cfg(unix)]
            {
                tokio::select! {
                    Some(()) = recv_or_pending(sigint.as_mut()) => {}
                    Some(()) = recv_or_pending(sigterm.as_mut()) => {}
                    else => std::future::pending::<()>().await,
                }
            }
            #[cfg(not(unix))]
            {
                let _ = tokio::signal::ctrl_c().await;
            }
        };
        tokio::select! {
            () = second => {
                eprintln!("\n[rocky] second signal — terminating");
            }
            () = settle_deadline(&unbounded, SIGNAL_SETTLE_GRACE) => {
                eprintln!(
                    "\n[rocky] the run did not settle within {}s — terminating without the \
                     settle",
                    SIGNAL_SETTLE_GRACE.as_secs()
                );
            }
        }
        std::process::exit(130);
    })
}

/// Receive from `stream`, or wait forever when it could not be registered.
#[cfg(unix)]
async fn recv_or_pending(stream: Option<&mut tokio::signal::unix::Signal>) -> Option<()> {
    match stream {
        Some(stream) => stream.recv().await,
        None => std::future::pending().await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_run_inside_a_scope_adopts_the_scope_token_and_spawns_no_listener() {
        let token = CancellationToken::new();
        let adopted = scope(token.clone(), async { for_run() }).await;
        assert!(
            adopted.listener.is_none(),
            "the scope owner listens, not the run"
        );
        token.cancel();
        assert!(
            adopted.token().is_cancelled(),
            "the run must observe the cancel its scope owner issues"
        );
    }

    #[tokio::test]
    async fn a_run_outside_a_scope_owns_its_token_and_listener() {
        let owned = for_run();
        assert!(owned.listener.is_some());
        assert!(!owned.token().is_cancelled());
    }

    #[tokio::test]
    async fn the_deadline_waits_for_every_hold_and_then_the_full_grace() {
        const GRACE: Duration = Duration::from_millis(400);
        let unbounded = Arc::new(AtomicUsize::new(1));
        let deadline = tokio::spawn({
            let unbounded = Arc::clone(&unbounded);
            async move { settle_deadline(&unbounded, GRACE).await }
        });
        tokio::time::sleep(GRACE * 3).await;
        assert!(!deadline.is_finished(), "a live hold suspends the deadline");
        unbounded.fetch_sub(1, Ordering::SeqCst);
        tokio::time::sleep(GRACE / 4).await;
        assert!(
            !deadline.is_finished(),
            "the grace restarts when the hold drops"
        );
        tokio::time::sleep(GRACE * 2).await;
        assert!(deadline.is_finished(), "the grace then ends the wait");
    }
}
