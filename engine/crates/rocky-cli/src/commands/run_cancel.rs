//! The one cancellation token a `rocky run` observes (#1606).
//!
//! A shutdown signal cancels the token: SIGINT or SIGTERM on Unix; Ctrl-C,
//! Ctrl-Break, console close, logoff or shutdown on Windows (#2440). Each run
//! phase checks it before it starts new warehouse work, so the run reaches its
//! own settle step before the process exits 130: the run record is persisted
//! and the idempotency claim released. The remote-state session is finalized
//! (model-only, replication `--all` model phase), checkpointed then released
//! (replication fan-out), or finalized by the dispatcher when the record
//! landed and released without upload when it did not (transformation,
//! quality and snapshot pipelines, #2442).
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
//! - **`rocky run --dag`.** The DAG calls [`for_run`] once and owns the
//!   listener. Each sub-run runs inside [`Shared::scope`], so it adopts the
//!   DAG's token and its [`RunCancel::hold_unbounded`] suspends the DAG
//!   listener's deadline, as it would its own.
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

/// What a caller that owns the signals hands to the run it drives.
#[derive(Clone)]
struct Ambient {
    token: CancellationToken,
    /// The hold counter of the deadline the caller's listener watches, when
    /// the caller has one. `None` under `--watch`, whose loop owns the
    /// deadline itself.
    unbounded: Option<Arc<AtomicUsize>>,
}

tokio::task_local! {
    static AMBIENT: Ambient;
}

/// Run `fut` with `token` as the cancellation token every [`for_run`] inside
/// it adopts.
pub(crate) async fn scope<F: Future>(token: CancellationToken, fut: F) -> F::Output {
    AMBIENT
        .scope(
            Ambient {
                token,
                unbounded: None,
            },
            fut,
        )
        .await
}

/// The token of the enclosing [`scope`], if there is one.
pub(crate) fn ambient() -> Option<CancellationToken> {
    AMBIENT.try_with(|ambient| ambient.token.clone()).ok()
}

/// A run's token and settle deadline, handed to the sub-runs it drives
/// (`rocky run --dag`).
///
/// Cheap to clone. The default is a fresh token and deadline that nothing
/// listens to, for tests.
#[derive(Clone, Default)]
pub(crate) struct Shared {
    token: CancellationToken,
    unbounded: Arc<AtomicUsize>,
}

impl Shared {
    /// The token the owning run observes.
    pub(crate) fn token(&self) -> &CancellationToken {
        &self.token
    }

    /// Run `fut` as part of the owning run: every [`for_run`] inside it adopts
    /// the run's token, and its [`RunCancel::hold_unbounded`] suspends the
    /// owning run's settle deadline.
    ///
    /// A task-local does not cross `tokio::spawn`, so call this inside the
    /// spawned task, around the sub-run itself.
    pub(crate) async fn scope<F: Future>(&self, fut: F) -> F::Output {
        AMBIENT
            .scope(
                Ambient {
                    token: self.token.clone(),
                    unbounded: Some(Arc::clone(&self.unbounded)),
                },
                fut,
            )
            .await
    }
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

    /// This run's token and deadline, for the sub-runs it drives.
    pub(crate) fn shared(&self) -> Shared {
        Shared {
            token: self.token.clone(),
            unbounded: Arc::clone(&self.unbounded),
        }
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
/// watermarks are deferred in memory. Once interrupted, the drain writes each
/// collected watermark before it waits on the next copy, but a copy still
/// running writes its watermark only after it is collected. Cutting the drain
/// would lose those, so the next run would re-append their deltas on adapters
/// without watermark recovery. That
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
    if let Ok(ambient) = AMBIENT.try_with(Ambient::clone) {
        return RunCancel {
            token: ambient.token,
            unbounded: ambient.unbounded.unwrap_or_default(),
            listener: None,
        };
    }
    let unbounded = Arc::new(AtomicUsize::new(0));
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
/// The streams are registered here, before the spawn, so a signal that lands
/// before the task is first polled is latched rather than lost.
fn spawn_signal_listener(
    token: CancellationToken,
    unbounded: Arc<AtomicUsize>,
) -> tokio::task::JoinHandle<()> {
    #[cfg(unix)]
    let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()).ok();
    #[cfg(unix)]
    let mut sigterm =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).ok();
    #[cfg(windows)]
    let mut stops = WindowsStopEvents::register(true);
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            tokio::select! {
                Some(()) = recv_or_pending(sigint.as_mut()) => {}
                Some(()) = recv_or_pending(sigterm.as_mut()) => {}
                else => return,
            }
        }
        #[cfg(windows)]
        {
            if stops.recv().await.is_none() {
                return;
            }
        }
        #[cfg(not(any(unix, windows)))]
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
            #[cfg(windows)]
            {
                if stops.recv().await.is_none() {
                    std::future::pending::<()>().await;
                }
            }
            #[cfg(not(any(unix, windows)))]
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

/// The Windows console events that stop a run (#2440): Ctrl-C, Ctrl-Break,
/// closing the console window, logoff and shutdown.
///
/// Registered eagerly, like the Unix streams, so an event that lands before
/// the first poll is latched. For close, logoff and shutdown, tokio's handler
/// never returns, so Windows waits out its own timeout (about 5 s for a
/// console close) before it ends the process: the run settles if it can
/// within that time. Logoff and shutdown reach a console process only when
/// Windows delivers them to it.
#[cfg(windows)]
pub(crate) struct WindowsStopEvents {
    ctrl_c: Option<tokio::signal::windows::CtrlC>,
    ctrl_break: Option<tokio::signal::windows::CtrlBreak>,
    ctrl_close: Option<tokio::signal::windows::CtrlClose>,
    ctrl_logoff: Option<tokio::signal::windows::CtrlLogoff>,
    ctrl_shutdown: Option<tokio::signal::windows::CtrlShutdown>,
}

#[cfg(windows)]
impl WindowsStopEvents {
    /// Every stop event. `with_ctrl_c = false` leaves Ctrl-C out for a
    /// caller that already listens for it.
    pub(crate) fn register(with_ctrl_c: bool) -> Self {
        use tokio::signal::windows;
        Self {
            ctrl_c: if with_ctrl_c {
                windows::ctrl_c().ok()
            } else {
                None
            },
            ctrl_break: windows::ctrl_break().ok(),
            ctrl_close: windows::ctrl_close().ok(),
            ctrl_logoff: windows::ctrl_logoff().ok(),
            ctrl_shutdown: windows::ctrl_shutdown().ok(),
        }
    }

    /// The next stop event. `None` when no stream is left to deliver one; a
    /// stream that could not be registered waits forever instead.
    pub(crate) async fn recv(&mut self) -> Option<()> {
        let Self {
            ctrl_c,
            ctrl_break,
            ctrl_close,
            ctrl_logoff,
            ctrl_shutdown,
        } = self;
        let ctrl_c = async {
            match ctrl_c.as_mut() {
                Some(s) => s.recv().await,
                None => std::future::pending().await,
            }
        };
        let ctrl_break = async {
            match ctrl_break.as_mut() {
                Some(s) => s.recv().await,
                None => std::future::pending().await,
            }
        };
        let ctrl_close = async {
            match ctrl_close.as_mut() {
                Some(s) => s.recv().await,
                None => std::future::pending().await,
            }
        };
        let ctrl_logoff = async {
            match ctrl_logoff.as_mut() {
                Some(s) => s.recv().await,
                None => std::future::pending().await,
            }
        };
        let ctrl_shutdown = async {
            match ctrl_shutdown.as_mut() {
                Some(s) => s.recv().await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            Some(()) = ctrl_c => Some(()),
            Some(()) = ctrl_break => Some(()),
            Some(()) = ctrl_close => Some(()),
            Some(()) = ctrl_logoff => Some(()),
            Some(()) = ctrl_shutdown => Some(()),
            else => None,
        }
    }
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

    /// `rocky run --dag` (#1606): a sub-run inside the DAG's shared scope
    /// adopts the DAG's token, spawns no listener, and its replication hold
    /// suspends the DAG listener's deadline. Without the shared counter the
    /// DAG listener would cut a replication drain after the grace and lose
    /// the watermarks of every table already copied.
    #[tokio::test]
    async fn a_sub_run_in_a_shared_scope_holds_the_owning_run_deadline() {
        let owner = for_run();
        let shared = owner.shared();
        shared
            .scope(async {
                let sub_run = for_run();
                assert!(sub_run.listener.is_none(), "the owning run listens");
                let hold = sub_run.hold_unbounded();
                assert_eq!(
                    owner.unbounded.load(Ordering::SeqCst),
                    1,
                    "the sub-run's hold must reach the owning run's deadline"
                );
                drop(hold);
                owner.token().cancel();
                assert!(sub_run.token().is_cancelled());
            })
            .await;
        assert_eq!(owner.unbounded.load(Ordering::SeqCst), 0);
    }

    /// `--watch` hands over only a token: a hold inside its scope stays local,
    /// because the watch loop owns the deadline.
    #[tokio::test]
    async fn a_run_in_a_plain_scope_keeps_its_own_hold_counter() {
        let token = CancellationToken::new();
        scope(token, async {
            let run = for_run();
            let _hold = run.hold_unbounded();
            assert_eq!(run.unbounded.load(Ordering::SeqCst), 1);
        })
        .await;
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
