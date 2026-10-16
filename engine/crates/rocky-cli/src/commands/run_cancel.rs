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
//! A token cannot stop a statement that has already started. A second SIGINT
//! therefore still exits 130 at once, as it did before.

use std::future::Future;

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
    listener: Option<tokio::task::JoinHandle<()>>,
}

impl RunCancel {
    /// The token the run's phases observe.
    pub(crate) fn token(&self) -> &CancellationToken {
        &self.token
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
/// cancels it on the first SIGINT or SIGTERM and exits 130 on a second SIGINT.
pub(crate) fn for_run() -> RunCancel {
    if let Some(token) = ambient() {
        return RunCancel {
            token,
            listener: None,
        };
    }
    let token = CancellationToken::new();
    let listener = spawn_signal_listener(token.clone());
    RunCancel {
        token,
        listener: Some(listener),
    }
}

/// Spawn the signal listener for a token this run owns.
///
/// The Unix streams are registered here, before the spawn, so a signal that
/// lands before the task is first polled is latched rather than lost.
fn spawn_signal_listener(token: CancellationToken) -> tokio::task::JoinHandle<()> {
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
        // Only SIGINT escalates. Kubernetes escalates SIGTERM to SIGKILL on its
        // own after `terminationGracePeriodSeconds`; the case to serve here is
        // the interactive "Ctrl-C again".
        #[cfg(unix)]
        {
            let _ = recv_or_pending(sigint.as_mut()).await;
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
        eprintln!("\n[rocky] second SIGINT — terminating");
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
}
