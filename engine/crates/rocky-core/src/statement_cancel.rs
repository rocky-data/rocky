//! Warehouse statements that are running now, and how to cancel them.
//!
//! A warehouse keeps running a statement after the client stops waiting for
//! it. When `rocky run` exits without the settle (a second signal, or the
//! settle grace has passed) or aborts its fan-out, the statements it still
//! waits on would run on. An adapter that can cancel records each statement
//! while it waits on it, in an [`InFlightStatements`]. Those paths read that
//! list through [`WarehouseAdapter::in_flight_statements`] and ask the
//! adapter to cancel each one through [`WarehouseAdapter::cancel_statement`].
//! A connector that gives up on a statement itself (its deadline, a poll
//! error) cancels it before it returns.
//!
//! ```text
//!   submit ──▶ warehouse returns an id ──▶ register(id) ──▶ poll ... ──▶ done
//!                                              │                          │
//!                                              └── guard lives ───────────┘
//!                                                  (dropped: id removed)
//! ```
//!
//! [`WarehouseAdapter::in_flight_statements`]: crate::traits::WarehouseAdapter::in_flight_statements
//! [`WarehouseAdapter::cancel_statement`]: crate::traits::WarehouseAdapter::cancel_statement

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

/// How long a caller that stops waiting on a statement waits for the answer
/// to its cancel request. A connector that gives up on a statement (its
/// deadline passed, or polling failed) and a fan-out abort both send one
/// cancel and wait at most this long, so giving up stays prompt.
pub const ABANDON_CANCEL_BUDGET: Duration = Duration::from_secs(3);

/// The warehouse's own id for one running statement.
///
/// Opaque to everyone but the adapter that made it: a Databricks
/// `statement_id`, a Snowflake `statementHandle`. Only the adapter that
/// registered a handle can cancel it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StatementHandle(String);

impl StatementHandle {
    /// Wrap the warehouse's id for a statement.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// The warehouse's id, as the adapter received it.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for StatementHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// What a cancel request achieved.
///
/// Match it exhaustively (no `_ =>`), so a new outcome fails to compile at
/// every caller instead of being read as one of these.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelOutcome {
    /// The warehouse accepted the cancel request. It does not prove that the
    /// statement stopped: a statement that had already finished is accepted
    /// too, and the warehouse stops a running one on its own schedule.
    Requested,
    /// This adapter cannot cancel a statement. Nothing was sent.
    Unsupported,
}

/// What came of the cancel a client sent for a statement it stopped waiting
/// on. Carried on the connector's error, so the caller can tell a requested
/// cancel from one that may have left the statement running.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AbandonCancel {
    /// The warehouse accepted the cancel request. As with
    /// [`CancelOutcome::Requested`], that does not prove the statement
    /// stopped.
    Requested,
    /// The cancel request failed, with the rendered error. The statement may
    /// still run.
    Failed(String),
    /// No answer within [`ABANDON_CANCEL_BUDGET`]. The statement may still
    /// run.
    NoAnswer,
}

impl fmt::Display for AbandonCancel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Requested => f.write_str("cancel requested"),
            Self::Failed(error) => write!(f, "cancel failed: {error}"),
            Self::NoAnswer => write!(
                f,
                "no answer to the cancel within {}s",
                ABANDON_CANCEL_BUDGET.as_secs()
            ),
        }
    }
}

#[derive(Debug, Default)]
struct Registry {
    next_key: u64,
    live: BTreeMap<u64, StatementHandle>,
}

/// The statements an adapter is waiting on now.
///
/// Cheap to clone; clones share one list. Give one list to every connector of
/// an adapter, so a statement sent through any of them can be found.
#[derive(Debug, Clone, Default)]
pub struct InFlightStatements {
    registry: Arc<Mutex<Registry>>,
}

impl InFlightStatements {
    /// An empty list.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record `handle` as in flight until the returned guard drops.
    ///
    /// Hold the guard for as long as the client waits on the statement. A
    /// guard that drops on any path (success, error, or a dropped future)
    /// removes the handle, so the list never names a statement nobody waits
    /// on.
    #[must_use = "the handle is removed as soon as the guard drops"]
    pub fn register(&self, handle: StatementHandle) -> InFlightGuard {
        let mut registry = lock(&self.registry);
        let key = registry.next_key;
        registry.next_key = registry.next_key.wrapping_add(1);
        registry.live.insert(key, handle);
        InFlightGuard {
            registry: Arc::clone(&self.registry),
            key,
        }
    }

    /// The handles in flight now, oldest first.
    pub fn snapshot(&self) -> Vec<StatementHandle> {
        lock(&self.registry).live.values().cloned().collect()
    }
}

/// Keeps one handle in its [`InFlightStatements`] while it lives.
#[derive(Debug)]
pub struct InFlightGuard {
    registry: Arc<Mutex<Registry>>,
    key: u64,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        lock(&self.registry).live.remove(&self.key);
    }
}

/// The lock is never held across a call that can panic, so a poisoned lock
/// still holds a consistent list.
fn lock(registry: &Mutex<Registry>) -> MutexGuard<'_, Registry> {
    registry.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_registered_handle_is_listed_until_its_guard_drops() {
        let in_flight = InFlightStatements::new();
        let guard = in_flight.register(StatementHandle::new("stmt-1"));
        assert_eq!(in_flight.snapshot(), vec![StatementHandle::new("stmt-1")]);
        drop(guard);
        assert!(in_flight.snapshot().is_empty());
    }

    #[test]
    fn clones_share_one_list() {
        let in_flight = InFlightStatements::new();
        let other = in_flight.clone();
        let _guard = other.register(StatementHandle::new("stmt-1"));
        assert_eq!(in_flight.snapshot(), vec![StatementHandle::new("stmt-1")]);
    }

    #[test]
    fn the_same_id_twice_is_two_entries_and_each_guard_removes_only_its_own() {
        let in_flight = InFlightStatements::new();
        let first = in_flight.register(StatementHandle::new("stmt-1"));
        let second = in_flight.register(StatementHandle::new("stmt-1"));
        assert_eq!(in_flight.snapshot().len(), 2);
        drop(first);
        assert_eq!(in_flight.snapshot(), vec![StatementHandle::new("stmt-1")]);
        drop(second);
        assert!(in_flight.snapshot().is_empty());
    }

    #[test]
    fn the_snapshot_lists_handles_oldest_first() {
        let in_flight = InFlightStatements::new();
        let _a = in_flight.register(StatementHandle::new("a"));
        let _b = in_flight.register(StatementHandle::new("b"));
        let _c = in_flight.register(StatementHandle::new("c"));
        let ids: Vec<String> = in_flight
            .snapshot()
            .iter()
            .map(|h| h.as_str().to_string())
            .collect();
        assert_eq!(ids, ["a", "b", "c"]);
    }

    #[tokio::test]
    async fn a_dropped_waiting_future_removes_its_handle() {
        let in_flight = InFlightStatements::new();
        let waiting = {
            let in_flight = in_flight.clone();
            async move {
                let _guard = in_flight.register(StatementHandle::new("stmt-1"));
                std::future::pending::<()>().await;
            }
        };
        let mut waiting = Box::pin(waiting);
        assert!(
            futures::poll!(waiting.as_mut()).is_pending(),
            "the future registers on its first poll"
        );
        assert_eq!(in_flight.snapshot().len(), 1);
        drop(waiting);
        assert!(in_flight.snapshot().is_empty());
    }
}
