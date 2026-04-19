//! Shared retry budget — caps the total number of retries across one run
//! so a single rate-limited statement can't drain the adapter's rate-limit
//! quota for the rest of the pipeline.
//!
//! Adapters construct a [`RetryBudget`] at connector creation time from
//! [`crate::config::RetryConfig::max_retries_per_run`]. Each retry call
//! consults [`RetryBudget::try_consume`]; when the budget is exhausted the
//! adapter short-circuits with its own `RetryBudgetExhausted` error so the
//! failure is attributable (not mistaken for a transient / terminal error).
//!
//! A `None` / `unbounded` budget preserves the pre-P2.7 behaviour: per-
//! statement [`crate::config::RetryConfig::max_retries`] is still honoured;
//! there is just no global cap.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

/// Shared counter for the run-level retry budget. Cheap to clone (wraps an
/// `Arc`); pass by value across adapter components.
#[derive(Debug, Clone, Default)]
pub struct RetryBudget {
    inner: Option<Arc<RetryBudgetInner>>,
}

#[derive(Debug)]
struct RetryBudgetInner {
    remaining: AtomicI64,
    initial: u32,
}

impl RetryBudget {
    /// Construct an unbounded budget — `try_consume` always returns `true`.
    /// This is the default and preserves legacy behaviour.
    pub fn unbounded() -> Self {
        Self { inner: None }
    }

    /// Construct a bounded budget with `limit` retries total.
    pub fn new(limit: u32) -> Self {
        Self {
            inner: Some(Arc::new(RetryBudgetInner {
                remaining: AtomicI64::new(i64::from(limit)),
                initial: limit,
            })),
        }
    }

    /// Construct a budget from the optional config field. Returns an
    /// [`RetryBudget::unbounded`] if `limit` is `None`.
    pub fn from_config(limit: Option<u32>) -> Self {
        match limit {
            None => Self::unbounded(),
            Some(n) => Self::new(n),
        }
    }

    /// Try to consume one retry from the budget. Returns `true` if a retry
    /// slot was available (and has been decremented), `false` if the budget
    /// is exhausted. Unbounded budgets always return `true`.
    pub fn try_consume(&self) -> bool {
        let Some(inner) = &self.inner else {
            return true;
        };
        inner
            .remaining
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                if current > 0 { Some(current - 1) } else { None }
            })
            .is_ok()
    }

    /// Remaining retry budget. `None` = unbounded.
    pub fn remaining(&self) -> Option<u32> {
        self.inner.as_ref().map(|inner| {
            let v = inner.remaining.load(Ordering::Acquire);
            u32::try_from(v.max(0)).unwrap_or(u32::MAX)
        })
    }

    /// Total budget the run was configured with. `None` = unbounded.
    pub fn total(&self) -> Option<u32> {
        self.inner.as_ref().map(|inner| inner.initial)
    }

    /// Is the budget exhausted? O(1) check without consuming.
    pub fn is_exhausted(&self) -> bool {
        self.inner
            .as_ref()
            .is_some_and(|inner| inner.remaining.load(Ordering::Acquire) <= 0)
    }

    /// Is this budget unbounded (retries have no run-level cap)?
    pub fn is_unbounded(&self) -> bool {
        self.inner.is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unbounded_always_consumes() {
        let b = RetryBudget::unbounded();
        for _ in 0..1_000 {
            assert!(b.try_consume());
        }
        assert!(b.is_unbounded());
        assert_eq!(b.remaining(), None);
        assert_eq!(b.total(), None);
        assert!(!b.is_exhausted());
    }

    #[test]
    fn bounded_counts_down_then_refuses() {
        let b = RetryBudget::new(3);
        assert_eq!(b.total(), Some(3));
        assert_eq!(b.remaining(), Some(3));
        assert!(b.try_consume());
        assert_eq!(b.remaining(), Some(2));
        assert!(b.try_consume());
        assert!(b.try_consume());
        assert_eq!(b.remaining(), Some(0));
        assert!(b.is_exhausted());
        // Further consumes refuse without touching the counter.
        assert!(!b.try_consume());
        assert_eq!(b.remaining(), Some(0));
        assert!(!b.try_consume());
        assert_eq!(b.remaining(), Some(0));
    }

    #[test]
    fn zero_budget_is_exhausted_from_the_start() {
        let b = RetryBudget::new(0);
        assert!(b.is_exhausted());
        assert!(!b.try_consume());
        assert_eq!(b.remaining(), Some(0));
    }

    #[test]
    fn clones_share_the_same_counter() {
        let a = RetryBudget::new(2);
        let b = a.clone();
        assert!(a.try_consume());
        assert_eq!(b.remaining(), Some(1));
        assert!(b.try_consume());
        assert!(a.is_exhausted());
        assert!(b.is_exhausted());
    }

    #[test]
    fn from_config_respects_none() {
        let b = RetryBudget::from_config(None);
        assert!(b.is_unbounded());
        assert_eq!(b.total(), None);
    }

    #[test]
    fn from_config_respects_some() {
        let b = RetryBudget::from_config(Some(7));
        assert_eq!(b.total(), Some(7));
        assert_eq!(b.remaining(), Some(7));
    }

    #[test]
    fn concurrent_consumers_never_exceed_limit() {
        use std::thread;

        let budget = RetryBudget::new(100);
        let mut handles = Vec::new();
        let per_thread = 50usize;
        let threads = 8;
        for _ in 0..threads {
            let b = budget.clone();
            handles.push(thread::spawn(move || {
                let mut success = 0;
                for _ in 0..per_thread {
                    if b.try_consume() {
                        success += 1;
                    }
                }
                success
            }));
        }
        let total_success: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
        assert_eq!(
            total_success, 100,
            "budget was 100, sum across threads must match"
        );
        assert!(budget.is_exhausted());
    }
}
