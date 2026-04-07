use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use tracing::warn;

/// Adaptive concurrency controller using AIMD (Additive Increase, Multiplicative Decrease).
///
/// Starts at `max_concurrency` and adjusts based on success/failure signals:
/// - On success: slowly increase concurrency (additive, every N successes)
/// - On rate limit (429/503): halve concurrency (multiplicative decrease)
/// - Respects min/max bounds
#[derive(Debug, Clone)]
pub struct AdaptiveThrottle {
    inner: Arc<ThrottleInner>,
}

#[derive(Debug)]
struct ThrottleInner {
    current: AtomicUsize,
    min_concurrency: usize,
    max_concurrency: usize,
    /// Count successes to know when to increase.
    success_count: AtomicU64,
    /// How many successes before increasing by 1.
    increase_interval: u64,
    /// Total rate limits received (for metrics).
    rate_limits_total: AtomicU64,
}

impl AdaptiveThrottle {
    /// Creates a new adaptive throttle.
    ///
    /// - `max_concurrency`: Starting (and maximum) concurrency level
    /// - `min_concurrency`: Floor — never go below this (clamped to at least 1)
    /// - `increase_interval`: Number of successes before increasing by 1 (clamped to at least 1)
    pub fn new(max_concurrency: usize, min_concurrency: usize, increase_interval: u64) -> Self {
        let min = min_concurrency.max(1);
        let max = max_concurrency.max(min);
        AdaptiveThrottle {
            inner: Arc::new(ThrottleInner {
                current: AtomicUsize::new(max),
                min_concurrency: min,
                max_concurrency: max,
                success_count: AtomicU64::new(0),
                increase_interval: increase_interval.max(1),
                rate_limits_total: AtomicU64::new(0),
            }),
        }
    }

    /// Returns the current concurrency level.
    pub fn current(&self) -> usize {
        self.inner.current.load(Ordering::Relaxed)
    }

    /// Signal a successful operation. May increase concurrency.
    ///
    /// Uses a slow-start / congestion-avoidance strategy inspired by TCP:
    /// - Below half of max concurrency: increase by 2 (slow-start phase, faster recovery)
    /// - Above half of max concurrency: increase by 1 (congestion avoidance, careful probing)
    pub fn on_success(&self) {
        let count = self.inner.success_count.fetch_add(1, Ordering::Relaxed) + 1;
        if count % self.inner.increase_interval == 0 {
            let _ = self
                .inner
                .current
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |c| {
                    if c >= self.inner.max_concurrency {
                        return None; // already at max
                    }
                    // Slow-start phase: if below half of max, increase by 2
                    // Congestion avoidance: if above half, increase by 1
                    let half_max = self.inner.max_concurrency / 2;
                    let increment = if c < half_max { 2 } else { 1 };
                    Some((c + increment).min(self.inner.max_concurrency))
                });
        }
    }

    /// Signal a rate limit (429/503). Halves concurrency (multiplicative decrease).
    pub fn on_rate_limit(&self) {
        self.inner.rate_limits_total.fetch_add(1, Ordering::Relaxed);
        // Reset success counter to prevent premature increase right after recovery
        self.inner.success_count.store(0, Ordering::Relaxed);
        let prev = self
            .inner
            .current
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |c| {
                let reduced = (c / 2).max(self.inner.min_concurrency);
                if reduced < c {
                    Some(reduced)
                } else {
                    None // already at min
                }
            });
        if let Ok(old) = prev {
            let new = (old / 2).max(self.inner.min_concurrency);
            warn!(
                from = old,
                to = new,
                "adaptive throttle: rate limit detected, reducing concurrency"
            );
        }
    }

    /// Returns total rate limits received.
    pub fn rate_limits_total(&self) -> u64 {
        self.inner.rate_limits_total.load(Ordering::Relaxed)
    }

    /// Returns the max concurrency setting.
    pub fn max_concurrency(&self) -> usize {
        self.inner.max_concurrency
    }

    /// Returns the min concurrency setting.
    pub fn min_concurrency(&self) -> usize {
        self.inner.min_concurrency
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_starts_at_max() {
        let throttle = AdaptiveThrottle::new(8, 1, 10);
        assert_eq!(throttle.current(), 8);
    }

    #[test]
    fn test_rate_limit_halves() {
        let throttle = AdaptiveThrottle::new(8, 1, 10);
        throttle.on_rate_limit();
        assert_eq!(throttle.current(), 4);
        throttle.on_rate_limit();
        assert_eq!(throttle.current(), 2);
        throttle.on_rate_limit();
        assert_eq!(throttle.current(), 1); // min
        throttle.on_rate_limit();
        assert_eq!(throttle.current(), 1); // stays at min
    }

    #[test]
    fn test_success_increases_after_interval() {
        let throttle = AdaptiveThrottle::new(8, 1, 5);
        // Start by reducing
        throttle.on_rate_limit(); // 8 -> 4
        assert_eq!(throttle.current(), 4);

        // 5 successes -> increase by 1
        for _ in 0..5 {
            throttle.on_success();
        }
        assert_eq!(throttle.current(), 5);

        // 5 more -> increase again
        for _ in 0..5 {
            throttle.on_success();
        }
        assert_eq!(throttle.current(), 6);
    }

    #[test]
    fn test_never_exceeds_max() {
        let throttle = AdaptiveThrottle::new(4, 1, 1);
        // Every success would try to increase
        for _ in 0..20 {
            throttle.on_success();
        }
        assert_eq!(throttle.current(), 4); // capped at max
    }

    #[test]
    fn test_min_floor() {
        let throttle = AdaptiveThrottle::new(2, 2, 10);
        throttle.on_rate_limit();
        assert_eq!(throttle.current(), 2); // can't go below min
    }

    #[test]
    fn test_rate_limits_counter() {
        let throttle = AdaptiveThrottle::new(8, 1, 10);
        assert_eq!(throttle.rate_limits_total(), 0);
        throttle.on_rate_limit();
        throttle.on_rate_limit();
        assert_eq!(throttle.rate_limits_total(), 2);
    }

    #[test]
    fn test_clone_shares_state() {
        let throttle = AdaptiveThrottle::new(8, 1, 10);
        let clone = throttle.clone();
        throttle.on_rate_limit();
        assert_eq!(clone.current(), 4); // shared state
    }
}
