//! Circuit breaker pattern for warehouse connections.
//!
//! Tracks consecutive transient failures across SQL statement executions.
//! When the failure count exceeds the configured threshold, the breaker
//! trips and all subsequent calls fail immediately without hitting the
//! warehouse — preventing cascading failures during outages.
//!
//! # States
//!
//! The breaker is a three-state machine:
//!
//! - **Closed** — requests flow through; every failure increments the
//!   counter. When the counter reaches the configured threshold, the
//!   breaker transitions to *Open* and stamps `tripped_at`.
//! - **Open** — every [`CircuitBreaker::check`] returns
//!   [`CircuitBreakerTripped`] without touching the warehouse. If
//!   [`CircuitBreaker::recovery_timeout`] is set and has elapsed since
//!   `tripped_at`, the next `check` transitions to *HalfOpen* instead.
//! - **HalfOpen** — requests flow through as a trial. A
//!   [`CircuitBreaker::record_success`] closes the breaker (clears the
//!   counter, returns to *Closed*); a
//!   [`CircuitBreaker::record_failure`] re-opens it with a fresh
//!   `tripped_at`. This matches the standard circuit-breaker auto-
//!   recovery pattern (Nygard, *Release It!*).
//!
//! Both transitions emit a [`TransitionOutcome`] on the relevant method
//! return so the caller — typically an adapter — can forward a
//! `circuit_breaker_tripped` / `_recovered` event to the pipeline event
//! bus and the `on_circuit_breaker_*` hooks without the breaker taking
//! a dependency on the observability crate.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use thiserror::Error;

#[derive(Debug, Error)]
#[error(
    "circuit breaker tripped after {consecutive_failures} consecutive failures (threshold: {threshold}) — last error: {last_error}"
)]
pub struct CircuitBreakerTripped {
    pub consecutive_failures: u32,
    pub threshold: u32,
    pub last_error: String,
}

/// State-change side effect returned by [`CircuitBreaker`] methods so
/// callers can forward observability signals without the breaker taking
/// a dependency on `rocky-observe` or the hook registry.
///
/// Most `record_*` calls return [`Self::Unchanged`]; the two interesting
/// outcomes are [`Self::Tripped`] (Closed/HalfOpen → Open) and
/// [`Self::Recovered`] (HalfOpen → Closed).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransitionOutcome {
    Unchanged,
    /// Breaker moved into the `Open` state on this call — either from
    /// `Closed` by crossing the threshold, or from `HalfOpen` by
    /// observing a failure during a trial request.
    Tripped,
    /// Breaker moved from `HalfOpen` back to `Closed` because a trial
    /// request succeeded. Emit `circuit_breaker_recovered`.
    Recovered,
}

const STATE_CLOSED: u32 = 0;
const STATE_OPEN: u32 = 1;
const STATE_HALF_OPEN: u32 = 2;

/// Thread-safe circuit breaker with optional timed auto-recovery.
///
/// Constructed via [`CircuitBreaker::new`] (no auto-recovery) or
/// [`CircuitBreaker::with_recovery_timeout`] (timed half-open). Shared
/// across concurrent tasks via `Arc<CircuitBreaker>`.
#[derive(Debug)]
pub struct CircuitBreaker {
    consecutive_failures: AtomicU32,
    threshold: u32,
    state: AtomicU32,
    /// Elapsed millis since [`Self::epoch`] when the breaker last
    /// transitioned into `Open`. Only meaningful when `state == Open`.
    tripped_at_ms: AtomicU64,
    /// When `Some`, the breaker will transition `Open → HalfOpen`
    /// after this timeout elapses and let one trial request through.
    /// `None` preserves the original "manual reset only" behaviour.
    recovery_timeout: Option<Duration>,
    /// Monotonic clock origin for elapsed comparisons. `Instant` has
    /// no `Ordering` / arithmetic we can stuff into `AtomicU64`, so we
    /// fix the origin at construction and track elapsed millis.
    epoch: Instant,
    last_error: std::sync::Mutex<String>,
}

impl CircuitBreaker {
    /// Creates a new breaker with the given threshold and no
    /// auto-recovery. A threshold of 0 disables the breaker entirely.
    pub fn new(threshold: u32) -> Self {
        Self::build(threshold, None)
    }

    /// Creates a breaker that transitions to [`STATE_HALF_OPEN`] after
    /// `recovery_timeout` elapses while tripped. See module docs.
    pub fn with_recovery_timeout(threshold: u32, recovery_timeout: Duration) -> Self {
        Self::build(threshold, Some(recovery_timeout))
    }

    fn build(threshold: u32, recovery_timeout: Option<Duration>) -> Self {
        Self {
            consecutive_failures: AtomicU32::new(0),
            threshold,
            state: AtomicU32::new(STATE_CLOSED),
            tripped_at_ms: AtomicU64::new(0),
            recovery_timeout,
            epoch: Instant::now(),
            last_error: std::sync::Mutex::new(String::new()),
        }
    }

    fn now_ms(&self) -> u64 {
        // Saturating cast: breaks after ~584 million years of uptime.
        self.epoch.elapsed().as_millis() as u64
    }

    /// Check if the circuit breaker allows a request through.
    ///
    /// Returns `Err` only when the breaker is firmly `Open`. When
    /// `recovery_timeout` is set and has elapsed, this method
    /// transitions the breaker to `HalfOpen` and returns `Ok(())`, which
    /// lets one or more trial requests through. The next
    /// `record_success` / `record_failure` resolves the trial.
    pub fn check(&self) -> Result<(), CircuitBreakerTripped> {
        if self.threshold == 0 {
            return Ok(());
        }

        let state = self.state.load(Ordering::Acquire);
        if state == STATE_CLOSED || state == STATE_HALF_OPEN {
            return Ok(());
        }

        // state == STATE_OPEN
        if let Some(timeout) = self.recovery_timeout {
            let tripped_at = self.tripped_at_ms.load(Ordering::Acquire);
            let now = self.now_ms();
            if now.saturating_sub(tripped_at) >= timeout.as_millis() as u64 {
                // CAS Open → HalfOpen. Losing the race is fine — the
                // other thread already transitioned, so we fall through
                // to the `Ok` path below.
                let _ = self.state.compare_exchange(
                    STATE_OPEN,
                    STATE_HALF_OPEN,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                );
                return Ok(());
            }
        }

        let last = self
            .last_error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Err(CircuitBreakerTripped {
            consecutive_failures: self.consecutive_failures.load(Ordering::Relaxed),
            threshold: self.threshold,
            last_error: last.clone(),
        })
    }

    /// Record a successful operation. In `Closed` this resets the
    /// failure counter; in `HalfOpen` it also closes the breaker.
    /// Returns [`TransitionOutcome::Recovered`] when a HalfOpen → Closed
    /// transition happened, otherwise [`TransitionOutcome::Unchanged`].
    pub fn record_success(&self) -> TransitionOutcome {
        self.consecutive_failures.store(0, Ordering::Release);
        let prev = self.state.swap(STATE_CLOSED, Ordering::AcqRel);
        if prev == STATE_HALF_OPEN {
            TransitionOutcome::Recovered
        } else {
            TransitionOutcome::Unchanged
        }
    }

    /// Record a transient failure. In `Closed` this increments the
    /// counter and trips the breaker if the threshold is crossed. In
    /// `HalfOpen` any failure re-opens immediately.
    /// Returns [`TransitionOutcome::Tripped`] when a Closed/HalfOpen →
    /// Open transition happened, otherwise [`TransitionOutcome::Unchanged`].
    pub fn record_failure(&self, error_msg: &str) -> TransitionOutcome {
        if self.threshold == 0 {
            return TransitionOutcome::Unchanged;
        }

        {
            let mut last = self
                .last_error
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *last = error_msg.to_string();
        }

        let state = self.state.load(Ordering::Acquire);
        if state == STATE_HALF_OPEN {
            // Trial failed — immediate re-trip. Use CAS so only the
            // first concurrent failure during half-open emits `Tripped`;
            // losing racers report `Unchanged` and the event bus sees
            // one transition per state change.
            match self.state.compare_exchange(
                STATE_HALF_OPEN,
                STATE_OPEN,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.tripped_at_ms.store(self.now_ms(), Ordering::Release);
                    return TransitionOutcome::Tripped;
                }
                Err(_) => return TransitionOutcome::Unchanged,
            }
        }

        let count = self.consecutive_failures.fetch_add(1, Ordering::AcqRel) + 1;
        if count >= self.threshold {
            // CAS Closed → Open. Losing the race against another thread
            // that already tripped is fine — only the winner stamps
            // tripped_at, and only the winner returns `Tripped`.
            match self.state.compare_exchange(
                STATE_CLOSED,
                STATE_OPEN,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.tripped_at_ms.store(self.now_ms(), Ordering::Release);
                    TransitionOutcome::Tripped
                }
                Err(_) => TransitionOutcome::Unchanged,
            }
        } else {
            TransitionOutcome::Unchanged
        }
    }

    /// Manually reset the circuit breaker, regardless of current state.
    pub fn reset(&self) {
        self.consecutive_failures.store(0, Ordering::Release);
        self.state.store(STATE_CLOSED, Ordering::Release);
    }

    /// Returns whether the breaker is currently `Open` (rejecting
    /// requests). `HalfOpen` is reported as *not* tripped — requests
    /// are flowing through, just under trial.
    pub fn is_tripped(&self) -> bool {
        self.state.load(Ordering::Acquire) == STATE_OPEN
    }

    /// Returns the current consecutive failure count.
    pub fn failure_count(&self) -> u32 {
        self.consecutive_failures.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_circuit_breaker_trips_at_threshold() {
        let cb = CircuitBreaker::new(3);
        assert!(!cb.is_tripped());
        assert!(cb.check().is_ok());

        assert_eq!(cb.record_failure("err 1"), TransitionOutcome::Unchanged);
        assert_eq!(cb.record_failure("err 2"), TransitionOutcome::Unchanged);
        assert!(!cb.is_tripped());
        assert!(cb.check().is_ok());

        assert_eq!(cb.record_failure("err 3"), TransitionOutcome::Tripped);
        assert!(cb.is_tripped());
        assert!(cb.check().is_err());
    }

    #[test]
    fn test_success_resets_counter() {
        let cb = CircuitBreaker::new(3);
        cb.record_failure("err 1");
        cb.record_failure("err 2");
        assert_eq!(cb.record_success(), TransitionOutcome::Unchanged);
        cb.record_failure("err 3");
        assert!(!cb.is_tripped()); // counter was reset
    }

    #[test]
    fn test_disabled_when_threshold_zero() {
        let cb = CircuitBreaker::new(0);
        for _ in 0..100 {
            cb.record_failure("err");
        }
        assert!(!cb.is_tripped());
        assert!(cb.check().is_ok());
    }

    #[test]
    fn test_manual_reset() {
        let cb = CircuitBreaker::new(1);
        cb.record_failure("err");
        assert!(cb.is_tripped());
        cb.reset();
        assert!(!cb.is_tripped());
        assert!(cb.check().is_ok());
    }

    #[test]
    fn test_no_auto_recovery_without_timeout() {
        let cb = CircuitBreaker::new(1);
        cb.record_failure("err");
        assert!(cb.is_tripped());
        // Sleep would do nothing — recovery_timeout is None.
        std::thread::sleep(Duration::from_millis(50));
        assert!(cb.check().is_err());
    }

    #[test]
    fn test_half_open_after_timeout() {
        // Threshold 2 so the counter-reset post-recovery is observable:
        // one follow-up failure should NOT re-trip.
        let cb = CircuitBreaker::with_recovery_timeout(2, Duration::from_millis(30));
        cb.record_failure("boom 1");
        assert_eq!(cb.record_failure("boom 2"), TransitionOutcome::Tripped);
        assert!(cb.check().is_err(), "still Open before timeout");

        std::thread::sleep(Duration::from_millis(40));
        assert!(cb.check().is_ok(), "HalfOpen should let trial through");
        assert_eq!(cb.record_success(), TransitionOutcome::Recovered);
        assert!(!cb.is_tripped());
        // Counter was reset, so a single failure post-recovery stays closed.
        assert_eq!(cb.record_failure("noise"), TransitionOutcome::Unchanged);
        assert!(!cb.is_tripped());
    }

    #[test]
    fn test_half_open_failure_reopens() {
        let cb = CircuitBreaker::with_recovery_timeout(1, Duration::from_millis(30));
        cb.record_failure("first");
        std::thread::sleep(Duration::from_millis(40));
        assert!(cb.check().is_ok(), "HalfOpen lets trial through");
        // Trial failure re-opens the breaker, returns Tripped again.
        assert_eq!(
            cb.record_failure("trial failed"),
            TransitionOutcome::Tripped
        );
        assert!(cb.is_tripped());
        assert!(cb.check().is_err(), "back to Open");
    }

    #[test]
    fn test_half_open_concurrent_failures_emit_single_tripped() {
        // Two back-to-back failures while half-open (simulating two
        // concurrent trial requests that both fail): only the first
        // should return `Tripped` so event-bus subscribers see exactly
        // one transition. This is the CAS-on-record_failure guarantee.
        let cb = CircuitBreaker::with_recovery_timeout(1, Duration::from_millis(30));
        cb.record_failure("first");
        std::thread::sleep(Duration::from_millis(40));
        assert!(cb.check().is_ok(), "moved to HalfOpen");

        assert_eq!(cb.record_failure("trial 1"), TransitionOutcome::Tripped);
        // Second failure observes state=Open and runs the threshold
        // path; the Closed→Open CAS fails because we're already Open,
        // so it reports Unchanged — no duplicate Tripped event.
        assert_eq!(cb.record_failure("trial 2"), TransitionOutcome::Unchanged);
    }

    #[test]
    fn test_crossing_threshold_emits_single_tripped_transition() {
        let cb = CircuitBreaker::new(2);
        let outcomes: Vec<_> = (0..5)
            .map(|i| cb.record_failure(&format!("err{i}")))
            .collect();
        assert_eq!(outcomes[0], TransitionOutcome::Unchanged);
        assert_eq!(outcomes[1], TransitionOutcome::Tripped);
        // Subsequent failures while Open don't re-emit Tripped.
        assert_eq!(outcomes[2], TransitionOutcome::Unchanged);
        assert_eq!(outcomes[3], TransitionOutcome::Unchanged);
        assert_eq!(outcomes[4], TransitionOutcome::Unchanged);
    }
}
