//! Circuit breaker pattern for warehouse connections.
//!
//! Tracks consecutive transient failures across SQL statement executions.
//! When the failure count exceeds the configured threshold, the breaker
//! trips and all subsequent calls fail immediately without hitting the
//! warehouse — preventing cascading failures during outages.

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use thiserror::Error;

#[derive(Debug, Error)]
#[error("circuit breaker tripped after {consecutive_failures} consecutive failures (threshold: {threshold}) — last error: {last_error}")]
pub struct CircuitBreakerTripped {
    pub consecutive_failures: u32,
    pub threshold: u32,
    pub last_error: String,
}

/// Thread-safe circuit breaker that tracks consecutive transient failures.
#[derive(Debug)]
pub struct CircuitBreaker {
    consecutive_failures: AtomicU32,
    threshold: u32,
    tripped: AtomicBool,
    last_error: std::sync::Mutex<String>,
}

impl CircuitBreaker {
    /// Creates a new circuit breaker with the given threshold.
    /// A threshold of 0 disables the circuit breaker (never trips).
    pub fn new(threshold: u32) -> Self {
        Self {
            consecutive_failures: AtomicU32::new(0),
            threshold,
            tripped: AtomicBool::new(false),
            last_error: std::sync::Mutex::new(String::new()),
        }
    }

    /// Check if the circuit breaker allows a request through.
    /// Returns `Err` if the breaker is tripped.
    pub fn check(&self) -> Result<(), CircuitBreakerTripped> {
        if self.threshold == 0 {
            return Ok(()); // disabled
        }
        if self.tripped.load(Ordering::Acquire) {
            let last = self.last_error.lock().unwrap_or_else(|e| e.into_inner());
            return Err(CircuitBreakerTripped {
                consecutive_failures: self.consecutive_failures.load(Ordering::Relaxed),
                threshold: self.threshold,
                last_error: last.clone(),
            });
        }
        Ok(())
    }

    /// Record a successful operation — resets the failure counter.
    pub fn record_success(&self) {
        self.consecutive_failures.store(0, Ordering::Release);
        // Don't auto-reset a tripped breaker — require explicit reset
    }

    /// Record a transient failure. If the threshold is exceeded, trip the breaker.
    pub fn record_failure(&self, error_msg: &str) {
        if self.threshold == 0 {
            return; // disabled
        }
        let count = self.consecutive_failures.fetch_add(1, Ordering::AcqRel) + 1;
        {
            let mut last = self.last_error.lock().unwrap_or_else(|e| e.into_inner());
            *last = error_msg.to_string();
        }
        if count >= self.threshold {
            self.tripped.store(true, Ordering::Release);
        }
    }

    /// Manually reset the circuit breaker (e.g., after a backoff period).
    pub fn reset(&self) {
        self.consecutive_failures.store(0, Ordering::Release);
        self.tripped.store(false, Ordering::Release);
    }

    /// Returns whether the circuit breaker is currently tripped.
    pub fn is_tripped(&self) -> bool {
        self.tripped.load(Ordering::Acquire)
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

        cb.record_failure("err 1");
        cb.record_failure("err 2");
        assert!(!cb.is_tripped());
        assert!(cb.check().is_ok());

        cb.record_failure("err 3");
        assert!(cb.is_tripped());
        assert!(cb.check().is_err());
    }

    #[test]
    fn test_success_resets_counter() {
        let cb = CircuitBreaker::new(3);
        cb.record_failure("err 1");
        cb.record_failure("err 2");
        cb.record_success();
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
}
