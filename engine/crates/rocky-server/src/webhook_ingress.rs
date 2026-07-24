//! Webhook-ingress configuration attached to [`crate::state::ServerState`] when
//! `rocky serve --scheduler` runs, plus a fixed in-memory rate limiter.
//!
//! The HTTP handler (`POST /api/v1/hooks/trigger/{pipeline}`) reads this to
//! decide whether the route is live (fail-closed) and to verify the HMAC before
//! spooling a durable demand. When `serve` runs without `--scheduler`, the field
//! is `None` and the route answers `404` unconditionally.

use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Fixed webhook-ingress configuration for a `--scheduler` server.
pub struct WebhookIngress {
    /// The shared HMAC secret (`ROCKY_WEBHOOK_SECRET`). `None` means "no secret
    /// configured": the route is only live on a loopback bind (dev), and then
    /// accepts without an HMAC check.
    pub secret: Option<String>,
    /// Whether the server bound a loopback host. A non-loopback bind with no
    /// secret fails closed (the route `404`s).
    pub bind_is_loopback: bool,
    /// The `.rocky` directory that holds the demand spool — the SAME directory
    /// the resident reconciler scans, so accept and consume agree.
    pub rocky_dir: PathBuf,
    /// A fixed token-bucket limiter shared across all webhook requests.
    pub rate_limiter: WebhookRateLimiter,
}

/// A fixed-rate token bucket for webhook ingress: a small burst capacity that
/// refills at a steady per-second rate. Shared across all callers (a flood
/// guard, not a per-sender quota), checked *before* any durable spool write so
/// an over-limit request never produces a `202` without a file.
pub struct WebhookRateLimiter {
    inner: Mutex<BucketState>,
    capacity: f64,
    refill_per_sec: f64,
}

struct BucketState {
    tokens: f64,
    last_refill: Instant,
}

/// The outcome of a rate-limit check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateDecision {
    /// A token was available and consumed — proceed.
    Allowed,
    /// Over the limit — reject with `429`. Carries the suggested `Retry-After`.
    Limited {
        /// How long until a token is available again.
        retry_after: Duration,
    },
}

impl WebhookRateLimiter {
    /// A limiter allowing `rps` steady requests per second with a burst
    /// capacity equal to `rps` (rounded up to at least 1 token).
    pub fn new(rps: f64) -> Self {
        let capacity = rps.max(1.0);
        Self {
            inner: Mutex::new(BucketState {
                tokens: capacity,
                last_refill: Instant::now(),
            }),
            capacity,
            refill_per_sec: rps.max(f64::MIN_POSITIVE),
        }
    }

    /// Attempt to consume one token as of `now`.
    pub fn check(&self, now: Instant) -> RateDecision {
        let mut b = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let elapsed = now.saturating_duration_since(b.last_refill).as_secs_f64();
        b.tokens = (b.tokens + elapsed * self.refill_per_sec).min(self.capacity);
        b.last_refill = now;
        if b.tokens >= 1.0 {
            b.tokens -= 1.0;
            RateDecision::Allowed
        } else {
            let deficit = 1.0 - b.tokens;
            let secs = deficit / self.refill_per_sec;
            RateDecision::Limited {
                retry_after: Duration::from_secs_f64(secs.max(0.0)).max(Duration::from_secs(1)),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_allows_burst_then_limits_then_refills() {
        let limiter = WebhookRateLimiter::new(10.0);
        let start = Instant::now();
        // The full burst of 10 is allowed immediately.
        for _ in 0..10 {
            assert_eq!(limiter.check(start), RateDecision::Allowed);
        }
        // The 11th at the same instant is limited with a >=1s Retry-After.
        match limiter.check(start) {
            RateDecision::Limited { retry_after } => {
                assert!(retry_after >= Duration::from_secs(1));
            }
            RateDecision::Allowed => panic!("expected the 11th request to be limited"),
        }
        // After a full second, the bucket has refilled to capacity again.
        let later = start + Duration::from_secs(2);
        assert_eq!(limiter.check(later), RateDecision::Allowed);
    }
}
