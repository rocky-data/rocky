//! The scheduler loop's injected clock.
//!
//! The resident reconciler reads time through a [`Clock`] rather than the wall
//! clock directly, for two reasons the plan freezes as non-negotiable:
//!
//! 1. `now()` feeds `tick_once` the evaluation instant — the reconciler core is
//!    already clock-free (it takes `now` as a parameter), so the loop is the one
//!    place a wall-clock instant enters.
//! 2. `sleep(d)` is the poll cadence — driving it through the trait lets tests
//!    advance a fake clock deterministically, with no real timer and no drift,
//!    instead of sleeping through wall-clock seconds.
//!
//! What the clock does **not** cover: the tick lock's heartbeat and the webhook
//! spool's mtime lease are filesystem-mtime signals compared against
//! `SystemTime::now()` inside `rocky-core`; they are genuine real-time liveness
//! signals and are exercised by manipulating file mtimes, not this clock.

use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

/// The two time operations the scheduler loop needs, behind a trait so tests
/// drive them deterministically.
#[async_trait]
pub trait Clock: Send + Sync {
    /// The current evaluation instant, passed to `tick_once`.
    fn now(&self) -> DateTime<Utc>;
    /// Sleep for `dur` (the poll interval, in production).
    async fn sleep(&self, dur: Duration);
}

/// The production clock: wall-clock `now`, real `tokio` sleep.
#[derive(Debug, Default, Clone)]
pub struct TokioClock;

#[async_trait]
impl Clock for TokioClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }

    async fn sleep(&self, dur: Duration) {
        tokio::time::sleep(dur).await;
    }
}
