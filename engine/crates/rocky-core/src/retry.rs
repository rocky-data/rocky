//! Shared retry helpers.
//!
//! Historically, `compute_backoff` lived as a private copy inside
//! `rocky-databricks::connector`, `rocky-snowflake::connector`, and
//! `rocky-core::state_sync`. All three copies implemented the same
//! exponential-with-jitter policy and were called from per-crate retry
//! loops. This module hoists the helper into a single shared location
//! so the adapter crates and the state-sync machinery agree on backoff
//! semantics by construction.

use std::time::{SystemTime, UNIX_EPOCH};

use crate::config::RetryConfig;

/// Computes backoff duration in milliseconds with exponential growth,
/// capped at [`RetryConfig::max_backoff_ms`], with optional jitter.
///
/// The base delay for attempt `attempt` is
/// `initial_backoff_ms * backoff_multiplier ^ attempt`, clamped to
/// `max_backoff_ms`. When [`RetryConfig::jitter`] is `true`, the returned
/// value is randomized within ±25 % of the capped base using subsecond
/// nanos as a cheap entropy source (no `rand` dependency).
pub fn compute_backoff(cfg: &RetryConfig, attempt: u32) -> u64 {
    let base = (cfg.initial_backoff_ms as f64) * cfg.backoff_multiplier.powi(attempt as i32);
    let capped = base.min(cfg.max_backoff_ms as f64) as u64;

    if cfg.jitter {
        // Use subsecond nanos as cheap jitter source (no rand dependency needed)
        let jitter_range = capped / 4; // +-25% jitter
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos() as u64;
        let jitter = nanos % (jitter_range.max(1));
        capped
            .saturating_sub(jitter_range / 2)
            .saturating_add(jitter)
    } else {
        capped
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backoff_exponential_no_jitter() {
        let cfg = RetryConfig {
            max_retries: 3,
            initial_backoff_ms: 1000,
            max_backoff_ms: 30000,
            backoff_multiplier: 2.0,
            jitter: false,
            ..Default::default()
        };
        assert_eq!(compute_backoff(&cfg, 0), 1000); // 1000 * 2^0
        assert_eq!(compute_backoff(&cfg, 1), 2000); // 1000 * 2^1
        assert_eq!(compute_backoff(&cfg, 2), 4000); // 1000 * 2^2
    }

    #[test]
    fn test_backoff_capped() {
        let cfg = RetryConfig {
            max_retries: 5,
            initial_backoff_ms: 1000,
            max_backoff_ms: 5000,
            backoff_multiplier: 2.0,
            jitter: false,
            ..Default::default()
        };
        assert_eq!(compute_backoff(&cfg, 3), 5000); // 1000 * 2^3 = 8000, capped at 5000
        assert_eq!(compute_backoff(&cfg, 4), 5000); // still capped
    }

    #[test]
    fn test_backoff_with_jitter_in_range() {
        let cfg = RetryConfig {
            max_retries: 3,
            initial_backoff_ms: 1000,
            max_backoff_ms: 30000,
            backoff_multiplier: 2.0,
            jitter: true,
            ..Default::default()
        };
        // With jitter, result should be within +-25% of base (1000)
        let result = compute_backoff(&cfg, 0);
        assert!(result >= 750, "backoff {result} should be >= 750");
        assert!(result <= 1250, "backoff {result} should be <= 1250");
    }
}
