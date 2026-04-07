//! Lightweight in-process metrics for Rocky.
//!
//! Designed for short-lived CLI runs — collects counters and histograms during a single
//! execution, then serializes to JSON as part of the run output.
//!
//! Thread-safe (uses atomics), zero external dependencies.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use schemars::JsonSchema;
use serde::Serialize;

/// Global metrics instance for a single Rocky run.
pub static METRICS: RunMetrics = RunMetrics::new();

/// Thread-safe metrics collector for a single pipeline run.
pub struct RunMetrics {
    // Counters
    pub tables_processed: AtomicU64,
    pub tables_failed: AtomicU64,
    pub statements_executed: AtomicU64,
    pub retries_attempted: AtomicU64,
    pub retries_succeeded: AtomicU64,
    pub anomalies_detected: AtomicU64,

    // Duration tracking (collected into histograms at snapshot time)
    table_durations_ms: Mutex<Vec<u64>>,
    query_durations_ms: Mutex<Vec<u64>>,
}

impl RunMetrics {
    const fn new() -> Self {
        RunMetrics {
            tables_processed: AtomicU64::new(0),
            tables_failed: AtomicU64::new(0),
            statements_executed: AtomicU64::new(0),
            retries_attempted: AtomicU64::new(0),
            retries_succeeded: AtomicU64::new(0),
            anomalies_detected: AtomicU64::new(0),
            table_durations_ms: Mutex::new(Vec::new()),
            query_durations_ms: Mutex::new(Vec::new()),
        }
    }

    pub fn inc_tables_processed(&self) {
        self.tables_processed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_tables_failed(&self) {
        self.tables_failed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_statements_executed(&self) {
        self.statements_executed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_retries_attempted(&self) {
        self.retries_attempted.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_retries_succeeded(&self) {
        self.retries_succeeded.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_anomalies_detected(&self) {
        self.anomalies_detected.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_table_duration_ms(&self, ms: u64) {
        if let Ok(mut v) = self.table_durations_ms.lock() {
            v.push(ms);
        }
    }

    pub fn record_query_duration_ms(&self, ms: u64) {
        if let Ok(mut v) = self.query_durations_ms.lock() {
            v.push(ms);
        }
    }

    /// Snapshot all metrics into a serializable summary.
    pub fn snapshot(&self) -> MetricsSnapshot {
        let table_durations = self
            .table_durations_ms
            .lock()
            .map(|v| v.clone())
            .unwrap_or_default();
        let query_durations = self
            .query_durations_ms
            .lock()
            .map(|v| v.clone())
            .unwrap_or_default();

        let processed = self.tables_processed.load(Ordering::Relaxed);
        let failed = self.tables_failed.load(Ordering::Relaxed);
        let error_rate_pct = if processed + failed > 0 {
            (failed as f64 / (processed + failed) as f64 * 100.0).round()
        } else {
            0.0
        };

        MetricsSnapshot {
            tables_processed: processed,
            tables_failed: failed,
            error_rate_pct,
            statements_executed: self.statements_executed.load(Ordering::Relaxed),
            retries_attempted: self.retries_attempted.load(Ordering::Relaxed),
            retries_succeeded: self.retries_succeeded.load(Ordering::Relaxed),
            anomalies_detected: self.anomalies_detected.load(Ordering::Relaxed),
            table_duration_p50_ms: percentile(&table_durations, 50),
            table_duration_p95_ms: percentile(&table_durations, 95),
            table_duration_max_ms: table_durations.iter().max().copied().unwrap_or(0),
            query_duration_p50_ms: percentile(&query_durations, 50),
            query_duration_p95_ms: percentile(&query_durations, 95),
            query_duration_max_ms: query_durations.iter().max().copied().unwrap_or(0),
        }
    }
}

/// Serializable metrics summary for JSON output.
#[derive(Debug, Serialize, JsonSchema)]
pub struct MetricsSnapshot {
    pub tables_processed: u64,
    pub tables_failed: u64,
    pub error_rate_pct: f64,
    pub statements_executed: u64,
    pub retries_attempted: u64,
    pub retries_succeeded: u64,
    pub anomalies_detected: u64,
    pub table_duration_p50_ms: u64,
    pub table_duration_p95_ms: u64,
    pub table_duration_max_ms: u64,
    pub query_duration_p50_ms: u64,
    pub query_duration_p95_ms: u64,
    pub query_duration_max_ms: u64,
}

/// Compute the Nth percentile of a slice (0-100).
fn percentile(data: &[u64], pct: usize) -> u64 {
    if data.is_empty() {
        return 0;
    }
    let mut sorted = data.to_vec();
    sorted.sort_unstable();
    let idx = (pct * (sorted.len() - 1)) / 100;
    sorted[idx]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_percentile_empty() {
        assert_eq!(percentile(&[], 50), 0);
    }

    #[test]
    fn test_percentile_single() {
        assert_eq!(percentile(&[42], 50), 42);
        assert_eq!(percentile(&[42], 95), 42);
    }

    #[test]
    fn test_percentile_sorted() {
        let data: Vec<u64> = (1..=100).collect();
        assert_eq!(percentile(&data, 50), 50);
        assert_eq!(percentile(&data, 95), 95);
        assert_eq!(percentile(&data, 0), 1);
        assert_eq!(percentile(&data, 100), 100);
    }

    #[test]
    fn test_snapshot() {
        let m = RunMetrics::new();
        m.inc_tables_processed();
        m.inc_tables_processed();
        m.inc_retries_attempted();
        m.record_table_duration_ms(100);
        m.record_table_duration_ms(200);

        let snap = m.snapshot();
        assert_eq!(snap.tables_processed, 2);
        assert_eq!(snap.retries_attempted, 1);
        assert_eq!(snap.table_duration_max_ms, 200);
    }
}
