//! Partition granularity for `time_interval` materialization.
//!
//! Lives in `rocky-ir` because [`TimeGrain`] is referenced by
//! [`crate::ir::MaterializationStrategy::TimeInterval`] and
//! [`crate::ir::MaterializationStrategy::Microbatch`]. Keeping it here lets
//! `rocky-core` (and downstream consumers) pull the IR types without
//! pulling the model-loading machinery in `rocky-core::models`.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Partition granularity for `time_interval` materialization.
///
/// The granularity determines:
/// - The canonical partition key format (see [`TimeGrain::format_str`]).
/// - How `@start_date` / `@end_date` placeholders are computed per partition.
/// - What column types are valid (`hour` requires TIMESTAMP; others accept DATE).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TimeGrain {
    Hour,
    Day,
    Month,
    Year,
}

impl TimeGrain {
    /// `chrono` format string for the canonical partition key of this grain.
    pub fn format_str(self) -> &'static str {
        match self {
            TimeGrain::Hour => "%Y-%m-%dT%H",
            TimeGrain::Day => "%Y-%m-%d",
            TimeGrain::Month => "%Y-%m",
            TimeGrain::Year => "%Y",
        }
    }

    /// Format a UTC timestamp as the canonical partition key for this grain.
    pub fn format_key(self, t: chrono::DateTime<chrono::Utc>) -> String {
        t.format(self.format_str()).to_string()
    }

    /// Truncate a UTC timestamp to the start of its containing partition window.
    ///
    /// For `Hour` this drops minutes/seconds; for `Day` this drops the time
    /// portion; for `Month` this snaps to the first of the month; for `Year`
    /// this snaps to January 1st. Used by `--latest` to find "the partition
    /// containing now()".
    pub fn truncate(self, t: chrono::DateTime<chrono::Utc>) -> chrono::DateTime<chrono::Utc> {
        use chrono::{Datelike, TimeZone, Timelike};
        match self {
            TimeGrain::Hour => chrono::Utc
                .with_ymd_and_hms(t.year(), t.month(), t.day(), t.hour(), 0, 0)
                .single()
                .expect("hour truncation is unambiguous"),
            TimeGrain::Day => chrono::Utc
                .with_ymd_and_hms(t.year(), t.month(), t.day(), 0, 0, 0)
                .single()
                .expect("day truncation is unambiguous"),
            TimeGrain::Month => chrono::Utc
                .with_ymd_and_hms(t.year(), t.month(), 1, 0, 0, 0)
                .single()
                .expect("first of month is unambiguous"),
            TimeGrain::Year => chrono::Utc
                .with_ymd_and_hms(t.year(), 1, 1, 0, 0, 0)
                .single()
                .expect("January 1st is unambiguous"),
        }
    }

    /// Advance a UTC timestamp to the start of the next partition window.
    ///
    /// Uses `chrono::Months` for month/year so calendar boundaries (28/29/30/31)
    /// are honored — never `Duration::days(30)`. The input is expected to already
    /// be a window start (i.e., the result of [`TimeGrain::truncate`]); behavior
    /// for non-truncated input is unspecified.
    pub fn next(self, t: chrono::DateTime<chrono::Utc>) -> chrono::DateTime<chrono::Utc> {
        match self {
            TimeGrain::Hour => t + chrono::Duration::hours(1),
            TimeGrain::Day => t + chrono::Duration::days(1),
            TimeGrain::Month => t
                .checked_add_months(chrono::Months::new(1))
                .expect("month overflow is impossible for any plausible date"),
            TimeGrain::Year => t
                .checked_add_months(chrono::Months::new(12))
                .expect("year overflow is impossible for any plausible date"),
        }
    }
}
