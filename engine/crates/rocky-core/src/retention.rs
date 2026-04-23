//! Data retention policies for Rocky model sidecars.
//!
//! Users declare a retention policy per model via the `retention` key in
//! the sidecar TOML:
//!
//! ```toml
//! name = "events_daily"
//! retention = "90d"
//! ```
//!
//! The grammar is `^\d+[dy]$` — positive integer followed by `d` (days) or
//! `y` (years). Years are flattened to 365 days — no leap-year semantics —
//! so the value Rocky hands to the warehouse is always a plain day count.
//!
//! At run time the resolved [`RetentionPolicy`] is threaded through
//! [`GovernanceAdapter::apply_retention_policy`] and each adapter compiles
//! it to a warehouse-native retention control:
//!
//! - **Databricks (Delta):** `ALTER TABLE ... SET TBLPROPERTIES
//!   ('delta.logRetentionDuration' = '{N} days',
//!    'delta.deletedFileRetentionDuration' = '{N} days')`.
//! - **Snowflake:** `ALTER TABLE ... SET DATA_RETENTION_TIME_IN_DAYS = {N}`.
//! - **BigQuery / DuckDB:** unsupported — those warehouses lack a
//!   first-class time-travel retention knob at the config level.
//!
//! [`GovernanceAdapter::apply_retention_policy`]: crate::traits::GovernanceAdapter::apply_retention_policy

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Number of days Rocky uses for a `"1y"` retention unit.
///
/// Flat 365 — no leap-year semantics. Rocky never advertises sub-day or
/// leap-year-accurate retention; warehouses already interpret retention as
/// a rolling day count, and adding calendar-aware semantics at this layer
/// would create a second source of truth.
pub const DAYS_PER_YEAR: u32 = 365;

/// Errors returned by [`RetentionPolicy::from_str`].
///
/// Surfaced verbatim through [`crate::models::ModelError::InvalidRetention`]
/// when a sidecar declares an unparseable `retention` value.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum RetentionParseError {
    /// The value was empty (`""`).
    #[error("retention value must not be empty")]
    Empty,

    /// The value ended in a character other than `d` or `y`, or had no unit
    /// suffix at all (e.g., `"90"`).
    #[error("retention value '{value}' must end with 'd' (days) or 'y' (years)")]
    MissingUnit { value: String },

    /// The numeric portion failed to parse as a positive integer — either
    /// non-digit characters (`"abc"`, `"1.5d"`), a leading sign (`"-3d"`,
    /// `"+3d"`), or a number that didn't fit in a `u32`.
    #[error("retention value '{value}' has invalid number: {reason}")]
    InvalidNumber { value: String, reason: String },

    /// Parsed number was zero (`"0d"` / `"0y"`). Rocky rejects zero because
    /// the warehouses interpret zero retention ambiguously (Snowflake treats
    /// it as "disable time-travel" — use `retention = null` for that intent
    /// instead).
    #[error("retention value '{value}' must be positive (use null to disable retention)")]
    Zero { value: String },

    /// The day-count would overflow `u32` after `y → *365` conversion.
    #[error("retention value '{value}' overflows when converted to days")]
    Overflow { value: String },
}

/// A resolved data retention policy in days.
///
/// Rocky normalises every declaration to a day count at parse time so the
/// adapter layer never has to know about the original spelling (`"90d"` vs
/// `"3y"`). See module-level docs for the grammar + warehouse mapping.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RetentionPolicy {
    /// Number of days the warehouse should retain time-travel / tombstone
    /// history. Converted from `"{N}d"` verbatim or `"{N}y"` via
    /// `N * 365` (flat, no leap-year semantics).
    pub duration_days: u32,
}

impl std::str::FromStr for RetentionPolicy {
    type Err = RetentionParseError;

    /// Parse a `"<N><unit>"` retention spec where unit is `d` or `y`.
    ///
    /// # Errors
    ///
    /// Returns [`RetentionParseError`] for empty input, missing unit,
    /// non-numeric prefix (including signed numbers like `"-3d"`), zero
    /// values, or `y → days` overflow.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::str::FromStr;
    /// use rocky_core::retention::RetentionPolicy;
    /// assert_eq!(RetentionPolicy::from_str("90d").unwrap().duration_days, 90);
    /// assert_eq!(RetentionPolicy::from_str("1y").unwrap().duration_days, 365);
    /// assert!(RetentionPolicy::from_str("abc").is_err());
    /// assert!(RetentionPolicy::from_str("90").is_err());
    /// assert!(RetentionPolicy::from_str("-3d").is_err());
    /// ```
    fn from_str(value: &str) -> Result<Self, RetentionParseError> {
        if value.is_empty() {
            return Err(RetentionParseError::Empty);
        }

        // Rocky grammar forbids leading sign, whitespace, or exponent
        // notation. Parse the numeric prefix manually so we can reject
        // them before `u32::from_str` would.
        let bytes = value.as_bytes();
        if !bytes[0].is_ascii_digit() {
            return Err(RetentionParseError::InvalidNumber {
                value: value.to_string(),
                reason: "must start with a digit (no sign, no whitespace)".to_string(),
            });
        }

        // The unit is the last byte — must be ASCII `d` or `y`. Anything
        // else (a trailing digit, a different letter, a space) is a
        // MissingUnit error so the diagnostic is actionable.
        let Some(unit) = bytes.last().copied() else {
            // unreachable: we already checked is_empty above.
            return Err(RetentionParseError::Empty);
        };
        if !unit.is_ascii_alphabetic() {
            return Err(RetentionParseError::MissingUnit {
                value: value.to_string(),
            });
        }
        if unit != b'd' && unit != b'y' {
            return Err(RetentionParseError::MissingUnit {
                value: value.to_string(),
            });
        }

        let number_str = &value[..value.len() - 1];
        let number: u32 = number_str.parse().map_err(|e: std::num::ParseIntError| {
            RetentionParseError::InvalidNumber {
                value: value.to_string(),
                reason: e.to_string(),
            }
        })?;

        if number == 0 {
            return Err(RetentionParseError::Zero {
                value: value.to_string(),
            });
        }

        let duration_days =
            match unit {
                b'd' => number,
                b'y' => number.checked_mul(DAYS_PER_YEAR).ok_or_else(|| {
                    RetentionParseError::Overflow {
                        value: value.to_string(),
                    }
                })?,
                _ => unreachable!("unit validated above"),
            };

        Ok(RetentionPolicy { duration_days })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    // Positive parses — canonical day / year grammar.
    #[test]
    fn parse_days() {
        assert_eq!(RetentionPolicy::from_str("90d").unwrap().duration_days, 90);
        assert_eq!(RetentionPolicy::from_str("1d").unwrap().duration_days, 1);
        assert_eq!(
            RetentionPolicy::from_str("365d").unwrap().duration_days,
            365
        );
    }

    #[test]
    fn parse_years_flat_365() {
        assert_eq!(RetentionPolicy::from_str("1y").unwrap().duration_days, 365);
        assert_eq!(RetentionPolicy::from_str("2y").unwrap().duration_days, 730);
        assert_eq!(RetentionPolicy::from_str("7y").unwrap().duration_days, 2555);
    }

    // Negative parses — every reject case from the waveplan.
    #[test]
    fn reject_table_driven() {
        let cases: &[(&str, &str)] = &[
            ("", "Empty"),
            ("abc", "InvalidNumber"),
            ("90", "MissingUnit"),
            ("-3d", "InvalidNumber"),
            ("-1y", "InvalidNumber"),
            ("+3d", "InvalidNumber"),
            ("1.5d", "InvalidNumber"),
            ("90days", "MissingUnit"),
            ("90x", "MissingUnit"),
            ("d", "InvalidNumber"),
            ("0d", "Zero"),
            ("0y", "Zero"),
            // u32::MAX / 365 < 12_000_000, so 12_000_000y overflows.
            ("12000000y", "Overflow"),
        ];

        for (input, kind) in cases {
            let err = match RetentionPolicy::from_str(input) {
                Err(e) => e,
                Ok(ok) => panic!("expected error for {input:?}, got Ok: {ok:?}"),
            };
            let kind_str = match err {
                RetentionParseError::Empty => "Empty",
                RetentionParseError::MissingUnit { .. } => "MissingUnit",
                RetentionParseError::InvalidNumber { .. } => "InvalidNumber",
                RetentionParseError::Zero { .. } => "Zero",
                RetentionParseError::Overflow { .. } => "Overflow",
            };
            assert_eq!(
                kind_str, *kind,
                "input {input:?}: expected {kind}, got {kind_str} ({err})"
            );
        }
    }

    #[test]
    fn reject_overflow_year_boundary() {
        // u32::MAX is 4_294_967_295. 4_294_967_295 / 365 ≈ 11_767_033.
        // So 11_767_034y must overflow, but 11_767_033y must still fit.
        let fits = RetentionPolicy::from_str("11767033y").unwrap();
        assert_eq!(fits.duration_days, 11_767_033 * DAYS_PER_YEAR);

        assert!(matches!(
            RetentionPolicy::from_str("11767034y").unwrap_err(),
            RetentionParseError::Overflow { .. }
        ));
    }

    #[test]
    fn serde_roundtrip() {
        let policy = RetentionPolicy { duration_days: 90 };
        let json = serde_json::to_string(&policy).unwrap();
        assert_eq!(json, r#"{"duration_days":90}"#);
        let parsed: RetentionPolicy = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, policy);
    }
}
