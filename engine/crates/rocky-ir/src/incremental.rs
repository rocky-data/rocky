//! Typed pieces of a transformation model's `incremental` strategy.
//!
//! A transformation `incremental` model reads only rows newer than the
//! watermark already in its target table. The window it re-reads to catch
//! late data is the [`IncrementalLookback`]; what happens when the model's
//! output columns no longer match the target is [`OnSchemaChange`].

use std::fmt;
use std::str::FromStr;

use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Time unit of an [`IncrementalLookback`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LookbackUnit {
    Second,
    Minute,
    Hour,
    Day,
}

impl LookbackUnit {
    /// The singular upper-case SQL interval keyword (`DAY`, `HOUR`, ...).
    #[must_use]
    pub fn sql_keyword(self) -> &'static str {
        match self {
            Self::Second => "SECOND",
            Self::Minute => "MINUTE",
            Self::Hour => "HOUR",
            Self::Day => "DAY",
        }
    }

    fn singular(self) -> &'static str {
        match self {
            Self::Second => "second",
            Self::Minute => "minute",
            Self::Hour => "hour",
            Self::Day => "day",
        }
    }
}

/// How far below the target's `MAX(watermark)` an incremental run re-reads.
///
/// Written in a sidecar as a string: `"3 days"`, `"1 hour"`, `"30 minutes"`,
/// `"45 seconds"`. Singular and plural unit names are both accepted. The
/// amount is a whole number; `0` is allowed and means no lookback.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct IncrementalLookback {
    pub amount: u32,
    pub unit: LookbackUnit,
}

/// Why a lookback string was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "invalid incremental lookback '{0}': expected '<whole number> <unit>', where unit is \
     second(s), minute(s), hour(s) or day(s) — for example \"3 days\""
)]
pub struct LookbackParseError(pub String);

impl FromStr for IncrementalLookback {
    type Err = LookbackParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let err = || LookbackParseError(s.to_string());
        let mut parts = s.split_whitespace();
        let (Some(amount), Some(unit), None) = (parts.next(), parts.next(), parts.next()) else {
            return Err(err());
        };
        let amount: u32 = amount.parse().map_err(|_| err())?;
        let unit = match unit.to_ascii_lowercase().as_str() {
            "second" | "seconds" => LookbackUnit::Second,
            "minute" | "minutes" => LookbackUnit::Minute,
            "hour" | "hours" => LookbackUnit::Hour,
            "day" | "days" => LookbackUnit::Day,
            _ => return Err(err()),
        };
        Ok(Self { amount, unit })
    }
}

impl fmt::Display for IncrementalLookback {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let plural = if self.amount == 1 { "" } else { "s" };
        write!(f, "{} {}{plural}", self.amount, self.unit.singular())
    }
}

impl Serialize for IncrementalLookback {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for IncrementalLookback {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        raw.parse().map_err(serde::de::Error::custom)
    }
}

impl JsonSchema for IncrementalLookback {
    fn schema_name() -> String {
        "IncrementalLookback".to_string()
    }

    fn json_schema(generator: &mut schemars::r#gen::SchemaGenerator) -> schemars::schema::Schema {
        String::json_schema(generator)
    }
}

/// What an incremental run does when the model's output columns differ from
/// the existing target's columns.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OnSchemaChange {
    /// Stop the run and name the added and removed columns. The default:
    /// nothing is written, and `rocky run --full-refresh` rebuilds the table.
    #[default]
    Fail,
    /// Add each new output column to the target with `ALTER TABLE ... ADD
    /// COLUMN`, then load. Existing rows hold `NULL` in the new column. A
    /// column removed from the model still fails the run.
    AppendNewColumns,
}

impl OnSchemaChange {
    /// The sidecar spelling (`fail`, `append_new_columns`).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Fail => "fail",
            Self::AppendNewColumns => "append_new_columns",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookback_parses_singular_and_plural_units() {
        let cases = [
            ("3 days", 3, LookbackUnit::Day),
            ("1 day", 1, LookbackUnit::Day),
            ("2 HOURS", 2, LookbackUnit::Hour),
            ("30 minutes", 30, LookbackUnit::Minute),
            ("0 seconds", 0, LookbackUnit::Second),
            ("  5   minute ", 5, LookbackUnit::Minute),
        ];
        for (raw, amount, unit) in cases {
            let parsed: IncrementalLookback = raw.parse().expect(raw);
            assert_eq!(parsed, IncrementalLookback { amount, unit }, "{raw}");
        }
    }

    #[test]
    fn lookback_refuses_malformed_strings() {
        for raw in [
            "",
            "3",
            "days",
            "-1 days",
            "1.5 days",
            "3 weeks",
            "3 days ago",
            "x day",
        ] {
            assert!(raw.parse::<IncrementalLookback>().is_err(), "{raw:?}");
        }
    }

    #[test]
    fn lookback_round_trips_through_serde() {
        let parsed: IncrementalLookback = serde_json::from_str("\"3 days\"").unwrap();
        assert_eq!(serde_json::to_string(&parsed).unwrap(), "\"3 days\"");
        let one: IncrementalLookback = serde_json::from_str("\"1 hours\"").unwrap();
        assert_eq!(one.to_string(), "1 hour");
        assert!(serde_json::from_str::<IncrementalLookback>("\"soon\"").is_err());
    }

    #[test]
    fn on_schema_change_accepts_only_supported_values() {
        let fail: OnSchemaChange = serde_json::from_str("\"fail\"").unwrap();
        assert_eq!(fail, OnSchemaChange::Fail);
        let add: OnSchemaChange = serde_json::from_str("\"append_new_columns\"").unwrap();
        assert_eq!(add, OnSchemaChange::AppendNewColumns);
        let err = serde_json::from_str::<OnSchemaChange>("\"sync_all_columns\"").unwrap_err();
        assert!(err.to_string().contains("append_new_columns"), "{err}");
    }
}
