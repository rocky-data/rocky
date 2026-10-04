//! Declarative source freshness for transformation pipelines.
//!
//! A transformation pipeline reads external tables its models do not build
//! (a Fivetran landing schema, a raw table another team loads). This module
//! lets a project say how fresh those tables must be, in the shape dbt's
//! `sources.yml` freshness block uses:
//!
//! ```toml
//! [[pipeline.silver.sources]]
//! schema = "raw"
//! table  = "orders"
//!
//! [pipeline.silver.sources.freshness]
//! loaded_at_field = "_loaded_at"
//! warn_after      = "12h"
//! error_after     = "24h"
//! filter          = "status <> 'test'"   # optional
//! ```
//!
//! `rocky freshness` reads `MAX(loaded_at_field)` per declared source and
//! grades its age against the two thresholds. The same grading applies to a
//! model's own `[freshness]` block (`max_lag_seconds` + `severity`), see
//! [`FreshnessThresholds::from_model`].
//!
//! The durations reuse the product-spec grammar
//! ([`crate::product::spec::parse_max_lag`]): a positive integer followed by
//! `s`, `h` or `d` (`"3600s"`, `"12h"`, `"7d"`).
//!
//! Everything here is pure: the clock is a parameter ([`evaluate`]), so the
//! grading is unit-testable without a warehouse or a sleeping test.

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::traits::SqlDialect;

/// One external source a transformation pipeline reads.
///
/// `catalog` may be omitted (DuckDB and other two-part warehouses); it then
/// defaults to the empty string, which the dialects render as `schema.table`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PipelineSourceConfig {
    /// Catalog (database) holding the table. Empty for two-part names.
    #[serde(default)]
    pub catalog: String,
    /// Schema holding the table.
    pub schema: String,
    /// Table name.
    pub table: String,
    /// Optional freshness expectation. A source without one is declared but
    /// never checked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub freshness: Option<SourceFreshnessConfig>,
}

impl PipelineSourceConfig {
    /// `catalog.schema.table`, or `schema.table` when the catalog is empty.
    pub fn full_name(&self) -> String {
        if self.catalog.is_empty() {
            format!("{}.{}", self.schema, self.table)
        } else {
            format!("{}.{}.{}", self.catalog, self.schema, self.table)
        }
    }
}

/// Freshness expectation for one source (dbt `freshness:` parity).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SourceFreshnessConfig {
    /// Column holding the load time of each row. Should be a TIMESTAMP or
    /// DATE column; `rocky freshness` reads `MAX(loaded_at_field)`.
    pub loaded_at_field: String,
    /// Age above which the source reports `warn` (`"12h"`, `"3600s"`, `"7d"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warn_after: Option<String>,
    /// Age above which the source reports `error` and `rocky freshness`
    /// exits non-zero. Must not be shorter than `warn_after`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_after: Option<String>,
    /// Optional SQL predicate limiting the rows the maximum is taken over,
    /// spliced as `WHERE (<filter>)`. A statement terminator is refused.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<String>,
}

/// Parsed thresholds, in seconds. At least one is set once validated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct FreshnessThresholds {
    pub warn_after_seconds: Option<u64>,
    pub error_after_seconds: Option<u64>,
}

impl FreshnessThresholds {
    /// The thresholds a model `[freshness]` block implies: its one TTL is the
    /// error threshold when `severity = "error"`, else the warn threshold.
    /// Severity defaults to `warning`, matching the field's documentation.
    pub fn from_model(config: &crate::models::ModelFreshnessConfig) -> Self {
        match config.severity {
            Some(crate::tests::TestSeverity::Error) => Self {
                warn_after_seconds: None,
                error_after_seconds: Some(config.max_lag_seconds),
            },
            _ => Self {
                warn_after_seconds: Some(config.max_lag_seconds),
                error_after_seconds: None,
            },
        }
    }
}

/// One problem with a [`SourceFreshnessConfig`]. Compile reports each as E050.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FreshnessConfigProblem {
    /// Neither `warn_after` nor `error_after` is set.
    NoThreshold,
    /// A duration did not parse. Carries the field name and the raw value.
    BadDuration { field: &'static str, raw: String },
    /// `error_after` is shorter than `warn_after`.
    ErrorBeforeWarn {
        warn_after: String,
        error_after: String,
    },
    /// `loaded_at_field` is not a plain SQL identifier.
    BadLoadedAtField(String),
    /// `filter` could end the statement or is otherwise unsafe to splice.
    BadFilter(String),
}

impl std::fmt::Display for FreshnessConfigProblem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoThreshold => {
                write!(
                    f,
                    "freshness declares neither `warn_after` nor `error_after`"
                )
            }
            Self::BadDuration { field, raw } => write!(
                f,
                "freshness `{field}` = \"{raw}\" does not parse: expected a positive integer \
                 followed by one unit of s (seconds), h (hours), or d (days), e.g. \"12h\""
            ),
            Self::ErrorBeforeWarn {
                warn_after,
                error_after,
            } => write!(
                f,
                "freshness `error_after` (\"{error_after}\") is shorter than `warn_after` \
                 (\"{warn_after}\"), so the source would error before it ever warns"
            ),
            Self::BadLoadedAtField(field) => write!(
                f,
                "freshness `loaded_at_field` = \"{field}\" is not a plain column name \
                 (letters, digits and underscores only)"
            ),
            Self::BadFilter(reason) => write!(f, "freshness `filter` is refused: {reason}"),
        }
    }
}

impl SourceFreshnessConfig {
    /// Parse and cross-check every field. Returns all problems, not the first.
    pub fn validate(&self) -> Result<FreshnessThresholds, Vec<FreshnessConfigProblem>> {
        let mut problems = Vec::new();

        if rocky_sql::validation::validate_identifier(&self.loaded_at_field).is_err() {
            problems.push(FreshnessConfigProblem::BadLoadedAtField(
                self.loaded_at_field.clone(),
            ));
        }

        let mut parse = |field: &'static str, raw: &Option<String>| -> Option<u64> {
            let raw = raw.as_ref()?;
            match crate::product::spec::parse_max_lag(raw) {
                Ok(secs) => Some(secs),
                Err(_) => {
                    problems.push(FreshnessConfigProblem::BadDuration {
                        field,
                        raw: raw.clone(),
                    });
                    None
                }
            }
        };
        let warn = parse("warn_after", &self.warn_after);
        let error = parse("error_after", &self.error_after);

        if self.warn_after.is_none() && self.error_after.is_none() {
            problems.push(FreshnessConfigProblem::NoThreshold);
        }
        if let (Some(w), Some(e)) = (warn, error)
            && e < w
        {
            problems.push(FreshnessConfigProblem::ErrorBeforeWarn {
                warn_after: self.warn_after.clone().unwrap_or_default(),
                error_after: self.error_after.clone().unwrap_or_default(),
            });
        }

        if let Some(filter) = self.filter.as_deref()
            && let Err(e) =
                rocky_sql::validation::reject_statement_terminator("freshness `filter`", filter)
        {
            problems.push(FreshnessConfigProblem::BadFilter(e.to_string()));
        }

        if problems.is_empty() {
            Ok(FreshnessThresholds {
                warn_after_seconds: warn,
                error_after_seconds: error,
            })
        } else {
            Err(problems)
        }
    }
}

/// Outcome of one freshness check. Serialized `pass` / `warn` / `error` /
/// `runtime_error`, matching dbt's `source freshness` statuses.
///
/// - `pass`: within every threshold.
/// - `warn`: older than `warn_after`, not older than `error_after`.
/// - `error`: older than `error_after`.
/// - `runtime_error`: the check could not be evaluated (invalid config, a
///   failed query, or a value that does not read as a timestamp).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FreshnessStatus {
    Pass,
    Warn,
    Error,
    RuntimeError,
}

/// Grade one measurement against `thresholds`, with `now` injected.
///
/// Returns the status and the age in seconds (`now - max_loaded_at`).
///
/// - Age strictly greater than a threshold trips it (dbt semantics).
/// - `max_loaded_at = None` means the table is empty, or every
///   `loaded_at_field` value is NULL (after `filter`). Nothing was ever
///   loaded, so the source is as stale as it can be: the worst configured
///   threshold trips. Age is `None`.
/// - A future `max_loaded_at` (clock skew) yields a negative age and `pass`.
pub fn evaluate(
    max_loaded_at: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
    thresholds: &FreshnessThresholds,
) -> (FreshnessStatus, Option<i64>) {
    let Some(max) = max_loaded_at else {
        let status = if thresholds.error_after_seconds.is_some() {
            FreshnessStatus::Error
        } else {
            FreshnessStatus::Warn
        };
        return (status, None);
    };
    let age = (now - max).num_seconds();
    let exceeds =
        |limit: Option<u64>| limit.is_some_and(|l| age > i64::try_from(l).unwrap_or(i64::MAX));
    let status = if exceeds(thresholds.error_after_seconds) {
        FreshnessStatus::Error
    } else if exceeds(thresholds.warn_after_seconds) {
        FreshnessStatus::Warn
    } else {
        FreshnessStatus::Pass
    };
    (status, Some(age))
}

/// Build `SELECT COUNT(*), MAX(<field>) FROM <table> [WHERE (<filter>)]`.
///
/// `field` is validated as an identifier and `filter` is refused when it
/// carries a statement terminator; the table parts go through the dialect's
/// own validated formatter.
pub fn generate_max_loaded_at_sql(
    catalog: &str,
    schema: &str,
    table: &str,
    field: &str,
    filter: Option<&str>,
    dialect: &dyn SqlDialect,
) -> Result<String, String> {
    rocky_sql::validation::validate_identifier(field).map_err(|e| e.to_string())?;
    let table_ref = dialect
        .format_table_ref(catalog, schema, table)
        .map_err(|e| e.to_string())?;
    let where_clause = match filter.map(str::trim).filter(|f| !f.is_empty()) {
        Some(f) => {
            rocky_sql::validation::reject_statement_terminator("freshness `filter`", f)
                .map_err(|e| e.to_string())?;
            format!(" WHERE ({f})")
        }
        None => String::new(),
    };
    Ok(format!(
        "SELECT COUNT(*) AS row_count, MAX({field}) AS max_loaded_at FROM {table_ref}{where_clause}"
    ))
}

/// Read a `MAX(loaded_at_field)` cell as a UTC instant.
///
/// Accepts RFC 3339 (what the DuckDB adapter renders a TIMESTAMP as), the
/// `YYYY-MM-DD HH:MM:SS[.fff][±hh[:mm]]` shape other warehouses use, a naive
/// `YYYY-MM-DDTHH:MM:SS[.fff]`, and a bare `YYYY-MM-DD` DATE (read as
/// midnight UTC). A naive value is taken as UTC. Anything else is `None`.
pub fn parse_loaded_at_cell(s: &str) -> Option<DateTime<Utc>> {
    let s = s.trim();
    if let Ok(dt) = s.parse::<DateTime<Utc>>() {
        return Some(dt);
    }
    for fmt in [
        "%Y-%m-%d %H:%M:%S%.f%#z",
        "%Y-%m-%d %H:%M:%S%.f%:z",
        "%Y-%m-%d %H:%M:%S%.f %z",
    ] {
        if let Ok(dt) = DateTime::parse_from_str(s, fmt) {
            return Some(dt.with_timezone(&Utc));
        }
    }
    for fmt in ["%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%dT%H:%M:%S%.f"] {
        if let Ok(naive) = chrono::NaiveDateTime::parse_from_str(s, fmt) {
            return Some(naive.and_utc());
        }
    }
    chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .ok()
        .and_then(|d| d.and_hms_opt(0, 0, 0))
        .map(|naive| naive.and_utc())
}

/// Read a `MAX(..)` cell from a named warehouse dialect.
///
/// The Snowflake adapter passes SQL API v2 cells through unchanged, and that
/// API renders temporal values as epoch numbers, not text:
///
/// - `TIMESTAMP_NTZ`: `"<seconds>.<9-digit fraction>"`
/// - `TIMESTAMP_TZ` / `TIMESTAMP_LTZ`: `"<seconds>.<fraction> <offset>"`, the
///   seconds already UTC (the trailing offset is display-only)
/// - `DATE`: `"<days since 1970-01-01>"`, an integer
///
/// Other dialects render text and go straight to [`parse_loaded_at_cell`].
pub fn parse_loaded_at_cell_for(dialect: &str, s: &str) -> Option<DateTime<Utc>> {
    if dialect == "snowflake"
        && let Some(ts) = parse_snowflake_epoch_cell(s)
    {
        return Some(ts);
    }
    parse_loaded_at_cell(s)
}

fn parse_snowflake_epoch_cell(s: &str) -> Option<DateTime<Utc>> {
    let epoch = s.trim().split(' ').next()?;
    let (secs, frac) = match epoch.split_once('.') {
        Some((secs, frac)) => (secs, Some(frac)),
        None => (epoch, None),
    };
    let digits = |d: &str| {
        !d.is_empty()
            && d.trim_start_matches('-')
                .bytes()
                .all(|b| b.is_ascii_digit())
    };
    if !digits(secs) {
        return None;
    }
    let whole: i64 = secs.parse().ok()?;
    match frac {
        // An integer is a DATE: days since the epoch.
        None => DateTime::from_timestamp(whole.checked_mul(86_400)?, 0),
        Some(frac) => {
            if frac.is_empty() || !frac.bytes().all(|b| b.is_ascii_digit()) || frac.len() > 9 {
                return None;
            }
            let nanos: u32 = format!("{frac:0<9}").parse().ok()?;
            DateTime::from_timestamp(whole, nanos)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn cfg(warn: Option<&str>, error: Option<&str>) -> SourceFreshnessConfig {
        SourceFreshnessConfig {
            loaded_at_field: "loaded_at".into(),
            warn_after: warn.map(Into::into),
            error_after: error.map(Into::into),
            filter: None,
        }
    }

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 10, 4, 12, 0, 0).unwrap()
    }

    fn hours_ago(h: i64) -> Option<DateTime<Utc>> {
        Some(now() - chrono::Duration::hours(h))
    }

    #[test]
    fn validate_parses_both_thresholds() {
        let t = cfg(Some("12h"), Some("24h")).validate().unwrap();
        assert_eq!(t.warn_after_seconds, Some(43_200));
        assert_eq!(t.error_after_seconds, Some(86_400));
    }

    #[test]
    fn validate_accepts_one_threshold_and_equal_thresholds() {
        assert!(cfg(Some("12h"), None).validate().is_ok());
        assert!(cfg(None, Some("1d")).validate().is_ok());
        assert!(cfg(Some("24h"), Some("1d")).validate().is_ok());
    }

    #[test]
    fn validate_rejects_error_before_warn() {
        let problems = cfg(Some("24h"), Some("12h")).validate().unwrap_err();
        assert!(matches!(
            problems.as_slice(),
            [FreshnessConfigProblem::ErrorBeforeWarn { .. }]
        ));
    }

    #[test]
    fn validate_rejects_missing_thresholds_bad_durations_field_and_filter() {
        assert_eq!(
            cfg(None, None).validate().unwrap_err(),
            vec![FreshnessConfigProblem::NoThreshold]
        );
        let problems = cfg(Some("12 hours"), Some("30m")).validate().unwrap_err();
        assert_eq!(problems.len(), 2, "{problems:?}");

        let mut bad = cfg(Some("1h"), None);
        bad.loaded_at_field = "loaded_at; DROP TABLE x".into();
        bad.filter = Some("1=1; DROP TABLE x".into());
        let problems = bad.validate().unwrap_err();
        assert!(
            problems
                .iter()
                .any(|p| matches!(p, FreshnessConfigProblem::BadLoadedAtField(_)))
        );
        assert!(
            problems
                .iter()
                .any(|p| matches!(p, FreshnessConfigProblem::BadFilter(_)))
        );
    }

    #[test]
    fn evaluate_grades_fresh_warn_and_error() {
        let t = cfg(Some("12h"), Some("24h")).validate().unwrap();
        assert_eq!(
            evaluate(hours_ago(1), now(), &t),
            (FreshnessStatus::Pass, Some(3_600))
        );
        assert_eq!(evaluate(hours_ago(13), now(), &t).0, FreshnessStatus::Warn);
        assert_eq!(evaluate(hours_ago(25), now(), &t).0, FreshnessStatus::Error);
    }

    #[test]
    fn evaluate_threshold_is_strictly_greater() {
        let t = cfg(Some("12h"), Some("24h")).validate().unwrap();
        assert_eq!(evaluate(hours_ago(12), now(), &t).0, FreshnessStatus::Pass);
        assert_eq!(evaluate(hours_ago(24), now(), &t).0, FreshnessStatus::Warn);
    }

    #[test]
    fn evaluate_empty_table_trips_worst_threshold() {
        let both = cfg(Some("12h"), Some("24h")).validate().unwrap();
        assert_eq!(evaluate(None, now(), &both), (FreshnessStatus::Error, None));
        let warn_only = cfg(Some("12h"), None).validate().unwrap();
        assert_eq!(
            evaluate(None, now(), &warn_only),
            (FreshnessStatus::Warn, None)
        );
    }

    #[test]
    fn evaluate_future_timestamp_passes_with_negative_age() {
        let t = cfg(Some("12h"), None).validate().unwrap();
        assert_eq!(
            evaluate(hours_ago(-1), now(), &t),
            (FreshnessStatus::Pass, Some(-3_600))
        );
    }

    #[test]
    fn model_thresholds_follow_severity() {
        let mut m = crate::models::ModelFreshnessConfig {
            max_lag_seconds: 60,
            time_column: None,
            severity: None,
            declared_in_sidecar: true,
        };
        assert_eq!(
            FreshnessThresholds::from_model(&m).warn_after_seconds,
            Some(60)
        );
        m.severity = Some(crate::tests::TestSeverity::Error);
        let t = FreshnessThresholds::from_model(&m);
        assert_eq!(
            (t.warn_after_seconds, t.error_after_seconds),
            (None, Some(60))
        );
    }

    #[test]
    fn parse_cell_accepts_warehouse_shapes() {
        let expect = Utc.with_ymd_and_hms(2026, 10, 4, 10, 30, 0).unwrap();
        for s in [
            "2026-10-04T10:30:00+00:00",
            "2026-10-04 10:30:00",
            "2026-10-04 10:30:00.000",
            "2026-10-04T10:30:00",
            "2026-10-04 12:30:00+02",
            "2026-10-04 12:30:00+02:00",
        ] {
            assert_eq!(parse_loaded_at_cell(s), Some(expect), "{s}");
        }
        assert_eq!(
            parse_loaded_at_cell("2026-10-04"),
            Some(Utc.with_ymd_and_hms(2026, 10, 4, 0, 0, 0).unwrap())
        );
        assert_eq!(parse_loaded_at_cell("yesterday"), None);
        assert_eq!(parse_loaded_at_cell("1700000000"), None);
    }

    #[test]
    fn snowflake_epoch_cells_parse_only_for_snowflake() {
        let expect = Utc.with_ymd_and_hms(2026, 10, 4, 10, 30, 0).unwrap();
        let secs = expect.timestamp();
        assert_eq!(
            parse_loaded_at_cell_for("snowflake", &format!("{secs}.000000000")),
            Some(expect)
        );
        assert_eq!(
            parse_loaded_at_cell_for("snowflake", &format!("{secs}.500000000 1560")),
            Some(expect + chrono::Duration::milliseconds(500))
        );
        let days = expect.timestamp() / 86_400;
        assert_eq!(
            parse_loaded_at_cell_for("snowflake", &days.to_string()),
            Some(Utc.with_ymd_and_hms(2026, 10, 4, 0, 0, 0).unwrap())
        );
        // Text shapes still parse on Snowflake, and epochs mean nothing elsewhere.
        assert_eq!(
            parse_loaded_at_cell_for("snowflake", "2026-10-04 10:30:00"),
            Some(expect)
        );
        assert_eq!(parse_loaded_at_cell_for("duckdb", &secs.to_string()), None);
        assert_eq!(parse_loaded_at_cell_for("snowflake", "1.2.3"), None);
    }

    #[test]
    fn source_config_parses_from_toml() {
        let src: PipelineSourceConfig = toml::from_str(
            r#"
schema = "raw"
table = "orders"
[freshness]
loaded_at_field = "loaded_at"
warn_after = "12h"
error_after = "24h"
filter = "status <> 'test'"
"#,
        )
        .unwrap();
        assert_eq!(src.full_name(), "raw.orders");
        assert_eq!(
            src.freshness.unwrap().filter.as_deref(),
            Some("status <> 'test'")
        );
        assert!(
            toml::from_str::<PipelineSourceConfig>("schema='raw'\ntable='o'\nbogus=1").is_err(),
            "unknown keys are refused"
        );
    }
}
