//! Executed proof that the DuckDB watermark literal compares at the column's
//! own precision and zone (#2395, #2396).
//!
//! Each case reads `MAX(ts)` through the connector, the same cell reader the
//! runner uses for the watermark and the hold-back maximum. It then renders
//! both bounds with the dialect and counts the rows each one lets through:
//!
//! - `ts > <max>` must match no row. A match is the newest row copied again.
//! - `ts < <max>` must exclude the max row. A match there lets the newest row
//!   through the hold-back bound (#2376).
//! - With the second-newest value as the watermark, `ts > <wm>` must match
//!   exactly the max row. Zero there is a skipped row.

use chrono::{DateTime, Utc};
use rocky_core::traits::{NullTimestamps, SqlDialect};
use rocky_duckdb::DuckDbConnector;
use rocky_duckdb::dialect::DuckDbSqlDialect;

/// The timestamp column types on DuckDB. The coarse ones (`_MS`, `_S`) are
/// here because a cast to them rounds: the bounds must still hold when the
/// literal comes from the column's own maximum.
const COLUMN_TYPES: &[&str] = &[
    "TIMESTAMP_NS",
    "TIMESTAMP",
    "TIMESTAMPTZ",
    "TIMESTAMP_MS",
    "TIMESTAMP_S",
];

/// `UTC`, and a zone west of UTC where a bare literal skipped rows (#2395).
const ZONES: &[&str] = &["UTC", "America/New_York"];

/// Two rows per column type, in UTC, one hour apart: closer than the
/// five-hour New York offset, so a literal read in that zone misses the
/// newest row. The newest one has sub-microsecond digits; DuckDB truncates
/// them on a microsecond column.
const OLDER: &str = "2026-03-02 09:00:00.000000001";
const NEWEST: &str = "2026-03-02 10:00:00.123456789";

fn count(conn: &DuckDbConnector, predicate: &str) -> i64 {
    let sql = format!("SELECT count(*) FROM t WHERE {predicate}");
    let result = conn
        .execute_sql(&sql)
        .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"));
    result.rows[0][0].as_str().unwrap().parse().unwrap()
}

fn read_timestamp(conn: &DuckDbConnector, sql: &str) -> DateTime<Utc> {
    let result = conn.execute_sql(sql).unwrap();
    let cell = result.rows[0][0].as_str().unwrap();
    DateTime::parse_from_rfc3339(cell)
        .unwrap_or_else(|e| panic!("`{cell}` from `{sql}` is not RFC 3339: {e}"))
        .with_timezone(&Utc)
}

fn setup(column_type: &str, zone: &str) -> DuckDbConnector {
    let conn = DuckDbConnector::in_memory().unwrap();
    conn.execute_statement(&format!("SET TimeZone = '{zone}'"))
        .unwrap();
    // `TimeZone` acts only with the ICU extension loaded. Without it the
    // New York cases would pass at UTC and prove nothing, so check the zone
    // took effect: 10:00 UTC is 05:00 in New York in March (EST).
    let hour = conn
        .execute_sql("SELECT strftime(TIMESTAMPTZ '2026-03-02 10:00:00+00:00', '%H')")
        .unwrap();
    let expected_hour = if zone == "UTC" { "10" } else { "05" };
    assert_eq!(
        hour.rows[0][0].as_str(),
        Some(expected_hour),
        "session zone {zone} did not take effect; is the ICU extension loaded?"
    );
    conn.execute_statement(&format!("CREATE TABLE t (ts {column_type})"))
        .unwrap();
    // A naive column stores the UTC wall clock; a zone column stores the
    // instant. The read-back check in the test pins both.
    let value = |v: &str| match column_type {
        "TIMESTAMPTZ" => format!("'{v}+00:00'"),
        _ => format!("'{v}'"),
    };
    conn.execute_statement(&format!(
        "INSERT INTO t VALUES ({}), ({})",
        value(OLDER),
        value(NEWEST)
    ))
    .unwrap();
    conn
}

#[test]
fn both_bounds_compare_at_the_column_precision_in_every_session_zone() {
    let d = DuckDbSqlDialect;
    for column_type in COLUMN_TYPES {
        for zone in ZONES {
            let case = format!("{column_type} in session zone {zone}");
            let conn = setup(column_type, zone);

            let max = read_timestamp(&conn, "SELECT MAX(ts) FROM t");
            let min = read_timestamp(&conn, "SELECT MIN(ts) FROM t");
            let expected_max =
                DateTime::parse_from_rfc3339(&format!("{}Z", NEWEST.replace(' ', "T")))
                    .unwrap()
                    .with_timezone(&Utc);
            // The read side keeps the column's precision (#2004).
            let dropped_nanos = match *column_type {
                "TIMESTAMP_NS" => 0,
                "TIMESTAMP_MS" => 456_789,
                "TIMESTAMP_S" => 123_456_789,
                _ => 789,
            };
            assert_eq!(
                max,
                expected_max - chrono::Duration::nanoseconds(dropped_nanos),
                "{case}: MAX read back"
            );

            let after_max = d.watermark_where("ts", Some(&max)).unwrap();
            let after_max = after_max.strip_prefix("WHERE ").unwrap();
            assert_eq!(
                count(&conn, after_max),
                0,
                "{case}: `{after_max}` copies the newest row again (#2396)"
            );

            let after_min = d.watermark_where("ts", Some(&min)).unwrap();
            let after_min = after_min.strip_prefix("WHERE ").unwrap();
            assert_eq!(
                count(&conn, after_min),
                1,
                "{case}: `{after_min}` must copy exactly the newest row; \
                 0 is a skipped row (#2395)"
            );

            let first_run = d.watermark_where("ts", None).unwrap();
            let first_run = first_run.strip_prefix("WHERE ").unwrap();
            assert_eq!(count(&conn, first_run), 2, "{case}: `{first_run}`");

            let below_max = d
                .watermark_hold_back("ts", &max, NullTimestamps::Exclude)
                .unwrap();
            assert_eq!(
                count(&conn, &below_max),
                1,
                "{case}: `{below_max}` must hold back exactly the newest row"
            );

            let below_min = d
                .watermark_hold_back("ts", &min, NullTimestamps::Exclude)
                .unwrap();
            assert_eq!(
                count(&conn, &below_min),
                0,
                "{case}: `{below_min}` must let no row through"
            );
        }
    }
}
