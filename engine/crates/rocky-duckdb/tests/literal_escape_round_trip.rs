//! Executed proof that `DuckDbSqlDialect::literal_escape` states the truth.
//!
//! Every other dialect's rule is either a live `#[ignore]` test (Snowflake,
//! Databricks), a docker-gated one (Trino), or doc-derived (BigQuery). This
//! one runs in-process on every CI run with no credentials, so the encoder
//! has at least one permanently executed dialect.
//!
//! What it proves: `encode_string_literal(dialect.literal_escape(), v)`
//! spliced into a statement is read back by a real DuckDB parser as exactly
//! `v` — byte-identical, not merely "the statement parsed".

use rocky_core::sql_gen::string_literal;
use rocky_core::traits::{LiteralEscape, SqlDialect};
use rocky_duckdb::DuckDbConnector;
use rocky_duckdb::dialect::DuckDbSqlDialect;
use rocky_sql::literal::encode_string_literal;

/// The values worth executing: a quote, a backslash, the `\'` pair that
/// bypasses quote-doubling on a backslash-reading lexer, a trailing
/// backslash, an empty string, and both line-break shapes.
const HOSTILE_VALUES: &[&str] = &[
    "it's",
    r"C:\tmp",
    r"a\'b",
    r"trailing\",
    "",
    "line1\nline2",
    "crlf\r\nvalue",
    r"'; DROP TABLE t; --",
    r"\'; DROP TABLE t; --",
    "plain",
];

/// Runs `SELECT <literal>` and returns the single string DuckDB read back.
///
/// # Panics
///
/// Panics if the statement fails to parse or execute. That is deliberate: a
/// wrong `literal_escape` shows up as a DuckDB syntax error, and swallowing
/// it would make a mutation look like it did not bite.
fn round_trip(db: &DuckDbConnector, literal: &str) -> String {
    let sql = format!("SELECT {literal} AS v");
    let result = db
        .execute_sql(&sql)
        .unwrap_or_else(|e| panic!("DuckDB rejected `{sql}`: {e}"));
    assert_eq!(result.rows.len(), 1, "expected one row from `{sql}`");
    match &result.rows[0][0] {
        serde_json::Value::String(s) => s.clone(),
        other => panic!("expected a string from `{sql}`, got {other:?}"),
    }
}

#[test]
fn the_duckdb_dialect_literal_round_trips_byte_identical() {
    let db = DuckDbConnector::in_memory().expect("in-memory DuckDB");
    let dialect = DuckDbSqlDialect;

    for value in HOSTILE_VALUES {
        let literal = string_literal(&dialect, value);
        assert_eq!(
            round_trip(&db, &literal),
            *value,
            "value {value:?} did not survive DuckDB as {literal}"
        );
    }
}

/// The same values through a column, so the literal is compared by the
/// engine rather than just echoed by the projection. An encoder that broke
/// out of the literal would change the comparison, not the value.
#[test]
fn the_duckdb_dialect_literal_matches_the_same_value_stored_in_a_column() {
    let db = DuckDbConnector::in_memory().expect("in-memory DuckDB");
    let dialect = DuckDbSqlDialect;

    db.execute_statement("CREATE TABLE probe (id INTEGER, v VARCHAR)")
        .expect("create probe");

    for (i, value) in HOSTILE_VALUES.iter().enumerate() {
        let literal = string_literal(&dialect, value);
        let sql = format!("INSERT INTO probe VALUES ({i}, {literal})");
        db.execute_statement(&sql)
            .unwrap_or_else(|e| panic!("DuckDB rejected `{sql}`: {e}"));
    }

    let count = db
        .execute_sql("SELECT COUNT(*) AS c FROM probe")
        .expect("count probe");
    assert_eq!(
        count.rows[0][0],
        serde_json::Value::String(HOSTILE_VALUES.len().to_string()),
        "every hostile value must have inserted exactly one row"
    );

    for (i, value) in HOSTILE_VALUES.iter().enumerate() {
        let literal = string_literal(&dialect, value);
        let sql = format!("SELECT COUNT(*) AS c FROM probe WHERE id = {i} AND v = {literal}");
        let result = db
            .execute_sql(&sql)
            .unwrap_or_else(|e| panic!("DuckDB rejected `{sql}`: {e}"));
        assert_eq!(
            result.rows[0][0],
            serde_json::Value::String("1".to_string()),
            "value {value:?} did not compare equal to itself as {literal}"
        );
    }
}

/// Pins the fact the dialect states, so flipping the variant is a visible
/// two-test failure (this one plus the executed round trip) rather than one.
#[test]
fn the_duckdb_dialect_states_the_standard_rule() {
    assert_eq!(DuckDbSqlDialect.literal_escape(), LiteralEscape::Standard);
}

/// The negative half: DuckDB's plain `'…'` does not read backslash escapes,
/// so the `Backslash` encoding is *wrong here* — and wrong loudly. This is
/// the evidence that `LiteralEscape` has to be a per-dialect fact and not one
/// hand-written encoder shared by all five adapters.
#[test]
fn the_backslash_rule_is_wrong_on_duckdb() {
    let db = DuckDbConnector::in_memory().expect("in-memory DuckDB");

    // `it's` under the Backslash rule is `'it\'s'`. DuckDB reads the
    // backslash literally, so the quote after it closes the literal and the
    // trailing `s'` is a parse error.
    let wrong = encode_string_literal(LiteralEscape::Backslash, "it's");
    assert_eq!(wrong, r"'it\'s'");
    assert!(
        db.execute_sql(&format!("SELECT {wrong} AS v")).is_err(),
        "DuckDB was expected to reject the Backslash encoding of a quote"
    );

    // `C:\tmp` under the Backslash rule parses fine but reads back with a
    // doubled backslash — a silently corrupted value, which is worse.
    let wrong = encode_string_literal(LiteralEscape::Backslash, r"C:\tmp");
    assert_eq!(wrong, r"'C:\\tmp'");
    assert_eq!(round_trip(&db, &wrong), r"C:\\tmp");

    // Same for the line break. `Backslash` encodes it as `\n` because
    // BigQuery refuses a raw one; DuckDB reads no escapes, so those two
    // characters stay two characters. The line break has to stay raw here,
    // which is what `Standard` does.
    let wrong = encode_string_literal(LiteralEscape::Backslash, "line1\nline2");
    assert_eq!(wrong, r"'line1\nline2'");
    assert_eq!(round_trip(&db, &wrong), r"line1\nline2");
}
