//! dbt-style unit test definitions for model sidecars.
//!
//! Users define inline input fixtures and expected output in `[[test]]` blocks
//! in a model's sidecar `.toml` file. The test runner seeds DuckDB with the
//! fixtures, executes the model SQL, and asserts row-by-row equality.
//!
//! ```toml
//! [[test]]
//! name = "high_value_orders"
//! description = "Orders over $100 should be flagged as high value"
//!
//! [[test.given]]
//! ref = "orders"
//! rows = [
//!     { id = 1, amount = 150.0, status = "completed" },
//!     { id = 2, amount = 50.0, status = "completed" },
//!     { id = 3, amount = 200.0, status = "cancelled" },
//! ]
//!
//! [test.expect]
//! rows = [
//!     { id = 1, amount = 150.0, is_high_value = true },
//!     { id = 3, amount = 200.0, is_high_value = true },
//! ]
//! ```

use std::collections::BTreeSet;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

/// A unit test definition from a model sidecar.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct UnitTestDef {
    /// Test name — must be unique within the model.
    pub name: String,
    /// Optional description.
    #[serde(default)]
    pub description: Option<String>,
    /// Input fixtures — mock upstream models/sources.
    #[serde(default)]
    pub given: Vec<TestFixture>,
    /// Expected output rows.
    pub expect: TestExpectation,
}

/// An input fixture that mocks an upstream model or source.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct TestFixture {
    /// The model/source name to mock (matches `depends_on` or `from` references).
    #[serde(rename = "ref")]
    pub model_ref: String,
    /// Inline rows as TOML tables.
    pub rows: Vec<JsonValue>,
}

/// Expected output definition.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct TestExpectation {
    /// Expected output rows.
    pub rows: Vec<JsonValue>,
    /// If true, order of rows matters. Default: false (set comparison).
    #[serde(default)]
    pub ordered: bool,
}

/// Result of running a single unit test.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct UnitTestResult {
    /// Model name.
    pub model: String,
    /// Test name.
    pub test: String,
    /// Whether the test passed.
    pub passed: bool,
    /// Error message if failed.
    #[serde(default)]
    pub error: Option<String>,
    /// Mismatched rows (for diagnostics).
    #[serde(default)]
    pub mismatches: Vec<RowMismatch>,
}

/// A single row mismatch between expected and actual output.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RowMismatch {
    pub row_index: usize,
    pub expected: String,
    pub actual: Option<String>,
    pub kind: MismatchKind,
}

/// Type of row mismatch.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MismatchKind {
    /// Row exists in expected but not in actual.
    Missing,
    /// Row exists in actual but not in expected.
    Extra,
    /// Row exists in both but values differ.
    ValueDiff,
}

/// Render a JSON scalar to a DuckDB SQL literal.
///
/// Strings are single-quote escaped; numbers and bools pass through their
/// canonical text form; anything else (including `null`, arrays, objects) is
/// rendered as `NULL`. Shared by [`fixture_to_sql`] and the unit-test runner's
/// expected-table builder so both stringify values identically.
pub fn json_to_sql_literal(value: &JsonValue) -> String {
    match value {
        JsonValue::String(s) => format!("'{}'", s.replace('\'', "''")),
        JsonValue::Number(n) => n.to_string(),
        JsonValue::Bool(b) => b.to_string(),
        _ => "NULL".to_string(),
    }
}

/// Generate DuckDB SQL to create a temporary table from fixture rows.
///
/// Returns a `CREATE TABLE <ref> AS SELECT ... UNION ALL ...` statement
/// that the test runner can execute before running the model.
///
/// The column set is the **union** of object keys across all rows, sorted for
/// deterministic SQL. Every per-row `SELECT` projects that same union, emitting
/// [`json_to_sql_literal`] for a present cell and SQL `NULL` for an absent one.
/// This keeps all `UNION ALL` branches column-consistent even when rows differ
/// in shape — a cell that is `null` in some rows but set in others (or a key
/// omitted by the dbt-import emitter because its value was JSON `null`)
/// materializes as SQL `NULL` rather than dropping the column or skewing the
/// `SELECT` width. (FR-045)
pub fn fixture_to_sql(fixture: &TestFixture) -> Option<String> {
    if fixture.rows.is_empty() {
        return None;
    }

    // Union of column names across every row (sorted → deterministic SQL).
    let mut col_set: BTreeSet<&str> = BTreeSet::new();
    for row in &fixture.rows {
        let obj = row.as_object()?;
        for key in obj.keys() {
            col_set.insert(key.as_str());
        }
    }
    if col_set.is_empty() {
        return None;
    }
    let columns: Vec<&str> = col_set.into_iter().collect();

    let mut selects = Vec::with_capacity(fixture.rows.len());
    for row in &fixture.rows {
        let table = row.as_object()?;
        let col_exprs: Vec<String> = columns
            .iter()
            .map(|col| {
                let lit = table
                    .get(*col)
                    .map_or_else(|| "NULL".to_string(), json_to_sql_literal);
                format!("{lit} AS {col}")
            })
            .collect();
        selects.push(format!("SELECT {}", col_exprs.join(", ")));
    }

    Some(format!(
        "CREATE OR REPLACE TABLE {ref_name} AS\n{unions}",
        ref_name = fixture.model_ref,
        unions = selects.join("\nUNION ALL\n"),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fixture_to_sql_basic() {
        let fixture = TestFixture {
            model_ref: "orders".to_string(),
            rows: vec![
                serde_json::json!({ "id": 1, "amount": 150.0 }),
                serde_json::json!({ "id": 2, "amount": 50.0 }),
            ],
        };

        let sql = fixture_to_sql(&fixture).unwrap();
        assert!(sql.starts_with("CREATE OR REPLACE TABLE orders AS"));
        assert!(sql.contains("UNION ALL"));
        assert!(sql.contains("1 AS id"));
    }

    #[test]
    fn test_fixture_empty_rows() {
        let fixture = TestFixture {
            model_ref: "empty".into(),
            rows: vec![],
        };
        assert!(fixture_to_sql(&fixture).is_none());
    }

    #[test]
    fn test_unit_test_def_deserialization() {
        let json_str = r#"{
            "name": "test_high_value",
            "description": "High-value orders",
            "given": [
                {
                    "ref": "orders",
                    "rows": [
                        { "id": 1, "amount": 150.0 },
                        { "id": 2, "amount": 50.0 }
                    ]
                }
            ],
            "expect": {
                "rows": [
                    { "id": 1, "amount": 150.0, "is_high_value": true }
                ]
            }
        }"#;
        let test_def: UnitTestDef = serde_json::from_str(json_str).unwrap();
        assert_eq!(test_def.name, "test_high_value");
        assert_eq!(test_def.given.len(), 1);
        assert_eq!(test_def.given[0].rows.len(), 2);
        assert_eq!(test_def.expect.rows.len(), 1);
    }

    #[test]
    fn test_string_escaping() {
        let fixture = TestFixture {
            model_ref: "names".into(),
            rows: vec![serde_json::json!({ "name": "O'Brien" })],
        };
        let sql = fixture_to_sql(&fixture).unwrap();
        assert!(sql.contains("O''Brien")); // escaped single quote
    }

    #[test]
    fn test_fixture_to_sql_union_of_keys_missing_key_is_null() {
        // Row 1 omits `email` (a null cell omitted on emit, or genuinely
        // absent); row 2 sets it. The union must carry `email`, and row 1
        // must project it as SQL NULL so both SELECTs share one column set.
        let fixture = TestFixture {
            model_ref: "users".into(),
            rows: vec![
                serde_json::json!({ "id": 1 }),
                serde_json::json!({ "id": 2, "email": "a@b.com" }),
            ],
        };
        let sql = fixture_to_sql(&fixture).unwrap();

        // Union column order is sorted: email before id.
        let selects: Vec<&str> = sql.split("UNION ALL").collect();
        assert_eq!(selects.len(), 2);
        // Both branches project the same two columns in the same order.
        assert!(selects[0].contains("NULL AS email"));
        assert!(selects[0].contains("1 AS id"));
        assert!(selects[1].contains("'a@b.com' AS email"));
        assert!(selects[1].contains("2 AS id"));
        // Every SELECT lists email then id (consistent shape across the union).
        for sel in &selects {
            let email_at = sel.find("AS email").expect("email column");
            let id_at = sel.find("AS id").expect("id column");
            assert!(email_at < id_at, "columns must be in stable union order");
        }
    }

    #[test]
    fn test_fixture_to_sql_explicit_null_cell_is_null() {
        // An explicit JSON null cell renders as SQL NULL (json_to_sql_literal's
        // catch-all arm), matching an omitted key.
        let fixture = TestFixture {
            model_ref: "users".into(),
            rows: vec![serde_json::json!({ "id": 1, "email": serde_json::Value::Null })],
        };
        let sql = fixture_to_sql(&fixture).unwrap();
        assert!(sql.contains("NULL AS email"));
        assert!(sql.contains("1 AS id"));
    }

    #[test]
    fn test_fixture_to_sql_all_empty_rows_is_none() {
        let fixture = TestFixture {
            model_ref: "empty".into(),
            rows: vec![serde_json::json!({})],
        };
        assert!(fixture_to_sql(&fixture).is_none());
    }
}
