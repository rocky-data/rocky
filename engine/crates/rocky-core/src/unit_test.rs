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
//! [[test.expect]]
//! rows = [
//!     { id = 1, amount = 150.0, is_high_value = true },
//!     { id = 3, amount = 200.0, is_high_value = true },
//! ]
//! ```

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

/// Generate DuckDB SQL to create a temporary table from fixture rows.
///
/// Returns a `CREATE TABLE <ref> AS SELECT ... UNION ALL ...` statement
/// that the test runner can execute before running the model.
pub fn fixture_to_sql(fixture: &TestFixture) -> Option<String> {
    if fixture.rows.is_empty() {
        return None;
    }

    // Extract column names from the first row
    let first_row = fixture.rows[0].as_object()?;
    let columns: Vec<&str> = first_row.keys().map(std::string::String::as_str).collect();

    let mut selects = Vec::with_capacity(fixture.rows.len());
    for row in &fixture.rows {
        let table = row.as_object()?;
        let values: Vec<String> = columns
            .iter()
            .map(|col| match table.get(*col) {
                Some(JsonValue::String(s)) => format!("'{}'", s.replace('\'', "''")),
                Some(JsonValue::Number(n)) => n.to_string(),
                Some(JsonValue::Bool(b)) => b.to_string(),
                _ => "NULL".to_string(),
            })
            .collect();

        let col_exprs: Vec<String> = columns
            .iter()
            .zip(values.iter())
            .map(|(col, val)| format!("{val} AS {col}"))
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
}
