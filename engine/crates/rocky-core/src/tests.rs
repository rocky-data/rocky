use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use rocky_sql::validation;

/// Errors from declarative test SQL generation.
#[derive(Debug, Error)]
pub enum TestGenError {
    #[error("validation error: {0}")]
    Validation(#[from] validation::ValidationError),

    #[error("test '{test_type}' requires a column but none was provided")]
    MissingColumn { test_type: String },

    #[error("accepted_values test requires at least one value")]
    EmptyAcceptedValues,

    #[error("relationships test requires 'to_table' and 'to_column'")]
    MissingRelationshipTarget,

    #[error("expression test requires an 'expression' field")]
    MissingExpression,

    #[error("row_count_range test requires at least one of 'min' or 'max'")]
    MissingRange,

    #[error("in_range test requires at least one of 'min' or 'max'")]
    MissingInRangeBound,

    #[error(
        "in_range bound '{value}' must parse as a number (Phase 4a supports numeric ranges only; use `time_window` or `expression` for temporal bounds)"
    )]
    InvalidInRangeBound { value: String },

    #[error("regex_match test requires a non-empty 'pattern'")]
    EmptyRegexPattern,

    #[error(
        "regex_match pattern contains unsafe character '{found}'; patterns may not contain single quotes, backticks, or semicolons"
    )]
    UnsafeRegexPattern { found: char },

    #[error("regex_match test is not supported by this adapter: {0}")]
    RegexNotSupported(String),
}

/// Severity of a test failure.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TestSeverity {
    /// Test failure is a hard error — pipeline fails.
    #[default]
    Error,
    /// Test failure is a warning — pipeline continues.
    Warning,
}

/// The kind of declarative test.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TestType {
    /// Assert that a column contains no NULL values.
    NotNull,
    /// Assert that a column contains only unique values.
    Unique,
    /// Assert that a column contains only values from a fixed set.
    AcceptedValues {
        /// The allowed values. Compared as string literals.
        values: Vec<String>,
    },
    /// Assert that every non-NULL value in a column exists in a
    /// referenced table's column (referential integrity).
    Relationships {
        /// Fully-qualified target table (`catalog.schema.table`).
        to_table: String,
        /// Column in the target table to join against.
        to_column: String,
    },
    /// Assert that a custom SQL expression holds for every row.
    Expression {
        /// A SQL boolean expression. Rows where `NOT (expression)` are failures.
        expression: String,
    },
    /// Assert that the table's row count falls within an inclusive range.
    RowCountRange {
        /// Minimum row count (inclusive). `None` means no lower bound.
        #[serde(default)]
        min: Option<u64>,
        /// Maximum row count (inclusive). `None` means no upper bound.
        #[serde(default)]
        max: Option<u64>,
    },
    /// Assert that a column's values fall within a numeric range
    /// (inclusive, half-open either side). Phase 4a supports numeric
    /// bounds only — use `time_window` (Phase 4b) or `expression` for
    /// temporal comparisons.
    ///
    /// NULL column values pass (consistent with existing `NOT IN` /
    /// `NOT (expr)` semantics).
    InRange {
        /// Minimum value (inclusive). `None` means no lower bound.
        #[serde(default)]
        min: Option<String>,
        /// Maximum value (inclusive). `None` means no upper bound.
        #[serde(default)]
        max: Option<String>,
    },
    /// Assert that a column's values match a regular expression.
    ///
    /// Dialect-specific operator (`REGEXP` / `RLIKE` / `REGEXP_LIKE` /
    /// `REGEXP_CONTAINS`) is produced by [`SqlDialect::regex_match_predicate`].
    /// Patterns are validated against a strict allowlist that rejects
    /// single quotes, backticks, and semicolons.
    ///
    /// NULL column values pass.
    RegexMatch {
        /// The regex pattern. Dialect-specific syntax — stick to the
        /// portable subset (character classes, anchors, quantifiers).
        pattern: String,
    },
}

/// A single declarative test declared in a model sidecar TOML.
///
/// Sidecar format:
/// ```toml
/// [[tests]]
/// type = "not_null"
/// column = "order_id"
///
/// [[tests]]
/// type = "accepted_values"
/// column = "status"
/// values = ["pending", "shipped", "delivered"]
/// severity = "warning"
///
/// [[tests]]
/// type = "row_count_range"
/// min = 1
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct TestDecl {
    /// The test type and its type-specific parameters.
    #[serde(flatten)]
    pub test_type: TestType,

    /// Column under test. Required for `not_null`, `unique`,
    /// `accepted_values`, `relationships`, `in_range`, `regex_match`.
    /// Ignored for `expression` and `row_count_range`.
    #[serde(default)]
    pub column: Option<String>,

    /// Severity of failure. Defaults to `error`.
    #[serde(default)]
    pub severity: TestSeverity,

    /// Optional SQL boolean predicate that scopes the assertion to a
    /// subset of rows. When set, only rows where `(filter)` evaluates
    /// to `TRUE` are subject to the assertion — rows where the filter
    /// is `FALSE` or `NULL` pass unconditionally.
    ///
    /// Filter is user-supplied SQL; the caller is responsible for
    /// sandboxing execution (same contract as `expression`).
    ///
    /// Example: `filter = "created_at > current_date - interval 30 day"`
    /// restricts a `not_null` check to rows created in the last 30 days.
    #[serde(default)]
    pub filter: Option<String>,
}

// ---------------------------------------------------------------------------
// SQL generation
// ---------------------------------------------------------------------------

/// Generates the assertion SQL for a declarative test.
///
/// Dialect-agnostic subset — for `RegexMatch` use
/// [`generate_test_sql_with_dialect`] instead. This wrapper returns
/// [`TestGenError::RegexNotSupported`] if the test is a `RegexMatch`.
///
/// The returned SQL is designed to be evaluated against the model's target
/// table. The caller checks the result:
///
/// - **`not_null`**: returns the count of NULL rows. Pass if 0.
/// - **`unique`**: returns rows with duplicate values. Pass if empty.
/// - **`accepted_values`**: returns distinct unexpected values. Pass if empty.
/// - **`relationships`**: returns orphaned foreign-key values. Pass if empty.
/// - **`expression`**: returns the count of violating rows. Pass if 0.
/// - **`row_count_range`**: returns the total row count. Caller asserts
///   the value falls within `[min, max]`.
/// - **`in_range`**: returns the count of rows outside `[min, max]`. Pass if 0.
pub fn generate_test_sql(test: &TestDecl, table: &str) -> Result<String, TestGenError> {
    generate_test_sql_inner(test, table, None)
}

/// Dialect-aware variant of [`generate_test_sql`]. Required for
/// `RegexMatch` (which needs a dialect-specific operator) and accepted
/// for any other kind.
pub fn generate_test_sql_with_dialect(
    test: &TestDecl,
    table: &str,
    dialect: &dyn crate::traits::SqlDialect,
) -> Result<String, TestGenError> {
    generate_test_sql_inner(test, table, Some(dialect))
}

fn generate_test_sql_inner(
    test: &TestDecl,
    table: &str,
    dialect: Option<&dyn crate::traits::SqlDialect>,
) -> Result<String, TestGenError> {
    // Table may be fully-qualified (catalog.schema.table), so validate each
    // dot-separated component individually — same approach as the
    // `Relationships` variant uses for `to_table`.
    for part in table.split('.') {
        validation::validate_identifier(part)?;
    }

    // Optional per-check `filter` applies to every kind by prefixing the
    // row-level WHERE clause. Unique / Relationships / RowCountRange
    // weave it in differently (see each arm).
    let filter = test
        .filter
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());

    match &test.test_type {
        TestType::NotNull => {
            let col = require_column(test, "not_null")?;
            validation::validate_identifier(col)?;
            Ok(format!(
                "SELECT COUNT(*) FROM {table} WHERE {}{col} IS NULL",
                filter_and(filter),
            ))
        }
        TestType::Unique => {
            let col = require_column(test, "unique")?;
            validation::validate_identifier(col)?;
            let where_clause = filter.map(|f| format!(" WHERE ({f})")).unwrap_or_default();
            Ok(format!(
                "SELECT {col}, COUNT(*) FROM {table}{where_clause} GROUP BY {col} HAVING COUNT(*) > 1"
            ))
        }
        TestType::AcceptedValues { values } => {
            let col = require_column(test, "accepted_values")?;
            validation::validate_identifier(col)?;
            if values.is_empty() {
                return Err(TestGenError::EmptyAcceptedValues);
            }
            let in_list = values
                .iter()
                .map(|v| format!("'{}'", v.replace('\'', "''")))
                .collect::<Vec<_>>()
                .join(", ");
            Ok(format!(
                "SELECT DISTINCT {col} FROM {table} WHERE {}{col} NOT IN ({in_list})",
                filter_and(filter),
            ))
        }
        TestType::Relationships {
            to_table,
            to_column,
        } => {
            let col = require_column(test, "relationships")?;
            validation::validate_identifier(col)?;
            // to_table may be fully-qualified (catalog.schema.table) so we
            // validate each dot-separated component individually.
            for part in to_table.split('.') {
                validation::validate_identifier(part)?;
            }
            validation::validate_identifier(to_column)?;
            let filter_clause = filter.map(|f| format!("({f}) AND ")).unwrap_or_default();
            Ok(format!(
                "SELECT t.{col} FROM {table} t \
                 LEFT JOIN {to_table} r ON t.{col} = r.{to_column} \
                 WHERE {filter_clause}r.{to_column} IS NULL AND t.{col} IS NOT NULL"
            ))
        }
        TestType::Expression { expression } => {
            if expression.trim().is_empty() {
                return Err(TestGenError::MissingExpression);
            }
            // Expression is user-supplied SQL — we cannot validate it as an
            // identifier. The caller is responsible for sandboxing execution.
            Ok(format!(
                "SELECT COUNT(*) FROM {table} WHERE {}NOT ({expression})",
                filter_and(filter),
            ))
        }
        TestType::RowCountRange { min, max } => {
            if min.is_none() && max.is_none() {
                return Err(TestGenError::MissingRange);
            }
            let where_clause = filter.map(|f| format!(" WHERE ({f})")).unwrap_or_default();
            Ok(format!("SELECT COUNT(*) FROM {table}{where_clause}"))
        }
        TestType::InRange { min, max } => {
            let col = require_column(test, "in_range")?;
            validation::validate_identifier(col)?;
            let predicate = in_range_fail_predicate(col, min.as_deref(), max.as_deref())?;
            Ok(format!(
                "SELECT COUNT(*) FROM {table} WHERE {}({predicate})",
                filter_and(filter),
            ))
        }
        TestType::RegexMatch { pattern } => {
            let col = require_column(test, "regex_match")?;
            validation::validate_identifier(col)?;
            validate_regex_pattern(pattern)?;
            let dialect = dialect.ok_or_else(|| {
                TestGenError::RegexNotSupported(
                    "use generate_test_sql_with_dialect for regex_match".into(),
                )
            })?;
            let match_pred = dialect
                .regex_match_predicate(col, pattern)
                .map_err(|e| TestGenError::RegexNotSupported(e.to_string()))?;
            Ok(format!(
                "SELECT COUNT(*) FROM {table} WHERE {}NOT ({match_pred})",
                filter_and(filter),
            ))
        }
    }
}

/// Returns `"(filter) AND "` or `""` for embedding into a WHERE clause
/// that already has a trailing predicate.
fn filter_and(filter: Option<&str>) -> String {
    filter.map(|f| format!("({f}) AND ")).unwrap_or_default()
}

/// Public re-export of [`in_range_fail_predicate`] for
/// [`crate::quarantine`], which needs the same numeric-range lowering
/// when building the NULL-permissive valid predicate.
pub fn in_range_fail_predicate_public(
    col: &str,
    min: Option<&str>,
    max: Option<&str>,
) -> Result<String, TestGenError> {
    in_range_fail_predicate(col, min, max)
}

/// Build the "failing" predicate for `in_range`: `col < min OR col > max`,
/// collapsing to one side when only one bound is set. Bounds are
/// parsed as `f64` and re-emitted as SQL numeric literals — no string
/// interpolation of user-supplied SQL text.
fn in_range_fail_predicate(
    col: &str,
    min: Option<&str>,
    max: Option<&str>,
) -> Result<String, TestGenError> {
    if min.is_none() && max.is_none() {
        return Err(TestGenError::MissingInRangeBound);
    }
    let min_n = min.map(parse_numeric_bound).transpose()?;
    let max_n = max.map(parse_numeric_bound).transpose()?;
    let parts: Vec<String> = [
        min_n.map(|n| format!("{col} < {}", format_numeric(n))),
        max_n.map(|n| format!("{col} > {}", format_numeric(n))),
    ]
    .into_iter()
    .flatten()
    .collect();
    Ok(parts.join(" OR "))
}

fn parse_numeric_bound(s: &str) -> Result<f64, TestGenError> {
    s.parse::<f64>()
        .map_err(|_| TestGenError::InvalidInRangeBound {
            value: s.to_string(),
        })
}

/// Format a parsed numeric bound as SQL. Prefers integer form when the
/// value is integral to avoid `42.0` where `42` is natural.
fn format_numeric(n: f64) -> String {
    if n.fract() == 0.0 && n.is_finite() && n.abs() < 1e18 {
        format!("{}", n as i64)
    } else {
        format!("{n}")
    }
}

/// Strict allowlist for regex patterns. Rejects characters that could
/// escape the single-quoted SQL literal context. No other regex-syntax
/// validation is performed — the dialect's regex engine decides at query
/// time whether the pattern is well-formed.
pub fn validate_regex_pattern(pattern: &str) -> Result<(), TestGenError> {
    if pattern.is_empty() {
        return Err(TestGenError::EmptyRegexPattern);
    }
    for ch in pattern.chars() {
        if matches!(ch, '\'' | '`' | ';') {
            return Err(TestGenError::UnsafeRegexPattern { found: ch });
        }
    }
    Ok(())
}

/// Extracts the required column from a test declaration, returning an error
/// if it's missing.
fn require_column<'a>(test: &'a TestDecl, test_type: &str) -> Result<&'a str, TestGenError> {
    test.column
        .as_deref()
        .ok_or_else(|| TestGenError::MissingColumn {
            test_type: test_type.to_string(),
        })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod unit_tests {
    use super::*;

    // ----- TOML deserialization -----

    #[test]
    fn test_not_null_deser() {
        let toml_str = r#"
type = "not_null"
column = "order_id"
"#;
        let decl: TestDecl = toml::from_str(toml_str).unwrap();
        assert_eq!(decl.column.as_deref(), Some("order_id"));
        assert_eq!(decl.severity, TestSeverity::Error);
        assert!(matches!(decl.test_type, TestType::NotNull));
    }

    #[test]
    fn test_unique_deser() {
        let toml_str = r#"
type = "unique"
column = "email"
severity = "warning"
"#;
        let decl: TestDecl = toml::from_str(toml_str).unwrap();
        assert_eq!(decl.column.as_deref(), Some("email"));
        assert_eq!(decl.severity, TestSeverity::Warning);
        assert!(matches!(decl.test_type, TestType::Unique));
    }

    #[test]
    fn test_accepted_values_deser() {
        let toml_str = r#"
type = "accepted_values"
column = "status"
values = ["pending", "shipped", "delivered"]
"#;
        let decl: TestDecl = toml::from_str(toml_str).unwrap();
        assert_eq!(decl.column.as_deref(), Some("status"));
        if let TestType::AcceptedValues { values } = &decl.test_type {
            assert_eq!(values, &["pending", "shipped", "delivered"]);
        } else {
            panic!("expected AcceptedValues");
        }
    }

    #[test]
    fn test_relationships_deser() {
        let toml_str = r#"
type = "relationships"
column = "customer_id"
to_table = "analytics.marts.dim_customers"
to_column = "id"
"#;
        let decl: TestDecl = toml::from_str(toml_str).unwrap();
        assert_eq!(decl.column.as_deref(), Some("customer_id"));
        if let TestType::Relationships {
            to_table,
            to_column,
        } = &decl.test_type
        {
            assert_eq!(to_table, "analytics.marts.dim_customers");
            assert_eq!(to_column, "id");
        } else {
            panic!("expected Relationships");
        }
    }

    #[test]
    fn test_expression_deser() {
        let toml_str = r#"
type = "expression"
expression = "amount > 0"
"#;
        let decl: TestDecl = toml::from_str(toml_str).unwrap();
        assert!(decl.column.is_none());
        if let TestType::Expression { expression } = &decl.test_type {
            assert_eq!(expression, "amount > 0");
        } else {
            panic!("expected Expression");
        }
    }

    #[test]
    fn test_row_count_range_deser() {
        let toml_str = r#"
type = "row_count_range"
min = 1
max = 1000000
"#;
        let decl: TestDecl = toml::from_str(toml_str).unwrap();
        assert!(decl.column.is_none());
        if let TestType::RowCountRange { min, max } = &decl.test_type {
            assert_eq!(*min, Some(1));
            assert_eq!(*max, Some(1_000_000));
        } else {
            panic!("expected RowCountRange");
        }
    }

    #[test]
    fn test_row_count_range_min_only() {
        let toml_str = r#"
type = "row_count_range"
min = 1
"#;
        let decl: TestDecl = toml::from_str(toml_str).unwrap();
        if let TestType::RowCountRange { min, max } = &decl.test_type {
            assert_eq!(*min, Some(1));
            assert!(max.is_none());
        } else {
            panic!("expected RowCountRange");
        }
    }

    #[test]
    fn test_severity_defaults_to_error() {
        let toml_str = r#"
type = "not_null"
column = "id"
"#;
        let decl: TestDecl = toml::from_str(toml_str).unwrap();
        assert_eq!(decl.severity, TestSeverity::Error);
    }

    // ----- Full sidecar model with tests -----

    #[test]
    fn test_model_config_with_tests_array() {
        let toml_str = r#"
name = "fct_orders"

[target]
catalog = "analytics"
schema = "marts"
table = "fct_orders"

[[tests]]
type = "not_null"
column = "order_id"

[[tests]]
type = "unique"
column = "order_id"

[[tests]]
type = "accepted_values"
column = "status"
values = ["pending", "shipped", "delivered"]
severity = "warning"

[[tests]]
type = "row_count_range"
min = 1
"#;
        let cfg: crate::models::ModelConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(cfg.tests.len(), 4);
        assert!(matches!(cfg.tests[0].test_type, TestType::NotNull));
        assert!(matches!(cfg.tests[1].test_type, TestType::Unique));
        assert!(matches!(
            cfg.tests[2].test_type,
            TestType::AcceptedValues { .. }
        ));
        assert_eq!(cfg.tests[2].severity, TestSeverity::Warning);
        assert!(matches!(
            cfg.tests[3].test_type,
            TestType::RowCountRange { .. }
        ));
    }

    #[test]
    fn test_model_config_without_tests_deserializes() {
        // Existing sidecars with no [[tests]] must still parse
        let toml_str = r#"
name = "dim_customers"
target = { catalog = "c", schema = "s", table = "t" }
"#;
        let cfg: crate::models::ModelConfig = toml::from_str(toml_str).unwrap();
        assert!(cfg.tests.is_empty());
    }

    // ----- SQL generation -----

    #[test]
    fn test_sql_not_null() {
        let decl = TestDecl {
            test_type: TestType::NotNull,
            column: Some("order_id".into()),
            severity: TestSeverity::Error,
            filter: None,
        };
        let sql = generate_test_sql(&decl, "fct_orders").unwrap();
        assert_eq!(
            sql,
            "SELECT COUNT(*) FROM fct_orders WHERE order_id IS NULL"
        );
    }

    #[test]
    fn test_sql_unique() {
        let decl = TestDecl {
            test_type: TestType::Unique,
            column: Some("email".into()),
            severity: TestSeverity::Error,
            filter: None,
        };
        let sql = generate_test_sql(&decl, "fct_orders").unwrap();
        assert_eq!(
            sql,
            "SELECT email, COUNT(*) FROM fct_orders GROUP BY email HAVING COUNT(*) > 1"
        );
    }

    #[test]
    fn test_sql_accepted_values() {
        let decl = TestDecl {
            test_type: TestType::AcceptedValues {
                values: vec!["pending".into(), "shipped".into()],
            },
            column: Some("status".into()),
            severity: TestSeverity::Error,
            filter: None,
        };
        let sql = generate_test_sql(&decl, "orders").unwrap();
        assert_eq!(
            sql,
            "SELECT DISTINCT status FROM orders WHERE status NOT IN ('pending', 'shipped')"
        );
    }

    #[test]
    fn test_sql_accepted_values_escapes_quotes() {
        let decl = TestDecl {
            test_type: TestType::AcceptedValues {
                values: vec!["it's".into()],
            },
            column: Some("name".into()),
            severity: TestSeverity::Error,
            filter: None,
        };
        let sql = generate_test_sql(&decl, "t").unwrap();
        assert!(sql.contains("'it''s'"), "single quotes should be escaped");
    }

    #[test]
    fn test_sql_relationships() {
        let decl = TestDecl {
            test_type: TestType::Relationships {
                to_table: "analytics.marts.dim_customers".into(),
                to_column: "id".into(),
            },
            column: Some("customer_id".into()),
            severity: TestSeverity::Error,
            filter: None,
        };
        let sql = generate_test_sql(&decl, "fct_orders").unwrap();
        assert_eq!(
            sql,
            "SELECT t.customer_id FROM fct_orders t \
             LEFT JOIN analytics.marts.dim_customers r ON t.customer_id = r.id \
             WHERE r.id IS NULL AND t.customer_id IS NOT NULL"
        );
    }

    #[test]
    fn test_sql_expression() {
        let decl = TestDecl {
            test_type: TestType::Expression {
                expression: "amount > 0".into(),
            },
            column: None,
            severity: TestSeverity::Error,
            filter: None,
        };
        let sql = generate_test_sql(&decl, "orders").unwrap();
        assert_eq!(sql, "SELECT COUNT(*) FROM orders WHERE NOT (amount > 0)");
    }

    #[test]
    fn test_sql_row_count_range() {
        let decl = TestDecl {
            test_type: TestType::RowCountRange {
                min: Some(1),
                max: Some(1_000_000),
            },
            column: None,
            severity: TestSeverity::Error,
            filter: None,
        };
        let sql = generate_test_sql(&decl, "orders").unwrap();
        assert_eq!(sql, "SELECT COUNT(*) FROM orders");
    }

    // ----- Error cases -----

    #[test]
    fn test_not_null_missing_column() {
        let decl = TestDecl {
            test_type: TestType::NotNull,
            column: None,
            severity: TestSeverity::Error,
            filter: None,
        };
        let result = generate_test_sql(&decl, "t");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("requires a column"));
    }

    #[test]
    fn test_unique_missing_column() {
        let decl = TestDecl {
            test_type: TestType::Unique,
            column: None,
            severity: TestSeverity::Error,
            filter: None,
        };
        let result = generate_test_sql(&decl, "t");
        assert!(result.is_err());
    }

    #[test]
    fn test_accepted_values_empty_values() {
        let decl = TestDecl {
            test_type: TestType::AcceptedValues { values: vec![] },
            column: Some("status".into()),
            severity: TestSeverity::Error,
            filter: None,
        };
        let result = generate_test_sql(&decl, "t");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("at least one value"));
    }

    #[test]
    fn test_accepted_values_missing_column() {
        let decl = TestDecl {
            test_type: TestType::AcceptedValues {
                values: vec!["a".into()],
            },
            column: None,
            severity: TestSeverity::Error,
            filter: None,
        };
        let result = generate_test_sql(&decl, "t");
        assert!(result.is_err());
    }

    #[test]
    fn test_relationships_missing_column() {
        let decl = TestDecl {
            test_type: TestType::Relationships {
                to_table: "ref_table".into(),
                to_column: "id".into(),
            },
            column: None,
            severity: TestSeverity::Error,
            filter: None,
        };
        let result = generate_test_sql(&decl, "t");
        assert!(result.is_err());
    }

    #[test]
    fn test_expression_empty() {
        let decl = TestDecl {
            test_type: TestType::Expression {
                expression: "  ".into(),
            },
            column: None,
            severity: TestSeverity::Error,
            filter: None,
        };
        let result = generate_test_sql(&decl, "t");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("expression"));
    }

    #[test]
    fn test_row_count_range_no_bounds() {
        let decl = TestDecl {
            test_type: TestType::RowCountRange {
                min: None,
                max: None,
            },
            column: None,
            severity: TestSeverity::Error,
            filter: None,
        };
        let result = generate_test_sql(&decl, "t");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("min"));
    }

    #[test]
    fn test_invalid_table_name_rejected() {
        let decl = TestDecl {
            test_type: TestType::NotNull,
            column: Some("col".into()),
            severity: TestSeverity::Error,
            filter: None,
        };
        let result = generate_test_sql(&decl, "drop table; --");
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_column_name_rejected() {
        let decl = TestDecl {
            test_type: TestType::NotNull,
            column: Some("col; DROP TABLE".into()),
            severity: TestSeverity::Error,
            filter: None,
        };
        let result = generate_test_sql(&decl, "t");
        assert!(result.is_err());
    }

    // ----- Phase 4a: InRange -----

    #[test]
    fn test_in_range_deser_both_bounds() {
        let toml_str = r#"
type = "in_range"
column = "amount"
min = "0"
max = "1000"
"#;
        let decl: TestDecl = toml::from_str(toml_str).unwrap();
        if let TestType::InRange { min, max } = &decl.test_type {
            assert_eq!(min.as_deref(), Some("0"));
            assert_eq!(max.as_deref(), Some("1000"));
        } else {
            panic!("expected InRange");
        }
    }

    #[test]
    fn test_sql_in_range_numeric_both() {
        let decl = TestDecl {
            test_type: TestType::InRange {
                min: Some("0".into()),
                max: Some("1000".into()),
            },
            column: Some("amount".into()),
            severity: TestSeverity::Error,
            filter: None,
        };
        let sql = generate_test_sql(&decl, "orders").unwrap();
        assert_eq!(
            sql,
            "SELECT COUNT(*) FROM orders WHERE (amount < 0 OR amount > 1000)"
        );
    }

    #[test]
    fn test_sql_in_range_min_only() {
        let decl = TestDecl {
            test_type: TestType::InRange {
                min: Some("1".into()),
                max: None,
            },
            column: Some("amount".into()),
            severity: TestSeverity::Error,
            filter: None,
        };
        let sql = generate_test_sql(&decl, "orders").unwrap();
        assert_eq!(sql, "SELECT COUNT(*) FROM orders WHERE (amount < 1)");
    }

    #[test]
    fn test_sql_in_range_formats_decimal_bounds() {
        let decl = TestDecl {
            test_type: TestType::InRange {
                min: Some("0.5".into()),
                max: Some("99.95".into()),
            },
            column: Some("ratio".into()),
            severity: TestSeverity::Error,
            filter: None,
        };
        let sql = generate_test_sql(&decl, "t").unwrap();
        assert!(sql.contains("ratio < 0.5"));
        assert!(sql.contains("ratio > 99.95"));
    }

    #[test]
    fn test_in_range_rejects_non_numeric() {
        let decl = TestDecl {
            test_type: TestType::InRange {
                min: Some("2026-01-01".into()),
                max: None,
            },
            column: Some("created_at".into()),
            severity: TestSeverity::Error,
            filter: None,
        };
        let result = generate_test_sql(&decl, "t");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("numeric ranges only")
        );
    }

    #[test]
    fn test_in_range_requires_at_least_one_bound() {
        let decl = TestDecl {
            test_type: TestType::InRange {
                min: None,
                max: None,
            },
            column: Some("amount".into()),
            severity: TestSeverity::Error,
            filter: None,
        };
        let result = generate_test_sql(&decl, "t");
        assert!(result.is_err());
    }

    // ----- Phase 4a: RegexMatch -----

    #[test]
    fn test_regex_match_deser() {
        let toml_str = r#"
type = "regex_match"
column = "email"
pattern = "^[a-z]+@example\\.com$"
"#;
        let decl: TestDecl = toml::from_str(toml_str).unwrap();
        if let TestType::RegexMatch { pattern } = &decl.test_type {
            assert_eq!(pattern, r"^[a-z]+@example\.com$");
        } else {
            panic!("expected RegexMatch");
        }
    }

    #[test]
    fn test_regex_match_needs_dialect() {
        // Non-dialect helper errors on RegexMatch (matches SQL needs dialect).
        let decl = TestDecl {
            test_type: TestType::RegexMatch {
                pattern: "^[a-z]+$".into(),
            },
            column: Some("email".into()),
            severity: TestSeverity::Error,
            filter: None,
        };
        let result = generate_test_sql(&decl, "t");
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_regex_pattern_allowlist() {
        assert!(validate_regex_pattern("^[a-z]+@example\\.com$").is_ok());
        assert!(validate_regex_pattern("").is_err());
        assert!(validate_regex_pattern("foo'bar").is_err()); // single quote
        assert!(validate_regex_pattern("foo;bar").is_err()); // semicolon
        assert!(validate_regex_pattern("foo`bar").is_err()); // backtick
    }

    // ----- Phase 4a: filter -----

    #[test]
    fn test_filter_wraps_not_null() {
        let decl = TestDecl {
            test_type: TestType::NotNull,
            column: Some("customer_id".into()),
            severity: TestSeverity::Error,
            filter: Some("created_at > '2026-01-01'".into()),
        };
        let sql = generate_test_sql(&decl, "orders").unwrap();
        assert_eq!(
            sql,
            "SELECT COUNT(*) FROM orders WHERE (created_at > '2026-01-01') AND customer_id IS NULL"
        );
    }

    #[test]
    fn test_filter_wraps_unique() {
        let decl = TestDecl {
            test_type: TestType::Unique,
            column: Some("email".into()),
            severity: TestSeverity::Error,
            filter: Some("active = true".into()),
        };
        let sql = generate_test_sql(&decl, "customers").unwrap();
        assert_eq!(
            sql,
            "SELECT email, COUNT(*) FROM customers WHERE (active = true) GROUP BY email HAVING COUNT(*) > 1"
        );
    }

    #[test]
    fn test_filter_wraps_accepted_values() {
        let decl = TestDecl {
            test_type: TestType::AcceptedValues {
                values: vec!["pending".into(), "shipped".into()],
            },
            column: Some("status".into()),
            severity: TestSeverity::Error,
            filter: Some("region = 'US'".into()),
        };
        let sql = generate_test_sql(&decl, "orders").unwrap();
        assert!(sql.contains("WHERE (region = 'US') AND status NOT IN"));
    }

    #[test]
    fn test_filter_blank_is_ignored() {
        let decl = TestDecl {
            test_type: TestType::NotNull,
            column: Some("id".into()),
            severity: TestSeverity::Error,
            filter: Some("   ".into()),
        };
        let sql = generate_test_sql(&decl, "t").unwrap();
        assert_eq!(sql, "SELECT COUNT(*) FROM t WHERE id IS NULL");
    }
}
