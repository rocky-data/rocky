//! Dialect-portability linter. Detects SQL constructs that don't run
//! portably across every warehouse Rocky supports, so a project targeting
//! a specific dialect can reject the ones that won't compile on its target.
//!
//! Detection is AST-based: we parse via [`crate::parser::parse_sql`] and
//! walk the tree for function calls, `QUALIFY` clauses, `ILIKE` operators,
//! and Snowflake-specific `FLATTEN` table functions. Substring scanning
//! would false-positive on identifiers and string literals, which is
//! unacceptable when the outcome is an error-severity diagnostic.
//!
//! The catalog mirrors what [`crate::transpile`] already knows how to
//! translate — this module promotes that knowledge from
//! "transpile-if-asked" to "reject-if-not-native for this target." No new
//! constructs are invented here; wave 1 deliberately ships only the
//! existing known-divergent set.
//!
//! Detection does not try to be dialect-correct beyond the catalog: a
//! construct we don't know about is silently portable. That's safer than
//! false-positive-heavy over-rejection for the first wave.

use sqlparser::ast::{Expr, Function, ObjectName, Query, TableFactor};
use sqlparser::ast::{Visit, Visitor};
use std::ops::ControlFlow;

use crate::parser::parse_sql;
use crate::transpile::Dialect;

/// A portability issue found in a SQL body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortabilityIssue {
    /// Short construct label (e.g., `"NVL"`, `"QUALIFY"`, `"ILIKE"`).
    pub construct: String,
    /// Dialects that support this construct natively.
    pub supported_by: Vec<Dialect>,
    /// The target dialect that does NOT support it.
    pub target: Dialect,
    /// Human-readable one-liner with the portable alternative.
    pub suggestion: String,
}

/// Check a SQL body for constructs that don't run on `target`.
///
/// Returns an empty vec when the SQL parses clean and uses only
/// target-portable constructs. Parse failures return an empty vec too —
/// the compile pipeline already surfaces parse errors elsewhere, and
/// re-emitting them here would double-report.
pub fn detect_portability_issues(sql: &str, target: Dialect) -> Vec<PortabilityIssue> {
    let statements = match parse_sql(sql) {
        Ok(s) => s,
        Err(_) => return Vec::new(),
    };

    let mut visitor = PortabilityVisitor {
        target,
        issues: Vec::new(),
    };
    for stmt in &statements {
        let _ = stmt.visit(&mut visitor);
    }
    visitor.issues
}

struct PortabilityVisitor {
    target: Dialect,
    issues: Vec<PortabilityIssue>,
}

impl Visitor for PortabilityVisitor {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        match expr {
            Expr::Function(Function { name, .. }) => {
                if let Some(issue) = check_function_name(name, self.target) {
                    self.issues.push(issue);
                }
            }
            Expr::ILike { .. } => {
                if !supports_ilike(self.target) {
                    self.issues.push(PortabilityIssue {
                        construct: "ILIKE".to_string(),
                        supported_by: vec![Dialect::Snowflake, Dialect::DuckDB, Dialect::Databricks],
                        target: self.target,
                        suggestion: "use LOWER(lhs) LIKE LOWER(pattern) for portable case-insensitive matching".to_string(),
                    });
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<Self::Break> {
        if let sqlparser::ast::SetExpr::Select(select) = &*query.body
            && select.qualify.is_some()
            && !supports_qualify(self.target)
        {
            self.issues.push(PortabilityIssue {
                construct: "QUALIFY".to_string(),
                supported_by: vec![Dialect::Snowflake, Dialect::Databricks, Dialect::BigQuery],
                target: self.target,
                suggestion: "wrap the query in a subquery and move the QUALIFY predicate to WHERE"
                    .to_string(),
            });
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, table_factor: &TableFactor) -> ControlFlow<Self::Break> {
        if let TableFactor::Function { name, .. } = table_factor
            && is_snowflake_flatten(name)
            && self.target != Dialect::Snowflake
        {
            self.issues.push(PortabilityIssue {
                construct: "FLATTEN".to_string(),
                supported_by: vec![Dialect::Snowflake],
                target: self.target,
                suggestion:
                    "replace FLATTEN(input => col) with UNNEST(col) for BigQuery/Databricks/DuckDB"
                        .to_string(),
            });
        }
        ControlFlow::Continue(())
    }
}

/// Catalog of function names that Rocky knows about and their native
/// dialects. Mirrors `transpile::get_function_mappings`.
fn check_function_name(name: &ObjectName, target: Dialect) -> Option<PortabilityIssue> {
    let last = name.0.last()?;
    let ident = match last {
        sqlparser::ast::ObjectNamePart::Identifier(i) => i,
        _ => return None,
    };
    let upper = ident.value.to_ascii_uppercase();

    let (construct, native) = match upper.as_str() {
        // Snowflake / Databricks
        "NVL" => ("NVL", &[Dialect::Snowflake, Dialect::Databricks][..]),
        // BigQuery-only null-coalesce spelling
        "IFNULL" => ("IFNULL", &[Dialect::BigQuery, Dialect::DuckDB][..]),
        // Snowflake legacy date math
        "DATEADD" => ("DATEADD", &[Dialect::Snowflake, Dialect::Databricks][..]),
        "DATEDIFF" => ("DATEDIFF", &[Dialect::Snowflake, Dialect::Databricks][..]),
        // BigQuery date math
        "DATE_ADD" => ("DATE_ADD", &[Dialect::BigQuery, Dialect::Databricks][..]),
        "DATE_DIFF" => ("DATE_DIFF", &[Dialect::BigQuery, Dialect::Databricks][..]),
        // Snowflake cast helpers
        "TO_VARCHAR" => ("TO_VARCHAR", &[Dialect::Snowflake][..]),
        "TO_CHAR" => (
            "TO_CHAR",
            &[Dialect::Snowflake, Dialect::Databricks, Dialect::DuckDB][..],
        ),
        "TO_DATE" => (
            "TO_DATE",
            &[Dialect::Snowflake, Dialect::Databricks, Dialect::DuckDB][..],
        ),
        "TO_TIMESTAMP" => (
            "TO_TIMESTAMP",
            &[Dialect::Snowflake, Dialect::Databricks, Dialect::DuckDB][..],
        ),
        // BigQuery cast helpers
        "PARSE_DATE" => ("PARSE_DATE", &[Dialect::BigQuery][..]),
        "PARSE_TIMESTAMP" => ("PARSE_TIMESTAMP", &[Dialect::BigQuery][..]),
        "FORMAT_TIMESTAMP" => ("FORMAT_TIMESTAMP", &[Dialect::BigQuery][..]),
        // String length/position
        "LEN" => ("LEN", &[Dialect::Snowflake][..]),
        "CHARINDEX" => ("CHARINDEX", &[Dialect::Snowflake][..]),
        // Array helpers
        "ARRAY_SIZE" => ("ARRAY_SIZE", &[Dialect::Snowflake][..]),
        // DATE_FORMAT — Databricks only
        "DATE_FORMAT" => ("DATE_FORMAT", &[Dialect::Databricks, Dialect::DuckDB][..]),
        _ => return None,
    };

    if native.contains(&target) {
        return None;
    }

    Some(PortabilityIssue {
        construct: construct.to_string(),
        supported_by: native.to_vec(),
        target,
        suggestion: suggestion_for(construct, target),
    })
}

fn supports_qualify(target: Dialect) -> bool {
    matches!(
        target,
        Dialect::Snowflake | Dialect::Databricks | Dialect::BigQuery
    )
}

fn supports_ilike(target: Dialect) -> bool {
    matches!(
        target,
        Dialect::Snowflake | Dialect::DuckDB | Dialect::Databricks
    )
}

fn is_snowflake_flatten(name: &ObjectName) -> bool {
    name.0.len() == 1
        && matches!(
            &name.0[0],
            sqlparser::ast::ObjectNamePart::Identifier(i) if i.value.eq_ignore_ascii_case("FLATTEN")
        )
}

fn suggestion_for(construct: &str, target: Dialect) -> String {
    match (construct, target) {
        ("NVL", Dialect::BigQuery) => "replace NVL(...) with IFNULL(...) or COALESCE(...)".into(),
        ("NVL", Dialect::DuckDB) => "replace NVL(...) with COALESCE(...)".into(),
        ("IFNULL", _) => "replace IFNULL(...) with COALESCE(...)".into(),
        ("DATEADD", Dialect::BigQuery) => "replace DATEADD(...) with DATE_ADD(...)".into(),
        ("DATEDIFF", Dialect::BigQuery) => "replace DATEDIFF(...) with DATE_DIFF(...)".into(),
        ("DATE_ADD", Dialect::Snowflake) => "replace DATE_ADD(...) with DATEADD(...)".into(),
        ("DATE_DIFF", Dialect::Snowflake) => "replace DATE_DIFF(...) with DATEDIFF(...)".into(),
        ("TO_VARCHAR", Dialect::BigQuery) => {
            "replace TO_VARCHAR(...) with FORMAT_TIMESTAMP(...) or CAST(... AS STRING)".into()
        }
        ("TO_VARCHAR", Dialect::Databricks) => {
            "replace TO_VARCHAR(...) with DATE_FORMAT(...) or CAST(... AS STRING)".into()
        }
        ("TO_VARCHAR", Dialect::DuckDB) => {
            "replace TO_VARCHAR(...) with CAST(... AS VARCHAR)".into()
        }
        ("LEN", _) => "replace LEN(...) with LENGTH(...)".into(),
        ("CHARINDEX", Dialect::BigQuery) => "replace CHARINDEX(...) with STRPOS(...)".into(),
        ("CHARINDEX", _) => "replace CHARINDEX(...) with POSITION(...)".into(),
        ("ARRAY_SIZE", Dialect::BigQuery) => {
            "replace ARRAY_SIZE(...) with ARRAY_LENGTH(...)".into()
        }
        ("ARRAY_SIZE", _) => "replace ARRAY_SIZE(...) with cardinality(...)".into(),
        ("DATE_FORMAT", Dialect::BigQuery) => {
            "replace DATE_FORMAT(...) with FORMAT_TIMESTAMP(...)".into()
        }
        ("DATE_FORMAT", Dialect::Snowflake) => {
            "replace DATE_FORMAT(...) with TO_VARCHAR(...)".into()
        }
        ("PARSE_DATE", _) => "replace PARSE_DATE(...) with TO_DATE(...)".into(),
        ("PARSE_TIMESTAMP", _) => "replace PARSE_TIMESTAMP(...) with TO_TIMESTAMP(...)".into(),
        ("FORMAT_TIMESTAMP", Dialect::Snowflake) => {
            "replace FORMAT_TIMESTAMP(...) with TO_VARCHAR(...)".into()
        }
        ("FORMAT_TIMESTAMP", _) => "replace FORMAT_TIMESTAMP(...) with DATE_FORMAT(...)".into(),
        ("TO_CHAR" | "TO_DATE" | "TO_TIMESTAMP", Dialect::BigQuery) => {
            format!("replace {construct}(...) with the BigQuery equivalent (PARSE_*/FORMAT_*)")
        }
        _ => format!("{construct} is not portable to {target}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clean_sql_has_no_issues() {
        let issues = detect_portability_issues("SELECT id, name FROM users", Dialect::BigQuery);
        assert!(issues.is_empty());
    }

    #[test]
    fn nvl_flagged_on_bigquery() {
        let issues = detect_portability_issues("SELECT NVL(a, b) FROM t", Dialect::BigQuery);
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].construct, "NVL");
    }

    #[test]
    fn nvl_ok_on_snowflake() {
        let issues = detect_portability_issues("SELECT NVL(a, b) FROM t", Dialect::Snowflake);
        assert!(issues.is_empty());
    }

    #[test]
    fn ifnull_flagged_on_snowflake() {
        let issues = detect_portability_issues("SELECT IFNULL(a, b) FROM t", Dialect::Snowflake);
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].construct, "IFNULL");
    }

    #[test]
    fn qualify_flagged_on_duckdb() {
        let issues = detect_portability_issues(
            "SELECT id, ROW_NUMBER() OVER (PARTITION BY g ORDER BY t) AS rn FROM t QUALIFY rn = 1",
            Dialect::DuckDB,
        );
        assert!(issues.iter().any(|i| i.construct == "QUALIFY"));
    }

    #[test]
    fn qualify_ok_on_snowflake() {
        let issues = detect_portability_issues(
            "SELECT id FROM t QUALIFY ROW_NUMBER() OVER (PARTITION BY g ORDER BY t) = 1",
            Dialect::Snowflake,
        );
        assert!(issues.is_empty());
    }

    #[test]
    fn ilike_flagged_on_bigquery() {
        let issues =
            detect_portability_issues("SELECT * FROM t WHERE name ILIKE 'a%'", Dialect::BigQuery);
        assert!(issues.iter().any(|i| i.construct == "ILIKE"));
    }

    #[test]
    fn ilike_ok_on_snowflake() {
        let issues =
            detect_portability_issues("SELECT * FROM t WHERE name ILIKE 'a%'", Dialect::Snowflake);
        assert!(issues.is_empty());
    }

    #[test]
    fn dateadd_flagged_on_bigquery() {
        let issues =
            detect_portability_issues("SELECT DATEADD(day, 1, ts) FROM t", Dialect::BigQuery);
        assert!(issues.iter().any(|i| i.construct == "DATEADD"));
    }

    #[test]
    fn date_add_flagged_on_snowflake() {
        let issues = detect_portability_issues(
            "SELECT DATE_ADD(ts, INTERVAL 1 DAY) FROM t",
            Dialect::Snowflake,
        );
        assert!(issues.iter().any(|i| i.construct == "DATE_ADD"));
    }

    #[test]
    fn to_varchar_flagged_on_non_snowflake() {
        let issues = detect_portability_issues("SELECT TO_VARCHAR(x) FROM t", Dialect::BigQuery);
        assert!(issues.iter().any(|i| i.construct == "TO_VARCHAR"));
    }

    #[test]
    fn len_flagged_on_non_snowflake() {
        let issues = detect_portability_issues("SELECT LEN(s) FROM t", Dialect::DuckDB);
        assert!(issues.iter().any(|i| i.construct == "LEN"));
    }

    #[test]
    fn array_size_flagged_on_bigquery() {
        let issues = detect_portability_issues("SELECT ARRAY_SIZE(arr) FROM t", Dialect::BigQuery);
        assert!(issues.iter().any(|i| i.construct == "ARRAY_SIZE"));
    }

    #[test]
    fn no_false_positive_on_identifier_named_nvl() {
        // `nvl` as a column name should not trigger — it's not a function call.
        let issues = detect_portability_issues("SELECT nvl FROM t", Dialect::BigQuery);
        assert!(
            issues.is_empty(),
            "false positive on column named nvl: {issues:?}"
        );
    }

    #[test]
    fn no_false_positive_on_string_literal() {
        // `NVL` inside a string literal should not trigger.
        let issues =
            detect_portability_issues("SELECT 'QUALIFY NVL(x)' AS msg FROM t", Dialect::BigQuery);
        assert!(
            issues.is_empty(),
            "false positive on string literal: {issues:?}"
        );
    }

    #[test]
    fn parse_failure_returns_empty() {
        // Garbled SQL → empty (not panic, not error). Parse errors are
        // surfaced by the upstream compile pipeline.
        let issues = detect_portability_issues("SELEC BOGUS", Dialect::BigQuery);
        assert!(issues.is_empty());
    }
}
