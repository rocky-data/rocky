//! E071: set-operation branches that return different numbers of columns
//! (#2661).
//!
//! `SELECT a, b FROM t UNION ALL SELECT a FROM u` is refused by every
//! warehouse. Type inference already notices the mismatch, but only to give
//! up on the model's columns, so the model compiled clean and failed at run
//! time.
//!
//! A branch's width is counted only when the SQL states it: a `*`, a
//! `t.*`, a multi-alias item, and a call that expands to several columns
//! (`COLUMNS(...)`, `UNNEST`, `EXPLODE`, ...) make it unknown, and an
//! unknown width is never a mismatch. `UNION BY NAME` matches columns by
//! name, not position, and is not checked.

use sqlparser::ast::{Expr, Query, SelectItem, SetExpr, SetQuantifier, Statement, Visit, Visitor};
use sqlparser::parser::Parser;

use crate::diagnostic::{Diagnostic, E071};

/// Report every set operation in `sql` whose two sides have different known
/// column counts.
pub fn check_set_operation_widths(model_name: &str, sql: &str) -> Vec<Diagnostic> {
    let Ok(statements) = Parser::parse_sql(&rocky_sql::dialect::DatabricksDialect, sql) else {
        return Vec::new();
    };
    let [Statement::Query(_)] = statements.as_slice() else {
        return Vec::new();
    };
    let mut visitor = Queries {
        model_name,
        out: Vec::new(),
    };
    let _ = statements.visit(&mut visitor);
    visitor.out
}

struct Queries<'m> {
    model_name: &'m str,
    out: Vec<Diagnostic>,
}

impl Visitor for Queries<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, query: &Query) -> std::ops::ControlFlow<()> {
        report(&query.body, self.model_name, &mut self.out);
        std::ops::ControlFlow::Continue(())
    }
}

/// Check the set operations of one query body. A parenthesised query inside
/// it is its own query, reported when the visitor reaches it.
fn report(body: &SetExpr, model_name: &str, out: &mut Vec<Diagnostic>) {
    let SetExpr::SetOperation {
        op,
        set_quantifier,
        left,
        right,
    } = body
    else {
        return;
    };
    report(left, model_name, out);
    report(right, model_name, out);
    if by_name(set_quantifier) {
        return;
    }
    if let (Some(l), Some(r)) = (width(left), width(right))
        && l != r
    {
        out.push(
            Diagnostic::error(
                E071,
                model_name,
                format!(
                    "the branches of this {op} return different numbers of columns: {l} on the \
                     left, {r} on the right. Every warehouse refuses this"
                ),
            )
            .with_suggestion(
                "select the same columns, in the same order, in every branch".to_string(),
            ),
        );
    }
}

fn by_name(quantifier: &SetQuantifier) -> bool {
    matches!(
        quantifier,
        SetQuantifier::ByName | SetQuantifier::AllByName | SetQuantifier::DistinctByName
    )
}

/// The number of columns `body` returns, when the SQL states it.
fn width(body: &SetExpr) -> Option<usize> {
    match body {
        SetExpr::Select(select) => {
            if select.projection.is_empty() {
                return None;
            }
            for item in &select.projection {
                match item {
                    SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                        if expands_to_columns(expr) {
                            return None;
                        }
                    }
                    SelectItem::ExprWithAliases { .. }
                    | SelectItem::QualifiedWildcard(..)
                    | SelectItem::Wildcard(_) => return None,
                }
            }
            Some(select.projection.len())
        }
        SetExpr::Query(query) => width(&query.body),
        SetExpr::SetOperation {
            set_quantifier,
            left,
            right,
            ..
        } => {
            if by_name(set_quantifier) {
                return None;
            }
            width(left).or_else(|| width(right))
        }
        SetExpr::Values(values) => {
            let first = values.rows.first()?.len();
            values
                .rows
                .iter()
                .all(|row| row.len() == first)
                .then_some(first)
        }
        _ => None,
    }
}

/// Whether `expr` holds, at any depth, a call that can return more than one
/// column.
fn expands_to_columns(expr: &Expr) -> bool {
    struct Finder(bool);
    impl Visitor for Finder {
        type Break = ();
        fn pre_visit_expr(&mut self, expr: &Expr) -> std::ops::ControlFlow<()> {
            if let Expr::Function(function) = expr
                && let Some(last) = function.name.0.last().and_then(|p| p.as_ident())
                && EXPANDERS
                    .iter()
                    .any(|name| last.value.eq_ignore_ascii_case(name))
            {
                self.0 = true;
                return std::ops::ControlFlow::Break(());
            }
            std::ops::ControlFlow::Continue(())
        }
    }
    let mut finder = Finder(false);
    let _ = expr.visit(&mut finder);
    finder.0
}

/// Calls that can return several columns in a select list.
const EXPANDERS: &[&str] = &[
    "columns",
    "unnest",
    "explode",
    "explode_outer",
    "posexplode",
    "posexplode_outer",
    "inline",
    "inline_outer",
    "stack",
    "json_tuple",
    "unpack",
    "struct_unpack",
];

#[cfg(test)]
mod tests {
    use super::*;

    fn codes(sql: &str) -> usize {
        check_set_operation_widths("m", sql).len()
    }

    #[test]
    fn a_column_count_mismatch_is_refused() {
        for sql in [
            "select a, b from t union all select a from u",
            "select a from t union select a, b from u",
            "select a from t except select a, b from u",
            "select a from t intersect select a, b from u",
            "select a, b from t union all select a, b from u union all select a from v",
            "with c as (select a from t union all select a, b from u) select * from c",
            "select * from (select a from t union all select a, b from u) x",
            "select a, b from t union all values (1)",
            "select a from t where a in (select a from t union all select a, b from u)",
            "(select a, b from t) union all (select a from u)",
        ] {
            assert_eq!(codes(sql), 1, "{sql}");
        }
    }

    #[test]
    fn an_unknown_or_matching_width_is_clean() {
        for sql in [
            "select a, b from t union all select a, b from u",
            "select * from t union all select a, b from u",
            "select t.* from t union all select a, b from u",
            "select a, * from t union all select a from u",
            "select a from t union all select * exclude (b), c from u",
            "select columns('x_.*') from t union all select a from u",
            "select unnest(s) from t union all select a from u",
            "select explode(arr) from t union all select a from u",
            "select stack(2, a, b) from t union all select a from u",
            "select a, b from t union all by name select a from u",
            "select a, b from t union by name select a from u",
            "select a from t union all values (1), (2)",
            "select a, b from t",
        ] {
            assert_eq!(codes(sql), 0, "{sql}");
        }
    }

    /// The bundled DuckDB refuses every mismatch this check reports.
    #[test]
    fn duckdb_refuses_what_e071_reports() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE t (a INTEGER, b INTEGER); CREATE TABLE u (a INTEGER, b INTEGER); CREATE TABLE v (a INTEGER, b INTEGER)")
            .unwrap();
        for sql in [
            "select a, b from t union all select a from u",
            "select a from t union select a, b from u",
            "select a from t except select a, b from u",
            "select a from t intersect select a, b from u",
            "select a, b from t union all select a, b from u union all select a from v",
            "with c as (select a from t union all select a, b from u) select * from c",
            "select a, b from t union all values (1)",
            "(select a, b from t) union all (select a from u)",
        ] {
            assert_eq!(codes(sql), 1, "{sql}");
            assert!(conn.prepare(sql).is_err(), "DuckDB accepts {sql}");
        }
    }
}
