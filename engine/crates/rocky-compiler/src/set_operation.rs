//! E071 / W071: set-operation branches that return different numbers of
//! columns (#2661).
//!
//! `SELECT a, b FROM t UNION ALL SELECT a FROM u` is refused by every
//! warehouse. Type inference already notices the mismatch, but only to give
//! up on the model's columns, so the model compiled clean and failed at run
//! time.
//!
//! Outside any CTE body the finding is the error `E071`: every warehouse
//! binds the whole main query, a subquery that never runs included. Inside
//! a CTE body or a named `WINDOW` definition it is the warning `W071`, since
//! DuckDB never binds a CTE nothing reads or a window nothing uses, and
//! telling which are used needs name resolution.
//! The check itself needs none: it compares the widths the syntax states.
//!
//! A branch's width is counted only when the SQL states it: a `*`, a
//! `t.*`, a multi-alias item, and a call that expands to several columns
//! (`COLUMNS(...)`, `UNNEST`, `EXPLODE`, ...) make it unknown, and an
//! unknown width is never a mismatch. `UNION BY NAME` matches columns by
//! name, not position, and is not checked.

use std::collections::HashSet;

use sqlparser::ast::{Expr, Query, SelectItem, SetExpr, SetQuantifier, Statement, Visit, Visitor};
use sqlparser::parser::Parser;

use crate::diagnostic::{Diagnostic, E071, W071};

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
        skippable: HashSet::new(),
        in_skippable: 0,
        out: Vec::new(),
    };
    let _ = statements.visit(&mut visitor);
    visitor.out
}

struct Queries<'m> {
    model_name: &'m str,
    /// Every query a warehouse may skip: CTE bodies and the subqueries of
    /// named window definitions. They are added when the visitor enters the
    /// query that holds them, so each is here before the visitor reaches it.
    skippable: HashSet<*const Query>,
    /// How many skippable queries the visitor is inside.
    in_skippable: usize,
    out: Vec<Diagnostic>,
}

impl Visitor for Queries<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, query: &Query) -> std::ops::ControlFlow<()> {
        if self.skippable.contains(&std::ptr::from_ref(query)) {
            self.in_skippable += 1;
        }
        if let Some(with) = &query.with {
            self.skippable.extend(
                with.cte_tables
                    .iter()
                    .map(|cte| std::ptr::from_ref(&*cte.query)),
            );
        }
        named_window_queries(&query.body, &mut self.skippable);
        let place = if self.in_skippable == 0 {
            Place::MainQuery
        } else {
            Place::CteBody
        };
        report(&query.body, self.model_name, place, &mut self.out);
        std::ops::ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, query: &Query) -> std::ops::ControlFlow<()> {
        if self.skippable.contains(&std::ptr::from_ref(query)) {
            self.in_skippable -= 1;
        }
        std::ops::ControlFlow::Continue(())
    }
}

/// Add the subqueries of every named `WINDOW` definition in `body`'s
/// selects to `out`. A parenthesised query in `body` is its own query and is
/// handled when the visitor enters it.
fn named_window_queries(body: &SetExpr, out: &mut HashSet<*const Query>) {
    struct Subqueries<'o>(&'o mut HashSet<*const Query>);
    impl Visitor for Subqueries<'_> {
        type Break = ();
        fn pre_visit_query(&mut self, query: &Query) -> std::ops::ControlFlow<()> {
            self.0.insert(std::ptr::from_ref(query));
            std::ops::ControlFlow::Continue(())
        }
    }
    match body {
        SetExpr::Select(select) => {
            let _ = select.named_window.visit(&mut Subqueries(out));
        }
        SetExpr::SetOperation { left, right, .. } => {
            named_window_queries(left, out);
            named_window_queries(right, out);
        }
        _ => {}
    }
}

/// Where a set operation sits, which sets the finding's severity.
#[derive(Clone, Copy)]
enum Place {
    /// Outside every CTE body: always bound, so an error (`E071`).
    MainQuery,
    /// Inside a CTE body or a named window definition, at any depth: bound
    /// only if it is used, so a warning (`W071`).
    CteBody,
}

/// Check the set operations of one query body. A parenthesised query inside
/// it is its own query, reported when the visitor reaches it.
fn report(body: &SetExpr, model_name: &str, place: Place, out: &mut Vec<Diagnostic>) {
    let SetExpr::SetOperation {
        op,
        set_quantifier,
        left,
        right,
    } = body
    else {
        return;
    };
    report(left, model_name, place, out);
    report(right, model_name, place, out);
    if by_name(set_quantifier) {
        return;
    }
    if let (Some(l), Some(r)) = (width(left), width(right))
        && l != r
    {
        let message = format!(
            "the branches of this {op} return different numbers of columns: {l} on the \
             left, {r} on the right. SQL requires the same number in every branch"
        );
        let diagnostic = match place {
            Place::MainQuery => Diagnostic::error(E071, model_name, message),
            Place::CteBody => Diagnostic::warning(
                W071,
                model_name,
                format!(
                    "{message}. It is inside a CTE or a named window, which the warehouse may \
                     skip if nothing uses it"
                ),
            ),
        };
        out.push(diagnostic.with_suggestion(
            "select the same columns, in the same order, in every branch".to_string(),
        ));
    }
}

/// Whether the set operation matches columns by name, not position. Every
/// quantifier is named, so a new one fails to compile here rather than being
/// checked as positional.
fn by_name(quantifier: &SetQuantifier) -> bool {
    match quantifier {
        SetQuantifier::All | SetQuantifier::Distinct | SetQuantifier::None => false,
        SetQuantifier::ByName | SetQuantifier::AllByName | SetQuantifier::DistinctByName => true,
    }
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
            match (width(left), width(right)) {
                // A mismatch inside is reported there, once.
                (Some(l), Some(r)) if l != r => None,
                (l, r) => l.or(r),
            }
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
pub(crate) fn expands_to_columns(expr: &Expr) -> bool {
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
    "untuple",
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

    /// A chain with one mismatch is reported once.
    #[test]
    fn a_chain_reports_one_mismatch_once() {
        assert_eq!(
            codes("select a, b from t union all select a from u union all select a from v"),
            1
        );
    }

    #[test]
    fn an_unknown_or_matching_width_is_clean() {
        for sql in [
            "select a, b from t union all select a, b from u",
            "select * from t union all select a, b from u",
            "select t.* from t union all select a, b from u",
            "select a, * from t union all select a from u",
            "select a from t union all select * exclude (b), c from u",
            "select columns('x_.*') from t union all select a, b, c from u",
            "select unnest(s) from t union all select a, b, c from u",
            "select explode(arr) from t union all select a, b, c from u",
            "select stack(2, a, b) from t union all select a, b, c from u",
            "select a, b from t union all by name select a from u",
            "select a, b from t union by name select a from u",
            "select a from t union all values (1), (2)",
            "select a, b from t",
            "select a, b from t union all select untuple(p) from u",
        ] {
            assert_eq!(codes(sql), 0, "{sql}");
        }
    }

    /// Each call that can return several columns makes its branch's width
    /// unknown, against a branch of any width.
    #[test]
    fn every_expander_makes_the_width_unknown() {
        // Listed here, not read from `EXPANDERS`, so dropping a name fails.
        for name in [
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
            "untuple",
            "UNNEST",
        ] {
            let sql = format!("select {name}(x) from t union all select a, b, c from u");
            assert_eq!(codes(&sql), 0, "{sql}");
        }
        assert_eq!(
            codes("select lower(x) from t union all select a, b, c from u"),
            1
        );
    }

    /// A named window nothing uses is never bound by DuckDB, like an unread
    /// CTE, so a mismatch in a window definition is `W071` (round-9 red
    /// team, #2661).
    #[test]
    fn a_mismatch_in_a_named_window_is_a_warning() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE t (a INTEGER); CREATE TABLE u (a INTEGER, b INTEGER)")
            .unwrap();
        let bad = "(SELECT a FROM u UNION ALL SELECT a, b FROM u)";
        for sql in [
            format!("SELECT a FROM t WINDOW w AS (ORDER BY {bad})"),
            format!("SELECT a FROM t WINDOW w AS (ORDER BY {bad}) UNION ALL SELECT a FROM t"),
            format!("SELECT a FROM t UNION ALL SELECT a FROM t WINDOW w AS (ORDER BY {bad})"),
            format!("SELECT sum(a) OVER w FROM t WINDOW w AS (ORDER BY a), w2 AS (ORDER BY {bad})"),
        ] {
            assert_eq!(severities(&sql), ["W071"], "{sql}");
            conn.prepare(&sql)
                .unwrap_or_else(|e| panic!("DuckDB refuses {sql}: {e}"));
        }
        // An inline window is always used: still an error.
        assert_eq!(
            severities(&format!("SELECT sum(a) OVER (ORDER BY {bad}) FROM t")),
            ["E071"]
        );
    }

    /// DuckDB binds a set operation in a subquery that never runs.
    #[test]
    fn a_never_evaluated_subquery_is_still_checked() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE t (s VARCHAR); CREATE TABLE u (a INTEGER); \
             CREATE TABLE v (a INTEGER, b INTEGER)",
        )
        .unwrap();
        let sql = "SELECT 1 FROM t WHERE false AND s IN \
                   (SELECT a FROM u UNION ALL SELECT a, b FROM v)";
        assert_eq!(codes(sql), 1);
        assert!(conn.prepare(sql).is_err());
    }

    fn severities(sql: &str) -> Vec<&'static str> {
        check_set_operation_widths("m", sql)
            .iter()
            .map(|d| match &*d.code {
                "E071" => "E071",
                "W071" => "W071",
                other => panic!("unexpected {other}"),
            })
            .collect()
    }

    /// A mismatch inside any CTE body is the warning `W071`, read or not:
    /// DuckDB binds every query here (round 6 to 8 red team, #2661).
    /// Outside every CTE body it stays `E071`.
    #[test]
    fn a_mismatch_inside_a_cte_is_a_warning() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE u (a INTEGER); CREATE TABLE v (a INTEGER, b INTEGER); \
             CREATE TABLE \"Ä\" (a INTEGER)",
        )
        .unwrap();
        let bad = "SELECT a FROM u UNION ALL SELECT a, b FROM v";
        for sql in [
            format!("WITH x AS ({bad}) SELECT a, b FROM v"),
            format!("WITH x AS ({bad}), y AS (SELECT * FROM x) SELECT a FROM u"),
            format!("WITH x AS ({bad}) SELECT * FROM (WITH x AS (SELECT 1 a) SELECT * FROM x) s"),
            format!("WITH x AS ({bad}) SELECT * FROM (WITH y AS (SELECT * FROM x) SELECT 1 a) s"),
            format!("WITH x AS (SELECT * FROM v), v AS ({bad}) SELECT * FROM x"),
            format!("WITH \"ä\" AS ({bad}) SELECT * FROM \"Ä\""),
            format!("WITH x AS (SELECT * FROM (SELECT a FROM u WHERE a IN ({bad})) d) SELECT 1"),
        ] {
            assert_eq!(severities(&sql), ["W071"], "{sql}");
            conn.prepare(&sql)
                .unwrap_or_else(|e| panic!("DuckDB refuses {sql}: {e}"));
        }
        // A read CTE is still flagged, as a warning.
        assert_eq!(
            severities(&format!("WITH x AS ({bad}) SELECT * FROM x")),
            ["W071"]
        );
        // Outside every CTE body: an error, also next to a CTE.
        for sql in [
            bad.to_string(),
            format!("WITH c AS (SELECT 1 a) SELECT * FROM ({bad}) d"),
            "(WITH c AS (SELECT a FROM u) SELECT a FROM c) UNION ALL SELECT a, b FROM v"
                .to_string(),
            format!("SELECT * FROM (WITH c AS (SELECT 1 a) {bad}) d"),
        ] {
            assert_eq!(severities(&sql), ["E071"], "{sql}");
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
            let err = conn.prepare(sql).err().map(|e| e.to_string());
            assert!(
                err.as_deref()
                    .is_some_and(|e| e.contains("same number of result columns")),
                "DuckDB does not refuse {sql} for its column count: {err:?}"
            );
        }
    }
}
