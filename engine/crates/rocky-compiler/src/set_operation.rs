//! E071 / W071: set-operation branches that return different numbers of
//! columns (#2661).
//!
//! `SELECT a, b FROM t UNION ALL SELECT a FROM u` is refused by every
//! warehouse. Type inference already notices the mismatch, but only to give
//! up on the model's columns, so the model compiled clean and failed at run
//! time.
//!
//! The finding is the error `E071` only where DuckDB is shown to bind the
//! query: the model's query, its set-operation branches, derived tables in
//! `FROM`, and subqueries in the select list, `WHERE`, `HAVING` and a join's
//! `ON` reached only through forms a macro cannot shadow ([`keeps_binding`]),
//! nested any number of times, even a subquery that never runs
//! ([`bound_subqueries`]). Everywhere else, a CTE body, a named `WINDOW`, an
//! `ORDER BY`, a function argument, an operand of an operator a macro can
//! shadow, or any place not on that list, it is the warning `W071`: DuckDB
//! skips a CTE nothing reads or a window nothing uses, a macro drops an
//! argument its body does not use, and an unlisted place may be skipped too.
//! A wildcard's `REPLACE` is not on the list either. The check needs no name
//! resolution: it compares the widths the syntax states.
//!
//! A branch's width is counted only when the SQL states it: a `*`, a
//! `t.*`, a multi-alias item, and a call that expands to several columns
//! (`COLUMNS(...)`, `UNNEST`, `EXPLODE`, ...) make it unknown, and an
//! unknown width is never a mismatch. `UNION BY NAME` matches columns by
//! name, not position, and is not checked.

use std::collections::HashSet;

use sqlparser::ast::{
    Expr, Function, InTarget, JoinConstraint, JoinOperator, Query, SelectItem, SetExpr,
    SetQuantifier, Statement, TableFactor, TableWithJoins, Visit, Visitor,
};
use sqlparser::parser::Parser;

use crate::diagnostic::{Diagnostic, E071, W071};

/// Report every set operation in `sql` whose two sides have different known
/// column counts.
pub fn check_set_operation_widths(model_name: &str, sql: &str) -> Vec<Diagnostic> {
    let Ok(statements) = Parser::parse_sql(&rocky_sql::dialect::DatabricksDialect, sql) else {
        return Vec::new();
    };
    let [Statement::Query(query)] = statements.as_slice() else {
        return Vec::new();
    };
    let mut places = Places {
        model_name,
        root: std::ptr::from_ref(&**query),
        bound: HashSet::new(),
        out: Vec::new(),
    };
    let _ = query.visit(&mut places);
    places.out
}

/// Where a set operation sits, which sets the finding's severity.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Place {
    /// Reached from the model's query only through places DuckDB is shown to
    /// bind ([`bound_subqueries`]), so the warehouse refuses it: `E071`.
    Bound,
    /// Anywhere else: a CTE body, a named window, an `ORDER BY`, a function
    /// argument, an operand of an operator a macro can shadow, a lambda, or
    /// any place not on that list. The warehouse may skip it: `W071`.
    MaybeSkipped,
}

/// Reports each query's set operations at its [`Place`]. The model's query
/// is bound. Entering a bound query records the queries directly in it at a
/// listed place ([`bound_subqueries`]), before the visitor reaches them; a
/// query not recorded, a CTE body or one in an unlisted place, is
/// [`Place::MaybeSkipped`], and so is everything inside it. A place nobody
/// listed can only give a warning.
struct Places<'m> {
    model_name: &'m str,
    root: *const Query,
    bound: HashSet<*const Query>,
    out: Vec<Diagnostic>,
}

impl Visitor for Places<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, query: &Query) -> std::ops::ControlFlow<()> {
        let this = std::ptr::from_ref(query);
        let place = if this == self.root || self.bound.contains(&this) {
            Place::Bound
        } else {
            Place::MaybeSkipped
        };
        if place == Place::Bound {
            bound_subqueries(&query.body, &mut self.bound);
        }
        report(&query.body, self.model_name, place, &mut self.out);
        std::ops::ControlFlow::Continue(())
    }
}

/// Add to `out` the queries directly in `body` at a place DuckDB binds
/// whenever it binds `body`, a subquery that never runs included (each is
/// covered by `duckdb_binds_every_listed_place`):
/// - a branch of a set operation, or the body in parentheses;
/// - a derived table in `FROM` or a join, `LATERAL` included;
/// - a subquery in the select list, `WHERE`, `HAVING` or a join's `ON`,
///   reached only through the expression forms [`keeps_binding`] lists.
fn bound_subqueries(body: &SetExpr, out: &mut HashSet<*const Query>) {
    let mut exprs = Vec::new();
    bound_places(body, &mut exprs, out);
    for expr in exprs {
        let _ = expr.visit(&mut ExprSubqueries(out, 0, 0));
    }
}

/// The places of `body` on the bound list: the expressions DuckDB binds
/// whenever it binds `body` (the select list, `WHERE`, `HAVING` and a join's
/// `ON`) go to `exprs`, and the queries directly in it at a bound place (a
/// set-operation branch in parentheses, a derived table) go to `queries`.
/// The one list both [`bound_subqueries`] and [`bound_calls`] read.
fn bound_places<'a>(
    body: &'a SetExpr,
    exprs: &mut Vec<&'a Expr>,
    queries: &mut HashSet<*const Query>,
) {
    match body {
        SetExpr::Select(select) => {
            // A wildcard's `REPLACE` and `EXCLUDE` are not on the list.
            for item in &select.projection {
                if let SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } = item
                {
                    exprs.push(expr);
                }
            }
            exprs.extend(select.selection.iter().chain(&select.having));
            for from in &select.from {
                bound_in_from(from, exprs, queries);
            }
        }
        SetExpr::SetOperation { left, right, .. } => {
            bound_places(left, exprs, queries);
            bound_places(right, exprs, queries);
        }
        SetExpr::Query(query) => {
            queries.insert(std::ptr::from_ref(&**query));
        }
        // Not on the list: `VALUES`, `TABLE`, `FROM`-first and the rest.
        _ => {}
    }
}

fn bound_in_from<'a>(
    from: &'a TableWithJoins,
    exprs: &mut Vec<&'a Expr>,
    queries: &mut HashSet<*const Query>,
) {
    bound_in_factor(&from.relation, exprs, queries);
    for join in &from.joins {
        bound_in_factor(&join.relation, exprs, queries);
        if let Some(JoinConstraint::On(on)) = on_constraint(&join.join_operator) {
            exprs.push(on);
        }
    }
}

/// The function calls in `query` that DuckDB binds whenever it binds the
/// model's query, by the rule `E071` uses: a call in a place on the bound
/// list of a bound query ([`bound_places`]), reached only through the forms
/// [`keeps_binding`] lists. A call passed as an argument to another call is
/// not one: a macro drops an argument its body does not use. Read by the
/// argument-count check `E068` (#2690); a call not returned here is at most
/// a warning (`W068`). The pointers are only valid for this parse of the SQL.
pub(crate) fn bound_calls(query: &Query) -> HashSet<*const Function> {
    struct BoundCalls {
        root: *const Query,
        bound: HashSet<*const Query>,
        calls: HashSet<*const Function>,
    }
    impl Visitor for BoundCalls {
        type Break = ();
        fn pre_visit_query(&mut self, query: &Query) -> std::ops::ControlFlow<()> {
            let this = std::ptr::from_ref(query);
            if this == self.root || self.bound.contains(&this) {
                bound_subqueries(&query.body, &mut self.bound);
                let mut exprs = Vec::new();
                bound_places(&query.body, &mut exprs, &mut HashSet::new());
                for expr in exprs {
                    let _ = expr.visit(&mut ExprCalls(&mut self.calls, 0, 0));
                }
            }
            std::ops::ControlFlow::Continue(())
        }
    }
    let mut visitor = BoundCalls {
        root: std::ptr::from_ref(query),
        bound: HashSet::new(),
        calls: HashSet::new(),
    };
    let _ = query.visit(&mut visitor);
    visitor.calls
}

/// Collects the outermost calls in an expression reached only through
/// [`keeps_binding`] forms, outside any subquery. Fields: output, query
/// depth, depth of other forms (a call is one).
struct ExprCalls<'o>(&'o mut HashSet<*const Function>, usize, usize);

impl Visitor for ExprCalls<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, _query: &Query) -> std::ops::ControlFlow<()> {
        self.1 += 1;
        std::ops::ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &Query) -> std::ops::ControlFlow<()> {
        self.1 -= 1;
        std::ops::ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &Expr) -> std::ops::ControlFlow<()> {
        if self.1 == 0
            && self.2 == 0
            && let Expr::Function(function) = expr
        {
            self.0.insert(std::ptr::from_ref(function));
        }
        if !keeps_binding(expr) {
            self.2 += 1;
        }
        std::ops::ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, expr: &Expr) -> std::ops::ControlFlow<()> {
        if !keeps_binding(expr) {
            self.2 -= 1;
        }
        std::ops::ControlFlow::Continue(())
    }
}

fn bound_in_factor<'a>(
    factor: &'a TableFactor,
    exprs: &mut Vec<&'a Expr>,
    queries: &mut HashSet<*const Query>,
) {
    match factor {
        TableFactor::Derived { subquery, .. } => {
            queries.insert(std::ptr::from_ref(&**subquery));
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => bound_in_from(table_with_joins, exprs, queries),
        // Not on the list: table functions, `UNNEST`, `PIVOT` and the rest.
        _ => {}
    }
}

/// The constraint of an ordinary inner or outer join. Other joins are not
/// on the list.
fn on_constraint(operator: &JoinOperator) -> Option<&JoinConstraint> {
    match operator {
        JoinOperator::Join(c)
        | JoinOperator::Inner(c)
        | JoinOperator::Left(c)
        | JoinOperator::LeftOuter(c)
        | JoinOperator::Right(c)
        | JoinOperator::RightOuter(c)
        | JoinOperator::FullOuter(c) => Some(c),
        _ => None,
    }
}

/// Whether a subquery under `expr` stays bound when `expr` is. This is an
/// allow-list of forms DuckDB binds in full and a user macro cannot shadow:
/// comparisons, `AND`, `OR`, `NOT`, `CASE`, `BETWEEN`, `IS [NOT] DISTINCT
/// FROM`, casts, `IN`, `EXISTS`, parentheses and a subquery itself (tests
/// show DuckDB refuses a mismatch under each, with a shadowing macro
/// defined). Anything else hides its operands: a function call, since a
/// macro drops an argument its body does not use; an operator or keyword
/// DuckDB rewrites into a call a macro can shadow (`+`, `||`, `LIKE`,
/// `TRIM`, ...); a lambda; and any form not listed.
fn keeps_binding(expr: &Expr) -> bool {
    use sqlparser::ast::{BinaryOperator as B, UnaryOperator as U};
    match expr {
        Expr::Nested(_)
        | Expr::Subquery(_)
        | Expr::Exists { .. }
        | Expr::In {
            target: InTarget::Subquery(_) | InTarget::List(_),
            ..
        }
        | Expr::Case { .. }
        | Expr::Between { .. }
        | Expr::IsDistinctFrom(..)
        | Expr::IsNotDistinctFrom(..)
        | Expr::Cast { .. } => true,
        Expr::BinaryOp { op, .. } => matches!(
            op,
            B::Eq | B::NotEq | B::Lt | B::LtEq | B::Gt | B::GtEq | B::And | B::Or
        ),
        Expr::UnaryOp { op, .. } => matches!(op, U::Not),
        _ => false,
    }
}

/// Collects the outermost queries in an expression reached only through
/// [`keeps_binding`] forms. Fields: output, query depth, depth of other
/// forms.
struct ExprSubqueries<'o>(&'o mut HashSet<*const Query>, usize, usize);

impl Visitor for ExprSubqueries<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, query: &Query) -> std::ops::ControlFlow<()> {
        if self.1 == 0 && self.2 == 0 {
            self.0.insert(std::ptr::from_ref(query));
        }
        self.1 += 1;
        std::ops::ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &Query) -> std::ops::ControlFlow<()> {
        self.1 -= 1;
        std::ops::ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &Expr) -> std::ops::ControlFlow<()> {
        if !keeps_binding(expr) {
            self.2 += 1;
        }
        std::ops::ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, expr: &Expr) -> std::ops::ControlFlow<()> {
        if !keeps_binding(expr) {
            self.2 -= 1;
        }
        std::ops::ControlFlow::Continue(())
    }
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
            Place::Bound => Diagnostic::error(E071, model_name, message),
            Place::MaybeSkipped => Diagnostic::warning(
                W071,
                model_name,
                format!(
                    "{message}. It is in a place the warehouse may skip, such as a CTE or a \
                     named window nothing uses"
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
        // An inline window belongs to a function call, whose arguments are
        // not on the bound list: a warning too.
        assert_eq!(
            severities(&format!("SELECT sum(a) OVER (ORDER BY {bad}) FROM t")),
            ["W071"]
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

    /// Every place on the bound list gives `E071`, and DuckDB refuses each
    /// one, also nested and also where the subquery never runs.
    #[test]
    fn duckdb_binds_every_listed_place() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE t (a INTEGER, s VARCHAR); CREATE TABLE u (a INTEGER, b INTEGER)",
        )
        .unwrap();
        // A macro named after each listed form, dropping its operands: none
        // of them is used, so the forms stay bound (round-12 red team).
        for name in [
            "=", "<>", "!=", "<", "<=", ">", ">=", "and", "or", "not", "in", "between", "cast",
            "try_cast", "case", "exists", "coalesce", "contains",
        ] {
            conn.execute_batch(&format!(
                "CREATE MACRO \"{name}\"(x) AS true, (x, y) AS true;"
            ))
            .unwrap_or_else(|e| panic!("macro {name}: {e}"));
        }
        let bad = "SELECT a FROM u UNION ALL SELECT a, b FROM u";
        for sql in [
            // The model's query, and a branch in parentheses.
            bad.to_string(),
            format!("SELECT a FROM t UNION ALL ({bad})"),
            format!("({bad})"),
            // Derived tables, LATERAL and nested joins included.
            format!("SELECT * FROM ({bad}) d"),
            format!("SELECT * FROM t JOIN ({bad}) d ON true"),
            format!("SELECT * FROM t, LATERAL ({bad}) d"),
            format!("SELECT * FROM (t JOIN ({bad}) d ON true)"),
            // Expression subqueries, never run included.
            format!("SELECT ({bad}) FROM t"),
            format!("SELECT CASE WHEN false THEN ({bad}) END FROM t"),
            format!("SELECT a FROM t WHERE false AND a IN ({bad})"),
            format!("SELECT a FROM t WHERE EXISTS ({bad})"),
            format!("SELECT a FROM t GROUP BY a HAVING false AND a IN ({bad})"),
            format!("SELECT * FROM t JOIN u ON false AND t.a IN ({bad})"),
            format!("SELECT * FROM t LEFT JOIN u ON t.a IN ({bad})"),
            // Nested through listed places.
            format!("SELECT * FROM (SELECT a FROM t WHERE a IN ({bad})) d"),
            format!("SELECT a FROM t WHERE a IN (SELECT a FROM ({bad}) d)"),
            format!("SELECT a FROM t WHERE a IN ({bad}) UNION ALL SELECT a FROM t"),
            // Each form `keeps_binding` lists.
            format!("SELECT a FROM t WHERE a = ({bad})"),
            format!("SELECT a FROM t WHERE a <> ({bad})"),
            format!("SELECT a FROM t WHERE a < ({bad})"),
            format!("SELECT a FROM t WHERE a <= ({bad})"),
            format!("SELECT a FROM t WHERE a > ({bad})"),
            format!("SELECT a FROM t WHERE a >= ({bad})"),
            format!("SELECT a FROM t WHERE true OR a IN ({bad})"),
            format!("SELECT a FROM t WHERE NOT EXISTS ({bad})"),
            format!("SELECT a FROM t WHERE NOT (a IN ({bad}))"),
            format!("SELECT a FROM t WHERE a BETWEEN ({bad}) AND 2"),
            format!("SELECT a FROM t WHERE a IS DISTINCT FROM ({bad})"),
            format!("SELECT a FROM t WHERE a IS NOT DISTINCT FROM ({bad})"),
            format!("SELECT CAST(({bad}) AS BIGINT) FROM t"),
            format!("SELECT a FROM t WHERE a IN (1, ({bad}))"),
            format!("SELECT ((({bad}))) FROM t"),
        ] {
            assert_eq!(severities(&sql), ["E071"], "{sql}");
            let err = conn.prepare(&sql).err().map(|e| e.to_string());
            assert!(
                err.as_deref()
                    .is_some_and(|e| e.contains("same number of result columns")),
                "DuckDB does not refuse {sql} for its column count: {err:?}"
            );
        }
    }

    /// A DuckDB macro drops an argument its body does not use before
    /// binding, so a subquery passed to any function call is `W071`
    /// (round-10 red team, #2661). DuckDB runs every query here.
    #[test]
    fn a_function_argument_is_a_warning() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE t (a INTEGER); CREATE TABLE u (a INTEGER, b INTEGER); \
             CREATE MACRO pick(x, fallback) AS CAST((x) AS BIGINT);",
        )
        .unwrap();
        let bad = "SELECT a FROM u UNION ALL SELECT a, b FROM u";
        for sql in [
            format!("SELECT has_table_privilege(({bad}), 'select')"),
            format!("SELECT a FROM t WHERE has_column_privilege('t', ({bad}), 'select')"),
            format!("SELECT * FROM (SELECT a, pg_table_is_visible(({bad})) v FROM t) d"),
            format!("SELECT pick(a, ({bad})) FROM t"),
        ] {
            assert_eq!(severities(&sql), ["W071"], "{sql}");
            conn.prepare(&sql)
                .unwrap_or_else(|e| panic!("DuckDB refuses {sql}: {e}"));
        }
    }

    /// DuckDB rewrites operators and keyword syntax into calls to built-ins,
    /// which a user macro can shadow and so drop an operand (round-11 red
    /// team, #2661). Only expressions on the transparent list keep a
    /// subquery bound. DuckDB runs every query here.
    #[test]
    fn a_shadowable_operator_is_a_warning() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE t (a INTEGER, s VARCHAR); CREATE TABLE u (a INTEGER, b INTEGER); \
             CREATE MACRO \"+\"(x, y) AS x; CREATE MACRO \"||\"(x, y) AS x; \
             CREATE MACRO \"~~\"(x, y) AS true; CREATE MACRO trim(x) AS 'k';",
        )
        .unwrap();
        let bad = "(SELECT a FROM u UNION ALL SELECT a, b FROM u)";
        for sql in [
            format!("SELECT a + {bad} AS v FROM t"),
            format!("SELECT s || {bad} AS v FROM t"),
            format!("SELECT a FROM t WHERE s LIKE {bad}"),
            format!("SELECT trim({bad}) AS v FROM t"),
        ] {
            assert_eq!(severities(&sql), ["W071"], "{sql}");
            conn.prepare(&sql)
                .unwrap_or_else(|e| panic!("DuckDB refuses {sql}: {e}"));
        }
    }

    /// A place not on the bound list is `W071`, and so is a listed place
    /// inside it.
    #[test]
    fn an_unlisted_place_is_a_warning() {
        let bad = "SELECT a FROM u UNION ALL SELECT a, b FROM u";
        for sql in [
            format!("WITH c AS ({bad}) SELECT * FROM c"),
            format!("SELECT a FROM t ORDER BY (SELECT count(*) FROM ({bad}) d)"),
            format!("SELECT a FROM t GROUP BY a, (SELECT 1 FROM ({bad}) d)"),
            format!("SELECT a FROM t QUALIFY a IN ({bad})"),
            format!("SELECT a FROM t LIMIT (SELECT count(*) FROM ({bad}) d)"),
            format!("SELECT list_transform([1], x -> (SELECT count(*) FROM ({bad}) d))"),
            format!("SELECT * FROM t SEMI JOIN u ON t.a IN ({bad})"),
            format!("SELECT * FROM range((SELECT count(*) FROM ({bad}) d))"),
            format!("SELECT a FROM t UNION ALL SELECT a FROM t ORDER BY (SELECT 1 FROM ({bad}) d)"),
            format!("SELECT a FROM t WINDOW w AS (ORDER BY ({bad}))"),
            format!("WITH c AS (SELECT * FROM ({bad}) d) SELECT 1"),
            format!("SELECT (WITH c AS ({bad}) SELECT 1) FROM t"),
            format!("SELECT * REPLACE (({bad}) AS a) FROM t"),
        ] {
            assert_eq!(severities(&sql), ["W071"], "{sql}");
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
