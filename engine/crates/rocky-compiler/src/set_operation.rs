//! E071: set-operation branches that return different numbers of columns
//! (#2661).
//!
//! `SELECT a, b FROM t UNION ALL SELECT a FROM u` is refused by every
//! warehouse. Type inference already notices the mismatch, but only to give
//! up on the model's columns, so the model compiled clean and failed at run
//! time.
//!
//! A CTE nothing reads, directly or through another CTE, is not checked:
//! DuckDB never binds it.
//!
//! A branch's width is counted only when the SQL states it: a `*`, a
//! `t.*`, a multi-alias item, and a call that expands to several columns
//! (`COLUMNS(...)`, `UNNEST`, `EXPLODE`, ...) make it unknown, and an
//! unknown width is never a mismatch. `UNION BY NAME` matches columns by
//! name, not position, and is not checked.

use std::collections::{HashMap, HashSet};

use sqlparser::ast::{
    Expr, ObjectName, Query, SelectItem, SetExpr, SetQuantifier, Statement, Visit, Visitor,
};
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
        unreferenced: unreferenced_cte_bodies(&statements),
        skipping: 0,
        out: Vec::new(),
    };
    let _ = statements.visit(&mut visitor);
    visitor.out
}

struct Queries<'m> {
    model_name: &'m str,
    /// The bodies of CTEs nothing reads ([`unreferenced_cte_bodies`]).
    unreferenced: HashSet<*const Query>,
    /// How many unreferenced CTE bodies the visitor is inside.
    skipping: usize,
    out: Vec<Diagnostic>,
}

impl Visitor for Queries<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, query: &Query) -> std::ops::ControlFlow<()> {
        if self.unreferenced.contains(&std::ptr::from_ref(query)) {
            self.skipping += 1;
        }
        if self.skipping == 0 {
            report(&query.body, self.model_name, &mut self.out);
        }
        std::ops::ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, query: &Query) -> std::ops::ControlFlow<()> {
        if self.unreferenced.contains(&std::ptr::from_ref(query)) {
            self.skipping -= 1;
        }
        std::ops::ControlFlow::Continue(())
    }
}

/// The bodies of the CTEs in `statements` that nothing reads: no table
/// reference in the query's body names them, directly or through another
/// CTE it reads. DuckDB never binds such a CTE, so a check that reports
/// what DuckDB refuses at bind time skips it.
///
/// A reference only from the query's `ORDER BY` or `LIMIT`, or one a nested
/// `WITH` shadows, is not followed: the CTE is then taken as unread, which
/// can only skip a check, never add a refusal.
pub(crate) fn unreferenced_cte_bodies(statements: &[Statement]) -> HashSet<*const Query> {
    struct Withs(HashSet<*const Query>);
    impl Visitor for Withs {
        type Break = ();
        fn pre_visit_query(&mut self, query: &Query) -> std::ops::ControlFlow<()> {
            let Some(with) = &query.with else {
                return std::ops::ControlFlow::Continue(());
            };
            let ctes: HashMap<String, (&Query, HashSet<String>)> = with
                .cte_tables
                .iter()
                .map(|cte| {
                    (
                        cte.alias.name.value.to_lowercase(),
                        (&*cte.query, relation_names(&*cte.query)),
                    )
                })
                .collect();
            let mut read: HashSet<String> = HashSet::new();
            let mut pending: Vec<String> = relation_names(&*query.body).into_iter().collect();
            while let Some(name) = pending.pop() {
                if let Some((_, refs)) = ctes.get(&name)
                    && read.insert(name)
                {
                    pending.extend(refs.iter().cloned());
                }
            }
            for (name, (body, _)) in &ctes {
                if !read.contains(name) {
                    self.0.insert(std::ptr::from_ref(*body));
                }
            }
            std::ops::ControlFlow::Continue(())
        }
    }
    let mut withs = Withs(HashSet::new());
    for statement in statements {
        let _ = statement.visit(&mut withs);
    }
    withs.0
}

/// The lower-case one-part table names `node` reads.
fn relation_names<V: Visit + ?Sized>(node: &V) -> HashSet<String> {
    struct Relations(HashSet<String>);
    impl Visitor for Relations {
        type Break = ();
        fn pre_visit_relation(&mut self, relation: &ObjectName) -> std::ops::ControlFlow<()> {
            if let [part] = relation.0.as_slice()
                && let Some(ident) = part.as_ident()
            {
                self.0.insert(ident.value.to_lowercase());
            }
            std::ops::ControlFlow::Continue(())
        }
    }
    let mut relations = Relations(HashSet::new());
    let _ = node.visit(&mut relations);
    relations.0
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
                     left, {r} on the right. SQL requires the same number in every branch"
                ),
            )
            .with_suggestion(
                "select the same columns, in the same order, in every branch".to_string(),
            ),
        );
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
            "select columns('x_.*') from t union all select a from u",
            "select unnest(s) from t union all select a from u",
            "select explode(arr) from t union all select a from u",
            "select stack(2, a, b) from t union all select a from u",
            "select a, b from t union all by name select a from u",
            "select a, b from t union by name select a from u",
            "select a from t union all values (1), (2)",
            "select a, b from t",
            "select a, b from t union all select untuple(p) from u",
        ] {
            assert_eq!(codes(sql), 0, "{sql}");
        }
    }

    /// A CTE nothing reads is never bound by DuckDB, so its mismatch is not
    /// reported (red team, #2661). One the query reads still is.
    #[test]
    fn an_unreferenced_cte_is_not_checked() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE u (a INTEGER); CREATE TABLE v (a INTEGER, b INTEGER)")
            .unwrap();
        for sql in [
            "with x as (select a from u union all select a, b from v) select a, b from v",
            "with x as (select a from u union all select a, b from v), \
             y as (select * from x) select a from u",
        ] {
            assert_eq!(codes(sql), 0, "{sql}");
            conn.prepare(sql)
                .unwrap_or_else(|e| panic!("DuckDB refuses {sql}: {e}"));
        }
        assert_eq!(
            codes(
                "with x as (select a from u union all select a, b from v), \
                   y as (select * from x) select * from y"
            ),
            1
        );
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
