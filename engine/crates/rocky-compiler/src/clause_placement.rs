//! Where an aggregate or a window function may appear, and whether a
//! `GROUP BY` position is in range (`E065`, `E066`, `E067`, #2660).
//!
//! Every warehouse Rocky targets refuses these three shapes when it plans
//! the statement, whatever the data:
//!
//! | Model SQL | DuckDB |
//! |---|---|
//! | `... WHERE SUM(amount) > 1 GROUP BY customer_id` | `WHERE clause cannot contain aggregates!` |
//! | `... WHERE ROW_NUMBER() OVER (ORDER BY id) = 1` | `WHERE clause cannot contain window functions!` |
//! | `SELECT customer_id, COUNT(*) FROM orders GROUP BY 3` | `GROUP BY term out of range` |
//!
//! # Only fire when certain
//!
//! A false refusal breaks a valid build, so each rule errs toward silence:
//!
//! - An aggregate is a call to `COUNT`, `SUM`, `AVG`, `MIN` or `MAX`, or any
//!   call with `FILTER (WHERE ...)` or `WITHIN GROUP (...)`, with no `OVER`.
//!   The wider list in [`crate::group_by`] holds names a user function could
//!   shadow (`FIRST`, `MODE`, `PRODUCT`); there, calling a function an
//!   aggregate only silences `E044`, but here it would refuse the model.
//! - A window function is any call with `OVER`: only the syntax is read.
//! - Only a `WHERE` that is not inside an expression subquery (scalar,
//!   `IN`, `EXISTS`) or a `LATERAL` derived table is checked, nor anything
//!   nested in one. There an aggregate over outer columns belongs to the
//!   outer query, which PostgreSQL accepts. A non-lateral derived table and
//!   a `WITH` query of the top level are checked.
//! - An aggregate or window inside a subquery of the `WHERE` belongs to that
//!   subquery, so it is never reported against the `WHERE` that holds it.
//! - A `GROUP BY` position is checked only at the top level of the clause,
//!   not inside `ROLLUP`, `CUBE` or `GROUPING SETS`. It must be a plain
//!   integer, and the select list must have a known length: no `*`, no
//!   `t.*`, no `expr AS (a, b)`, and no call, at any depth, to a function
//!   that can output several columns (DuckDB `COLUMNS` and `UNNEST`, the
//!   Spark generators `EXPLODE`, `POSEXPLODE`, `INLINE`, `STACK`,
//!   `JSON_TUPLE`). `GROUP BY ALL` is not checked.
//! - ClickHouse with `enable_positional_arguments = 0` also reads a position
//!   as a constant. It is on by default; Rocky does not model it.
//! - Spark's `spark.sql.groupByOrdinal = false` reads `GROUP BY 3` as the
//!   constant 3. It is `true` by default, and Rocky does not model it.

use std::ops::ControlFlow;

use sqlparser::ast::{
    self, Expr, GroupByExpr, Query, Select, SelectItem, SetExpr, Statement, TableFactor,
    TableWithJoins, Visit, Visitor,
};
use sqlparser::parser::Parser;

use crate::diagnostic::{Diagnostic, E065, E066, E067};
use crate::group_by::immediate_subqueries;

/// Check every query scope in `sql` for `E065`, `E066` and `E067`.
pub(crate) fn check_clause_placement(model_name: &str, sql: &str) -> Vec<Diagnostic> {
    let Ok(statements) = Parser::parse_sql(&rocky_sql::dialect::DatabricksDialect, sql) else {
        return Vec::new();
    };
    let [Statement::Query(query)] = statements.as_slice() else {
        return Vec::new();
    };
    let mut diagnostics = Vec::new();
    check_query(query, false, model_name, &mut diagnostics);
    diagnostics
}

/// `correlated` is true when the query may read columns of an outer query.
fn check_query(query: &Query, correlated: bool, model: &str, out: &mut Vec<Diagnostic>) {
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            check_query(&cte.query, correlated, model, out);
        }
    }
    if let Some(order_by) = &query.order_by {
        for nested in immediate_subqueries(order_by) {
            check_query(&nested, true, model, out);
        }
    }
    check_set_expr(&query.body, correlated, model, out);
}

fn check_set_expr(body: &SetExpr, correlated: bool, model: &str, out: &mut Vec<Diagnostic>) {
    match body {
        SetExpr::Select(select) => check_select(select, correlated, model, out),
        SetExpr::Query(query) => check_query(query, correlated, model, out),
        SetExpr::SetOperation { left, right, .. } => {
            check_set_expr(left, correlated, model, out);
            check_set_expr(right, correlated, model, out);
        }
        _ => {}
    }
}

fn check_select(select: &Select, correlated: bool, model: &str, out: &mut Vec<Diagnostic>) {
    // Derived tables keep the scope's correlation; a LATERAL one may read
    // the relations beside it. Subqueries in join conditions are expression
    // subqueries.
    for table in &select.from {
        check_table(table, correlated, model, out);
    }
    // Every other nested query is an expression subquery, which may read
    // this query's columns.
    for nested in expression_subqueries(select) {
        check_query(&nested, true, model, out);
    }

    if !correlated && let Some(selection) = &select.selection {
        if contains_core_aggregate(selection) {
            out.push(
                Diagnostic::error(
                    E065,
                    model,
                    "the WHERE clause contains an aggregate; WHERE filters rows before they \
                     are grouped, so no warehouse can run it",
                )
                .with_suggestion(
                    "move the condition to HAVING, or compute the aggregate in a subquery or CTE",
                ),
            );
        }
        if contains_window(selection) {
            out.push(
                Diagnostic::error(
                    E066,
                    model,
                    "the WHERE clause contains a window function (a call with OVER); window \
                     functions run after WHERE, so no warehouse can run it",
                )
                .with_suggestion(
                    "filter on the window result with QUALIFY, or compute it in a subquery or CTE",
                ),
            );
        }
    }

    check_group_by_positions(select, model, out);
}

fn check_table(table: &TableWithJoins, correlated: bool, model: &str, out: &mut Vec<Diagnostic>) {
    check_factor(&table.relation, correlated, model, out);
    for join in &table.joins {
        check_factor(&join.relation, correlated, model, out);
        for nested in immediate_subqueries(&join.join_operator) {
            check_query(&nested, true, model, out);
        }
    }
}

fn check_factor(factor: &TableFactor, correlated: bool, model: &str, out: &mut Vec<Diagnostic>) {
    match factor {
        TableFactor::Derived {
            lateral, subquery, ..
        } => check_query(subquery, correlated || *lateral, model, out),
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => check_table(table_with_joins, correlated, model, out),
        // A table function's arguments may read the relations beside it.
        other => {
            for nested in immediate_subqueries(other) {
                check_query(&nested, true, model, out);
            }
        }
    }
}

/// The queries nested in `select` outside its `FROM` clause.
fn expression_subqueries(select: &Select) -> Vec<Query> {
    let mut nested = immediate_subqueries(&select.projection);
    nested.extend(immediate_subqueries(&select.selection));
    nested.extend(immediate_subqueries(&select.group_by));
    nested.extend(immediate_subqueries(&select.having));
    nested.extend(immediate_subqueries(&select.qualify));
    nested.extend(immediate_subqueries(&select.named_window));
    nested
}

/// `E067`: a top-level `GROUP BY` position outside `1..=` the select list.
fn check_group_by_positions(select: &Select, model: &str, out: &mut Vec<Diagnostic>) {
    let GroupByExpr::Expressions(exprs, _) = &select.group_by else {
        return;
    };
    let Some(width) = projection_width(select) else {
        return;
    };
    for expr in exprs {
        let Expr::Value(value) = expr else {
            continue;
        };
        let ast::Value::Number(number, _) = &value.value else {
            continue;
        };
        let Ok(position) = number.parse::<u64>() else {
            continue;
        };
        if position == 0 || position > width as u64 {
            out.push(
                Diagnostic::error(
                    E067,
                    model,
                    format!(
                        "GROUP BY {position} names no output column; the select list has \
                         {width} column(s), so a position must be between 1 and {width}"
                    ),
                )
                .with_suggestion("group by a column name, or by a position in the select list"),
            );
        }
    }
}

/// The number of select-list items, when each is one output column.
fn projection_width(select: &Select) -> Option<usize> {
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                if contains_call(expr, |function| {
                    last_name_part(function).is_some_and(|name| expands_to_columns(&name))
                }) {
                    return None;
                }
            }
            SelectItem::ExprWithAliases { .. }
            | SelectItem::Wildcard(_)
            | SelectItem::QualifiedWildcard(..) => return None,
        }
    }
    Some(select.projection.len())
}

/// A function that can turn one select item into several output columns:
/// DuckDB `COLUMNS(...)` and `UNNEST` of a struct, and the Spark generators.
/// Matched at any depth (`COLUMNS('a.*')::INT` expands too).
fn expands_to_columns(name: &str) -> bool {
    matches!(
        name,
        "COLUMNS"
            | "UNNEST"
            | "EXPLODE"
            | "EXPLODE_OUTER"
            | "POSEXPLODE"
            | "POSEXPLODE_OUTER"
            | "INLINE"
            | "INLINE_OUTER"
            | "STACK"
            | "JSON_TUPLE"
    )
}

/// The last part of a call's name, upper-cased: `POSEXPLODE` for
/// `builtin.posexplode(arr)`.
fn last_name_part(function: &ast::Function) -> Option<String> {
    function
        .name
        .0
        .last()
        .and_then(ast::ObjectNamePart::as_ident)
        .map(|ident| ident.value.to_uppercase())
}

fn function_name(function: &ast::Function) -> Option<String> {
    let [part] = function.name.0.as_slice() else {
        return None;
    };
    part.as_ident().map(|ident| ident.value.to_uppercase())
}

/// Whether `expr` holds, outside any subquery, a call `matches` accepts.
fn contains_call(expr: &Expr, matches: fn(&ast::Function) -> bool) -> bool {
    struct Finder {
        depth: usize,
        matches: fn(&ast::Function) -> bool,
    }
    impl Visitor for Finder {
        type Break = ();
        fn pre_visit_query(&mut self, _query: &Query) -> ControlFlow<()> {
            self.depth += 1;
            ControlFlow::Continue(())
        }
        fn post_visit_query(&mut self, _query: &Query) -> ControlFlow<()> {
            self.depth -= 1;
            ControlFlow::Continue(())
        }
        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
            if self.depth == 0
                && let Expr::Function(function) = expr
                && (self.matches)(function)
            {
                return ControlFlow::Break(());
            }
            ControlFlow::Continue(())
        }
    }
    expr.visit(&mut Finder { depth: 0, matches }).is_break()
}

/// An aggregate no user function can be: see the module docs.
fn contains_core_aggregate(expr: &Expr) -> bool {
    contains_call(expr, |function| {
        function.over.is_none()
            && (!function.within_group.is_empty()
                || function.filter.is_some()
                || function_name(function).is_some_and(|name| {
                    matches!(name.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX")
                }))
    })
}

fn contains_window(expr: &Expr) -> bool {
    contains_call(expr, |function| function.over.is_some())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn codes(sql: &str) -> Vec<String> {
        check_clause_placement("m", sql)
            .into_iter()
            .map(|d| d.code.to_string())
            .collect()
    }

    #[test]
    fn the_issue_repros_are_refused() {
        assert_eq!(
            codes("select customer_id from main.orders where sum(amount) > 1 group by customer_id"),
            ["E065"]
        );
        assert_eq!(
            codes(
                "select order_id from main.orders \
                 where row_number() over (order by order_id) = 1"
            ),
            ["E066"]
        );
        assert_eq!(
            codes("select customer_id, count(*) as n from main.orders group by 3"),
            ["E067"]
        );
    }

    #[test]
    fn valid_shapes_stay_clean() {
        for sql in [
            // The issue's control.
            "select customer_id, count(*) as n from main.orders group by 1",
            "select customer_id, count(*) as n from main.orders group by 2",
            // An aggregate inside a subquery of the WHERE is valid.
            "select id from t where amount > (select avg(amount) from t)",
            "select id from t where id in (select max(id) from u group by k)",
            "select id from t where exists (select 1 from u having count(*) > 1)",
            // A window inside a WHERE subquery belongs to it.
            "select id from t where id in \
             (select id from (select id, row_number() over (order by id) as r from u) where r = 1)",
            // The aggregate belongs in HAVING and the window in QUALIFY.
            "select k, sum(x) from t group by k having sum(x) > 1",
            "select id from t qualify row_number() over (order by id) = 1",
            // A scalar function and a name a UDF may own.
            "select id from t where abs(x) > 1 and first(x) > 0 and mode(x) = 1",
            // Positions the width cannot be read for, or not plain integers.
            "select * from t group by 3",
            "select t.* from t group by 9",
            "select a, b from t group by rollup (3)",
            "select a from t group by all",
            "select a from t group by -1",
        ] {
            assert_eq!(codes(sql), Vec::<String>::new(), "{sql}");
        }
    }

    #[test]
    fn a_correlated_subquery_where_is_not_checked() {
        // In an expression subquery an aggregate may belong to the outer
        // query (PostgreSQL), so the inner WHERE is left alone.
        assert_eq!(
            codes("select (select 1 from u where sum(t.x) > 0) from t group by t.k"),
            Vec::<String>::new()
        );
    }

    #[test]
    fn nested_scopes_are_checked() {
        // A CTE, a derived table and a set-operation branch.
        assert_eq!(
            codes("with c as (select k from t where count(*) > 1) select k from c"),
            ["E065"]
        );
        assert_eq!(
            codes("select k from (select k from t where max(x) > 1 group by k) d"),
            ["E065"]
        );
        assert_eq!(
            codes("select k from t union all select k from u where sum(x) over () > 1"),
            ["E066"]
        );
        // FILTER and WITHIN GROUP mark an aggregate of any name.
        assert_eq!(
            codes("select k from t where my_agg(x) filter (where x > 0) > 1 group by k"),
            ["E065"]
        );
    }

    #[test]
    fn more_aggregate_shapes_and_scopes() {
        for sql in [
            "select k from t where avg(x) > 1 group by k",
            "select k from t where min(x) > 1 group by k",
            "select k from t where percentile_cont(0.5) within group (order by x) > 1 group by k",
        ] {
            assert_eq!(codes(sql), ["E065"], "{sql}");
        }
        for sql in [
            // A LATERAL table and a join condition may read outer columns.
            "select t.k from t, lateral (select 1 as one from u where sum(t.x) > 0) l",
            "select t.k from t join u on u.k = (select 1 from v where count(t.x) > 0)",
        ] {
            assert_eq!(codes(sql), Vec::<String>::new(), "{sql}");
        }
    }

    #[test]
    fn a_position_is_checked_in_every_scope() {
        for sql in [
            "with c as (select a from t group by 2) select a from c",
            "select a from (select a from t group by 2) d",
            "select a from t union all select a from u group by 2",
        ] {
            assert_eq!(codes(sql), ["E067"], "{sql}");
        }
        for sql in [
            // A select list whose width Rocky cannot read.
            "select x as (a, b) from t group by 2",
            "select unnest(s) from t group by 2",
            "select columns('a.*')::int from t group by 2",
            "select posexplode(arr) from t group by 2",
            "select builtin.posexplode(arr) from t group by 2",
            "select stack(2, a, b, c, d) from t group by 2",
        ] {
            assert_eq!(codes(sql), Vec::<String>::new(), "{sql}");
        }
    }

    #[test]
    fn every_column_expanding_function_skips_the_position_check() {
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
        ] {
            let sql = format!("select {name}(a) from t group by 2");
            assert_eq!(codes(&sql), Vec::<String>::new(), "{sql}");
        }
        // Without one, the same width is checked.
        assert_eq!(codes("select upper(a) from t group by 2"), ["E067"]);
    }

    #[test]
    fn a_zero_position_is_out_of_range() {
        assert_eq!(codes("select a, b from t group by 0"), ["E067"]);
        assert_eq!(codes("select a, b from t group by 1, 4"), ["E067"]);
    }
}
