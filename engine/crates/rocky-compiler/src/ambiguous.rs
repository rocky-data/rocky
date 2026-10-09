//! Ambiguous unqualified column names (`E029`).
//!
//! `SELECT customer_id FROM customers AS c JOIN ltv AS l ON …` fails in every
//! warehouse Rocky targets when both `customers` and `ltv` have a
//! `customer_id` column: the bare name could mean either one. This pass
//! catches that shape at compile time.
//!
//! # Only fire when certain
//!
//! A false refusal breaks a valid build, so every rule errs toward silence:
//!
//! - A name is reported only when two or more relations of the *same*
//!   `FROM` clause provably have a column of that name. A relation counts
//!   only when Rocky knows names it outputs: an upstream model, a source
//!   schema (seed or cache), a CTE or a derived table. Partial knowledge is
//!   enough, since the check only proves presence. A relation Rocky cannot
//!   enumerate never makes a name ambiguous.
//! - A name merged by `JOIN … USING (…)` is not reported. A scope with a
//!   `NATURAL` join, a semi or anti join, an `ARRAY JOIN` or a `LATERAL VIEW`
//!   is not checked at all: those change which names are visible.
//! - A name that equals a `SELECT` alias or the output name of a qualified
//!   projection (`c.customer_id`) is not reported, and `ORDER BY` is not
//!   judged when the projection has a star: it may be a lateral alias
//!   or an `ORDER BY` / `GROUP BY` reference to the output, and dialects
//!   differ on precedence. Neither is a name that equals a relation's binding
//!   (a whole-row reference), a lambda parameter, a date-part keyword, a
//!   niladic keyword such as `current_date`, or a quoted name. A scope with a
//!   `->` operator is not checked: a lambda parses as that operator, and its
//!   parameter would look like a column.
//! - Each sub-query is its own scope and is checked against its own `FROM`
//!   only. A name it does not bind itself resolves outward (correlation) and
//!   is not reported in either scope.

use std::collections::HashSet;
use std::ops::ControlFlow;

use sqlparser::ast::{
    self, Expr, GroupByExpr, JoinConstraint, JoinOperator, Query, Select, SelectItem, SetExpr,
    Statement, TableFactor, TableWithJoins, Visit, Visitor,
};
use sqlparser::parser::Parser;

use crate::diagnostic::{Diagnostic, E029};
use crate::group_by::{
    Binding, Env, collect_bindings, immediate_subqueries, is_date_part, is_keyword_value, lower,
    query_output_names,
};

/// Check every query scope in `sql` for ambiguous unqualified column names.
///
/// `relation_columns` maps a relation name, as written in a `FROM` clause
/// (parts joined by `.`), to the output column names Rocky knows for it, or
/// `None` when the relation is unknown. CTE names are resolved first and
/// shadow anything the callback would return.
pub(crate) fn check_ambiguous_columns(
    model_name: &str,
    sql: &str,
    relation_columns: &dyn Fn(&str) -> Option<Vec<String>>,
) -> Vec<Diagnostic> {
    let Ok(statements) = Parser::parse_sql(&rocky_sql::dialect::DatabricksDialect, sql) else {
        return Vec::new();
    };
    let [Statement::Query(query)] = statements.as_slice() else {
        return Vec::new();
    };
    let mut findings = Vec::new();
    check_query(query, &Env::default(), relation_columns, &mut findings);

    let mut seen = HashSet::new();
    findings
        .into_iter()
        .filter(|finding| seen.insert(finding.column.to_lowercase()))
        .map(|finding| finding.into_diagnostic(model_name))
        .collect()
}

struct Finding {
    column: String,
    /// Binding names of the relations that have the column, in `FROM` order.
    relations: Vec<String>,
}

impl Finding {
    fn into_diagnostic(self, model_name: &str) -> Diagnostic {
        let Finding { column, relations } = self;
        let listed = relations
            .iter()
            .map(|r| format!("'{r}'"))
            .collect::<Vec<_>>()
            .join(", ");
        let example = relations
            .first()
            .map_or_else(|| column.clone(), |r| format!("{r}.{column}"));
        Diagnostic::error(
            E029,
            model_name,
            format!(
                "column '{column}' is ambiguous: the joined relations {listed} all have a column \
                 called '{column}', so the warehouse cannot tell which one the bare name means"
            ),
        )
        .with_suggestion(format!(
            "qualify the column with its relation, for example '{example}', or join with \
             USING ({column}) if the columns are the same key"
        ))
    }
}

fn check_query(
    query: &Query,
    outer: &Env,
    relation_columns: &dyn Fn(&str) -> Option<Vec<String>>,
    findings: &mut Vec<Finding>,
) {
    let mut env = outer.clone();
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            let name = lower(&cte.alias.name);
            let columns = if cte.alias.columns.is_empty() {
                query_output_names(&cte.query)
            } else {
                Some(cte.alias.columns.iter().map(|c| lower(&c.name)).collect())
            };
            if with.recursive {
                env.ctes.push((name.clone(), columns.clone()));
            }
            check_query(&cte.query, &env, relation_columns, findings);
            env.ctes.push((name, columns));
        }
    }

    if let Some(order_by) = &query.order_by {
        for nested in immediate_subqueries(order_by) {
            check_query(&nested, &env, relation_columns, findings);
        }
    }

    match query.body.as_ref() {
        SetExpr::Select(select) => check_select(
            select,
            query.order_by.as_ref(),
            &env,
            relation_columns,
            findings,
        ),
        other => check_set_expr(other, &env, relation_columns, findings),
    }
}

fn check_set_expr(
    body: &SetExpr,
    env: &Env,
    relation_columns: &dyn Fn(&str) -> Option<Vec<String>>,
    findings: &mut Vec<Finding>,
) {
    match body {
        // ORDER BY on a set operation orders the combined result by output
        // name, so a branch is checked without it.
        SetExpr::Select(select) => check_select(select, None, env, relation_columns, findings),
        SetExpr::Query(query) => check_query(query, env, relation_columns, findings),
        SetExpr::SetOperation { left, right, .. } => {
            check_set_expr(left, env, relation_columns, findings);
            check_set_expr(right, env, relation_columns, findings);
        }
        _ => {}
    }
}

/// How the joins of one `FROM` clause affect bare names.
#[derive(Default)]
struct JoinShape {
    /// Names merged by `USING (…)`: one column, not two.
    using: HashSet<String>,
    /// A join that changes which names are visible in a way this pass does
    /// not model (`NATURAL`, semi / anti, `ARRAY JOIN`).
    unmodelled: bool,
}

fn join_shape(from: &[TableWithJoins]) -> JoinShape {
    fn walk(table: &TableWithJoins, shape: &mut JoinShape) {
        walk_factor(&table.relation, shape);
        for join in &table.joins {
            walk_factor(&join.relation, shape);
            let constraint = match &join.join_operator {
                JoinOperator::Join(c)
                | JoinOperator::Inner(c)
                | JoinOperator::Left(c)
                | JoinOperator::LeftOuter(c)
                | JoinOperator::Right(c)
                | JoinOperator::RightOuter(c)
                | JoinOperator::FullOuter(c)
                | JoinOperator::CrossJoin(c)
                | JoinOperator::StraightJoin(c)
                | JoinOperator::AsOf { constraint: c, .. } => Some(c),
                JoinOperator::CrossApply | JoinOperator::OuterApply => None,
                JoinOperator::Semi(_)
                | JoinOperator::LeftSemi(_)
                | JoinOperator::RightSemi(_)
                | JoinOperator::Anti(_)
                | JoinOperator::LeftAnti(_)
                | JoinOperator::RightAnti(_)
                | JoinOperator::ArrayJoin
                | JoinOperator::LeftArrayJoin
                | JoinOperator::InnerArrayJoin => {
                    shape.unmodelled = true;
                    None
                }
            };
            match constraint {
                Some(JoinConstraint::Using(names)) => {
                    for name in names {
                        if let Some(ident) = name.0.last().and_then(ast::ObjectNamePart::as_ident) {
                            shape.using.insert(lower(ident));
                        }
                    }
                }
                Some(JoinConstraint::Natural) => shape.unmodelled = true,
                Some(JoinConstraint::On(_) | JoinConstraint::None) | None => {}
            }
        }
    }
    fn walk_factor(factor: &TableFactor, shape: &mut JoinShape) {
        if let TableFactor::NestedJoin {
            table_with_joins, ..
        } = factor
        {
            walk(table_with_joins, shape);
        }
    }
    let mut shape = JoinShape::default();
    for table in from {
        walk(table, &mut shape);
    }
    shape
}

fn check_select(
    select: &Select,
    order_by: Option<&ast::OrderBy>,
    env: &Env,
    relation_columns: &dyn Fn(&str) -> Option<Vec<String>>,
    findings: &mut Vec<Finding>,
) {
    // Every query nested in this SELECT is its own scope: derived tables,
    // scalar subqueries, EXISTS / IN subqueries.
    for nested in immediate_subqueries(select) {
        check_query(&nested, env, relation_columns, findings);
    }

    if !select.lateral_views.is_empty() {
        return;
    }
    let shape = join_shape(&select.from);
    if shape.unmodelled {
        return;
    }

    let mut bindings: Vec<Binding> = Vec::new();
    for table in &select.from {
        collect_bindings(&table.relation, env, relation_columns, &mut bindings);
        for join in &table.joins {
            collect_bindings(&join.relation, env, relation_columns, &mut bindings);
        }
    }
    if bindings.iter().filter(|b| b.columns.is_some()).count() < 2 {
        return;
    }

    let mut exempt: HashSet<String> = shape.using;
    for item in &select.projection {
        match item {
            SelectItem::ExprWithAlias { alias, .. } => {
                exempt.insert(lower(alias));
            }
            SelectItem::ExprWithAliases { aliases, .. } => {
                exempt.extend(aliases.iter().map(lower));
            }
            SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) => {
                if let Some(last) = parts.last() {
                    exempt.insert(lower(last));
                }
            }
            _ => {}
        }
    }
    exempt.extend(bindings.iter().filter_map(|b| b.name.clone()));

    let mut names = BareNames::default();
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(expr)
            | SelectItem::ExprWithAlias { expr, .. }
            | SelectItem::ExprWithAliases { expr, .. } => {
                let _ = expr.visit(&mut names);
            }
            _ => {}
        }
    }
    let _ = select.selection.visit(&mut names);
    let _ = select.having.visit(&mut names);
    let _ = select.qualify.visit(&mut names);
    if let GroupByExpr::Expressions(exprs, _) = &select.group_by {
        let _ = exprs.visit(&mut names);
    }
    for join in select.from.iter().flat_map(|table| &table.joins) {
        if let Some(JoinConstraint::On(on)) = join_constraint(&join.join_operator) {
            let _ = on.visit(&mut names);
        }
    }
    // A bare `ORDER BY` name resolves to an output column first. A star can
    // output any name, so with a star in the projection it is not judged.
    let projects_a_star = select.projection.iter().any(|item| {
        matches!(
            item,
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _)
        )
    });
    if let Some(ast::OrderBy {
        kind: ast::OrderByKind::Expressions(items),
        ..
    }) = order_by.filter(|_| !projects_a_star)
    {
        for item in items {
            let _ = item.expr.visit(&mut names);
        }
    }

    if names.saw_arrow {
        return;
    }
    for column in names.found {
        let key = column.to_lowercase();
        if exempt.contains(&key) || is_date_part(&key) || is_keyword_value(&key) {
            continue;
        }
        let relations: Vec<String> = bindings
            .iter()
            .filter(|b| b.columns.as_ref().is_some_and(|c| c.contains(&key)))
            .map(|b| b.name.clone().unwrap_or_else(|| "(subquery)".to_string()))
            .collect();
        if relations.len() >= 2 {
            findings.push(Finding { column, relations });
        }
    }
}

fn join_constraint(operator: &JoinOperator) -> Option<&JoinConstraint> {
    match operator {
        JoinOperator::Join(c)
        | JoinOperator::Inner(c)
        | JoinOperator::Left(c)
        | JoinOperator::LeftOuter(c)
        | JoinOperator::Right(c)
        | JoinOperator::RightOuter(c)
        | JoinOperator::FullOuter(c)
        | JoinOperator::CrossJoin(c)
        | JoinOperator::StraightJoin(c)
        | JoinOperator::Semi(c)
        | JoinOperator::LeftSemi(c)
        | JoinOperator::RightSemi(c)
        | JoinOperator::Anti(c)
        | JoinOperator::LeftAnti(c)
        | JoinOperator::RightAnti(c)
        | JoinOperator::AsOf { constraint: c, .. } => Some(c),
        JoinOperator::CrossApply
        | JoinOperator::OuterApply
        | JoinOperator::ArrayJoin
        | JoinOperator::LeftArrayJoin
        | JoinOperator::InnerArrayJoin => None,
    }
}

/// Unquoted bare identifiers of one scope, in order: outside nested queries
/// and outside lambda bodies.
#[derive(Default)]
struct BareNames {
    query_depth: usize,
    lambda_depth: usize,
    /// A `->` operator was seen. This dialect parses a lambda
    /// (`x -> x + 1`) as that operator, so its parameter looks like a column.
    saw_arrow: bool,
    found: Vec<String>,
}

impl Visitor for BareNames {
    type Break = ();

    fn pre_visit_query(&mut self, _query: &Query) -> ControlFlow<()> {
        self.query_depth += 1;
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &Query) -> ControlFlow<()> {
        self.query_depth -= 1;
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
        match expr {
            Expr::Lambda(_) => self.lambda_depth += 1,
            Expr::BinaryOp {
                op: ast::BinaryOperator::Arrow | ast::BinaryOperator::LongArrow,
                ..
            } => self.saw_arrow = true,
            Expr::Identifier(ident)
                if self.query_depth == 0
                    && self.lambda_depth == 0
                    && ident.quote_style.is_none() =>
            {
                self.found.push(ident.value.clone());
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
        if matches!(expr, Expr::Lambda(_)) {
            self.lambda_depth -= 1;
        }
        ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `customers` and `ltv` both have `customer_id`; `orders` has it too.
    fn lookup(name: &str) -> Option<Vec<String>> {
        let columns: &[&str] = match name {
            "customers" | "raw.customers" => &["customer_id", "name", "email", "country"],
            "ltv" => &["customer_id", "lifetime_value", "order_count"],
            "orders" => &["order_id", "customer_id", "amount", "status"],
            _ => return None,
        };
        Some(columns.iter().map(|c| (*c).to_string()).collect())
    }

    fn check(sql: &str) -> Vec<Diagnostic> {
        check_ambiguous_columns("m", sql, &lookup)
    }

    #[test]
    fn a_bare_name_both_joined_relations_have_is_refused() {
        let diagnostics = check(
            "SELECT customer_id, c.name, COALESCE(l.lifetime_value, 0) AS lifetime_value \
             FROM customers AS c LEFT JOIN ltv AS l ON c.customer_id = l.customer_id",
        );
        assert_eq!(diagnostics.len(), 1, "{diagnostics:?}");
        let d = &diagnostics[0];
        assert_eq!(&*d.code, "E029");
        assert!(d.is_error());
        assert!(d.message.contains("'customer_id'"), "{}", d.message);
        assert!(d.message.contains("'c', 'l'"), "{}", d.message);
        assert!(
            d.suggestion.as_deref().unwrap().contains("c.customer_id"),
            "{:?}",
            d.suggestion
        );
    }

    #[test]
    fn ambiguous_shapes_are_refused() {
        for sql in [
            // In WHERE, GROUP BY, HAVING, ORDER BY and a join condition.
            "SELECT c.name FROM customers c JOIN ltv l ON c.customer_id = l.customer_id \
             WHERE customer_id > 3",
            "SELECT COUNT(*) AS n FROM customers c JOIN ltv l ON c.customer_id = l.customer_id \
             GROUP BY customer_id",
            "SELECT c.name FROM customers c JOIN ltv l ON customer_id = l.customer_id",
            "SELECT c.name FROM customers c JOIN ltv l ON c.customer_id = l.customer_id \
             ORDER BY customer_id",
            // Unaliased tables, a comma join, a self-join.
            "SELECT customer_id FROM customers JOIN ltv ON customers.customer_id = ltv.customer_id",
            "SELECT customer_id FROM customers, ltv",
            "SELECT customer_id FROM customers a JOIN customers b ON a.name = b.name",
            // A source schema, a CTE and a derived table count as known.
            "SELECT customer_id FROM raw.customers c JOIN ltv l ON c.customer_id = l.customer_id",
            "WITH x AS (SELECT customer_id, 1 AS k FROM orders) \
             SELECT customer_id FROM x JOIN ltv l ON x.customer_id = l.customer_id",
            "SELECT customer_id FROM (SELECT customer_id FROM orders) s \
             JOIN ltv l ON s.customer_id = l.customer_id",
            // Inside a function call and a sub-query's own join.
            "SELECT UPPER(CAST(customer_id AS VARCHAR)) AS k FROM customers c JOIN ltv l ON true",
            "SELECT o.order_id FROM orders o WHERE o.customer_id IN \
             (SELECT customer_id FROM customers c JOIN ltv l ON c.customer_id = l.customer_id)",
        ] {
            let diagnostics = check(sql);
            assert_eq!(diagnostics.len(), 1, "{sql}: {diagnostics:?}");
            assert_eq!(&*diagnostics[0].code, "E029", "{sql}");
        }
    }

    #[test]
    fn valid_and_unprovable_shapes_stay_clean() {
        for sql in [
            // Qualified everywhere.
            "SELECT c.customer_id, c.name, l.lifetime_value FROM customers AS c \
             LEFT JOIN ltv AS l ON c.customer_id = l.customer_id",
            // A name only one relation has.
            "SELECT customer_id, lifetime_value, name FROM customers c JOIN ltv l USING (customer_id)",
            "SELECT name, lifetime_value FROM customers c JOIN ltv l ON c.customer_id = l.customer_id",
            // USING merges the key; NATURAL is not modelled.
            "SELECT customer_id FROM customers JOIN ltv USING (customer_id)",
            "SELECT customer_id FROM customers NATURAL JOIN ltv",
            // A relation Rocky cannot enumerate never makes a name ambiguous.
            "SELECT customer_id FROM customers c JOIN unknown_table u ON c.customer_id = u.customer_id",
            "SELECT customer_id FROM unknown_a a JOIN unknown_b b ON a.id = b.id",
            "SELECT customer_id FROM customers c JOIN read_parquet('x.parquet') p ON true",
            // One relation only.
            "SELECT customer_id FROM customers",
            // A SELECT alias, or a qualified projection's output name, may be
            // what ORDER BY / GROUP BY / WHERE means.
            "SELECT c.customer_id AS customer_id FROM customers c JOIN ltv l \
             ON c.customer_id = l.customer_id ORDER BY customer_id",
            "SELECT c.customer_id FROM customers c JOIN ltv l ON c.customer_id = l.customer_id \
             GROUP BY customer_id",
            "SELECT l.customer_id * 2 AS customer_id FROM customers c JOIN ltv l ON true \
             WHERE customer_id > 1",
            // A correlated sub-query: the inner bare name binds to its own FROM.
            "SELECT c.customer_id, (SELECT COUNT(*) FROM orders AS so \
             WHERE so.customer_id = c.customer_id) AS all_orders \
             FROM customers AS c LEFT JOIN ltv AS l ON c.customer_id = l.customer_id",
            "SELECT c.customer_id FROM customers c JOIN ltv l ON c.customer_id = l.customer_id \
             WHERE EXISTS (SELECT 1 FROM orders WHERE customer_id = c.customer_id)",
            // An outer name inside a sub-query is not judged there.
            "SELECT c.name FROM customers c JOIN ltv l ON c.customer_id = l.customer_id \
             WHERE c.customer_id IN (SELECT order_id FROM orders WHERE amount > lifetime_value)",
            // A CTE that shadows a known table with other columns.
            "WITH ltv AS (SELECT 1 AS k) SELECT customer_id FROM customers c JOIN ltv ON true",
            // Lambda parameters, date parts, quoted names, whole-row references.
            "SELECT list_transform(l.tags, customer_id -> customer_id + 1) AS t \
             FROM customers c JOIN ltv l ON c.customer_id = l.customer_id",
            "SELECT \"customer_id\" FROM customers c JOIN ltv l ON true",
            "SELECT c, l FROM customers c JOIN ltv l ON true",
            // Semi and anti joins expose only the left side.
            "SELECT customer_id FROM customers SEMI JOIN ltv ON customers.customer_id = ltv.customer_id",
            // A star outputs one `customer_id`; ORDER BY means that output.
            "SELECT c.*, l.lifetime_value FROM customers c JOIN ltv l \
             ON c.customer_id = l.customer_id ORDER BY customer_id",
            // A set operation's branches are separate scopes.
            "SELECT customer_id FROM customers UNION ALL SELECT customer_id FROM ltv",
        ] {
            let diagnostics = check(sql);
            assert!(diagnostics.is_empty(), "{sql}: {diagnostics:?}");
        }
    }

    #[test]
    fn repeated_ambiguous_name_reports_once() {
        let diagnostics =
            check("SELECT customer_id FROM customers c JOIN ltv l ON true WHERE customer_id > 1");
        assert_eq!(diagnostics.len(), 1, "{diagnostics:?}");
    }
}
