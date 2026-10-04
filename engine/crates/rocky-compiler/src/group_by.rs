//! GROUP BY validity (`E044`).
//!
//! An aggregating query may only read a column outside an aggregate when the
//! column is grouped. `SELECT customer_id, status, SUM(amount) FROM orders
//! GROUP BY customer_id` compiles in a naive checker and then fails in every
//! warehouse Rocky targets, because `status` has no single value per group.
//! This pass catches that shape at compile time.
//!
//! # Only fire when certain
//!
//! A false refusal breaks a valid build, which is worse than a miss. Every
//! rule below therefore errs toward silence:
//!
//! - A reference must resolve to a column of a relation in the *same* query
//!   scope whose output names Rocky knows: an upstream model, a source schema
//!   (seed or cache), a CTE, or a derived table. A name Rocky cannot place —
//!   an unknown relation, a session variable, an outer-scope reference — is
//!   left alone. Postgres-style functional dependence on a primary key is not
//!   assumed by DuckDB, Databricks, Snowflake, or BigQuery, so it is not
//!   modelled; it never matters because only grouped names are accepted.
//! - "Grouped" is decided by *name*. Every identifier that appears anywhere in
//!   the GROUP BY clause (after expanding ordinals and projection aliases)
//!   covers every reference with that column name, whatever its qualifier.
//!   This accepts `UPPER(status)` when `status` is grouped, `o.id` when `id`
//!   is grouped, and an expression that matches a grouped expression. It also
//!   accepts `order_date` when only `DATE_TRUNC('month', order_date)` is
//!   grouped — a deliberate miss, since some engines fold such expressions.
//! - Grouping by a projection alias is accepted in every dialect. DuckDB,
//!   Databricks, Snowflake, and BigQuery all allow it:
//!   <https://duckdb.org/docs/sql/query_syntax/groupby>,
//!   <https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-qry-select-groupby.html>,
//!   <https://docs.snowflake.com/en/sql-reference/constructs/group-by>,
//!   <https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#group_by_clause>.
//!   Trino does not, but accepting it there is only a miss.
//! - A name that equals a projection alias is never reported: it may be a
//!   lateral column alias (DuckDB, Databricks, Snowflake, BigQuery in some
//!   clauses), and alias-versus-column precedence differs by dialect.
//! - `GROUP BY ALL` groups by every non-aggregate item, so it is never checked.
//! - Arguments of aggregates (including `FILTER`, `WITHIN GROUP`, `DISTINCT`)
//!   are not checked. Window functions are evaluated after grouping, so their
//!   arguments and `PARTITION BY` / `ORDER BY` are checked like any other
//!   expression; an aggregate nested inside them (`SUM(SUM(x)) OVER ()`) is
//!   still an aggregate.
//! - An unknown function might be a user-defined aggregate, so its arguments
//!   are not checked. Only an allow-listed scalar or window function has its
//!   arguments walked. Expression shapes the walker does not model stay
//!   silent. Bare date-part keywords (`DATEADD(day, 1, d)`) are not columns.
//! - `QUALIFY` is not checked.
//! - Subqueries are checked as their own scopes. Outer references inside a
//!   subquery (correlation) are never reported, in either scope.

use std::collections::HashSet;
use std::ops::ControlFlow;

use sqlparser::ast::{
    self, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, Query, Select,
    SelectItem, SetExpr, Statement, TableFactor, Visit, Visitor, WindowType,
};
use sqlparser::parser::Parser;

use crate::diagnostic::{Diagnostic, E044};

/// Check every query scope in `sql` for E044.
///
/// `relation_columns` maps a relation name, as written in a `FROM` clause
/// (parts joined by `.`), to the output column names Rocky knows for it.
/// It returns `None` when the relation is unknown. CTE names are resolved
/// here first and shadow anything the callback would return.
pub(crate) fn check_group_by(
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
        .filter(|finding| seen.insert((finding.global, finding.clause, finding.column_key())))
        .map(|finding| finding.into_diagnostic(model_name))
        .collect()
}

/// CTEs visible at a point in the query, innermost last.
#[derive(Clone, Default)]
struct Env {
    ctes: Vec<(String, Option<HashSet<String>>)>,
}

impl Env {
    fn lookup(&self, name: &str) -> Option<&Option<HashSet<String>>> {
        self.ctes
            .iter()
            .rev()
            .find(|(cte, _)| cte == name)
            .map(|(_, columns)| columns)
    }
}

/// A relation bound in one `FROM` clause.
struct Binding {
    /// The name a qualifier uses: the alias, or the table's last name part.
    name: Option<String>,
    /// Output names known to exist. `None` when the relation is unknown.
    columns: Option<HashSet<String>>,
}

struct Finding {
    column: String,
    clause: &'static str,
    global: bool,
}

impl Finding {
    fn column_key(&self) -> String {
        self.column.to_lowercase()
    }

    fn into_diagnostic(self, model_name: &str) -> Diagnostic {
        let Finding {
            column,
            clause,
            global,
        } = self;
        if global {
            Diagnostic::error(
                E044,
                model_name,
                format!(
                    "column '{column}' in the {clause} is read outside an aggregate, but the \
                     query aggregates without GROUP BY"
                ),
            )
            .with_suggestion(format!(
                "add GROUP BY {column}, or wrap it in an aggregate such as ANY_VALUE({column}) \
                 or MAX({column})"
            ))
        } else {
            Diagnostic::error(
                E044,
                model_name,
                format!(
                    "column '{column}' in the {clause} is neither in GROUP BY nor inside an \
                     aggregate"
                ),
            )
            .with_suggestion(format!(
                "add '{column}' to GROUP BY, or wrap it in an aggregate such as \
                 ANY_VALUE({column}) or MAX({column})"
            ))
        }
    }
}

fn lower(ident: &ast::Ident) -> String {
    ident.value.to_lowercase()
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
        // ORDER BY on a set operation orders the combined result, not one
        // branch, so a branch is checked without it.
        SetExpr::Select(select) => check_select(select, None, env, relation_columns, findings),
        SetExpr::Query(query) => check_query(query, env, relation_columns, findings),
        SetExpr::SetOperation { left, right, .. } => {
            check_set_expr(left, env, relation_columns, findings);
            check_set_expr(right, env, relation_columns, findings);
        }
        _ => {}
    }
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

    let group_exprs = match &select.group_by {
        GroupByExpr::All(_) => return,
        GroupByExpr::Expressions(exprs, _) => exprs,
    };

    let has_aggregate = select.projection.iter().any(|item| match item {
        SelectItem::UnnamedExpr(expr)
        | SelectItem::ExprWithAlias { expr, .. }
        | SelectItem::ExprWithAliases { expr, .. } => contains_aggregate(expr),
        _ => false,
    });
    let has_group_by = !group_exprs.is_empty();
    if !has_group_by && select.having.is_none() && !has_aggregate {
        return;
    }

    let aliases = projection_aliases(select);
    let Some(grouped) = grouped_names(select, group_exprs, &aliases) else {
        return;
    };

    let mut bindings = Vec::new();
    for table in &select.from {
        collect_bindings(&table.relation, env, relation_columns, &mut bindings);
        for join in &table.joins {
            collect_bindings(&join.relation, env, relation_columns, &mut bindings);
        }
    }
    if bindings.iter().all(|binding| binding.columns.is_none()) {
        return;
    }

    let mut checker = Checker {
        grouped: &grouped,
        aliases: &aliases,
        bindings: &bindings,
        clause: "SELECT list",
        global: !has_group_by,
        findings,
    };
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(expr)
            | SelectItem::ExprWithAlias { expr, .. }
            | SelectItem::ExprWithAliases { expr, .. } => checker.walk(expr),
            _ => {}
        }
    }
    if let Some(having) = &select.having {
        checker.clause = "HAVING clause";
        checker.walk(having);
    }
    if let Some(ast::OrderBy {
        kind: ast::OrderByKind::Expressions(items),
        ..
    }) = order_by
    {
        checker.clause = "ORDER BY clause";
        for item in items {
            checker.walk(&item.expr);
        }
    }
}

fn projection_aliases(select: &Select) -> HashSet<String> {
    let mut aliases = HashSet::new();
    for item in &select.projection {
        match item {
            SelectItem::ExprWithAlias { alias, .. } => {
                aliases.insert(lower(alias));
            }
            SelectItem::ExprWithAliases { aliases: names, .. } => {
                aliases.extend(names.iter().map(lower));
            }
            _ => {}
        }
    }
    aliases
}

/// Every name the GROUP BY clause groups by, with ordinals and projection
/// aliases expanded to the names inside the projected expression.
///
/// Returns `None` when an ordinal cannot be resolved (out of range, or it
/// points at a star), so the caller stays silent.
fn grouped_names(
    select: &Select,
    group_exprs: &[Expr],
    aliases: &HashSet<String>,
) -> Option<HashSet<String>> {
    let mut grouped = HashSet::new();
    let mut keys: Vec<&Expr> = Vec::new();
    for expr in group_exprs {
        match expr {
            Expr::Rollup(sets) | Expr::Cube(sets) | Expr::GroupingSets(sets) => {
                for set in sets {
                    for key in set {
                        match key {
                            Expr::Tuple(items) => keys.extend(items.iter()),
                            key => keys.push(key),
                        }
                    }
                }
            }
            expr => keys.push(expr),
        }
    }

    for key in keys {
        if let Expr::Value(value) = key
            && let ast::Value::Number(number, _) = &value.value
        {
            let position: usize = number.parse().ok()?;
            let item = select.projection.get(position.checked_sub(1)?)?;
            match item {
                SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                    collect_names(expr, &mut grouped);
                }
                _ => return None,
            }
            if let SelectItem::ExprWithAlias { alias, .. } = item {
                grouped.insert(lower(alias));
            }
            continue;
        }
        collect_names(key, &mut grouped);
    }

    // `GROUP BY alias` groups by the aliased expression.
    let named_aliases: Vec<String> = grouped
        .iter()
        .filter(|name| aliases.contains(*name))
        .cloned()
        .collect();
    for alias in named_aliases {
        for item in &select.projection {
            if let SelectItem::ExprWithAlias { expr, alias: name } = item
                && lower(name) == alias
            {
                collect_names(expr, &mut grouped);
            }
        }
    }
    Some(grouped)
}

/// Collect every identifier part in `expr`, at any depth.
fn collect_names(expr: &Expr, into: &mut HashSet<String>) {
    struct Names<'a>(&'a mut HashSet<String>);
    impl Visitor for Names<'_> {
        type Break = ();
        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
            match expr {
                Expr::Identifier(ident) => {
                    self.0.insert(lower(ident));
                }
                // Only the column part: a qualifier is not a grouped name.
                Expr::CompoundIdentifier(parts) => {
                    if let Some(last) = parts.last() {
                        self.0.insert(lower(last));
                    }
                }
                _ => {}
            }
            ControlFlow::Continue(())
        }
    }
    let _ = expr.visit(&mut Names(into));
}

/// The output names a query certainly produces. Partial knowledge is fine:
/// the set is only ever used to prove that a name exists.
fn query_output_names(query: &Query) -> Option<HashSet<String>> {
    fn body_names(body: &SetExpr) -> Option<HashSet<String>> {
        match body {
            SetExpr::Select(select) => {
                let mut names = HashSet::new();
                for item in &select.projection {
                    match item {
                        SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                            names.insert(lower(ident));
                        }
                        SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) => {
                            if let Some(last) = parts.last() {
                                names.insert(lower(last));
                            }
                        }
                        SelectItem::ExprWithAlias { alias, .. } => {
                            names.insert(lower(alias));
                        }
                        _ => {}
                    }
                }
                Some(names)
            }
            SetExpr::Query(query) => query_output_names(query),
            SetExpr::SetOperation { left, .. } => body_names(left),
            _ => None,
        }
    }
    body_names(&query.body)
}

fn collect_bindings(
    factor: &TableFactor,
    env: &Env,
    relation_columns: &dyn Fn(&str) -> Option<Vec<String>>,
    bindings: &mut Vec<Binding>,
) {
    match factor {
        TableFactor::Table { name, alias, .. } => {
            let parts: Vec<String> = name
                .0
                .iter()
                .filter_map(|part| part.as_ident().map(|ident| ident.value.clone()))
                .collect();
            if parts.len() != name.0.len() || parts.is_empty() {
                bindings.push(Binding {
                    name: alias.as_ref().map(|alias| lower(&alias.name)),
                    columns: None,
                });
                return;
            }
            let columns = if let Some(alias) = alias.as_ref().filter(|a| !a.columns.is_empty()) {
                Some(alias.columns.iter().map(|c| lower(&c.name)).collect())
            } else if let [single] = parts.as_slice()
                && let Some(cte) = env.lookup(&single.to_lowercase())
            {
                cte.clone()
            } else {
                relation_columns(&parts.join(".")).map(|columns| {
                    columns
                        .into_iter()
                        .map(|column| column.to_lowercase())
                        .collect()
                })
            };
            let binding_name = alias
                .as_ref()
                .map(|alias| lower(&alias.name))
                .or_else(|| parts.last().map(|part| part.to_lowercase()));
            bindings.push(Binding {
                name: binding_name,
                columns,
            });
        }
        TableFactor::Derived {
            subquery, alias, ..
        } => {
            let columns = match alias.as_ref().filter(|a| !a.columns.is_empty()) {
                Some(alias) => Some(alias.columns.iter().map(|c| lower(&c.name)).collect()),
                None => query_output_names(subquery),
            };
            bindings.push(Binding {
                name: alias.as_ref().map(|alias| lower(&alias.name)),
                columns,
            });
        }
        TableFactor::NestedJoin {
            table_with_joins,
            alias,
        } => {
            if alias.is_some() {
                bindings.push(Binding {
                    name: alias.as_ref().map(|alias| lower(&alias.name)),
                    columns: None,
                });
                return;
            }
            collect_bindings(&table_with_joins.relation, env, relation_columns, bindings);
            for join in &table_with_joins.joins {
                collect_bindings(&join.relation, env, relation_columns, bindings);
            }
        }
        // Table functions, UNNEST, PIVOT, and the like: the binding exists,
        // but Rocky does not know what it produces.
        other => {
            let alias = match other {
                TableFactor::Function { alias, .. }
                | TableFactor::UNNEST { alias, .. }
                | TableFactor::TableFunction { alias, .. }
                | TableFactor::Pivot { alias, .. }
                | TableFactor::Unpivot { alias, .. } => alias.as_ref(),
                _ => None,
            };
            bindings.push(Binding {
                name: alias.map(|alias| lower(&alias.name)),
                columns: None,
            });
        }
    }
}

/// Queries directly nested in `node`, not counting queries nested in those.
fn immediate_subqueries<T: Visit>(node: &T) -> Vec<Query> {
    struct Collector {
        depth: usize,
        found: Vec<Query>,
    }
    impl Visitor for Collector {
        type Break = ();
        fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<()> {
            if self.depth == 0 {
                self.found.push(query.clone());
            }
            self.depth += 1;
            ControlFlow::Continue(())
        }
        fn post_visit_query(&mut self, _query: &Query) -> ControlFlow<()> {
            self.depth -= 1;
            ControlFlow::Continue(())
        }
    }
    let mut collector = Collector {
        depth: 0,
        found: Vec::new(),
    };
    let _ = node.visit(&mut collector);
    collector.found
}

/// Whether `expr` contains an aggregate of the current query level: an
/// aggregate call with no `OVER`, outside any subquery.
fn contains_aggregate(expr: &Expr) -> bool {
    struct Finder {
        depth: usize,
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
                && function.over.is_none()
                && (!function.within_group.is_empty()
                    || function.filter.is_some()
                    || function_name(function).is_some_and(|name| is_aggregate(&name)))
            {
                return ControlFlow::Break(());
            }
            ControlFlow::Continue(())
        }
    }
    expr.visit(&mut Finder { depth: 0 }).is_break()
}

fn function_name(function: &ast::Function) -> Option<String> {
    let [part] = function.name.0.as_slice() else {
        return None;
    };
    part.as_ident().map(|ident| ident.value.to_uppercase())
}

struct Checker<'a> {
    grouped: &'a HashSet<String>,
    aliases: &'a HashSet<String>,
    bindings: &'a [Binding],
    clause: &'static str,
    global: bool,
    findings: &'a mut Vec<Finding>,
}

impl Checker<'_> {
    fn walk(&mut self, expr: &Expr) {
        match expr {
            Expr::Identifier(ident) => self.reference(std::slice::from_ref(ident)),
            Expr::CompoundIdentifier(parts) => self.reference(parts),
            Expr::Nested(inner)
            | Expr::UnaryOp { expr: inner, .. }
            | Expr::Cast { expr: inner, .. }
            | Expr::IsNull(inner)
            | Expr::IsNotNull(inner)
            | Expr::IsTrue(inner)
            | Expr::IsNotTrue(inner)
            | Expr::IsFalse(inner)
            | Expr::IsNotFalse(inner)
            | Expr::IsUnknown(inner)
            | Expr::IsNotUnknown(inner)
            | Expr::Collate { expr: inner, .. }
            | Expr::Extract { expr: inner, .. }
            | Expr::Ceil { expr: inner, .. }
            | Expr::Floor { expr: inner, .. }
            | Expr::InSubquery { expr: inner, .. } => self.walk(inner),
            Expr::BinaryOp { left, right, .. }
            | Expr::IsDistinctFrom(left, right)
            | Expr::IsNotDistinctFrom(left, right)
            | Expr::AtTimeZone {
                timestamp: left,
                time_zone: right,
            }
            | Expr::Position {
                expr: left,
                r#in: right,
            } => {
                self.walk(left);
                self.walk(right);
            }
            Expr::Like {
                expr,
                pattern,
                escape_char,
                ..
            }
            | Expr::ILike {
                expr,
                pattern,
                escape_char,
                ..
            } => {
                self.walk(expr);
                self.walk(pattern);
                if let Some(escape) = escape_char {
                    self.walk(escape);
                }
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                self.walk(expr);
                self.walk(low);
                self.walk(high);
            }
            Expr::InList { expr, list, .. } => {
                self.walk(expr);
                list.iter().for_each(|item| self.walk(item));
            }
            Expr::Substring {
                expr,
                substring_from,
                substring_for,
                ..
            } => {
                self.walk(expr);
                substring_from.iter().for_each(|e| self.walk(e));
                substring_for.iter().for_each(|e| self.walk(e));
            }
            Expr::Trim {
                expr,
                trim_what,
                trim_characters,
                ..
            } => {
                self.walk(expr);
                trim_what.iter().for_each(|e| self.walk(e));
                trim_characters.iter().flatten().for_each(|e| self.walk(e));
            }
            Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                operand.iter().for_each(|e| self.walk(e));
                for when in conditions {
                    self.walk(&when.condition);
                    self.walk(&when.result);
                }
                else_result.iter().for_each(|e| self.walk(e));
            }
            Expr::Tuple(items) => items.iter().for_each(|item| self.walk(item)),
            Expr::Function(function) => self.function(function),
            // Literals, subqueries (their own scope), lambdas, field access,
            // and every shape not modelled here: nothing to report.
            _ => {}
        }
    }

    fn function(&mut self, function: &ast::Function) {
        if !function.within_group.is_empty() || function.filter.is_some() {
            return; // an aggregate (ordered-set or filtered)
        }
        let Some(name) = function_name(function) else {
            return;
        };
        let FunctionArguments::List(list) = &function.args else {
            return;
        };
        match &function.over {
            None => {
                if is_aggregate(&name)
                    || list.duplicate_treatment.is_some()
                    || !list.clauses.is_empty()
                    || !is_scalar(&name)
                {
                    return;
                }
            }
            Some(over) => {
                if !(is_aggregate(&name) || is_window(&name) || is_scalar(&name))
                    || list.duplicate_treatment.is_some()
                {
                    return;
                }
                if let WindowType::WindowSpec(spec) = over
                    && spec.window_name.is_none()
                {
                    spec.partition_by.iter().for_each(|e| self.walk(e));
                    spec.order_by.iter().for_each(|o| self.walk(&o.expr));
                }
            }
        }
        for arg in &list.args {
            let arg = match arg {
                FunctionArg::Unnamed(arg)
                | FunctionArg::Named { arg, .. }
                | FunctionArg::ExprNamed { arg, .. } => arg,
            };
            if let FunctionArgExpr::Expr(expr) = arg {
                if let Expr::Identifier(ident) = expr
                    && ident.quote_style.is_none()
                    && is_date_part(&ident.value)
                {
                    continue;
                }
                self.walk(expr);
            }
        }
    }

    fn reference(&mut self, parts: &[ast::Ident]) {
        let Some(last) = parts.last() else {
            return;
        };
        let column = lower(last);
        if parts.iter().any(|part| self.grouped.contains(&lower(part))) {
            return;
        }
        let first = lower(&parts[0]);
        if self.aliases.contains(&first) {
            return;
        }
        let certain = match parts {
            [only] => {
                (only.quote_style.is_some() || !is_keyword_value(&only.value))
                    && !self
                        .bindings
                        .iter()
                        .any(|binding| binding.name.as_deref() == Some(column.as_str()))
                    && self.bindings.iter().any(|binding| {
                        binding
                            .columns
                            .as_ref()
                            .is_some_and(|columns| columns.contains(&column))
                    })
            }
            [qualifier, _] => {
                let qualifier = lower(qualifier);
                // `s.f` may be a field of a STRUCT column `s` rather than a
                // qualified column; stay silent when both readings exist.
                let struct_reading = self.bindings.iter().any(|binding| {
                    binding
                        .columns
                        .as_ref()
                        .is_some_and(|columns| columns.contains(&qualifier))
                });
                let mut matches = self
                    .bindings
                    .iter()
                    .filter(|binding| binding.name.as_deref() == Some(qualifier.as_str()));
                let only = matches.next();
                !struct_reading
                    && matches.next().is_none()
                    && only.is_some_and(|binding| {
                        binding
                            .columns
                            .as_ref()
                            .is_some_and(|columns| columns.contains(&column))
                    })
            }
            _ => false,
        };
        if certain {
            let shown = parts
                .iter()
                .map(|part| part.value.as_str())
                .collect::<Vec<_>>()
                .join(".");
            self.findings.push(Finding {
                column: shown,
                clause: self.clause,
                global: self.global,
            });
        }
    }
}

/// Aggregate functions across DuckDB, Databricks, Snowflake, BigQuery, and
/// Trino. A call to one of these without `OVER` makes the query aggregating.
fn is_aggregate(name: &str) -> bool {
    matches!(
        name,
        "COUNT"
            | "SUM"
            | "AVG"
            | "MIN"
            | "MAX"
            | "ANY_VALUE"
            | "ARBITRARY"
            | "FIRST"
            | "LAST"
            | "ARRAY_AGG"
            | "LIST"
            | "STRING_AGG"
            | "LISTAGG"
            | "GROUP_CONCAT"
            | "COLLECT_LIST"
            | "COLLECT_SET"
            | "COUNT_IF"
            | "COUNTIF"
            | "BOOL_AND"
            | "BOOL_OR"
            | "LOGICAL_AND"
            | "LOGICAL_OR"
            | "EVERY"
            | "BIT_AND"
            | "BIT_OR"
            | "BIT_XOR"
            | "MEDIAN"
            | "MODE"
            | "PERCENTILE"
            | "PERCENTILE_CONT"
            | "PERCENTILE_DISC"
            | "APPROX_PERCENTILE"
            | "QUANTILE"
            | "QUANTILE_CONT"
            | "QUANTILE_DISC"
            | "APPROX_COUNT_DISTINCT"
            | "APPROX_DISTINCT"
            | "APPROX_QUANTILES"
            | "APPROX_TOP_K"
            | "STDDEV"
            | "STDDEV_POP"
            | "STDDEV_SAMP"
            | "VARIANCE"
            | "VAR_POP"
            | "VAR_SAMP"
            | "CORR"
            | "COVAR_POP"
            | "COVAR_SAMP"
            | "ARG_MAX"
            | "ARG_MIN"
            | "ARGMAX"
            | "ARGMIN"
            | "MAX_BY"
            | "MIN_BY"
            | "PRODUCT"
            | "KURTOSIS"
            | "SKEWNESS"
            | "ENTROPY"
            | "HISTOGRAM"
            | "FSUM"
            | "SUMKAHAN"
            | "KAHAN_SUM"
            | "FAVG"
            | "TRY_SUM"
            | "TRY_AVG"
            | "OBJECT_AGG"
            | "HASH_AGG"
            | "GROUPING"
            | "GROUPING_ID"
    ) || name.starts_with("REGR_")
}

/// Window-only functions. Their arguments are evaluated after grouping.
fn is_window(name: &str) -> bool {
    matches!(
        name,
        "ROW_NUMBER"
            | "RANK"
            | "DENSE_RANK"
            | "PERCENT_RANK"
            | "CUME_DIST"
            | "NTILE"
            | "LAG"
            | "LEAD"
            | "FIRST_VALUE"
            | "LAST_VALUE"
            | "NTH_VALUE"
    )
}

/// Scalar functions whose arguments are checked. Anything not listed may be a
/// user-defined aggregate, so its arguments are left alone.
fn is_scalar(name: &str) -> bool {
    matches!(
        name,
        "UPPER"
            | "LOWER"
            | "COALESCE"
            | "NULLIF"
            | "IFNULL"
            | "NVL"
            | "NVL2"
            | "IF"
            | "IFF"
            | "CONCAT"
            | "CONCAT_WS"
            | "LENGTH"
            | "LEN"
            | "TRIM"
            | "LTRIM"
            | "RTRIM"
            | "REPLACE"
            | "SUBSTR"
            | "SUBSTRING"
            | "LEFT"
            | "RIGHT"
            | "LPAD"
            | "RPAD"
            | "SPLIT_PART"
            | "REGEXP_REPLACE"
            | "ROUND"
            | "ABS"
            | "FLOOR"
            | "CEIL"
            | "CEILING"
            | "SIGN"
            | "SQRT"
            | "POWER"
            | "POW"
            | "MOD"
            | "GREATEST"
            | "LEAST"
            | "DATE_TRUNC"
            | "DATE_PART"
            | "DATEADD"
            | "DATE_ADD"
            | "DATEDIFF"
            | "DATE_DIFF"
            | "YEAR"
            | "MONTH"
            | "DAY"
            | "TO_DATE"
            | "MD5"
            | "SHA256"
    )
}

/// Date-part keywords that appear as bare identifiers in function arguments
/// (`DATEADD(day, 1, d)`, `DATE_TRUNC(month, d)`). They are not columns.
fn is_date_part(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "year"
            | "years"
            | "quarter"
            | "month"
            | "months"
            | "week"
            | "weeks"
            | "day"
            | "days"
            | "dayofweek"
            | "dayofyear"
            | "dow"
            | "doy"
            | "hour"
            | "hours"
            | "minute"
            | "minutes"
            | "second"
            | "seconds"
            | "millisecond"
            | "milliseconds"
            | "microsecond"
            | "microseconds"
            | "nanosecond"
            | "epoch"
            | "isoyear"
            | "isodow"
    )
}

/// Unquoted names that are niladic functions, not columns.
fn is_keyword_value(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "current_date"
            | "current_time"
            | "current_timestamp"
            | "localtime"
            | "localtimestamp"
            | "current_user"
            | "session_user"
            | "user"
            | "current_role"
            | "current_catalog"
            | "current_schema"
            | "current_database"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Corpus seed: `raw.orders`, `raw.customers`, plus one upstream model.
    fn lookup(name: &str) -> Option<Vec<String>> {
        let columns: &[&str] = match name.to_lowercase().as_str() {
            "raw.orders" => &["order_id", "customer_id", "amount", "status", "order_date"],
            "raw.customers" => &["customer_id", "customer_name", "email"],
            "stg_orders" => &["order_id", "customer_id", "amount"],
            "raw.events" => &["event_id", "payload", "tags"],
            _ => return None,
        };
        Some(columns.iter().map(|c| (*c).to_string()).collect())
    }

    fn check(sql: &str) -> Vec<Diagnostic> {
        check_group_by("m", sql, &lookup)
    }

    #[test]
    fn d6_names_status_and_suggests_both_fixes() {
        let diags = check(
            "SELECT customer_id, status, SUM(amount) AS t FROM raw.orders GROUP BY customer_id",
        );
        assert_eq!(diags.len(), 1, "{diags:?}");
        let diag = &diags[0];
        assert_eq!(&*diag.code, E044);
        assert!(diag.message.contains("'status'"), "{}", diag.message);
        let suggestion = diag.suggestion.as_deref().unwrap_or_default();
        assert!(suggestion.contains("GROUP BY"), "{suggestion}");
        assert!(suggestion.contains("ANY_VALUE(status)"), "{suggestion}");
    }

    const VALID: &[&str] = &[
        // C2
        "SELECT c.customer_name, SUM(o.amount) AS total FROM stg_orders o \
         JOIN raw.customers c ON o.customer_id = c.customer_id GROUP BY c.customer_name",
        // Not aggregating at all.
        "SELECT order_id, status FROM raw.orders",
        "SELECT DISTINCT status, customer_id FROM raw.orders",
        // V1 lateral alias, not aggregating.
        "SELECT order_id AS id2, id2 + 1 AS next_id FROM raw.orders",
        // GROUP BY ALL.
        "SELECT customer_id, status, SUM(amount) FROM raw.orders GROUP BY ALL",
        // Ordinals.
        "SELECT customer_id, status, SUM(amount) FROM raw.orders GROUP BY 1, 2",
        // Alias grouping.
        "SELECT UPPER(status) AS s, COUNT(*) FROM raw.orders GROUP BY s",
        "SELECT status AS st, COUNT(*) AS n FROM raw.orders GROUP BY st ORDER BY st",
        // Expressions over grouped columns.
        "SELECT UPPER(status), COUNT(*) FROM raw.orders GROUP BY status",
        "SELECT customer_id + 1 AS next, SUM(amount) FROM raw.orders GROUP BY customer_id",
        "SELECT DATE_TRUNC('month', order_date) AS m, SUM(amount) FROM raw.orders \
         GROUP BY DATE_TRUNC('month', order_date)",
        // Constants and literals.
        "SELECT customer_id, 'x' AS tag, 1 + 2 AS three, NULL AS n, SUM(amount) \
         FROM raw.orders GROUP BY customer_id",
        // Qualification differs between SELECT and GROUP BY.
        "SELECT o.customer_id, COUNT(*) FROM raw.orders o GROUP BY customer_id",
        "SELECT customer_id, COUNT(*) FROM raw.orders o GROUP BY o.customer_id",
        // ROLLUP / CUBE / GROUPING SETS.
        "SELECT customer_id, status, SUM(amount), GROUPING(status) FROM raw.orders \
         GROUP BY ROLLUP (customer_id, status)",
        "SELECT customer_id, status, SUM(amount) FROM raw.orders GROUP BY CUBE (customer_id, status)",
        "SELECT customer_id, status, SUM(amount) FROM raw.orders \
         GROUP BY GROUPING SETS ((customer_id), (status), ())",
        // Window over aggregate.
        "SELECT customer_id, SUM(SUM(amount)) OVER () AS grand FROM raw.orders GROUP BY customer_id",
        "SELECT customer_id, RANK() OVER (ORDER BY SUM(amount) DESC) AS r FROM raw.orders \
         GROUP BY customer_id",
        // FILTER and ANY_VALUE / FIRST / ARBITRARY.
        "SELECT customer_id, COUNT(*) FILTER (WHERE status = 'done') AS done FROM raw.orders \
         GROUP BY customer_id",
        "SELECT customer_id, ANY_VALUE(status), FIRST(order_date), ARBITRARY(amount) \
         FROM raw.orders GROUP BY customer_id",
        // QUALIFY is not checked.
        "SELECT customer_id, SUM(amount) AS t FROM raw.orders GROUP BY customer_id \
         QUALIFY ROW_NUMBER() OVER (ORDER BY t) = 1",
        // Global aggregates with only aggregates and constants.
        "SELECT COUNT(*), MAX(order_date), 'all' AS scope FROM raw.orders",
        // Lateral alias referencing an aggregate alias.
        "SELECT customer_id, SUM(amount) AS total, total * 2 AS doubled FROM raw.orders \
         GROUP BY customer_id",
        // HAVING and ORDER BY on aggregates, aliases, ordinals.
        "SELECT customer_id, SUM(amount) AS t FROM raw.orders GROUP BY customer_id \
         HAVING SUM(amount) > 10 AND customer_id > 0 ORDER BY t DESC, 1",
        // Subqueries are their own scope; correlated outer refs are silent.
        "SELECT customer_id, (SELECT MAX(c.email) FROM raw.customers c \
         WHERE c.customer_id = o.customer_id) AS e FROM raw.orders o GROUP BY customer_id",
        "SELECT customer_id, COUNT(*) FROM raw.orders o WHERE EXISTS \
         (SELECT 1 FROM raw.customers c WHERE c.customer_id = o.customer_id) GROUP BY customer_id",
        "SELECT s.customer_id, s.t FROM (SELECT customer_id, SUM(amount) AS t FROM raw.orders \
         GROUP BY customer_id) AS s",
        // CTEs.
        "WITH agg AS (SELECT customer_id, SUM(amount) AS t FROM raw.orders GROUP BY customer_id) \
         SELECT customer_id, t FROM agg",
        "WITH stg_orders AS (SELECT order_id, status FROM raw.orders) \
         SELECT status, COUNT(*) FROM stg_orders GROUP BY status",
        // Unknown relations and unresolved names stay silent.
        "SELECT customer_id, region, SUM(amount) FROM warehouse.unknown GROUP BY customer_id",
        "SELECT customer_id, region, SUM(amount) FROM raw.orders GROUP BY customer_id",
        // Unknown functions might be user-defined aggregates.
        "SELECT customer_id, my_udaf(status) FROM raw.orders GROUP BY customer_id",
        // Date-part keywords are not columns, even if a column shares the name.
        "SELECT DATEADD(day, 1, order_date), COUNT(*) FROM raw.orders GROUP BY order_date",
        // A name that is also a projection alias is never reported.
        "SELECT customer_id AS status, SUM(amount) FROM raw.orders GROUP BY customer_id \
         ORDER BY status",
        // Niladic keywords.
        "SELECT customer_id, CURRENT_DATE AS d, SUM(amount) FROM raw.orders GROUP BY customer_id",
        // UNION branches each valid; ORDER BY applies to the union.
        "SELECT customer_id, SUM(amount) AS t FROM raw.orders GROUP BY customer_id \
         UNION ALL SELECT customer_id, 0 FROM raw.customers ORDER BY customer_id",
        // Grouping by an expression covers its columns (deliberate miss).
        "SELECT order_date, COUNT(*) FROM raw.orders GROUP BY DATE_TRUNC('month', order_date)",
        // Struct field access on a column.
        "SELECT payload.kind, COUNT(*) FROM raw.events GROUP BY payload",
        // More shapes, each confirmed valid against DuckDB 1.5.
        "SELECT customer_id AS c, COUNT(*) FROM raw.orders GROUP BY c HAVING c > 1",
        "SELECT status FROM raw.orders GROUP BY status HAVING COUNT(*) > 1",
        "SELECT customer_id, COUNT(*) FROM raw.orders JOIN raw.customers USING (customer_id) GROUP BY customer_id",
        "SELECT orders.status, COUNT(*) FROM raw.orders GROUP BY orders.status",
        "SELECT customer_id, COUNT(*) AS n FROM raw.orders GROUP BY customer_id ORDER BY COUNT(*) DESC, n",
        "SELECT customer_id, COALESCE(SUM(amount), 0) AS t, CAST(MAX(amount) - MIN(amount) AS INT) AS spread FROM raw.orders GROUP BY customer_id",
        "SELECT customer_id, string_agg(status, ',' ORDER BY order_date) AS s, COUNT(DISTINCT status) AS d FROM raw.orders GROUP BY customer_id",
        "SELECT customer_id, list_transform(list(status), x -> upper(x)) AS l FROM raw.orders GROUP BY customer_id",
        "SELECT 1 AS one FROM raw.orders WHERE status = 'x' GROUP BY customer_id",
        "SELECT customer_id FROM raw.orders o GROUP BY customer_id HAVING EXISTS (SELECT 1 FROM raw.customers c WHERE c.customer_id = o.customer_id)",
        "SELECT customer_id, (SELECT COUNT(*) + o.customer_id FROM raw.customers) AS x FROM raw.orders o GROUP BY customer_id",
        "SELECT Customer_ID, SUM(amount) FROM raw.orders GROUP BY customer_id",
        "SELECT customer_id, SUM(amount) FROM raw.orders GROUP BY \"customer_id\"",
        "SELECT status, COUNT(*) FROM raw.orders GROUP BY 1 ORDER BY 2 DESC",
        "SELECT customer_id, EXTRACT(YEAR FROM order_date) AS y, COUNT(*) FROM raw.orders GROUP BY customer_id, EXTRACT(YEAR FROM order_date)",
        "SELECT customer_id, status, SUM(SUM(amount)) OVER (PARTITION BY status) AS s FROM raw.orders GROUP BY customer_id, status",
        "SELECT customer_id, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY MAX(order_date)) AS rn FROM raw.orders GROUP BY customer_id",
        "SELECT customer_id, CASE WHEN SUM(amount) > 10 THEN 'big' ELSE 'small' END AS size FROM raw.orders GROUP BY customer_id",
        "SELECT o.customer_id, c.customer_name, SUM(o.amount) FROM raw.orders o JOIN raw.customers c ON o.customer_id = c.customer_id GROUP BY o.customer_id, c.customer_name",
        "SELECT customer_id, amount > 0 AS positive, COUNT(*) FROM raw.orders GROUP BY customer_id, amount > 0",
        "SELECT customer_id, SUM(amount) FROM raw.orders GROUP BY customer_id ORDER BY customer_id NULLS LAST",
        "SELECT customer_id, upper(status) AS us, COUNT(*) FROM raw.orders GROUP BY customer_id, us",
        "SELECT MAX(amount) AS m FROM raw.orders HAVING MAX(amount) > 0",
        "SELECT customer_id, GREATEST(MAX(amount), 0) AS g FROM raw.orders GROUP BY customer_id",
        UNPARSEABLE,
    ];

    /// Unparseable SQL stays silent.
    const UNPARSEABLE: &str = "SELECT FROM WHERE GROUP BY BY";

    #[test]
    fn valid_grouping_shapes_stay_clean() {
        assert!(VALID.len() >= 25);
        for sql in VALID {
            // A clean result must come from the check, not a parse failure.
            assert_eq!(
                Parser::parse_sql(&rocky_sql::dialect::DatabricksDialect, sql).is_ok(),
                *sql != UNPARSEABLE,
                "parse outcome for `{sql}`"
            );
            let diags = check(sql);
            assert!(diags.is_empty(), "false refusal for `{sql}`: {diags:?}");
        }
    }

    /// (SQL, the column E044 must name)
    const INVALID: &[(&str, &str)] = &[
        (
            "SELECT customer_id, status, SUM(amount) AS t FROM raw.orders GROUP BY customer_id",
            "status",
        ),
        (
            "SELECT o.customer_id, o.status, SUM(o.amount) FROM raw.orders o GROUP BY o.customer_id",
            "o.status",
        ),
        // Global aggregate with a bare column.
        ("SELECT status, COUNT(*) FROM raw.orders", "status"),
        // Scalar function over an ungrouped column.
        (
            "SELECT customer_id, UPPER(status), COUNT(*) FROM raw.orders GROUP BY customer_id",
            "status",
        ),
        // HAVING on an ungrouped column.
        (
            "SELECT customer_id, COUNT(*) FROM raw.orders GROUP BY customer_id \
             HAVING status = 'done'",
            "status",
        ),
        // ORDER BY an ungrouped column.
        (
            "SELECT customer_id, COUNT(*) FROM raw.orders GROUP BY customer_id ORDER BY order_date",
            "order_date",
        ),
        // Window over an ungrouped column in a grouped query.
        (
            "SELECT customer_id, SUM(amount) OVER () FROM raw.orders GROUP BY customer_id",
            "amount",
        ),
        // Join: grouped by one side, reads another column.
        (
            "SELECT c.customer_name, c.email, SUM(o.amount) FROM raw.orders o \
             JOIN raw.customers c ON o.customer_id = c.customer_id GROUP BY c.customer_name",
            "c.email",
        ),
        // Inside a CTE.
        (
            "WITH agg AS (SELECT customer_id, status, SUM(amount) AS t FROM raw.orders \
             GROUP BY customer_id) SELECT * FROM agg",
            "status",
        ),
        // Inside a derived table.
        (
            "SELECT * FROM (SELECT customer_id, amount, COUNT(*) AS n FROM raw.orders \
             GROUP BY customer_id) AS s",
            "amount",
        ),
        // Inside a subquery of a non-aggregating outer query.
        (
            "SELECT order_id FROM raw.orders WHERE amount > \
             (SELECT AVG(amount) + order_id FROM raw.orders)",
            "order_id",
        ),
        // Upstream model columns resolve too.
        (
            "SELECT customer_id, amount, COUNT(*) FROM stg_orders GROUP BY customer_id",
            "amount",
        ),
        // CASE over an ungrouped column.
        (
            "SELECT customer_id, CASE WHEN status = 'x' THEN 1 ELSE 0 END, COUNT(*) \
             FROM raw.orders GROUP BY customer_id",
            "status",
        ),
    ];

    #[test]
    fn invalid_grouping_shapes_refuse_with_e044() {
        assert!(INVALID.len() >= 8);
        for (sql, column) in INVALID {
            let diags = check(sql);
            assert!(
                diags
                    .iter()
                    .any(|d| &*d.code == E044 && d.message.contains(&format!("'{column}'"))),
                "expected E044 naming '{column}' for `{sql}`, got {diags:?}"
            );
        }
    }

    #[test]
    fn repeated_reference_reports_once_per_clause() {
        let diags = check(
            "SELECT customer_id, status, status || 'x', COUNT(*) FROM raw.orders \
             GROUP BY customer_id",
        );
        assert_eq!(diags.len(), 1, "{diags:?}");
    }

    #[test]
    fn global_aggregate_message_names_missing_group_by() {
        let diags = check("SELECT status, COUNT(*) FROM raw.orders");
        assert_eq!(diags.len(), 1);
        assert!(
            diags[0].message.contains("without GROUP BY"),
            "{}",
            diags[0].message
        );
    }
}
