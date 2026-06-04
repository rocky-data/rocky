//! Canonical re-emit of model SQL for cosmetic-invariant comparison.
//!
//! [`normalize`] parses a single model `SELECT` through the same parser the
//! rest of the engine uses, deterministically renames *internal* table /
//! CTE / derived-table aliases to positional tokens (`_t0`, `_t1`, ...), and
//! re-emits the AST via its `Display` impl. The re-emit alone collapses
//! whitespace and strips comments, and the parser's `Display` canonicalises
//! keyword case (`select` and `SELECT` render identically) — so the returned
//! string is invariant to those cosmetic edits.
//!
//! ## What is and is not normalised
//!
//! - **Renamed:** table-level aliases — the alias on a `FROM` / `JOIN`
//!   relation, on a derived (sub-query) table, and on a `WITH` CTE — plus
//!   every reference to them (a bare relation name that points at a CTE, and
//!   the table-qualifier of a `t.col` column reference). References are
//!   rewritten in lockstep with their definition so `t AS a` and `t AS b`
//!   normalise to the same string.
//! - **Untouched:** projection / output column aliases (`SELECT expr AS foo`).
//!   Output column names are a *semantic* fact — they are carried separately
//!   in the typed schema — so they must survive normalisation unchanged.
//!   String and quoted-identifier *contents* are likewise never rewritten:
//!   the re-emit is structural, never a blanket `to_lowercase`, so two queries
//!   differing only in a literal can never collapse to the same string.
//!
//! ## Conservatism
//!
//! This is a best-effort *cosmetic* canonicaliser, deliberately biased toward
//! under-normalising. When an alias cannot be unambiguously renamed (for
//! example a name reused across nested scopes) it is left as-is: a missed
//! collapse costs one redundant comparison-miss, whereas a wrong collapse
//! would equate two genuinely different queries. The function errs toward
//! distinctness.
//!
//! Alias *collection* walks `FROM` / `JOIN` / `WITH` positions; aliases bound
//! only inside a `WHERE` / projection sub-query are not collected, so they are
//! not renamed. The effect is purely under-normalisation (such queries stay
//! more distinct), never a wrong collapse.

use std::collections::BTreeMap;
use std::ops::ControlFlow;

use sqlparser::ast::{
    Cte, Expr, Ident, Query, SetExpr, Statement, TableAlias, TableFactor, TableWithJoins,
    visit_expressions_mut, visit_relations_mut,
};

use crate::parser::parse_single_statement;

/// Re-emit `sql` in a canonical, cosmetic-invariant form.
///
/// Returns `Some(normalized)` when `sql` parses as a single statement, or
/// `None` when it does not. A `None` here is a deliberate fail-safe signal:
/// callers that compare normalised forms must treat an un-normalisable model
/// as *never equal* to any other, never as a match.
///
/// The result is invariant to comment edits, insignificant whitespace,
/// keyword case, and internal table/CTE/derived-table alias renames. It is
/// **not** a semantic-equivalence oracle — it does not fold join-order,
/// parenthesisation, or `AS`-keyword optionality, and it never rewrites
/// string-literal or output-column-name content.
#[must_use]
pub fn normalize(sql: &str) -> Option<String> {
    let mut statement = parse_single_statement(sql).ok()?;

    // Pass 1: collect internal table-level alias names in deterministic
    // first-seen traversal order and assign each a positional token.
    let mut renames: BTreeMap<String, String> = BTreeMap::new();
    let mut order: Vec<String> = Vec::new();
    if let Statement::Query(query) = &statement {
        collect_aliases(query, &mut order);
    }
    for name in order {
        let next = renames.len();
        renames.entry(name).or_insert_with(|| format!("_t{next}"));
    }

    if renames.is_empty() {
        // No internal aliases to rewrite — the Display re-emit already gives
        // us comment/whitespace/keyword-case invariance.
        return Some(statement.to_string());
    }

    // Pass 2a: rewrite the alias *definitions* (CTE alias, table/derived
    // alias). Neither `visit_relations_mut` nor `visit_expressions_mut`
    // reaches these, so they need a dedicated structural walk.
    if let Statement::Query(query) = &mut statement {
        rewrite_alias_defs(query, &renames);
    }

    // Pass 2b: rewrite bare relation references that point at a renamed CTE.
    // A multi-part name (`schema.table`) is a real table, never a CTE alias,
    // so only single-part names are candidates.
    let _: ControlFlow<()> = visit_relations_mut(&mut statement, |relation| {
        if relation.0.len() == 1
            && let Some(ident) = relation.0[0].as_ident()
            && let Some(token) = renames.get(&ident.value)
        {
            relation.0[0] = sqlparser::ast::ObjectNamePart::Identifier(plain_ident(token));
        }
        ControlFlow::Continue(())
    });

    // Pass 2c: rewrite the table-qualifier of `alias.col` column references.
    let _: ControlFlow<()> = visit_expressions_mut(&mut statement, |expr| {
        if let Expr::CompoundIdentifier(parts) = expr
            && parts.len() >= 2
        {
            let qualifier = &parts[parts.len() - 2].value;
            if let Some(token) = renames.get(qualifier) {
                let idx = parts.len() - 2;
                parts[idx] = plain_ident(token);
            }
        }
        ControlFlow::Continue(())
    });

    Some(statement.to_string())
}

/// Build an unquoted identifier carrying the positional token. Tokens are
/// `^_t[0-9]+$`, always safe to emit bare.
fn plain_ident(value: &str) -> Ident {
    Ident::new(value.to_string())
}

/// Collect internal table-level alias names in deterministic traversal order.
///
/// Order is fixed by the AST shape: a query's `WITH` CTEs in declaration
/// order, then the body. The same name may appear more than once (shadowing);
/// the caller's first-seen numbering handles that. Names that flow into the
/// rename map are matched verbatim later, so collection is purely additive.
fn collect_aliases(query: &Query, order: &mut Vec<String>) {
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            order.push(cte.alias.name.value.clone());
            collect_aliases(&cte.query, order);
        }
    }
    collect_set_expr_aliases(&query.body, order);
}

fn collect_set_expr_aliases(body: &SetExpr, order: &mut Vec<String>) {
    match body {
        SetExpr::Select(select) => {
            for twj in &select.from {
                collect_table_with_joins(twj, order);
            }
        }
        SetExpr::Query(inner) => collect_aliases(inner, order),
        SetExpr::SetOperation { left, right, .. } => {
            collect_set_expr_aliases(left, order);
            collect_set_expr_aliases(right, order);
        }
        _ => {}
    }
}

fn collect_table_with_joins(twj: &TableWithJoins, order: &mut Vec<String>) {
    collect_table_factor(&twj.relation, order);
    for join in &twj.joins {
        collect_table_factor(&join.relation, order);
    }
}

fn collect_table_factor(factor: &TableFactor, order: &mut Vec<String>) {
    match factor {
        TableFactor::Table { alias: Some(a), .. } => order.push(a.name.value.clone()),
        TableFactor::Derived {
            subquery, alias, ..
        } => {
            if let Some(a) = alias {
                order.push(a.name.value.clone());
            }
            collect_aliases(subquery, order);
        }
        _ => {}
    }
}

/// Rewrite every internal alias *definition* to its positional token.
fn rewrite_alias_defs(query: &mut Query, renames: &BTreeMap<String, String>) {
    if let Some(with) = &mut query.with {
        for cte in &mut with.cte_tables {
            rewrite_cte_alias(cte, renames);
            rewrite_alias_defs(&mut cte.query, renames);
        }
    }
    rewrite_set_expr_alias_defs(&mut query.body, renames);
}

fn rewrite_cte_alias(cte: &mut Cte, renames: &BTreeMap<String, String>) {
    if let Some(token) = renames.get(&cte.alias.name.value) {
        cte.alias.name = plain_ident(token);
    }
}

fn rewrite_set_expr_alias_defs(body: &mut SetExpr, renames: &BTreeMap<String, String>) {
    match body {
        SetExpr::Select(select) => {
            for twj in &mut select.from {
                rewrite_table_with_joins_alias(twj, renames);
            }
        }
        SetExpr::Query(inner) => rewrite_alias_defs(inner, renames),
        SetExpr::SetOperation { left, right, .. } => {
            rewrite_set_expr_alias_defs(left, renames);
            rewrite_set_expr_alias_defs(right, renames);
        }
        _ => {}
    }
}

fn rewrite_table_with_joins_alias(twj: &mut TableWithJoins, renames: &BTreeMap<String, String>) {
    rewrite_table_factor_alias(&mut twj.relation, renames);
    for join in &mut twj.joins {
        rewrite_table_factor_alias(&mut join.relation, renames);
    }
}

fn rewrite_table_factor_alias(factor: &mut TableFactor, renames: &BTreeMap<String, String>) {
    match factor {
        TableFactor::Table { alias, .. } => rewrite_alias_opt(alias, renames),
        TableFactor::Derived {
            subquery, alias, ..
        } => {
            rewrite_alias_opt(alias, renames);
            rewrite_alias_defs(subquery, renames);
        }
        _ => {}
    }
}

fn rewrite_alias_opt(alias: &mut Option<TableAlias>, renames: &BTreeMap<String, String>) {
    if let Some(a) = alias
        && let Some(token) = renames.get(&a.name.value)
    {
        a.name = plain_ident(token);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn comments_are_stripped() {
        let with_comment = "SELECT id /* the key */ FROM cat.sch.t -- trailing\n WHERE id > 0";
        let without = "SELECT id FROM cat.sch.t WHERE id > 0";
        assert_eq!(normalize(with_comment), normalize(without));
        assert!(!normalize(with_comment).unwrap().contains("the key"));
    }

    #[test]
    fn whitespace_is_collapsed() {
        let spaced = "SELECT     id,\n\n   name\nFROM    cat.sch.t";
        let tight = "SELECT id, name FROM cat.sch.t";
        assert_eq!(normalize(spaced), normalize(tight));
    }

    #[test]
    fn keyword_case_is_invariant() {
        // The end is case-invariance, delivered by the AST Display re-emit —
        // not a blanket lowercase. Mixed- and upper-case keyword inputs must
        // normalise to the *same* string.
        let lower = "select id from cat.sch.t where id > 10";
        let upper = "SELECT id FROM cat.sch.t WHERE id > 10";
        assert_eq!(normalize(lower), normalize(upper));
    }

    #[test]
    fn string_literal_case_is_preserved() {
        // A blanket lowercase would collapse these into a false match. The
        // structural re-emit keeps literal contents byte-exact, so they must
        // normalise to *different* strings.
        let a = "SELECT id FROM cat.sch.t WHERE name = 'Alice'";
        let b = "SELECT id FROM cat.sch.t WHERE name = 'alice'";
        assert_ne!(normalize(a), normalize(b));
    }

    #[test]
    fn internal_table_alias_rename_is_invariant() {
        let a = "SELECT a.id FROM cat.sch.orders AS a WHERE a.id > 0";
        let b = "SELECT z.id FROM cat.sch.orders AS z WHERE z.id > 0";
        assert_eq!(normalize(a), normalize(b));
        // The alias really was rewritten to a positional token.
        assert!(normalize(a).unwrap().contains("_t0"));
    }

    #[test]
    fn join_alias_rename_is_invariant() {
        let a =
            "SELECT o.id, c.name FROM cat.sch.orders o JOIN cat.sch.customers c ON o.cid = c.id";
        let b =
            "SELECT x.id, y.name FROM cat.sch.orders x JOIN cat.sch.customers y ON x.cid = y.id";
        assert_eq!(normalize(a), normalize(b));
    }

    #[test]
    fn cte_alias_rename_is_invariant() {
        let a = "WITH foo AS (SELECT id FROM cat.sch.t) SELECT id FROM foo";
        let b = "WITH bar AS (SELECT id FROM cat.sch.t) SELECT id FROM bar";
        assert_eq!(normalize(a), normalize(b));
    }

    #[test]
    fn derived_table_alias_rename_is_invariant() {
        let a = "SELECT s.id FROM (SELECT id FROM cat.sch.t) AS s";
        let b = "SELECT q.id FROM (SELECT id FROM cat.sch.t) AS q";
        assert_eq!(normalize(a), normalize(b));
    }

    #[test]
    fn output_column_alias_is_preserved() {
        // Renaming the *output* column name is a semantic change and must NOT
        // be collapsed by the alias normaliser.
        let a = "SELECT id AS customer_id FROM cat.sch.t";
        let b = "SELECT id AS user_id FROM cat.sch.t";
        assert_ne!(normalize(a), normalize(b));
        assert!(normalize(a).unwrap().contains("customer_id"));
    }

    #[test]
    fn semantic_predicate_change_is_distinct() {
        let a = "SELECT id FROM cat.sch.t WHERE id > 10";
        let b = "SELECT id FROM cat.sch.t WHERE id > 20";
        assert_ne!(normalize(a), normalize(b));
    }

    #[test]
    fn unparseable_sql_returns_none() {
        assert_eq!(normalize("SELCT FROM"), None);
        assert_eq!(normalize(""), None);
        // Multiple statements are not a single model body.
        assert_eq!(normalize("SELECT 1; SELECT 2"), None);
    }

    #[test]
    fn normalization_is_idempotent() {
        let sql = "SELECT a.id FROM cat.sch.orders AS a";
        let once = normalize(sql).unwrap();
        let twice = normalize(&once).unwrap();
        assert_eq!(once, twice);
    }
}
