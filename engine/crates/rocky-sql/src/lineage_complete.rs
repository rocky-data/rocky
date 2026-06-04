//! Whitelist completeness check for the simple FROM/JOIN source walk.
//!
//! [`lineage_is_provably_complete`] returns `true` only when a model's SQL is a
//! shape for which the plain FROM/JOIN table enumeration in
//! [`crate::lineage::extract_lineage`] is *guaranteed* to surface **every**
//! upstream source. It is the gate-local fail-safe that keeps the
//! `--skip-unchanged` model-skip gate from proving "upstream unchanged" against
//! sources the lineage walk silently dropped.
//!
//! # Why this exists
//!
//! `extract_lineage` walks only the top-level `FROM`/`JOIN` relations and
//! records non-`Table` table-factors as either the opaque `(subquery)` marker
//! (for a derived table) or nothing at all (Pivot, Unpivot, Unnest, table
//! functions, JSON tables, nested joins — its `_ => {}` arm). It also never
//! descends into `WHERE`/`HAVING`/projection sub-queries or CTE bodies. So for
//! anything but a plain single `SELECT` over bare tables, its source set can be
//! **incomplete** — and an incomplete-but-non-empty (or empty) source set would
//! let the gate skip a model whose true upstream actually moved. That is silent
//! production staleness, the worst failure the gate exists to prevent.
//!
//! Rather than make `extract_lineage` recurse (it feeds dependency derivation
//! and the lineage/Inspector surfaces, which have different needs), this check
//! stays contained to the skip-gate decision: if the model is not provably
//! simple, the gate forces a build.
//!
//! # The whitelist (allow only known-simple; build on anything else)
//!
//! `true` IFF **all** hold:
//!
//! - the statement parses to a single plain `SELECT` query — a `Query` whose
//!   body is `SetExpr::Select`, **not** a set operation (`UNION`/`INTERSECT`/
//!   `EXCEPT`);
//! - the query carries no CTEs (`WITH` is `None`/empty);
//! - **every** table-factor reachable in `FROM` and every `JOIN` is a bare
//!   `TableFactor::Table` (rejecting `Derived`, `Pivot`, `Unpivot`, `Unnest`,
//!   `Function`, `TableFunction`, `JsonTable`, `NestedJoin`, and any future
//!   variant);
//! - there is **no** sub-query anywhere in the statement — not in the
//!   projection, `WHERE`, `HAVING`, `GROUP BY`, a qualifier, a function
//!   argument, or as a FROM-derived table.
//!
//! The sub-query rule is enforced *structurally*, by counting `Query` nodes:
//! the whole statement must contain exactly one (the root). Every sub-query of
//! any kind — a derived table, an `IN (…)` / `EXISTS (…)` / scalar sub-select,
//! a sub-query passed as a function argument (`ARRAY(SELECT …)`), or a set-op
//! branch — introduces an additional `Query` node that the derived `Visit`
//! walk reaches. Counting nodes therefore catches sub-queries an explicit
//! `Expr`-variant blacklist would miss (e.g. `FunctionArguments::Subquery`),
//! which is exactly the silent-staleness class this guard closes.
//!
//! This is a **whitelist**: an unparseable body, an unrecognised future
//! construct, or anything not positively matched returns `false` ⇒ the caller
//! builds. A false "incomplete" verdict costs at most a redundant rebuild; a
//! false "complete" verdict could let the gate skip on stale data.

use std::ops::ControlFlow;

use sqlparser::ast::{Query, SetExpr, Statement, TableFactor, Visit, Visitor};

use crate::parser::parse_single_statement;

/// Returns `true` iff the plain FROM/JOIN table walk is *guaranteed* to
/// enumerate every upstream source of `sql` — i.e. `sql` is a single plain
/// `SELECT` over bare tables with no CTEs and no sub-queries anywhere.
///
/// Fail-safe: an un-parseable body, a non-`SELECT` statement, a set operation,
/// a CTE, a non-bare table-factor, or any embedded sub-query returns `false`.
/// The caller must treat `false` as "lineage may be incomplete — do not trust
/// the enumerated source set", not merely "is complex".
#[must_use]
pub fn lineage_is_provably_complete(sql: &str) -> bool {
    let Ok(statement) = parse_single_statement(sql) else {
        return false;
    };
    let Statement::Query(query) = &statement else {
        // Only SELECT/query bodies are model SQL; anything else is out of
        // scope and treated conservatively.
        return false;
    };

    // Top-level structure: a plain SELECT body (reject set operations, which do
    // not add a nested Query node and so are invisible to the count) and no
    // CTEs.
    if !matches!(query.body.as_ref(), SetExpr::Select(_)) {
        return false;
    }
    if query.with.is_some() {
        return false;
    }

    // Structural walk: exactly one Query node (the root — no sub-queries of any
    // kind), and every table-factor a bare `Table`.
    let mut visitor = CompletenessVisitor {
        query_count: 0,
        complex_factor: false,
    };
    let _: ControlFlow<()> = statement.visit(&mut visitor);
    visitor.query_count == 1 && !visitor.complex_factor
}

/// Counts `Query` nodes (must be exactly one) and trips `complex_factor` on the
/// first table-factor that is not a bare [`TableFactor::Table`].
struct CompletenessVisitor {
    query_count: usize,
    complex_factor: bool,
}

impl Visitor for CompletenessVisitor {
    type Break = ();

    fn pre_visit_query(&mut self, _query: &Query) -> ControlFlow<()> {
        self.query_count += 1;
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, table_factor: &TableFactor) -> ControlFlow<()> {
        // Whitelist: only a bare table reference is enumerable by the plain
        // FROM/JOIN walk. Everything else (Derived, Pivot, Unpivot, Unnest,
        // Function/LATERAL FLATTEN, TableFunction, JsonTable, NestedJoin, and
        // any future variant) hides or omits its sources.
        //
        // A table-valued-function call (`UNNEST(...)`, `generate_series(...)`,
        // a LATERAL `FLATTEN(...)`) parses under the engine dialect as a
        // `TableFactor::Table` whose `args` is `Some(...)` — its real inputs
        // live in the function arguments, not in the table name, so the plain
        // walk records only the bare function name. Require `args.is_none()`
        // to reject these while still accepting a regular table reference.
        let bare_table = matches!(table_factor, TableFactor::Table { args: None, .. });
        if !bare_table {
            self.complex_factor = true;
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_select_from_bare_table_is_complete() {
        assert!(lineage_is_provably_complete("SELECT id FROM main.src"));
        assert!(lineage_is_provably_complete(
            "SELECT id, name FROM cat.sch.t WHERE id > 10"
        ));
    }

    #[test]
    fn bare_table_joins_are_complete() {
        // Every relation is a bare table — `extract_tables` walks the relation
        // plus every join, so a multi-table join is fully enumerable and must
        // stay eligible (otherwise the feature is inert for joined models).
        assert!(lineage_is_provably_complete(
            "SELECT o.id, c.name FROM cat.sch.orders o \
             JOIN cat.sch.customers c ON o.cid = c.id"
        ));
        assert!(lineage_is_provably_complete(
            "SELECT a.id FROM s.a a \
             JOIN s.b b ON a.id = b.id \
             LEFT JOIN s.c c ON b.id = c.id"
        ));
    }

    #[test]
    fn aggregation_over_bare_table_is_complete() {
        assert!(lineage_is_provably_complete(
            "SELECT customer_id, SUM(amount) AS total \
             FROM cat.sch.orders GROUP BY customer_id HAVING SUM(amount) > 0"
        ));
    }

    #[test]
    fn subquery_in_from_is_incomplete() {
        // Derived table — the lineage walk records only `(subquery)`.
        assert!(!lineage_is_provably_complete(
            "SELECT id FROM (SELECT id FROM main.src) t"
        ));
    }

    #[test]
    fn nested_join_in_from_is_incomplete() {
        // Parenthesised join parses as TableFactor::NestedJoin, dropped by the
        // lineage walk's `_ => {}` arm.
        assert!(!lineage_is_provably_complete(
            "SELECT a.id FROM (cat.sch.a a JOIN cat.sch.b b ON a.id = b.id)"
        ));
    }

    #[test]
    fn pivot_source_is_incomplete() {
        assert!(!lineage_is_provably_complete(
            "SELECT * FROM main.monthly_sales \
             PIVOT(SUM(amount) FOR month IN ('jan', 'feb'))"
        ));
    }

    #[test]
    fn unnest_in_from_is_incomplete() {
        // `UNNEST(...)` parses as a `TableFactor::Table` with `args = Some(...)`
        // (a table-valued function), so the plain walk would record only the
        // bare "UNNEST" name and miss the real input column.
        assert!(!lineage_is_provably_complete(
            "SELECT x FROM UNNEST(ARRAY[1, 2, 3]) AS t(x)"
        ));
        // The realistic lateral-over-a-source form: the source `main.events`
        // IS enumerable, but the UNNEST table function still disqualifies the
        // whole model (fail-safe over partial enumeration).
        assert!(!lineage_is_provably_complete(
            "SELECT id, e FROM main.events CROSS JOIN UNNEST(items) AS t(e)"
        ));
    }

    #[test]
    fn table_valued_function_is_incomplete() {
        // Any table-valued function in FROM (`generate_series`, a UDF) parses
        // as `Table { args: Some(...) }` — reject it; its inputs aren't a
        // surfaced source.
        assert!(!lineage_is_provably_complete(
            "SELECT v FROM generate_series(1, 10) AS t(v)"
        ));
    }

    #[test]
    fn in_subquery_is_incomplete() {
        // The hidden source `facts` lives only inside the IN sub-query, which
        // the lineage walk never descends into.
        assert!(!lineage_is_provably_complete(
            "SELECT id FROM cat.sch.dim WHERE id IN (SELECT dim_id FROM cat.sch.facts)"
        ));
    }

    #[test]
    fn exists_subquery_is_incomplete() {
        assert!(!lineage_is_provably_complete(
            "SELECT id FROM cat.sch.dim d \
             WHERE EXISTS (SELECT 1 FROM cat.sch.facts f WHERE f.dim_id = d.id)"
        ));
    }

    #[test]
    fn scalar_subquery_in_projection_is_incomplete() {
        assert!(!lineage_is_provably_complete(
            "SELECT id, (SELECT MAX(v) FROM cat.sch.facts) AS m FROM cat.sch.dim"
        ));
    }

    #[test]
    fn subquery_in_function_argument_is_incomplete() {
        // `FunctionArguments::Subquery` is NOT an `Expr::Subquery`/`InSubquery`/
        // `Exists`, so an Expr-variant blacklist would miss it — but it adds a
        // Query node, so the structural count rejects it.
        assert!(!lineage_is_provably_complete(
            "SELECT ARRAY(SELECT v FROM cat.sch.facts) AS vs FROM cat.sch.dim"
        ));
    }

    #[test]
    fn cte_is_incomplete() {
        // CTE bodies are never walked by the lineage extractor.
        assert!(!lineage_is_provably_complete(
            "WITH c AS (SELECT id FROM cat.sch.facts) SELECT id FROM c"
        ));
    }

    #[test]
    fn set_operation_is_incomplete() {
        // A top-level UNION's branches are not enumerated by the plain walk and
        // do not add a nested Query node — the body-shape check rejects them.
        assert!(!lineage_is_provably_complete(
            "SELECT id FROM cat.sch.a UNION ALL SELECT id FROM cat.sch.b"
        ));
    }

    #[test]
    fn unparseable_or_non_select_is_incomplete() {
        assert!(!lineage_is_provably_complete("SELCT FROM"));
        assert!(!lineage_is_provably_complete(""));
        assert!(!lineage_is_provably_complete("CREATE TABLE t (id INT)"));
    }
}
