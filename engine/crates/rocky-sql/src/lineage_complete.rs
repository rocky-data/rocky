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
//! `extract_lineage`'s `source_tables` holds only the top-level `FROM`/`JOIN`
//! relations, recording non-`Table` table-factors as either the opaque
//! `(subquery)` marker (for a derived table) or nothing at all (Pivot, Unpivot,
//! Unnest, table functions, JSON tables, nested joins — its `_ => {}` arm).
//!
//! Since #1867 it does descend into derived tables and `WITH` bodies, carrying
//! what it finds on `LineageResult::nested_sources`. That is a **widening, not
//! completeness**, and this gate must not be relaxed for it. Expression
//! sub-queries (`WHERE … IN (SELECT …)`, scalar sub-queries) are now read too.
//! Still not reached: a sub-query inside a table function or other table
//! factor the extractor does not read, and any body that is a set operation (`extract_query_lineage`
//! returns `Err` for `SetExpr::SetOperation`, so a `UNION` CTE body is skipped
//! entirely). Partial coverage that reads as complete is exactly the silent
//! staleness this gate exists to prevent.
//!
//! So for anything but a plain single `SELECT` over bare tables, the source set
//! can be **incomplete** — and an incomplete-but-non-empty (or empty) source set
//! would let the gate skip a model whose true upstream actually moved. That is
//! silent production staleness, the worst failure the gate exists to prevent.
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

/// Every table `sql` reads, for failure containment only, or `None` when the
/// read set cannot be listed (#2737).
///
/// Wider than [`lineage_is_provably_complete`], which the skip gate, backfill
/// and the defer edges keep. The walk visits every table factor in the
/// statement: the `FROM` and every `JOIN`, `WITH` bodies, derived tables,
/// parenthesised joins, both branches of every set operation, and every
/// expression sub-query. Each bare table (`TableFactor::Table` with no
/// arguments) is a read, lowercased as the containment caller reads
/// `source_tables`.
///
/// A one-part, unquoted name that a `WITH` clause in scope defines is a
/// reference to that CTE, not a read. The scope follows SQL: a CTE body sees
/// the CTEs declared before it (and itself under `WITH RECURSIVE`); the query
/// body, its sub-queries and its derived tables see every CTE of the clause.
/// A quoted CTE name, or a quoted reference, is never taken as the CTE, so
/// the name stays a read. Any doubt keeps the read: an extra read can only
/// withhold a model, a missing one could let it build on stale output.
///
/// `None` (fail closed) when:
///
/// - the SQL does not parse, or is not a query;
/// - any table factor is not a bare table, a derived table or a
///   parenthesised join (a table function, `PIVOT`, `UNPIVOT`, `UNNEST`, a
///   JSON table, any future variant);
/// - a table name has a part that is not a plain identifier
///   (`IDENTIFIER('t')`);
/// - any query body is `TABLE t` or a DML form.
#[must_use]
pub fn containment_read_set(sql: &str) -> Option<Vec<String>> {
    let Ok(statement) = parse_single_statement(sql) else {
        return None;
    };
    if !matches!(statement, Statement::Query(_)) {
        return None;
    }
    let mut visitor = ReadSetVisitor {
        frames: Vec::new(),
        reads: Vec::new(),
        unlistable: false,
    };
    let _: ControlFlow<()> = statement.visit(&mut visitor);
    if visitor.unlistable {
        return None;
    }
    let mut reads = visitor.reads;
    reads.sort();
    reads.dedup();
    Some(reads)
}

/// One `Query` node on the walk: the CTE names its table factors see, and
/// its own `WITH` clause, to scope its children.
struct ReadSetFrame {
    /// Lowercased unquoted CTE names visible to this query's own body.
    scope: Vec<String>,
    /// This query's CTE bodies, by address, with the names each body sees.
    cte_scopes: Vec<(*const Query, Vec<String>)>,
}

struct ReadSetVisitor {
    frames: Vec<ReadSetFrame>,
    reads: Vec<String>,
    unlistable: bool,
}

impl ReadSetVisitor {
    fn refuse(&mut self) -> ControlFlow<()> {
        self.unlistable = true;
        ControlFlow::Break(())
    }
}

/// Whether a query body, through its set operations, holds only forms the
/// table-factor walk lists: `SELECT`, `VALUES` and nested queries.
fn body_is_listable(body: &SetExpr) -> bool {
    match body {
        SetExpr::Select(_) | SetExpr::Values(_) | SetExpr::Query(_) => true,
        SetExpr::SetOperation { left, right, .. } => {
            body_is_listable(left) && body_is_listable(right)
        }
        _ => false,
    }
}

/// A CTE name that a reference can bind to: lowercased, unquoted only.
fn cte_name(cte: &sqlparser::ast::Cte) -> Option<String> {
    let ident = &cte.alias.name;
    ident
        .quote_style
        .is_none()
        .then(|| ident.value.to_lowercase())
}

impl Visitor for ReadSetVisitor {
    type Break = ();

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<()> {
        if !body_is_listable(&query.body) {
            return self.refuse();
        }
        // The names this query sees from outside: a CTE body of the parent
        // sees the parent's earlier CTEs; anything else under the parent
        // sees all of them.
        let address: *const Query = query;
        let outer = match self.frames.last() {
            None => Vec::new(),
            Some(parent) => parent
                .cte_scopes
                .iter()
                .find(|(body, _)| *body == address)
                .map_or_else(|| parent.scope.clone(), |(_, seen)| seen.clone()),
        };
        let mut scope = outer.clone();
        let mut cte_scopes = Vec::new();
        if let Some(with) = &query.with {
            let mut declared = outer;
            for cte in &with.cte_tables {
                let name = cte_name(cte);
                let mut seen = declared.clone();
                if with.recursive {
                    seen.extend(name.clone());
                }
                cte_scopes.push((std::ptr::from_ref(cte.query.as_ref()), seen));
                declared.extend(name);
            }
            scope = declared;
        }
        self.frames.push(ReadSetFrame { scope, cte_scopes });
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &Query) -> ControlFlow<()> {
        self.frames.pop();
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, table_factor: &TableFactor) -> ControlFlow<()> {
        match table_factor {
            TableFactor::Table {
                name, args: None, ..
            } => {
                let mut parts = Vec::with_capacity(name.0.len());
                for part in &name.0 {
                    let Some(ident) = part.as_ident() else {
                        return self.refuse();
                    };
                    parts.push(ident);
                }
                let cte_reference = match parts.as_slice() {
                    [only] if only.quote_style.is_none() => {
                        let folded = only.value.to_lowercase();
                        self.frames
                            .last()
                            .is_some_and(|frame| frame.scope.contains(&folded))
                    }
                    _ => false,
                };
                if !cte_reference {
                    self.reads.push(name.to_string().to_lowercase());
                }
                ControlFlow::Continue(())
            }
            TableFactor::Derived { .. } | TableFactor::NestedJoin { .. } => {
                ControlFlow::Continue(())
            }
            _ => self.refuse(),
        }
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

    fn reads(sql: &str) -> Option<Vec<String>> {
        containment_read_set(sql)
    }

    fn list(names: &[&str]) -> Option<Vec<String>> {
        Some(names.iter().map(|n| (*n).to_string()).collect())
    }

    /// #2737: the containment read set lists the reads inside CTE bodies,
    /// derived tables, set-operation branches and expression sub-queries.
    #[test]
    fn the_read_set_lists_every_nested_read() {
        assert_eq!(reads("SELECT id FROM main.src"), list(&["main.src"]));
        assert_eq!(
            reads("WITH t AS (SELECT id FROM raw.y) SELECT id FROM t"),
            list(&["raw.y"])
        );
        assert_eq!(
            reads("SELECT id FROM (SELECT id FROM raw.y) AS d JOIN raw.z ON true"),
            list(&["raw.y", "raw.z"])
        );
        assert_eq!(
            reads("SELECT id FROM (SELECT id FROM raw.y)"),
            list(&["raw.y"])
        );
        assert_eq!(
            reads("SELECT id FROM raw.a UNION ALL SELECT id FROM raw.b"),
            list(&["raw.a", "raw.b"])
        );
        assert_eq!(
            reads("SELECT id FROM raw.a WHERE id IN (SELECT id FROM raw.b)"),
            list(&["raw.a", "raw.b"])
        );
        assert_eq!(
            reads(
                "SELECT (SELECT max(id) FROM raw.b) AS m FROM raw.a ORDER BY (SELECT 1 FROM raw.c)"
            ),
            list(&["raw.a", "raw.b", "raw.c"])
        );
        assert_eq!(
            reads("SELECT * FROM (raw.a JOIN raw.b ON true)"),
            list(&["raw.a", "raw.b"])
        );
        assert_eq!(reads("SELECT 1"), list(&[]));
        assert_eq!(
            reads("SELECT id FROM Main.SRC"),
            list(&["main.src"]),
            "lowercased, as the containment caller reads `source_tables`"
        );
    }

    /// A CTE name is a CTE only where SQL scopes it: a body sees the CTEs
    /// before it, not itself (unless recursive); a quoted name is kept as a
    /// read. A CTE that shadows a produced table never hides a real read.
    #[test]
    fn a_cte_name_hides_a_read_only_in_its_scope() {
        // The CTE shadows `orders`; the model reads only the CTE.
        assert_eq!(
            reads("WITH orders AS (SELECT 1 AS id) SELECT id FROM orders"),
            list(&[])
        );
        assert_eq!(
            reads("WITH Orders AS (SELECT 1 AS id) SELECT id FROM ORDERS"),
            list(&[])
        );
        // A body reading its own name reads the table.
        assert_eq!(
            reads("WITH orders AS (SELECT id FROM orders) SELECT id FROM orders"),
            list(&["orders"])
        );
        // A body reading a later CTE's name reads the table.
        assert_eq!(
            reads("WITH a AS (SELECT id FROM b), b AS (SELECT 1 AS id) SELECT id FROM a"),
            list(&["b"])
        );
        // An earlier CTE is the CTE.
        assert_eq!(
            reads("WITH b AS (SELECT id FROM raw.x), a AS (SELECT id FROM b) SELECT id FROM a"),
            list(&["raw.x"])
        );
        // Recursive: the body's own name is the CTE.
        assert_eq!(
            reads(
                "WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE n < 3) \
                 SELECT n FROM r"
            ),
            list(&[])
        );
        // A qualified read is never the CTE.
        assert_eq!(
            reads("WITH orders AS (SELECT 1 AS id) SELECT id FROM main.orders"),
            list(&["main.orders"])
        );
        // Quoted on either side: kept as a read.
        assert_eq!(
            reads("WITH \"orders\" AS (SELECT 1 AS id) SELECT id FROM orders"),
            list(&["orders"])
        );
        assert_eq!(
            reads("WITH orders AS (SELECT 1 AS id) SELECT id FROM \"orders\""),
            list(&["\"orders\""])
        );
        // A CTE of a sub-query is not in scope outside it.
        assert_eq!(
            reads("SELECT id FROM (WITH t AS (SELECT 1 AS id) SELECT id FROM t) AS d, t"),
            list(&["t"])
        );
        // The CTE is in scope inside a derived table and a sub-query.
        assert_eq!(
            reads(
                "WITH t AS (SELECT id FROM raw.x) SELECT id FROM (SELECT id FROM t) AS d \
                 WHERE id IN (SELECT id FROM t)"
            ),
            list(&["raw.x"])
        );
    }

    /// Anything the walk cannot list fails closed.
    #[test]
    fn an_unlistable_read_set_is_none() {
        for sql in [
            "SELCT FROM",
            "",
            "CREATE TABLE t (id INT)",
            "SELECT * FROM generate_series(1, 3)",
            "SELECT * FROM read_parquet('x.parquet')",
            "SELECT * FROM raw.a PIVOT (sum(v) FOR k IN ('x'))",
            "SELECT * FROM UNNEST([1, 2])",
            "SELECT id FROM raw.a UNION ALL TABLE raw.b",
            "TABLE raw.b",
            "WITH t AS (SELECT * FROM generate_series(1, 3)) SELECT * FROM t",
            "SELECT id FROM raw.a WHERE id IN (SELECT * FROM generate_series(1, 3))",
        ] {
            assert_eq!(reads(sql), None, "{sql}");
        }
    }
}
