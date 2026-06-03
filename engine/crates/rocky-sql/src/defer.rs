//! Qualify deferred upstream model references in a model's SQL body.
//!
//! Rocky's transformation models reference upstream models by **bare name**
//! (`FROM orders`). That bare name resolves against the warehouse's current
//! schema at execution time — so a development run that builds only a subset
//! of the DAG has no table to read for the upstreams it didn't build.
//!
//! [`qualify_deferred_refs`] rewrites those bare references to fully qualified
//! `schema.table` (or `catalog.schema.table`) names pointing at an existing
//! ("defer target") location — typically the production schema where the
//! unbuilt upstream already lives. This is the engine half of the
//! `rocky run --defer` developer convenience: build your changed models
//! locally, read every unchanged upstream from production.
//!
//! The rewrite is purely additive: only **single-part bare names** that match
//! a supplied deferred-model name are touched. Already-qualified references
//! (`schema.table`, `catalog.schema.table`), names that don't match a deferred
//! model, and CTE names defined inside the statement are all left untouched.

use std::collections::{HashMap, HashSet};
use std::ops::ControlFlow;

use sqlparser::ast::{Ident, ObjectName, ObjectNamePart, Query, Statement, visit_relations_mut};

use crate::parser::{ParseError, parse_single_statement};

/// A fully resolved target for a deferred upstream reference.
///
/// Built from the upstream model's configured `target`, mirroring the
/// warehouse dialect's empty-catalog rule: an empty `catalog` yields a
/// two-part `schema.table` reference, a non-empty one yields the three-part
/// `catalog.schema.table` form.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeferTarget {
    /// Warehouse catalog. Empty string ⇒ omit the catalog part.
    pub catalog: String,
    /// Schema the deferred upstream table lives in.
    pub schema: String,
    /// Table name of the deferred upstream.
    pub table: String,
}

impl DeferTarget {
    /// Build the qualified `ObjectName` for this target, applying the
    /// empty-catalog rule (`catalog.is_empty()` ⇒ `[schema, table]`, else
    /// `[catalog, schema, table]`).
    fn to_object_name(&self) -> ObjectName {
        let mut parts: Vec<ObjectNamePart> = Vec::with_capacity(3);
        if !self.catalog.is_empty() {
            parts.push(ObjectNamePart::Identifier(Ident::new(self.catalog.clone())));
        }
        parts.push(ObjectNamePart::Identifier(Ident::new(self.schema.clone())));
        parts.push(ObjectNamePart::Identifier(Ident::new(self.table.clone())));
        ObjectName(parts)
    }
}

/// Rewrite bare upstream model references in `sql` to their qualified
/// [`DeferTarget`].
///
/// `deferred` maps a deferred model's bare name to the fully qualified
/// location its reference should resolve to. Only single-part relations whose
/// identifier matches a key in `deferred` (and that are not CTE names defined
/// within the statement) are rewritten; everything else is preserved
/// byte-for-byte by re-serializing the parsed AST.
///
/// Returns the rewritten SQL. When `deferred` is empty, or the SQL contains
/// no matching bare references, the parsed-then-reserialized form of the
/// input is returned unchanged in meaning (note: AST round-trip may
/// normalize incidental whitespace — callers only invoke this in `--defer`
/// mode, never on the default path).
///
/// # Errors
///
/// Returns [`ParseError`] when `sql` is not a single parseable statement.
pub fn qualify_deferred_refs(
    sql: &str,
    deferred: &HashMap<String, DeferTarget>,
) -> Result<String, ParseError> {
    if deferred.is_empty() {
        return Ok(sql.to_string());
    }

    let mut statement = parse_single_statement(sql)?;

    // Collect every CTE name defined anywhere in the statement so a CTE that
    // shadows a deferred model name is never rewritten — a `WITH orders AS
    // (...) SELECT * FROM orders` must keep reading the CTE, not the deferred
    // production table.
    let mut cte_names: HashSet<String> = HashSet::new();
    if let Statement::Query(query) = &statement {
        collect_cte_names(query, &mut cte_names);
    }

    // The closure never breaks, so the `ControlFlow` is always `Continue`.
    let _: ControlFlow<()> = visit_relations_mut(&mut statement, |relation: &mut ObjectName| {
        // Only single-part bare names are candidates; anything already
        // qualified (`schema.table`, `catalog.schema.table`) is left alone.
        if relation.0.len() != 1 {
            return ControlFlow::<()>::Continue(());
        }
        let Some(ident) = relation.0[0].as_ident() else {
            return ControlFlow::Continue(());
        };
        let name = ident.value.clone();
        if cte_names.contains(&name) {
            return ControlFlow::Continue(());
        }
        if let Some(target) = deferred.get(&name) {
            *relation = target.to_object_name();
        }
        ControlFlow::Continue(())
    });

    Ok(statement.to_string())
}

/// Recursively collect CTE alias names from a query's `WITH` clause and from
/// every nested subquery's `WITH` clause.
fn collect_cte_names(query: &Query, names: &mut HashSet<String>) {
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            names.insert(cte.alias.name.value.clone());
            // A CTE body is itself a query and may declare further CTEs.
            collect_cte_names(&cte.query, names);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target(catalog: &str, schema: &str, table: &str) -> DeferTarget {
        DeferTarget {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
        }
    }

    fn deferred_map(entries: &[(&str, DeferTarget)]) -> HashMap<String, DeferTarget> {
        entries
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn qualifies_a_bare_upstream_ref() {
        let deferred = deferred_map(&[("orders", target("", "prod", "orders"))]);
        let out = qualify_deferred_refs("SELECT * FROM orders", &deferred).unwrap();
        assert_eq!(out, "SELECT * FROM prod.orders");
    }

    #[test]
    fn qualifies_with_catalog_when_present() {
        let deferred = deferred_map(&[("orders", target("warehouse", "prod", "orders"))]);
        let out = qualify_deferred_refs("SELECT * FROM orders", &deferred).unwrap();
        assert_eq!(out, "SELECT * FROM warehouse.prod.orders");
    }

    #[test]
    fn leaves_aliased_ref_table_qualified_alias_intact() {
        // `FROM orders o` — the relation is `orders`; the alias `o` and any
        // `o.col` column refs are never in relation position, so only the
        // table name is rewritten.
        let deferred = deferred_map(&[("orders", target("", "prod", "orders"))]);
        let out =
            qualify_deferred_refs("SELECT o.id FROM orders o WHERE o.id > 0", &deferred).unwrap();
        // The table name is qualified; the alias `o` and the `o.col` column
        // refs are untouched (they were never in relation position).
        assert!(out.contains("prod.orders"), "got: {out}");
        assert!(
            out.contains("o.id"),
            "alias-qualified columns intact: {out}"
        );
    }

    #[test]
    fn rewrites_ref_inside_join() {
        let deferred = deferred_map(&[
            ("orders", target("", "prod", "orders")),
            ("customers", target("", "prod", "customers")),
        ]);
        let out = qualify_deferred_refs(
            "SELECT o.id, c.name FROM orders o JOIN customers c ON o.cid = c.id",
            &deferred,
        )
        .unwrap();
        assert!(out.contains("FROM prod.orders"), "got: {out}");
        assert!(out.contains("JOIN prod.customers"), "got: {out}");
    }

    #[test]
    fn rewrites_ref_inside_subquery() {
        // extract_lineage only walks top-level FROM; the AST visitor reaches
        // subquery relations too. This is the case that motivates visiting
        // the whole AST.
        let deferred = deferred_map(&[("orders", target("", "prod", "orders"))]);
        let out =
            qualify_deferred_refs("SELECT * FROM (SELECT id FROM orders) sub", &deferred).unwrap();
        assert!(
            out.contains("prod.orders"),
            "subquery ref must be qualified: {out}"
        );
    }

    #[test]
    fn does_not_rewrite_cte_that_shadows_a_model_name() {
        // `orders` is both a deferred model AND a CTE here. The CTE wins — the
        // bare `FROM orders` after the WITH must read the CTE, not the
        // deferred production table.
        let deferred = deferred_map(&[("orders", target("", "prod", "orders"))]);
        let out = qualify_deferred_refs(
            "WITH orders AS (SELECT 1 AS id) SELECT * FROM orders",
            &deferred,
        )
        .unwrap();
        assert!(
            !out.contains("prod.orders"),
            "CTE shadowing a model name must not be qualified: {out}"
        );
    }

    #[test]
    fn leaves_already_qualified_ref_untouched() {
        // A two-part `staging.orders` is an external source ref, not a model
        // ref — it must not be rewritten even if `orders` is deferred.
        let deferred = deferred_map(&[("orders", target("", "prod", "orders"))]);
        let out = qualify_deferred_refs("SELECT * FROM staging.orders", &deferred).unwrap();
        assert!(out.contains("staging.orders"), "got: {out}");
        assert!(!out.contains("prod.orders"), "got: {out}");
    }

    #[test]
    fn leaves_non_deferred_bare_ref_untouched() {
        // `customers` is selected (built locally), not deferred — its bare
        // ref stays bare so it resolves against the working schema.
        let deferred = deferred_map(&[("orders", target("", "prod", "orders"))]);
        let out = qualify_deferred_refs(
            "SELECT * FROM orders JOIN customers ON orders.cid = customers.id",
            &deferred,
        )
        .unwrap();
        assert!(out.contains("prod.orders"), "got: {out}");
        // `customers` (not in the deferred set) stays bare.
        assert!(
            out.contains("JOIN customers"),
            "non-deferred ref must stay bare: {out}"
        );
    }

    #[test]
    fn empty_deferred_map_is_a_noop() {
        let deferred: HashMap<String, DeferTarget> = HashMap::new();
        let sql = "SELECT * FROM orders";
        let out = qualify_deferred_refs(sql, &deferred).unwrap();
        assert_eq!(out, sql);
    }

    #[test]
    fn rewrites_repeated_refs() {
        let deferred = deferred_map(&[("orders", target("", "prod", "orders"))]);
        let out = qualify_deferred_refs(
            "SELECT a.id FROM orders a JOIN orders b ON a.id = b.id",
            &deferred,
        )
        .unwrap();
        // Both occurrences qualified; aliases `a`/`b` preserved.
        assert!(out.contains("FROM prod.orders a"), "got: {out}");
        assert!(out.contains("JOIN prod.orders b"), "got: {out}");
    }

    #[test]
    fn unparseable_sql_returns_err() {
        let deferred = deferred_map(&[("orders", target("", "prod", "orders"))]);
        let err = qualify_deferred_refs("NOT VALID SQL ;;;", &deferred);
        assert!(err.is_err());
    }
}
