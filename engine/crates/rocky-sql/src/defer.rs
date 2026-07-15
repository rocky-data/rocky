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

use sqlparser::ast::{Ident, ObjectName, ObjectNamePart, Query, VisitMut, VisitorMut};

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
    /// Optional identifier quote character to wrap each part in when the
    /// reference is re-serialized.
    ///
    /// `None` renders bare identifiers (`catalog.schema.table`) — the default
    /// for warehouses whose qualified parts always match the strict
    /// SQL-identifier rule (Databricks, Snowflake, DuckDB). Dialects that
    /// permit characters bare identifiers reject — notably BigQuery, whose
    /// project IDs allow hyphens (`my-gcp-project-123`) — set this so the
    /// rewritten reference is quoted exactly as the warehouse expects
    /// (`` `my-gcp-project-123`.`schema`.`table` ``) instead of parsing the
    /// hyphen as subtraction.
    pub quote_style: Option<char>,
}

impl DeferTarget {
    /// Build the qualified `ObjectName` for this target, applying the
    /// empty-catalog rule (`catalog.is_empty()` ⇒ `[schema, table]`, else
    /// `[catalog, schema, table]`) and the configured [`quote_style`].
    ///
    /// [`quote_style`]: DeferTarget::quote_style
    fn to_object_name(&self) -> ObjectName {
        let ident = |value: &str| -> ObjectNamePart {
            let part = match self.quote_style {
                Some(q) => Ident::with_quote(q, value.to_string()),
                None => Ident::new(value.to_string()),
            };
            ObjectNamePart::Identifier(part)
        };
        let mut parts: Vec<ObjectNamePart> = Vec::with_capacity(3);
        if !self.catalog.is_empty() {
            parts.push(ident(&self.catalog));
        }
        parts.push(ident(&self.schema));
        parts.push(ident(&self.table));
        ObjectName(parts)
    }
}

/// Rewrite bare upstream model references in `sql` to their qualified
/// [`DeferTarget`].
///
/// `deferred` maps a deferred model's bare name to the fully qualified
/// location its reference should resolve to. Only single-part relations whose
/// identifier matches a key in `deferred` — and that are not shadowed by a CTE
/// in scope at that point — are rewritten; everything else is left as parsed.
///
/// CTE shadowing is resolved with lexical scope: the rewrite walks the AST
/// maintaining a stack of the CTE names in scope (each query's `WITH` names
/// cover its own body and every CTE body nested under it), so a `WITH orders
/// AS (…)` inside a derived-table / `IN` / `UNION` subquery only shadows the
/// deferred `orders` *within that subquery*, while a genuine top-level `FROM
/// orders` with no shadowing CTE is still qualified.
///
/// Re-serialization preserves the statement's semantics, but the AST round
/// trip may strip comments and trailing semicolons and normalize incidental
/// whitespace — callers only invoke this in `--defer` mode, never on the
/// default path.
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

    // Walk the whole AST with a scope-aware visitor. `visit_relations_mut`
    // reaches every relation (including those inside derived-table / scalar /
    // `IN` subqueries and set-op branches), but it is scope-blind — so the
    // visitor tracks the CTE names in scope on a stack and only rewrites a
    // bare reference that is *not* shadowed at that point. The visitor never
    // breaks, so the `ControlFlow` is always `Continue`.
    let mut rewriter = DeferRewriter {
        deferred,
        scopes: Vec::new(),
    };
    let _: ControlFlow<()> = statement.visit(&mut rewriter);

    Ok(statement.to_string())
}

/// Outcome of [`rewrite_upstream_refs`].
#[derive(Debug)]
pub struct UpstreamRewriteOutcome {
    /// The statement re-serialized after rewriting.
    pub sql: String,
    /// The rename keys (lowercase `catalog.schema.table`) that matched at
    /// least one relation and were rewritten. Callers that require *every*
    /// upstream reference to be redirected compare this against the rename
    /// key set and fail closed on any miss.
    pub rewritten_keys: HashSet<String>,
    /// Display strings of relations that matched **more than one** rename
    /// key (a partially-qualified reference whose tail is shared by several
    /// upstreams). Ambiguous references are left untouched — callers must
    /// fail closed rather than guess.
    pub ambiguous_refs: Vec<String>,
}

/// Rewrite references to known upstream tables — bare, `schema.table`, or
/// `catalog.schema.table` — to a replacement [`DeferTarget`].
///
/// `renames` is keyed by the upstream's lowercase fully-qualified
/// `catalog.schema.table` identity. A relation matches a key when every part
/// it *does* spell agrees case-insensitively with the key's tail: a three-part
/// reference must match catalog, schema, and table; a two-part reference
/// schema and table; a bare name just the table (subject to CTE shadowing,
/// exactly as [`qualify_deferred_refs`] resolves it). A relation whose tail
/// matches several keys is ambiguous: it is left as parsed and reported in
/// [`UpstreamRewriteOutcome::ambiguous_refs`] so the caller can refuse the
/// rewrite instead of redirecting a reference to the wrong upstream.
///
/// # Errors
///
/// Returns [`ParseError`] when `sql` is not a single parseable statement.
pub fn rewrite_upstream_refs(
    sql: &str,
    renames: &HashMap<String, DeferTarget>,
) -> Result<UpstreamRewriteOutcome, ParseError> {
    let mut statement = parse_single_statement(sql)?;

    // Pre-split each rename key into (catalog, schema, table) parts once.
    let split: Vec<(Vec<String>, &String, &DeferTarget)> = renames
        .iter()
        .map(|(key, target)| {
            let parts: Vec<String> = key.split('.').map(str::to_ascii_lowercase).collect();
            (parts, key, target)
        })
        .collect();

    let mut rewriter = UpstreamRewriter {
        renames: &split,
        scopes: Vec::new(),
        rewritten_keys: HashSet::new(),
        ambiguous_refs: Vec::new(),
    };
    let _: ControlFlow<()> = statement.visit(&mut rewriter);

    let UpstreamRewriter {
        rewritten_keys,
        ambiguous_refs,
        ..
    } = rewriter;
    Ok(UpstreamRewriteOutcome {
        sql: statement.to_string(),
        rewritten_keys,
        ambiguous_refs,
    })
}

/// Scope-aware visitor behind [`rewrite_upstream_refs`]. Shares the CTE
/// shadowing discipline with [`DeferRewriter`]: a bare name shadowed by a CTE
/// in scope is a reference to that CTE, never to the upstream.
struct UpstreamRewriter<'a> {
    /// `(key parts, original key, replacement)` triples, key parts lowercase.
    renames: &'a [(Vec<String>, &'a String, &'a DeferTarget)],
    scopes: Vec<HashSet<String>>,
    rewritten_keys: HashSet<String>,
    ambiguous_refs: Vec<String>,
}

impl UpstreamRewriter<'_> {
    fn is_shadowed(&self, name: &str) -> bool {
        self.scopes.iter().any(|frame| frame.contains(name))
    }
}

impl VisitorMut for UpstreamRewriter<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        let mut frame: HashSet<String> = HashSet::new();
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                frame.insert(cte.alias.name.value.clone());
            }
        }
        self.scopes.push(frame);
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &mut Query) -> ControlFlow<Self::Break> {
        self.scopes.pop();
        ControlFlow::Continue(())
    }

    fn pre_visit_relation(&mut self, relation: &mut ObjectName) -> ControlFlow<Self::Break> {
        // Extract the spelled parts — bail on anything that is not a plain
        // identifier chain (e.g. a table function).
        let mut original: Vec<&str> = Vec::with_capacity(relation.0.len());
        for part in &relation.0 {
            let Some(ident) = part.as_ident() else {
                return ControlFlow::Continue(());
            };
            original.push(ident.value.as_str());
        }
        if original.is_empty() || original.len() > 3 {
            return ControlFlow::Continue(());
        }
        // A bare name shadowed by a CTE in scope refers to the CTE.
        if original.len() == 1 && self.is_shadowed(original[0]) {
            return ControlFlow::Continue(());
        }
        let spelled: Vec<String> = original.iter().map(|p| p.to_ascii_lowercase()).collect();
        // Tail-match the spelled parts against each rename key.
        let matches: Vec<&(Vec<String>, &String, &DeferTarget)> = self
            .renames
            .iter()
            .filter(|(key_parts, _, _)| {
                key_parts.len() >= spelled.len()
                    && key_parts[key_parts.len() - spelled.len()..] == spelled[..]
            })
            .collect();
        match matches.as_slice() {
            [] => {}
            [(_, key, target)] => {
                *relation = target.to_object_name();
                self.rewritten_keys.insert((*key).clone());
            }
            _ => self.ambiguous_refs.push(relation.to_string()),
        }
        ControlFlow::Continue(())
    }
}

/// Scope-aware visitor that qualifies bare deferred-model references while
/// honouring lexical CTE shadowing.
///
/// `scopes` is a stack of the CTE names introduced by each enclosing query's
/// `WITH` clause. A query's frame is pushed in `pre_visit_query` (before its
/// body *and* its CTE bodies are walked) and popped in `post_visit_query`, so
/// a name on the stack shadows the deferred model for exactly the subtree
/// where that `WITH` is in scope.
struct DeferRewriter<'a> {
    deferred: &'a HashMap<String, DeferTarget>,
    scopes: Vec<HashSet<String>>,
}

impl DeferRewriter<'_> {
    /// Whether `name` is shadowed by a CTE in any enclosing scope.
    fn is_shadowed(&self, name: &str) -> bool {
        self.scopes.iter().any(|frame| frame.contains(name))
    }
}

impl VisitorMut for DeferRewriter<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        let mut frame: HashSet<String> = HashSet::new();
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                frame.insert(cte.alias.name.value.clone());
            }
        }
        self.scopes.push(frame);
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &mut Query) -> ControlFlow<Self::Break> {
        self.scopes.pop();
        ControlFlow::Continue(())
    }

    fn pre_visit_relation(&mut self, relation: &mut ObjectName) -> ControlFlow<Self::Break> {
        // Only single-part bare names are candidates; anything already
        // qualified (`schema.table`, `catalog.schema.table`) is left alone.
        if relation.0.len() != 1 {
            return ControlFlow::Continue(());
        }
        let Some(ident) = relation.0[0].as_ident() else {
            return ControlFlow::Continue(());
        };
        let name = ident.value.clone();
        if self.is_shadowed(&name) {
            return ControlFlow::Continue(());
        }
        if let Some(target) = self.deferred.get(&name) {
            *relation = target.to_object_name();
        }
        ControlFlow::Continue(())
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
            quote_style: None,
        }
    }

    fn quoted_target(catalog: &str, schema: &str, table: &str, quote: char) -> DeferTarget {
        DeferTarget {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
            quote_style: Some(quote),
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
    fn does_not_rewrite_cte_shadow_inside_derived_table_subquery() {
        // Case A: the shadowing `WITH orders` lives inside a derived-table
        // subquery (`FROM (... ) sub`). Its inner `FROM orders` must read the
        // CTE, not the deferred production table — the scope-aware walk must
        // not flatten the nested CTE name into a global shadow set either, but
        // here we only assert the inner ref stays unqualified.
        let deferred = deferred_map(&[("orders", target("", "prod", "orders"))]);
        let out = qualify_deferred_refs(
            "SELECT * FROM (WITH orders AS (SELECT 1 AS id) SELECT * FROM orders) sub",
            &deferred,
        )
        .unwrap();
        assert!(
            !out.contains("prod.orders"),
            "CTE inside a derived-table subquery must shadow the deferred ref: {out}"
        );
    }

    #[test]
    fn does_not_rewrite_cte_shadow_inside_in_subquery() {
        // Case B: the shadowing `WITH orders` lives inside an `IN (...)`
        // scalar subquery — an `Expr`, not a `TableFactor`. The whole-AST
        // visitor still reaches it, and the scope stack keeps the inner
        // `FROM orders` reading the CTE.
        let deferred = deferred_map(&[("orders", target("", "prod", "orders"))]);
        let out = qualify_deferred_refs(
            "SELECT id FROM events WHERE id IN \
             (WITH orders AS (SELECT 1 AS id) SELECT id FROM orders)",
            &deferred,
        )
        .unwrap();
        assert!(
            !out.contains("prod.orders"),
            "CTE inside an IN-subquery must shadow the deferred ref: {out}"
        );
    }

    #[test]
    fn does_not_rewrite_cte_shadow_inside_union_branch() {
        // Case C: the shadowing `WITH orders` lives inside a set-op (UNION)
        // branch. The visitor descends into both branches; the inner branch's
        // CTE shadows its own `FROM orders`.
        let deferred = deferred_map(&[("orders", target("", "prod", "orders"))]);
        let out = qualify_deferred_refs(
            "SELECT 1 AS id UNION ALL \
             (WITH orders AS (SELECT 2 AS id) SELECT id FROM orders)",
            &deferred,
        )
        .unwrap();
        assert!(
            !out.contains("prod.orders"),
            "CTE inside a UNION branch must shadow the deferred ref: {out}"
        );
    }

    #[test]
    fn qualifies_top_level_ref_with_no_shadowing_cte() {
        // Case D (positive control): a genuine top-level `FROM orders` with no
        // shadowing CTE anywhere is still qualified.
        let deferred = deferred_map(&[("orders", target("", "prod", "orders"))]);
        let out = qualify_deferred_refs("SELECT * FROM orders", &deferred).unwrap();
        assert!(
            out.contains("prod.orders"),
            "top-level deferred ref with no shadow must be qualified: {out}"
        );
    }

    #[test]
    fn qualifies_outer_ref_but_not_nested_cte_shadow() {
        // Case E (the discriminating case): a flat global shadow set would
        // wrongly skip the top-level `FROM orders`, and a scope-blind walk
        // would wrongly qualify the inner one. Only a scope-aware stack gets
        // both: the top-level ref IS qualified, the nested CTE-shadowed ref is
        // NOT.
        let deferred = deferred_map(&[("orders", target("", "prod", "orders"))]);
        let out = qualify_deferred_refs(
            "SELECT * FROM orders WHERE id IN \
             (WITH orders AS (SELECT 1 AS id) SELECT id FROM orders)",
            &deferred,
        )
        .unwrap();
        // Exactly one occurrence of the qualified production ref (the outer
        // FROM); the inner CTE ref stays bare.
        assert_eq!(
            out.matches("prod.orders").count(),
            1,
            "outer ref qualified, nested CTE-shadowed ref left bare: {out}"
        );
        assert!(
            out.contains("FROM prod.orders WHERE"),
            "the outer FROM must be the qualified one: {out}"
        );
    }

    #[test]
    fn quotes_hyphenated_catalog_for_bigquery_style_target() {
        // A BigQuery-style defer target: the project (catalog) contains
        // hyphens, so the rewritten reference must be backtick-quoted to match
        // what the BigQuery dialect emits — bare hyphens would parse as
        // subtraction and the warehouse would reject the SQL.
        let deferred = deferred_map(&[(
            "orders",
            quoted_target("my-gcp-project-123", "prod", "orders", '`'),
        )]);
        let out = qualify_deferred_refs("SELECT * FROM orders", &deferred).unwrap();
        assert_eq!(out, "SELECT * FROM `my-gcp-project-123`.`prod`.`orders`");
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

    // -- rewrite_upstream_refs ---------------------------------------------

    fn renames_map(entries: &[(&str, DeferTarget)]) -> HashMap<String, DeferTarget> {
        entries
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn rewrites_fully_qualified_ref() {
        let renames = renames_map(&[("cat.raw.orders", target("cat", "replay_ns", "raw__orders"))]);
        let out = rewrite_upstream_refs("SELECT * FROM cat.raw.orders", &renames).unwrap();
        assert_eq!(out.sql, "SELECT * FROM cat.replay_ns.raw__orders");
        assert!(out.rewritten_keys.contains("cat.raw.orders"));
        assert!(out.ambiguous_refs.is_empty());
    }

    #[test]
    fn rewrites_two_part_and_bare_tails() {
        let renames = renames_map(&[("cat.raw.orders", target("cat", "replay_ns", "raw__orders"))]);
        let two = rewrite_upstream_refs("SELECT * FROM raw.orders", &renames).unwrap();
        assert_eq!(two.sql, "SELECT * FROM cat.replay_ns.raw__orders");
        let bare = rewrite_upstream_refs("SELECT * FROM orders", &renames).unwrap();
        assert_eq!(bare.sql, "SELECT * FROM cat.replay_ns.raw__orders");
        assert!(bare.rewritten_keys.contains("cat.raw.orders"));
    }

    #[test]
    fn matches_case_insensitively() {
        let renames = renames_map(&[("cat.raw.orders", target("cat", "replay_ns", "raw__orders"))]);
        let out = rewrite_upstream_refs("SELECT * FROM CAT.Raw.ORDERS", &renames).unwrap();
        assert_eq!(out.sql, "SELECT * FROM cat.replay_ns.raw__orders");
    }

    #[test]
    fn ambiguous_tail_is_reported_not_rewritten() {
        // Two upstreams share the `orders` table segment: a bare reference
        // cannot be attributed to either — it must be reported, not guessed.
        let renames = renames_map(&[
            ("cat.raw.orders", target("cat", "replay_ns", "raw__orders")),
            (
                "cat.staging.orders",
                target("cat", "replay_ns", "staging__orders"),
            ),
        ]);
        let out = rewrite_upstream_refs("SELECT * FROM orders", &renames).unwrap();
        assert_eq!(out.sql, "SELECT * FROM orders", "ambiguous ref untouched");
        assert!(out.rewritten_keys.is_empty());
        assert_eq!(out.ambiguous_refs, vec!["orders".to_string()]);
        // A fully qualified reference disambiguates.
        let fq = rewrite_upstream_refs("SELECT * FROM cat.staging.orders", &renames).unwrap();
        assert_eq!(fq.sql, "SELECT * FROM cat.replay_ns.staging__orders");
        assert!(fq.ambiguous_refs.is_empty());
    }

    #[test]
    fn cte_shadowing_protects_bare_names_only_in_scope() {
        let renames = renames_map(&[("cat.raw.orders", target("cat", "replay_ns", "raw__orders"))]);
        let out = rewrite_upstream_refs(
            "WITH orders AS (SELECT 1 AS id) SELECT * FROM orders",
            &renames,
        )
        .unwrap();
        assert!(
            out.sql.contains("SELECT * FROM orders"),
            "CTE-shadowed bare name untouched: {}",
            out.sql
        );
        assert!(out.rewritten_keys.is_empty());
    }

    #[test]
    fn unmatched_key_reported_via_rewritten_keys_absence() {
        let renames = renames_map(&[("cat.raw.orders", target("cat", "replay_ns", "raw__orders"))]);
        let out = rewrite_upstream_refs("SELECT * FROM cat.raw.customers", &renames).unwrap();
        assert!(out.rewritten_keys.is_empty());
        assert_eq!(out.sql, "SELECT * FROM cat.raw.customers");
    }

    #[test]
    fn non_matching_relations_are_left_alone() {
        let renames = renames_map(&[("cat.raw.orders", target("cat", "replay_ns", "raw__orders"))]);
        let out = rewrite_upstream_refs(
            "SELECT o.id FROM cat.raw.orders o JOIN other.schema.customers c ON o.id = c.id",
            &renames,
        )
        .unwrap();
        assert!(
            out.sql.contains("cat.replay_ns.raw__orders o"),
            "{}",
            out.sql
        );
        assert!(out.sql.contains("other.schema.customers c"), "{}", out.sql);
    }
}
