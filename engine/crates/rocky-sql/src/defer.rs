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

/// Whether each component of a qualified name carries case as part of object
/// identity on the dialect a rewrite is being performed for.
///
/// [`rewrite_upstream_refs`] compares each component of a reference against each
/// rename key under the rule for that component: exactly where case is part of
/// identity, case-insensitively where it is not.
///
/// Comparing case-insensitively everywhere — as this matcher used to — is not
/// merely imprecise, it redirects reads to the wrong table. On a case-sensitive
/// dialect `raw.Orders` and `raw.orders` are two different objects, so a model
/// reading the first must not be rewritten to the replacement registered for the
/// second. Conversely, comparing exactly on a dialect that folds would strand
/// references that legitimately name the renamed object.
///
/// Per component rather than one boolean because a warehouse may apply different
/// rules to the catalog than to the schema and table. **No dialect Rocky
/// supports exercises that today** — duckdb, databricks and trino are uniformly
/// case-insensitive, bigquery and snowflake uniformly case-sensitive. The shape
/// is kept because the one live divergence is per-object, not per-dialect:
/// BigQuery datasets carry an `is_case_insensitive` flag that Rocky cannot
/// observe and must assume unset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IdentifierCaseRules {
    /// Case distinguishes two catalogs.
    pub catalog: bool,
    /// Case distinguishes two schemas.
    pub schema: bool,
    /// Case distinguishes two tables.
    pub table: bool,
    /// The dialect resolves an UNQUOTED identifier by upper-casing it, so its
    /// written spelling is not its identity.
    ///
    /// Quoting is a second identity axis, independent of case. Snowflake stores
    /// an unquoted `orders` as `ORDERS` while a quoted `"orders"` stays exactly
    /// as written — and Rocky renders Snowflake targets QUOTED, so a configured
    /// target `orders` is the object `orders`, which an unquoted `FROM orders`
    /// does NOT name. Ignoring this makes two texts that look identical refer to
    /// different tables.
    ///
    /// Only meaningful where `catalog`/`schema`/`table` are case-sensitive; a
    /// dialect that folds every identifier already treats the two forms alike.
    pub unquoted_uppercases: bool,
}

impl IdentifierCaseRules {
    /// Every component case-insensitive: two spellings differing only by case
    /// name one object. Callers whose rename keys are already case-folded want
    /// this — disambiguation can never separate keys that were folded together.
    #[must_use]
    pub const fn all_insensitive() -> Self {
        Self {
            catalog: false,
            schema: false,
            table: false,
            unquoted_uppercases: false,
        }
    }

    /// The same rule for every component, which is what all five in-repo
    /// dialects need.
    #[must_use]
    pub const fn uniform(case_sensitive: bool) -> Self {
        Self {
            catalog: case_sensitive,
            schema: case_sensitive,
            table: case_sensitive,
            unquoted_uppercases: false,
        }
    }

    /// [`uniform`] plus the unquoted-uppercasing rule — Snowflake's shape.
    ///
    /// [`uniform`]: IdentifierCaseRules::uniform
    #[must_use]
    pub const fn uniform_uppercasing(case_sensitive: bool) -> Self {
        Self {
            catalog: case_sensitive,
            schema: case_sensitive,
            table: case_sensitive,
            unquoted_uppercases: true,
        }
    }

    /// The rule for the component at `index` of a `catalog.schema.table` key.
    const fn at(self, index: usize) -> bool {
        match index {
            0 => self.catalog,
            1 => self.schema,
            _ => self.table,
        }
    }
}

/// Whether `spelled` matches the tail of `key_exact`, comparing each component
/// case-sensitively only where `rules` says case is part of identity.
///
/// Alignment is by tail, so a two-part reference is compared against the key's
/// schema and table — and therefore against the *schema* and *table* rules, not
/// the catalog's.
fn tail_matches_with_case(
    key_exact: &[String],
    spelled: &[&str],
    rules: IdentifierCaseRules,
) -> bool {
    if key_exact.len() < spelled.len() {
        return false;
    }
    let offset = key_exact.len() - spelled.len();
    spelled.iter().enumerate().all(|(i, part)| {
        let key_part = &key_exact[offset + i];
        if rules.at(offset + i) {
            key_part.as_str() == *part
        } else {
            key_part.eq_ignore_ascii_case(part)
        }
    })
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
    /// Display strings of relations that match NO key under the supplied
    /// [`IdentifierCaseRules`] but WOULD match one if case were ignored.
    ///
    /// Whether such a reference names the routed upstream depends on connection
    /// state (`QUOTED_IDENTIFIERS_IGNORE_CASE`, a dataset's
    /// `is_case_insensitive`) that Rocky cannot observe, and both guesses are
    /// unsafe — rewriting reads the wrong table, not rewriting reads production.
    /// Callers that require isolation must fail closed on any. Always empty when
    /// `case_rules` is [`IdentifierCaseRules::all_insensitive`].
    pub case_fold_only_refs: Vec<String>,
}

/// Rewrite references to known upstream tables — bare, `schema.table`, or
/// `catalog.schema.table` — to a replacement [`DeferTarget`].
///
/// `renames` is keyed by the upstream's fully-qualified `catalog.schema.table`
/// identity, spelled as the dialect stores it — callers on a case-sensitive
/// dialect must NOT pre-fold their keys, or two distinct objects collapse into
/// one entry. A relation matches a key when every part it *does* spell agrees
/// with the key's tail under `case_rules`: a three-part
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
    case_rules: IdentifierCaseRules,
) -> Result<UpstreamRewriteOutcome, ParseError> {
    let mut statement = parse_single_statement(sql)?;

    // Pre-split each rename key into its (catalog, schema, table) parts once,
    // preserving case — the comparison below decides per component whether case
    // matters.
    let split: Vec<(Vec<String>, &String, &DeferTarget)> = renames
        .iter()
        .map(|(key, target)| {
            let exact: Vec<String> = key.split('.').map(str::to_string).collect();
            (exact, key, target)
        })
        .collect();

    let mut rewriter = UpstreamRewriter {
        renames: &split,
        case_rules,
        scopes: Vec::new(),
        rewritten_keys: HashSet::new(),
        ambiguous_refs: Vec::new(),
        case_fold_only_refs: Vec::new(),
    };
    let _: ControlFlow<()> = statement.visit(&mut rewriter);

    let UpstreamRewriter {
        rewritten_keys,
        ambiguous_refs,
        case_fold_only_refs,
        ..
    } = rewriter;
    Ok(UpstreamRewriteOutcome {
        sql: statement.to_string(),
        rewritten_keys,
        ambiguous_refs,
        case_fold_only_refs,
    })
}

/// Scope-aware visitor behind [`rewrite_upstream_refs`]. Shares the CTE
/// shadowing discipline with [`DeferRewriter`]: a bare name shadowed by a CTE
/// in scope is a reference to that CTE, never to the upstream.
struct UpstreamRewriter<'a> {
    /// `(key parts, original key, replacement)` triples, key parts as spelled.
    renames: &'a [(Vec<String>, &'a String, &'a DeferTarget)],
    /// Per-component case semantics for this dialect.
    case_rules: IdentifierCaseRules,
    scopes: Vec<HashSet<String>>,
    rewritten_keys: HashSet<String>,
    ambiguous_refs: Vec<String>,
    case_fold_only_refs: Vec<String>,
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
        // `(value, was_quoted)`. Quotedness is part of identity on dialects that
        // fold unquoted identifiers, so it must survive to the comparison.
        let mut spelled_parts: Vec<(&str, bool)> = Vec::with_capacity(relation.0.len());
        for part in &relation.0 {
            let Some(ident) = part.as_ident() else {
                return ControlFlow::Continue(());
            };
            spelled_parts.push((ident.value.as_str(), ident.quote_style.is_some()));
        }
        if spelled_parts.is_empty() || spelled_parts.len() > 3 {
            return ControlFlow::Continue(());
        }
        // A bare name shadowed by a CTE in scope refers to the CTE.
        if spelled_parts.len() == 1 && self.is_shadowed(spelled_parts[0].0) {
            return ControlFlow::Continue(());
        }
        // Resolve each part to the name the warehouse will actually look up.
        let resolved: Vec<String> = spelled_parts
            .iter()
            .map(|(value, quoted)| {
                if self.case_rules.unquoted_uppercases && !quoted {
                    value.to_uppercase()
                } else {
                    (*value).to_string()
                }
            })
            .collect();
        let original: Vec<&str> = resolved.iter().map(String::as_str).collect();
        // Tail-match each rename key, comparing every component under the
        // dialect's own rule for that component. Where case is part of identity
        // this is an exact comparison, because `raw.Orders` and `raw.orders` are
        // then two DIFFERENT objects and redirecting a read of one to the
        // other's replacement would silently read the wrong table. Where it is
        // not, the comparison folds, exactly as it always has.
        let candidates: Vec<&(Vec<String>, &String, &DeferTarget)> = self
            .renames
            .iter()
            .filter(|(key_exact, _, _)| {
                tail_matches_with_case(key_exact, &original, self.case_rules)
            })
            .collect();
        match candidates.as_slice() {
            [] => {
                // No key matches under the dialect's declared rules — but if one
                // matches when case is IGNORED, the right answer depends on
                // state this process cannot see.
                //
                // Neither guess is safe here, which is why this is reported
                // rather than decided. Rewriting a reference that names a
                // genuinely different object reads the wrong table; NOT
                // rewriting one that does name the routed upstream reads
                // PRODUCTION while the model writes its shadow — an isolation
                // break that reports success and then shows up as a clean
                // `rocky compare`. Case-sensitivity is connection state (a
                // Snowflake account may set QUOTED_IDENTIFIERS_IGNORE_CASE; a
                // BigQuery dataset may be `is_case_insensitive`), so the caller
                // is handed the near-miss and fails closed.
                //
                // Inert wherever `case_rules` is already all-insensitive: folded
                // and rule-based matching coincide, so this can never fire.
                if self.renames.iter().any(|(key_exact, _, _)| {
                    tail_matches_with_case(
                        key_exact,
                        &original,
                        IdentifierCaseRules::all_insensitive(),
                    )
                }) {
                    self.case_fold_only_refs.push(relation.to_string());
                }
            }
            [(_, key, target)] => {
                *relation = target.to_object_name();
                self.rewritten_keys.insert((*key).clone());
            }
            // Several upstreams are indistinguishable from this reference under
            // the dialect's own rules, so it genuinely names more than one of
            // them.
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
        let out = rewrite_upstream_refs(
            "SELECT * FROM cat.raw.orders",
            &renames,
            IdentifierCaseRules::all_insensitive(),
        )
        .unwrap();
        assert_eq!(out.sql, "SELECT * FROM cat.replay_ns.raw__orders");
        assert!(out.rewritten_keys.contains("cat.raw.orders"));
        assert!(out.ambiguous_refs.is_empty());
    }

    #[test]
    fn rewrites_two_part_and_bare_tails() {
        let renames = renames_map(&[("cat.raw.orders", target("cat", "replay_ns", "raw__orders"))]);
        let two = rewrite_upstream_refs(
            "SELECT * FROM raw.orders",
            &renames,
            IdentifierCaseRules::all_insensitive(),
        )
        .unwrap();
        assert_eq!(two.sql, "SELECT * FROM cat.replay_ns.raw__orders");
        let bare = rewrite_upstream_refs(
            "SELECT * FROM orders",
            &renames,
            IdentifierCaseRules::all_insensitive(),
        )
        .unwrap();
        assert_eq!(bare.sql, "SELECT * FROM cat.replay_ns.raw__orders");
        assert!(bare.rewritten_keys.contains("cat.raw.orders"));
    }

    #[test]
    fn matches_case_insensitively() {
        let renames = renames_map(&[("cat.raw.orders", target("cat", "replay_ns", "raw__orders"))]);
        let out = rewrite_upstream_refs(
            "SELECT * FROM CAT.Raw.ORDERS",
            &renames,
            IdentifierCaseRules::all_insensitive(),
        )
        .unwrap();
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
        let out = rewrite_upstream_refs(
            "SELECT * FROM orders",
            &renames,
            IdentifierCaseRules::all_insensitive(),
        )
        .unwrap();
        assert_eq!(out.sql, "SELECT * FROM orders", "ambiguous ref untouched");
        assert!(out.rewritten_keys.is_empty());
        assert_eq!(out.ambiguous_refs, vec!["orders".to_string()]);
        // A fully qualified reference disambiguates.
        let fq = rewrite_upstream_refs(
            "SELECT * FROM cat.staging.orders",
            &renames,
            IdentifierCaseRules::all_insensitive(),
        )
        .unwrap();
        assert_eq!(fq.sql, "SELECT * FROM cat.replay_ns.staging__orders");
        assert!(fq.ambiguous_refs.is_empty());
    }

    #[test]
    fn cte_shadowing_protects_bare_names_only_in_scope() {
        let renames = renames_map(&[("cat.raw.orders", target("cat", "replay_ns", "raw__orders"))]);
        let out = rewrite_upstream_refs(
            "WITH orders AS (SELECT 1 AS id) SELECT * FROM orders",
            &renames,
            IdentifierCaseRules::all_insensitive(),
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
        let out = rewrite_upstream_refs(
            "SELECT * FROM cat.raw.customers",
            &renames,
            IdentifierCaseRules::all_insensitive(),
        )
        .unwrap();
        assert!(out.rewritten_keys.is_empty());
        assert_eq!(out.sql, "SELECT * FROM cat.raw.customers");
    }

    // -- case-aware disambiguation ------------------------------------------

    /// Case-INsensitive dialect: any spelling reaches the one upstream.
    #[test]
    fn a_folding_dialect_matches_every_spelling() {
        let renames = renames_map(&[("cat.raw.orders", target("cat", "shadow", "orders"))]);
        for spelling in ["Orders", "raw.ORDERS", "CAT.Raw.Orders"] {
            let out = rewrite_upstream_refs(
                &format!("SELECT * FROM {spelling}"),
                &renames,
                IdentifierCaseRules::all_insensitive(),
            )
            .unwrap();
            assert_eq!(
                out.sql, "SELECT * FROM cat.shadow.orders",
                "case-insensitive dialect must still match {spelling}"
            );
        }
    }

    /// Case-SENSITIVE dialect: a differently-cased reference names a DIFFERENT
    /// object, so it must be left alone.
    ///
    /// This is the correction that the `apply_shadow_rewrite` guard tests forced.
    /// An earlier revision of this matcher gathered candidates by folding and
    /// only compared case when several matched — which meant a lone key
    /// `raw.orders` captured a read of `raw.Orders`, redirecting it to a table it
    /// never named. Matching on identity, not resemblance, is the whole point.
    #[test]
    fn a_case_sensitive_dialect_leaves_a_differently_cased_reference_alone() {
        let renames = renames_map(&[("cat.raw.orders", target("cat", "shadow", "orders"))]);
        for spelling in ["cat.raw.Orders", "raw.ORDERS", "Orders"] {
            let out = rewrite_upstream_refs(
                &format!("SELECT * FROM {spelling}"),
                &renames,
                IdentifierCaseRules::uniform(true),
            )
            .unwrap();
            assert_eq!(
                out.sql,
                format!("SELECT * FROM {spelling}"),
                "{spelling} names a different object than cat.raw.orders"
            );
            assert!(out.rewritten_keys.is_empty());
            assert!(
                out.ambiguous_refs.is_empty(),
                "not ambiguous — it matches no key under these rules"
            );
            // But it is NOT silently fine: it matches when case is ignored, and
            // whether that means it names the routed upstream depends on
            // connection state, so it is reported for the caller to refuse on.
            assert_eq!(
                out.case_fold_only_refs,
                vec![spelling.to_string()],
                "a case-only near-miss must be reported, never silently skipped"
            );
        }
        // The ordinary spellings still match and report nothing. This is the
        // usability floor for the refusal above: a model that spells its
        // upstream the way the target is configured — including the bare-name
        // idiom Rocky models normally use — is unaffected on a case-sensitive
        // dialect that does not fold unquoted identifiers (BigQuery). Snowflake,
        // which does fold them, is covered separately below.
        for spelling in ["cat.raw.orders", "raw.orders", "orders"] {
            let out = rewrite_upstream_refs(
                &format!("SELECT * FROM {spelling}"),
                &renames,
                IdentifierCaseRules::uniform(true),
            )
            .unwrap();
            assert_eq!(
                out.sql, "SELECT * FROM cat.shadow.orders",
                "matching-case spelling {spelling} must route normally"
            );
            assert!(
                out.case_fold_only_refs.is_empty(),
                "{spelling} matches exactly, so nothing is reported"
            );
        }
    }

    /// Two upstreams differing only by case are two objects on a case-sensitive
    /// dialect. The reference's own spelling picks one.
    ///
    /// This is the case that used to force `apply_shadow_rewrite` to refuse the
    /// entire run. Resolving it here is what allows that guard to be deleted.
    #[test]
    fn case_separates_two_keys_that_fold_together() {
        let renames = renames_map(&[
            ("cat.raw.Orders", target("cat", "shadow", "upper__orders")),
            ("cat.raw.orders", target("cat", "shadow", "lower__orders")),
        ]);
        let rules = IdentifierCaseRules::uniform(true);

        let upper = rewrite_upstream_refs("SELECT * FROM cat.raw.Orders", &renames, rules).unwrap();
        assert_eq!(upper.sql, "SELECT * FROM cat.shadow.upper__orders");
        assert!(upper.rewritten_keys.contains("cat.raw.Orders"));
        assert!(upper.ambiguous_refs.is_empty());

        let lower = rewrite_upstream_refs("SELECT * FROM cat.raw.orders", &renames, rules).unwrap();
        assert_eq!(lower.sql, "SELECT * FROM cat.shadow.lower__orders");
        assert!(lower.rewritten_keys.contains("cat.raw.orders"));
        assert!(lower.ambiguous_refs.is_empty());
    }

    /// A third spelling matches neither key, so it names neither object and is
    /// left as parsed. Not ambiguous — unmatched.
    #[test]
    fn a_third_spelling_matches_neither_key() {
        let renames = renames_map(&[
            ("cat.raw.Orders", target("cat", "shadow", "upper__orders")),
            ("cat.raw.orders", target("cat", "shadow", "lower__orders")),
        ]);
        let out = rewrite_upstream_refs(
            "SELECT * FROM cat.raw.ORDERS",
            &renames,
            IdentifierCaseRules::uniform(true),
        )
        .unwrap();
        assert_eq!(out.sql, "SELECT * FROM cat.raw.ORDERS", "left as parsed");
        assert!(out.rewritten_keys.is_empty());
        assert!(out.ambiguous_refs.is_empty());
    }

    /// On a case-INsensitive dialect the same two keys name ONE object, so no
    /// spelling can separate them and every reference is ambiguous. The caller's
    /// duplicate-target checks are what should catch this first; the matcher
    /// still refuses rather than guessing.
    #[test]
    fn folding_keys_stay_ambiguous_on_a_case_insensitive_dialect() {
        let renames = renames_map(&[
            ("cat.raw.Orders", target("cat", "shadow", "upper__orders")),
            ("cat.raw.orders", target("cat", "shadow", "lower__orders")),
        ]);
        let out = rewrite_upstream_refs(
            "SELECT * FROM cat.raw.Orders",
            &renames,
            IdentifierCaseRules::all_insensitive(),
        )
        .unwrap();
        assert!(out.rewritten_keys.is_empty());
        assert_eq!(out.ambiguous_refs, vec!["cat.raw.Orders".to_string()]);
    }

    /// Disambiguation aligns by TAIL, so a two-part reference is judged by the
    /// schema and table rules — never the catalog's. With only the catalog
    /// case-sensitive, two keys differing in the schema's case still fold
    /// together and stay ambiguous.
    #[test]
    fn per_component_rules_align_to_the_tail() {
        let renames = renames_map(&[
            ("cat.Raw.orders", target("cat", "shadow", "upper__raw")),
            ("cat.raw.orders", target("cat", "shadow", "lower__raw")),
        ]);
        let catalog_only = IdentifierCaseRules {
            catalog: true,
            schema: false,
            table: false,
            unquoted_uppercases: false,
        };
        let out =
            rewrite_upstream_refs("SELECT * FROM Raw.orders", &renames, catalog_only).unwrap();
        assert_eq!(
            out.ambiguous_refs,
            vec!["Raw.orders".to_string()],
            "the schema rule governs a two-part ref, and it is insensitive here"
        );

        // Turning the schema rule on separates them.
        let schema_sensitive = IdentifierCaseRules {
            catalog: true,
            schema: true,
            table: false,
            unquoted_uppercases: false,
        };
        let out =
            rewrite_upstream_refs("SELECT * FROM Raw.orders", &renames, schema_sensitive).unwrap();
        assert_eq!(out.sql, "SELECT * FROM cat.shadow.upper__raw");
        assert!(out.ambiguous_refs.is_empty());
    }

    /// Snowflake's quoting axis: identical TEXT can name different objects.
    ///
    /// Rocky renders Snowflake targets quoted, so a configured target `orders`
    /// is the object `orders`. Snowflake resolves an unquoted `FROM orders` to
    /// `ORDERS` — a different table. Matching on spelled text alone (which this
    /// matcher did before, and on `main` still does) rewrites that read to the
    /// shadow and silently changes its source.
    ///
    /// The matrix below is the whole point: with an UPPERCASE configured target
    /// — the Snowflake-idiomatic choice — an unquoted reference resolves onto it
    /// and routes normally, so the common case is unaffected. It is only the
    /// lowercase/mixed-case configured target that becomes a near-miss, and such
    /// a model could not have read its upstream in production either.
    #[test]
    fn snowflake_resolves_unquoted_identifiers_before_matching() {
        let rules = IdentifierCaseRules::uniform_uppercasing(true);

        // Upper-case target: the idiomatic Snowflake shape. Unquoted references
        // resolve onto it, quoted ones must spell it exactly.
        let upper = renames_map(&[("CAT.RAW.ORDERS", target("CAT", "SHADOW", "ORDERS"))]);
        for spelling in ["cat.raw.orders", "CAT.RAW.ORDERS", "raw.orders", "orders"] {
            let out =
                rewrite_upstream_refs(&format!("SELECT * FROM {spelling}"), &upper, rules).unwrap();
            assert_eq!(
                out.sql, "SELECT * FROM CAT.SHADOW.ORDERS",
                "unquoted {spelling} resolves upper and routes"
            );
            assert!(out.case_fold_only_refs.is_empty(), "{spelling}");
        }

        // Lower-case target: Rocky created `"orders"`, which an UNQUOTED
        // reference cannot name. Reported, not silently rewritten.
        let lower = renames_map(&[("cat.raw.orders", target("cat", "shadow", "orders"))]);
        let out = rewrite_upstream_refs("SELECT * FROM cat.raw.orders", &lower, rules).unwrap();
        assert!(
            out.rewritten_keys.is_empty(),
            "an unquoted ref resolves to CAT.RAW.ORDERS, not cat.raw.orders: {}",
            out.sql
        );
        assert_eq!(
            out.case_fold_only_refs,
            vec!["cat.raw.orders".to_string()],
            "the near-miss must be reported so the caller refuses"
        );

        // Quoted, spelled exactly: that IS the object, so it routes.
        let out = rewrite_upstream_refs("SELECT * FROM \"cat\".\"raw\".\"orders\"", &lower, rules)
            .unwrap();
        assert_eq!(out.sql, "SELECT * FROM cat.shadow.orders");
        assert!(out.case_fold_only_refs.is_empty());
    }

    /// A CTE still shadows a bare name before any of this runs.
    #[test]
    fn cte_shadowing_wins_over_case_disambiguation() {
        let renames = renames_map(&[
            ("cat.raw.Orders", target("cat", "shadow", "upper__orders")),
            ("cat.raw.orders", target("cat", "shadow", "lower__orders")),
        ]);
        let out = rewrite_upstream_refs(
            "WITH Orders AS (SELECT 1 AS id) SELECT * FROM Orders",
            &renames,
            IdentifierCaseRules::uniform(true),
        )
        .unwrap();
        assert!(out.rewritten_keys.is_empty());
        assert!(out.ambiguous_refs.is_empty(), "a CTE ref is not ambiguous");
    }

    #[test]
    fn non_matching_relations_are_left_alone() {
        let renames = renames_map(&[("cat.raw.orders", target("cat", "replay_ns", "raw__orders"))]);
        let out = rewrite_upstream_refs(
            "SELECT o.id FROM cat.raw.orders o JOIN other.schema.customers c ON o.id = c.id",
            &renames,
            IdentifierCaseRules::all_insensitive(),
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
