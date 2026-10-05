//! Inline `ephemeral` models into their consumers as CTEs.
//!
//! An ephemeral model is never materialized. Each model that reads one gets
//! the ephemeral model's SQL as a CTE at the front of its own `WITH` clause,
//! and its references are rewritten to that CTE:
//!
//! ```sql
//! -- eph_orders (ephemeral): SELECT order_id, amount FROM raw.orders
//! -- fct (consumer), as authored:
//! SELECT order_id FROM eph_orders
//! -- fct, as executed:
//! WITH __rocky_ephemeral__eph_orders AS (SELECT order_id, amount FROM raw.orders)
//! SELECT order_id FROM __rocky_ephemeral__eph_orders AS eph_orders
//! ```
//!
//! The CTE name is `__rocky_ephemeral__<model>` (dbt spells the same idea
//! `__dbt__cte__<model>`). When that name already appears in the statement, a
//! numeric suffix (`_2`, `_3`, …) keeps it unique. A reference with no alias
//! gets the model's own name as its alias, so a qualified column such as
//! `eph_orders.amount` still binds.
//!
//! The rewrite walks the parsed AST, not the text:
//!
//! - Only a **bare, single-part** relation is a model reference — the same rule
//!   `rocky-compiler`'s dependency resolver uses. Matching is exact on the
//!   identifier value, so a quoted `"eph_orders"` matches and `EPH_ORDERS` does
//!   not. Qualified references (`schema.eph_orders`) are never rewritten; they
//!   are reported in [`InlineOutcome::target_refs`] when they spell the
//!   ephemeral model's nominal `[target]`, which no table backs.
//! - A CTE in scope with the same name shadows the model (the reference reads
//!   the CTE). Shadowing compares case-insensitively, so a reference is left
//!   alone whenever any warehouse could bind it to the CTE.
//! - Chains inline transitively. An ephemeral model that reads another adds
//!   that one's CTE ahead of its own, in dependency order, each CTE once per
//!   consumer.
//! - The new CTEs go in FRONT of any existing `WITH` list. A non-recursive CTE
//!   sees only the CTEs before it, so the consumer's own CTEs cannot capture a
//!   name the inlined SQL reads. Under `WITH RECURSIVE` some dialects let an
//!   earlier CTE see a later one, so a clash there is refused
//!   ([`InlineError::RecursiveCapture`]) rather than guessed.
//!
//! Re-serialization preserves meaning, but the AST round trip drops comments
//! and normalizes whitespace. `-- rocky-allow:` pragma lines from the consumer
//! and every inlined model are kept, at the top. A statement with no ephemeral
//! reference is never re-serialized ([`InlineOutcome::sql`] is `None`).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::ControlFlow;

use sqlparser::ast::{
    Cte, Ident, ObjectName, Query, Statement, TableAlias, TableFactor, Visit, VisitMut, Visitor,
    VisitorMut, With, helpers::attached_token::AttachedToken,
};

use crate::defer::{CteScopeStack, IdentifierCaseRules, RecursiveCteVisibility};
use crate::parser::{ParseError, parse_single_statement};

/// Prefix of every CTE name the inliner introduces.
pub const CTE_PREFIX: &str = "__rocky_ephemeral__";

/// One ephemeral model the inliner may splice into a consumer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EphemeralModel {
    /// The model's SELECT, as authored (run variables already substituted).
    pub sql: String,
    /// The model's nominal `[target]` parts. Empty `catalog` means the target
    /// is two-part. Used only to flag qualified reads of a table that does
    /// not exist; never to rewrite.
    pub catalog: String,
    /// Nominal target schema.
    pub schema: String,
    /// Nominal target table.
    pub table: String,
}

/// Result of [`inline_ephemeral_refs`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InlineOutcome {
    /// The rewritten statement, or `None` when the SQL reads no ephemeral
    /// model by bare name (the input is then left exactly as written).
    pub sql: Option<String>,
    /// The ephemeral models inlined, in CTE (dependency) order.
    pub inlined: Vec<String>,
    /// Qualified relations in the input SQL that spell an ephemeral model's
    /// nominal target, as `(model, spelled reference)`. No table backs that
    /// name, so such a read hits a catalog error or a stale table.
    pub target_refs: Vec<(String, String)>,
}

/// Why a consumer could not be inlined.
#[derive(Debug, thiserror::Error)]
pub enum InlineError {
    /// The consumer or an ephemeral model's SQL did not parse.
    #[error("the SQL of model '{model}' did not parse: {source}")]
    Parse {
        /// Model whose SQL failed.
        model: String,
        /// Parser error.
        #[source]
        source: ParseError,
    },
    /// The consumer or an ephemeral model is not a single `SELECT` query.
    #[error("model '{model}' is not a single SELECT query")]
    NotAQuery {
        /// Model whose SQL is not a query.
        model: String,
    },
    /// Ephemeral models read each other in a cycle.
    #[error("ephemeral models read each other in a cycle: {}", chain.join(" -> "))]
    Cycle {
        /// The cycle, first model repeated at the end.
        chain: Vec<String>,
    },
    /// Under `WITH RECURSIVE`, a CTE of the consumer has the same name as a
    /// relation the inlined SQL reads, and some dialects would bind the read
    /// to that CTE.
    #[error(
        "the consumer's `WITH RECURSIVE` declares a CTE named `{name}`, which the SQL of \
         ephemeral model '{ephemeral}' also reads; some dialects would bind that read to the CTE"
    )]
    RecursiveCapture {
        /// Ephemeral model whose read would be captured.
        ephemeral: String,
        /// The clashing name.
        name: String,
    },
}

/// Inline every ephemeral model `sql` reads (directly or through another
/// ephemeral model) as a CTE, and rewrite the references.
///
/// `consumer` names the model that owns `sql`, for error messages.
/// `ephemerals` maps each ephemeral model's name to its definition.
///
/// # Errors
///
/// See [`InlineError`]. An input with no ephemeral reference only fails when
/// it does not parse.
pub fn inline_ephemeral_refs(
    consumer: &str,
    sql: &str,
    ephemerals: &BTreeMap<String, EphemeralModel>,
) -> Result<InlineOutcome, InlineError> {
    let mut root = parse_query(consumer, sql)?;

    // Pass 1: which ephemeral models are read, transitively.
    let direct = scan(&mut root, ephemerals, true);
    let target_refs = direct.target_refs;
    if direct.found.is_empty() {
        return Ok(InlineOutcome {
            sql: None,
            inlined: Vec::new(),
            target_refs,
        });
    }

    let mut bodies: HashMap<String, Query> = HashMap::new();
    let mut body_refs: HashMap<String, Vec<String>> = HashMap::new();
    let mut queue: Vec<String> = direct.found.clone();
    while let Some(name) = queue.pop() {
        if bodies.contains_key(&name) {
            continue;
        }
        let model = &ephemerals[&name];
        let mut body = parse_query(&name, &model.sql)?;
        let refs = scan(&mut body, ephemerals, false).found;
        queue.extend(refs.iter().cloned());
        body_refs.insert(name.clone(), refs);
        bodies.insert(name, body);
    }

    let order = dependency_order(&direct.found, &body_refs)?;

    // Every name already present anywhere in the statement, folded, so a
    // generated CTE name never collides with — or captures — one of them.
    let mut reserved: HashSet<String> = HashSet::new();
    collect_names(&root, &mut reserved);
    for body in bodies.values() {
        collect_names(body, &mut reserved);
    }
    let mut cte_names: HashMap<String, String> = HashMap::new();
    for name in &order {
        let base = format!("{CTE_PREFIX}{}", sanitize(name));
        let mut candidate = base.clone();
        let mut n = 2;
        while reserved.contains(&candidate.to_lowercase()) {
            candidate = format!("{base}_{n}");
            n += 1;
        }
        reserved.insert(candidate.to_lowercase());
        cte_names.insert(name.clone(), candidate);
    }

    // Pass 2: rewrite the references.
    rewrite(&mut root, ephemerals, &cte_names);
    let mut ctes = Vec::with_capacity(order.len());
    let mut free_names: Vec<(String, String)> = Vec::new();
    for name in &order {
        let mut body = bodies
            .remove(name)
            .expect("every ordered model was parsed in pass 1");
        let free = rewrite(&mut body, ephemerals, &cte_names);
        free_names.extend(free.into_iter().map(|f| (name.clone(), f)));
        ctes.push(Cte {
            alias: TableAlias {
                explicit: false,
                name: Ident::new(cte_names[name].clone()),
                columns: Vec::new(),
                at: None,
            },
            query: Box::new(body),
            from: None,
            materialized: None,
            closing_paren_token: AttachedToken::empty(),
        });
    }

    match &mut root.with {
        Some(with) => {
            if with.recursive {
                let declared: HashSet<String> = with
                    .cte_tables
                    .iter()
                    .map(|cte| cte.alias.name.value.to_lowercase())
                    .collect();
                if let Some((ephemeral, name)) = free_names
                    .iter()
                    .find(|(_, free)| declared.contains(&free.to_lowercase()))
                {
                    return Err(InlineError::RecursiveCapture {
                        ephemeral: ephemeral.clone(),
                        name: name.clone(),
                    });
                }
            }
            ctes.append(&mut with.cte_tables);
            with.cte_tables = ctes;
        }
        None => {
            root.with = Some(With {
                with_token: AttachedToken::empty(),
                recursive: false,
                cte_tables: ctes,
            });
        }
    }

    // The AST round trip drops comments, and `rocky-allow` pragmas live in
    // line comments. Carry the consumer's and every inlined model's pragma
    // lines over, so a construct they allow stays allowed in the merged SQL.
    let mut header = String::new();
    for source in std::iter::once(sql).chain(order.iter().map(|n| ephemerals[n].sql.as_str())) {
        for line in source.lines() {
            let trimmed = line.trim_start();
            if trimmed.starts_with("--") && trimmed.contains("rocky-allow") {
                header.push_str(trimmed);
                header.push('\n');
            }
        }
    }

    Ok(InlineOutcome {
        sql: Some(format!("{header}{root}")),
        inlined: order,
        target_refs,
    })
}

fn parse_query(model: &str, sql: &str) -> Result<Query, InlineError> {
    match parse_single_statement(sql) {
        Ok(Statement::Query(query)) => Ok(*query),
        Ok(_) => Err(InlineError::NotAQuery {
            model: model.to_string(),
        }),
        Err(source) => Err(InlineError::Parse {
            model: model.to_string(),
            source,
        }),
    }
}

/// Generated CTE names stay unquoted, so they fold the same way at the
/// definition and at every reference on every dialect.
fn sanitize(name: &str) -> String {
    name.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect()
}

/// Post-order over the ephemeral read graph: a model's CTE comes after every
/// CTE it reads.
fn dependency_order(
    roots: &[String],
    refs: &HashMap<String, Vec<String>>,
) -> Result<Vec<String>, InlineError> {
    fn visit(
        name: &str,
        refs: &HashMap<String, Vec<String>>,
        done: &mut HashSet<String>,
        stack: &mut Vec<String>,
        out: &mut Vec<String>,
    ) -> Result<(), InlineError> {
        if done.contains(name) {
            return Ok(());
        }
        if let Some(pos) = stack.iter().position(|n| n == name) {
            let mut chain = stack[pos..].to_vec();
            chain.push(name.to_string());
            return Err(InlineError::Cycle { chain });
        }
        stack.push(name.to_string());
        for dep in refs.get(name).map(Vec::as_slice).unwrap_or_default() {
            visit(dep, refs, done, stack, out)?;
        }
        stack.pop();
        done.insert(name.to_string());
        out.push(name.to_string());
        Ok(())
    }
    let mut done = HashSet::new();
    let mut out = Vec::new();
    for root in roots {
        visit(root, refs, &mut done, &mut Vec::new(), &mut out)?;
    }
    Ok(out)
}

/// Shadowing is answered case-insensitively: when any dialect could bind the
/// reference to a CTE, it is left alone. `Forward` for the same reason.
fn scope_stack() -> CteScopeStack {
    CteScopeStack::new(
        IdentifierCaseRules::all_insensitive(),
        RecursiveCteVisibility::Forward,
    )
}

struct Scan {
    found: Vec<String>,
    target_refs: Vec<(String, String)>,
}

/// Find the ephemeral models `query` reads by bare name, in first-seen order.
/// With `want_targets`, also report qualified reads of their nominal targets.
fn scan(
    query: &mut Query,
    ephemerals: &BTreeMap<String, EphemeralModel>,
    want_targets: bool,
) -> Scan {
    let mut visitor = RefVisitor {
        ephemerals,
        cte_names: None,
        scopes: scope_stack(),
        found: Vec::new(),
        free: Vec::new(),
        target_refs: Vec::new(),
        want_targets,
    };
    let _: ControlFlow<()> = VisitMut::visit(query, &mut visitor);
    Scan {
        found: visitor.found,
        target_refs: visitor.target_refs,
    }
}

/// Rewrite the ephemeral references in `query` to their CTE names. Returns
/// the bare relation names left unrewritten and unshadowed — the names the
/// query reads from outside itself.
fn rewrite(
    query: &mut Query,
    ephemerals: &BTreeMap<String, EphemeralModel>,
    cte_names: &HashMap<String, String>,
) -> Vec<String> {
    let mut visitor = RefVisitor {
        ephemerals,
        cte_names: Some(cte_names),
        scopes: scope_stack(),
        found: Vec::new(),
        free: Vec::new(),
        target_refs: Vec::new(),
        want_targets: false,
    };
    let _: ControlFlow<()> = VisitMut::visit(query, &mut visitor);
    visitor.free
}

struct RefVisitor<'a> {
    ephemerals: &'a BTreeMap<String, EphemeralModel>,
    /// `None` scans; `Some` rewrites.
    cte_names: Option<&'a HashMap<String, String>>,
    scopes: CteScopeStack,
    found: Vec<String>,
    free: Vec<String>,
    target_refs: Vec<(String, String)>,
    want_targets: bool,
}

impl RefVisitor<'_> {
    fn check_target(&mut self, name: &ObjectName) {
        let parts: Option<Vec<&str>> = name
            .0
            .iter()
            .map(|p| p.as_ident().map(|i| i.value.as_str()))
            .collect();
        let Some(parts) = parts else { return };
        for (model, def) in self.ephemerals {
            let matches = match parts.as_slice() {
                [schema, table] => {
                    schema.eq_ignore_ascii_case(&def.schema)
                        && table.eq_ignore_ascii_case(&def.table)
                }
                [catalog, schema, table] => {
                    !def.catalog.is_empty()
                        && catalog.eq_ignore_ascii_case(&def.catalog)
                        && schema.eq_ignore_ascii_case(&def.schema)
                        && table.eq_ignore_ascii_case(&def.table)
                }
                _ => false,
            };
            if matches {
                self.target_refs.push((model.clone(), name.to_string()));
            }
        }
    }
}

impl VisitorMut for RefVisitor<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        self.scopes.enter_query(query);
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &mut Query) -> ControlFlow<Self::Break> {
        self.scopes.exit_query();
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, factor: &mut TableFactor) -> ControlFlow<Self::Break> {
        let TableFactor::Table {
            name, alias, args, ..
        } = factor
        else {
            return ControlFlow::Continue(());
        };
        if args.is_some() {
            return ControlFlow::Continue(());
        }
        if name.0.len() != 1 {
            if self.want_targets {
                self.check_target(name);
            }
            return ControlFlow::Continue(());
        }
        let Some(ident) = name.0[0].as_ident().cloned() else {
            return ControlFlow::Continue(());
        };
        if self
            .scopes
            .is_shadowed(&ident.value, ident.quote_style.is_some())
        {
            return ControlFlow::Continue(());
        }
        if !self.ephemerals.contains_key(&ident.value) {
            self.free.push(ident.value.clone());
            return ControlFlow::Continue(());
        }
        match self.cte_names {
            None => {
                if !self.found.contains(&ident.value) {
                    self.found.push(ident.value.clone());
                }
            }
            Some(cte_names) => {
                let cte = &cte_names[&ident.value];
                *name = ObjectName::from(vec![Ident::new(cte.clone())]);
                if alias.is_none() {
                    *alias = Some(TableAlias {
                        explicit: true,
                        name: ident,
                        columns: Vec::new(),
                        at: None,
                    });
                }
            }
        }
        ControlFlow::Continue(())
    }
}

/// Collect every CTE alias and every relation-name part in `query`, folded.
pub(crate) fn collect_names(query: &Query, out: &mut HashSet<String>) {
    struct Names<'a>(&'a mut HashSet<String>);
    impl Visitor for Names<'_> {
        type Break = ();
        fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<()> {
            if let Some(with) = &query.with {
                for cte in &with.cte_tables {
                    self.0.insert(cte.alias.name.value.to_lowercase());
                }
            }
            ControlFlow::Continue(())
        }
        fn pre_visit_table_factor(&mut self, factor: &TableFactor) -> ControlFlow<()> {
            if let TableFactor::Table { name, alias, .. } = factor {
                for part in &name.0 {
                    if let Some(ident) = part.as_ident() {
                        self.0.insert(ident.value.to_lowercase());
                    }
                }
                if let Some(alias) = alias {
                    self.0.insert(alias.name.value.to_lowercase());
                }
            }
            ControlFlow::Continue(())
        }
    }
    let _ = Visit::visit(query, &mut Names(out));
}

#[cfg(test)]
mod tests {
    use super::*;

    fn eph(sql: &str, table: &str) -> EphemeralModel {
        EphemeralModel {
            sql: sql.to_string(),
            catalog: String::new(),
            schema: "main".to_string(),
            table: table.to_string(),
        }
    }

    fn map(entries: &[(&str, EphemeralModel)]) -> BTreeMap<String, EphemeralModel> {
        entries
            .iter()
            .map(|(n, m)| ((*n).to_string(), m.clone()))
            .collect()
    }

    fn inline(sql: &str, ephemerals: &BTreeMap<String, EphemeralModel>) -> String {
        inline_ephemeral_refs("consumer", sql, ephemerals)
            .unwrap()
            .sql
            .expect("an ephemeral reference was rewritten")
    }

    #[test]
    fn inlines_a_single_reference_and_keeps_the_name_as_alias() {
        let e = map(&[(
            "eph_orders",
            eph("SELECT order_id, amount FROM raw.orders", "eph_orders"),
        )]);
        let out = inline("SELECT eph_orders.amount FROM eph_orders", &e);
        assert_eq!(
            out,
            "WITH __rocky_ephemeral__eph_orders AS (SELECT order_id, amount FROM raw.orders) \
             SELECT eph_orders.amount FROM __rocky_ephemeral__eph_orders AS eph_orders"
        );
    }

    #[test]
    fn existing_alias_is_kept() {
        let e = map(&[("eph", eph("SELECT 1 AS a", "eph"))]);
        let out = inline("SELECT o.a FROM eph AS o", &e);
        assert_eq!(
            out,
            "WITH __rocky_ephemeral__eph AS (SELECT 1 AS a) SELECT o.a FROM __rocky_ephemeral__eph AS o"
        );
    }

    #[test]
    fn no_reference_leaves_sql_untouched() {
        let e = map(&[("eph", eph("SELECT 1 AS a", "eph"))]);
        let sql = "SELECT  a -- comment\nFROM other";
        let outcome = inline_ephemeral_refs("c", sql, &e).unwrap();
        assert_eq!(outcome.sql, None);
        assert!(outcome.inlined.is_empty());
    }

    #[test]
    fn merges_into_an_existing_with_clause_ahead_of_its_ctes() {
        let e = map(&[("eph", eph("SELECT id FROM raw.t", "eph"))]);
        let out = inline("WITH x AS (SELECT id FROM eph) SELECT id FROM x", &e);
        assert_eq!(
            out,
            "WITH __rocky_ephemeral__eph AS (SELECT id FROM raw.t), \
             x AS (SELECT id FROM __rocky_ephemeral__eph AS eph) SELECT id FROM x"
        );
    }

    #[test]
    fn a_cte_with_the_models_name_shadows_it() {
        let e = map(&[("eph", eph("SELECT id FROM raw.t", "eph"))]);
        let outcome =
            inline_ephemeral_refs("c", "WITH eph AS (SELECT 1 AS id) SELECT id FROM eph", &e)
                .unwrap();
        assert_eq!(outcome.sql, None, "the CTE wins, nothing to inline");
        // Case-insensitive shadowing: any dialect that could bind to the CTE wins.
        let outcome =
            inline_ephemeral_refs("c", "WITH EPH AS (SELECT 1 AS id) SELECT id FROM eph", &e)
                .unwrap();
        assert_eq!(outcome.sql, None);
    }

    #[test]
    fn generated_name_avoids_a_collision() {
        let e = map(&[("eph", eph("SELECT 1 AS id", "eph"))]);
        let out = inline(
            "WITH __rocky_ephemeral__eph AS (SELECT 2 AS id) \
             SELECT a.id FROM eph AS a JOIN __rocky_ephemeral__eph AS b ON a.id = b.id",
            &e,
        );
        assert_eq!(
            out,
            "WITH __rocky_ephemeral__eph_2 AS (SELECT 1 AS id), \
             __rocky_ephemeral__eph AS (SELECT 2 AS id) \
             SELECT a.id FROM __rocky_ephemeral__eph_2 AS a \
             JOIN __rocky_ephemeral__eph AS b ON a.id = b.id"
        );
    }

    #[test]
    fn nested_chain_inlines_in_dependency_order_once() {
        let e = map(&[
            ("a", eph("SELECT id FROM raw.t", "a")),
            ("b", eph("SELECT id FROM a", "b")),
            ("c", eph("SELECT b.id FROM b JOIN a ON a.id = b.id", "c")),
        ]);
        let outcome =
            inline_ephemeral_refs("fct", "SELECT id FROM c UNION ALL SELECT id FROM a", &e)
                .unwrap();
        assert_eq!(outcome.inlined, vec!["a", "b", "c"]);
        assert_eq!(
            outcome.sql.unwrap(),
            "WITH __rocky_ephemeral__a AS (SELECT id FROM raw.t), \
             __rocky_ephemeral__b AS (SELECT id FROM __rocky_ephemeral__a AS a), \
             __rocky_ephemeral__c AS (SELECT b.id FROM __rocky_ephemeral__b AS b \
             JOIN __rocky_ephemeral__a AS a ON a.id = b.id) \
             SELECT id FROM __rocky_ephemeral__c AS c UNION ALL \
             SELECT id FROM __rocky_ephemeral__a AS a"
        );
    }

    #[test]
    fn quoted_names_match_by_value_and_keep_their_quotes_in_the_alias() {
        let e = map(&[("eph", eph("SELECT 1 AS id", "eph"))]);
        let out = inline("SELECT \"eph\".id FROM \"eph\"", &e);
        assert_eq!(
            out,
            "WITH __rocky_ephemeral__eph AS (SELECT 1 AS id) \
             SELECT \"eph\".id FROM __rocky_ephemeral__eph AS \"eph\""
        );
        // A different case is a different model name: not a reference.
        let outcome = inline_ephemeral_refs("c", "SELECT id FROM EPH", &e).unwrap();
        assert_eq!(outcome.sql, None);
    }

    #[test]
    fn qualified_references_are_not_rewritten_but_target_reads_are_reported() {
        let e = map(&[("eph", eph("SELECT 1 AS id", "eph"))]);
        let outcome = inline_ephemeral_refs(
            "c",
            "SELECT id FROM raw.eph UNION ALL SELECT id FROM main.eph",
            &e,
        )
        .unwrap();
        assert_eq!(
            outcome.sql, None,
            "qualified names are not model references"
        );
        assert_eq!(
            outcome.target_refs,
            vec![("eph".to_string(), "main.eph".to_string())]
        );
    }

    #[test]
    fn rocky_allow_pragmas_survive_the_rewrite() {
        let e = map(&[("eph", eph("-- rocky-allow: QUALIFY\nSELECT 1 AS id", "eph"))]);
        let out = inline(
            "-- rocky-allow: NVL\n-- plain comment\nSELECT id FROM eph",
            &e,
        );
        assert!(
            out.starts_with("-- rocky-allow: NVL\n-- rocky-allow: QUALIFY\nWITH "),
            "{out}"
        );
        assert!(!out.contains("plain comment"), "{out}");
    }

    #[test]
    fn two_consumers_get_independent_copies() {
        let e = map(&[("eph", eph("SELECT 1 AS id", "eph"))]);
        let one = inline("SELECT id FROM eph", &e);
        let two = inline("SELECT COUNT(*) AS n FROM eph", &e);
        assert!(one.starts_with("WITH __rocky_ephemeral__eph AS (SELECT 1 AS id) "));
        assert!(two.starts_with("WITH __rocky_ephemeral__eph AS (SELECT 1 AS id) "));
    }

    #[test]
    fn references_inside_subqueries_are_rewritten() {
        let e = map(&[("eph", eph("SELECT 1 AS id", "eph"))]);
        let out = inline(
            "SELECT * FROM (SELECT id FROM eph) AS s WHERE id IN (SELECT id FROM eph)",
            &e,
        );
        assert_eq!(
            out.matches("FROM __rocky_ephemeral__eph AS eph").count(),
            2,
            "{out}"
        );
    }

    #[test]
    fn recursive_with_capturing_an_inlined_read_is_refused() {
        let e = map(&[("eph", eph("SELECT id FROM orders", "eph"))]);
        let err = inline_ephemeral_refs(
            "c",
            "WITH RECURSIVE orders AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM orders \
             WHERE id < 3) SELECT id FROM eph",
            &e,
        )
        .unwrap_err();
        assert!(matches!(err, InlineError::RecursiveCapture { ref name, .. } if name == "orders"));
    }

    #[test]
    fn recursive_with_without_a_clash_is_merged() {
        let e = map(&[("eph", eph("SELECT id FROM raw.t", "eph"))]);
        let out = inline(
            "WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE n < 3) \
             SELECT n FROM r JOIN eph ON eph.id = r.n",
            &e,
        );
        assert!(
            out.starts_with(
                "WITH RECURSIVE __rocky_ephemeral__eph AS (SELECT id FROM raw.t), r AS"
            ),
            "{out}"
        );
    }

    #[test]
    fn a_cycle_between_ephemerals_is_an_error() {
        let e = map(&[
            ("a", eph("SELECT id FROM b", "a")),
            ("b", eph("SELECT id FROM a", "b")),
        ]);
        let err = inline_ephemeral_refs("c", "SELECT id FROM a", &e).unwrap_err();
        assert!(matches!(err, InlineError::Cycle { .. }), "{err}");
    }

    #[test]
    fn a_body_that_does_not_parse_names_the_model() {
        let e = map(&[("eph", eph("SELEC nope", "eph"))]);
        let err = inline_ephemeral_refs("c", "SELECT 1 FROM eph", &e).unwrap_err();
        assert!(matches!(err, InlineError::Parse { ref model, .. } if model == "eph"));
    }
}
