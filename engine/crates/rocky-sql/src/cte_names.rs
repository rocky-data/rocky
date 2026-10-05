//! Give every CTE in a statement a distinct name, renaming in scope.
//!
//! Standard SQL scopes a CTE to its own query, so two nested queries may each
//! define `final`:
//!
//! ```sql
//! WITH a AS (WITH final AS (SELECT 1 AS id) SELECT * FROM final),
//!      final AS (SELECT * FROM a)
//! SELECT * FROM final
//! ```
//!
//! A dialect that accepts `WITH` only at the head of a statement (T-SQL) must
//! lift every nested CTE into one list, and one list cannot hold two `final`s.
//! [`uniquify_cte_names`] renames the nested one (`final__2`) and rewrites the
//! references that bind to it — only those, found by the same scope rules the
//! ephemeral inliner uses — so the lifted list is unambiguous and every
//! reference still reads the CTE it read before.
//!
//! The rewrite walks the parsed AST, not the text:
//!
//! - The outermost query's CTEs keep their names. A nested CTE is renamed
//!   when an earlier CTE (in walk order) already holds its name, or when the
//!   statement reads a TABLE by that bare name somewhere it is not in scope
//!   (lifting it would capture that read).
//! - The new name is `<name>__<n>`, the smallest `n >= 2` that no CTE,
//!   relation part or alias in the statement uses.
//! - A rewritten reference with no alias gets the CTE's old name as its alias,
//!   so a qualified column such as `final.id` still binds.
//! - Names compare case-insensitively (SQL Server's default collations).
//!
//! The AST round trip drops comments and normalizes whitespace, so callers use
//! it only when the statement cannot be lifted as written.

use std::collections::HashSet;
use std::ops::ControlFlow;

use sqlparser::ast::{
    Ident, ObjectName, Query, Statement, TableAlias, TableFactor, VisitMut, VisitorMut,
};
use sqlparser::dialect::Dialect;
use sqlparser::parser::Parser;

use crate::defer::{CteScopeStack, IdentifierCaseRules, RecursiveCteVisibility};
use crate::ephemeral::collect_names;
use crate::parser::ParseError;

/// Parse `sql` with `dialect` and rename nested CTEs so that no two CTEs in
/// the statement share a name (see the module docs).
///
/// `Ok(None)` when no CTE needed a new name, or the statement is not a
/// query — the input is then left exactly as written. `Ok(Some(sql))` is the
/// re-serialized statement.
///
/// # Errors
///
/// [`ParseError`] when `sql` is not exactly one statement in `dialect`.
pub fn uniquify_cte_names(sql: &str, dialect: &dyn Dialect) -> Result<Option<String>, ParseError> {
    let mut statements = Parser::parse_sql(dialect, sql)?;
    match statements.len() {
        0 => return Err(ParseError::EmptyInput),
        1 => {}
        n => return Err(ParseError::MultipleStatements(n)),
    }
    let mut statement = statements.swap_remove(0);
    let Statement::Query(query) = &mut statement else {
        return Ok(None);
    };

    let mut used = HashSet::new();
    collect_names(query, &mut used);

    let mut free = FreeRelations {
        scopes: scope_stack(),
        free: HashSet::new(),
    };
    let _: ControlFlow<()> = VisitMut::visit(query.as_mut(), &mut free);

    let mut renamer = Renamer {
        scopes: scope_stack(),
        names: Vec::new(),
        taken: HashSet::new(),
        free: free.free,
        used,
        changed: false,
    };
    let _: ControlFlow<()> = VisitMut::visit(query.as_mut(), &mut renamer);
    Ok(renamer.changed.then(|| statement.to_string()))
}

/// T-SQL scoping: case-insensitive, and a CTE always sees its own name.
fn scope_stack() -> CteScopeStack {
    CteScopeStack::new(
        IdentifierCaseRules::all_insensitive(),
        RecursiveCteVisibility::PrecedingAndSelf,
    )
    .with_implicit_recursion()
}

fn key(ident: &Ident) -> String {
    ident.value.to_lowercase()
}

/// A bare, single-part relation with no table-function arguments.
fn bare_relation(factor: &mut TableFactor) -> Option<(&mut ObjectName, &mut Option<TableAlias>)> {
    let TableFactor::Table {
        name, alias, args, ..
    } = factor
    else {
        return None;
    };
    if args.is_some() || name.0.len() != 1 || name.0[0].as_ident().is_none() {
        return None;
    }
    Some((name, alias))
}

/// Pass 1: the bare relation names that bind to no CTE — tables.
struct FreeRelations {
    scopes: CteScopeStack,
    free: HashSet<String>,
}

impl VisitorMut for FreeRelations {
    type Break = ();

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<()> {
        self.scopes.enter_query(query);
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &mut Query) -> ControlFlow<()> {
        self.scopes.exit_query();
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, factor: &mut TableFactor) -> ControlFlow<()> {
        if let Some((name, _)) = bare_relation(factor)
            && let Some(ident) = name.0[0].as_ident()
            && self
                .scopes
                .resolve(&ident.value, ident.quote_style.is_some())
                .is_none()
        {
            self.free.insert(key(ident));
        }
        ControlFlow::Continue(())
    }
}

/// Pass 2: assign each CTE its final name and rewrite what binds to it.
struct Renamer {
    scopes: CteScopeStack,
    /// Per open query, the final name of each of its CTEs (`None` = kept).
    names: Vec<Vec<Option<String>>>,
    /// Lower-cased CTE names already assigned.
    taken: HashSet<String>,
    free: HashSet<String>,
    /// Every CTE alias, relation part and alias in the statement, lower-cased.
    used: HashSet<String>,
    changed: bool,
}

impl Renamer {
    fn fresh(&mut self, base: &str) -> String {
        (2u32..)
            .map(|n| format!("{base}__{n}"))
            .find(|candidate| {
                let k = candidate.to_lowercase();
                !self.taken.contains(&k) && !self.used.contains(&k)
            })
            .unwrap_or_else(|| format!("{base}__rocky"))
    }
}

impl VisitorMut for Renamer {
    type Break = ();

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<()> {
        // Scope lookups use the names as written, so enter first, rename after.
        let outermost = self.scopes.depth() == 0;
        self.scopes.enter_query(query);
        let mut names = Vec::new();
        if let Some(with) = &mut query.with {
            for cte in &mut with.cte_tables {
                let alias = &mut cte.alias.name;
                let k = key(alias);
                let collides = self.taken.contains(&k) || self.free.contains(&k);
                if outermost || !collides {
                    self.taken.insert(k);
                    names.push(None);
                    continue;
                }
                let new = self.fresh(&alias.value);
                self.taken.insert(new.to_lowercase());
                alias.value.clone_from(&new);
                names.push(Some(new));
                self.changed = true;
            }
        }
        self.names.push(names);
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &mut Query) -> ControlFlow<()> {
        self.scopes.exit_query();
        self.names.pop();
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, factor: &mut TableFactor) -> ControlFlow<()> {
        let Some((name, alias)) = bare_relation(factor) else {
            return ControlFlow::Continue(());
        };
        let Some(ident) = name.0[0].as_ident().cloned() else {
            return ControlFlow::Continue(());
        };
        let Some((depth, index)) = self
            .scopes
            .resolve(&ident.value, ident.quote_style.is_some())
        else {
            return ControlFlow::Continue(());
        };
        let Some(Some(new)) = self.names.get(depth).and_then(|n| n.get(index)) else {
            return ControlFlow::Continue(());
        };
        let mut renamed = ident.clone();
        renamed.value.clone_from(new);
        *name = ObjectName::from(vec![renamed]);
        if alias.is_none() {
            *alias = Some(TableAlias {
                explicit: true,
                name: ident,
                columns: Vec::new(),
                at: None,
            });
        }
        ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::dialect::MsSqlDialect;

    use super::*;

    fn uniquify(sql: &str) -> Option<String> {
        uniquify_cte_names(sql, &MsSqlDialect {}).expect("parses")
    }

    #[test]
    fn distinct_names_are_left_untouched() {
        assert_eq!(
            uniquify(
                "WITH a AS (SELECT 1 AS v) SELECT * FROM (WITH b AS (SELECT v FROM a) SELECT v FROM b) AS s"
            ),
            None
        );
    }

    #[test]
    fn nested_duplicates_are_renamed_in_their_own_scope() {
        let sql = "WITH a AS (WITH final AS (SELECT 1 AS id) SELECT * FROM final), \
                   b AS (WITH final AS (SELECT 2 AS id) SELECT final.id FROM final), \
                   final AS (SELECT a.id FROM a JOIN b ON a.id = b.id) SELECT * FROM final";
        assert_eq!(
            uniquify(sql).unwrap(),
            "WITH a AS (WITH final__2 AS (SELECT 1 AS id) SELECT * FROM final__2 AS final), \
             b AS (WITH final__3 AS (SELECT 2 AS id) SELECT final.id FROM final__3 AS final), \
             final AS (SELECT a.id FROM a JOIN b ON a.id = b.id) SELECT * FROM final"
        );
    }

    #[test]
    fn a_nested_cte_named_like_a_table_read_elsewhere_is_renamed() {
        let sql = "SELECT * FROM x JOIN (WITH x AS (SELECT 2 AS v) SELECT v FROM x) AS s ON 1 = 1";
        assert_eq!(
            uniquify(sql).unwrap(),
            "SELECT * FROM x JOIN (WITH x__2 AS (SELECT 2 AS v) SELECT v FROM x__2 AS x) AS s ON 1 = 1"
        );
    }

    #[test]
    fn the_new_name_avoids_every_name_in_the_statement_and_keeps_quotes() {
        let sql = "WITH [final] AS (SELECT 1 AS id), final__2 AS (SELECT 2 AS id) \
                   SELECT * FROM (WITH [Final] AS (SELECT id FROM final__2) SELECT id FROM [FINAL]) AS s";
        assert_eq!(
            uniquify(sql).unwrap(),
            "WITH [final] AS (SELECT 1 AS id), final__2 AS (SELECT 2 AS id) \
             SELECT * FROM (WITH [Final__3] AS (SELECT id FROM final__2) SELECT id FROM [Final__3] AS [FINAL]) AS s"
        );
    }

    #[test]
    fn a_shadowed_outer_reference_is_not_rewritten() {
        // `final` in the outer body reads the OUTER cte; only the inner read
        // moves to the renamed one.
        let sql = "WITH final AS (SELECT 1 AS id) \
                   SELECT * FROM final JOIN (WITH final AS (SELECT 2 AS id) SELECT id FROM final) AS s ON 1 = 1";
        assert_eq!(
            uniquify(sql).unwrap(),
            "WITH final AS (SELECT 1 AS id) \
             SELECT * FROM final JOIN (WITH final__2 AS (SELECT 2 AS id) SELECT id FROM final__2 AS final) AS s ON 1 = 1"
        );
    }

    #[test]
    fn a_self_referencing_nested_cte_keeps_its_recursion() {
        let sql = "WITH n AS (SELECT 1 AS v) SELECT * FROM (WITH n AS (SELECT 1 AS v UNION ALL SELECT v + 1 FROM n WHERE v < 3) SELECT v FROM n) AS s";
        assert_eq!(
            uniquify(sql).unwrap(),
            "WITH n AS (SELECT 1 AS v) SELECT * FROM (WITH n__2 AS (SELECT 1 AS v UNION ALL SELECT v + 1 FROM n__2 AS n WHERE v < 3) SELECT v FROM n__2 AS n) AS s"
        );
    }

    #[test]
    fn not_one_query_is_left_alone_or_refused() {
        assert_eq!(uniquify("DELETE FROM t"), None);
        assert!(uniquify_cte_names("SELECT 1; SELECT 2", &MsSqlDialect {}).is_err());
        assert!(uniquify_cte_names("SELECT (", &MsSqlDialect {}).is_err());
    }
}
