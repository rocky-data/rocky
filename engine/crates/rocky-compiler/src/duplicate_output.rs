//! Repeated output column names (`E061`, `W002`).
//!
//! `SELECT a.id, b.id FROM a JOIN b ON …` gives the model two output columns
//! called `id`. A strategy that writes into the model's existing table cannot
//! tell them apart: DuckDB inserts by name and refuses the statement
//! (`Duplicate column name "id" in INSERT`) from the second run on, because
//! the first run's `CREATE TABLE AS` renamed the repeat to `id_1`. Those
//! strategies get the `E061` error. Every other strategy gets the `W002`
//! warning: the warehouse may rename the repeat or refuse the table.
//!
//! # Only fire when certain
//!
//! An error breaks a build, so the pass reads only names it can prove:
//!
//! - It reads the outermost projection only (the first branch of a `UNION`,
//!   which names the output). Sub-queries and CTEs are not checked.
//! - A projection item has a provable name only when it is aliased
//!   (`x AS id`) or is a bare or qualified column (`id`, `b.id`). Any other
//!   expression is named by the warehouse's own rules and is ignored.
//! - A star (`*`, `b.*`) is ignored: the columns it expands to are not known
//!   for certain here, and `JOIN … USING` changes them.
//! - Two unquoted names match ignoring case. Two quoted names match only when
//!   they are spelled the same. A quoted name never matches an unquoted one,
//!   because warehouses fold unquoted names differently (Snowflake folds to
//!   upper case, PostgreSQL to lower case).

use std::collections::HashMap;

use rocky_core::models::{Model, StrategyConfig};
use sqlparser::ast::{Expr, Ident, Query, SelectItem, SetExpr, Statement};
use sqlparser::parser::Parser;

use crate::diagnostic::{Diagnostic, E061, W002};

/// How a repeated output name is reported for a strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Severity {
    /// The strategy writes into the model's existing table, so the repeat
    /// breaks the write (`E061`).
    Error,
    /// The strategy rebuilds or defines the relation (`W002`).
    Warning,
}

/// The severity for a strategy. The match is exhaustive on purpose: a new
/// strategy must decide whether it writes into an existing table.
fn severity_for(strategy: &StrategyConfig) -> Severity {
    match strategy {
        // `microbatch` is an alias of `time_interval`; the loader rewrites it
        // before the compiler sees it.
        StrategyConfig::Incremental { .. }
        | StrategyConfig::DeleteInsert { .. }
        | StrategyConfig::TimeInterval { .. }
        | StrategyConfig::Microbatch { .. }
        | StrategyConfig::Merge { .. } => Severity::Error,
        StrategyConfig::FullRefresh
        | StrategyConfig::Ephemeral
        | StrategyConfig::View
        | StrategyConfig::MaterializedView
        | StrategyConfig::DynamicTable { .. }
        | StrategyConfig::ContentAddressed { .. }
        | StrategyConfig::Snapshot { .. } => Severity::Warning,
    }
}

/// One diagnostic per output name the model's projection repeats: `E061` for
/// a strategy that writes into the existing table, `W002` otherwise.
pub fn check_duplicate_output_names(model: &Model) -> Vec<Diagnostic> {
    let repeated = repeated_output_names(&model.sql);
    if repeated.is_empty() {
        return Vec::new();
    }
    let strategy = &model.config.strategy;
    let kind = rocky_core::selector::strategy_kind(strategy);
    let model_name = model.config.name.as_str();
    repeated
        .into_iter()
        .map(|(name, count)| {
            let suggestion = format!(
                "give each '{name}' column its own name with an alias, for example \
                 'b.{name} AS b_{name}'"
            );
            match severity_for(strategy) {
                Severity::Error => Diagnostic::error(
                    E061,
                    model_name,
                    format!(
                        "the model's output has {count} columns called '{name}'. A '{kind}' \
                         model writes into its existing table, and that write cannot tell the \
                         columns apart"
                    ),
                ),
                Severity::Warning => Diagnostic::warning(
                    W002,
                    model_name,
                    format!(
                        "the model's output has {count} columns called '{name}'. The warehouse \
                         may rename the repeat or refuse the table"
                    ),
                ),
            }
            .with_suggestion(suggestion)
        })
        .collect()
}

/// The provable output names the outermost projection repeats, with how
/// often each appears, in projection order. Unparsable SQL repeats nothing.
fn repeated_output_names(sql: &str) -> Vec<(String, usize)> {
    let Ok(statements) = Parser::parse_sql(&rocky_sql::dialect::DatabricksDialect, sql) else {
        return Vec::new();
    };
    let [Statement::Query(query)] = statements.as_slice() else {
        return Vec::new();
    };
    let Some(projection) = output_projection(query) else {
        return Vec::new();
    };

    // Key: (quoted, folded name). Value: (first spelling, count).
    let mut counts: HashMap<(bool, String), (String, usize)> = HashMap::new();
    let mut order: Vec<(bool, String)> = Vec::new();
    for ident in projection.iter().flat_map(provable_names) {
        let key = match ident.quote_style {
            Some(_) => (true, ident.value.clone()),
            None => (false, ident.value.to_lowercase()),
        };
        let entry = counts.entry(key.clone()).or_insert_with(|| {
            order.push(key);
            (ident.value.clone(), 0)
        });
        entry.1 += 1;
    }
    order
        .into_iter()
        .filter_map(|key| counts.remove(&key))
        .filter(|(_, count)| *count > 1)
        .collect()
}

/// The projection that names the query's output: the outermost `SELECT`, or
/// the first branch of a set operation.
fn output_projection(query: &Query) -> Option<&[SelectItem]> {
    let mut body = query.body.as_ref();
    loop {
        match body {
            SetExpr::Select(select) => return Some(&select.projection),
            SetExpr::Query(inner) => body = inner.body.as_ref(),
            SetExpr::SetOperation { left, .. } => body = left.as_ref(),
            SetExpr::Values(_)
            | SetExpr::Insert(_)
            | SetExpr::Update(_)
            | SetExpr::Delete(_)
            | SetExpr::Merge(_)
            | SetExpr::Table(_) => return None,
        }
    }
}

/// The output names of one projection item that do not depend on the
/// warehouse's naming rules.
fn provable_names(item: &SelectItem) -> Vec<&Ident> {
    match item {
        SelectItem::ExprWithAlias { alias, .. } => vec![alias],
        SelectItem::ExprWithAliases { aliases, .. } => aliases.iter().collect(),
        SelectItem::UnnamedExpr(Expr::Identifier(ident)) => vec![ident],
        SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) => {
            parts.last().into_iter().collect()
        }
        SelectItem::UnnamedExpr(_)
        | SelectItem::QualifiedWildcard(..)
        | SelectItem::Wildcard(_) => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn repeated(sql: &str) -> Vec<(String, usize)> {
        repeated_output_names(sql)
    }

    #[test]
    fn qualified_columns_with_one_name_repeat() {
        assert_eq!(
            repeated("SELECT a.id, b.id FROM a JOIN b ON a.k = b.k"),
            [("id".to_string(), 2)]
        );
    }

    #[test]
    fn an_alias_and_a_column_with_one_name_repeat() {
        assert_eq!(
            repeated("SELECT id, other AS ID, x FROM t"),
            [("id".to_string(), 2)]
        );
    }

    #[test]
    fn distinct_names_do_not_repeat() {
        assert!(repeated("SELECT a.id, b.id AS b_id FROM a JOIN b ON a.k = b.k").is_empty());
    }

    #[test]
    fn stars_and_unnamed_expressions_are_ignored() {
        assert!(repeated("SELECT *, id FROM t").is_empty());
        assert!(repeated("SELECT a.*, b.* FROM a JOIN b ON a.id = b.id").is_empty());
        assert!(repeated("SELECT a, CAST(a AS INT), UPPER(a) FROM t").is_empty());
    }

    #[test]
    fn a_quoted_name_matches_only_its_own_spelling() {
        assert!(repeated(r#"SELECT "id", ID FROM t"#).is_empty());
        assert!(repeated(r#"SELECT "Id", "id" FROM t"#).is_empty());
        assert_eq!(
            repeated(r#"SELECT "id" AS "id", t."id" FROM t"#),
            [("id".to_string(), 2)]
        );
    }

    #[test]
    fn only_the_outermost_projection_counts() {
        assert!(
            repeated("WITH c AS (SELECT a.id, b.id FROM a JOIN b ON a.k = b.k) SELECT 1 AS id")
                .is_empty()
        );
        assert!(repeated("SELECT x FROM (SELECT id, id FROM t) AS s").is_empty());
        assert_eq!(
            repeated("SELECT id, id FROM t UNION ALL SELECT a, b FROM u"),
            [("id".to_string(), 2)]
        );
    }
}
