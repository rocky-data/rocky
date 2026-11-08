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
//! - Names match by the rules of the warehouse the model runs on
//!   ([`NameFold`]). DuckDB, Databricks, BigQuery and Trino compare every
//!   name ignoring case, quoted or not. Snowflake folds an unquoted name to
//!   upper case and keeps a quoted one as spelled. PostgreSQL and Redshift
//!   fold an unquoted name to lower case and keep a quoted one as spelled.
//!   With no known target, or on SQL Server, two unquoted names match
//!   ignoring case, two quoted names match only when spelled the same, and a
//!   quoted name never matches an unquoted one. A model that runs on several
//!   warehouses is checked under each rule. "Ignoring case" folds ASCII
//!   letters only; a non-ASCII letter compares exactly.

use std::collections::HashMap;

use rocky_core::models::{Model, StrategyConfig};
use sqlparser::ast::{Expr, Ident, Query, SelectItem, SetExpr, Statement};
use sqlparser::parser::Parser;

use crate::diagnostic::{Diagnostic, E061, W002};
use crate::operand_check::{OperandDialect, OperandTarget};

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

/// How a warehouse decides that two output names are the same column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NameFold {
    /// Every name, quoted or not, compares ignoring case: DuckDB, Databricks,
    /// BigQuery, Trino.
    AllIgnoreCase,
    /// An unquoted name folds to upper case; a quoted one stays as spelled:
    /// Snowflake. With `QUOTED_IDENTIFIERS_IGNORE_CASE = TRUE` more names
    /// collide, never fewer, so this rule never reports a false repeat.
    UnquotedUpper,
    /// An unquoted name folds to lower case; a quoted one stays as spelled:
    /// PostgreSQL, and Redshift. Redshift by default folds quoted names too
    /// (`enable_case_sensitive_identifier = false`), so this rule can miss a
    /// Redshift repeat but never reports a false one.
    UnquotedLower,
    /// No known rule: unquoted names compare ignoring case, quoted names by
    /// spelling, and a quoted name never matches an unquoted one.
    Conservative,
}

impl NameFold {
    /// The rule for a dialect. Exhaustive on purpose: a new dialect must
    /// decide how it compares names.
    fn for_dialect(dialect: OperandDialect) -> Self {
        match dialect {
            OperandDialect::DuckDb
            | OperandDialect::Databricks
            | OperandDialect::BigQuery
            | OperandDialect::Trino => Self::AllIgnoreCase,
            OperandDialect::Snowflake => Self::UnquotedUpper,
            OperandDialect::Postgres | OperandDialect::Redshift => Self::UnquotedLower,
            // Name comparison follows the database collation.
            OperandDialect::SqlServer => Self::Conservative,
        }
    }

    /// The rules to check a model under: one per target dialect, or the
    /// conservative rule when no target dialect is known (ClickHouse and
    /// other unruled adapters included).
    fn for_target(target: &OperandTarget) -> Vec<Self> {
        let mut folds: Vec<Self> = Vec::new();
        match target {
            OperandTarget::Targets { dialects, .. } => {
                for fold in dialects.iter().copied().map(Self::for_dialect) {
                    if !folds.contains(&fold) {
                        folds.push(fold);
                    }
                }
            }
            OperandTarget::Unconfigured => {}
        }
        if folds.is_empty() {
            folds.push(Self::Conservative);
        }
        folds
    }

    /// The identity of a name under this rule: (in the separate quoted
    /// namespace, folded name).
    ///
    /// Case folds only ASCII letters. A warehouse's fold of a non-ASCII
    /// letter can differ from Unicode's (`ß` upper-cases to `SS`, the
    /// Kelvin sign U+212A lower-cases to `k`), so a
    /// full Unicode fold could invent a repeat. Comparing non-ASCII letters
    /// exactly can miss a repeat but never reports a false one.
    fn key(self, ident: &Ident) -> (bool, String) {
        let quoted = ident.quote_style.is_some();
        let value = &ident.value;
        match self {
            Self::AllIgnoreCase => (false, value.to_ascii_lowercase()),
            Self::UnquotedUpper if quoted => (false, value.clone()),
            Self::UnquotedUpper => (false, value.to_ascii_uppercase()),
            Self::UnquotedLower if quoted => (false, value.clone()),
            Self::UnquotedLower => (false, value.to_ascii_lowercase()),
            Self::Conservative if quoted => (true, value.clone()),
            Self::Conservative => (false, value.to_ascii_lowercase()),
        }
    }
}

/// One diagnostic per output name the model's projection repeats: `E061` for
/// a strategy that writes into the existing table, `W002` otherwise. Names
/// match by the rules of each warehouse in `target`.
pub fn check_duplicate_output_names(model: &Model, target: &OperandTarget) -> Vec<Diagnostic> {
    let repeated = repeated_output_names(&model.sql, &NameFold::for_target(target));
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

/// The provable output names the outermost projection repeats under any of
/// `folds`, with how often each appears, in projection order. Unparsable SQL
/// repeats nothing.
fn repeated_output_names(sql: &str, folds: &[NameFold]) -> Vec<(String, usize)> {
    let Ok(statements) = Parser::parse_sql(&rocky_sql::dialect::DatabricksDialect, sql) else {
        return Vec::new();
    };
    let [Statement::Query(query)] = statements.as_slice() else {
        return Vec::new();
    };
    let Some(projection) = output_projection(query) else {
        return Vec::new();
    };
    let names: Vec<&Ident> = projection.iter().flat_map(provable_names).collect();

    // One entry per repeat, merged across the rules by the case-blind name so
    // a repeat that several rules see is reported once.
    let mut repeated: Vec<(String, usize)> = Vec::new();
    for fold in folds {
        for (name, count) in repeated_under(&names, *fold) {
            match repeated
                .iter_mut()
                .find(|(seen, _)| seen.eq_ignore_ascii_case(&name))
            {
                Some(entry) => entry.1 = entry.1.max(count),
                None => repeated.push((name, count)),
            }
        }
    }
    repeated
}

/// The names that repeat under one rule, with their counts, in order.
fn repeated_under(names: &[&Ident], fold: NameFold) -> Vec<(String, usize)> {
    // Key: the rule's identity. Value: (first spelling, count).
    let mut counts: HashMap<(bool, String), (String, usize)> = HashMap::new();
    let mut order: Vec<(bool, String)> = Vec::new();
    for ident in names {
        let key = fold.key(ident);
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

    /// Repeats with no known target dialect.
    fn repeated(sql: &str) -> Vec<(String, usize)> {
        repeated_output_names(sql, &NameFold::for_target(&OperandTarget::Unconfigured))
    }

    /// Repeats for a model that runs on `dialect`.
    fn repeated_on(dialect: OperandDialect, sql: &str) -> Vec<(String, usize)> {
        repeated_output_names(sql, &NameFold::for_target(&Some(dialect).into()))
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
    fn with_no_known_dialect_a_quoted_name_matches_only_its_own_spelling() {
        assert!(repeated(r#"SELECT "id", ID FROM t"#).is_empty());
        assert!(repeated(r#"SELECT "Id", "id" FROM t"#).is_empty());
        assert_eq!(
            repeated(r#"SELECT "id" AS "id", t."id" FROM t"#),
            [("id".to_string(), 2)]
        );
    }

    #[test]
    fn duckdb_databricks_bigquery_and_trino_ignore_case_even_when_quoted() {
        for dialect in [
            OperandDialect::DuckDb,
            OperandDialect::Databricks,
            OperandDialect::BigQuery,
            OperandDialect::Trino,
        ] {
            assert_eq!(
                repeated_on(dialect, r#"SELECT a.id, b."ID" FROM a JOIN b ON a.k = b.k"#),
                [("id".to_string(), 2)],
                "{dialect:?}"
            );
            assert_eq!(
                repeated_on(dialect, r#"SELECT "Id", "id" FROM t"#),
                [("Id".to_string(), 2)],
                "{dialect:?}"
            );
        }
    }

    #[test]
    fn snowflake_folds_unquoted_names_to_upper_case() {
        let snowflake = OperandDialect::Snowflake;
        assert_eq!(
            repeated_on(snowflake, r#"SELECT "ID", id FROM t"#),
            [("ID".to_string(), 2)]
        );
        assert!(repeated_on(snowflake, r#"SELECT "id", ID FROM t"#).is_empty());
        assert!(repeated_on(snowflake, r#"SELECT "Id", "id" FROM t"#).is_empty());
    }

    #[test]
    fn postgres_and_redshift_fold_unquoted_names_to_lower_case() {
        for dialect in [OperandDialect::Postgres, OperandDialect::Redshift] {
            assert_eq!(
                repeated_on(dialect, r#"SELECT "id", ID FROM t"#),
                [("id".to_string(), 2)],
                "{dialect:?}"
            );
            assert!(
                repeated_on(dialect, r#"SELECT "ID", id FROM t"#).is_empty(),
                "{dialect:?}"
            );
        }
    }

    #[test]
    fn sql_server_keeps_the_conservative_rule() {
        let sql_server = OperandDialect::SqlServer;
        assert!(repeated_on(sql_server, r#"SELECT "ID", id FROM t"#).is_empty());
        assert_eq!(
            repeated_on(sql_server, "SELECT ID, id FROM t"),
            [("ID".to_string(), 2)]
        );
    }

    #[test]
    fn case_folds_only_ascii_letters() {
        // Unicode lower-cases the Kelvin sign (U+212A) to `k`; a warehouse
        // need not. Comparing non-ASCII letters exactly keeps this from a
        // false repeat.
        for dialect in [
            OperandDialect::DuckDb,
            OperandDialect::Databricks,
            OperandDialect::BigQuery,
            OperandDialect::Trino,
        ] {
            assert!(
                repeated_on(dialect, "SELECT k, \"\u{212A}\" FROM t").is_empty(),
                "{dialect:?}"
            );
        }
        // ASCII letters still fold.
        assert_eq!(
            repeated_on(OperandDialect::DuckDb, r#"SELECT id, "ID" FROM t"#),
            [("id".to_string(), 2)]
        );
    }

    #[test]
    fn unquoted_folds_touch_only_ascii_letters() {
        // The SQL parser refuses a non-ASCII letter in an unquoted name
        // today, so these folds see only ASCII. Checked on the key directly,
        // so a wider parser cannot bring the Unicode fold back: Unicode
        // upper-cases `ß` to `SS`, which would make Snowflake's unquoted
        // `straße` match a quoted `"STRASSE"`.
        let snowflake = NameFold::UnquotedUpper;
        assert_ne!(
            snowflake.key(&Ident::new("straße")),
            snowflake.key(&Ident::with_quote('"', "STRASSE"))
        );
        for fold in [NameFold::UnquotedLower, NameFold::Conservative] {
            assert_eq!(
                fold.key(&Ident::new("\u{212A}")),
                (false, "\u{212A}".to_string()),
                "{fold:?}"
            );
        }
        assert_eq!(snowflake.key(&Ident::new("id")), (false, "ID".to_string()));
    }

    #[test]
    fn a_model_on_several_warehouses_is_checked_under_each_rule() {
        let target = OperandTarget::Targets {
            dialects: vec![OperandDialect::Snowflake, OperandDialect::DuckDb],
            unruled: Vec::new(),
        };
        assert_eq!(
            repeated_output_names(r#"SELECT "id", ID FROM t"#, &NameFold::for_target(&target)),
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
