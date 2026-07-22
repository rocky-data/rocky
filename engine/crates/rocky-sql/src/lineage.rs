use std::collections::HashMap;
use std::fmt;

use serde::{Deserialize, Serialize};
use sqlparser::ast::{
    CastKind, Expr, Query, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins,
};
use sqlparser::parser::Parser;

use crate::dialect::DatabricksDialect;

/// How a column value is transformed from source to target.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TransformKind {
    /// Direct column reference (no transformation).
    Direct,
    /// Infallible type cast (`CAST(...)` or `expr :: type`). Preserves the
    /// input's nullability — a non-null input yields a non-null output.
    Cast,
    /// Fallible type cast (`TRY_CAST(...)` / `SAFE_CAST(...)`) that returns
    /// `NULL` when the conversion fails. The output is nullable regardless of
    /// the input's nullability.
    TryCast,
    /// Aggregate function (SUM, COUNT, etc.).
    Aggregation(String),
    /// Complex expression (arithmetic, CASE, etc.).
    Expression,
}

impl TransformKind {
    /// Whether this edge is a type cast — either the infallible [`Cast`] form or
    /// the fallible [`TryCast`] form. The two differ only in output nullability;
    /// both let the compiler recover the cast's target type from the SQL.
    ///
    /// [`Cast`]: TransformKind::Cast
    /// [`TryCast`]: TransformKind::TryCast
    #[must_use]
    pub fn is_cast(&self) -> bool {
        matches!(self, TransformKind::Cast | TransformKind::TryCast)
    }
}

impl fmt::Display for TransformKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransformKind::Direct => write!(f, "direct"),
            TransformKind::Cast => write!(f, "cast"),
            TransformKind::TryCast => write!(f, "try_cast"),
            TransformKind::Aggregation(func) => write!(f, "aggregation: {}", func.to_lowercase()),
            TransformKind::Expression => write!(f, "expression"),
        }
    }
}

/// A column lineage edge: source_table.column → target alias.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ColumnLineage {
    /// Source table (or alias) the column comes from.
    pub source_table: Option<String>,
    /// Source column name.
    pub source_column: String,
    /// Target column name (alias or original).
    pub target_column: String,
    /// How the column is transformed.
    pub transform: TransformKind,
}

/// Full lineage result for a SQL statement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineageResult {
    /// Tables referenced in FROM/JOIN clauses.
    pub source_tables: Vec<TableReference>,
    /// Column-level lineage edges.
    pub columns: Vec<ColumnLineage>,
    /// Whether SELECT * was used (lineage is partial).
    pub has_star: bool,
    /// How many projection items did **not** yield a column entry whose
    /// `target_column` is the item's true output name.
    ///
    /// [`Self::columns`] is best-effort: it is a lineage graph first and an
    /// output-schema enumeration second. Two kinds of projection item leave it
    /// short of the real schema:
    ///
    /// - an unnamed expression the extractor cannot trace at all
    ///   (`SELECT (order_id)` — a parenthesised expression, `SELECT 1`), which
    ///   contributes no entry;
    /// - an unnamed expression it *can* trace to an upstream column
    ///   (`SELECT UPPER(name)`, `SELECT CAST(x AS INT)`), which contributes an
    ///   entry named after the traced *source* column. That name is the lineage
    ///   answer, not the output-schema answer: the warehouse names such a
    ///   column by its own rules, which Rocky does not model.
    ///
    /// Both are counted here. A consumer that needs the model's *complete*
    /// output column set — rather than whatever lineage could recover — must
    /// treat `columns` as authoritative only when this is `0` (and
    /// [`Self::has_star`] is `false`). Aliased items (`expr AS name`) always
    /// yield a correctly named entry and are never counted, which is why the
    /// overwhelmingly common explicit-projection model stays fully checkable.
    ///
    /// `#[serde(default)]` so a `LineageResult` cached by an older build
    /// deserializes as "fully understood" rather than failing to load. That is
    /// the pre-existing behaviour for such caches, not a new risk.
    #[serde(default)]
    pub unresolved_projections: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableReference {
    /// Full table name (e.g., "catalog.schema.table").
    pub name: String,
    /// Alias if any.
    pub alias: Option<String>,
    /// Output column names of a derived table (subquery in the `FROM` clause),
    /// when they can be determined statically — i.e. the subquery does not
    /// itself project a `SELECT *`. `None` for plain table references and for
    /// subqueries whose column set can't be enumerated (e.g. the inner query
    /// is itself a `SELECT *`, or a nested derived table that doesn't expand).
    ///
    /// This lets `SELECT * FROM (<subquery>) AS alias` resolve to the inner
    /// query's projected columns instead of an empty schema.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub derived_columns: Option<Vec<String>>,
    /// Source table names referenced inside a derived table (subquery in the
    /// `FROM` clause) whose own projection is an unresolved `SELECT *`.
    ///
    /// When [`Self::derived_columns`] can't be enumerated because the inner
    /// query is itself `SELECT * FROM <up>`, this carries `<up>` so a consumer
    /// that owns the model/source graph (e.g. the semantic-graph builder) can
    /// resolve the inner star transitively. Empty for plain table references
    /// and for derived tables that already resolved their columns.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub derived_sources: Vec<String>,
}

/// Extracts the names of tables referenced in FROM/JOIN clauses.
///
/// Returns unique, lowercased table names. Useful for auto-deriving
/// `depends_on` from SQL model bodies: the returned names can be
/// intersected with known model names to produce implicit dependencies
/// without requiring explicit `depends_on` declarations in sidecar TOMLs.
///
/// Subqueries return the alias `(subquery)` which callers should filter
/// out (it can't match a model name).
pub fn referenced_tables(sql: &str) -> Result<Vec<String>, String> {
    let result = extract_lineage(sql)?;
    let mut names: Vec<String> = result
        .source_tables
        .iter()
        .map(|t| t.name.to_lowercase())
        .filter(|n| n != "(subquery)")
        .collect();
    names.sort();
    names.dedup();
    Ok(names)
}

/// Extracts column-level lineage from a SQL SELECT statement.
///
/// Parses the SQL and traces which columns from which tables
/// are selected or aliased in the output.
pub fn extract_lineage(sql: &str) -> Result<LineageResult, String> {
    let dialect = DatabricksDialect;
    let statements = Parser::parse_sql(&dialect, sql).map_err(|e| e.to_string())?;

    let stmt = statements.first().ok_or_else(|| "empty SQL".to_string())?;

    match stmt {
        Statement::Query(query) => extract_query_lineage(query),
        _ => Err("lineage extraction only supports SELECT statements".to_string()),
    }
}

fn extract_query_lineage(query: &Query) -> Result<LineageResult, String> {
    match query.body.as_ref() {
        SetExpr::Select(select) => {
            let source_tables = extract_tables(&select.from);
            let alias_map = build_alias_map(&source_tables);
            let (columns, has_star, unresolved_projections) =
                extract_select_columns(&select.projection, &alias_map, &source_tables);

            Ok(LineageResult {
                source_tables,
                columns,
                has_star,
                unresolved_projections,
            })
        }
        SetExpr::Query(inner) => extract_query_lineage(inner),
        _ => Err("unsupported query type for lineage".to_string()),
    }
}

fn extract_tables(from: &[TableWithJoins]) -> Vec<TableReference> {
    let mut tables = Vec::new();

    for table_with_joins in from {
        extract_table_factor(&table_with_joins.relation, &mut tables);
        for join in &table_with_joins.joins {
            extract_table_factor(&join.relation, &mut tables);
        }
    }

    tables
}

fn extract_table_factor(factor: &TableFactor, tables: &mut Vec<TableReference>) {
    match factor {
        TableFactor::Table { name, alias, .. } => {
            tables.push(TableReference {
                name: name.to_string(),
                alias: alias.as_ref().map(|a| a.name.value.clone()),
                derived_columns: None,
                derived_sources: Vec::new(),
            });
        }
        TableFactor::Derived {
            subquery,
            alias: Some(a),
            ..
        } => {
            // Resolve the subquery's output columns so that an outer
            // `SELECT *` over this derived table can expand to them. We can
            // only do this when the inner query enumerates its columns — an
            // inner `SELECT *` (or a nested derived table that doesn't expand)
            // leaves `has_star = true` with no individual columns, in which
            // case we fall back to `None`.
            let inner = extract_query_lineage(subquery).ok();
            let derived_columns = inner.as_ref().and_then(|inner| {
                if inner.has_star || inner.columns.is_empty() {
                    None
                } else {
                    Some(
                        inner
                            .columns
                            .iter()
                            .map(|c| c.target_column.clone())
                            .collect(),
                    )
                }
            });
            // When the inner query is an unresolved `SELECT *` (so its columns
            // can't be enumerated here), keep the inner source-table names so a
            // model/source-graph-aware consumer can resolve the star
            // transitively. e.g. the importer's microbatch wrapper
            // `SELECT * FROM (SELECT * FROM up) AS _rocky_microbatch`.
            let derived_sources = if derived_columns.is_none() {
                inner
                    .map(|inner| inner.source_tables.into_iter().map(|t| t.name).collect())
                    .unwrap_or_default()
            } else {
                Vec::new()
            };
            tables.push(TableReference {
                name: "(subquery)".to_string(),
                alias: Some(a.name.value.clone()),
                derived_columns,
                derived_sources,
            });
        }
        _ => {}
    }
}

fn build_alias_map(tables: &[TableReference]) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for t in tables {
        if let Some(alias) = &t.alias {
            map.insert(alias.to_lowercase(), t.name.clone());
        }
    }
    map
}

/// A model output column with no column-level lineage to any source — an
/// aliased `COUNT(*)`, a literal, a computed expression. It belongs in the
/// model's column set but has no upstream edge.
fn source_less_column(target: &str) -> ColumnLineage {
    ColumnLineage {
        source_table: None,
        source_column: String::new(),
        target_column: target.to_string(),
        transform: TransformKind::Expression,
    }
}

/// Returns `(columns, has_star, unresolved_projections)` — see
/// [`LineageResult::unresolved_projections`] for what the third element means
/// and why `columns` alone cannot answer "is this the model's whole output?".
fn extract_select_columns(
    projection: &[SelectItem],
    alias_map: &HashMap<String, String>,
    source_tables: &[TableReference],
) -> (Vec<ColumnLineage>, bool, usize) {
    let mut columns = Vec::new();
    let mut has_star = false;
    let mut unresolved_projections = 0usize;

    for item in projection {
        match item {
            SelectItem::Wildcard(_) => {
                has_star = true;
            }
            SelectItem::QualifiedWildcard(_, _) => {
                has_star = true;
            }
            SelectItem::UnnamedExpr(expr) => {
                // An unnamed item has a predictable output name only when it is
                // a bare (optionally table-qualified) identifier: `SELECT a` and
                // `SELECT t.a` both output a column named `a`. Everything else —
                // `SELECT (a)`, `SELECT a + b`, `SELECT UPPER(a)`, `SELECT 1` —
                // is named by the warehouse's own rules, which Rocky does not
                // model. Count those as unresolved even when `extract_expr_lineage`
                // succeeds: the entry it produces is named after the *traced
                // source* column, which is the right answer for lineage and the
                // wrong one for the output schema.
                if !matches!(expr, Expr::Identifier(_) | Expr::CompoundIdentifier(_)) {
                    unresolved_projections += 1;
                }
                // Push whatever lineage we can recover regardless — the edge is
                // still useful for impact analysis even when the output name
                // isn't authoritative.
                if let Some(lineage) = extract_expr_lineage(expr, alias_map, source_tables) {
                    columns.push(lineage);
                }
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                match extract_expr_lineage(expr, alias_map, source_tables) {
                    Some(mut lineage) => {
                        lineage.target_column = alias.value.clone();
                        columns.push(lineage);
                    }
                    // A named projection with no upstream column — `COUNT(*)`,
                    // a literal, a multi-column expression — is still a real
                    // output column. Emit a source-less entry so it's not
                    // dropped from the model's schema (the column set drives
                    // `rocky profile` / the Inspector Columns tab and the
                    // typed schema); it simply has no column-level lineage edge.
                    None => columns.push(source_less_column(&alias.value)),
                }
            }
            // Spark SQL `SELECT expr AS (a, b, c)` — multi-alias binding.
            // Emit one lineage entry per alias, cloning the upstream lineage.
            SelectItem::ExprWithAliases { expr, aliases } => {
                match extract_expr_lineage(expr, alias_map, source_tables) {
                    Some(base) => {
                        for alias in aliases {
                            let mut lineage = base.clone();
                            lineage.target_column = alias.value.clone();
                            columns.push(lineage);
                        }
                    }
                    None => {
                        for alias in aliases {
                            columns.push(source_less_column(&alias.value));
                        }
                    }
                }
            }
        }
    }

    (columns, has_star, unresolved_projections)
}

fn extract_expr_lineage(
    expr: &Expr,
    alias_map: &HashMap<String, String>,
    source_tables: &[TableReference],
) -> Option<ColumnLineage> {
    match expr {
        Expr::Identifier(ident) => {
            let col_name = ident.value.clone();
            // No table qualifier — try to resolve from single source
            let source_table = if source_tables.len() == 1 {
                Some(
                    source_tables[0]
                        .alias
                        .clone()
                        .unwrap_or(source_tables[0].name.clone()),
                )
            } else {
                None
            };
            Some(ColumnLineage {
                source_table,
                source_column: col_name.clone(),
                target_column: col_name,
                transform: TransformKind::Direct,
            })
        }
        Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
            let table_part = parts[parts.len() - 2].value.to_lowercase();
            let col_name = parts[parts.len() - 1].value.clone();
            let resolved_table = alias_map.get(&table_part).cloned().unwrap_or(table_part);
            Some(ColumnLineage {
                source_table: Some(resolved_table),
                source_column: col_name.clone(),
                target_column: col_name,
                transform: TransformKind::Direct,
            })
        }
        Expr::Cast { expr, kind, .. } => {
            let mut lineage = extract_expr_lineage(expr, alias_map, source_tables)?;
            // A fallible cast (`TRY_CAST` / `SAFE_CAST`) yields NULL on a failed
            // conversion, so its output is nullable even over a non-null input.
            // Track it distinctly so typecheck doesn't carry the input's
            // non-null bit through (#1148).
            lineage.transform = match kind {
                // Fallible outer cast: nullable output regardless of the inner.
                CastKind::TryCast | CastKind::SafeCast => TransformKind::TryCast,
                // Infallible outer cast: fallibility is sticky — an inner edge
                // already classified `TryCast` (e.g.
                // `CAST(TRY_CAST(x AS INT) AS BIGINT)`) still returns NULL when
                // the inner conversion fails, so the output stays nullable. A
                // cast chain with no fallible link keeps the plain `Cast`
                // classification (the outer cast's target type is recovered in
                // typecheck Step 2 either way).
                CastKind::Cast | CastKind::DoubleColon => {
                    if lineage.transform == TransformKind::TryCast {
                        TransformKind::TryCast
                    } else {
                        TransformKind::Cast
                    }
                }
            };
            Some(lineage)
        }
        Expr::Function(func) => {
            // Try to trace through aggregate/scalar functions to their column args
            let func_name = func.name.to_string().to_uppercase();
            let args = &func.args;
            match args {
                sqlparser::ast::FunctionArguments::List(arg_list) => {
                    // Trace through first column argument
                    for arg in &arg_list.args {
                        if let sqlparser::ast::FunctionArg::Unnamed(
                            sqlparser::ast::FunctionArgExpr::Expr(inner_expr),
                        ) = arg
                            && let Some(mut lineage) =
                                extract_expr_lineage(inner_expr, alias_map, source_tables)
                        {
                            lineage.transform = TransformKind::Aggregation(func_name.clone());
                            return Some(lineage);
                        }
                    }
                    None
                }
                _ => None,
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_select() {
        let result = extract_lineage("SELECT id, name FROM catalog.schema.users").unwrap();
        assert_eq!(result.source_tables.len(), 1);
        assert_eq!(result.source_tables[0].name, "catalog.schema.users");
        assert!(!result.has_star);
        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.columns[0].source_column, "id");
        assert_eq!(result.columns[0].target_column, "id");
        assert_eq!(result.columns[1].source_column, "name");
    }

    #[test]
    fn test_select_star() {
        let result = extract_lineage("SELECT * FROM catalog.schema.users").unwrap();
        assert!(result.has_star);
        assert!(result.columns.is_empty()); // star doesn't produce individual lineage
    }

    // ----- projection completeness (`unresolved_projections`) -----
    //
    // `columns` is a lineage graph that doubles as a schema enumeration, and it
    // is not always a faithful one. These pin the cases where it isn't, so a
    // consumer that needs the model's *whole* output set has a signal to gate
    // on rather than inferring completeness from `has_star` or emptiness.

    #[test]
    fn test_plain_identifiers_are_fully_resolved() {
        let result = extract_lineage("SELECT id, name FROM catalog.schema.users").unwrap();
        assert_eq!(result.unresolved_projections, 0);
        assert!(!result.has_star);
    }

    #[test]
    fn test_qualified_identifiers_are_fully_resolved() {
        // `u.id` outputs a column named `id` — predictable, so not unresolved.
        let result = extract_lineage("SELECT u.id, u.name FROM catalog.schema.users u").unwrap();
        assert_eq!(result.unresolved_projections, 0);
    }

    #[test]
    fn test_aliased_expressions_are_fully_resolved() {
        // `expr AS name` always yields a correctly named output column, so
        // computing something does not cost completeness — only *not naming*
        // the result does.
        let result = extract_lineage(
            "SELECT UPPER(name) AS upper_name, COUNT(*) AS n FROM catalog.schema.users GROUP BY 1",
        )
        .unwrap();
        assert_eq!(result.unresolved_projections, 0);
        assert_eq!(result.columns.len(), 2);
    }

    #[test]
    fn test_parenthesised_projection_counts_as_unresolved() {
        // `(id)` is `Expr::Nested`, which the extractor does not trace: it
        // yields no column entry, and with no star and no alias there is
        // nothing else to signal the gap.
        let result = extract_lineage("SELECT (id) FROM catalog.schema.users").unwrap();
        assert_eq!(result.unresolved_projections, 1);
        assert!(!result.has_star);
        assert!(result.columns.is_empty());
    }

    #[test]
    fn test_partial_parenthesised_projection_counts_as_unresolved() {
        // The case that defeats an emptiness heuristic: one item resolves, one
        // does not, so `columns` is non-empty and still short of the truth.
        let result = extract_lineage("SELECT (id), name FROM catalog.schema.users").unwrap();
        assert_eq!(result.unresolved_projections, 1);
        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.columns[0].target_column, "name");
    }

    #[test]
    fn test_unnamed_traced_expression_counts_as_unresolved() {
        // `UPPER(name)` *is* traced — to `name`. That entry is correct lineage
        // and an incorrect output name (the warehouse names this column by its
        // own rules), so completeness must not be claimed.
        let result = extract_lineage("SELECT UPPER(name) FROM catalog.schema.users").unwrap();
        assert_eq!(result.unresolved_projections, 1);
        // The lineage edge is still recorded — the signal is additive and does
        // not change what `columns` contains.
        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.columns[0].source_column, "name");
    }

    #[test]
    fn test_unresolved_projections_counts_every_offending_item() {
        let result =
            extract_lineage("SELECT (id), (name), email FROM catalog.schema.users").unwrap();
        assert_eq!(result.unresolved_projections, 2);
    }

    #[test]
    fn test_derived_table_columns_resolved() {
        // `SELECT * FROM (<subquery>) AS alias` records the inner query's
        // output columns on the derived TableReference so the outer star can
        // expand to them.
        let result =
            extract_lineage("SELECT * FROM (SELECT ts, id FROM raw.base) AS x WHERE ts >= '2026'")
                .unwrap();
        assert!(result.has_star);
        assert_eq!(result.source_tables.len(), 1);
        assert_eq!(result.source_tables[0].name, "(subquery)");
        assert_eq!(result.source_tables[0].alias, Some("x".to_string()));
        assert_eq!(
            result.source_tables[0].derived_columns,
            Some(vec!["ts".to_string(), "id".to_string()])
        );
    }

    #[test]
    fn test_derived_table_inner_star_records_sources() {
        // When the inner query is itself an unresolved `SELECT *`, its columns
        // can't be enumerated here — but the inner source names are recorded on
        // `derived_sources` so a model/source-graph-aware consumer can resolve
        // the star transitively. This is the import-dbt microbatch wrapper over
        // a `SELECT *` staging body.
        let result = extract_lineage(
            "SELECT * FROM (SELECT * FROM up) AS _rocky_microbatch \
             WHERE ts >= @start_date AND ts < @end_date",
        )
        .unwrap();
        assert!(result.has_star);
        assert_eq!(result.source_tables.len(), 1);
        assert_eq!(result.source_tables[0].name, "(subquery)");
        assert_eq!(result.source_tables[0].derived_columns, None);
        assert_eq!(
            result.source_tables[0].derived_sources,
            vec!["up".to_string()]
        );
    }

    #[test]
    fn test_derived_table_columns_with_time_interval_placeholders() {
        // The exact shape the import-dbt microbatch→time_interval rewrite emits.
        let result = extract_lineage(
            "SELECT * FROM (SELECT ts, id FROM raw.base) AS _rocky_microbatch \
             WHERE ts >= @start_date AND ts < @end_date",
        )
        .unwrap();
        assert!(result.has_star);
        assert_eq!(
            result.source_tables[0].derived_columns,
            Some(vec!["ts".to_string(), "id".to_string()])
        );
    }

    #[test]
    fn test_derived_table_uses_inner_output_names() {
        // Inner aliases become the derived table's output column names.
        let result =
            extract_lineage("SELECT * FROM (SELECT ts AS event_time, id FROM raw.base) AS x")
                .unwrap();
        assert_eq!(
            result.source_tables[0].derived_columns,
            Some(vec!["event_time".to_string(), "id".to_string()])
        );
    }

    #[test]
    fn test_qualified_wildcard_over_derived_table() {
        // `SELECT x.*` (QualifiedWildcard) over a derived table still resolves.
        let result = extract_lineage("SELECT x.* FROM (SELECT ts, id FROM raw.base) AS x").unwrap();
        assert!(result.has_star);
        assert_eq!(
            result.source_tables[0].derived_columns,
            Some(vec!["ts".to_string(), "id".to_string()])
        );
    }

    #[test]
    fn test_derived_table_inner_star_not_resolved() {
        // An inner `SELECT *` can't be enumerated statically — no derived
        // columns, falling back to the pre-existing (empty) behavior.
        let result = extract_lineage("SELECT * FROM (SELECT * FROM raw.base) AS x").unwrap();
        assert!(result.has_star);
        assert_eq!(result.source_tables[0].derived_columns, None);
    }

    #[test]
    fn test_plain_table_has_no_derived_columns() {
        let result = extract_lineage("SELECT * FROM catalog.schema.users").unwrap();
        assert_eq!(result.source_tables[0].derived_columns, None);
    }

    #[test]
    fn test_aliased_columns() {
        let result =
            extract_lineage("SELECT id, name AS customer_name FROM catalog.schema.customers")
                .unwrap();
        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.columns[1].source_column, "name");
        assert_eq!(result.columns[1].target_column, "customer_name");
    }

    #[test]
    fn test_join_with_table_aliases() {
        let result = extract_lineage(
            "SELECT o.order_id, c.name FROM catalog.schema.orders o JOIN catalog.schema.customers c ON o.customer_id = c.id",
        )
        .unwrap();
        assert_eq!(result.source_tables.len(), 2);
        assert_eq!(result.source_tables[0].alias, Some("o".to_string()));
        assert_eq!(result.source_tables[1].alias, Some("c".to_string()));
        assert_eq!(result.columns.len(), 2);
        // o.order_id resolves to catalog.schema.orders
        assert_eq!(
            result.columns[0].source_table,
            Some("catalog.schema.orders".to_string())
        );
        assert_eq!(result.columns[0].source_column, "order_id");
        // c.name resolves to catalog.schema.customers
        assert_eq!(
            result.columns[1].source_table,
            Some("catalog.schema.customers".to_string())
        );
    }

    #[test]
    fn test_star_with_qualified_columns() {
        let result = extract_lineage(
            "SELECT *, CAST(NULL AS STRING) AS _loaded_by FROM catalog.schema.table",
        )
        .unwrap();
        assert!(result.has_star);
        // CAST(NULL...) is a function-like expression, may not produce lineage
    }

    #[test]
    fn test_subquery_in_from() {
        let result =
            extract_lineage("SELECT id FROM (SELECT id FROM catalog.schema.users) t").unwrap();
        // Outer query sees the subquery as a derived table
        assert!(!result.source_tables.is_empty());
    }

    #[test]
    fn test_lineage_serialization() {
        let result = extract_lineage("SELECT id, name FROM cat.sch.tbl").unwrap();
        let json = serde_json::to_string(&result).unwrap();
        assert!(json.contains("source_column"));
        assert!(json.contains("target_column"));
    }

    #[test]
    fn test_empty_sql() {
        let result = extract_lineage("");
        assert!(result.is_err());
    }

    #[test]
    fn test_non_select() {
        let result = extract_lineage("CREATE TABLE t (id INT)");
        assert!(result.is_err());
    }

    // ── TransformKind Display tests ────────────────────────────────────────

    #[test]
    fn test_transform_kind_display_direct() {
        assert_eq!(TransformKind::Direct.to_string(), "direct");
    }

    #[test]
    fn test_transform_kind_display_cast() {
        assert_eq!(TransformKind::Cast.to_string(), "cast");
    }

    #[test]
    fn test_transform_kind_display_aggregation() {
        assert_eq!(
            TransformKind::Aggregation("SUM".to_string()).to_string(),
            "aggregation: sum"
        );
        assert_eq!(
            TransformKind::Aggregation("COUNT".to_string()).to_string(),
            "aggregation: count"
        );
    }

    #[test]
    fn test_transform_kind_display_try_cast() {
        assert_eq!(TransformKind::TryCast.to_string(), "try_cast");
    }

    #[test]
    fn test_transform_kind_display_expression() {
        assert_eq!(TransformKind::Expression.to_string(), "expression");
    }

    #[test]
    fn test_transform_kind_is_cast() {
        assert!(TransformKind::Cast.is_cast());
        assert!(TransformKind::TryCast.is_cast());
        assert!(!TransformKind::Direct.is_cast());
        assert!(!TransformKind::Expression.is_cast());
        assert!(!TransformKind::Aggregation("SUM".to_string()).is_cast());
    }

    #[test]
    fn test_fallible_casts_classified_distinctly() {
        // TRY_CAST / SAFE_CAST return NULL on a failed conversion, so lineage
        // must classify them as `TryCast` (nullable output) rather than the
        // infallible `Cast` — the input's non-null bit must not carry through
        // (#1148). `CAST` and `::` stay `Cast`.
        let try_cast =
            extract_lineage("SELECT TRY_CAST(id AS BIGINT) AS id FROM catalog.schema.users")
                .unwrap();
        assert_eq!(try_cast.columns[0].transform, TransformKind::TryCast);

        let safe_cast =
            extract_lineage("SELECT SAFE_CAST(id AS BIGINT) AS id FROM catalog.schema.users")
                .unwrap();
        assert_eq!(safe_cast.columns[0].transform, TransformKind::TryCast);

        let plain_cast =
            extract_lineage("SELECT CAST(id AS BIGINT) AS id FROM catalog.schema.users").unwrap();
        assert_eq!(plain_cast.columns[0].transform, TransformKind::Cast);

        let double_colon =
            extract_lineage("SELECT id::BIGINT AS id FROM catalog.schema.users").unwrap();
        assert_eq!(double_colon.columns[0].transform, TransformKind::Cast);
    }

    #[test]
    fn test_fallible_cast_nested_in_infallible_stays_try_cast() {
        // Fallibility is sticky: an infallible `CAST` / `::` wrapping an inner
        // `TRY_CAST` still returns NULL when the inner conversion fails, so the
        // whole edge must remain `TryCast` — the outer cast must not overwrite
        // the inner fallibility back to `Cast` (#1148).
        let nested_cast = extract_lineage(
            "SELECT CAST(TRY_CAST(id AS INT) AS BIGINT) AS id FROM catalog.schema.users",
        )
        .unwrap();
        assert_eq!(nested_cast.columns[0].transform, TransformKind::TryCast);

        let nested_colon =
            extract_lineage("SELECT TRY_CAST(id AS INT)::BIGINT AS id FROM catalog.schema.users")
                .unwrap();
        assert_eq!(nested_colon.columns[0].transform, TransformKind::TryCast);

        // Two infallible casts with no fallible link stay `Cast`.
        let nested_plain = extract_lineage(
            "SELECT CAST(CAST(id AS STRING) AS BIGINT) AS id FROM catalog.schema.users",
        )
        .unwrap();
        assert_eq!(nested_plain.columns[0].transform, TransformKind::Cast);
    }

    #[test]
    fn test_lineage_transform_kinds_in_select() {
        // Direct reference
        let result = extract_lineage("SELECT id FROM catalog.schema.users").unwrap();
        assert_eq!(result.columns[0].transform, TransformKind::Direct);

        // Cast
        let result =
            extract_lineage("SELECT CAST(id AS BIGINT) AS id FROM catalog.schema.users").unwrap();
        assert_eq!(result.columns[0].transform, TransformKind::Cast);

        // Aggregation
        let result =
            extract_lineage("SELECT SUM(amount) AS total FROM catalog.schema.orders").unwrap();
        assert_eq!(
            result.columns[0].transform,
            TransformKind::Aggregation("SUM".to_string())
        );
    }

    /// Named projections with no traceable source (an aliased `COUNT(*)`, a
    /// computed multi-source expression) must still appear as output columns —
    /// with no source, so they carry no lineage edge but aren't dropped from
    /// the model's schema. (Regression: `COUNT(*) AS order_count` was silently
    /// omitted, so it vanished from `rocky profile` / the Inspector Columns.)
    #[test]
    fn source_less_named_projections_are_kept() {
        let result = extract_lineage(
            "SELECT customer_id, COUNT(*) AS order_count, \
             total_revenue / order_count AS avg_order_value \
             FROM cat.sch.customer_orders GROUP BY customer_id",
        )
        .unwrap();
        let by_name: std::collections::HashMap<_, _> = result
            .columns
            .iter()
            .map(|c| (c.target_column.as_str(), c))
            .collect();
        // COUNT(*) and the division expression both lack a source column, but
        // are present in the column set.
        let order_count = by_name.get("order_count").expect("order_count kept");
        assert_eq!(order_count.source_table, None);
        let avg = by_name
            .get("avg_order_value")
            .expect("avg_order_value kept");
        assert_eq!(avg.source_table, None);
        // The sourced column still resolves normally.
        assert!(by_name.contains_key("customer_id"));
    }
}
